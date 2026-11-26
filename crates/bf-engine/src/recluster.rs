//! Recluster compaction: a namespace-wide ANN layout beside the existing segments.
//!
//! Each segment clusters its own rows, so one query neighborhood is spread over every segment and
//! a cold query must probe clusters in all of them (about 580K rows scanned at 10M for the
//! turbopuffer-matching recall). A recluster repartitions every row by position in vector space
//! into a few dozen **slabs** (a coarse k-means over a sample), and clusters each slab finely
//! with the ordinary index builder. A slab is a pseudo segment (an index object with the
//! quantized codes and full-precision vectors, plus a block fragment), listed in
//! `Manifest::ann_tiers` with a block-layout directory over the slabs. A slab's row IDs are
//! global row numbers (`SlabCover::base` plus the segment-local row), so a winner maps back to its
//! segment and row without a lookup table, and documents and attributes stay where they are.
//!
//! The job streams: segment vectors are copied once into a local spill file as 8-bit codes (SQ8,
//! one byte per dimension), the coarse partition and the leaves are trained and assigned from
//! those codes, and then each slab reads the exact stored vectors of its rows back from their
//! source segments (never more than a slab's rows are in memory at a time) and is built and
//! uploaded, one after another. Every fetched vector is checked against its spilled row.
//!
//! The build runs as the automatic recluster's job (`auto_recluster.rs`): it reads a manifest
//! snapshot, claims nothing, and hands the finished tier to the indexer, which installs it in its
//! next fenced publication. It runs on a pool of half the cores, and between slabs it waits while
//! the indexer flushes or compacts.
use crate::{
    Engine, ann_dir,
    auto_recluster::{Job, Kind, Target},
    log_error, segment,
};
use bf_types::ApiError;
use bf_wal::{Segment, SlabCover, SlabSet, Wal};
use futures::{StreamExt, TryStreamExt};
use rayon::prelude::*;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

mod assignment;
#[cfg(test)]
mod experiment;
mod math;
mod source;
pub(crate) mod spill;
mod sq8;

/// Wall time within one build stage. Concurrent uploads overlap CPU stages; aggregate I/O times
/// across workers are service time, so neither is added to the total build's wall time.
struct Stage(&'static str, std::time::Instant);
impl Stage {
    fn new(name: &'static str) -> Self {
        #[cfg(test)]
        if name == "leaf_assignment" {
            experiment::perf_control(true);
        }
        Self(name, std::time::Instant::now())
    }
    fn record(name: &'static str, elapsed: std::time::Duration) {
        #[cfg(test)]
        if name == "leaf_assignment" {
            experiment::perf_control(false);
        }
        let seconds = elapsed.as_secs_f64();
        if std::env::var_os("BF_TRACE_STAGES").is_some()
            || (cfg!(test) && std::env::var_os("RR4_INPUT").is_some())
        {
            eprintln!("RR4 stage={name} seconds={seconds:.6}");
        }
        metrics::histogram!("blowfish_recluster_stage_seconds", "stage" => name).record(seconds);
        tracing::info!(stage = name, seconds, "recluster: stage finished");
    }
}
impl Drop for Stage {
    fn drop(&mut self) {
        Self::record(self.0, self.1.elapsed());
    }
}

/// What to build.
#[derive(Clone, Debug)]
pub struct ReclusterOptions {
    /// The vector attribute.
    pub attribute: String,
    /// Coarse partitions (slabs). Cells larger than `max_slab_rows` are split into several slabs.
    pub cells: usize,
    /// Target rows per leaf cluster inside a slab; zero chooses from the tier's live population.
    pub cluster_rows: usize,
    /// Most rows in one slab (bounds the builder's memory: about 6.6 KB per row at 1,024 dims in f16).
    pub max_slab_rows: usize,
    /// Directory for the spill file (deleted when the job ends).
    pub scratch: std::path::PathBuf,
}

/// What a recluster built.
#[derive(Clone, Debug, Default)]
pub struct ReclusterReport {
    pub rows: u64,
    /// Target leaf population used for this build.
    pub cluster_rows: usize,
    pub slabs: usize,
    pub clusters: usize,
    pub directory_bytes: u64,
    pub seconds: f64,
}

/// The leaf clusters and coarse cells every build uses.
pub(crate) const CELLS: usize = 32;
/// Two measured regimes, with no interpolation into an unqualified warm layout. NQ's
/// 320-row leaves regress at matched recall, so smaller tiers retain 200-row leaves. The
/// 640-row layout wins at 10M; that is the first population where it is selected, and the
/// leaf size stays capped there for larger tiers pending their own measurements.
pub(crate) fn target_leaf_rows(rows: u64) -> usize {
    if rows < 10_000_000 { 200 } else { 640 }
}

/// A finished build: the tier, ready for the indexer to install.
pub(crate) struct Built {
    pub set: SlabSet,
    pub report: ReclusterReport,
    /// The slab bytes written locally beside the upload, awaiting the manifest commit.
    pub prestage: Option<crate::local_ann::Prestage>,
}

/// The pool builds run on: half the cores (at least one), so queries keep the rest.
fn pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
        rayon::ThreadPoolBuilder::new()
            .num_threads((cores / 2).max(1))
            .thread_name(|i| format!("bf-recluster-{i}"))
            .build()
            .expect("recluster pool")
    })
}

/// A build stopped between rows because the server is shutting down.
fn shutting_down() -> ApiError {
    ApiError::new(503, "the server is shutting down")
}

/// Run `f` on a blocking thread, inside the build pool.
async fn on_pool<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, ApiError> {
    tokio::task::spawn_blocking(move || pool().install(f))
        .await
        .map_err(segment::index_error)
}

impl Engine {
    /// Recluster `ns` now: the automatic recluster's job, run in the foreground whatever its
    /// thresholds say. Over the newest manifest it builds a delta tier when a base is installed
    /// and rows are outside the tiers (unless a merge is due), and otherwise a base over every
    /// segment; then it runs index passes until one installs the tier. For tests and benchmarks;
    /// a server does this by itself.
    pub async fn recluster(
        &self,
        ns: &str,
        opts: &ReclusterOptions,
    ) -> Result<ReclusterReport, ApiError> {
        // Unit tests share one process, and a recluster owns its process-global build state.
        // Hold the shared side of the gate so graph-admission tests can take it exclusively.
        #[cfg(test)]
        let _shared = crate::warm_graph::ADMISSION_GATE.read().await;
        self.run_recluster_job(ns, opts).await
    }

    /// Build the tier `target` describes for `opts.attribute` over `manifest` (a snapshot: no
    /// claim is taken, and indexing continues). Uploads its slabs, directory, and (for a base
    /// that trains them) coarse cells, and publishes nothing.
    // Boxed with an explicit `Send`: the build's streams of closures otherwise trip rustc's
    // higher-ranked `Send` inference when the job is spawned.
    pub(crate) fn build_tier<'a>(
        &'a self,
        ns: &'a str,
        manifest: &'a bf_wal::Manifest,
        target: &'a Target,
        opts: &'a ReclusterOptions,
        job: &'a Job,
    ) -> futures::future::BoxFuture<'a, Result<Built, ApiError>> {
        Box::pin(async move {
            let _busy = bf_alloc::busy();
            let context = crate::read_cache::ReadContext::new(self.0.read_cache.clone());
            crate::read_cache::scope(
                context,
                crate::vector_locator::scope_manifest(
                    manifest,
                    self.build_tier_inner(ns, manifest, target, opts, job),
                ),
            )
            .await
        })
    }

    async fn build_tier_inner(
        &self,
        ns: &str,
        manifest: &bf_wal::Manifest,
        target: &Target,
        opts: &ReclusterOptions,
        job: &Job,
    ) -> Result<Built, ApiError> {
        let t0 = std::time::Instant::now();
        let segments = &target.segments;
        let store = &self.0.store;
        let attr = opts.attribute.as_str();
        let wal = Wal::new(store.clone(), ns).map_err(log_error)?;
        let sources = crate::vector_locator::Sources::from_manifest(manifest);
        if segments.is_empty()
            || segments
                .iter()
                .any(|s| crate::upgrade::files(s, attr).is_none() && !sources.covers(s, attr))
        {
            return Err(ApiError::new(
                400,
                "every segment needs an authoritative vector source",
            ));
        }

        // Global row numbers: segment bases in manifest order.
        let mut covered = Vec::new();
        let mut total: u64 = 0;
        for seg in segments {
            covered.push(SlabCover {
                segment: seg.id.clone(),
                base: total as u32,
                rows: seg.row_count,
                live_generation: seg.live_generation,
            });
            total += u64::from(seg.row_count);
        }
        if total >= u64::from(u32::MAX) {
            return Err(ApiError::new(400, "too many rows for one recluster"));
        }
        let (dims, dtype) = segments[0]
            .schema
            .get(attr)
            .and_then(segment::dense_type)
            .ok_or_else(segment::corrupt)?;
        if segments
            .iter()
            .any(|s| s.schema.get(attr).and_then(segment::dense_type) != Some((dims, dtype)))
        {
            return Err(segment::corrupt());
        }
        let width = dims * dtype.width();
        let metric = segment::vector_metric(manifest.distance_metric);
        let inputs = crate::vector_locator::inputs::inputs(store, manifest, segments, attr).await?;

        // Pass 1: copy every live row's vector into a spill file indexed by global row.
        // Keep the owner in this future: dropping it unlinks the spill even if a blocking
        // worker still has the open file. Unix keeps that worker's descriptor valid.
        let spill = spill::Spill::create(&opts.scratch, total * dims as u64)
            .map_err(segment::index_error)?;
        self.recluster_with_spill(
            &wal,
            opts,
            covered,
            spill.file.clone(),
            (dims, width, dtype, metric),
            inputs,
            target,
            t0,
            job,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn recluster_with_spill(
        &self,
        wal: &Wal,
        opts: &ReclusterOptions,
        covered: Vec<SlabCover>,
        spill: Arc<std::fs::File>,
        (dims, width, dtype, metric): (usize, usize, bf_vector::VectorType, bf_vector::Metric),
        inputs: Vec<crate::vector_locator::inputs::Input>,
        target: &Target,
        t0: std::time::Instant,
        job: &Job,
    ) -> Result<Built, ApiError> {
        let store = &self.0.store;
        let (attr, ns) = (opts.attribute.as_str(), wal.namespace().to_string());

        // Cluster payloads arrive contiguously. Keep that order on disk instead of scattering
        // millions of tiny writes by global row; a four-byte address per row preserves identity.
        // Each row also remembers where its exact vector lives (input, leaf, and place in it).
        let total_rows: usize = covered.iter().map(|c| c.rows as usize).sum();
        let inputs = Arc::new(inputs);
        let sq8 = Arc::new(sample_codes(store, &inputs, dims, dtype).await?);
        let zeros = || -> Vec<std::sync::atomic::AtomicU32> {
            (0..total_rows)
                .map(|_| std::sync::atomic::AtomicU32::new(0))
                .collect()
        };
        let spill_offsets = zeros();
        let locations = source::Locations::new(total_rows);
        let next_spill_row = std::sync::atomic::AtomicU32::new(0);
        let copy_stage = Stage::new("copy");
        let spill_write_nanos = std::sync::atomic::AtomicU64::new(0);
        let copied: Vec<Vec<u32>> = futures::stream::iter((0..inputs.len()).map(|n| {
            let spill = spill.clone();
            let inputs = inputs.clone();
            let sq8 = sq8.clone();
            let spill_write_nanos = &spill_write_nanos;
            let spill_offsets = &spill_offsets;
            let next_spill_row = &next_spill_row;
            let locations = &locations;
            async move {
                let input = &inputs[n];
                job.pause().await;
                let index = Arc::new(
                    bf_vector::Index::open_with_metadata_len(
                        segment::ObjectReader::new(store, &input.file).bypass_cache(),
                        input.file.metadata_len,
                    )
                    .await
                    .map_err(segment::index_error)?,
                );
                if index.dims() != dims || index.vector_type() != dtype {
                    return Err(segment::corrupt());
                }
                let mut present = Vec::new();
                let clusters = index.cluster_count();
                let mut reads = futures::stream::iter((0..clusters).map(|c| {
                    let index = index.clone();
                    async move {
                        index
                            .read_cluster_rows(c)
                            .await
                            .map(|(rows, bytes)| (c, rows, bytes))
                    }
                }))
                .buffer_unordered(32);
                while let Some((cluster, rows, bytes)) =
                    reads.try_next().await.map_err(segment::index_error)?
                {
                    // Dead rows never reach the spill: compact the live rows of this
                    // cluster, keeping their order, and address them by global row below.
                    let globals: Vec<Option<u32>> = rows
                        .iter()
                        .map(|r| input.global(*r))
                        .collect::<Result<_, _>>()?;
                    let kept = globals.iter().flatten().count();
                    if kept == 0 {
                        continue;
                    }
                    let writing = std::time::Instant::now();
                    let start =
                        next_spill_row.fetch_add(kept as u32, std::sync::atomic::Ordering::Relaxed);
                    let packed = {
                        let (sq8, live) = (sq8.clone(), globals.clone());
                        on_pool(move || {
                            let mut packed = vec![0u8; kept * dims];
                            let live_rows = live
                                .iter()
                                .enumerate()
                                .filter(|(_, g)| g.is_some())
                                .map(|(i, _)| i);
                            for (out, i) in packed.chunks_exact_mut(dims).zip(live_rows) {
                                sq8.encode(
                                    &crate::vector_locator::decode(
                                        dtype,
                                        &bytes[i * width..(i + 1) * width],
                                    ),
                                    out,
                                );
                            }
                            packed
                        })
                        .await?
                    };
                    spill
                        .write_all_at(&packed, u64::from(start) * dims as u64)
                        .map_err(segment::index_error)?;
                    let live = globals
                        .iter()
                        .enumerate()
                        .filter_map(|(i, g)| g.map(|g| (i, g)));
                    for (slot, (ordinal, row)) in live.enumerate() {
                        spill_offsets[row as usize]
                            .store(start + slot as u32, std::sync::atomic::Ordering::Relaxed);
                        locations.set(row, n as u32, cluster as u32, ordinal as u32);
                        present.push(row);
                    }
                    spill_write_nanos.fetch_add(
                        writing.elapsed().as_nanos() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                Ok::<_, ApiError>(present)
            }
        }))
        .buffer_unordered(6)
        .try_collect()
        .await?;
        drop(copy_stage);
        #[cfg(test)]
        LAST_SPILL_ROWS.store(
            u64::from(next_spill_row.load(std::sync::atomic::Ordering::Relaxed)),
            std::sync::atomic::Ordering::Relaxed,
        );
        Stage::record(
            "spill_write",
            std::time::Duration::from_nanos(spill_write_nanos.into_inner()),
        );
        let spill_offsets: Arc<Vec<u32>> = Arc::new(
            spill_offsets
                .into_iter()
                .map(std::sync::atomic::AtomicU32::into_inner)
                .collect(),
        );
        let source = Arc::new(source::Source::new(
            store.clone(),
            inputs,
            locations,
            spill.clone(),
            spill_offsets.clone(),
            sq8.clone(),
            dims,
            dtype,
        ));
        let mut present: Vec<u32> = copied.into_iter().flatten().collect();
        present.sort_unstable();
        if present.is_empty() {
            return Err(ApiError::new(400, "no live rows to recluster"));
        }
        tracing::info!(rows = present.len(), "recluster: vectors copied");
        tracing::info!(
            "recluster {ns}: copied {} rows in {:.0}s",
            present.len(),
            t0.elapsed().as_secs_f64()
        );

        // Pass 2: coarse partition. Train on a sample (or take the base's cells), then assign
        // every row.
        let present = Arc::new(present);
        let spill_read_nanos = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let read_row = {
            let spill = spill.clone();
            let spill_read_nanos = spill_read_nanos.clone();
            let spill_offsets = spill_offsets.clone();
            let sq8 = sq8.clone();
            move |g: u32, buf: &mut Vec<u8>| -> Vec<f32> {
                buf.resize(dims, 0);
                let reading = std::time::Instant::now();
                spill
                    .read_exact_at(buf, u64::from(spill_offsets[g as usize]) * dims as u64)
                    .expect("spill file read");
                spill_read_nanos.fetch_add(
                    reading.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                bf_vector::prepare_vector(&sq8.decode(buf), metric)
            }
        };
        let reused = match &target.coarse {
            Some((file, rows)) => {
                Some((read_coarse(store, file, dims).await?, file.clone(), *rows))
            }
            None => None,
        };
        let cells = opts.cells.max(1).min(present.len());
        let (assignment, cell_rows, coarse) = {
            let (present, read_row) = (present.clone(), read_row.clone());
            let cells_n = cells;
            let known = reused.as_ref().map(|(centers, _, _)| centers.clone());
            let stop = self.0.stop.clone();
            on_pool(move || {
                let centers = known.unwrap_or_else(|| {
                    let _training = Stage::new("coarse_kmeans");
                    let mut state = 0x9E37_79B9_7F4A_7C15u64;
                    let mut next = move || {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state
                    };
                    // A coarse partition needs a few hundred points per cell, not the whole corpus.
                    let sample_n = (cells_n * 600).clamp(8_000, 50_000).min(present.len());
                    let picks: Vec<u32> = (0..sample_n)
                        .map(|_| present[next() as usize % present.len()])
                        .collect();
                    let sample: Vec<Vec<f32>> = picks
                        .par_iter()
                        .map_init(Vec::new, |buf, g| read_row(*g, buf))
                        .collect();
                    let refs: Vec<&[f32]> = sample.iter().map(|v| v.as_slice()).collect();
                    math::kmeans(&refs, cells_n, 8, 7)
                });
                let _assignment = Stage::new("coarse_assignment");
                let packed = math::Centers::new(centers.iter().map(Vec::as_slice));
                let assignment: Vec<u16> = present
                    .par_chunks(128)
                    .map_init(Vec::new, |buf, ids| {
                        if stop.stopping() {
                            return None;
                        }
                        let rows: Vec<Vec<f32>> = ids.iter().map(|g| read_row(*g, buf)).collect();
                        let refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
                        Some(
                            packed
                                .nearest(&refs)
                                .into_iter()
                                .map(|c| c as u16)
                                .collect::<Vec<_>>(),
                        )
                    })
                    .while_some()
                    .flatten_iter()
                    .collect();
                if stop.stopping() {
                    return None;
                }
                let mut cell_rows: Vec<Vec<u32>> = vec![Vec::new(); centers.len()];
                for (g, c) in present.iter().zip(&assignment) {
                    cell_rows[*c as usize].push(*g);
                }
                Some((assignment.len(), cell_rows, centers))
            })
            .await?
            .ok_or_else(shutting_down)?
        };
        // A base keeps its cells for the deltas built against it and for its merge.
        let coarse_file = match (target.kind, reused) {
            (Kind::Delta, _) => None,
            (Kind::Base, Some((_, file, rows))) => Some((file, rows)),
            (Kind::Base, None) => {
                let mut bytes = Vec::with_capacity(coarse.len() * dims * 4);
                for x in coarse.iter().flatten() {
                    bytes.extend_from_slice(&x.to_le_bytes());
                }
                let key = format!("ns/{ns}/slabs/{}coarse.bin", job.slab_prefix());
                Some((
                    segment::put_file(store, key, bytes, 0).await?,
                    present.len() as u64,
                ))
            }
        };
        tracing::info!(
            "recluster {ns}: {assignment} rows in {} cells (largest {}) at {:.0}s",
            cell_rows.len(),
            cell_rows.iter().map(Vec::len).max().unwrap_or(0),
            t0.elapsed().as_secs_f64()
        );

        // Pass 3: every row joins its nearest leaf cluster over its near cells (see
        // `assign_leaves`), and each cell's leaves make a slab (a huge cell is split), built and
        // uploaded in turn.
        let max = opts.max_slab_rows.max(1);
        let cluster_rows = if opts.cluster_rows == 0 {
            target_leaf_rows(present.len() as u64)
        } else {
            opts.cluster_rows
        };
        let jobs: Vec<SlabJob> = {
            let (present, read_row) = (present.clone(), read_row.clone());
            // A delta's few rows go in as few slabs as memory allows.
            let pack = target.kind == Kind::Delta;
            let stop = self.0.stop.clone();
            on_pool(move || {
                let jobs = assign_leaves(
                    &present,
                    &read_row,
                    &coarse,
                    cell_rows,
                    cluster_rows,
                    max,
                    &|| stop.stopping(),
                )?;
                Some(if pack { pack_slabs(jobs, max) } else { jobs })
            })
            .await?
            .ok_or_else(shutting_down)?
        };
        tracing::info!(
            "recluster {ns}: {} slabs planned at {:.0}s",
            jobs.len(),
            t0.elapsed().as_secs_f64()
        );
        let mut slabs: Vec<Segment> = Vec::new();
        let mut clusters = 0usize;
        let mut pending: Option<tokio::task::JoinHandle<Result<(), ApiError>>> = None;
        // Slab bytes that this node's stage will want are written locally while they upload.
        let prestage = crate::local_ann::Prestage::begin(dims);
        let mut rows_left = present.len() as u64;
        let uploads = Arc::new(tokio::sync::Mutex::new(Vec::<Segment>::new()));
        // Each slab's exact vectors are read while the slab before it builds, so at most two
        // slabs' vectors are in memory. Dropping the handle on an error ends the read.
        let fetch = |rows: Vec<u32>| {
            let source = source.clone();
            AbortOnDrop(tokio::spawn(async move { source.rows(&rows).await }))
        };
        let mut jobs = jobs.into_iter().enumerate().peekable();
        let mut fetching = jobs.peek().map(|(_, j)| fetch(j.rows.clone()));
        while let Some((
            n,
            SlabJob {
                rows,
                centers,
                assignment,
            },
        )) = jobs.next()
        {
            let k = centers.len();
            clusters += k;
            let rows_n = rows.len() as u32;
            // Between slabs, let a queued flush or compaction run first.
            job.pause().await;
            let staging = prestage.is_some();
            // The slab's exact vectors, at the stored precision (about 2 KB each at 1,024 dims
            // in f16), read back from their source segments and checked against the spill.
            let reading = Stage::new("source_read");
            let data = fetching
                .take()
                .expect("a fetch for every slab")
                .join()
                .await??;
            drop(reading);
            fetching = jobs.peek().map(|(_, j)| fetch(j.rows.clone()));
            let blob = on_pool(move || -> Result<BuiltSlab, ApiError> {
                let encoding = Stage::new("encoding");
                let params = bf_vector::BuildParams {
                    clusters: Some(k),
                    ..Default::default()
                };
                let builder = bf_vector::IndexBuilder::new(dims, metric)
                    .map_err(segment::index_error)?
                    .params(params)
                    .vector_type(dtype);
                let blob = builder
                    .build_packed_assigned(rows, data, centers, assignment)
                    .map_err(segment::index_error)?;
                drop(encoding);
                let _packing = Stage::new("packing_container");
                let (blob, meta) =
                    bf_vector::Fragment::build_ann(&blob).map_err(segment::index_error)?;
                let lens = if staging {
                    stage_lengths(&blob, meta)
                } else {
                    (0, 0)
                };
                Ok((blob, meta, lens))
            })
            .await??;
            let (blob, meta, lens) = blob;
            rows_left -= u64::from(rows_n);
            // Under the job's prefix, which its build record protects from GC until published.
            let id = format!("{}{n:05}", job.slab_prefix());
            let (store2, ns2, attr2, id2, uploads2, prestage2) = (
                store.clone(),
                ns.clone(),
                attr.to_string(),
                id.clone(),
                uploads.clone(),
                prestage.as_ref().map(|p| p.writer()),
            );
            // Upload while the next slab builds; at most one upload is outstanding.
            if let Some(p) = pending.take() {
                p.await.map_err(segment::index_error)??;
            }
            pending = Some(tokio::spawn(async move {
                let _upload = Stage::new("upload");
                let prefix = format!("ns/{ns2}/slabs/{id2}");
                let key = format!("{prefix}/ann.{}.bfa", uuid::Uuid::new_v4());
                let blob = bytes::Bytes::from(blob);
                let member = bf_vector::Index::open_with_metadata_len(blob.clone(), meta)
                    .await
                    .map_err(segment::index_error)?
                    .membership()
                    .map_err(segment::index_error)?;
                let put = segment::put_bytes(&store2, key.clone(), blob.clone(), meta);
                let ann = match (&prestage2, lens) {
                    (Some(pre), lens) if lens.0 > 0 => {
                        // Beside the upload, a local copy of what a stage of this node holds.
                        pre.upload(&store2, &key, blob, lens, rows_left, put)
                            .await?
                    }
                    _ => put.await?,
                };
                let mut files = std::collections::BTreeMap::new();
                let rows = crate::slab_rows::write(&store2, &ns2, &ann, &member).await?;
                files.insert(crate::slab_rows::role(&attr2), rows);
                files.insert(format!("annv2:{attr2}"), ann);
                uploads2.lock().await.push(Segment {
                    id: id2,
                    wal_start: 0,
                    wal_end: 0,
                    row_count: rows_n,
                    schema: Default::default(),
                    files,
                    live: bf_wal::SegmentFile {
                        key: String::new(),
                        size: 0,
                        metadata_len: 0,
                        stats_index: None,
                        text_header: None,
                    },
                    live_generation: 0,
                });
                Ok(())
            }));
            tracing::info!(
                "recluster {ns}: slab {} built at {:.0}s",
                n + 1,
                t0.elapsed().as_secs_f64()
            );
        }
        if let Some(p) = pending.take() {
            p.await.map_err(segment::index_error)??;
        }
        slabs.extend(uploads.lock().await.drain(..));
        slabs.sort_by(|a, b| a.id.cmp(&b.id));
        Stage::record(
            "spill_random_read",
            std::time::Duration::from_nanos(
                spill_read_nanos.load(std::sync::atomic::Ordering::Relaxed),
            ),
        );

        // The directory over the slabs. The indexer publishes the tier. The cold-query hint is
        // the base's (a delta's directory is small, and read beside it).
        let _directory = Stage::new("directory");
        let (dir, pages) =
            slab_directories(store, &ns, attr, &slabs, target.kind == Kind::Base).await?;
        let report = ReclusterReport {
            rows: present.len() as u64,
            cluster_rows,
            slabs: slabs.len(),
            clusters,
            directory_bytes: dir.size,
            seconds: 0.0,
        };
        Ok(Built {
            prestage,
            set: SlabSet {
                covered,
                slabs,
                dir,
                pages,
                format: bf_wal::TIER_FORMAT_VERSION,
                coarse_rows: coarse_file.as_ref().map_or(0, |(_, rows)| *rows),
                coarse: coarse_file.map(|(file, _)| file),
            },
            report: ReclusterReport {
                seconds: t0.elapsed().as_secs_f64(),
                ..report
            },
        })
    }
}

/// A spawned task that stops with its owner.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> AbortOnDrop<T> {
    async fn join(mut self) -> Result<T, ApiError> {
        (&mut self.0).await.map_err(segment::index_error)
    }
}
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Leaves sampled across all inputs to fit the spill's per-dimension scale.
const SCALE_LEAVES: usize = 256;

/// Fit the spill's 8-bit codes: the range of every dimension over evenly spaced leaves of every
/// input. Stored `i8` vectors need no fit, since their codes are exact.
async fn sample_codes(
    store: &bf_storage::Storage,
    inputs: &Arc<Vec<crate::vector_locator::inputs::Input>>,
    dims: usize,
    dtype: bf_vector::VectorType,
) -> Result<sq8::Sq8, ApiError> {
    if dtype == bf_vector::VectorType::I8 {
        return Ok(sq8::Sq8::identity(dims));
    }
    let per_input = (SCALE_LEAVES / inputs.len().max(1)).clamp(1, 16);
    let ranges: Vec<sq8::Range> = futures::stream::iter((0..inputs.len()).map(|n| {
        let inputs = inputs.clone();
        async move {
            let file = &inputs[n].file;
            let index = bf_vector::Index::open_with_metadata_len(
                segment::ObjectReader::new(store, file).bypass_cache(),
                file.metadata_len,
            )
            .await
            .map_err(segment::index_error)?;
            if index.dims() != dims || index.vector_type() != dtype {
                return Err(segment::corrupt());
            }
            let leaves = index.cluster_count();
            let picks = per_input.min(leaves);
            let mut range = sq8::Range::new(dims);
            for j in 0..picks {
                let (_, bytes) = index
                    .read_cluster_rows(j * leaves / picks)
                    .await
                    .map_err(segment::index_error)?;
                for row in bytes.chunks_exact(dims * dtype.width()) {
                    range.add(&crate::vector_locator::decode(dtype, row));
                }
            }
            Ok::<_, ApiError>(range)
        }
    }))
    .buffer_unordered(6)
    .try_collect()
    .await?;
    let mut all = sq8::Range::new(dims);
    for range in &ranges {
        all.merge(range);
    }
    Ok(all.fit())
}

/// Write the directory over `slabs`, and for a large one its paged copy; then the hint a cold
/// query reads beside the manifest (the paged header, or else the whole directory).
pub(crate) async fn slab_directories(
    store: &bf_storage::Storage,
    ns: &str,
    attr: &str,
    slabs: &[Segment],
    hint: bool,
) -> Result<(bf_wal::SegmentFile, Option<bf_wal::SegmentFile>), ApiError> {
    let dir = ann_dir::build(store, ns, slabs, attr, ann_dir::Layout::Slabs, None)
        .await?
        .ok_or_else(|| ApiError::new(500, "slab directory not built"))?;
    let paged_dir = ann_dir::load(store, &dir, None).await?;
    let pages = ann_dir::build_paged(store, ns, attr, paged_dir, slabs).await?;
    if pages.is_none() && hint {
        let bytes = segment::ObjectReader::new(store, &dir)
            .bypass_cache()
            .read(0..dir.size)
            .await
            .map_err(segment::index_error)?;
        ann_dir::write_hint(store, ns, ann_dir::Layout::Slabs, attr, &dir.key, &bytes).await;
    }
    Ok((dir, pages))
}

/// A base's coarse cells, as `build_tier` wrote them.
async fn read_coarse(
    store: &bf_storage::Storage,
    file: &bf_wal::SegmentFile,
    dims: usize,
) -> Result<Vec<Vec<f32>>, ApiError> {
    let bytes = segment::read_whole_uncached(store, file).await?;
    if dims == 0 || bytes.is_empty() || bytes.len() % (dims * 4) != 0 {
        return Err(segment::index_error(
            "coarse cells do not match the vectors",
        ));
    }
    Ok(bytes
        .chunks_exact(dims * 4)
        .map(|cell| {
            cell.as_chunks::<4>()
                .0
                .iter()
                .map(|x| f32::from_le_bytes(*x))
                .collect()
        })
        .collect())
}

/// A slab's object bytes, its metadata length, and the lengths a stage holds of it.
type BuiltSlab = (Vec<u8>, u64, (u64, u64));

/// How many bytes of a slab object a stage holds: through its codes, and through its exact
/// vectors. Zero for both when the layout is unfamiliar (the slab then simply is not staged).
pub(crate) fn stage_lengths(blob: &[u8], metadata_len: u64) -> (u64, u64) {
    let measure = || -> Option<(u64, u64)> {
        let fragment =
            bf_vector::Fragment::decode(blob.get(..usize::try_from(metadata_len).ok()?)?).ok()?;
        let raw = bf_vector::Directory::build(&[("s".into(), "k".into(), "k".into(), fragment)])
            .ok()??;
        let dir = bf_vector::Directory::decode(&raw).ok()?;
        let codes = dir
            .entries()
            .iter()
            .map(|e| e.blob_range().end.max(e.codes_range().end))
            .max()?;
        let vectors = dir
            .entries()
            .iter()
            .map(|e| e.vectors_range().end)
            .max()?
            .max(codes);
        Some((codes, vectors))
    };
    measure().unwrap_or((0, 0))
}

/// Consecutive slabs merged while they fit in `max` rows together (a delta's cells are small).
fn pack_slabs(jobs: Vec<SlabJob>, max: usize) -> Vec<SlabJob> {
    let mut out: Vec<SlabJob> = Vec::new();
    for job in jobs {
        match out.last_mut() {
            Some(last) if last.rows.len() + job.rows.len() <= max => {
                let offset = last.centers.len();
                let mut rows: Vec<(u32, usize)> = last
                    .rows
                    .iter()
                    .copied()
                    .zip(last.assignment.iter().copied())
                    .chain(
                        job.rows
                            .iter()
                            .copied()
                            .zip(job.assignment.iter().map(|a| a + offset)),
                    )
                    .collect();
                rows.sort_unstable();
                (last.rows, last.assignment) = rows.into_iter().unzip();
                last.centers.extend(job.centers);
            }
            _ => out.push(job),
        }
    }
    out
}

/// One slab to build: its global rows (ascending), its leaf clusters' centers, and each row's
/// cluster among them.
struct SlabJob {
    rows: Vec<u32>,
    centers: Vec<Vec<f32>>,
    assignment: Vec<usize>,
}

/// A cell's leaf groups: their centers, and each group's leaves (global leaf numbers).
type LeafGroups = (Vec<Vec<f32>>, Vec<Vec<u32>>);

/// Rows compared with their candidate centers together (see `assign_leaves`).
const ASSIGN_BLOCK: usize = 4;

/// Squared norm of `v`.
fn norm2(v: &[f32]) -> f32 {
    bf_vector::dots_x4([v, v, v, v], v)[0]
}

/// Squared distances from each of `rows` (with squared norms `norms`) to `count` centers given
/// by `center(n)` as (center, squared norm): `out[row][n]`. The SIMD kernel reads each center
/// once for four rows, without repacking the selected centers for every block.
#[allow(clippy::needless_range_loop)] // n addresses the center and each row's output.
fn block_distances<'a>(
    rows: &[&[f32]],
    norms: &[f32],
    count: usize,
    center: impl Fn(usize) -> (&'a [f32], f32),
) -> Vec<Vec<f32>> {
    let mut out = vec![vec![0f32; count]; rows.len()];
    for n in 0..count {
        let (c, cn) = center(n);
        for start in (0..rows.len()).step_by(4) {
            let pick = |j: usize| rows[(start + j).min(rows.len() - 1)];
            let dots = bf_vector::dots_x4([pick(0), pick(1), pick(2), pick(3)], c);
            for (j, dot) in dots.iter().enumerate().take(rows.len() - start) {
                out[start + j][n] = norms[start + j] + cn - 2.0 * dot;
            }
        }
    }
    out
}

/// Coarse cells a row's nearest leaf is looked for in.
const NEAR_CELLS: usize = 4;
/// Training rows per leaf cluster (as the index builder samples).
const LEAF_SAMPLES: usize = 32;
/// Most leaves in a cell that train one-level (about the index builder's two-level threshold).
const ONE_LEVEL_LEAVES: usize = 1024;
/// Leaf groups searched per row, over its near cells.
const GROUP_PROBES: usize = 12;

/// Train each cell's leaf clusters on a sample of its rows, then give every row the nearest leaf
/// among the leaves of its `NEAR_CELLS` nearest cells, and recompute each leaf's center from all
/// of its rows and assign again once. A row near a cell boundary then sits in the cluster
/// whose center is nearest to it, wherever that cluster's slab is, so the clusters a query
/// probes hold more of its neighbors: on BEIR NQ, a row's own cluster was its nearest one only
/// 80% of the time with each cell clustered on its own. Each cell's leaves (and the rows that
/// chose them) make one slab, or several when they hold more than `max` rows. `None` once
/// `stopping` says so (checked between cells and rows).
fn assign_leaves(
    present: &[u32],
    read_row: &(dyn Fn(u32, &mut Vec<u8>) -> Vec<f32> + Sync),
    coarse: &[Vec<f32>],
    cell_rows: Vec<Vec<u32>>,
    cluster_rows: usize,
    max: usize,
    stopping: &(dyn Fn() -> bool + Sync),
) -> Option<Vec<SlabJob>> {
    #[cfg(test)]
    experiment::capture(present, read_row, coarse, &cell_rows, cluster_rows, max);
    let distance = bf_vector::squared_distance;
    let t0 = std::time::Instant::now();
    let training = Stage::new("leaf_kmeans");
    let mut leaves: Vec<Vec<f32>> = Vec::new();
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::with_capacity(cell_rows.len());
    // Each cell's leaf groups (global leaf numbers).
    let mut members: Vec<Vec<Vec<u32>>> = Vec::with_capacity(cell_rows.len());
    for (cell, rows) in cell_rows.iter().enumerate() {
        if stopping() {
            return None;
        }
        let start = leaves.len();
        if !rows.is_empty() {
            let k = (rows.len() / cluster_rows).max(1);
            // A deterministic sample: a partial Fisher-Yates shuffle seeded by the cell.
            let mut picks = rows.clone();
            let n = (k * LEAF_SAMPLES).min(picks.len());
            let mut state =
                0x2545_F491_4F6C_DD1Du64 ^ (cell as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for i in 0..n {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let j = i + (state as usize) % (picks.len() - i);
                picks.swap(i, j);
            }
            picks.truncate(n);
            let sample: Vec<Vec<f32>> = picks
                .par_iter()
                .map_init(Vec::new, |buf, g| read_row(*g, buf))
                .collect();
            let refs: Vec<&[f32]> = sample.iter().map(|v| v.as_slice()).collect();
            // Leaves are grouped under about sqrt(k) centers for the assignment below. A large
            // cell trains two-level, and its parents' leaves are the groups: one-level training
            // compares every sample with every leaf, so its cost grows with the square of the
            // leaves (5.5 times the per-cell build at 1,700 leaves a cell). A small cell trains
            // one-level (0.003 more recall@10 on NQ at 420 leaves a cell), and its leaves are
            // grouped by a k-means over them.
            let mut cell_groups = Vec::new();
            if k > ONE_LEVEL_LEAVES {
                let (_, children) = bf_vector::kmeans_two_level(&refs, k, 8, 42 + cell as u64);
                for group in children {
                    let first = leaves.len() as u32;
                    leaves.extend(group);
                    cell_groups.push((first..leaves.len() as u32).collect::<Vec<u32>>());
                }
            } else {
                let trained = math::kmeans(&refs, k, 8, 42 + cell as u64);
                let trained_refs: Vec<&[f32]> = trained.iter().map(Vec::as_slice).collect();
                let g = (trained.len() as f64).sqrt().ceil() as usize;
                let centers = math::kmeans(&trained_refs, g, 8, 7 + cell as u64);
                cell_groups = vec![Vec::new(); centers.len()];
                for leaf in &trained {
                    cell_groups[bf_vector::nearest_center(leaf, &centers)]
                        .push(leaves.len() as u32);
                    leaves.push(leaf.clone());
                }
                cell_groups.retain(|g| !g.is_empty());
            }
            members.push(cell_groups);
        } else {
            members.push(Vec::new());
        }
        ranges.push(start..leaves.len());
    }
    drop(cell_rows);
    drop(training);
    tracing::info!(
        "recluster: {} leaves trained in {:.0}s",
        leaves.len(),
        t0.elapsed().as_secs_f64()
    );
    let near = NEAR_CELLS.min(coarse.len()).max(1);
    let probes = GROUP_PROBES;
    let by_distance = |a: &(f32, usize), b: &(f32, usize)| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1));
    #[cfg(test)]
    let mut drift = experiment::Drift::new(present.len(), &leaves);
    // Native BF16 matrix throughput amortizes packing for large center tables. Other CPUs
    // and small layouts retain the existing four-row kernel.
    let native = bf_vector::bf16_matrix_accelerated()
        && leaves.len() >= 1024
        && leaves.first().is_some_and(|l| l.len() >= 128);
    #[cfg(test)]
    let experiment = std::env::var_os("RR4_INPUT").is_some();
    #[cfg(test)]
    let batched = if experiment {
        std::env::var_os("RR4_BATCHED").is_some()
    } else {
        native
    };
    #[cfg(not(test))]
    let batched = native;
    // Coarse centers never move during leaf refinement. Cache their four nearest IDs, with
    // four bytes per row for the ordinary 32-cell layout. Larger explicit layouts retain
    // the uncached path, so no index is narrowed to a byte.
    let near_cells_cache = std::sync::OnceLock::<Vec<[u8; NEAR_CELLS]>>::new();
    #[cfg(test)]
    let cache_neighbors = batched
        && coarse.len() <= 256
        && (!experiment || std::env::var_os("RR4_CACHE_CELLS").is_some());
    #[cfg(not(test))]
    let cache_neighbors = batched && coarse.len() <= 256;
    #[cfg(test)]
    let batch_rows = std::env::var("RR4_BATCH_ROWS")
        .ok()
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(2048);
    #[cfg(not(test))]
    let batch_rows: usize = 2048;
    assert!(batch_rows > 0 && batch_rows.is_multiple_of(ASSIGN_BLOCK));
    let assign = |leaves: &[Vec<f32>],
                  #[cfg(test)] drift: Option<&experiment::Drift>|
     -> Option<Vec<u32>> {
        let _assignment = Stage::new("leaf_assignment");
        // Each cell's leaves under about sqrt(leaves) group centers (the means of their leaves),
        // so a row compares against the groups of its near cells and the leaves of its nearest
        // groups only, not every leaf of every near cell (a 100M layout has about 15,600 leaves a
        // cell).
        let dims = leaves.first().map_or(0, Vec::len);
        let groups: Vec<LeafGroups> = members
            .par_iter()
            .map(|cell_groups| {
                let centers = cell_groups
                    .iter()
                    .map(|group| {
                        let mut sum = vec![0f64; dims];
                        for &l in group {
                            for (s, x) in sum.iter_mut().zip(&leaves[l as usize]) {
                                *s += f64::from(*x);
                            }
                        }
                        sum.iter()
                            .map(|s| (*s / group.len() as f64) as f32)
                            .collect()
                    })
                    .collect();
                (centers, cell_groups.clone())
            })
            .collect();
        #[cfg(test)]
        let order_compared = std::sync::atomic::AtomicU64::new(0);
        // A row's nearest groups among its near cells, nearest first.
        let near_groups = |v: &[f32], cached: Option<&[u8; NEAR_CELLS]>| {
            let mut found: Vec<(f32, usize)> = Vec::new();
            let mut packed = [0u8; NEAR_CELLS];
            let mut visit = |cell: usize| {
                for (j, center) in groups[cell].0.iter().enumerate() {
                    found.push((distance(v, center), cell << 32 | j));
                }
            };
            if let Some(cells) = cached {
                for cell in &cells[..near] {
                    visit(usize::from(*cell));
                }
            } else {
                let mut cells: Vec<(f32, usize)> = coarse
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (distance(v, c), i))
                    .collect();
                if cells.len() > near {
                    cells.select_nth_unstable_by(near - 1, by_distance);
                    cells.truncate(near);
                }
                for (i, (_, cell)) in cells.into_iter().enumerate() {
                    if cache_neighbors {
                        packed[i] = cell as u8;
                    }
                    visit(cell);
                }
            }
            #[cfg(test)]
            order_compared.fetch_add(
                (if cached.is_some() { 0 } else { coarse.len() } + found.len()) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            if found.len() > probes && !cache_neighbors {
                found.select_nth_unstable_by(probes - 1, by_distance);
                found.truncate(probes);
            }
            (found, packed)
        };
        // Leaves are far larger than the CPU caches (13,400 of 4 KB each on BEIR NQ), and rows in
        // ID order are scattered over them, so every comparison would miss. Rows are visited in
        // the order of their nearest group instead: neighbors in that order compare with the same
        // few groups' leaves, which stay cached. The group pass costs a small part of the leaf one.
        let offsets: Vec<usize> = groups
            .iter()
            .scan(0usize, |at, (centers, _)| {
                let start = *at;
                *at += centers.len();
                Some(start)
            })
            .collect();
        let group_count = offsets
            .last()
            .map_or(0, |o| o + groups.last().map_or(0, |g| g.0.len()));
        let order_stage = Stage::new("assignment_order");
        let nearest = |v: &[f32], cached: Option<&[u8; NEAR_CELLS]>| {
            let (found, packed) = near_groups(v, cached);
            let group = found.into_iter().min_by(by_distance).map_or(0, |(_, key)| {
                (offsets[key >> 32] + (key & 0xFFFF_FFFF)) as u32
            });
            (group, packed)
        };
        let nearest_group: Vec<u32> = if cache_neighbors {
            let nearest_and_cells: Vec<_> = present
                .par_iter()
                .enumerate()
                .map_init(Vec::new, |buf, (i, g)| {
                    if stopping() {
                        return None;
                    }
                    let v = read_row(*g, buf);
                    Some(nearest(&v, near_cells_cache.get().map(|cells| &cells[i])))
                })
                .collect::<Option<_>>()?;
            let (nearest_group, cells): (Vec<_>, Vec<_>) = nearest_and_cells.into_iter().unzip();
            if near_cells_cache.get().is_none() {
                let _ = near_cells_cache.set(cells);
            }
            nearest_group
        } else {
            present
                .par_iter()
                .map_init(Vec::new, |buf, g| {
                    if stopping() {
                        return None;
                    }
                    Some(nearest(&read_row(*g, buf), None).0)
                })
                .collect::<Option<_>>()?
        };
        // A counting sort of row positions by nearest group.
        let mut starts = vec![0usize; group_count + 1];
        for g in &nearest_group {
            starts[*g as usize + 1] += 1;
        }
        for i in 1..starts.len() {
            starts[i] += starts[i - 1];
        }
        let mut order = vec![0u32; present.len()];
        for (i, g) in nearest_group.iter().enumerate() {
            order[starts[*g as usize]] = i as u32;
            starts[*g as usize] += 1;
        }
        drop(nearest_group);
        drop(starts);
        drop(order_stage);
        let score_stage = Stage::new("assignment_score");
        // Rows in blocks of neighbors in that order: a block compares with the union of its rows'
        // nearest groups (nearly the same groups for neighbors), four rows per pass over each
        // center, as dot products against precomputed squared norms.
        let leaf_norms: Vec<f32> = leaves.par_iter().map(|l| norm2(l)).collect();
        let group_norms: Vec<Vec<f32>> = groups
            .iter()
            .map(|(centers, _)| centers.iter().map(|c| norm2(c)).collect())
            .collect();
        let compared = std::sync::atomic::AtomicU64::new(0);
        let routing_nanos = std::sync::atomic::AtomicU64::new(0);
        let profile = std::env::var_os("BF_TRACE_STAGES").is_some();
        let route = |buf: &mut Vec<u8>, block: &[u32]| {
            if stopping() {
                return None;
            }
            let route_started = profile.then(std::time::Instant::now);
            let rows: Vec<Vec<f32>> = block
                .iter()
                .map(|i| read_row(present[*i as usize], buf))
                .collect();
            let refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
            let norms: Vec<f32> = rows.iter().map(|r| norm2(r)).collect();
            // The block's near cells, and each row's nearest groups among all of their groups.
            let mut cells: Vec<usize> = Vec::new();
            for (r, v) in refs.iter().enumerate() {
                if let Some(cached) = near_cells_cache.get() {
                    cells.extend(
                        cached[block[r] as usize][..near]
                            .iter()
                            .map(|c| usize::from(*c)),
                    );
                    continue;
                }
                let mut by_cell: Vec<(f32, usize)> = coarse
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (distance(v, c), i))
                    .collect();
                if by_cell.len() > near {
                    by_cell.select_nth_unstable_by(near - 1, by_distance);
                    by_cell.truncate(near);
                }
                cells.extend(by_cell.into_iter().map(|(_, c)| c));
            }
            cells.sort_unstable();
            cells.dedup();
            let keys: Vec<usize> = cells
                .iter()
                .flat_map(|&cell| (0..groups[cell].0.len()).map(move |j| cell << 32 | j))
                .collect();
            let group_d = block_distances(&refs, &norms, keys.len(), |n| {
                let key = keys[n];
                (
                    groups[key >> 32].0[key & 0xFFFF_FFFF].as_slice(),
                    group_norms[key >> 32][key & 0xFFFF_FFFF],
                )
            });
            let mut wanted: Vec<usize> = Vec::new();
            for d in &group_d {
                let mut found: Vec<(f32, usize)> =
                    d.iter().enumerate().map(|(n, d)| (*d, keys[n])).collect();
                if found.len() > probes {
                    found.select_nth_unstable_by(probes - 1, by_distance);
                    found.truncate(probes);
                }
                wanted.extend(found.into_iter().map(|(_, key)| key));
            }
            wanted.sort_unstable();
            wanted.dedup();
            compared.fetch_add(
                ((keys.len()
                    + wanted
                        .iter()
                        .map(|key| groups[key >> 32].1[key & 0xFFFF_FFFF].len())
                        .sum::<usize>())
                    * refs.len()) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            if let Some(start) = route_started {
                routing_nanos.fetch_add(
                    start.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            Some(assignment::Block {
                rows,
                norms,
                wanted,
                #[cfg(test)]
                positions: block.to_vec(),
            })
        };
        #[cfg(test)]
        let reduced = if experiment {
            std::env::var_os("RR4_BF16").is_some()
        } else {
            native
        };
        #[cfg(not(test))]
        let reduced = native;
        let scorer =
            batched.then(|| assignment::Scorer::with_bf16(leaves, &leaf_norms, &groups, reduced));
        let ordered: Vec<u32> = order
            .par_chunks(if batched { batch_rows } else { ASSIGN_BLOCK })
            .map_init(Vec::new, |buf, batch| {
                let blocks: Vec<_> = batch
                    .chunks(ASSIGN_BLOCK)
                    .map(|block| route(buf, block))
                    .collect::<Option<_>>()?;
                if batched {
                    return Some(scorer.as_ref().unwrap().nearest(&blocks));
                }
                let mut out = Vec::new();
                for block in blocks {
                    let refs: Vec<_> = block.rows.iter().map(Vec::as_slice).collect();
                    let candidates: Vec<u32> = block
                        .wanted
                        .iter()
                        .flat_map(|key| groups[key >> 32].1[key & 0xFFFF_FFFF].iter().copied())
                        .collect();
                    let ds = block_distances(&refs, &block.norms, candidates.len(), |n| {
                        let l = candidates[n] as usize;
                        (leaves[l].as_slice(), leaf_norms[l])
                    });
                    #[cfg(test)]
                    if let Some(drift) = drift {
                        drift.observe(
                            &block.positions,
                            &block.wanted,
                            &block.norms,
                            &candidates,
                            &ds,
                        );
                    }
                    for row in ds {
                        let mut best = (f32::INFINITY, u32::MAX);
                        for (d, &l) in row.iter().zip(&candidates) {
                            if *d < best.0 || (*d == best.0 && l < best.1) {
                                best = (*d, l);
                            }
                        }
                        out.push(best.1);
                    }
                }
                Some(out)
            })
            .while_some()
            .flatten_iter()
            .collect();
        drop(score_stage);
        if profile {
            Stage::record(
                "assignment_routing_service",
                std::time::Duration::from_nanos(routing_nanos.into_inner()),
            );
        }
        #[cfg(test)]
        if let Some(scorer) = &scorer {
            eprintln!("RR4 matrix_and_verification_evals={:?}", scorer.counts());
        }
        #[cfg(test)]
        if std::env::var_os("RR4_INPUT").is_some() {
            let main =
                compared.load(std::sync::atomic::Ordering::Relaxed) as f64 / present.len() as f64;
            let ordering = order_compared.load(std::sync::atomic::Ordering::Relaxed) as f64
                / present.len() as f64;
            let total = main
                + ordering
                + if cache_neighbors {
                    0.
                } else {
                    coarse.len() as f64
                };
            let (matrix, verification) = scorer.as_ref().map_or((0, 0), |s| s.counts());
            let verification = verification as f64 / present.len() as f64;
            let saved = if reduced {
                matrix as f64 / present.len() as f64 * 2.
            } else {
                0.
            };
            let bytes = ((total + verification) * 4. - saved) * dims as f64;
            eprintln!(
                "RR4 score_evals_per_row={main:.3} order_evals_per_row={ordering:.3} total_evals_per_row={:.3} logical_center_bytes_per_row={bytes:.0}",
                total + verification
            );
        }
        // `while_some` ends at the first block skipped for shutdown.
        if stopping() {
            return None;
        }
        tracing::info!(
            "recluster: {:.0} centers compared per row",
            compared.into_inner() as f64 / present.len().max(1) as f64
        );
        let mut leaf_of = vec![0u32; present.len()];
        for (i, l) in order.iter().zip(ordered) {
            leaf_of[*i as usize] = l;
        }
        #[cfg(test)]
        experiment::record_assignments(&leaf_of);
        Some(leaf_of)
    };
    let mut leaf_of = assign(
        &leaves,
        #[cfg(test)]
        drift.as_ref(),
    )?;
    tracing::info!(
        "recluster: rows assigned at {:.0}s",
        t0.elapsed().as_secs_f64()
    );
    // One refinement: recompute every leaf center from its rows, and assign again. (Stopping at
    // the recomputed centers, without the second assignment, cost 0.003 of recall@10 on NQ.)
    {
        let refinement = Stage::new("refine_centers");
        let mut members: Vec<Vec<u32>> = vec![Vec::new(); leaves.len()];
        for (g, l) in present.iter().zip(&leaf_of) {
            members[*l as usize].push(*g);
        }
        let dims = leaves.first().map_or(0, Vec::len);
        let centers: Vec<Vec<f32>> = members
            .par_iter()
            .zip(leaves.par_iter())
            .map_init(Vec::new, |buf, (rows, old)| {
                if rows.is_empty() {
                    return old.clone();
                }
                let mut sum = vec![0f64; dims];
                for g in rows {
                    for (s, x) in sum.iter_mut().zip(read_row(*g, buf)) {
                        *s += f64::from(x);
                    }
                }
                sum.iter()
                    .map(|s| (*s / rows.len() as f64) as f32)
                    .collect()
            })
            .collect();
        #[cfg(test)]
        if let Some(drift) = &mut drift {
            drift.advance(&centers);
        }
        leaves = centers;
        drop(refinement);
        leaf_of = assign(
            &leaves,
            #[cfg(test)]
            drift.as_ref(),
        )?;
        tracing::info!("recluster: refined at {:.0}s", t0.elapsed().as_secs_f64());
    }
    // Each cell's leaves, with the rows that chose them, in ascending row order.
    let cell_of: Vec<usize> = ranges
        .iter()
        .enumerate()
        .flat_map(|(cell, r)| std::iter::repeat_n(cell, r.len()))
        .collect();
    let mut by_cell: Vec<Vec<(u32, u32)>> = vec![Vec::new(); ranges.len()];
    for (g, l) in present.iter().zip(&leaf_of) {
        by_cell[cell_of[*l as usize]].push((*g, *l));
    }
    drop(leaf_of);
    let mut jobs = Vec::new();
    for (cell, rows) in by_cell.into_iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let range = ranges[cell].clone();
        // A cell over `max` rows is split by leaves into pieces of about equal rows.
        let mut counts = vec![0usize; range.len()];
        for (_, l) in &rows {
            counts[*l as usize - range.start] += 1;
        }
        let pieces = rows.len().div_ceil(max);
        let target = rows.len().div_ceil(pieces);
        let mut piece_of = vec![0usize; range.len()];
        let (mut piece, mut filled) = (0usize, 0usize);
        for (i, n) in counts.iter().enumerate() {
            if filled > 0 && filled + n > target && piece + 1 < pieces {
                piece += 1;
                filled = 0;
            }
            piece_of[i] = piece;
            filled += n;
        }
        for p in 0..=piece {
            let local: Vec<usize> = (0..range.len()).filter(|i| piece_of[*i] == p).collect();
            let mut index_of = vec![usize::MAX; range.len()];
            for (j, i) in local.iter().enumerate() {
                index_of[*i] = j;
            }
            let (ids, assignment): (Vec<u32>, Vec<usize>) = rows
                .iter()
                .filter(|(_, l)| piece_of[*l as usize - range.start] == p)
                .map(|(g, l)| (*g, index_of[*l as usize - range.start]))
                .unzip();
            if ids.is_empty() {
                continue;
            }
            let centers = local
                .iter()
                .map(|i| leaves[range.start + i].clone())
                .collect();
            jobs.push(SlabJob {
                rows: ids,
                centers,
                assignment,
            });
        }
    }
    Some(jobs)
}

/// Rows the latest copy pass wrote to the spill (tests only).
#[cfg(test)]
static LAST_SPILL_ROWS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
mod dead_row_tests {
    use super::*;
    use bf_storage::{Storage, StorageConfig};
    use bf_types::WriteRequest;
    use serde_json::json;

    fn vector(id: usize) -> Vec<f32> {
        (0..64)
            .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
            .collect()
    }

    /// Recluster 6,000 rows with every third deleted. Returns the spill rows and a digest of
    /// every object the recluster wrote, so a run can be compared with another build's.
    async fn run(deleted: &[usize]) -> (u64, String) {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let e = Engine::with_config(
            store.clone(),
            crate::EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                ..Default::default()
            },
        );
        for batch in 0..2 {
            let docs: Vec<_> = (batch * 3000..(batch + 1) * 3000)
                .map(|id| json!({"id": id, "vector": vector(id)}))
                .collect();
            let body = json!({"distance_metric": "euclidean_squared",
                "schema": {"vector": {"type": "[64]f16", "ann": true}}, "upsert_rows": docs});
            e.write("t", serde_json::from_value::<WriteRequest>(body).unwrap())
                .await
                .unwrap();
            e.index_namespace("t", true).await.unwrap();
        }
        let body = json!({ "deletes": deleted });
        e.write("t", serde_json::from_value::<WriteRequest>(body).unwrap())
            .await
            .unwrap();
        e.index_namespace("t", true).await.unwrap();
        let scratch = tempfile::tempdir().unwrap();
        e.recluster(
            "t",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 60,
                max_slab_rows: 3000,
                scratch: scratch.path().to_owned(),
            },
        )
        .await
        .unwrap();
        let mut keys: Vec<String> = store
            .list("")
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        keys.sort();
        let mut digest = std::collections::hash_map::DefaultHasher::new();
        for key in keys.iter().filter(|k| k.contains("slab")) {
            std::hash::Hash::hash(&store.get(key).await.unwrap().to_vec(), &mut digest);
        }
        let rows = LAST_SPILL_ROWS.load(std::sync::atomic::Ordering::Relaxed);
        (rows, format!("{:x}", std::hash::Hasher::finish(&digest)))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spill_holds_only_live_rows_and_output_is_stable() {
        let deleted: Vec<usize> = (0..6000).filter(|i| i % 10 < 3).collect();
        let (rows, digest) = run(&deleted).await;
        eprintln!("RR8_DIGEST {digest}");
        assert_eq!(
            rows,
            6000 - deleted.len() as u64,
            "spill holds no dead rows"
        );
        assert_eq!(digest, run(&deleted).await.1);
    }
}

#[cfg(test)]
mod leaf_policy_tests {
    use super::target_leaf_rows;

    #[test]
    fn coarse_leaves_start_at_the_measured_large_tier_and_then_stop_growing() {
        assert_eq!(target_leaf_rows(0), 200);
        assert_eq!(target_leaf_rows(100_000), 200);
        assert_eq!(target_leaf_rows(1_000_000), 200);
        assert_eq!(target_leaf_rows(2_681_468), 200);
        assert_eq!(target_leaf_rows(9_999_999), 200);
        assert_eq!(target_leaf_rows(10_000_000), 640);
        assert_eq!(target_leaf_rows(100_000_000), 640);
        assert_eq!(target_leaf_rows(u64::MAX), 640);
        let mut previous = 200;
        for rows in (0..=100_000_000).step_by(100_000) {
            let leaf = target_leaf_rows(rows);
            assert!(leaf >= previous && leaf <= 640);
            assert!(leaf == 200 || leaf.is_multiple_of(32));
            previous = leaf;
        }
    }
}
