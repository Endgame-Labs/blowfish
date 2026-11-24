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
//! The job streams: segment vectors are copied once into a local spill file (never more than a
//! slab's rows are in memory at a time), the coarse partition is trained on a sample, and the
//! slabs are built and uploaded one after another.
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

/// What to build.
#[derive(Clone, Debug)]
pub struct ReclusterOptions {
    /// The vector attribute.
    pub attribute: String,
    /// Coarse partitions (slabs). Cells larger than `max_slab_rows` are split into several slabs.
    pub cells: usize,
    /// Target rows per leaf cluster inside a slab.
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
    pub slabs: usize,
    pub clusters: usize,
    pub directory_bytes: u64,
    pub seconds: f64,
}

/// The leaf clusters and coarse cells every build uses.
pub(crate) const CELLS: usize = 32;
pub(crate) const CLUSTER_ROWS: usize = 200;

/// A finished build: the tier, ready for the indexer to install.
pub(crate) struct Built {
    pub set: SlabSet,
    pub report: ReclusterReport,
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
                self.build_tier_inner(ns, manifest, target, opts, job),
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
        let ann_key = format!("annv2:{attr}");
        let source = crate::vector_locator::Source::from(manifest);
        if segments.is_empty()
            || segments
                .iter()
                .any(|s| !s.files.contains_key(&ann_key) && !source.covers(s, attr))
        {
            return Err(ApiError::new(
                400,
                "every segment needs a current ANN source",
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
        let first_segment = segments
            .iter()
            .find(|s| s.files.contains_key(&ann_key))
            .or_else(|| {
                manifest
                    .ann_tiers
                    .get(attr)
                    .into_iter()
                    .flatten()
                    .flat_map(|t| &t.slabs)
                    .find(|s| s.files.contains_key(&ann_key))
            })
            .ok_or_else(segment::corrupt)?;
        let first = segment_index(store, first_segment, &ann_key).await?;
        let (dims, width) = (first.dims(), first.vector_row_bytes());
        let (dtype, metric) = (
            first.vector_type(),
            segment::vector_metric(manifest.distance_metric),
        );

        // Pass 1: copy every live row's vector into a spill file indexed by global row.
        std::fs::create_dir_all(&opts.scratch).map_err(segment::index_error)?;
        let spill_path = opts
            .scratch
            .join(format!("spill-{}.bin", uuid::Uuid::new_v4()));
        let spill = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&spill_path)
            .map_err(segment::index_error)?;
        spill
            .set_len(total * width as u64)
            .map_err(segment::index_error)?;
        let spill = Arc::new(spill);
        let result = self
            .recluster_with_spill(
                target,
                manifest,
                &wal,
                opts,
                covered,
                spill.clone(),
                (dims, width, dtype, metric),
                first,
                t0,
                job,
            )
            .await;
        drop(spill);
        let _ = std::fs::remove_file(&spill_path);
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn recluster_with_spill(
        &self,
        target: &Target,
        manifest: &bf_wal::Manifest,
        wal: &Wal,
        opts: &ReclusterOptions,
        covered: Vec<SlabCover>,
        spill: Arc<std::fs::File>,
        (dims, width, dtype, metric): (usize, usize, bf_vector::VectorType, bf_vector::Metric),
        first: Arc<bf_vector::Index<segment::ObjectReader>>,
        t0: std::time::Instant,
        job: &Job,
    ) -> Result<Built, ApiError> {
        let store = &self.0.store;
        let (attr, ns) = (opts.attribute.as_str(), wal.namespace().to_string());
        let ann_key = format!("annv2:{attr}");
        let segments = target.segments.clone();
        let source = crate::vector_locator::Source::from(manifest);
        let bases: Vec<u32> = covered.iter().map(|c| c.base).collect();
        let copied: Vec<Vec<u32>> = futures::stream::iter(segments.into_iter().zip(bases).map(
            |(seg, base): (Segment, u32)| {
                let spill = spill.clone();
                let ann_key = ann_key.clone();
                async move {
                    job.pause().await;
                    if source.covers(&seg, attr) {
                        let live = segment::live(store, &seg).await?;
                        let mut it = live.iter();
                        let mut present = Vec::with_capacity(live.len() as usize);
                        loop {
                            let selected =
                                it.by_ref().take(4096).collect::<bf_vector::RoaringBitmap>();
                            if selected.is_empty() {
                                break;
                            }
                            let (rows, bytes) =
                                source.read(store, &seg, &selected, attr, true).await?;
                            for (row, raw) in rows.into_iter().zip(bytes.chunks_exact(width)) {
                                spill
                                    .write_all_at(
                                        raw,
                                        (u64::from(base) + u64::from(row)) * width as u64,
                                    )
                                    .map_err(segment::index_error)?;
                                present.push(base + row);
                            }
                        }
                        return Ok::<_, ApiError>(present);
                    }
                    let index = segment_index(store, &seg, &ann_key).await?;
                    let live = if seg.live_generation == 0 {
                        None
                    } else {
                        Some(segment::live(store, &seg).await?)
                    };
                    let mut present = Vec::new();
                    let clusters = index.cluster_count();
                    let mut reads = futures::stream::iter((0..clusters).map(|c| {
                        let index = index.clone();
                        async move { index.read_cluster_rows(c).await }
                    }))
                    .buffer_unordered(32);
                    while let Some((rows, bytes)) =
                        reads.try_next().await.map_err(segment::index_error)?
                    {
                        for (i, row) in rows.iter().enumerate() {
                            if live.as_ref().is_some_and(|l| !l.contains(*row)) {
                                continue;
                            }
                            let at = (u64::from(base) + u64::from(*row)) * width as u64;
                            spill
                                .write_all_at(&bytes[i * width..(i + 1) * width], at)
                                .map_err(segment::index_error)?;
                            present.push(base + *row);
                        }
                    }
                    Ok::<_, ApiError>(present)
                }
            },
        ))
        .buffer_unordered(6)
        .try_collect()
        .await?;
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
        let read_row = {
            let spill = spill.clone();
            let first = first.clone();
            move |g: u32, buf: &mut Vec<u8>| -> Vec<f32> {
                buf.resize(width, 0);
                spill
                    .read_exact_at(buf, u64::from(g) * width as u64)
                    .expect("spill file read");
                bf_vector::prepare_vector(&first.decode_stored(buf), metric)
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
                    bf_vector::kmeans(&refs, cells_n, 8, 7)
                });
                let assignment: Vec<u16> = present
                    .par_iter()
                    .map_init(Vec::new, |buf, g| {
                        (!stop.stopping())
                            .then(|| bf_vector::nearest_center(&read_row(*g, buf), &centers) as u16)
                    })
                    .collect::<Option<_>>()?;
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
        let jobs: Vec<SlabJob> = {
            let (present, read_row) = (present.clone(), read_row.clone());
            let cluster_rows = opts.cluster_rows.max(1);
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
        let uploads = Arc::new(tokio::sync::Mutex::new(Vec::<Segment>::new()));
        for (
            n,
            SlabJob {
                rows,
                centers,
                assignment,
            },
        ) in jobs.into_iter().enumerate()
        {
            let k = centers.len();
            clusters += k;
            let spill = spill.clone();
            let rows_n = rows.len() as u32;
            let global_rows = rows.clone();
            // Between slabs, let a queued flush or compaction run first.
            job.pause().await;
            let blob = on_pool(move || -> Result<(Vec<u8>, u64), ApiError> {
                // Rows stay packed at the stored precision (about 2 KB each at 1,024 dims in f16).
                let mut data = vec![0u8; rows.len() * width];
                for (g, chunk) in rows.iter().zip(data.chunks_exact_mut(width)) {
                    spill
                        .read_exact_at(chunk, u64::from(*g) * width as u64)
                        .map_err(segment::index_error)?;
                }
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
                bf_vector::Fragment::build_ann(&blob).map_err(segment::index_error)
            })
            .await??;
            let (blob, meta) = blob;
            // Under the job's prefix, which its build record protects from GC until published.
            let id = format!("{}{n:05}", job.slab_prefix());
            let (store2, ns2, attr2, id2, uploads2) = (
                store.clone(),
                ns.clone(),
                attr.to_string(),
                id.clone(),
                uploads.clone(),
            );
            // Upload while the next slab builds; at most one upload is outstanding.
            if let Some(p) = pending.take() {
                p.await.map_err(segment::index_error)??;
            }
            pending = Some(tokio::spawn(async move {
                let prefix = format!("ns/{ns2}/slabs/{id2}");
                let ann = segment::put_file(
                    &store2,
                    format!("{prefix}/ann.{}.bfa", uuid::Uuid::new_v4()),
                    blob,
                    meta,
                )
                .await?;
                let mut files = std::collections::BTreeMap::new();
                let row_file = crate::vector_locator::write_rows(
                    &store2,
                    format!("{prefix}/vector-rows.bfr"),
                    &global_rows,
                )
                .await?;
                files.insert(format!("annv2:{attr2}"), ann);
                files.insert("vector_rows".into(), row_file);
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

        // The directory over the slabs. The indexer publishes the tier. The cold-query hint is
        // the base's (a delta's directory is small, and read beside it).
        let (dir, pages) =
            slab_directories(store, &ns, attr, &slabs, target.kind == Kind::Base).await?;
        let report = ReclusterReport {
            rows: present.len() as u64,
            slabs: slabs.len(),
            clusters,
            directory_bytes: dir.size,
            seconds: 0.0,
        };
        Ok(Built {
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

/// Open a segment's index directly (not through the decoded cache: a recluster reads each once).
async fn segment_index(
    store: &bf_storage::Storage,
    seg: &Segment,
    key: &str,
) -> Result<Arc<bf_vector::Index<segment::ObjectReader>>, ApiError> {
    let file = &seg.files[key];
    bf_vector::Index::open_with_metadata_len(
        segment::ObjectReader::new(store, file).bypass_cache(),
        file.metadata_len,
    )
    .await
    .map(Arc::new)
    .map_err(segment::index_error)
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
/// by `center(n)` as (center, squared norm): `out[row][n]`. Four rows share each pass over a
/// center.
#[allow(clippy::needless_range_loop)] // `n` indexes both the centers and each row's output.
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
    let distance = bf_vector::squared_distance;
    let t0 = std::time::Instant::now();
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
                let trained = bf_vector::kmeans(&refs, k, 8, 42 + cell as u64);
                let trained_refs: Vec<&[f32]> = trained.iter().map(Vec::as_slice).collect();
                let g = (trained.len() as f64).sqrt().ceil() as usize;
                let centers = bf_vector::kmeans(&trained_refs, g, 8, 7 + cell as u64);
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
    tracing::info!(
        "recluster: {} leaves trained in {:.0}s",
        leaves.len(),
        t0.elapsed().as_secs_f64()
    );
    let near = NEAR_CELLS.min(coarse.len()).max(1);
    let probes = GROUP_PROBES;
    let by_distance = |a: &(f32, usize), b: &(f32, usize)| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1));
    let assign = |leaves: &[Vec<f32>]| -> Option<Vec<u32>> {
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
        // A row's nearest groups among its near cells, nearest first.
        let near_groups = |v: &[f32]| -> Vec<(f32, usize)> {
            let mut cells: Vec<(f32, usize)> = coarse
                .iter()
                .enumerate()
                .map(|(i, c)| (distance(v, c), i))
                .collect();
            if cells.len() > near {
                cells.select_nth_unstable_by(near - 1, by_distance);
                cells.truncate(near);
            }
            let mut found: Vec<(f32, usize)> = Vec::new();
            for (_, cell) in cells {
                for (j, center) in groups[cell].0.iter().enumerate() {
                    found.push((distance(v, center), cell << 32 | j));
                }
            }
            if found.len() > probes {
                found.select_nth_unstable_by(probes - 1, by_distance);
                found.truncate(probes);
            }
            found
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
        let nearest_group: Vec<u32> = present
            .par_iter()
            .map_init(Vec::new, |buf, g| {
                if stopping() {
                    return None;
                }
                let v = read_row(*g, buf);
                Some(
                    near_groups(&v)
                        .into_iter()
                        .min_by(by_distance)
                        .map_or(0, |(_, key)| {
                            (offsets[key >> 32] + (key & 0xFFFF_FFFF)) as u32
                        }),
                )
            })
            .collect::<Option<_>>()?;
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
        // Rows in blocks of neighbors in that order: a block compares with the union of its rows'
        // nearest groups (nearly the same groups for neighbors), four rows per pass over each
        // center, as dot products against precomputed squared norms.
        let leaf_norms: Vec<f32> = leaves.par_iter().map(|l| norm2(l)).collect();
        let group_norms: Vec<Vec<f32>> = groups
            .iter()
            .map(|(centers, _)| centers.iter().map(|c| norm2(c)).collect())
            .collect();
        let compared = std::sync::atomic::AtomicU64::new(0);
        let ordered: Vec<u32> = order
            .par_chunks(ASSIGN_BLOCK)
            .map_init(Vec::new, |buf, block| {
                if stopping() {
                    return None;
                }
                let rows: Vec<Vec<f32>> = block
                    .iter()
                    .map(|i| read_row(present[*i as usize], buf))
                    .collect();
                let refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
                let norms: Vec<f32> = rows.iter().map(|r| norm2(r)).collect();
                // The block's near cells, and each row's nearest groups among all of their groups.
                let mut cells: Vec<usize> = Vec::new();
                for v in &refs {
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
                let candidates: Vec<u32> = wanted
                    .iter()
                    .flat_map(|key| groups[key >> 32].1[key & 0xFFFF_FFFF].iter().copied())
                    .collect();
                compared.fetch_add(
                    ((keys.len() + candidates.len()) * refs.len()) as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                let leaf_d = block_distances(&refs, &norms, candidates.len(), |n| {
                    let l = candidates[n] as usize;
                    (leaves[l].as_slice(), leaf_norms[l])
                });
                Some(
                    leaf_d
                        .iter()
                        .map(|d| {
                            let mut best = (f32::INFINITY, u32::MAX);
                            for (d, &l) in d.iter().zip(&candidates) {
                                if *d < best.0 || (*d == best.0 && l < best.1) {
                                    best = (*d, l);
                                }
                            }
                            best.1
                        })
                        .collect::<Vec<u32>>(),
                )
            })
            .while_some()
            .flatten_iter()
            .collect();
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
        Some(leaf_of)
    };
    let mut leaf_of = assign(&leaves)?;
    tracing::info!(
        "recluster: rows assigned at {:.0}s",
        t0.elapsed().as_secs_f64()
    );
    // One refinement: recompute every leaf center from its rows, and assign again. (Stopping at
    // the recomputed centers, without the second assignment, cost 0.003 of recall@10 on NQ.)
    {
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
        leaves = centers;
        leaf_of = assign(&leaves)?;
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
