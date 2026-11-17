//! Recluster compaction: a namespace-wide ANN layout beside the existing segments.
//!
//! Each segment clusters its own rows, so one query neighborhood is spread over every segment and
//! a cold query must probe clusters in all of them (about 580K rows scanned at 10M for the
//! turbopuffer-matching recall). A recluster repartitions every row by position in vector space
//! into a few dozen **slabs** (a coarse k-means over a sample), and clusters each slab finely
//! with the ordinary index builder. A slab is a pseudo segment (an index object with the
//! quantized codes and full-precision vectors, plus a block fragment), listed in
//! `Manifest::ann_slabs` with a block-layout directory over the slabs. A slab's row IDs are
//! global row numbers (`SlabCover::base` plus the segment-local row), so a winner maps back to its
//! segment and row without a lookup table, and documents and attributes stay where they are.
//!
//! The job streams: segment vectors are copied once into a local spill file (never more than a
//! slab's rows are in memory at a time), the coarse partition is trained on a sample, and the
//! slabs are built and uploaded one after another.
use crate::{Engine, ann_dir, log_error, segment};
use bf_types::ApiError;
use bf_wal::{Manifest, Segment, SlabCover, SlabSet, Wal};
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
    /// Most rows in one slab (bounds the builder's memory: about 9 KB per row at 1,024 dims).
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

impl Engine {
    /// Build a namespace-wide ANN layout for `opts.attribute` over every live segment and publish
    /// it. Additive: segments, documents, and the per-segment directories are untouched, and a
    /// previous layout is retired. Stops an indexer on another node from publishing meanwhile
    /// (it claims a new epoch), so run it with the namespace's indexing otherwise idle.
    pub async fn recluster(
        &self,
        ns: &str,
        opts: &ReclusterOptions,
    ) -> Result<ReclusterReport, ApiError> {
        let context = crate::read_cache::ReadContext {
            cache: self.0.read_cache.clone(),
            stats: Default::default(),
            backend: Default::default(),
        };
        crate::read_cache::scope(context, self.recluster_inner(ns, opts)).await
    }

    async fn recluster_inner(
        &self,
        ns: &str,
        opts: &ReclusterOptions,
    ) -> Result<ReclusterReport, ApiError> {
        let t0 = std::time::Instant::now();
        let store = &self.0.store;
        let attr = opts.attribute.as_str();
        let wal = Wal::new(store.clone(), ns).map_err(log_error)?;
        let (old_sequence, mut manifest) = wal
            .latest_manifest()
            .await
            .map_err(log_error)?
            .ok_or_else(|| ApiError::new(404, "namespace has no checkpoint"))?;
        let (ann_key, frag_key) = (format!("ann:{attr}"), format!("annfrag2:{attr}"));
        if manifest.segments.is_empty()
            || manifest
                .segments
                .iter()
                .any(|s| !s.files.contains_key(&ann_key) || !s.files.contains_key(&frag_key))
        {
            return Err(ApiError::new(
                400,
                "every segment needs an ANN index and a block fragment",
            ));
        }
        // Claim a new epoch: it fences any other indexer's publication until ours.
        let epoch = manifest.indexer_epoch;
        manifest.indexer_epoch += 1;
        let claim = wal
            .publish_fenced(old_sequence, epoch, &manifest)
            .await
            .map_err(log_error)?;
        let mut manifest = wal
            .latest_manifest()
            .await
            .map_err(log_error)?
            .ok_or_else(|| ApiError::new(503, "manifest disappeared"))?
            .1;
        if manifest.indexer_epoch != epoch + 1 {
            return Err(ApiError::new(409, "another indexer claimed the namespace"));
        }

        // Global row numbers: segment bases in manifest order.
        let mut covered = Vec::new();
        let mut total: u64 = 0;
        for seg in &manifest.segments {
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
        let first = segment_index(store, &manifest.segments[0], &ann_key).await?;
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
                &mut manifest,
                &wal,
                claim,
                opts,
                covered,
                spill.clone(),
                (dims, width, dtype, metric),
                first,
                t0,
            )
            .await;
        drop(spill);
        let _ = std::fs::remove_file(&spill_path);
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn recluster_with_spill(
        &self,
        manifest: &mut Manifest,
        wal: &Wal,
        claim: u64,
        opts: &ReclusterOptions,
        covered: Vec<SlabCover>,
        spill: Arc<std::fs::File>,
        (dims, width, dtype, metric): (usize, usize, bf_vector::VectorType, bf_vector::Metric),
        first: Arc<bf_vector::Index<segment::ObjectReader>>,
        t0: std::time::Instant,
    ) -> Result<ReclusterReport, ApiError> {
        let store = &self.0.store;
        let (attr, ns) = (opts.attribute.as_str(), wal.namespace().to_string());
        let ann_key = format!("ann:{attr}");
        let segments = manifest.segments.clone();
        let copied: Vec<Vec<u32>> =
            futures::stream::iter(segments.iter().zip(&covered).map(|(seg, cover)| {
                let spill = spill.clone();
                let ann_key = ann_key.clone();
                let base = cover.base;
                async move {
                    let index = segment_index(store, seg, &ann_key).await?;
                    let live = if seg.live_generation == 0 {
                        None
                    } else {
                        Some(segment::live(store, seg).await?)
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
            }))
            .buffer_unordered(6)
            .try_collect()
            .await?;
        let mut present: Vec<u32> = copied.into_iter().flatten().collect();
        present.sort_unstable();
        if present.is_empty() {
            return Err(ApiError::new(400, "no live rows to recluster"));
        }
        tracing::info!(rows = present.len(), "recluster: vectors copied");
        eprintln!(
            "recluster: copied {} rows in {:.0}s",
            present.len(),
            t0.elapsed().as_secs_f64()
        );

        // Pass 2: coarse partition. Train on a sample, then assign every row.
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
        let cells = opts.cells.max(1).min(present.len());
        let (assignment, cell_rows) = {
            let (present, read_row) = (present.clone(), read_row.clone());
            let cells_n = cells;
            tokio::task::spawn_blocking(move || {
                let mut state = 0x9E37_79B9_7F4A_7C15u64;
                let mut next = move || {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state
                };
                let sample_n = 100_000.min(present.len());
                let picks: Vec<u32> = (0..sample_n)
                    .map(|_| present[next() as usize % present.len()])
                    .collect();
                let sample: Vec<Vec<f32>> = picks
                    .par_iter()
                    .map_init(Vec::new, |buf, g| read_row(*g, buf))
                    .collect();
                let refs: Vec<&[f32]> = sample.iter().map(|v| v.as_slice()).collect();
                let centers = bf_vector::kmeans(&refs, cells_n, 8, 7);
                drop(sample);
                let assignment: Vec<u16> = present
                    .par_iter()
                    .map_init(Vec::new, |buf, g| {
                        bf_vector::nearest_center(&read_row(*g, buf), &centers) as u16
                    })
                    .collect();
                let mut cell_rows: Vec<Vec<u32>> = vec![Vec::new(); centers.len()];
                for (g, c) in present.iter().zip(&assignment) {
                    cell_rows[*c as usize].push(*g);
                }
                (assignment.len(), cell_rows)
            })
            .await
            .map_err(segment::index_error)?
        };
        eprintln!(
            "recluster: {assignment} rows in {} cells (largest {}) at {:.0}s",
            cell_rows.len(),
            cell_rows.iter().map(Vec::len).max().unwrap_or(0),
            t0.elapsed().as_secs_f64()
        );

        // Pass 3: one slab per cell (a huge cell is split), built and uploaded in turn.
        let max = opts.max_slab_rows.max(1);
        let mut jobs: Vec<Vec<u32>> = Vec::new();
        for rows in cell_rows {
            if rows.is_empty() {
                continue;
            }
            let pieces = rows.len().div_ceil(max);
            let size = rows.len().div_ceil(pieces);
            jobs.extend(rows.chunks(size).map(<[u32]>::to_vec));
        }
        let mut slabs: Vec<Segment> = Vec::new();
        let mut clusters = 0usize;
        let mut pending: Option<tokio::task::JoinHandle<Result<(), ApiError>>> = None;
        let uploads = Arc::new(tokio::sync::Mutex::new(Vec::<Segment>::new()));
        for (n, rows) in jobs.into_iter().enumerate() {
            let k = (rows.len() / opts.cluster_rows.max(1)).max(1);
            clusters += k;
            let spill = spill.clone();
            let first2 = first.clone();
            let rows_n = rows.len() as u32;
            let blob = tokio::task::spawn_blocking(
                move || -> Result<(Vec<u8>, Vec<u8>, u64), ApiError> {
                    let mut buf = vec![0u8; width];
                    let mut vectors: Vec<(u32, Vec<f32>)> = Vec::with_capacity(rows.len());
                    for g in &rows {
                        spill
                            .read_exact_at(&mut buf, u64::from(*g) * width as u64)
                            .map_err(segment::index_error)?;
                        vectors.push((*g, first2.decode_stored(&buf)));
                    }
                    let params = bf_vector::BuildParams {
                        clusters: Some(k),
                        ..Default::default()
                    };
                    let blob = bf_vector::IndexBuilder::new(dims, metric)
                        .map_err(segment::index_error)?
                        .params(params)
                        .vector_type(dtype)
                        .build(&vectors)
                        .map_err(segment::index_error)?;
                    drop(vectors);
                    let (frag, frag_meta) =
                        bf_vector::Fragment::build_blocks(&blob).map_err(segment::index_error)?;
                    Ok((blob, frag, frag_meta))
                },
            )
            .await
            .map_err(segment::index_error)??;
            let (blob, frag, frag_meta) = blob;
            let meta = bf_vector::metadata_len(&blob).map_err(segment::index_error)?;
            let id = format!("slab-{}", uuid::Uuid::new_v4());
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
                    format!("{prefix}/ann.{}.bfv", uuid::Uuid::new_v4()),
                    blob,
                    meta,
                )
                .await?;
                let frag = segment::put_file(
                    &store2,
                    format!("{prefix}/ann2.{}.bfb", uuid::Uuid::new_v4()),
                    frag,
                    frag_meta,
                )
                .await?;
                let mut files = std::collections::BTreeMap::new();
                files.insert(format!("ann:{attr2}"), ann);
                files.insert(format!("annfrag2:{attr2}"), frag);
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
                    },
                    live_generation: 0,
                });
                Ok(())
            }));
            eprintln!(
                "recluster: slab {} built at {:.0}s",
                n + 1,
                t0.elapsed().as_secs_f64()
            );
        }
        if let Some(p) = pending.take() {
            p.await.map_err(segment::index_error)??;
        }
        slabs.extend(uploads.lock().await.drain(..));
        slabs.sort_by(|a, b| a.id.cmp(&b.id));

        // The directory over the slabs, then publish.
        let dir = ann_dir::build(store, &ns, &slabs, attr, ann_dir::Layout::Slabs, None)
            .await?
            .ok_or_else(|| ApiError::new(500, "slab directory not built"))?;
        let report = ReclusterReport {
            rows: present.len() as u64,
            slabs: slabs.len(),
            clusters,
            directory_bytes: dir.size,
            seconds: 0.0,
        };
        if let Some(old) = manifest.ann_slabs.remove(attr) {
            crate::indexer::retire_slabs(manifest, &old);
        }
        manifest.ann_slabs.insert(
            attr.to_string(),
            SlabSet {
                covered,
                slabs,
                dir,
            },
        );
        wal.publish_fenced(claim, manifest.indexer_epoch, manifest)
            .await
            .map_err(log_error)?;
        Ok(ReclusterReport {
            seconds: t0.elapsed().as_secs_f64(),
            ..report
        })
    }
}

/// Open a segment's index directly (not through the decoded cache: a recluster reads each once).
async fn segment_index(
    store: &bf_storage::Storage,
    seg: &Segment,
    key: &str,
) -> Result<Arc<bf_vector::Index<segment::ObjectReader>>, ApiError> {
    let file = &seg.files[key];
    bf_vector::Index::open_with_metadata_len(
        segment::ObjectReader::new(store, file),
        file.metadata_len,
    )
    .await
    .map(Arc::new)
    .map_err(segment::index_error)
}
