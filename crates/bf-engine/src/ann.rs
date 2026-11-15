//! Cross-segment ANN: rank leaf clusters of every segment together, scan only the globally
//! nearest clusters, and rerank one merged candidate pool. Probing each segment separately
//! scans and reranks once per segment, so cost grew with the segment count instead of the
//! corpus size.
use crate::{EngineConfig, NamespaceState, read_cache, segment, snapshot};
use bf_storage::Storage;
use bf_types::*;
use bf_vector::{Candidate, Index, RoaringBitmap};
use futures::{StreamExt, TryStreamExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
};

/// Above this many matching rows, fetching only the matching vectors stops being cheaper
/// than scanning quantized codes.
const EXACT_ROWS: usize = 2048;
/// Never hold more than this many bytes of exact vectors for one query.
const EXACT_BYTES: usize = 256 << 20;

struct Seg {
    index: Arc<Index<segment::ObjectReader>>,
    allowed: Arc<RoaringBitmap>,
    counts: Vec<u32>,
    distances: Vec<f32>,
}

/// Run a segment-local task on a blocking worker (bounded by the engine's worker permits)
/// so CPU-heavy scans from different segments use different cores.
async fn on_worker<T: Send + 'static>(
    fut: impl Future<Output = Result<T, ApiError>> + Send + 'static,
) -> Result<T, ApiError> {
    let Some(context) = read_cache::context() else {
        return fut.await;
    };
    let permit = context
        .cache
        .workers
        .clone()
        .acquire_owned()
        .await
        .map_err(segment::index_error)?;
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.block_on(read_cache::scope(context, fut))
    })
    .await
    .map_err(segment::index_error)?
}

/// Returns `None` when a segment lacks an ANN index for the attribute (legacy segments),
/// so the caller can use the per-segment path.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn search(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    attribute: &str,
    vector: &[f32],
    config: &EngineConfig,
    k: usize,
    columns: &BTreeSet<String>,
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    let key = format!("ann:{attribute}");
    if state.segments.iter().any(|s| !s.files.contains_key(&key)) {
        return Ok(None);
    }
    let vector: Arc<[f32]> = vector.into();
    let mut base = state.clone();
    base.docs.clear();
    base.scores.clear();
    base.segments.clear();
    let base = Arc::new(base);
    let mut segs: Vec<Option<Seg>> = Vec::new();
    segs.resize_with(state.segments.len(), || None);
    let mut loaded =
        futures::stream::iter(state.segments.iter().cloned().enumerate().map(|(i, seg)| {
            let store = store.clone();
            let base = base.clone();
            let filter = q.filters.clone();
            let vector = vector.clone();
            let file = seg.files.get(&key).cloned().expect("checked above");
            async move {
                let (allowed, index) = tokio::try_join!(
                    snapshot::allowed(&store, &base, &seg, filter.as_ref()),
                    segment::vector_index(&store, &file),
                )?;
                if allowed.is_empty() {
                    return Ok::<_, ApiError>((i, None));
                }
                let (allowed, index) = (Arc::new(allowed), index);
                let task = {
                    let (allowed, index) = (allowed.clone(), index.clone());
                    on_worker(async move {
                        let counts = index.cluster_counts(Some(&allowed));
                        let distances = index
                            .center_distances(&vector)
                            .map_err(segment::index_error)?;
                        Ok((counts, distances))
                    })
                };
                let (counts, distances) = task.await?;
                Ok((
                    i,
                    Some(Seg {
                        index,
                        allowed,
                        counts,
                        distances,
                    }),
                ))
            }
        }))
        .buffer_unordered(config.search_concurrency.max(1) * 2);
    while let Some((i, seg)) = loaded.try_next().await? {
        segs[i] = seg;
    }
    drop(loaded);

    // Flat indexes are tiny and exact. Everything else joins the global probe plan.
    let mut winners: Vec<(f32, usize, u32)> = Vec::new(); // (exact distance, segment, row)
    let mut ann = Vec::new();
    let mut flat = Vec::new();
    for (i, seg) in segs.iter().enumerate() {
        match seg {
            Some(s) if s.index.is_flat() => flat.push(i),
            Some(_) => ann.push(i),
            None => {}
        }
    }
    let matching: usize = ann
        .iter()
        .map(|&i| segs[i].as_ref().unwrap().allowed.len() as usize)
        .sum();
    let rows: usize = ann
        .iter()
        .map(|&i| segs[i].as_ref().unwrap().index.len())
        .sum();
    let clusters: usize = ann
        .iter()
        .map(|&i| segs[i].as_ref().unwrap().index.cluster_count())
        .sum();
    let target = k.saturating_mul(config.ann_rerank_factor).min(matching);
    let mut plan: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut exact = flat.iter().map(|&i| (i, None)).collect::<Vec<_>>();
    if !ann.is_empty() {
        let width = segs[ann[0]].as_ref().unwrap().index.vector_row_bytes();
        let mut order: Vec<(f32, usize, usize)> = Vec::with_capacity(clusters);
        for &i in &ann {
            let s = segs[i].as_ref().unwrap();
            for (c, (&n, &d)) in s.counts.iter().zip(&s.distances).enumerate() {
                if n > 0 {
                    order.push((d, i, c));
                }
            }
        }
        // Equal cluster counts and base probes at one segment reproduce per-index nprobe.
        let scale = (clusters as f64 / (rows as f64).sqrt()).max(1.0);
        let probes = ((config.ann_nprobe as f64 * scale * rows as f64 / matching.max(1) as f64)
            .ceil() as usize)
            .min(clusters);
        order.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
        let (mut chosen, mut candidates) = (0usize, 0usize);
        let mut picked = Vec::new();
        for &(_, i, c) in &order {
            picked.push((i, c));
            chosen += 1;
            candidates += segs[i].as_ref().unwrap().counts[c] as usize;
            if chosen >= probes && candidates >= target {
                break;
            }
        }
        if matching <= k
            || (matching <= EXACT_ROWS.max(chosen + target) && matching * width <= EXACT_BYTES)
        {
            exact.extend(ann.iter().map(|&i| (i, None::<()>)));
        } else {
            for (i, c) in picked {
                plan.entry(i).or_default().push(c);
            }
        }
    }

    // Exact segments: every matching vector is reranked.
    let mut tasks = Vec::new();
    for (i, _) in exact {
        let s = segs[i].as_ref().unwrap();
        let (index, allowed, vector) = (s.index.clone(), s.allowed.clone(), vector.clone());
        tasks.push(on_worker(async move {
            let hits = index
                .exact_search(&vector, k, Some(&allowed))
                .await
                .map_err(segment::index_error)?;
            Ok(hits
                .into_iter()
                .map(|(row, d)| (d, i, row))
                .collect::<Vec<_>>())
        }));
    }
    // ANN segments: scan the globally chosen clusters, then rerank one merged pool.
    let mut scans = Vec::new();
    for (i, cs) in plan {
        let s = segs[i].as_ref().unwrap();
        let (index, allowed, vector) = (s.index.clone(), s.allowed.clone(), vector.clone());
        scans.push(on_worker(async move {
            let found = index
                .scan_clusters(&vector, &cs, Some(&allowed))
                .await
                .map_err(segment::index_error)?;
            Ok(found.into_iter().map(|c| (i, c)).collect::<Vec<_>>())
        }));
    }
    let limit = config.search_concurrency.max(1) * 2;
    let exact_hits: Vec<Vec<(f32, usize, u32)>> = futures::stream::iter(tasks)
        .buffer_unordered(limit)
        .try_collect()
        .await?;
    winners.extend(exact_hits.into_iter().flatten());
    let mut pool: Vec<(usize, Candidate)> = futures::stream::iter(scans)
        .buffer_unordered(limit)
        .try_collect::<Vec<Vec<(usize, Candidate)>>>()
        .await?
        .into_iter()
        .flatten()
        .collect();
    if pool.len() > target {
        pool.select_nth_unstable_by(target, |a, b| {
            a.1.distance
                .total_cmp(&b.1.distance)
                .then(a.1.row.cmp(&b.1.row))
        });
        pool.truncate(target);
    }
    let mut by_segment: BTreeMap<usize, Vec<(u32, u32)>> = BTreeMap::new();
    for (i, c) in pool {
        by_segment
            .entry(i)
            .or_default()
            .push((c.cluster, c.ordinal));
    }
    let mut reranks = Vec::new();
    for (i, locations) in by_segment {
        let index = segs[i].as_ref().unwrap().index.clone();
        let vector = vector.clone();
        reranks.push(on_worker(async move {
            let hits = index
                .rerank_candidates(&vector, k, locations)
                .await
                .map_err(segment::index_error)?;
            Ok(hits
                .into_iter()
                .map(|(row, d)| (d, i, row))
                .collect::<Vec<_>>())
        }));
    }
    let reranked: Vec<Vec<(f32, usize, u32)>> = futures::stream::iter(reranks)
        .buffer_unordered(limit)
        .try_collect()
        .await?;
    winners.extend(reranked.into_iter().flatten());

    // Global top-k by (distance, id), then hydrate only the winners.
    let mut id_indexes = BTreeMap::new();
    for &(_, i, _) in &winners {
        if let std::collections::btree_map::Entry::Vacant(e) = id_indexes.entry(i) {
            e.insert(segment::ids(store, &state.segments[i]).await?);
        }
    }
    let mut ranked: Vec<(f32, Id, usize, u32)> = winners
        .into_iter()
        .map(|(d, i, row)| {
            let id = id_indexes[&i]
                .get(row as usize)
                .cloned()
                .ok_or_else(|| segment::index_error("row outside id index"))?;
            Ok((d, id, i, row))
        })
        .collect::<Result<_, ApiError>>()?;
    ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    ranked.truncate(k);
    let mut scores = BTreeMap::new();
    let mut rows_by_segment: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
    for (d, id, i, row) in ranked {
        scores.insert(id, d as f64);
        rows_by_segment.entry(i).or_default().insert(row);
    }
    let mut docs = BTreeMap::new();
    for (i, rows) in rows_by_segment {
        docs.extend(
            segment::projected_documents(store, &state.segments[i], &rows, Some(columns)).await?,
        );
    }
    Ok(Some((scores, docs)))
}
