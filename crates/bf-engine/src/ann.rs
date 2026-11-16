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
    ids: Arc<segment::IdIndex>,
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
    // `BF_ANN_DIRECTORY=0` forces the per-segment path (A/B measurements only).
    static DIRECTORY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *DIRECTORY.get_or_init(|| std::env::var("BF_ANN_DIRECTORY").is_ok_and(|v| v == "0")) {
        // fall through
    } else if q.filters.is_none()
        && state.shadowed.is_empty()
        && let Some(file) = state.ann_dirs.get(&key)
    {
        let (dir, lives) = tokio::try_join!(
            crate::ann_dir::load(store, file),
            crate::ann_dir::live_masks(store, state)
        )?;
        if crate::ann_dir::covers(&dir, &state.segments, attribute) {
            return crate::ann_dir::search(
                store, state, file, dir, lives, vector, config, k, columns, attribute,
            )
            .await
            .map(Some);
        }
    }
    let vector: Arc<[f32]> = vector.into();
    let t0 = std::time::Instant::now();
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
                let (allowed, index, ids) = tokio::try_join!(
                    snapshot::allowed(&store, &base, &seg, filter.as_ref()),
                    segment::vector_index(&store, &file),
                    segment::ids(&store, &seg),
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
                        ids,
                        allowed,
                        counts,
                        distances,
                    }),
                ))
            }
        }))
        .buffer_unordered((config.search_concurrency.max(1) * 2).max(48));
    while let Some((i, seg)) = loaded.try_next().await? {
        segs[i] = seg;
    }
    drop(loaded);
    stage(t0, "load segments");

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

    stage(t0, "plan");
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
    // Flat segments and clustered scans are independent: overlap their reads.
    let (exact_hits, scanned) = tokio::try_join!(
        futures::stream::iter(tasks)
            .buffer_unordered(limit)
            .try_collect::<Vec<Vec<(f32, usize, u32)>>>(),
        futures::stream::iter(scans)
            .buffer_unordered(limit)
            .try_collect::<Vec<Vec<(usize, Candidate)>>>(),
    )?;
    winners.extend(exact_hits.into_iter().flatten());
    let mut pool: Vec<(usize, Candidate)> = scanned.into_iter().flatten().collect();
    stage(t0, "scan");
    if pool.len() > target {
        pool.select_nth_unstable_by(target, |a, b| {
            a.1.distance
                .total_cmp(&b.1.distance)
                .then(a.1.row.cmp(&b.1.row))
        });
        pool.truncate(target);
    }
    // Documents are fetched after the winners are known, which would add a round trip. Start
    // reading the best candidates' documents now: the winners are almost always among them,
    // and the later hydration is then served from the cache.
    let mut speculative: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
    if !columns.is_empty() {
        let mut best: Vec<&(usize, Candidate)> = pool.iter().collect();
        let keep = (k * 2).min(best.len());
        if keep > 0 && best.len() > keep {
            best.select_nth_unstable_by(keep, |a, b| {
                a.1.distance
                    .total_cmp(&b.1.distance)
                    .then(a.1.row.cmp(&b.1.row))
            });
        }
        for (i, c) in best.into_iter().take(keep) {
            speculative.entry(*i).or_default().insert(c.row);
        }
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
    let prefetch = futures::future::join_all(speculative.iter().map(|(&i, rows)| async move {
        let _ = segment::projected_documents(store, &state.segments[i], rows, Some(columns)).await;
    }));
    let (reranked, _) = tokio::join!(
        futures::stream::iter(reranks)
            .buffer_unordered(limit)
            .try_collect::<Vec<Vec<(f32, usize, u32)>>>(),
        prefetch
    );
    let reranked = reranked?;
    winners.extend(reranked.into_iter().flatten());
    stage(t0, "rerank");

    // Global top-k by (distance, id), then hydrate only the winners.
    let mut ranked: Vec<(f32, Id, usize, u32)> = winners
        .into_iter()
        .map(|(d, i, row)| {
            let id = segs[i]
                .as_ref()
                .expect("winning segment is loaded")
                .ids
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
    for part in futures::future::try_join_all(rows_by_segment.iter().map(|(&i, rows)| {
        segment::projected_documents(store, &state.segments[i], rows, Some(columns))
    }))
    .await?
    {
        docs.extend(part);
    }
    stage(t0, "hydrate");
    Ok(Some((scores, docs)))
}

/// `BF_TRACE_STAGES=1` prints elapsed time at each ANN stage (diagnostics only).
pub(crate) fn stage(t0: std::time::Instant, name: &str) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *ON.get_or_init(|| std::env::var_os("BF_TRACE_STAGES").is_some()) {
        eprintln!("STAGE {name} {} ms", t0.elapsed().as_millis());
    }
}
