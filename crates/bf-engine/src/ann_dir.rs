//! Namespace ANN directory (see `docs/design/ann-directory.md`): build it at publication, and
//! answer unfiltered ANN queries from it in a fixed number of read waves without opening any
//! segment's index metadata.
use crate::{EngineConfig, NamespaceState, read_cache, segment};
use bf_storage::Storage;
use bf_types::*;
use bf_vector::{Candidate, Directory, Index, RoaringBitmap};
use futures::{StreamExt, TryStreamExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
};

/// Fetch merged ranges at most this far apart as one request.
const BLOB_GAP: u64 = 64 << 10;
const BLOB_MERGE_MAX: u64 = 8 << 20;
/// Above this many matching rows, fetching only the matching vectors stops being cheaper
/// than scanning quantized codes.
const EXACT_ROWS: usize = 2048;
/// Never hold more than this many bytes of exact vectors for one query.
const EXACT_BYTES: usize = 256 << 20;

type Reader = segment::ObjectReader;
/// Exact hits `(distance, row)` and estimated candidates from one segment.
type Found = (Vec<(f32, u32)>, Vec<Candidate>);

/// Load (and pin in the decoded cache) a directory object: one ranged read of its metadata part.
pub(crate) async fn load(
    store: &Storage,
    file: &bf_wal::SegmentFile,
) -> Result<Arc<Directory>, ApiError> {
    if let Some(dir) = read_cache::get(&file.key, file.metadata_len) {
        return Ok(dir);
    }
    if file.metadata_len > file.size {
        return Err(segment::index_error("directory metadata exceeds object"));
    }
    let bytes = Reader::new(store, file)
        .read(0..file.metadata_len)
        .await
        .map_err(segment::index_error)?;
    let dir = Directory::decode(&bytes).map_err(segment::index_error)?;
    let resident = dir.resident_bytes();
    Ok(read_cache::insert(&file.key, resident, dir))
}

/// Whether `dir` lists exactly these segments' indexes for `attr`, in order.
pub(crate) fn covers(dir: &Directory, segments: &[bf_wal::Segment], attr: &str) -> bool {
    let key = format!("ann:{attr}");
    dir.segments().len() == segments.len()
        && dir
            .segments()
            .iter()
            .zip(segments)
            .all(|(d, s)| d.id == s.id && s.files.get(&key).is_some_and(|f| f.key == d.key))
}

/// Build (or keep) the directory for `key` (`ann:<attribute>`) over `segments`. Returns `None`
/// when some segment lacks the index or the segments do not share one vector space.
pub(crate) async fn build(
    store: &Storage,
    ns: &str,
    segments: &[bf_wal::Segment],
    key: &str,
    previous: Option<&bf_wal::SegmentFile>,
) -> Result<Option<bf_wal::SegmentFile>, ApiError> {
    if segments.is_empty() || segments.iter().any(|s| !s.files.contains_key(key)) {
        return Ok(None);
    }
    let attr = key.trim_start_matches("ann:");
    if let Some(old) = previous
        && let Ok(dir) = load(store, old).await
        && covers(&dir, segments, attr)
    {
        return Ok(Some(old.clone()));
    }
    let mut indexes = Vec::with_capacity(segments.len());
    for seg in segments {
        indexes.push(segment::vector_index(store, &seg.files[key]).await?);
    }
    let sources: Vec<_> = segments
        .iter()
        .zip(&indexes)
        .map(|(s, i)| (s.id.clone(), s.files[key].key.clone(), &**i))
        .collect();
    let Some((bytes, meta)) = Directory::build(&sources).map_err(segment::index_error)? else {
        return Ok(None);
    };
    Ok(Some(
        segment::put_file(
            store,
            format!("ns/{ns}/segments/ann-dir-{}.bfd", uuid::Uuid::new_v4()),
            bytes,
            meta,
        )
        .await?,
    ))
}

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

/// Live-row masks for segments with deletions (generation above zero); others are all live.
pub(crate) async fn live_masks(
    store: &Storage,
    state: &NamespaceState,
) -> Result<Vec<Option<Arc<RoaringBitmap>>>, ApiError> {
    futures::future::try_join_all(state.segments.iter().map(|seg| async move {
        if seg.live_generation == 0 {
            return Ok(None);
        }
        Ok(Some(Arc::new(segment::live(store, seg).await?)))
    }))
    .await
}

struct Probed {
    segment: usize,
    index: Arc<Index<Reader>>,
    clusters: Vec<usize>,
    exact: bool,
}

/// Answer an unfiltered ANN query from the directory. The caller has checked that the
/// directory covers `state.segments` and that no tail ID shadows a segment row.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn search(
    store: &Storage,
    state: &NamespaceState,
    dir_file: &bf_wal::SegmentFile,
    dir: Arc<Directory>,
    lives: Vec<Option<Arc<RoaringBitmap>>>,
    vector: &[f32],
    config: &EngineConfig,
    k: usize,
    columns: &BTreeSet<String>,
    attribute: &str,
) -> Result<(BTreeMap<Id, f64>, BTreeMap<Id, Document>), ApiError> {
    let t0 = std::time::Instant::now();
    let key = format!("ann:{attribute}");
    let vector: Arc<[f32]> = vector.into();
    let width = dir.vector_row_bytes();
    let distances = {
        let (dir, vector) = (dir.clone(), vector.clone());
        on_worker(async move { dir.center_distances(&vector).map_err(segment::index_error) })
            .await?
    };
    crate::ann::stage(t0, "rank");
    let entries = dir.entries();
    let live_rows = |i: usize| {
        lives[i]
            .as_ref()
            .map_or(state.segments[i].row_count as usize, |l| l.len() as usize)
    };
    // Rows in clusters of ANN (non-flat) segments, minus deleted rows.
    let seg_rows: Vec<usize> = (0..dir.segments().len())
        .map(|i| {
            entries
                .iter()
                .filter(|e| e.segment == i)
                .map(|e| e.rows as usize)
                .sum::<usize>()
        })
        .collect();
    let mut ann_rows = 0usize;
    let mut matching = 0usize;
    let mut clusters = 0usize;
    for (i, seg) in dir.segments().iter().enumerate() {
        if !seg.flat {
            ann_rows += seg_rows[i];
            matching += live_rows(i).min(seg_rows[i]);
            clusters += entries.iter().filter(|e| e.segment == i).count();
        }
    }
    let flat_rows: usize = dir
        .segments()
        .iter()
        .enumerate()
        .filter(|(_, s)| s.flat)
        .map(|(i, _)| live_rows(i))
        .sum();
    let target = k.saturating_mul(config.ann_rerank_factor).min(matching);
    let mut chosen: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut exact_all = false;
    if matching > 0 {
        let mut order: Vec<(f32, usize)> = entries
            .iter()
            .enumerate()
            .filter(|(_, e)| !dir.segments()[e.segment].flat && e.rows > 0)
            .map(|(n, _)| (distances[n], n))
            .collect();
        let scale = (clusters as f64 / (ann_rows as f64).sqrt()).max(1.0);
        let probes = ((config.ann_nprobe as f64 * scale * ann_rows as f64 / matching.max(1) as f64)
            .ceil() as usize)
            .min(clusters);
        order.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let (mut count, mut candidates) = (0usize, 0usize);
        let mut picked = Vec::new();
        for &(_, n) in &order {
            picked.push(n);
            count += 1;
            candidates += entries[n].rows as usize;
            if count >= probes && candidates >= target {
                break;
            }
        }
        if matching <= k
            || (matching <= EXACT_ROWS.max(count + target) && matching * width <= EXACT_BYTES)
        {
            exact_all = true;
        } else {
            for n in picked {
                chosen
                    .entry(entries[n].segment)
                    .or_default()
                    .push(entries[n].cluster);
            }
        }
    }
    let _ = flat_rows;
    // Flat segments, and every ANN segment when the namespace is small, are scanned exactly.
    let mut exact_segments = BTreeSet::new();
    for (i, seg) in dir.segments().iter().enumerate() {
        if seg.flat || exact_all {
            exact_segments.insert(i);
            chosen.insert(
                i,
                entries
                    .iter()
                    .filter(|e| e.segment == i)
                    .map(|e| e.cluster)
                    .collect(),
            );
        }
    }
    crate::ann::stage(t0, "plan");

    // Wave 3: probed codes and cluster blobs for each chosen segment, in parallel. Docs
    // footers for the chosen segments are warmed in the same wave.
    let dir_reader = Reader::new(store, dir_file);
    let mut tasks = Vec::new();
    for (&i, cs) in &chosen {
        let (dir, vector) = (dir.clone(), vector.clone());
        let exact = exact_segments.contains(&i);
        let cs = cs.clone();
        let file = state.segments[i]
            .files
            .get(&key)
            .cloned()
            .ok_or_else(|| segment::index_error("segment lost its ANN index"))?;
        let reader = Reader::new(store, &file);
        let dir_reader = dir_reader.clone();
        let live = lives[i].clone();
        let segment_meta = state.segments[i].clone();
        let store = store.clone();
        let seg_entries: Vec<_> = dir
            .entries()
            .iter()
            .filter(|e| e.segment == i && cs.contains(&e.cluster))
            .cloned()
            .collect();
        tasks.push(async move {
            let mut partial = dir.partial_index(i, reader).map_err(segment::index_error)?;
            let ranges: Vec<_> = seg_entries.iter().map(|e| e.blob_range()).collect();
            let wanted: Vec<usize> = seg_entries.iter().map(|e| e.cluster).collect();
            let read_blobs = async {
                bf_vector::read_merged(&dir_reader, ranges, BLOB_GAP, BLOB_MERGE_MAX)
                    .await
                    .map_err(segment::index_error)
            };
            let read_codes = async {
                if exact {
                    Ok(Vec::new())
                } else {
                    partial
                        .index()
                        .read_codes(&wanted)
                        .await
                        .map_err(segment::index_error)
                }
            };
            let (blobs, codes, ()) = tokio::try_join!(read_blobs, read_codes, async {
                segment::warm_docs(&store, &segment_meta).await;
                Ok(())
            })?;
            for (cluster, blob) in wanted.iter().zip(&blobs) {
                partial
                    .install(*cluster, blob)
                    .map_err(segment::index_error)?;
            }
            let index = Arc::new(partial.into_index());
            let probed = Probed {
                segment: i,
                index: index.clone(),
                clusters: wanted.clone(),
                exact,
            };
            let found = on_worker(async move {
                if exact {
                    let hits = index
                        .exact_search(&vector, k, live.as_deref())
                        .await
                        .map_err(segment::index_error)?;
                    return Ok((
                        hits.into_iter().map(|(r, d)| (d, r)).collect::<Vec<_>>(),
                        Vec::new(),
                    ));
                }
                let found = index
                    .scan_blocks(&vector, &wanted, codes, live.as_deref())
                    .map_err(segment::index_error)?;
                Ok((Vec::new(), found))
            })
            .await?;
            Ok::<_, ApiError>((probed, found))
        });
    }
    let limit = config.search_concurrency.max(1) * 2;
    let results: Vec<(Probed, Found)> = futures::stream::iter(tasks)
        .buffer_unordered(limit.max(48))
        .try_collect()
        .await?;
    crate::ann::stage(t0, "scan");
    let mut winners: Vec<(f32, usize, u32)> = Vec::new(); // (exact distance, segment, row)
    let mut pool: Vec<(usize, Candidate)> = Vec::new();
    let mut indexes: BTreeMap<usize, Arc<Index<Reader>>> = BTreeMap::new();
    for (probed, (exact_hits, found)) in results {
        let _ = (&probed.clusters, probed.exact);
        winners.extend(exact_hits.into_iter().map(|(d, r)| (d, probed.segment, r)));
        pool.extend(found.into_iter().map(|c| (probed.segment, c)));
        indexes.insert(probed.segment, probed.index);
    }
    let cmp = |a: &(usize, Candidate), b: &(usize, Candidate)| {
        a.1.distance
            .total_cmp(&b.1.distance)
            .then(a.0.cmp(&b.0))
            .then(a.1.row.cmp(&b.1.row))
    };
    // Error-bounded reranking: only candidates whose lower bound can still beat the k-th best
    // upper bound need their exact distance. `ann_rerank_factor` stays as a cap.
    if k > 0 && pool.len() > k {
        let mut upper: Vec<f32> = pool.iter().map(|(_, c)| c.distance + c.error).collect();
        upper.select_nth_unstable_by(k - 1, f32::total_cmp);
        let threshold = upper[k - 1];
        pool.retain(|(_, c)| c.distance - c.error <= threshold);
    }
    if pool.len() > target {
        pool.select_nth_unstable_by(target, cmp);
        pool.truncate(target);
    }
    if std::env::var_os("BF_TRACE_STAGES").is_some() {
        eprintln!("RERANK pool={} cap={target}", pool.len());
    }
    // Start reading the likeliest winners' documents while their vectors are reranked.
    let mut speculative: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
    if !pool.is_empty() {
        let mut best: Vec<&(usize, Candidate)> = pool.iter().collect();
        // Document blocks hold 256 rows, so reading more of them is cheap unless a vector column
        // is returned. The winners are almost always among the best estimates.
        let heavy = columns.iter().any(|c| {
            matches!(
                state.schema.get(c).and_then(|s| s.data_type.as_ref()),
                Some(AttributeType::Vector { .. } | AttributeType::VectorI8 { .. })
            )
        });
        let keep = (k * if heavy { 3 } else { 8 }).min(best.len());
        if best.len() > keep {
            best.select_nth_unstable_by(keep, |a, b| cmp(a, b));
        }
        for (i, c) in best.into_iter().take(keep) {
            speculative.entry(*i).or_default().insert(c.row);
        }
    }
    // Exactly scanned segments already know their best rows.
    winners.sort_by(|a, b| a.0.total_cmp(&b.0));
    for (_, i, row) in winners.iter().take(k * 2) {
        speculative.entry(*i).or_default().insert(*row);
    }
    let mut by_segment: BTreeMap<usize, Vec<(u32, u32)>> = BTreeMap::new();
    for (i, c) in pool {
        by_segment
            .entry(i)
            .or_default()
            .push((c.cluster, c.ordinal));
    }
    let reranks = by_segment.into_iter().map(|(i, locations)| {
        let (index, vector) = (indexes[&i].clone(), vector.clone());
        on_worker(async move {
            let hits = index
                .rerank_candidates(&vector, k, locations)
                .await
                .map_err(segment::index_error)?;
            Ok(hits.into_iter().map(|(r, d)| (d, i, r)).collect::<Vec<_>>())
        })
    });
    let prefetch = futures::future::join_all(speculative.iter().map(|(&i, rows)| async move {
        let _ = segment::projected_rows(store, &state.segments[i], rows, Some(columns)).await;
    }));
    let (reranked, _) = tokio::join!(
        futures::stream::iter(reranks)
            .buffer_unordered(limit)
            .try_collect::<Vec<Vec<(f32, usize, u32)>>>(),
        prefetch
    );
    winners.extend(reranked?.into_iter().flatten());
    crate::ann::stage(t0, "rerank");

    // Top k by (distance, segment, row), extended by every row tied with the k-th distance so
    // that the final tie-break by ID is the same as the per-segment path's.
    winners.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    let mut keep = k.min(winners.len());
    if keep > 0 {
        let edge = winners[keep - 1].0;
        while keep < winners.len() && winners[keep].0 == edge {
            keep += 1;
        }
    }
    winners.truncate(keep);
    let mut rows_by_segment: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
    for (_, i, row) in &winners {
        rows_by_segment.entry(*i).or_default().insert(*row);
    }
    let mut hydrated: BTreeMap<(usize, u32), Document> = BTreeMap::new();
    for (i, part) in
        futures::future::try_join_all(rows_by_segment.iter().map(|(&i, rows)| async move {
            Ok::<_, ApiError>((
                i,
                segment::projected_rows(store, &state.segments[i], rows, Some(columns)).await?,
            ))
        }))
        .await?
    {
        hydrated.extend(part.into_iter().map(|(row, doc)| ((i, row), doc)));
    }
    let mut ranked = Vec::with_capacity(winners.len());
    for (d, i, row) in winners {
        let doc = hydrated
            .remove(&(i, row))
            .ok_or_else(|| segment::index_error("winner row missing from documents"))?;
        let id: Id = serde_json::from_value(
            doc.get("id")
                .cloned()
                .ok_or_else(|| segment::index_error("document without id"))?,
        )
        .map_err(segment::index_error)?;
        ranked.push((d, id, doc));
    }
    ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    ranked.truncate(k);
    let mut scores = BTreeMap::new();
    let mut docs = BTreeMap::new();
    for (d, id, mut doc) in ranked {
        scores.insert(id.clone(), d as f64);
        if columns.is_empty() {
            doc.retain(|name, _| name == "id");
        }
        docs.insert(id, doc);
    }
    crate::ann::stage(t0, "hydrate");
    Ok((scores, docs))
}
