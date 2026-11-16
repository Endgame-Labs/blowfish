//! Namespace ANN directory (see `docs/design/ann-directory.md`): build it at publication, and
//! answer unfiltered ANN queries from it in a fixed number of read waves without opening any
//! segment's index metadata.
use crate::{EngineConfig, NamespaceState, read_cache, segment};
use bf_storage::Storage;
use bf_types::*;
use bf_vector::{Candidate, Directory, Index, Membership, RoaringBitmap};
use futures::{FutureExt, StreamExt, TryStreamExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
};

/// Fetch merged ranges at most this far apart as one request.
const BLOB_GAP: u64 = 64 << 10;
const BLOB_MERGE_MAX: u64 = 8 << 20;
/// Exact-scan vector prefetch is split into concurrent reads of this size.
const PREFETCH_PIECE: u64 = 1 << 20;
/// Above this many matching rows, fetching only the matching vectors stops being cheaper
/// than scanning quantized codes.
const EXACT_ROWS: usize = 2048;
/// Never hold more than this many bytes of exact vectors for one query.
const EXACT_BYTES: usize = 256 << 20;

type Reader = segment::ObjectReader;
/// Exact hits `(distance, row)` and estimated candidates from one segment.
type Found = (Vec<(f32, u32)>, Vec<Candidate>);

/// Load (and pin in the decoded cache) a directory object: one read of the whole object, which
/// is all resident metadata.
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
    let frag = format!("annfrag:{attr}");
    dir.segments().len() == segments.len()
        && dir.segments().iter().zip(segments).all(|(d, s)| {
            d.id == s.id
                && s.files.get(&key).is_some_and(|f| f.key == d.key)
                && s.files.get(&frag).is_some_and(|f| f.key == d.fragment)
        })
}

/// Build (or keep) the directory for `key` (`ann:<attribute>`) over `segments`. Returns `None`
/// when some segment lacks the index or its fragment, or the segments do not share one vector
/// space. Incremental: sections of segments already in `previous` are carried over, and only
/// new segments' fragments (small, one ranged read each) are read.
pub(crate) async fn build(
    store: &Storage,
    ns: &str,
    segments: &[bf_wal::Segment],
    key: &str,
    previous: Option<&bf_wal::SegmentFile>,
) -> Result<Option<bf_wal::SegmentFile>, ApiError> {
    let attr = key.trim_start_matches("ann:");
    let frag_key = format!("annfrag:{attr}");
    if segments.is_empty()
        || segments
            .iter()
            .any(|s| !s.files.contains_key(key) || !s.files.contains_key(&frag_key))
    {
        return Ok(None);
    }
    let old = match previous {
        Some(old) => load(store, old).await.ok().map(|dir| (old, dir)),
        None => None,
    };
    if let Some((old, dir)) = &old
        && covers(dir, segments, attr)
    {
        return Ok(Some((*old).clone()));
    }
    let carried: BTreeMap<&str, usize> = old
        .iter()
        .flat_map(|(_, d)| {
            d.segments()
                .iter()
                .enumerate()
                .map(|(i, s)| (s.fragment.as_str(), i))
        })
        .collect();
    let mut sources = Vec::with_capacity(segments.len());
    let reads = futures::future::try_join_all(segments.iter().map(|seg| {
        let carried = &carried;
        let old = &old;
        let frag_key = &frag_key;
        async move {
            let file = &seg.files[frag_key];
            if let Some(&i) = carried.get(file.key.as_str())
                && let Some((_, dir)) = old
            {
                return Ok::<_, ApiError>(dir.fragment_of(i));
            }
            let bytes = Reader::new(store, file)
                .read(0..file.metadata_len)
                .await
                .map_err(segment::index_error)?;
            bf_vector::Fragment::decode(&bytes).map_err(segment::index_error)
        }
    }))
    .await;
    // A fragment in an older format (without row-to-cluster maps) means no directory yet:
    // queries open each segment until compaction rewrites it.
    let reads = match reads {
        Ok(reads) => reads,
        Err(e) => {
            tracing::warn!(error=?e, "ANN directory not built: fragment unreadable");
            return Ok(None);
        }
    };
    for (seg, fragment) in segments.iter().zip(reads) {
        sources.push((
            seg.id.clone(),
            seg.files[key].key.clone(),
            seg.files[&frag_key].key.clone(),
            fragment,
        ));
    }
    let Some(bytes) = Directory::build(&sources).map_err(segment::index_error)? else {
        tracing::warn!("ANN directory not built: segments do not share one vector space");
        return Ok(None);
    };
    let len = bytes.len() as u64;
    Ok(Some(
        segment::put_file(
            store,
            format!("ns/{ns}/segments/ann-dir-{}.bfd", uuid::Uuid::new_v4()),
            bytes,
            len,
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

/// Row masks per segment: with a filter, the live rows that pass it (every segment gets one);
/// without, the live rows of segments with deletions (generation above zero), and `None` (all
/// rows are live) for the others. The caller has checked that no tail ID shadows a segment row.
pub(crate) async fn live_masks(
    store: &Storage,
    state: &NamespaceState,
    filter: Option<&Filter>,
) -> Result<Vec<Option<Arc<RoaringBitmap>>>, ApiError> {
    futures::future::try_join_all(state.segments.iter().map(|seg| async move {
        if let Some(f) = filter {
            return Ok(Some(Arc::new(
                crate::snapshot::allowed(store, state, seg, Some(f)).await?,
            )));
        }
        if seg.live_generation == 0 {
            return Ok(None);
        }
        Ok(Some(Arc::new(segment::live(store, seg).await?)))
    }))
    .await
}

/// Load (and cache) the row-to-cluster map of every segment that has one. `None` for flat
/// segments and for segments whose fragment predates the map.
pub(crate) async fn memberships(
    store: &Storage,
    segments: &[bf_wal::Segment],
    dir: &Directory,
    attr: &str,
) -> Result<Vec<Option<Arc<Membership>>>, ApiError> {
    let frag_key = format!("annfrag:{attr}");
    futures::future::try_join_all(dir.segments().iter().zip(segments).map(|(d, seg)| {
        let frag_key = &frag_key;
        async move {
            let (Some(range), Some(file)) = (d.membership.clone(), seg.files.get(frag_key)) else {
                return Ok(None);
            };
            let key = format!("member:{}", file.key);
            if let Some(m) = read_cache::get(&key, range.end - range.start) {
                return Ok(Some(m));
            }
            let bytes = Reader::new(store, file)
                .read(range.clone())
                .await
                .map_err(segment::index_error)?;
            let m = Membership::decode(&bytes, d.rows, d.clusters).map_err(segment::index_error)?;
            let resident = m.resident_bytes();
            Ok(Some(read_cache::insert(&key, resident, m)))
        }
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
    dir: Arc<Directory>,
    lives: Vec<Option<Arc<RoaringBitmap>>>,
    vector: &[f32],
    config: &EngineConfig,
    k: usize,
    columns: &BTreeSet<String>,
    attribute: &str,
    members: Option<Vec<Option<Arc<Membership>>>>,
) -> Result<(BTreeMap<Id, f64>, BTreeMap<Id, Document>), ApiError> {
    let filtered = members.is_some();
    let members = members.unwrap_or_default();
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
    // Rows in clusters of ANN (non-flat) segments, per segment and in all.
    let mut seg_rows = vec![0usize; dir.segments().len()];
    let mut seg_clusters = vec![0usize; dir.segments().len()];
    for e in entries {
        seg_rows[e.segment] += e.rows as usize;
        seg_clusters[e.segment] += 1;
    }
    let mut ann_rows = 0usize;
    let mut matching = 0usize;
    let mut clusters = 0usize;
    // Expected allowed rows per cluster of each segment: the segment's allowed fraction.
    let mut fraction = vec![1.0f64; dir.segments().len()];
    for (i, seg) in dir.segments().iter().enumerate() {
        if !seg.flat {
            ann_rows += seg_rows[i];
            matching += live_rows(i).min(seg_rows[i]);
            clusters += seg_clusters[i];
            fraction[i] = live_rows(i).min(seg_rows[i]) as f64 / seg_rows[i].max(1) as f64;
        }
    }
    let target = k.saturating_mul(config.ann_rerank_factor).min(matching);
    // Filtered: allowed rows in each cluster, exactly, from the row-to-cluster maps.
    let counts: Vec<Vec<u32>> = if filtered {
        dir.segments()
            .iter()
            .enumerate()
            .map(
                |(i, seg)| match (members.get(i).and_then(Option::as_ref), &lives[i]) {
                    (Some(m), Some(allowed)) if !seg.flat => m.counts(allowed, seg.clusters),
                    _ => Vec::new(),
                },
            )
            .collect()
    } else {
        Vec::new()
    };
    let in_cluster = |n: usize| -> f64 {
        let e = &entries[n];
        match counts.get(e.segment).filter(|c| !c.is_empty()) {
            Some(c) => f64::from(c[e.cluster]),
            None => e.rows as f64 * fraction[e.segment],
        }
    };
    let mut chosen: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut exact_all = false;
    // Segments whose few allowed rows are fetched and ranked exactly, without codes or blobs.
    let mut exact_rows = BTreeSet::new();
    if matching > 0 {
        if filtered && matching <= config.exact_fetch_rows.max(target) {
            for (i, seg) in dir.segments().iter().enumerate() {
                if !seg.flat && live_rows(i) > 0 {
                    exact_rows.insert(i);
                }
            }
        } else {
            let mut order: Vec<(f32, usize)> = entries
                .iter()
                .enumerate()
                .filter(|(n, e)| {
                    !dir.segments()[e.segment].flat && e.rows > 0 && in_cluster(*n) > 0.0
                })
                .map(|(n, _)| (distances[n], n))
                .collect();
            let scale = (clusters as f64 / (ann_rows as f64).sqrt()).max(1.0);
            // Deleted rows and filtered-out rows thin the probed clusters, so probe more (a
            // filter at most `filtered_probe_cap` times) and keep probing until the allowed
            // candidates reach `target`.
            let thin = {
                let thin = ann_rows as f64 / matching.max(1) as f64;
                if filtered {
                    thin.min(config.filtered_probe_cap.max(1) as f64)
                } else {
                    thin
                }
            };
            let probes = ((config.ann_nprobe as f64 * scale * thin).ceil() as usize).min(clusters);
            order.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let (mut count, mut candidates) = (0usize, 0f64);
            let mut picked = Vec::new();
            for &(_, n) in &order {
                picked.push(n);
                count += 1;
                candidates += in_cluster(n);
                if count >= probes && candidates >= target as f64 {
                    break;
                }
            }
            if !filtered
                && (matching <= k
                    || (matching <= EXACT_ROWS.max(count + target)
                        && matching * width <= EXACT_BYTES))
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
    }
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
    let mut tasks: Vec<futures::future::BoxFuture<'_, Result<(Probed, Found), ApiError>>> =
        Vec::new();
    for &i in &exact_rows {
        let Some(member) = members.get(i).cloned().flatten() else {
            return Err(segment::index_error("segment lost its membership map"));
        };
        let live = lives[i].clone().unwrap_or_default();
        let file = state.segments[i]
            .files
            .get(&key)
            .cloned()
            .ok_or_else(|| segment::index_error("segment lost its ANN index"))?;
        let partial = dir
            .partial_index(i, Reader::new(store, &file))
            .map_err(segment::index_error)?;
        let (vector, segment_meta, store) =
            (vector.clone(), state.segments[i].clone(), store.clone());
        tasks.push(
            async move {
                let locations = member.locations(&live, |_| true);
                let (hits, ()) = tokio::try_join!(
                    async {
                        partial
                            .exact_at(&vector, k, locations)
                            .await
                            .map_err(segment::index_error)
                    },
                    async {
                        segment::warm_docs(&store, &segment_meta).await;
                        Ok(())
                    }
                )?;
                let probed = Probed {
                    segment: i,
                    index: Arc::new(partial.into_index()),
                    clusters: Vec::new(),
                    exact: true,
                };
                Ok((
                    probed,
                    (hits.into_iter().map(|(r, d)| (d, r)).collect(), Vec::new()),
                ))
            }
            .boxed(),
        );
    }
    for (&i, cs) in &chosen {
        let (dir, vector) = (dir.clone(), vector.clone());
        let exact = exact_segments.contains(&i);
        let flat = dir.segments()[i].flat;
        let cs = cs.clone();
        let file = state.segments[i]
            .files
            .get(&key)
            .cloned()
            .ok_or_else(|| segment::index_error("segment lost its ANN index"))?;
        let reader = Reader::new(store, &file);
        let frag_file = state.segments[i]
            .files
            .get(&format!("annfrag:{attribute}"))
            .cloned()
            .ok_or_else(|| segment::index_error("segment lost its ANN fragment"))?;
        let dir_reader = Reader::new(store, &frag_file);
        let live = lives[i].clone();
        let segment_meta = state.segments[i].clone();
        let store = store.clone();
        let seg_entries: Vec<_> = dir
            .entries()
            .iter()
            .filter(|e| e.segment == i && cs.contains(&e.cluster))
            .cloned()
            .collect();
        tasks.push(
            async move {
                // An exactly scanned segment needs all of its vectors, and its row sets (from the
                // blobs) only say which. Read the vectors in parallel with the blobs, as
                // concurrent pieces, so the later exact scan is served from the cache.
                let prefetch_ranges: Vec<_> = if exact && (flat || exact_all) {
                    seg_entries.iter().map(|e| e.vectors_range()).collect()
                } else {
                    Vec::new()
                };
                let prefetch_reader = reader.clone();
                let prefetch = async move {
                    let pieces = prefetch_ranges.into_iter().flat_map(|r| {
                        let mut out = Vec::new();
                        let mut start = r.start;
                        while start < r.end {
                            let end = (start + PREFETCH_PIECE).min(r.end);
                            out.push(start..end);
                            start = end;
                        }
                        out
                    });
                    // Best effort: a failure here only means the scan reads the range itself.
                    let _ =
                        futures::future::join_all(pieces.map(|r| prefetch_reader.read(r))).await;
                    Ok::<(), ApiError>(())
                };
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
                let (blobs, codes, (), ()) =
                    tokio::try_join!(read_blobs, read_codes, prefetch, async {
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
            }
            .boxed(),
        );
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
        let keep = (k * 3).min(best.len());
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
