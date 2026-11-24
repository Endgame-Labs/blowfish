//! Namespace ANN directory (see `docs/design/ann-directory.md`): build it at publication, and
//! answer unfiltered ANN queries from it in a fixed number of read waves without opening any
//! segment's index metadata.
use crate::{EngineConfig, NamespaceState, read_cache, segment};
use bf_storage::Storage;
use bf_types::*;
use bf_vector::{Candidate, Directory, Index, Membership, Page, PagedDirectory, RoaringBitmap};
use futures::{FutureExt, StreamExt, TryStreamExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
};

/// Maximum merged read of leaf blobs and codes.
const BLOB_MERGE_MAX: u64 = 8 << 20;
/// Exact-scan vector prefetch is split into concurrent reads of this size.
const PREFETCH_PIECE: u64 = 1 << 20;
/// Above this many matching rows, fetching only the matching vectors stops being cheaper
/// than scanning quantized codes.
const EXACT_ROWS: usize = 2048;
/// Never hold more than this many bytes of exact vectors for one query.
const EXACT_BYTES: usize = 256 << 20;

/// Merge nearby leaf blocks, which hold blobs and codes together.
const BLOCK_GAP: u64 = 16 << 10;

/// Read the blobs of `entries` (one segment's probed clusters) and, unless `exact`, their
/// quantized codes from the same ANN object.
/// Read the probed clusters' blobs and code blocks. `cached[i]` skips entry `i`'s blob (its
/// decoded form is already at hand); blobs come back only for the others, in order.
async fn read_clusters(
    entries: &[&bf_vector::DirEntry],
    exact: bool,
    fragment: &Reader,
    partial: &bf_vector::PartialIndex<Reader>,
    codes_in_fragment: bool,
    cached: Option<&[bool]>,
) -> Result<(Vec<bytes::Bytes>, Vec<bytes::Bytes>), ApiError> {
    if !codes_in_fragment {
        return crate::upgrade::read_clusters(entries, exact, fragment, partial, cached).await;
    }
    let blob_ranges: Vec<_> = entries
        .iter()
        .enumerate()
        .filter(|(i, _)| !cached.is_some_and(|c| c[*i]))
        .map(|(_, e)| e.blob_range())
        .collect();
    {
        let n = blob_ranges.len();
        let mut ranges = blob_ranges;
        if !exact {
            ranges.extend(entries.iter().map(|e| e.codes_range()));
        }
        let mut all = bf_vector::read_merged(
            fragment,
            ranges,
            bf_vector::read_gap(BLOCK_GAP),
            BLOB_MERGE_MAX,
        )
        .await
        .map_err(segment::index_error)?;
        let codes = all.split_off(n);
        Ok((all, codes))
    }
}

/// Split full-precision vector ranges into concurrent reads of at most `PREFETCH_PIECE` bytes.
fn vector_pieces(
    ranges: impl IntoIterator<Item = std::ops::Range<u64>>,
) -> Vec<std::ops::Range<u64>> {
    let mut out = Vec::new();
    for r in ranges {
        let mut start = r.start;
        while start < r.end {
            let end = (start + PREFETCH_PIECE).min(r.end);
            out.push(start..end);
            start = end;
        }
    }
    out
}

type Reader = segment::ObjectReader;
/// Exact hits `(distance, row)` and estimated candidates from one segment.
type Found = (Vec<(f32, u32)>, Vec<Candidate>);

/// Load (and pin in the decoded cache) a directory object: one read of the whole object, which
/// is all resident metadata.
pub(crate) async fn load(
    store: &Storage,
    file: &bf_wal::SegmentFile,
    prefetched: Option<&Prefetched>,
) -> Result<Arc<Directory>, ApiError> {
    if let Some(dir) = read_cache::get(&file.key, file.metadata_len) {
        return Ok(dir);
    }
    if let Some((key, bytes)) = prefetched
        && *key == file.key
        && bytes.len() as u64 >= file.metadata_len
        && let Ok(dir) = bf_vector::upgrade::directory(&bytes[..file.metadata_len as usize])
    {
        let resident = dir.resident_bytes();
        return Ok(read_cache::insert(&file.key, resident, dir));
    }
    if file.metadata_len > file.size {
        return Err(segment::index_error("directory metadata exceeds object"));
    }
    let bytes = Reader::new(store, file)
        .read(0..file.metadata_len)
        .await
        .map_err(segment::index_error)?;
    let dir = bf_vector::upgrade::directory(&bytes).map_err(segment::index_error)?;
    let resident = dir.resident_bytes();
    Ok(read_cache::insert(&file.key, resident, dir))
}

/// The current segment directory or the namespace-wide slab directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Layout {
    Segments,
    Slabs,
}
impl Layout {
    pub(crate) fn dir_key(self, attr: &str) -> String {
        match self {
            Self::Segments => format!("annv2:{attr}"),
            Self::Slabs => format!("ann3:{attr}"),
        }
    }
}

/// Whether `dir` lists exactly these segments' indexes for `attr`, in order.
pub(crate) fn covers(
    dir: &Directory,
    segments: &[bf_wal::Segment],
    attr: &str,
    _layout: Layout,
) -> bool {
    dir.segments().len() == segments.len()
        && dir
            .segments()
            .iter()
            .zip(segments)
            .all(|(d, s)| crate::upgrade::covers(d, s, attr))
}

/// Build (or keep) the directory of `layout` for `attr` over `segments`. Returns `None`
/// when some segment lacks its ANN object, or the segments do not share one vector
/// space. Incremental: sections of segments already in `previous` are carried over, and only
/// new segments' fragments (small, one ranged read each) are read.
pub(crate) async fn build(
    store: &Storage,
    ns: &str,
    segments: &[bf_wal::Segment],
    attr: &str,
    layout: Layout,
    previous: Option<&bf_wal::SegmentFile>,
) -> Result<Option<bf_wal::SegmentFile>, ApiError> {
    if segments.is_empty()
        || segments
            .iter()
            .any(|s| crate::upgrade::files(s, attr).is_none())
    {
        return Ok(None);
    }
    let old = match previous {
        Some(old) => load(store, old, None).await.ok().map(|dir| (old, dir)),
        None => None,
    };
    if let Some((old, dir)) = &old
        && covers(dir, segments, attr, layout)
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
        async move {
            let (_, file) = crate::upgrade::files(seg, attr).expect("checked sources");
            if let Some(&i) = carried.get(file.key.as_str())
                && let Some((_, dir)) = old
            {
                return Ok::<_, ApiError>((file.key.clone(), dir.fragment_of(i)));
            }
            crate::upgrade::fragment(store, seg, attr).await
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
    for (seg, (fragment_key, fragment)) in segments.iter().zip(reads) {
        sources.push((
            seg.id.clone(),
            crate::upgrade::files(seg, attr).unwrap().0.key.clone(),
            fragment_key,
            fragment,
        ));
    }
    let Some(bytes) = Directory::build(&sources).map_err(segment::index_error)? else {
        tracing::warn!("ANN directory not built: segments do not share one vector space");
        return Ok(None);
    };
    let len = bytes.len() as u64;
    let file = segment::put_file(
        store,
        format!(
            "ns/{ns}/segments/ann-dir{}-{}.bfd",
            match layout {
                Layout::Segments => "v2",
                Layout::Slabs => "3",
            },
            uuid::Uuid::new_v4()
        ),
        bytes.clone(),
        len,
    )
    .await?;
    // The slabs' hint is written by the recluster, which decides whether the directory is paged.
    if layout != Layout::Slabs {
        write_hint(store, ns, layout, attr, &file.key, &bytes).await;
    }
    Ok(Some(file))
}

/// A copy at a fixed key lets a cold reader fetch the directory (or a paged directory's header)
/// while it reads the manifest. Best effort: readers verify the embedded key against the manifest,
/// so a stale or missing hint only costs the extra wave.
pub(crate) async fn write_hint(
    store: &Storage,
    ns: &str,
    layout: Layout,
    attr: &str,
    object_key: &str,
    bytes: &[u8],
) {
    write_hint_named(
        store,
        ns,
        layout.dir_key(attr).trim_start_matches("ann:"),
        object_key,
        bytes,
    )
    .await;
}

/// Name (for [`hint_key`]) of the hint holding a paged slab directory's header. It is separate
/// from the flat slab directory's hint, which readers without paging still use.
pub(crate) fn paged_hint_name(attr: &str) -> String {
    format!("ann3p:{attr}")
}

async fn write_hint_named(store: &Storage, ns: &str, name: &str, object_key: &str, bytes: &[u8]) {
    let mut hint = Vec::with_capacity(bytes.len() + object_key.len() + 4);
    hint.extend_from_slice(&(object_key.len() as u32).to_le_bytes());
    hint.extend_from_slice(object_key.as_bytes());
    hint.extend_from_slice(bytes);
    if let Err(e) = store.put(&hint_key(ns, name), hint).await {
        tracing::warn!(error=?e, "ANN directory hint not written");
    }
}

/// Fixed key of the newest directory's copy for one vector attribute (protected from GC while the
/// manifest lists a directory for it).
pub(crate) fn hint_key(ns: &str, attr: &str) -> String {
    let hex: String = attr.bytes().map(|b| format!("{b:02x}")).collect();
    format!("ns/{ns}/segments/ann-dir-latest-{hex}.bin")
}

/// A directory fetched before the manifest said which one is current: its object key and bytes.
pub(crate) type Prefetched = (String, bytes::Bytes);

/// Read the hint copy for `attr` (one GET, issued in parallel with the manifest read).
pub(crate) async fn prefetch(store: &Storage, ns: &str, attr: &str) -> Option<Prefetched> {
    let bytes = store.get(&hint_key(ns, attr)).await.ok()?;
    let klen = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
    let key = String::from_utf8(bytes.get(4..4 + klen)?.to_vec()).ok()?;
    Some((key, bytes.slice(4 + klen..)))
}

/// Run `tasks` concurrently, each on its own runtime task (in this query's read context), and
/// collect their results in completion order. The first error cancels the rest.
async fn run_parallel<T: Send + 'static>(
    tasks: Vec<futures::future::BoxFuture<'static, Result<T, ApiError>>>,
) -> Result<Vec<T>, ApiError> {
    if !crate::fan_out() {
        // Busy: poll the tasks from this one (the query's own thread), so they run one after
        // another without handoffs.
        return futures::future::try_join_all(tasks).await;
    }
    let context = read_cache::context();
    let mut set = tokio::task::JoinSet::new();
    for task in tasks {
        let context = context.clone();
        set.spawn(async move {
            match context {
                Some(context) => read_cache::scope(context, task).await,
                None => task.await,
            }
        });
    }
    let mut out = Vec::with_capacity(set.len());
    while let Some(joined) = set.join_next().await {
        out.push(joined.map_err(segment::index_error)??);
    }
    Ok(out)
}

async fn on_worker<T: Send + 'static>(
    fut: impl Future<Output = Result<T, ApiError>> + Send + 'static,
) -> Result<T, ApiError> {
    let Some(context) = read_cache::context().filter(|_| crate::fan_out()) else {
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

/// Start an unfiltered ANN query's scan reads (cluster blobs and code blocks) before the WAL tail
/// is known, so that they overlap with it and land in the block cache: the query's own scan then
/// finds them there. Advisory: a checkpoint with deletions, or one small enough to scan exactly,
/// reads nothing, and a tail that shadows rows only wastes the prefetched bytes.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn prefetch_scan(
    store: &Storage,
    segments: &[bf_wal::Segment],
    dir: &Arc<Directory>,
    attribute: &str,
    layout: Layout,
    vector: &[f32],
    k: usize,
    config: &EngineConfig,
) -> Result<(), ApiError> {
    // Let the query consume arriving scan batches instead of waiting for the whole
    // speculative scan here, ahead of query execution.
    if stream_scan(dir, layout) {
        return Ok(());
    }
    if segments.iter().any(|s| s.live_generation != 0) || !covers(dir, segments, attribute, layout)
    {
        return Ok(());
    }
    let t0 = std::time::Instant::now();
    let vector: Arc<[f32]> = vector.into();
    let distances = {
        let (dir, vector) = (dir.clone(), vector.clone());
        on_worker(async move {
            dir.center_distances_on(&vector, crate::fan_out())
                .map_err(segment::index_error)
        })
        .await?
    };
    crate::ann::stage(t0, "prefetch rank");
    let n = dir.segments().len();
    let (seg_rows, seg_clusters): (Vec<usize>, Vec<usize>) =
        dir.segment_totals().into_iter().unzip();
    let (mut ann_rows, mut matching, mut clusters) = (0usize, 0usize, 0usize);
    let mut fraction = vec![1.0f64; n];
    for (i, seg) in dir.segments().iter().enumerate() {
        if !seg.flat {
            let live = (segments[i].row_count as usize).min(seg_rows[i]);
            ann_rows += seg_rows[i];
            matching += live;
            clusters += seg_clusters[i];
            fraction[i] = live as f64 / seg_rows[i].max(1) as f64;
        }
    }
    let target = k.saturating_mul(config.ann_rerank_factor).min(matching);
    let in_cluster = |n: usize| -> f64 {
        let e = &dir.entries()[n];
        e.rows as f64 * fraction[e.segment]
    };
    let picked = pick_clusters(
        dir,
        &distances,
        &in_cluster,
        &Totals {
            layout,
            ann_rows,
            matching,
            clusters,
            target,
            k,
            filtered: false,
            spread: layout_spread(config, dir, layout, &seg_rows, &|_| false),
        },
        config,
    );
    if crate::ann::trace_stages() {
        eprintln!(
            "PREFETCH matching={matching} k={k} picked={} target={target}",
            picked.len()
        );
    }
    if matching <= k
        || (matching <= EXACT_ROWS.max(picked.len() + target)
            && matching * dir.vector_row_bytes() <= EXACT_BYTES)
    {
        return Ok(());
    }
    let likely = likely_segments(dir, &picked);
    let mut chosen: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for p in picked {
        let e = &dir.entries()[p];
        chosen.entry(e.segment).or_default().push(e.cluster);
    }
    let memoize = layout == Layout::Slabs;
    let vector_key = vector_hash(&vector);
    let reads = chosen.into_iter().map(|(i, cs)| {
        let vector = vector.clone();
        async move {
            let (Some(file), Some(frag)) = (
                segments[i]
                    .files
                    .values()
                    .find(|f| f.key == dir.segments()[i].key),
                segments[i]
                    .files
                    .values()
                    .find(|f| f.key == dir.segments()[i].fragment),
            ) else {
                return;
            };
            let wanted_set: std::collections::HashSet<usize, foldhash::fast::RandomState> =
                cs.iter().copied().collect();
            let seg_entries: Vec<_> = dir
                .segment_entries(i)
                .iter()
                .filter(|e| wanted_set.contains(&e.cluster))
                .collect();
            let Ok(partial) = crate::upgrade::partial(store, dir, i, file).await else {
                return;
            };
            let frag_reader = Reader::new(store, frag);
            let Ok((blobs, codes)) = read_clusters(
                &seg_entries,
                false,
                &frag_reader,
                &partial,
                dir.segments()[i].codes_in_fragment,
                None,
            )
            .await
            else {
                return;
            };
            // Scan now, while other segments' reads are still in flight: the query then needs
            // only the rerank. Only for a lone segment list without deletions, where the query
            // scans with no row mask too.
            if !memoize {
                return;
            }
            let wanted: Vec<usize> = seg_entries.iter().map(|e| e.cluster).collect();
            let mut partial = partial;
            for (cluster, blob) in wanted.iter().zip(&blobs) {
                if partial.install(*cluster, blob).is_err() {
                    return;
                }
            }
            let index = Arc::new(partial.into_index());
            let (scanner, vector, wanted2) = (index.clone(), vector.clone(), wanted.clone());
            let found = on_worker(async move {
                scanner
                    .scan_blocks(&vector, &wanted2, codes, None)
                    .map_err(segment::index_error)
            })
            .await;
            if let Ok(found) = found {
                scan_memo_put(scan_key(&file.key, vector_key, &wanted), (index, found));
            }
        }
    });
    // Flat segments are scanned exactly in the same wave: their blobs and all their vectors.
    let flats = dir
        .segments()
        .iter()
        .enumerate()
        .filter(|(_, s)| s.flat && s.rows > 0)
        .map(|(i, _)| async move {
            let (Some(file), Some(frag)) = (
                segments[i]
                    .files
                    .values()
                    .find(|f| f.key == dir.segments()[i].key),
                segments[i]
                    .files
                    .values()
                    .find(|f| f.key == dir.segments()[i].fragment),
            ) else {
                return;
            };
            let seg_entries: Vec<_> = dir.segment_entries(i).iter().collect();
            let frag_reader = Reader::new(store, frag);
            let vectors = Reader::new(store, file);
            let pieces = vector_pieces(seg_entries.iter().map(|e| e.vectors_range()));
            let Ok(partial) = crate::upgrade::partial(store, dir, i, file).await else {
                return;
            };
            let _ = tokio::join!(
                read_clusters(
                    &seg_entries,
                    true,
                    &frag_reader,
                    &partial,
                    dir.segments()[i].codes_in_fragment,
                    None
                ),
                futures::future::join_all(pieces.into_iter().map(|r| vectors.read(r))),
            );
        });
    // The documents' footers of the likely winners' segments (and of the flat segments, which
    // are scanned exactly) are read in the same wave.
    let warm = likely
        .into_iter()
        .chain(
            dir.segments()
                .iter()
                .enumerate()
                .filter(|(_, s)| s.flat && s.rows > 0)
                .map(|(i, _)| i),
        )
        .map(|i| segment::warm_docs(store, &segments[i]));
    tokio::join!(
        futures::future::join_all(reads),
        futures::future::join_all(flats),
        futures::future::join_all(warm)
    );
    Ok(())
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
            crate::filter::validate(f, &state.schema, false)?;
            return Ok(Some(
                segment::filter_live(store, seg, f, &state.schema).await?,
            ));
        }
        if seg.live_generation == 0 {
            return Ok(None);
        }
        Ok(Some(Arc::new(segment::live(store, seg).await?)))
    }))
    .await
}

/// Pin the immutable predicate along with its derived counts, so its address cannot be
/// reused while a cache entry names it. The fragment key fences vector membership, and the
/// decoded cache scopes this key to the namespace's storage view.
struct ClusterCounts {
    _allowed: Arc<RoaringBitmap>,
    counts: Vec<u32>,
    matching: usize,
}
fn cluster_counts(
    fragment: &str,
    member: &Membership,
    allowed: &Arc<RoaringBitmap>,
    clusters: usize,
) -> Arc<ClusterCounts> {
    let key = format!("ann-counts:{fragment}:{:p}", Arc::as_ptr(allowed));
    if let Some(counts) = read_cache::get(&key, 0) {
        return counts;
    }
    let counts = member.counts(allowed, clusters);
    let matching = counts.iter().map(|n| *n as usize).sum();
    // Charge the pinned predicate even if its protected-cache entry is evicted. Both key
    // copies and the value's containers are included in this conservative reservation.
    let bytes =
        (counts.capacity() * 4 + allowed.serialized_size() * 2 + key.len() * 2 + 256) as u64;
    read_cache::insert(
        &key,
        bytes,
        ClusterCounts {
            _allowed: allowed.clone(),
            counts,
            matching,
        },
    )
}

/// Load (and cache) the row-to-cluster map of every segment that has one. `None` for flat
/// segments and for segments whose fragment predates the map.
pub(crate) async fn memberships(
    store: &Storage,
    segments: &[bf_wal::Segment],
    dir: &Directory,
    attr: &str,
) -> Result<Vec<Option<Arc<Membership>>>, ApiError> {
    futures::future::try_join_all(
        dir.segments()
            .iter()
            .zip(segments)
            .map(|(d, seg)| async move {
                let (Some(range), Some(file)) = (
                    d.membership.clone(),
                    seg.files.values().find(|f| f.key == d.fragment),
                ) else {
                    return crate::upgrade::membership(store, seg, attr).await;
                };
                let key = format!("member:{}", file.key);
                if let Some(m) = read_cache::get(&key, range.end - range.start) {
                    return Ok(Some(m));
                }
                let bytes = Reader::new(store, file)
                    .read(range.clone())
                    .await
                    .map_err(segment::index_error)?;
                let m =
                    Membership::decode(&bytes, d.rows, d.clusters).map_err(segment::index_error)?;
                let resident = m.resident_bytes();
                Ok(Some(read_cache::insert(&key, resident, m)))
            }),
    )
    .await
}

/// Warm the documents' footers in the scan wave only when few segments are probed: the final
/// wave then needs one hop less, at a cost of a few requests. With dozens of segments the
/// footers are most of the extra bytes and requests (measured at 70 segments: slower), and the
/// speculative document read after the scan reads the few that matter.
const WARM_DOCS_SEGMENTS: usize = 8;

fn likely_segments(dir: &Directory, picked: &[usize]) -> BTreeSet<usize> {
    let all: BTreeSet<usize> = picked.iter().map(|n| dir.entries()[*n].segment).collect();
    if all.len() <= WARM_DOCS_SEGMENTS {
        all
    } else {
        BTreeSet::new()
    }
}

/// Row and cluster totals the probe plan is sized from.
struct Totals {
    layout: Layout,
    ann_rows: usize,
    matching: usize,
    clusters: usize,
    target: usize,
    k: usize,
    filtered: bool,
    /// [`independent_spread`] of the layout.
    spread: f64,
}

/// Inverse selectivity, with one additional surviving witness and one standard deviation of
/// the trials needed to find `k + 1` survivors under independent thinning. The margin depends
/// on both selectivity and k; correlated real filters still need measured recall validation.
pub(crate) fn filtered_probe_scale(rows: usize, matching: usize, k: usize) -> f64 {
    if matching >= rows {
        return 1.0;
    }
    let p = matching.max(1) as f64 / rows.max(1) as f64;
    let witnesses = k.saturating_add(1).max(1) as f64;
    (witnesses + (witnesses * (1.0 - p)).sqrt()) / (p * k.max(1) as f64)
}

/// [`independent_spread`] over a directory's searched ANN segments; 1 for slabs (one clustering).
fn layout_spread(
    config: &EngineConfig,
    dir: &Directory,
    layout: Layout,
    seg_rows: &[usize],
    skipped: &dyn Fn(usize) -> bool,
) -> f64 {
    if layout == Layout::Slabs {
        return 1.0;
    }
    independent_spread(
        config,
        dir.segments()
            .iter()
            .zip(seg_rows)
            .enumerate()
            .filter(|(i, (s, _))| !s.flat && !skipped(*i))
            .map(|(_, (_, n))| *n),
    )
}

/// The probe share of segments searched beside a recluster tier (the ones not `skipped`).
///
/// `probe_count` sizes a probe for the rows it is given as if they were the whole namespace:
/// about `ann_nprobe` times the square root of their count. One clustering of all the rows would
/// reach these rows in proportion to their share of them, so a query of the whole namespace
/// probes `sqrt(rows / all rows)` of that here (1 when nothing is skipped). The tier answers for
/// the rest.
fn tier_share(dir: &Directory, seg_rows: &[usize], skipped: &dyn Fn(usize) -> bool) -> f64 {
    let (mut mine, mut all) = (0usize, 0usize);
    for (i, (seg, rows)) in dir.segments().iter().zip(seg_rows).enumerate() {
        if !seg.flat {
            all += rows;
            if !skipped(i) {
                mine += rows;
            }
        }
    }
    if mine == all || all == 0 {
        return 1.0;
    }
    (mine as f64 / all as f64).sqrt()
}

/// Clusters a query probes: `ann_nprobe` per square root of the rows' worth of clusters, more
/// when `thin` (total rows over matching rows) says the clusters are sparse.
fn probe_count(
    config: &EngineConfig,
    ann_rows: usize,
    clusters: usize,
    thin: f64,
    layout: Layout,
) -> usize {
    let budget = if layout == Layout::Slabs {
        bf_vector::tier_probe_budget(config.ann_nprobe, ann_rows, clusters)
    } else {
        config.ann_nprobe as f64 * (clusters as f64 / (ann_rows as f64).sqrt()).max(1.0)
    };
    ((budget * thin).ceil() as usize).min(clusters)
}

/// How many more clusters to probe because the segments were clustered independently.
///
/// `probe_count`'s square-root scaling fits one clustering of all the rows, as a `recluster`'s
/// slabs share. Segments built one by one as data arrives each cluster a sample of the whole
/// space, so a query's neighbors spread over every segment, each segment's clusters are coarser
/// than a global clustering's, and the same probe count finds fewer of them (BEIR NQ, 2.68M rows
/// in 25 segments: recall@10 0.907 against 0.954 reclustered). Such a layout probes
/// `S^exponent` times as many clusters, where `S = (sum of rows)^2 / sum of rows^2` is the
/// effective number of equal segments (tiny tail segments barely count). The exponent is
/// [`SPREAD_EXPONENT`] (0.3): NQ needed 2.5 times the probes to
/// reach the reclustered recall, and 25^0.3 is 2.6. The multiplier is at most
/// [`MAX_SPREAD`] (reached at about 100 segments), so a namespace with many segments (a large
/// load before its recluster, or one still ingesting) reads at most that many times the
/// clusters on a cold query. An explicit `ann_nprobe` is never scaled.
pub(crate) fn independent_spread(
    config: &EngineConfig,
    segment_rows: impl Iterator<Item = usize>,
) -> f64 {
    if config.ann_nprobe_explicit {
        return 1.0;
    }
    let (sum, squares) = segment_rows.fold((0f64, 0f64), |(s, q), n| {
        let n = n as f64;
        (s + n, q + n * n)
    });
    if squares <= 0.0 {
        return 1.0;
    }
    (sum * sum / squares)
        .max(1.0)
        .powf(SPREAD_EXPONENT)
        .min(MAX_SPREAD)
}

/// Exponent of the independent-segment multiplier.
const SPREAD_EXPONENT: f64 = 0.3;

/// Upper bound of [`independent_spread`].
pub(crate) const MAX_SPREAD: f64 = 4.0;

/// The directory entries to probe, nearest first. `in_cluster` gives each entry's expected
/// allowed rows.
fn pick_clusters(
    dir: &Directory,
    distances: &[f32],
    in_cluster: &dyn Fn(usize) -> f64,
    t: &Totals,
    config: &EngineConfig,
) -> Vec<usize> {
    let Totals {
        layout,
        ann_rows,
        matching,
        clusters,
        target,
        k,
        filtered,
        spread,
    } = *t;
    // Each candidate as one integer key, ordered as (distance by `f32::total_cmp`, entry): the
    // partition and sort below compare integers.
    let flat: Vec<bool> = dir.segments().iter().map(|s| s.flat).collect();
    let mut order: Vec<u64> = dir
        .entries()
        .iter()
        .enumerate()
        .filter(|(n, e)| !flat[e.segment] && e.rows > 0 && in_cluster(*n) > 0.0)
        .map(|(n, _)| (u64::from(total_order_key(distances[n])) << 32) | n as u64)
        .collect();
    // Preserve candidate coverage under selective filters, including survivor variance.
    // Selectivity changes the plan automatically; there is no lower-recall configuration.
    let thin = if filtered {
        filtered_probe_scale(ann_rows, matching, k)
    } else {
        ann_rows as f64 / matching.max(1) as f64
    };
    // Reproduce the removed cap only in the private regression test.
    #[cfg(test)]
    let thin = if filtered && config.legacy_three_filter_test {
        (ann_rows as f64 / matching.max(1) as f64).min(3.0)
    } else {
        thin
    };
    let probes = probe_count(config, ann_rows, clusters, thin * spread, layout);
    let base_probes = probe_count(config, ann_rows, clusters, spread, layout);
    // An early stop is reserved for cluster-local masks: most source rows lie in leaves
    // with no allowed member. Distributed and broad masks retain the existing geometric
    // floor; replacing it with a rerank-sized witness pool lost true neighbors.
    let eligible_rows: u64 = if filtered {
        order
            .iter()
            .map(|key| u64::from(dir.entries()[(*key as u32) as usize].rows))
            .sum()
    } else {
        0
    };
    let coverage = if filtered && eligible_rows.saturating_mul(2) < ann_rows as u64 {
        // Exact membership counts determine when this population has actually been
        // covered. Keep two unfiltered probe populations, with the existing witness margin;
        // a smaller population regressed localized NQ queries despite filling rerank.
        let witnesses = thin * matching as f64 / ann_rows.max(1) as f64;
        (2.0 * base_probes as f64 * ann_rows as f64 / clusters.max(1) as f64 * witnesses)
            .ceil()
            .max(target as f64)
            .min(matching as f64)
    } else {
        f64::INFINITY
    };
    #[cfg(test)]
    let coverage = if config.baseline_filter_test || config.legacy_three_filter_test {
        f64::INFINITY
    } else {
        coverage
    };
    // Only the nearest few times `probes` are ever walked: partition them off, and sort the rest
    // only in the unlikely case the walk runs past them.
    let mut sorted = if order.len() > probes.saturating_mul(8).max(256) {
        let head = probes * 2 + 64;
        order.select_nth_unstable(head);
        order[..head].sort_unstable();
        head
    } else {
        order.sort_unstable();
        order.len()
    };
    let (mut count, mut candidates) = (0usize, 0f64);
    let mut picked = Vec::with_capacity(probes);
    let mut at = 0;
    while at < order.len() {
        if at == sorted {
            order[sorted..].sort_unstable();
            sorted = order.len();
        }
        let n = (order[at] & u64::from(u32::MAX)) as usize;
        at += 1;
        picked.push(n);
        count += 1;
        candidates += in_cluster(n);
        if (count >= probes && candidates >= target as f64)
            || (count >= base_probes && candidates >= coverage)
        {
            break;
        }
    }
    if crate::ann::trace_stages() {
        eprintln!(
            "COVERAGE clusters={} allowed={candidates} minimum={probes} target={target}",
            picked.len()
        );
    }
    picked
}

/// Recluster tiers searched for one query, a base and its delta, as one search.
///
/// The base probes as it would alone (its rows are most of the namespace, and its leaves the
/// same as when it was the only tier), and the delta `sqrt(its share of the matching rows)` of
/// what it would alone, the rule for any rows searched beside a tier (see [`tier_share`]): one
/// clustering of all the rows would reach them in about that proportion. (Their leaves don't
/// rank as one list: a delta's leaves hold as many rows as the base's over a sparser sample, so
/// their centers sit nearer every query, and a merged walk spends most of the probe on them.)
/// After the scan they agree on what to rerank and hydrate: the k-th smallest upper bound over
/// all of their candidates (so a tier reranks only candidates that can still make the global top
/// k), and the k-th smallest exact distance (so a tier hydrates only its rows in the global top
/// k). Each agreement is one exchange; a tier that stops early (no rows to rank, or an error)
/// withdraws, and the others go on without it.
pub(crate) struct Joint {
    state: std::sync::Mutex<JointState>,
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct JointState {
    /// Each part's matching rows; `Some(None)` once it withdrew.
    totals: Vec<Option<Option<usize>>>,
    /// Each part's smallest values for each exchange after the scan (see [`Seat::nth_smallest`]).
    rounds: [Vec<Option<Option<Vec<f32>>>>; ROUNDS],
}

/// The exchanges after the scan: upper bounds, then exact distances.
const ROUNDS: usize = 2;
const UPPER_BOUNDS: usize = 0;
const EXACT: usize = 1;

impl Joint {
    /// One seat for each of `parts` tiers.
    pub(crate) fn seats(parts: usize) -> Vec<Seat> {
        let joint = Arc::new(Joint {
            state: std::sync::Mutex::new(JointState {
                totals: vec![None; parts],
                rounds: std::array::from_fn(|_| vec![None; parts]),
            }),
            changed: tokio::sync::Notify::new(),
        });
        (0..parts)
            .map(|part| Seat {
                joint: joint.clone(),
                part,
            })
            .collect()
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, JointState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Wait until `ready` returns a value (it is checked under the lock after every change).
    async fn wait<T>(&self, ready: impl Fn(&JointState) -> Option<T>) -> T {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(out) = ready(&self.lock()) {
                return out;
            }
            notified.await;
        }
    }
}

/// One tier's place in a [`Joint`] plan.
pub(crate) struct Seat {
    joint: Arc<Joint>,
    part: usize,
}

impl Seat {
    /// Leave the probe plan (this tier ranked nothing, or ranked already): the other tiers stop
    /// waiting for its totals and leaves.
    fn leave_plan(&self) {
        let mut state = self.joint.lock();
        let part = self.part;
        state.totals[part].get_or_insert(None);
        drop(state);
        self.joint.changed.notify_waiters();
    }

    /// Leave the plan and every exchange after it: the other tiers stop waiting for this one.
    fn withdraw(&self) {
        let mut state = self.joint.lock();
        let part = self.part;
        state.totals[part].get_or_insert(None);
        for round in &mut state.rounds {
            round[part].get_or_insert(None);
        }
        drop(state);
        self.joint.changed.notify_waiters();
    }

    /// The `n`-th smallest (counting from 1) of all the tiers' `values` in exchange `round`, or
    /// infinity when they hold fewer than `n`. Each tier sends only its own `n` smallest.
    async fn nth_smallest(&self, round: usize, mut values: Vec<f32>, n: usize) -> f32 {
        if n == 0 {
            return f32::INFINITY;
        }
        if values.len() > n {
            values.select_nth_unstable_by(n - 1, f32::total_cmp);
            values.truncate(n);
        }
        {
            let mut state = self.joint.lock();
            let slot = &mut state.rounds[round][self.part];
            if slot.is_some() {
                // Withdrawn: this tier is on its own.
                values.sort_unstable_by(f32::total_cmp);
                return values.get(n - 1).copied().unwrap_or(f32::INFINITY);
            }
            *slot = Some(Some(values));
        }
        self.joint.changed.notify_waiters();
        self.joint
            .wait(|state| {
                let all: Option<Vec<&Option<Vec<f32>>>> =
                    state.rounds[round].iter().map(Option::as_ref).collect();
                let mut merged: Vec<f32> = all?.into_iter().flatten().flatten().copied().collect();
                if merged.len() < n {
                    return Some(f32::INFINITY);
                }
                merged.select_nth_unstable_by(n - 1, f32::total_cmp);
                Some(merged[n - 1])
            })
            .await
    }

    /// This tier's probe beside the others (see [`Joint`]): the base probes as it would alone,
    /// and a delta `sqrt(its share of the matching rows)` of what it would alone. Returns the
    /// entries and that scale, which also scales the tier's rerank cap.
    async fn pick(
        &self,
        dir: &Directory,
        distances: &[f32],
        in_cluster: &(dyn Fn(usize) -> f64 + Sync),
        t: &Totals,
        config: &EngineConfig,
    ) -> (Vec<usize>, f64) {
        {
            let mut state = self.joint.lock();
            state.totals[self.part] = Some(Some(t.matching));
        }
        self.joint.changed.notify_waiters();
        let matching: usize = self
            .joint
            .wait(|state| {
                let all: Option<Vec<&Option<usize>>> =
                    state.totals.iter().map(Option::as_ref).collect();
                Some(all?.into_iter().flatten().sum())
            })
            .await;
        if self.part == 0 {
            return (pick_clusters(dir, distances, in_cluster, t, config), 1.0);
        }
        let scale = (t.matching as f64 / matching.max(1) as f64).sqrt();
        let mine = Totals {
            spread: t.spread * scale,
            target: (t.target as f64 * scale).ceil() as usize,
            ..*t
        };
        (
            pick_clusters(dir, distances, in_cluster, &mine, config),
            scale,
        )
    }
}

impl Drop for Seat {
    fn drop(&mut self) {
        self.withdraw();
    }
}

/// `x`'s bits mapped so that unsigned order is `f32::total_cmp` order.
fn total_order_key(x: f32) -> u32 {
    let bits = x.to_bits();
    if bits >> 31 == 1 {
        !bits
    } else {
        bits | 1 << 31
    }
}

type Real<'a> = (
    &'a NamespaceState,
    &'a [bf_wal::SlabCover],
    &'a crate::lineage::Placement,
);
/// Large slab searches can amortize parallel read and scan planning; small and busy
/// workloads retain their per-slab path.
fn stream_scan(dir: &Directory, layout: Layout) -> bool {
    crate::fan_out()
        && read_cache::admitting()
        && layout == Layout::Slabs
        && dir.segments().iter().map(|s| s.rows).sum::<usize>() >= 100_000
}

struct Probed {
    segment: usize,
    index: Arc<Index<Reader>>,
    clusters: Vec<usize>,
    exact: bool,
}

/// Answer an unfiltered ANN query from the directory. The caller has checked that the
/// directory covers `state.segments` and that no tail ID shadows a segment row. Segments marked
/// in `skip` (rows a recluster tier answers for) are left out entirely: of the probe plan, its
/// totals, and every scan.
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
    _attribute: &str,
    members: Option<Vec<Option<Arc<Membership>>>>,
    layout: Layout,
    real: Option<Real<'_>>,
    skip: &[bool],
    joint: Option<&Seat>,
    warm: Option<Arc<crate::warm_ann::WarmTier>>,
) -> Result<(BTreeMap<Id, f64>, BTreeMap<Id, Document>), ApiError> {
    // With slabs, `state` lists the slabs and `real` the segments that hold the documents: a
    // slab row is a global row number, mapped back by the covered segments' bases to a covered
    // segment, and from there (through lineage, if it was merged) to the live segment holding it.
    // A row a merge dropped maps to nothing (the tier's mask leaves such rows out).
    let docs_state = real.map_or(state, |r| r.0);
    let to_real = |i: usize, row: u32| -> Option<(usize, u32)> {
        match real {
            None => Some((i, row)),
            Some((_, covers, at)) => at.real(covers, row),
        }
    };
    let skipped = |i: usize| skip.get(i).copied().unwrap_or(false);
    let filtered = members.is_some();
    let members = members.unwrap_or_default();
    let t0 = std::time::Instant::now();
    if let Some(warm) = &warm
        && layout == Layout::Slabs && real.is_some() && joint.is_none() && !filtered
        && !skip.iter().any(|skip| *skip) && lives.iter().all(Option::is_none)
        // Small populations and very large k use the normal exact-all plan.
        && warm.rows > EXACT_ROWS.max(dir.entries().len().saturating_add(k.saturating_mul(config.ann_rerank_factor)))
        && let Some(found) = warm.search(vector, config.ann_nprobe, config.ann_rerank_factor, k).map_err(segment::index_error)?
    {
        let winners = found.winners;
        let scanned = found.scanned_rows;
        metrics::counter!("blowfish_ann_compiled_queries_total").increment(1);
        if let Some(context) = read_cache::context() {
            context
                .ann_rows
                .fetch_add(scanned as u64, std::sync::atomic::Ordering::Relaxed);
            context.stats.record_memory_hit(
                (scanned as u64).saturating_mul((dir.dims().next_power_of_two() / 8 + 16) as u64),
            );
        }
        if crate::ann::trace_stages() {
            eprintln!("COMPILED rows={scanned} winners={}", winners.len());
        }
        return hydrate(store, state, real, columns, k, winners, t0).await;
    }
    let vector: Arc<[f32]> = vector.into();
    let width = dir.vector_row_bytes();
    // One rotation serves the ranking and every segment's scan (they share one vector space).
    let rotated = Arc::new(dir.rotate_query(&vector).map_err(segment::index_error)?);
    let distances = {
        let (dir, rotated, skip) = (dir.clone(), rotated.clone(), skip.to_vec());
        on_worker(async move {
            // Segments a recluster tier answers for are not ranked at all.
            dir.center_distances_rotated_except(&rotated, crate::fan_out(), &skip)
                .map_err(segment::index_error)
        })
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
    let (seg_rows, seg_clusters): (Vec<usize>, Vec<usize>) =
        dir.segment_totals().into_iter().unzip();
    // Filtered: allowed rows in each cluster, exactly, from the row-to-cluster maps.
    let counts: Vec<Option<Arc<ClusterCounts>>> = if filtered {
        dir.segments()
            .iter()
            .enumerate()
            .map(
                |(i, seg)| match (members.get(i).and_then(Option::as_ref), &lives[i]) {
                    (Some(m), Some(allowed)) if !seg.flat => {
                        Some(cluster_counts(&seg.fragment, m, allowed, seg.clusters))
                    }
                    _ => None,
                },
            )
            .collect()
    } else {
        Vec::new()
    };
    let mut ann_rows = 0usize;
    let mut matching = 0usize;
    let mut clusters = 0usize;
    // Expected allowed rows per cluster of each segment: the segment's allowed fraction.
    let mut fraction = vec![1.0f64; dir.segments().len()];
    for (i, seg) in dir.segments().iter().enumerate() {
        if skipped(i) {
            fraction[i] = 0.0;
        } else if !seg.flat {
            ann_rows += seg_rows[i];
            // Dictionary masks also contain rows whose vector is null. Count actual
            // vector members, rather than treating those rows as surviving witnesses.
            let selected = counts
                .get(i)
                .and_then(Option::as_ref)
                .map_or_else(|| live_rows(i).min(seg_rows[i]), |counts| counts.matching);
            matching += selected;
            clusters += seg_clusters[i];
            fraction[i] = selected as f64 / seg_rows[i].max(1) as f64;
        }
    }
    let target = k.saturating_mul(config.ann_rerank_factor).min(matching);
    // The most candidates reranked (a delta beside its base reranks fewer: see `Seat::pick`).
    let mut rerank_cap = target;
    let in_cluster = |n: usize| -> f64 {
        let e = &entries[n];
        match counts.get(e.segment).and_then(Option::as_ref) {
            Some(c) => f64::from(c.counts[e.cluster]),
            None => e.rows as f64 * fraction[e.segment],
        }
    };
    // Each probed segment's entries (indices into `entries`).
    let mut chosen: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut exact_all = false;
    let mut likely = BTreeSet::new();
    // Segments whose few allowed rows are fetched and ranked exactly, without codes or blobs.
    let mut exact_rows = BTreeSet::new();
    if matching > 0 {
        if filtered && matching <= config.exact_fetch_rows.max(target) {
            for (i, seg) in dir.segments().iter().enumerate() {
                if !seg.flat && !skipped(i) && live_rows(i) > 0 {
                    exact_rows.insert(i);
                }
            }
        } else {
            let totals = Totals {
                layout,
                ann_rows,
                matching,
                clusters,
                target,
                k,
                filtered,
                spread: layout_spread(config, dir.as_ref(), layout, &seg_rows, &skipped)
                    * tier_share(dir.as_ref(), &seg_rows, &skipped),
            };
            let picked = match joint {
                Some(seat) => {
                    let (picked, scale) = seat
                        .pick(dir.as_ref(), &distances, &in_cluster, &totals, config)
                        .await;
                    rerank_cap = ((target as f64 * scale).ceil() as usize).max(k);
                    picked
                }
                None => pick_clusters(dir.as_ref(), &distances, &in_cluster, &totals, config),
            };
            let count = picked.len();
            likely = likely_segments(dir.as_ref(), &picked);
            if !filtered
                && (matching <= k
                    || (matching <= EXACT_ROWS.max(count + target)
                        && matching * width <= EXACT_BYTES))
            {
                exact_all = true;
            } else {
                for n in picked {
                    chosen.entry(entries[n].segment).or_default().push(n);
                }
            }
        }
    }
    // A tier that ranked nothing leaves the probe plan to the others.
    if let Some(seat) = joint {
        seat.leave_plan();
    }
    // Flat segments, and every ANN segment when the namespace is small, are scanned exactly.
    let mut exact_segments = BTreeSet::new();
    for (i, seg) in dir.segments().iter().enumerate() {
        if (seg.flat || exact_all) && !skipped(i) {
            exact_segments.insert(i);
            chosen.insert(
                i,
                (0..entries.len())
                    .filter(|n| entries[*n].segment == i)
                    .collect(),
            );
        }
    }
    if let Some(context) = read_cache::context() {
        let rows: u64 = chosen
            .values()
            .flatten()
            .map(|&n| u64::from(entries[n].rows))
            .sum::<u64>()
            + exact_rows.iter().map(|&i| live_rows(i) as u64).sum::<u64>();
        context
            .ann_rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
        if crate::ann::trace_stages() {
            eprintln!("PLAN rows={rows}");
        }
    }
    crate::ann::stage(t0, "plan");

    // Wave 3: probed codes and cluster blobs for each chosen segment, in parallel. Docs
    // footers for the chosen segments are warmed in the same wave.
    let mut tasks: Vec<futures::future::BoxFuture<'static, Result<(Probed, Found), ApiError>>> =
        Vec::new();
    for &i in &exact_rows {
        let Some(member) = members.get(i).cloned().flatten() else {
            return Err(segment::index_error("segment lost its membership map"));
        };
        let live = lives[i].clone().unwrap_or_default();
        let file = state.segments[i]
            .files
            .values()
            .find(|f| f.key == dir.segments()[i].key)
            .cloned()
            .ok_or_else(|| segment::index_error("segment lost its ANN index"))?;
        let dir = dir.clone();
        let (vector, segment_meta, store) =
            (vector.clone(), state.segments[i].clone(), store.clone());
        tasks.push(
            async move {
                let partial = crate::upgrade::partial(&store, &dir, i, &file).await?;
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
    let vector_key = vector_hash(&vector);
    // A completed slab (or streamed batch) with k witnesses supplies a valid global cut to scans
    // that have not run yet. Read planning stays concurrent; busy warm queries scan on their own
    // thread.
    let scan_upper = Arc::new(std::sync::atomic::AtomicU32::new(f32::INFINITY.to_bits()));
    // Native mappings are resident even when the block cache has no retention budget.
    // Otherwise, sample a selected code range per slab without starting I/O.
    let parallel_slab_scan = stream_scan(&dir, layout) && !filtered && joint.is_none();
    let resident = parallel_slab_scan
        && (store.has_mapped_reads()
            || chosen.iter().all(|(&i, cs)| {
                let seg = &dir.segments()[i];
                let Some(file) = state.segments[i].files.values().find(|f| f.key == seg.key) else {
                    return false;
                };
                let Ok(reader) = store.range_reader_with_length(&file.key, file.size, true) else {
                    return false;
                };
                cs.first()
                    .is_none_or(|&n| reader.is_memory_resident(entries[n].codes_range()))
            }));
    // Streaming seeds the raw cache before its scan readers start. With no retention budget,
    // completed seed reads are discarded, and fast backends fetch them again for the scans.
    let streaming = store.cache_limits().0 > 0 && parallel_slab_scan && !resident;
    let mut bulk = tokio::task::JoinSet::new();
    if streaming {
        let mut order: Vec<_> = chosen
            .iter()
            .filter(|(i, _)| !exact_segments.contains(i))
            .collect();
        order.sort_unstable_by(|a, b| distances[a.1[0]].total_cmp(&distances[b.1[0]]));
        let mut reservations = Vec::new();
        for (&i, cs) in order {
            let seg = &dir.segments()[i];
            let files = &state.segments[i].files;
            let index = files
                .values()
                .find(|f| f.key == seg.key)
                .ok_or_else(|| segment::index_error("segment lost its ANN index"))?;
            let fragment = files
                .values()
                .find(|f| f.key == seg.fragment)
                .ok_or_else(|| segment::index_error("segment lost its ANN fragment"))?;
            let mut blobs: Vec<_> = cs.iter().map(|&n| entries[n].blob_range()).collect();
            let codes: Vec<_> = cs.iter().map(|&n| entries[n].codes_range()).collect();
            let reads = if seg.codes_in_fragment {
                blobs.extend(codes);
                vec![(fragment, blobs)]
            } else {
                vec![(fragment, blobs), (index, codes)]
            };
            for (file, ranges) in reads {
                let reader = store
                    .range_reader_with_length(&file.key, file.size, true)
                    .map_err(segment::index_error)?;
                let stats = read_cache::stats();
                let (ready, reserved) = tokio::sync::oneshot::channel();
                reservations.push(reserved);
                bulk.spawn(async move {
                    let _ = reader.read_batch_notifying(&ranges, &stats, ready).await;
                });
            }
        }
        for ready in reservations {
            let _ = ready.await;
        }
    }
    // A query may put most of its probed rows in one coarse slab. Bound resident CPU
    // batches by the query's work and available share of workers, so that slab can use
    // idle workers without repeating a lone query's fine-grained plan under concurrency.
    // Busy resident queries retain their per-slab scan; streamed reads retain small batches.
    let resident_batch_rows = (resident && crate::fan_out()).then(|| {
        let rows: usize = chosen
            .values()
            .flatten()
            .map(|&n| entries[n].rows as usize)
            .sum();
        let queries = crate::QUERIES_IN_FLIGHT
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(1);
        let workers = config.search_concurrency.max(1).div_ceil(queries);
        let row_bytes = dir.dims().next_power_of_two() / 8 + 16;
        rows.div_ceil(workers.saturating_mul(2))
            .max((512 * 1024 / row_bytes).max(1))
    });
    let mut batches = Vec::new();
    for (&i, cs) in &chosen {
        if streaming && !exact_segments.contains(&i) {
            let mut ranked = cs.clone();
            ranked.sort_unstable_by(|&a, &b| distances[a].total_cmp(&distances[b]));
            for chunk in ranked.chunks(4) {
                batches.push((i, chunk.to_vec()));
            }
        } else if let Some(max_rows) = resident_batch_rows
            && !exact_segments.contains(&i)
        {
            let (mut start, mut rows) = (0, 0);
            for (end, &n) in cs.iter().enumerate() {
                let next = entries[n].rows as usize;
                if end > start && rows + next > max_rows {
                    batches.push((i, cs[start..end].to_vec()));
                    start = end;
                    rows = 0;
                }
                rows += next;
            }
            if start < cs.len() {
                batches.push((i, cs[start..].to_vec()));
            }
        } else {
            batches.push((i, cs.clone()));
        }
    }
    if streaming {
        batches.sort_unstable_by(|a, b| {
            let nearest = |ns: &[usize]| ns.first().map_or(f32::INFINITY, |&n| distances[n]);
            nearest(&a.1).total_cmp(&nearest(&b.1))
        });
    }
    for (i, cs) in batches {
        let scan_upper = scan_upper.clone();
        let (dir, vector, rotated) = (dir.clone(), vector.clone(), rotated.clone());
        let exact = exact_segments.contains(&i);
        let flat = dir.segments()[i].flat;
        let file = state.segments[i]
            .files
            .values()
            .find(|f| f.key == dir.segments()[i].key)
            .cloned()
            .ok_or_else(|| segment::index_error("segment lost its ANN index"))?;
        let reader = Reader::new(store, &file);
        let object_key = file.key.clone();
        let memo_hit = layout == Layout::Slabs && !exact;
        let frag_file = state.segments[i]
            .files
            .values()
            .find(|f| f.key == dir.segments()[i].fragment)
            .cloned()
            .ok_or_else(|| segment::index_error("segment lost its ANN fragment"))?;
        let warm_docs = exact || likely.contains(&i);
        let (segment_docs, docs_store) = (state.segments[i].clone(), store.clone());
        let dir_reader = Reader::new(store, &frag_file);
        let live = lives[i].clone();
        // In directory order (as the segment lists them), which keeps nearby clusters' reads
        // together.
        let mut ns = cs.clone();
        ns.sort_unstable();
        let seg_entries: Vec<_> = ns.iter().map(|n| entries[*n].clone()).collect();
        let trace_t0 = t0;
        let warm = warm.clone();
        tasks.push(
            async move {
                let task_start = trace_t0.elapsed().as_micros();
                // The prefetch may have scanned this segment's clusters already (see
                // `scan_memo_put`): that is exact only without a row mask.
                if memo_hit && live.is_none() {
                    let wanted: Vec<usize> = seg_entries.iter().map(|e| e.cluster).collect();
                    if let Some((index, found)) =
                        scan_memo_take(&scan_key(&object_key, vector_key, &wanted))
                    {
                        let probed = Probed {
                            segment: i,
                            index,
                            clusters: wanted,
                            exact: false,
                        };
                        return Ok((probed, (Vec::new(), found)));
                    }
                }
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
                    let pieces = vector_pieces(prefetch_ranges);
                    // Best effort: a failure here only means the scan reads the range itself.
                    let _ = futures::future::join_all(
                        pieces.into_iter().map(|r| prefetch_reader.read(r)),
                    )
                    .await;
                    Ok::<(), ApiError>(())
                };
                let mut partial = crate::upgrade::partial(&docs_store,&dir,i,&file).await?;
                let wanted: Vec<usize> = seg_entries.iter().map(|e| e.cluster).collect();
                let entry_refs: Vec<_> = seg_entries.iter().collect();
                // Clusters decoded by earlier queries skip their blob read and decode.
                let clusters = read_cache::context().map(|c| c.cache.clusters(&object_key));
                let decoded: Vec<Option<bf_vector::DecodedCluster>> = wanted
                    .iter()
                    .map(|&c| {
                        let (cache, key) = clusters.as_ref()?;
                        cache.get(&(key.clone(), c as u32))
                    })
                    .collect();
                let cached: Vec<bool> = decoded.iter().map(Option::is_some).collect();
                // Documents' footers are warmed only for the segments holding likely winners (and
                // for exactly scanned ones); the speculative document read reads the rest.
                let (read, (), ()) = tokio::try_join!(
                    read_clusters(&entry_refs, exact, &dir_reader, &partial, dir.segments()[i].codes_in_fragment, Some(&cached)),
                    prefetch,
                    async {
                        if warm_docs {
                            segment::warm_docs(&docs_store, &segment_docs).await;
                        }
                        Ok(())
                    }
                )?;
                let (blobs, codes) = read;
                let read_done = trace_t0.elapsed().as_micros();
                let mut blobs = blobs.iter();
                for (position, (cluster, known)) in wanted.iter().zip(decoded).enumerate() {
                    let d = match known {
                        Some(d) => d,
                        None => {
                            let blob = blobs
                                .next()
                                .ok_or_else(|| segment::index_error("missing cluster blob"))?;
                            let d = partial.decode(blob).map_err(segment::index_error)?;
                            if let Some((cache, key)) = &clusters {
                                cache.insert((key.clone(), *cluster as u32), d.clone());
                            }
                            d
                        }
                    };
                    if let Some(warm) = &warm && let Some(code) = codes.get(position) {
                        // Best effort: unsupported layouts keep their ordinary reader. Validation
                        // here covers the same immutable bytes the scan would check below.
                        let _ = warm.codes.admit(i, *cluster, &d, code);
                    }
                    partial
                        .install_decoded(*cluster, &d)
                        .map_err(segment::index_error)?;
                }
                let index = Arc::new(partial.into_index());
                let probed = Probed {
                    segment: i,
                    index: index.clone(),
                    clusters: wanted.clone(),
                    exact,
                };
                let install_done = trace_t0.elapsed().as_micros();
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
                    // The rerank below keeps only candidates whose lower bound is within the
                    // k-th smallest upper bound, so the scan drops the rest as it goes.
                    let found = index
                        .scan_blocks_bounded_rotated(
                            &rotated,
                            &wanted,
                            codes,
                            live.as_deref(),
                            k,
                            f32::from_bits(scan_upper.load(std::sync::atomic::Ordering::Relaxed)),
                        )
                        .map_err(segment::index_error)?;
                    if k > 0 && found.len() >= k {
                        let mut upper: Vec<f32> = found.iter().map(|c| c.distance + c.error).collect();
                        upper.select_nth_unstable_by(k - 1, f32::total_cmp);
                        // Distances and errors are non-negative, so their float bits have
                        // the same order as the values. Every published cut has k witnesses.
                        scan_upper.fetch_min(upper[k - 1].to_bits(), std::sync::atomic::Ordering::Relaxed);
                    }
                    Ok((Vec::new(), found))
                })
                .await?;
                if crate::ann::trace_stages() {
                    eprintln!(
                        "TASK seg={i} clusters={} start={task_start} read={read_done} install={install_done} scan={}",
                        probed.clusters.len(),
                        trace_t0.elapsed().as_micros()
                    );
                }
                Ok::<_, ApiError>((probed, found))
            }
            .boxed(),
        );
    }
    // Every segment's reads start at once: a second batch of segments would add a whole wave.
    // Each runs as its own task: on a warm query the reads are cache hits that finish without
    // yielding, so tasks polled from this one would run their copies and decodes one after the
    // other on a single thread.
    crate::ann::stage(t0, "tasks built");
    let limit = config.search_concurrency.max(1) * 2;
    let mut early = tokio::task::JoinSet::new();
    let results: Vec<(Probed, Found)> = if streaming {
        let context = read_cache::context();
        let mut scans = tokio::task::JoinSet::new();
        let count = tasks.len();
        for task in tasks {
            let context = context.clone();
            scans.spawn(async move {
                match context {
                    Some(context) => read_cache::scope(context, task).await,
                    None => task.await,
                }
            });
        }
        let mut results: Vec<(Probed, Found)> = Vec::new();
        let mut launched = false;
        while let Some(result) = scans.join_next().await {
            results.push(result.map_err(segment::index_error)??);
            if !launched && results.len() * 5 >= count * 2 && !scans.is_empty() {
                launched = true;
                let mut leading: Vec<_> = results
                    .iter()
                    .enumerate()
                    .flat_map(|(n, (_, (_, found)))| found.iter().map(move |c| (n, c)))
                    .collect();
                leading.sort_unstable_by(|a, b| a.1.distance.total_cmp(&b.1.distance));
                leading.truncate(k.saturating_mul(8).min(80));
                let mut locations: BTreeMap<usize, Vec<(u32, u32)>> = BTreeMap::new();
                let mut rows: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
                let docs_limit = k.saturating_mul(3).min(30);
                for (rank, (n, c)) in leading.into_iter().enumerate() {
                    locations.entry(n).or_default().push((c.cluster, c.ordinal));
                    if rank < docs_limit
                        && let Some((i, row)) = to_real(results[n].0.segment, c.row)
                    {
                        rows.entry(i).or_default().insert(row);
                    }
                }
                for (n, locations) in locations {
                    let (index, vector, context) =
                        (results[n].0.index.clone(), vector.clone(), context.clone());
                    early.spawn(async move {
                        let work = async move {
                            let _ = index.rerank_candidates(&vector, k, locations).await;
                        };
                        match context {
                            Some(context) => read_cache::scope(context, work).await,
                            None => work.await,
                        }
                    });
                }
                for (i, rows) in rows {
                    let (store, segment, columns, context) = (
                        store.clone(),
                        docs_state.segments[i].clone(),
                        columns.clone(),
                        context.clone(),
                    );
                    early.spawn(async move {
                        let work = async move {
                            let _ =
                                segment::projected_rows(&store, &segment, &rows, Some(&columns))
                                    .await;
                        };
                        match context {
                            Some(context) => read_cache::scope(context, work).await,
                            None => work.await,
                        }
                    });
                }
                crate::ann::stage(t0, "early rerank");
            }
        }
        // These reads only populate the immutable cache. Keep them alive through the final
        // rerank so its misses share any requests still in flight.
        results
    } else {
        run_parallel(tasks).await?
    };
    crate::ann::stage(t0, "scan");
    let mut winners: Vec<(f32, usize, u32)> = Vec::new(); // (exact distance, segment, row)
    let mut pool: Vec<(usize, Candidate)> = Vec::new();
    let mut indexes: BTreeMap<usize, Arc<Index<Reader>>> = BTreeMap::new();
    let mut sources = Vec::new();
    for (n, (probed, (exact_hits, found))) in results.into_iter().enumerate() {
        let _ = (&probed.clusters, probed.exact);
        winners.extend(exact_hits.into_iter().map(|(d, r)| (d, probed.segment, r)));
        pool.extend(found.into_iter().map(|c| (n, c)));
        sources.push(probed.segment);
        indexes.insert(n, probed.index);
    }
    let cmp = |a: &(usize, Candidate), b: &(usize, Candidate)| {
        a.1.distance
            .total_cmp(&b.1.distance)
            .then(sources[a.0].cmp(&sources[b.0]))
            .then(a.1.row.cmp(&b.1.row))
    };
    // Error-bounded reranking: only candidates whose lower bound can still beat the k-th best
    // upper bound need their exact distance. `ann_rerank_factor` stays as a cap.
    // Beside other tiers, the bound is the k-th best over all of their candidates (exactly
    // scanned rows count with their exact distance).
    if let Some(seat) = joint {
        let upper: Vec<f32> = pool
            .iter()
            .map(|(_, c)| c.distance + c.error)
            .chain(winners.iter().map(|w| w.0))
            .collect();
        let threshold = seat.nth_smallest(UPPER_BOUNDS, upper, k).await;
        pool.retain(|(_, c)| c.distance - c.error <= threshold);
    } else if k > 0 && pool.len() > k {
        let mut upper: Vec<f32> = pool.iter().map(|(_, c)| c.distance + c.error).collect();
        upper.select_nth_unstable_by(k - 1, f32::total_cmp);
        let threshold = upper[k - 1];
        pool.retain(|(_, c)| c.distance - c.error <= threshold);
    }
    if pool.len() > rerank_cap {
        pool.select_nth_unstable_by(rerank_cap, cmp);
        pool.truncate(rerank_cap);
    }
    if crate::ann::trace_stages() {
        eprintln!("RERANK pool={} cap={rerank_cap}", pool.len());
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
            if let Some((i, row)) = to_real(sources[*i], c.row) {
                speculative.entry(i).or_default().insert(row);
            }
        }
    }
    // Exactly scanned segments already know their best rows.
    winners.sort_by(|a, b| a.0.total_cmp(&b.0));
    for (_, i, row) in winners.iter().take(k * 2) {
        if let Some((i, row)) = to_real(*i, *row) {
            speculative.entry(i).or_default().insert(row);
        }
    }
    let mut by_segment: BTreeMap<usize, Vec<(u32, u32)>> = BTreeMap::new();
    for (i, c) in pool {
        by_segment
            .entry(i)
            .or_default()
            .push((c.cluster, c.ordinal));
    }
    // A rerank is a few small reads and a little arithmetic. Running it on a worker would hold
    // one of the few permits through the read latency, and the segments would queue for them.
    let reranks = by_segment.into_iter().map(|(i, locations)| {
        let (index, vector, warm) = (indexes[&i].clone(), vector.clone(), warm.clone());
        let i = sources[i];
        async move {
            let hits = index
                .rerank_candidates_recorded(
                    &vector,
                    k,
                    locations,
                    warm.as_deref().map(|w| w as &dyn bf_vector::VectorRecorder),
                )
                .await
                .map_err(segment::index_error)?;
            Ok::<_, ApiError>(hits.into_iter().map(|(r, d)| (d, i, r)).collect::<Vec<_>>())
        }
    });
    let rerank_limit = reranks.len().max(limit);
    let prefetch = futures::future::join_all(speculative.iter().map(|(&i, rows)| async move {
        let _ = segment::projected_rows(store, &docs_state.segments[i], rows, Some(columns)).await;
    }));
    let (reranked, _) = tokio::join!(
        futures::stream::iter(reranks)
            .buffer_unordered(rerank_limit)
            .try_collect::<Vec<Vec<(f32, usize, u32)>>>(),
        prefetch
    );
    winners.extend(reranked?.into_iter().flatten());
    crate::ann::stage(t0, "rerank");
    // Beside other tiers, only this tier's rows in the global top k are hydrated (with every row
    // tied with the k-th distance, as below).
    if let Some(seat) = joint {
        let edge = seat
            .nth_smallest(EXACT, winners.iter().map(|w| w.0).collect(), k)
            .await;
        winners.retain(|w| w.0 <= edge);
        seat.withdraw();
    }

    hydrate(store, state, real, columns, k, winners, t0).await
}

#[allow(clippy::too_many_arguments)]
async fn hydrate(
    store: &Storage,
    state: &NamespaceState,
    real: Option<Real<'_>>,
    columns: &BTreeSet<String>,
    k: usize,
    mut winners: Vec<(f32, usize, u32)>,
    t0: std::time::Instant,
) -> Result<(BTreeMap<Id, f64>, BTreeMap<Id, Document>), ApiError> {
    let docs_state = real.map_or(state, |r| r.0);
    let to_real = |i: usize, row: u32| -> Option<(usize, u32)> {
        match real {
            None => Some((i, row)),
            Some((_, covers, at)) => at.real(covers, row),
        }
    };
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
    let winners: Vec<(f32, usize, u32)> = winners
        .into_iter()
        .map(|(d, i, row)| {
            let (i, row) = to_real(i, row)
                .ok_or_else(|| segment::index_error("tier row outside its lineage"))?;
            Ok((d, i, row))
        })
        .collect::<Result<_, ApiError>>()?;
    let mut rows_by_segment: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
    for (_, i, row) in &winners {
        rows_by_segment.entry(*i).or_default().insert(*row);
    }
    let mut hydrated: BTreeMap<(usize, u32), Document> = BTreeMap::new();
    for (i, part) in
        futures::future::try_join_all(rows_by_segment.iter().map(|(&i, rows)| async move {
            Ok::<_, ApiError>((
                i,
                segment::projected_rows(store, &docs_state.segments[i], rows, Some(columns))
                    .await?,
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

fn env_number<T: std::str::FromStr + Copy>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A paged query reads cells until they hold this many times the clusters it will probe.
fn page_margin() -> f64 {
    static V: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_number("BF_ANN_PAGE_MARGIN", 16.0f64).max(1.0))
}

/// How much a cell's spread pulls its rank toward the query (see `PagedDirectory::select_cells`).
fn page_kappa() -> f32 {
    static V: std::sync::OnceLock<f32> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_number("BF_ANN_PAGE_KAPPA", 0.5f32))
}

/// Pages this close together are read as one request, and one request reads at most this much.
const PAGE_GAP: u64 = 64 << 10;
const PAGE_MERGE_MAX: u64 = 4 << 20;

/// Build a paged copy of a slab directory when it lists at least `BF_ANN_PAGED_MIN_ENTRIES`
/// clusters (default 100,000), with about `BF_ANN_PAGE_ENTRIES` entries per page (default 24).
/// Writes the object and the hint (the header). Returns `None` for a small directory.
pub(crate) async fn build_paged(
    store: &Storage,
    ns: &str,
    attr: &str,
    dir: Arc<Directory>,
    slabs: &[bf_wal::Segment],
) -> Result<Option<bf_wal::SegmentFile>, ApiError> {
    let entries = dir.entries().len();
    if entries < env_number("BF_ANN_PAGED_MIN_ENTRIES", 100_000usize) {
        return Ok(None);
    }
    let per_cell = env_number("BF_ANN_PAGE_ENTRIES", 24usize).max(1);
    let files: Vec<[(u64, u64); 2]> = slabs
        .iter()
        .zip(dir.segments())
        .map(|(s, d)| {
            let size = |k: &str| {
                s.files
                    .values()
                    .find(|f| f.key == k)
                    .map_or((0, 0), |f| (f.size, f.metadata_len))
            };
            [size(&d.key), size(&d.fragment)]
        })
        .collect();
    let (bytes, header_len) =
        tokio::task::spawn_blocking(move || dir.build_paged(per_cell, 7, &files))
            .await
            .map_err(segment::index_error)?
            .map_err(segment::index_error)?;
    let file = segment::put_file(
        store,
        format!("ns/{ns}/segments/ann-dir3p-{}.bfd", uuid::Uuid::new_v4()),
        bytes.clone(),
        header_len,
    )
    .await?;
    write_hint_named(
        store,
        ns,
        &paged_hint_name(attr),
        &file.key,
        &bytes[..header_len as usize],
    )
    .await;
    Ok(Some(file))
}

/// Load a paged directory's header: from the decoded cache, the hint read in the manifest's
/// wave, or one ranged read.
pub(crate) async fn load_paged(
    store: &Storage,
    file: &bf_wal::SegmentFile,
    prefetched: Option<&Prefetched>,
) -> Result<Arc<PagedDirectory>, ApiError> {
    if let Some(dir) = read_cache::get(&file.key, file.metadata_len) {
        return Ok(dir);
    }
    if let Some((key, bytes)) = prefetched
        && *key == file.key
        && bytes.len() as u64 >= file.metadata_len
        && let Ok(dir) = PagedDirectory::decode(&bytes[..file.metadata_len as usize])
    {
        let resident = dir.resident_bytes();
        return Ok(read_cache::insert(&file.key, resident, dir));
    }
    if file.metadata_len > file.size {
        return Err(segment::index_error("directory header exceeds object"));
    }
    let bytes = Reader::new(store, file)
        .read(0..file.metadata_len)
        .await
        .map_err(segment::index_error)?;
    let dir = PagedDirectory::decode(&bytes).map_err(segment::index_error)?;
    let resident = dir.resident_bytes();
    Ok(read_cache::insert(&file.key, resident, dir))
}

/// The slab directory a query ranks clusters with: an ordinary directory holding only the entries
/// of the cells nearest the query when the set has a paged one, else the whole directory.
pub(crate) async fn slab_directory(
    store: &Storage,
    set: &bf_wal::SlabSet,
    prefetched: Option<&Prefetched>,
    vector: &[f32],
    config: &EngineConfig,
) -> Result<(Arc<Directory>, bool), ApiError> {
    let Some(file) = set.pages.as_ref() else {
        let cached = read_cache::get::<Directory>(&set.dir.key, set.dir.metadata_len).is_some();
        return Ok((load(store, &set.dir, prefetched).await?, cached));
    };
    let t0 = std::time::Instant::now();
    let warm = read_cache::get::<PagedDirectory>(&file.key, file.metadata_len).is_some();
    let paged = load_paged(store, file, prefetched).await?;
    crate::ann::stage(t0, "directory header");
    paged_view(store, file, &paged, vector, config, warm).await
}

/// The ordinary directory over the cells of `paged` nearest `vector`: their pages, read (merged)
/// or taken from the decoded cache. `warm` says the header was already resident; the result's
/// flag says the pages were too.
async fn paged_view(
    store: &Storage,
    file: &bf_wal::SegmentFile,
    paged: &Arc<PagedDirectory>,
    vector: &[f32],
    config: &EngineConfig,
    mut warm: bool,
) -> Result<(Arc<Directory>, bool), ApiError> {
    let t0 = std::time::Instant::now();
    let (rows, clusters) = paged.totals();
    let want = (probe_count(config, rows as usize, clusters as usize, 1.0, Layout::Slabs) as f64
        * page_margin())
    .ceil() as usize;
    let kappa = page_kappa();
    let cells = {
        let (paged, vector) = (paged.clone(), vector.to_vec());
        on_worker(async move {
            paged
                .select_cells(&vector, want, kappa)
                .map_err(segment::index_error)
        })
        .await?
    };
    let reader = Reader::new(store, file);
    let page_key = |cell: usize| format!("{}#{cell}", file.key);
    let mut pages: Vec<Option<Arc<Page>>> = cells
        .iter()
        .map(|&cell| {
            let range = paged.page_range(cell);
            read_cache::get::<Page>(&page_key(cell), range.end - range.start)
        })
        .collect();
    // Pages of neighboring cells sit next to each other: read them as few merged ranges.
    let missing: Vec<usize> = (0..cells.len()).filter(|i| pages[*i].is_none()).collect();
    warm &= missing.is_empty();
    let bytes = bf_vector::read_merged(
        &reader,
        missing
            .iter()
            .map(|&i| paged.page_range(cells[i]))
            .collect(),
        bf_vector::read_gap(PAGE_GAP),
        PAGE_MERGE_MAX,
    )
    .await
    .map_err(segment::index_error)?;
    for (&i, bytes) in missing.iter().zip(bytes) {
        let page = paged.decode_page(&bytes).map_err(segment::index_error)?;
        let resident = page.resident_bytes();
        pages[i] = Some(read_cache::insert(&page_key(cells[i]), resident, page));
    }
    let pages: Vec<Arc<Page>> = pages.into_iter().flatten().collect();
    crate::ann::stage(t0, "directory pages");
    let view = {
        let paged = paged.clone();
        on_worker(async move { Ok(paged.assemble(&pages)) }).await?
    };
    Ok((Arc::new(view), warm))
}

/// Scan results computed while a cold query's reads were still arriving, so the query's own scan
/// finds them instead of reading the cached bytes again and scanning after the last read lands.
/// A scan is a pure function of the (immutable) index object, the query, and the probed clusters,
/// so a hit is exact; entries are taken once and expire.
type Scanned = (Arc<Index<Reader>>, Vec<Candidate>);
type ScanKey = (String, u64);
static SCAN_MEMO: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<ScanKey, (std::time::Instant, Scanned)>>,
> = std::sync::OnceLock::new();
/// Entries in `SCAN_MEMO`: a warm query (nothing prefetched) skips the lock.
static SCAN_MEMO_LEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// Seeds the memo's key hashes (once per process).
static SCAN_HASH: std::sync::OnceLock<foldhash::quality::RandomState> = std::sync::OnceLock::new();

/// A query vector's hash, computed once a query for every segment's memo key.
fn vector_hash(vector: &[f32]) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = SCAN_HASH.get_or_init(Default::default).build_hasher();
    vector.iter().for_each(|x| h.write_u32(x.to_bits()));
    h.write_usize(vector.len());
    h.finish()
}

fn scan_key(object: &str, vector: u64, clusters: &[usize]) -> ScanKey {
    use std::hash::{BuildHasher, Hash, Hasher};
    let mut h = SCAN_HASH.get_or_init(Default::default).build_hasher();
    h.write_u64(vector);
    clusters.hash(&mut h);
    (object.to_string(), h.finish())
}

fn scan_memo_put(key: ScanKey, value: Scanned) {
    let memo = SCAN_MEMO.get_or_init(Default::default);
    let mut memo = memo.lock().expect("scan memo lock");
    let now = std::time::Instant::now();
    memo.retain(|_, (at, _)| now.duration_since(*at) < std::time::Duration::from_secs(30));
    // Entries are consumed by the query that made them. Bound what an unconsumed one can hold
    // (about 24 bytes a candidate) when many cold queries overlap.
    let held: usize = memo.values().map(|(_, (_, found))| found.len()).sum();
    if memo.len() < 4096 && (held + value.1.len()) * 24 < (128 << 20) {
        memo.insert(key, (now, value));
    }
    SCAN_MEMO_LEN.store(memo.len(), std::sync::atomic::Ordering::Relaxed);
}

fn scan_memo_take(key: &ScanKey) -> Option<Scanned> {
    if SCAN_MEMO_LEN.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return None;
    }
    let mut memo = SCAN_MEMO.get()?.lock().expect("scan memo lock");
    let taken = memo.remove(key).map(|(_, v)| v);
    SCAN_MEMO_LEN.store(memo.len(), std::sync::atomic::Ordering::Relaxed);
    taken
}

/// Start a cold unfiltered ANN query's directory pages and scan reads from the hint alone, before
/// the manifest has been read: a paged directory's header lists every slab (key, sizes, and
/// fragment), so nothing else is needed to know what to read. The reads land in the caches, and
/// the query's own reads join them while in flight. Advisory: a manifest that disagrees (the slabs
/// were replaced) only costs the bytes.
pub(crate) async fn speculate_slab_scan(
    store: &Storage,
    hint: Prefetched,
    attribute: &str,
    vector: &[f32],
    k: usize,
    config: &EngineConfig,
) {
    let (key, bytes) = hint;
    if !PagedDirectory::is_paged(&bytes) {
        return;
    }
    let Ok(decoded) = PagedDirectory::decode(&bytes) else {
        return;
    };
    let file = bf_wal::SegmentFile {
        key,
        size: decoded.object_len(),
        metadata_len: bytes.len() as u64,
        stats_index: None,
        text_header: None,
    };
    let resident = decoded.resident_bytes();
    let paged = read_cache::get::<PagedDirectory>(&file.key, file.metadata_len)
        .unwrap_or_else(|| read_cache::insert(&file.key, resident, decoded));
    let Ok((dir, _)) = paged_view(store, &file, &paged, vector, config, false).await else {
        return;
    };
    let slabs: Option<Vec<bf_wal::Segment>> = paged
        .segments()
        .iter()
        .map(|s| {
            let [(isize, imeta), (fsize, fmeta)] = s.files?;
            let object = |key: &str, size, metadata_len| bf_wal::SegmentFile {
                key: key.to_string(),
                size,
                metadata_len,
                stats_index: None,
                text_header: None,
            };
            Some(bf_wal::Segment {
                id: s.id.clone(),
                wal_start: 0,
                wal_end: 0,
                row_count: s.rows as u32,
                schema: Default::default(),
                files: crate::upgrade::hint_files(
                    s,
                    attribute,
                    object(&s.key, isize, imeta),
                    object(&s.fragment, fsize, fmeta),
                ),
                live: object("", 0, 0),
                live_generation: 0,
            })
        })
        .collect();
    if let Some(slabs) = slabs {
        let _ = prefetch_scan(
            store,
            &slabs,
            &dir,
            attribute,
            Layout::Slabs,
            vector,
            k,
            config,
        )
        .await;
    }
}

#[cfg(test)]
mod spread_tests {
    use super::{Layout, MAX_SPREAD, filtered_probe_scale, independent_spread, probe_count};
    use crate::EngineConfig;

    #[test]
    fn local_coverage_skips_empty_leaves_and_keeps_distributed_probe_geometry() {
        use bf_vector::{BuildParams, Directory, Fragment, IndexBuilder, Metric};
        let rows: Vec<_> = (0..1000u32)
            .map(|id| {
                let mut vector = vec![0.; 8];
                vector[0] = (id / 100) as f32;
                (id, vector)
            })
            .collect();
        let blob = IndexBuilder::new(8, Metric::EuclideanSquared)
            .unwrap()
            .params(BuildParams {
                flat_threshold: 0,
                clusters: Some(10),
                ..Default::default()
            })
            .build(&rows)
            .unwrap();
        let (ann, length) = Fragment::build_ann(&blob).unwrap();
        let fragment = Fragment::decode(&ann[..length as usize]).unwrap();
        let bytes = Directory::build(&[("segment".into(), "ann".into(), "ann".into(), fragment)])
            .unwrap()
            .unwrap();
        let directory = Directory::decode(&bytes).unwrap();
        assert_eq!(directory.entries().len(), 10);
        assert!(directory.entries().iter().all(|e| e.rows == 100));
        let distances: Vec<_> = (0..10).map(|i| (10 - i) as f32).collect();
        let config = EngineConfig {
            ann_nprobe: 1,
            ann_nprobe_explicit: true,
            ..Default::default()
        };
        let baseline = EngineConfig {
            baseline_filter_test: true,
            ..config.clone()
        };
        let totals = super::Totals {
            layout: Layout::Segments,
            ann_rows: 1000,
            matching: 400,
            clusters: 10,
            target: 10,
            k: 10,
            filtered: true,
            spread: 1.,
        };
        let local = |n| if n < 4 { 100. } else { 0. };
        assert_eq!(
            super::pick_clusters(&directory, &distances, &local, &totals, &config),
            vec![3, 2, 1]
        );
        assert_eq!(
            super::pick_clusters(&directory, &distances, &local, &totals, &baseline),
            vec![3, 2, 1, 0]
        );
        // The same allowed cardinality spread across all leaves retains main's probe floor.
        let distributed = |_| 40.;
        assert_eq!(
            super::pick_clusters(&directory, &distances, &distributed, &totals, &config),
            super::pick_clusters(&directory, &distances, &distributed, &totals, &baseline)
        );
    }

    #[test]
    fn filtered_margin_covers_survivor_variance_under_independent_thinning() {
        for matching in [10_000, 100_000, 500_000, 900_000] {
            let p = matching as f64 / 1_000_000.0;
            for k in [1, 10, 100] {
                let trials =
                    (filtered_probe_scale(1_000_000, matching, k) * k as f64).ceil() as usize;
                // Exact binomial probability of fewer than k survivors, independently of
                // the planner's waiting-time approximation. A mean-only budget fails this.
                let mut term = (1.0 - p).powi(trials as i32);
                let mut failure = term;
                for j in 0..k - 1 {
                    term *= (trials - j) as f64 / (j + 1) as f64 * p / (1.0 - p);
                    failure += term;
                }
                assert!(failure < 0.2, "p={p}, k={k}, failure={failure}");
            }
        }
        assert_eq!(filtered_probe_scale(100, 100, 10), 1.0);
    }

    #[test]
    fn selective_probes_preserve_candidate_coverage_for_both_layouts() {
        let config = EngineConfig::default();
        for layout in [Layout::Segments, Layout::Slabs] {
            let unfiltered = probe_count(&config, 1_000_000, 10_000, 1.0, layout);
            for matching in [1, 10, 1000, 10_000, 100_000, 500_000, 1_000_000] {
                let probes = probe_count(
                    &config,
                    1_000_000,
                    10_000,
                    filtered_probe_scale(1_000_000, matching, 10),
                    layout,
                );
                assert!(probes == 10_000 || probes * matching >= unfiltered * 1_000_000);
            }
        }
    }

    #[test]
    fn spread_counts_effective_equal_segments() {
        let c = EngineConfig::default();
        let spread = |rows: &[usize]| independent_spread(&c, rows.iter().copied());
        // One segment, or none: no extra probes.
        assert_eq!(spread(&[1000]), 1.0);
        assert_eq!(spread(&[]), 1.0);
        // 25 equal segments: 25^0.3.
        assert!((spread(&[100_000; 25]) - 25f64.powf(0.3)).abs() < 1e-9);
        // Tiny tail segments barely count.
        assert!(spread(&[1_000_000, 100, 100]) < 1.001);
        // Many segments: capped.
        assert_eq!(spread(&[50_000; 200]), MAX_SPREAD);
        // An explicit probe count is used as given.
        let explicit = EngineConfig {
            ann_nprobe_explicit: true,
            ..EngineConfig::default()
        };
        assert_eq!(independent_spread(&explicit, [1; 25].into_iter()), 1.0);
    }
}

#[cfg(test)]
mod key_tests {
    use super::total_order_key;

    #[test]
    fn total_order_keys_sort_as_total_cmp() {
        let xs = [
            f32::NEG_INFINITY,
            -2.5,
            -0.0,
            0.0,
            1e-30,
            0.5,
            3.0,
            f32::INFINITY,
            f32::NAN,
        ];
        for a in xs {
            for b in xs {
                assert_eq!(
                    total_order_key(a).cmp(&total_order_key(b)),
                    a.total_cmp(&b),
                    "{a} {b}"
                );
            }
        }
    }
}
