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

/// Fetch merged ranges at most this far apart as one request, for block fragments (a block is
/// tens of KB, so only adjacent blocks should merge).
const BLOCK_GAP: u64 = 16 << 10;

/// Read the blobs of `entries` (one segment's probed clusters) and, unless `exact`, their
/// quantized codes. With block fragments both come from one set of ranged reads of the
/// fragment; otherwise the blobs come from the fragment and the codes from the index object.
async fn read_clusters(
    entries: &[&bf_vector::DirEntry],
    blocks: bool,
    exact: bool,
    fragment: &Reader,
    partial: &bf_vector::PartialIndex<Reader>,
) -> Result<(Vec<bytes::Bytes>, Vec<bytes::Bytes>), ApiError> {
    let blob_ranges: Vec<_> = entries.iter().map(|e| e.blob_range()).collect();
    if blocks {
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
        return Ok((all, codes));
    }
    let wanted: Vec<usize> = entries.iter().map(|e| e.cluster).collect();
    let read_blobs = async {
        bf_vector::read_merged(
            fragment,
            blob_ranges,
            bf_vector::read_gap(BLOB_GAP),
            BLOB_MERGE_MAX,
        )
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
    tokio::try_join!(read_blobs, read_codes)
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
        && let Ok(dir) = Directory::decode(&bytes[..file.metadata_len as usize])
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
    let dir = Directory::decode(&bytes).map_err(segment::index_error)?;
    let resident = dir.resident_bytes();
    Ok(read_cache::insert(&file.key, resident, dir))
}

/// Which fragment layout a directory lists. `Plain` fragments hold cluster blobs, and the
/// quantized codes stay in the segment's index object (two reads per probed cluster). `Blocks`
/// fragments hold each cluster's blob and codes together, in locality order, and serve
/// unfiltered queries; they sit beside the plain ones, so older readers keep working.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Layout {
    Plain,
    Blocks,
}
impl Layout {
    /// Key of the directory in the manifest.
    pub(crate) fn dir_key(self, attr: &str) -> String {
        match self {
            Self::Plain => format!("ann:{attr}"),
            Self::Blocks => format!("ann2:{attr}"),
        }
    }
    /// Key of a segment's fragment in its file map.
    pub(crate) fn frag_key(self, attr: &str) -> String {
        match self {
            Self::Plain => format!("annfrag:{attr}"),
            Self::Blocks => format!("annfrag2:{attr}"),
        }
    }
}

/// `BF_ANN_BLOCKS=0` turns the block layout off for queries (A/B measurements only).
pub(crate) fn blocks_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("BF_ANN_BLOCKS").is_ok_and(|v| v != "0")
            || std::env::var_os("BF_ANN_BLOCKS").is_none()
    })
}

/// Whether `dir` lists exactly these segments' indexes for `attr`, in order.
pub(crate) fn covers(
    dir: &Directory,
    segments: &[bf_wal::Segment],
    attr: &str,
    layout: Layout,
) -> bool {
    let key = format!("ann:{attr}");
    let frag = layout.frag_key(attr);
    dir.segments().len() == segments.len()
        && dir.segments().iter().zip(segments).all(|(d, s)| {
            d.id == s.id
                && s.files.get(&key).is_some_and(|f| f.key == d.key)
                && s.files.get(&frag).is_some_and(|f| f.key == d.fragment)
        })
}

/// Build (or keep) the directory of `layout` for `attr` over `segments`. Returns `None`
/// when some segment lacks the index or its fragment, or the segments do not share one vector
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
    let key = format!("ann:{attr}");
    let key = key.as_str();
    let frag_key = layout.frag_key(attr);
    if segments.is_empty()
        || segments
            .iter()
            .any(|s| !s.files.contains_key(key) || !s.files.contains_key(&frag_key))
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
    let file = segment::put_file(
        store,
        format!(
            "ns/{ns}/segments/ann-dir{}-{}.bfd",
            if layout == Layout::Blocks { "2" } else { "" },
            uuid::Uuid::new_v4()
        ),
        bytes.clone(),
        len,
    )
    .await?;
    // A copy at a fixed key lets a cold reader fetch the directory while it reads the manifest.
    // Best effort: readers verify the embedded key against the manifest, so a stale or missing
    // hint only costs the extra wave.
    let mut hint = Vec::with_capacity(bytes.len() + file.key.len() + 4);
    hint.extend_from_slice(&(file.key.len() as u32).to_le_bytes());
    hint.extend_from_slice(file.key.as_bytes());
    hint.extend_from_slice(&bytes);
    if let Err(e) = store
        .put(
            &hint_key(ns, layout.dir_key(attr).trim_start_matches("ann:")),
            hint,
        )
        .await
    {
        tracing::warn!(error=?e, "ANN directory hint not written");
    }
    Ok(Some(file))
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
    if segments.iter().any(|s| s.live_generation != 0) || !covers(dir, segments, attribute, layout)
    {
        return Ok(());
    }
    let t0 = std::time::Instant::now();
    let vector: Arc<[f32]> = vector.into();
    let distances = {
        let (dir, vector) = (dir.clone(), vector.clone());
        on_worker(async move { dir.center_distances(&vector).map_err(segment::index_error) })
            .await?
    };
    crate::ann::stage(t0, "prefetch rank");
    let n = dir.segments().len();
    let mut seg_rows = vec![0usize; n];
    let mut seg_clusters = vec![0usize; n];
    for e in dir.entries() {
        seg_rows[e.segment] += e.rows as usize;
        seg_clusters[e.segment] += 1;
    }
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
            ann_rows,
            matching,
            clusters,
            target,
            filtered: false,
        },
        config,
    );
    if std::env::var_os("BF_TRACE_STAGES").is_some() {
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
    let key = format!("ann:{attribute}");
    let frag_key = layout.frag_key(attribute);
    let reads = chosen.into_iter().map(|(i, cs)| {
        let (key, frag_key) = (&key, &frag_key);
        async move {
            let (Some(file), Some(frag)) =
                (segments[i].files.get(key), segments[i].files.get(frag_key))
            else {
                return;
            };
            let seg_entries: Vec<_> = dir
                .entries()
                .iter()
                .filter(|e| e.segment == i && cs.contains(&e.cluster))
                .collect();
            let Ok(partial) = dir.partial_index(i, Reader::new(store, file)) else {
                return;
            };
            let frag_reader = Reader::new(store, frag);
            let _ = read_clusters(
                &seg_entries,
                dir.segments()[i].codes_in_fragment,
                false,
                &frag_reader,
                &partial,
            )
            .await;
        }
    });
    // Flat segments are scanned exactly in the same wave: their blobs and all their vectors.
    let flats = dir
        .segments()
        .iter()
        .enumerate()
        .filter(|(_, s)| s.flat && s.rows > 0)
        .map(|(i, _)| {
            let (key, frag_key) = (&key, &frag_key);
            async move {
                let (Some(file), Some(frag)) =
                    (segments[i].files.get(key), segments[i].files.get(frag_key))
                else {
                    return;
                };
                let seg_entries: Vec<_> = dir.entries().iter().filter(|e| e.segment == i).collect();
                let Ok(partial) = dir.partial_index(i, Reader::new(store, file)) else {
                    return;
                };
                let frag_reader = Reader::new(store, frag);
                let vectors = Reader::new(store, file);
                let pieces = vector_pieces(seg_entries.iter().map(|e| e.vectors_range()));
                let _ = tokio::join!(
                    read_clusters(
                        &seg_entries,
                        dir.segments()[i].codes_in_fragment,
                        true,
                        &frag_reader,
                        &partial,
                    ),
                    futures::future::join_all(pieces.into_iter().map(|r| vectors.read(r))),
                );
            }
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
            return Ok(Some(Arc::new(
                crate::snapshot::allowed_with(store, state, seg, Some(f), false).await?,
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
    ann_rows: usize,
    matching: usize,
    clusters: usize,
    target: usize,
    filtered: bool,
}

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
        ann_rows,
        matching,
        clusters,
        target,
        filtered,
    } = *t;
    let mut order: Vec<(f32, usize)> = dir
        .entries()
        .iter()
        .enumerate()
        .filter(|(n, e)| !dir.segments()[e.segment].flat && e.rows > 0 && in_cluster(*n) > 0.0)
        .map(|(n, _)| (distances[n], n))
        .collect();
    let scale = (clusters as f64 / (ann_rows as f64).sqrt()).max(1.0);
    // Deleted rows and filtered-out rows thin the probed clusters, so probe more (a filter at
    // most `filtered_probe_cap` times) and keep probing until the allowed candidates reach
    // `target`.
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
    picked
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
    layout: Layout,
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
    let mut likely = BTreeSet::new();
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
            let picked = pick_clusters(
                dir.as_ref(),
                &distances,
                &in_cluster,
                &Totals {
                    ann_rows,
                    matching,
                    clusters,
                    target,
                    filtered,
                },
                config,
            );
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
            .get(&layout.frag_key(attribute))
            .cloned()
            .ok_or_else(|| segment::index_error("segment lost its ANN fragment"))?;
        let blocks = dir.segments()[i].codes_in_fragment;
        let warm_docs = exact || likely.contains(&i);
        let (segment_docs, docs_store) = (state.segments[i].clone(), store.clone());
        let dir_reader = Reader::new(store, &frag_file);
        let live = lives[i].clone();
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
                    let pieces = vector_pieces(prefetch_ranges);
                    // Best effort: a failure here only means the scan reads the range itself.
                    let _ = futures::future::join_all(
                        pieces.into_iter().map(|r| prefetch_reader.read(r)),
                    )
                    .await;
                    Ok::<(), ApiError>(())
                };
                let mut partial = dir.partial_index(i, reader).map_err(segment::index_error)?;
                let wanted: Vec<usize> = seg_entries.iter().map(|e| e.cluster).collect();
                let entry_refs: Vec<_> = seg_entries.iter().collect();
                // Documents' footers are warmed only for the segments holding likely winners (and
                // for exactly scanned ones); the speculative document read reads the rest.
                let (read, (), ()) = tokio::try_join!(
                    read_clusters(&entry_refs, blocks, exact, &dir_reader, &partial),
                    prefetch,
                    async {
                        if warm_docs {
                            segment::warm_docs(&docs_store, &segment_docs).await;
                        }
                        Ok(())
                    }
                )?;
                let (blobs, codes) = read;
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
    // Every segment's reads start at once: a second batch of segments would add a whole wave.
    let tasks_len = tasks.len();
    let results: Vec<(Probed, Found)> = futures::stream::iter(tasks)
        .buffer_unordered(limit.max(tasks_len))
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
    // A rerank is a few small reads and a little arithmetic. Running it on a worker would hold
    // one of the few permits through the read latency, and the segments would queue for them.
    let reranks = by_segment.into_iter().map(|(i, locations)| {
        let (index, vector) = (indexes[&i].clone(), vector.clone());
        async move {
            let hits = index
                .rerank_candidates(&vector, k, locations)
                .await
                .map_err(segment::index_error)?;
            Ok::<_, ApiError>(hits.into_iter().map(|(r, d)| (d, i, r)).collect::<Vec<_>>())
        }
    });
    let rerank_limit = reranks.len().max(limit);
    let prefetch = futures::future::join_all(speculative.iter().map(|(&i, rows)| async move {
        let _ = segment::projected_rows(store, &state.segments[i], rows, Some(columns)).await;
    }));
    let (reranked, _) = tokio::join!(
        futures::stream::iter(reranks)
            .buffer_unordered(rerank_limit)
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
