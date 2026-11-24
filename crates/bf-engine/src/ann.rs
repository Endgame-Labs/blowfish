//! Cross-segment ANN: rank leaf clusters of every segment together, scan only the globally
//! nearest clusters, and rerank one merged candidate pool. Probing each segment separately
//! scans and reranks once per segment, so cost grew with the segment count instead of the
//! corpus size.
use crate::{EngineConfig, NamespaceState, ann_dir::Layout, segment};
use bf_storage::Storage;
use bf_types::*;
use bf_vector::RoaringBitmap;
use futures::FutureExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Search current and upgrading objects through one namespace-wide leaf plan. Only segments
/// without any ANN index require the document-only fallback.
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
    if crate::ann::trace_stages() {
        eprintln!(
            "ANN snapshot: segments={} shadowed={} directories={:?} tail_docs={}",
            state.segments.len(),
            state.shadowed.len(),
            state.ann_dirs.keys().collect::<Vec<_>>(),
            state.docs.len()
        );
    }
    // A recluster's tiers answer unfiltered queries for the rows they cover, and the namespace
    // directory for the rest (segments written since).
    if q.filters.is_none()
        && let Some(tiers) = state.ann_tiers.get(attribute)
        && let Some(found) = from_tiers(
            store, state, q, attribute, vector, config, k, columns, tiers,
        )
        .await?
    {
        return Ok(Some(found));
    }
    if state
        .segments
        .iter()
        .any(|s| crate::upgrade::files(s, attribute).is_none())
    {
        return Ok(None);
    }
    if let Some((file, _)) = crate::upgrade::directory_file(&state.ann_dirs, attribute)
        && let Some(found) = from_directory(
            store,
            state,
            q,
            attribute,
            vector,
            config,
            k,
            columns,
            Some(file),
            Layout::Segments,
            &[],
        )
        .await?
    {
        return Ok(Some(found));
    }
    if state
        .segments
        .iter()
        .any(|s| s.files.contains_key(&format!("ann:{attribute}")))
    {
        return from_directory(
            store,
            state,
            q,
            attribute,
            vector,
            config,
            k,
            columns,
            None,
            Layout::Segments,
            &[],
        )
        .await;
    }
    Ok(None)
}

/// Answer an ANN query from one namespace directory, or `None` when it cannot (stale or
/// unusable directory, or shadowed rows eat into the results), so the caller scans document columns.
#[allow(clippy::too_many_arguments)]
async fn from_directory(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    attribute: &str,
    vector: &[f32],
    config: &EngineConfig,
    k: usize,
    columns: &BTreeSet<String>,
    file: Option<&bf_wal::SegmentFile>,
    layout: Layout,
    skip: &[bool],
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    // Filtered queries compute each segment's allowed rows (live rows that pass the filter)
    // alongside the directory, and read each segment's row-to-cluster map, so the
    // directory ranks clusters by their allowed rows without opening any segment's index
    // or ID index.
    let (dir, lives) = tokio::join!(
        async {
            match file {
                Some(f) => crate::ann_dir::load(store, f, None).await,
                None => crate::upgrade::directory(store, &state.segments, attribute).await,
            }
        },
        crate::ann_dir::live_masks(store, state, q.filters.as_ref())
    );
    // Upgrade readers normalize older directories before the current planner uses them.
    if crate::ann::trace_stages() {
        match &dir {
            Ok(d) => eprintln!(
                "DIRECTORY covers={} dir_segments={} state_segments={}",
                crate::ann_dir::covers(d, &state.segments, attribute, layout),
                d.segments().len(),
                state.segments.len()
            ),
            Err(e) => eprintln!("DIRECTORY unusable: {e:?}"),
        }
    }
    let lives = if dir.is_ok() { Some(lives?) } else { None };
    if let (Ok(dir), Some(lives)) = (dir, lives)
        && crate::ann_dir::covers(&dir, &state.segments, attribute, layout)
    {
        let members = if q.filters.is_some() {
            let members =
                crate::ann_dir::memberships(store, &state.segments, &dir, attribute).await?;
            // A segment whose fragment predates the maps cannot take a filter.
            dir.segments()
                .iter()
                .zip(&members)
                .all(|(s, m)| s.flat || m.is_some())
                .then_some(members)
        } else {
            None
        };
        if q.filters.is_none() || members.is_some() {
            // Unindexed upserts and deletes shadow rows of the segments by ID. Rather than
            // opening every ID index to find the shadowed rows, ask for a few extra
            // winners and drop the shadowed ones afterwards. If the shadowed rows eat into
            // the results so that fewer than k remain while the pool was full, use the
            // document scan instead.
            let slack = if state.shadowed.is_empty() {
                0
            } else {
                (2 * k).max(20)
            };
            let (mut scores, mut docs) = crate::ann_dir::search(
                store,
                state,
                dir,
                lives,
                vector,
                config,
                k + slack,
                columns,
                attribute,
                members,
                layout,
                None,
                skip,
                None,
            )
            .await?;
            if slack > 0 {
                let pooled = scores.len();
                scores.retain(|id, _| !state.shadowed.contains(id));
                docs.retain(|id, _| !state.shadowed.contains(id));
                if scores.len() >= k || pooled < k + slack {
                    return Ok(Some((scores, docs)));
                }
            } else {
                return Ok(Some((scores, docs)));
            }
        }
    }
    Ok(None)
}

/// Whether `BF_TRACE_STAGES` is set (read once: a query checks it many times).
pub(crate) fn trace_stages() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("BF_TRACE_STAGES").is_some())
}

/// `BF_TRACE_STAGES=1` prints elapsed time at each ANN stage (diagnostics only).
pub(crate) fn stage(t0: std::time::Instant, name: &str) {
    if trace_stages() {
        eprintln!("STAGE {name} {} us", t0.elapsed().as_micros());
    }
}

/// Queries the tiers answered (tests check the plan is used).
#[cfg(test)]
pub(crate) static TIER_ANSWERS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Whether every tier of `tiers` can answer for its covered segments: each is live, or merged
/// into live segments by lineage (see `lineage::reachable`).
pub(crate) fn tiers_usable(
    segments: &[bf_wal::Segment],
    lineage: &[bf_wal::Lineage],
    tiers: &[bf_wal::SlabSet],
) -> bool {
    tiers
        .iter()
        .all(|t| crate::lineage::reachable(segments, lineage, t))
}

/// Unfiltered ANN over a recluster's tiers and the segments no tier covers, as one plan: each
/// tier's slabs and the uncovered segments (through the namespace directory, with the covered
/// segments left out of its probe plan) are searched at the same time, and their exactly
/// reranked winners merged. `None` when some part cannot answer, so the caller falls back.
#[allow(clippy::too_many_arguments)]
async fn from_tiers(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    attribute: &str,
    vector: &[f32],
    config: &EngineConfig,
    k: usize,
    columns: &BTreeSet<String>,
    tiers: &[bf_wal::SlabSet],
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    let mut covered = vec![false; state.segments.len()];
    let mut positions = Vec::with_capacity(tiers.len());
    for set in tiers {
        let Some(at) = crate::lineage::place(store, &state.segments, &state.lineage, set).await?
        else {
            return Ok(None);
        };
        for i in at.positions() {
            covered[i] = true;
        }
        positions.push(at);
    }
    let uncovered = covered.iter().any(|c| !c);
    // A base and its delta rank their leaves as one list.
    let seats = if tiers.len() > 1 {
        crate::ann_dir::Joint::seats(tiers.len())
            .into_iter()
            .map(Some)
            .collect()
    } else {
        vec![None]
    };
    let parts = tiers
        .iter()
        .zip(&positions)
        .zip(seats)
        .map(|((set, at), seat)| {
            async move {
                from_slabs(
                    store,
                    state,
                    attribute,
                    vector,
                    config,
                    k,
                    columns,
                    set,
                    at,
                    None,
                    seat.as_ref(),
                )
                .await
            }
            .boxed()
        });
    // Segments with no ANN object at all leave the whole query to the document scan.
    if uncovered
        && state
            .segments
            .iter()
            .any(|s| crate::upgrade::files(s, attribute).is_none())
    {
        return Ok(None);
    }
    let rest = uncovered.then(|| {
        let covered = &covered;
        async move {
            let file = crate::upgrade::directory_file(&state.ann_dirs, attribute).map(|(f, _)| f);
            from_directory(
                store,
                state,
                q,
                attribute,
                vector,
                config,
                k,
                columns,
                file,
                Layout::Segments,
                covered,
            )
            .await
        }
        .boxed()
    });
    let found = futures::future::try_join_all(parts.chain(rest)).await?;
    if crate::ann::trace_stages() {
        eprintln!(
            "TIERS tiers={} uncovered_segments={}",
            tiers.len(),
            covered.iter().filter(|c| !**c).count()
        );
    }
    let mut ranked: Vec<(f64, Id, Document)> = Vec::new();
    for part in found {
        let Some((scores, mut docs)) = part else {
            return Ok(None);
        };
        for (id, score) in scores {
            if let Some(doc) = docs.remove(&id) {
                ranked.push((score, id, doc));
            }
        }
    }
    #[cfg(test)]
    TIER_ANSWERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut scores = BTreeMap::new();
    let mut docs = BTreeMap::new();
    for (score, id, doc) in ranked {
        if scores.len() == k {
            break;
        }
        if let std::collections::btree_map::Entry::Vacant(e) = scores.entry(id.clone()) {
            e.insert(score);
            docs.insert(id, doc);
        }
    }
    Ok(Some((scores, docs)))
}

/// Live-row masks of recluster tiers whose covered segments changed since the build, by tier
/// directory and the changed segments' live objects.
static TIER_MASKS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, Arc<RoaringBitmap>>>,
> = std::sync::LazyLock::new(Default::default);

/// The live rows of a tier as a mask over its global row numbers, or `None` when every row it
/// holds is still live: rows deleted since the slabs were built, and rows dropped by a merge, are
/// left out. Built once per tier and set of live objects and lineage maps (all immutable).
pub(crate) async fn tier_mask(
    store: &Storage,
    state: &NamespaceState,
    set: &bf_wal::SlabSet,
    at: &crate::lineage::Placement,
) -> Result<Option<Arc<RoaringBitmap>>, ApiError> {
    use crate::lineage::Place;
    // Covered segments still here whose rows were deleted since the build, and merged ones (whose
    // targets' live rows decide).
    let mut changed: Vec<usize> = Vec::new();
    for (p, place) in at.places.iter().enumerate() {
        match place {
            Place::Here(i) => {
                if state.segments[*i].live_generation != set.covered[p].live_generation {
                    changed.push(p);
                }
            }
            Place::Moved(_) => changed.push(p),
        }
    }
    if changed.is_empty() {
        return Ok(None);
    }
    let mut key = set.dir.key.clone();
    let mut lives: BTreeSet<usize> = BTreeSet::new();
    for p in &changed {
        match &at.places[*p] {
            Place::Here(i) => {
                lives.insert(*i);
            }
            Place::Moved(parts) => lives.extend(parts.iter().map(|(i, _)| *i)),
        }
    }
    for i in &lives {
        key.push('|');
        key.push_str(&state.segments[*i].live.key);
    }
    for map in &at.maps {
        key.push('|');
        key.push_str(map);
    }
    if let Some(mask) = TIER_MASKS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .cloned()
    {
        return Ok(Some(mask));
    }
    let lives: Vec<usize> = lives.into_iter().collect();
    let loaded = futures::future::try_join_all(
        lives
            .iter()
            .map(|i| segment::live(store, &state.segments[*i])),
    )
    .await?;
    let live_of = |i: usize| &loaded[lives.binary_search(&i).expect("loaded above")];
    let mut mask = RoaringBitmap::new();
    for (p, c) in set.covered.iter().enumerate() {
        if !changed.contains(&p) {
            // Unchanged segments stay fully live.
            mask.insert_range(c.base..c.base + c.rows);
            continue;
        }
        match &at.places[p] {
            Place::Here(i) => {
                for r in live_of(*i) {
                    mask.insert(c.base + r);
                }
            }
            Place::Moved(parts) => {
                for (i, map) in parts {
                    let live = live_of(*i);
                    for (from, to) in map.pairs() {
                        if live.contains(to) {
                            mask.insert(c.base + from);
                        }
                    }
                }
            }
        }
    }
    let mask = Arc::new(mask);
    let mut masks = TIER_MASKS.lock().unwrap_or_else(|e| e.into_inner());
    // A few tiers' current masks; older generations are dropped wholesale.
    if masks.len() >= 16 {
        masks.clear();
    }
    masks.insert(key, mask.clone());
    Ok(Some(mask))
}

/// Unfiltered ANN from one tier's slabs: the same directory search, over slabs as pseudo
/// segments, with winners mapped back to the segments holding them now (`at`) for hydration.
#[allow(clippy::too_many_arguments)]
async fn from_slabs(
    store: &Storage,
    state: &NamespaceState,
    attribute: &str,
    vector: &[f32],
    config: &EngineConfig,
    k: usize,
    columns: &BTreeSet<String>,
    set: &bf_wal::SlabSet,
    at: &crate::lineage::Placement,
    prefetched: Option<&crate::ann_dir::Prefetched>,
    seat: Option<&crate::ann_dir::Seat>,
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    let Ok((dir, _)) = crate::ann_dir::slab_directory(store, set, prefetched, vector, config).await
    else {
        return Ok(None);
    };
    let mut view = state.without_docs();
    view.segments = set.slabs.clone().into();
    if !crate::ann_dir::covers(&dir, &view.segments, attribute, Layout::Slabs) {
        return Ok(None);
    }
    let mask = tier_mask(store, state, set, at).await?;
    if crate::ann::trace_stages() {
        eprintln!(
            "SLABS slabs={} masked={}",
            view.segments.len(),
            mask.is_some()
        );
    }
    let lives = vec![mask; view.segments.len()];
    let slack = if state.shadowed.is_empty() {
        0
    } else {
        (2 * k).max(20)
    };
    let (mut scores, mut docs) = crate::ann_dir::search(
        store,
        &view,
        dir,
        lives,
        vector,
        config,
        k + slack,
        columns,
        attribute,
        None,
        Layout::Slabs,
        Some((state, &set.covered, at)),
        &[],
        seat,
    )
    .await?;
    if slack > 0 {
        let pooled = scores.len();
        scores.retain(|id, _| !state.shadowed.contains(id));
        docs.retain(|id, _| !state.shadowed.contains(id));
        if scores.len() >= k || pooled < k + slack {
            return Ok(Some((scores, docs)));
        }
        return Ok(None);
    }
    Ok(Some((scores, docs)))
}
