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
    if let Some(tiers) = state.ann_tiers.get(attribute) {
        if let Some(found) = from_tiers(
            store, state, q, attribute, vector, config, k, columns, tiers,
        )
        .await?
        {
            return Ok(Some(found));
        }
        // Incomplete coverage and tail shadowing can still decline a tier plan. Expose every
        // such query, including filtered locator scans, instead of hiding it as ordinary ANN.
        metrics::counter!(
            "blowfish_ann_tier_fallbacks_total",
            "filtered" => if q.filters.is_some() { "true" } else { "false" }
        )
        .increment(1);
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
        eprintln!(
            "STAGE_TIME {name} {}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_micros())
        );
    }
}

/// Identify concurrent warm tasks in the existing diagnostic timeline. Allocate the label
/// only while tracing; timestamps share the request's wall clock with physical reads.
pub(crate) fn task_stage(t0: std::time::Instant, phase: &str, key: &str) {
    if trace_stages() {
        stage(t0, &format!("{phase}:{key}"));
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

type TierResult = Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError>;

/// ANN over a recluster's tiers and the segments no tier covers, as one plan: each
/// tier's slabs and the uncovered segments (through the namespace directory, with the covered
/// segments left out of its probe plan) are searched at the same time, and their exactly
/// reranked winners merged. `None` when some part cannot answer, so the caller falls back.
#[allow(clippy::too_many_arguments)]
fn from_tiers<'a>(
    store: &'a Storage,
    state: &'a NamespaceState,
    q: &'a QueryRequest,
    attribute: &'a str,
    vector: &'a [f32],
    config: &'a EngineConfig,
    k: usize,
    columns: &'a BTreeSet<String>,
    tiers: &'a [bf_wal::SlabSet],
) -> futures::future::BoxFuture<'a, TierResult> {
    // Keep the entire tier plan off caller stacks, including its graph-to-IVF retry.
    async move {
        let t0 = std::time::Instant::now();
        stage(t0, "tiers_start");
        let mut covered = vec![false; state.segments.len()];
        let mut positions = Vec::with_capacity(tiers.len());
        for set in tiers {
            let Some(at) = crate::lineage::place_memoized(store, state, set).await? else {
                return Ok(None);
            };
            for i in at.positions() {
                covered[i] = true;
            }
            positions.push(at);
        }
        let uncovered = covered.iter().any(|c| !c);
        stage(t0, "tiers_placement");
        let allowed = match q.filters.as_ref() {
            Some(filter) => Some(crate::ann_dir::live_masks(store, state, Some(filter)).await?),
            None => None,
        };
        stage(t0, "tiers_filter_masks");
        // The graph hides shadowed rows during navigation, so it needs no extra shortlist.
        let graph_k = k;
        let mut graph_tiers = Vec::with_capacity(tiers.len());
        for (set, at) in tiers.iter().zip(&positions) {
            let enabled = match &allowed {
                None => {
                    if crate::warm_graph::available(state, attribute, set, vector, graph_k) {
                        let mask = tier_mask(store, state, set, at).await?;
                        crate::warm_graph::mask_eligible(
                            state,
                            attribute,
                            set,
                            mask.as_ref(),
                            graph_k,
                        )
                    } else {
                        false
                    }
                }
                Some(allowed) => crate::warm_graph::scan_eligible(
                    state,
                    attribute,
                    set,
                    vector,
                    graph_k,
                    &filtered_tier_mask(set, at, allowed),
                ),
            };
            graph_tiers.push(enabled);
        }
        let accelerated = graph_tiers.iter().any(|enabled| *enabled);
        let declined = std::sync::atomic::AtomicBool::new(false);
        // Graph tiers and IVF tiers contribute independent top-k candidates. Unsupported graph
        // shapes retain the ordinary joint IVF scan, including its candidate bounds.
        let seats = if tiers.len() > 1 && !accelerated {
            crate::ann_dir::Joint::seats(tiers.len())
                .into_iter()
                .map(Some)
                .collect()
        } else {
            (0..tiers.len()).map(|_| None).collect::<Vec<_>>()
        };
        let parts = tiers
            .iter()
            .zip(&positions)
            .zip(seats)
            .zip(graph_tiers)
            .map(|(((set, at), seat), expected)| {
                let declined = &declined;
                let allowed = &allowed;
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
                        allowed.as_deref(),
                        None,
                        seat.as_ref(),
                        expected,
                        declined,
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
                .zip(&covered)
                .any(|(s, c)| !c && crate::upgrade::files(s, attribute).is_none())
        {
            return Ok(None);
        }
        let rest = uncovered.then(|| {
            let mut view = state.without_docs();
            view.segments = state
                .segments
                .iter()
                .zip(&covered)
                .filter(|(_, c)| !**c)
                .map(|(s, _)| s.clone())
                .collect::<Vec<_>>()
                .into();
            async move {
                let file =
                    crate::upgrade::directory_file(&state.ann_dirs, attribute).map(|(f, _)| f);
                if file.is_some()
                    && let Some(found) = from_directory(
                        store,
                        &view,
                        q,
                        attribute,
                        vector,
                        config,
                        k,
                        columns,
                        file,
                        Layout::Segments,
                        &[],
                    )
                    .await?
                {
                    return Ok(Some(found));
                }
                from_directory(
                    store,
                    &view,
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
                .await
            }
            .boxed()
        });
        let found = futures::future::try_join_all(parts.chain(rest)).await?;
        if declined.load(std::sync::atomic::Ordering::Relaxed) {
            // Admission, masks, or source availability can change after planning. Restore the
            // complete joint IVF plan, including its shared probe bounds, for this query.
            if let Some(mut context) = crate::read_cache::context() {
                if let Some(scope) = context.hot_graph.take() {
                    scope.abandon();
                }
                return crate::read_cache::scope(
                    context,
                    Box::pin(from_tiers(
                        store, state, q, attribute, vector, config, k, columns, tiers,
                    )),
                )
                .await;
            }
        }
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
    .boxed()
}

/// Live-row masks of recluster tiers whose covered segments changed since the build, by tier
/// directory and the changed segments' live objects.
type TierMask = Option<Arc<RoaringBitmap>>;
static TIER_MASKS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, TierMask>>,
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
    if let Some(mask) = at.mask.get() {
        return Ok(mask.clone());
    }
    let mask = tier_mask_uncached(store, state, set, at).await?;
    let _ = at.mask.set(mask.clone());
    Ok(mask)
}
async fn tier_mask_uncached(
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
        return Ok(mask);
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
    // A merge can move every row without deleting any. Preserve the mask-free plan in
    // that case: a full bitmap otherwise disables the compiled tier and its resident scan.
    // Cache the proof with the same immutable live objects and lineage maps as a real mask.
    let full = set.covered.iter().all(|cover| {
        cover
            .base
            .checked_add(cover.rows)
            .is_some_and(|end| mask.contains_range(cover.base..end))
    });
    let mask = (!full).then(|| Arc::new(mask));
    let mut masks = TIER_MASKS.lock().unwrap_or_else(|e| e.into_inner());
    // A few tiers' current masks; older generations are dropped wholesale.
    if masks.len() >= 16 {
        masks.clear();
    }
    masks.insert(key, mask.clone());
    Ok(mask)
}

/// `graph_mask` results by tier, snapshot, and tier mask. Each is computed once even when many
/// queries on the same snapshot ask at the same time (a write stream makes a new snapshot every
/// few milliseconds, and 16 clients would otherwise each resolve it).
type ShadowCell = Arc<tokio::sync::OnceCell<Arc<RoaringBitmap>>>;
static SHADOW_MASKS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, (TierMask, ShadowCell)>>,
> = std::sync::LazyLock::new(Default::default);
/// The newest resolved snapshot per tier and set of immutable segments: its shadowed IDs and the
/// rows left. While the tail only grows, the next snapshot resolves just the IDs added since.
type ShadowLatest = (TierMask, Arc<BTreeSet<Id>>, Arc<RoaringBitmap>);
static SHADOW_LATEST: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, ShadowLatest>>,
> = std::sync::LazyLock::new(Default::default);

fn same_mask(a: &TierMask, b: Option<&Arc<RoaringBitmap>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        (None, None) => true,
        _ => false,
    }
}

/// The rows the warm graph may return for this snapshot: the tier's live rows (`mask`, `None`
/// for all) minus the rows the unindexed tail shadows (overwritten or deleted IDs). The graph
/// still walks through hidden rows, so recall does not suffer, but it never shortlists them and
/// `k` results are `k` live rows. Resolved once per snapshot and tier, then cached.
pub(crate) async fn graph_mask(
    store: &Storage,
    state: &NamespaceState,
    set: &bf_wal::SlabSet,
    at: &crate::lineage::Placement,
    mask: Option<&Arc<RoaringBitmap>>,
) -> Result<Option<Arc<RoaringBitmap>>, ApiError> {
    if state.shadowed.is_empty() {
        return Ok(mask.cloned());
    }
    // Everything that fixes the immutable rows: the tier, its segments, and the tier mask.
    let mut base = format!(
        "{}|{}|{}|{:p}",
        set.dir.key,
        state.manifest_seq,
        state.indexed_through,
        mask.map_or(std::ptr::null(), Arc::as_ptr)
    );
    for map in &at.maps {
        base.push('|');
        base.push_str(map);
    }
    let key = format!("{base}|{}|{}", state.seq, state.shadowed.len());
    let cell = {
        let mut masks = SHADOW_MASKS.lock().unwrap_or_else(|e| e.into_inner());
        match masks.get(&key) {
            Some((input, cell)) if same_mask(input, mask) => cell.clone(),
            _ => {
                // Older snapshots' entries are dropped wholesale; queries still holding one finish.
                if masks.len() >= 64 {
                    masks.clear();
                }
                let cell = ShadowCell::default();
                masks.insert(key, (mask.cloned(), cell.clone()));
                cell
            }
        }
    };
    let live = cell
        .get_or_try_init(|| async {
            let previous = SHADOW_LATEST
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&base)
                .filter(|(input, ids, _)| {
                    same_mask(input, mask)
                        && ids.len() <= state.shadowed.len()
                        && ids.is_subset(&state.shadowed)
                })
                .map(|(_, ids, live)| (ids.clone(), live.clone()));
            let live = shadow_free_rows(store, state, set, at, mask, previous).await?;
            let mut latest = SHADOW_LATEST.lock().unwrap_or_else(|e| e.into_inner());
            if latest.len() >= 16 {
                latest.clear();
            }
            if latest
                .get(&base)
                .is_none_or(|(_, ids, _)| ids.len() <= state.shadowed.len())
            {
                latest.insert(
                    base,
                    (
                        mask.cloned(),
                        Arc::new(state.shadowed.clone()),
                        live.clone(),
                    ),
                );
            }
            Ok::<_, ApiError>(live)
        })
        .await?;
    Ok(Some(live.clone()))
}
/// The tier rows left once the shadowed IDs are hidden. With `previous` (an earlier snapshot's
/// shadowed IDs, a subset of this one's, and its result) only the added IDs are looked up.
async fn shadow_free_rows(
    store: &Storage,
    state: &NamespaceState,
    set: &bf_wal::SlabSet,
    at: &crate::lineage::Placement,
    mask: Option<&Arc<RoaringBitmap>>,
    previous: Option<(Arc<BTreeSet<Id>>, Arc<RoaringBitmap>)>,
) -> Result<Arc<RoaringBitmap>, ApiError> {
    use crate::lineage::Place;
    let added: BTreeSet<Id>;
    let wanted = match &previous {
        Some((ids, _)) => {
            added = state.shadowed.difference(ids).cloned().collect();
            &added
        }
        None => &state.shadowed,
    };
    let positions: Vec<usize> = at
        .positions()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let found = futures::future::try_join_all(
        positions
            .iter()
            .map(|i| segment::lookup_many(store, &state.segments[*i], wanted)),
    )
    .await?;
    let shadow_of = |i: usize| &found[positions.binary_search(&i).expect("looked up above")];
    let mut hidden = RoaringBitmap::new();
    for (cover, place) in set.covered.iter().zip(&at.places) {
        match place {
            Place::Here(i) => {
                hidden.extend(shadow_of(*i).iter().map(|r| cover.base + r));
            }
            Place::Moved(parts) => {
                // Walk the shadowed rows, not the whole map: a map holds every row of the source.
                for (i, map) in parts {
                    hidden.extend(
                        shadow_of(*i)
                            .iter()
                            .filter_map(|to| map.back(to))
                            .map(|from| cover.base + from),
                    );
                }
            }
        }
    }
    let mut live = match (&previous, mask) {
        (Some((_, live)), _) => RoaringBitmap::clone(live),
        (None, Some(m)) => RoaringBitmap::clone(m),
        (None, None) => {
            let mut all = RoaringBitmap::new();
            for c in &set.covered {
                all.insert_range(c.base..c.base + c.rows);
            }
            all
        }
    };
    live -= &hidden;
    Ok(Arc::new(live))
}

/// ANN from one tier's slabs: the same directory search, over slabs as pseudo
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
    allowed: Option<&[Option<Arc<RoaringBitmap>>]>,
    prefetched: Option<&crate::ann_dir::Prefetched>,
    seat: Option<&crate::ann_dir::Seat>,
    graph_expected: bool,
    declined: &std::sync::atomic::AtomicBool,
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    let t0 = std::time::Instant::now();
    stage(t0, "slabs_start");
    let mask = match allowed {
        Some(allowed) => Some(filtered_tier_mask(set, at, allowed)),
        None => tier_mask(store, state, set, at).await?,
    };
    stage(t0, "slabs_mask");
    if trace_stages()
        && let Some(mask) = &mask
    {
        eprintln!("FILTER matching={}", mask.len());
    }
    if mask.as_ref().is_some_and(|m| m.is_empty()) {
        return Ok(Some((BTreeMap::new(), BTreeMap::new())));
    }
    // The graph keeps shadowed rows traversable but out of its shortlist (`graph_mask`), so it
    // returns `k` live rows. The IVF path drops them afterwards and asks for a few extra.
    let slack = if state.shadowed.is_empty() {
        0
    } else {
        (2 * k).max(20)
    };
    // Both graph paths take the snapshot's shadow-free rows (`graph_mask`: the tier mask, which
    // is the filter's allowed rows for a filtered query, minus rows the tail shadows), so each
    // returns `k` live rows with no over-fetch.
    let accelerated = if let (true, Some(mask)) = (graph_expected && allowed.is_some(), &mask) {
        if crate::warm_graph::scan_eligible(state, attribute, set, vector, k, mask) {
            let live = graph_mask(store, state, set, at, Some(mask)).await?;
            match live {
                Some(live) => {
                    crate::warm_graph::scan(
                        store, state, attribute, set, at, vector, live, k, columns,
                    )
                    .await?
                }
                None => None,
            }
        } else {
            None
        }
    } else if allowed.is_none() && crate::warm_graph::available(state, attribute, set, vector, k) {
        let navigate = graph_mask(store, state, set, at, mask.as_ref()).await?;
        crate::warm_graph::search(
            store,
            state,
            attribute,
            set,
            at,
            vector,
            mask.clone(),
            navigate,
            k,
            columns,
        )
        .await?
    } else {
        None
    };
    let graph_served = accelerated.is_some();
    if graph_expected && accelerated.is_none() {
        declined.store(true, std::sync::atomic::Ordering::Relaxed);
        return Ok(None);
    }
    let (mut scores, mut docs) = match accelerated {
        Some(found) => found,
        None => {
            let directory = if allowed.is_some() {
                crate::ann_dir::load(store, &set.dir, None)
                    .await
                    .map(|d| (d, false))
            } else {
                crate::ann_dir::slab_directory(
                    store,
                    set,
                    prefetched,
                    vector,
                    config,
                    attribute,
                    &state.segments,
                )
                .await
            };
            let mut view = state.without_docs();
            stage(t0, "slabs_directory");
            view.segments = set.slabs.clone().into();
            let dir = match directory {
                Ok((dir, _))
                    if crate::ann_dir::covers(&dir, &view.segments, attribute, Layout::Slabs) =>
                {
                    dir
                }
                result => {
                    // Complete tiers have all immutable inputs to reconstruct their directory.
                    // A missing or stale advisory directory must not turn filtered ANN into a scan.
                    metrics::counter!("blowfish_ann_tier_directory_rebuilds_total", "reason" => if result.is_err() { "read" } else { "coverage" }).increment(1);
                    crate::upgrade::directory(store, &view.segments, attribute).await?
                }
            };
            let warm_eligible = mask.is_none() && seat.is_none();
            let members = if allowed.is_some() {
                Some(match crate::read_cache::query_memberships(&set.dir.key) {
                    Some(members) => members,
                    None => {
                        crate::ann_dir::memberships(store, &view.segments, &dir, attribute).await?
                    }
                })
            } else {
                None
            };
            stage(t0, "slabs_memberships");
            let lives = match (&members, &mask) {
                (Some(members), Some(global)) => {
                    futures::future::try_join_all(members.iter().zip(&view.segments).map(
                        |(member, slab)| {
                            let (member, global) = (member.clone(), global.clone());
                            let key = crate::upgrade::files(slab, attribute)
                                .expect("directory checked above")
                                .0
                                .key
                                .clone();
                            crate::ann_dir::on_worker(async move {
                                Ok(Some(match member {
                                    Some(member) => slab_predicate(&key, &member, &global),
                                    None => global,
                                }))
                            })
                        },
                    ))
                    .await?
                }
                _ => vec![mask; view.segments.len()],
            };
            stage(t0, "slabs_predicates");

            let warm = warm_eligible
                .then(|| crate::warm_ann::WarmTier::for_tier(&set.dir.key, dir.clone()))
                .flatten();
            if warm_eligible && warm.is_none() {
                metrics::counter!("blowfish_warm_declined_total", "reason" => "no_tier")
                    .increment(1);
            }
            crate::ann_dir::search(
                store,
                &view,
                dir,
                lives,
                vector,
                config,
                k + slack,
                columns,
                attribute,
                members,
                Layout::Slabs,
                Some((state, &set.covered, at)),
                &[],
                seat,
                warm,
            )
            .await?
        }
    };

    if slack > 0 && !graph_served {
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

/// A cached translated predicate pins its immutable inputs, preventing pointer-key reuse.
struct PredicateMask {
    _inputs: Vec<Option<Arc<RoaringBitmap>>>,
    rows: Arc<RoaringBitmap>,
}
fn bitmap_bytes(rows: &RoaringBitmap) -> u64 {
    let s = rows.statistics();
    s.n_bytes_array_containers
        + s.n_bytes_bitset_containers
        + s.n_bytes_run_containers
        + u64::from(s.n_containers) * 64
}
fn cache_predicate(
    key: &str,
    inputs: Vec<Option<Arc<RoaringBitmap>>>,
    rows: RoaringBitmap,
) -> Arc<RoaringBitmap> {
    let bytes = bitmap_bytes(&rows)
        + inputs
            .iter()
            .flatten()
            .map(|r| bitmap_bytes(r))
            .sum::<u64>()
        + (inputs.len() * 16 + key.len() * 2 + 256) as u64;
    let rows = Arc::new(rows);
    crate::read_cache::insert(
        key,
        bytes,
        PredicateMask {
            _inputs: inputs,
            rows: rows.clone(),
        },
    );
    rows
}
fn filtered_tier_mask(
    set: &bf_wal::SlabSet,
    at: &crate::lineage::Placement,
    allowed: &[Option<Arc<RoaringBitmap>>],
) -> Arc<RoaringBitmap> {
    let mut key = format!("tier-filter:{}", set.dir.key);
    for map in &at.maps {
        key.push('|');
        key.push_str(map);
    }
    let positions: BTreeSet<_> = at.positions().collect();
    let mut inputs = Vec::with_capacity(positions.len());
    for i in positions {
        match &allowed[i] {
            Some(rows) => key.push_str(&format!("|{i}:{:p}", Arc::as_ptr(rows))),
            None => key.push_str(&format!("|{i}:all")),
        }
        inputs.push(allowed[i].clone());
    }
    if let Some(cached) = crate::read_cache::get::<PredicateMask>(&key, 0) {
        return cached.rows.clone();
    }
    // Each covered segment translates its own rows, in parallel (a broad predicate is millions of
    // rows, and this runs once on the query's critical path). Their ranges are disjoint.
    use rayon::prelude::*;
    let parts: Vec<RoaringBitmap> = set
        .covered
        .par_iter()
        .zip(&at.places)
        .map(|(cover, place)| {
            let mut mask = RoaringBitmap::new();
            match place {
                crate::lineage::Place::Here(i) => {
                    if let Some(rows) = &allowed[*i] {
                        mask |=
                            RoaringBitmap::from_sorted_iter(rows.iter().map(|r| cover.base + r))
                                .expect("translated segment rows stay sorted");
                    } else {
                        mask.insert_range(cover.base..cover.base + cover.rows);
                    }
                }
                crate::lineage::Place::Moved(parts) => {
                    for (i, map) in parts {
                        if let Some(rows) = &allowed[*i]
                            && rows.len().saturating_mul(64) < map.to.len()
                        {
                            // A sparse predicate should visit its matching target rows, rather
                            // than every source row of a compaction. Both sides of a lineage map
                            // are ascending, so reverse translation preserves this order.
                            let selected = rows.as_ref() & &map.to;
                            mask |= RoaringBitmap::from_sorted_iter(selected.iter().map(|to| {
                                cover.base + map.back(to).expect("intersected lineage target")
                            }))
                            .expect("reverse lineage rows stay sorted");
                            continue;
                        }
                        mask |= RoaringBitmap::from_sorted_iter(
                            map.pairs()
                                .filter(|(_, to)| {
                                    allowed[*i].as_ref().is_none_or(|a| a.contains(*to))
                                })
                                .map(|(from, _)| cover.base + from),
                        )
                        .expect("lineage source rows stay sorted");
                    }
                }
            }
            mask
        })
        .collect();
    let mut mask = RoaringBitmap::new();
    for part in parts {
        mask |= part;
    }
    cache_predicate(&key, inputs, mask)
}
fn slab_predicate(
    key: &str,
    member: &bf_vector::Membership,
    global: &Arc<RoaringBitmap>,
) -> Arc<RoaringBitmap> {
    let key = format!("slab-filter:{key}:{:p}", Arc::as_ptr(global));
    if let Some(cached) = crate::read_cache::get::<PredicateMask>(&key, 0) {
        return cached.rows.clone();
    }
    cache_predicate(
        &key,
        vec![Some(global.clone())],
        member.intersection(global),
    )
}
