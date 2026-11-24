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
    // A recluster's tiers answer vector queries for the rows they cover, and the namespace
    // directory for the rest (segments written since).
    if let Some(tiers) = state.ann_tiers.get(attribute)
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
            state,
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
            state,
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
    source: &NamespaceState,
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    // Filtered queries compute each segment's allowed rows (live rows that pass the filter)
    // alongside the directory, and read each segment's row-to-cluster map, so the
    // directory ranks clusters by their allowed rows without opening any segment's index
    // or ID index.
    let (dir, lives) = tokio::join!(
        async {
            match file {
                Some(f) => {
                    let dir = crate::ann_dir::load(store, f, None).await?;
                    if crate::ann_dir::covers(&dir, &state.segments, attribute, layout) {
                        Ok(dir)
                    } else {
                        crate::upgrade::directory(store, &state.segments, attribute).await
                    }
                }
                None => crate::upgrade::directory(store, &state.segments, attribute).await,
            }
        },
        crate::ann_dir::live_masks(store, state, source, q.filters.as_ref())
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
                source,
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

/// ANN over a recluster's tiers and the segments no tier covers, as one plan: each
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
                    q.filters.as_ref(),
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
            .enumerate()
            .any(|(i, s)| !covered[i] && crate::upgrade::files(s, attribute).is_none())
    {
        return Ok(None);
    }
    let rest = uncovered.then(|| {
        let covered = &covered;
        async move {
            let mut rest_state = state.without_docs();
            rest_state.segments = state
                .segments
                .iter()
                .enumerate()
                .filter(|(i, _)| !covered[*i])
                .map(|(_, s)| s.clone())
                .collect::<Vec<_>>()
                .into();
            let file = crate::upgrade::directory_file(&state.ann_dirs, attribute).map(|(f, _)| f);
            from_directory(
                store,
                &rest_state,
                q,
                attribute,
                vector,
                config,
                k,
                columns,
                file,
                Layout::Segments,
                &[],
                state,
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

/// Masked or unfiltered ANN from one tier's slabs: the same directory search, over slabs as pseudo
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
    filter: Option<&Filter>,
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    let allowed = if let Some(filter) = filter {
        Some(filtered_slab_masks(store, state, set, at, filter, attribute).await?)
    } else {
        None
    };
    // A paged directory must expose enough leaves for the selectivity-adjusted probe, too.
    // Size it from actual vector rows, including nullable columns and compaction lineage.
    let directory_config = if let Some(allowed) = &allowed {
        let matching = allowed
            .iter()
            .flatten()
            .map(|rows| rows.len() as usize)
            .sum::<usize>();
        if matching == 0 {
            return Ok(Some((BTreeMap::new(), BTreeMap::new())));
        }
        let rows = set
            .slabs
            .iter()
            .map(|slab| slab.row_count as usize)
            .sum::<usize>();
        let mut tuning = config.clone();
        tuning.ann_nprobe = (tuning.ann_nprobe as f64
            * crate::ann_dir::filtered_tier_scale(rows, matching, k))
        .ceil() as usize;
        Some(tuning)
    } else {
        None
    };
    let Ok((dir, _)) = crate::ann_dir::slab_directory(
        store,
        set,
        prefetched,
        vector,
        directory_config.as_ref().unwrap_or(config),
    )
    .await
    else {
        return Ok(None);
    };
    let mut view = state.without_docs();
    view.segments = set.slabs.clone().into();
    if !crate::ann_dir::covers(&dir, &view.segments, attribute, Layout::Slabs) {
        return Ok(None);
    }
    let mask = if filter.is_none() {
        tier_mask(store, state, set, at).await?
    } else {
        None
    };
    if crate::ann::trace_stages() {
        eprintln!(
            "SLABS slabs={} masked={}",
            view.segments.len(),
            allowed.is_some() || mask.is_some()
        );
    }
    let warm = if allowed.is_none() && mask.is_none() && seat.is_none() {
        crate::warm_ann::WarmTier::for_tier(&set.dir.key, dir.clone())
    } else {
        None
    };
    let lives = allowed.unwrap_or_else(|| vec![mask; view.segments.len()]);
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
        filter.map(|_| vec![None; view.segments.len()]),
        Layout::Slabs,
        Some((state, &set.covered, at)),
        &[],
        seat,
        state,
        warm,
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

// Derived masks describe only immutable indexed rows. Branches can share tiers while having
// equal WAL/manifest sequence numbers, so use their actual file, live, schema, and lineage
// identities. Tail shadows bypass the derived cache; segment predicates remain reusable.
fn slab_filter_generation(
    state: &NamespaceState,
    set: &bf_wal::SlabSet,
    filter: &Filter,
    attribute: &str,
) -> Result<Option<String>, ApiError> {
    if !state.shadowed.is_empty() {
        return Ok(None);
    }
    let segments: Vec<_> = state
        .segments
        .iter()
        .map(|s| {
            let files: Vec<_> = s
                .files
                .iter()
                .map(|(role, file)| (role, &file.key))
                .collect();
            (&s.id, &s.live.key, &s.schema, files)
        })
        .collect();
    let lineage: Vec<_> = state
        .lineage
        .iter()
        .map(|l| {
            let parts: Vec<_> = l
                .parts
                .iter()
                .map(|p| (&p.target, p.rows, &p.map.key))
                .collect();
            (&l.source, parts)
        })
        .collect();
    serde_json::to_string(&(
        1u8,
        attribute,
        &set.dir.key,
        &set.covered,
        filter,
        &state.schema,
        segments,
        lineage,
    ))
    .map(Some)
    .map_err(segment::index_error)
}

/// Filtered slab membership, cached in the byte-bounded predicate pool. Cold row lists and
/// predicates are independent reads, so open them together; warm queries reuse the intersections.
async fn filtered_slab_masks(
    store: &Storage,
    state: &NamespaceState,
    set: &bf_wal::SlabSet,
    at: &crate::lineage::Placement,
    filter: &Filter,
    attribute: &str,
) -> Result<Vec<Option<Arc<RoaringBitmap>>>, ApiError> {
    let keys = slab_filter_generation(state, set, filter, attribute)?
        .map(|generation| {
            set.slabs
                .iter()
                .map(|slab| {
                    let identity = serde_json::to_string(&(
                        &slab.id,
                        slab.row_count,
                        slab.files.get(&format!("annv2:{attribute}")),
                        slab.files.get("vector_rows"),
                    ))
                    .map_err(segment::index_error)?;
                    // Both components are complete serialized values, with unambiguous boundaries.
                    Ok::<_, ApiError>(format!("slab-filter:[{generation},{identity}]"))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let mut allowed: Vec<_> = set
        .slabs
        .iter()
        .enumerate()
        .map(|(i, _)| {
            keys.as_ref()
                .and_then(|keys| crate::read_cache::predicate(&keys[i]))
        })
        .collect();
    let missing: Vec<_> = allowed
        .iter()
        .enumerate()
        .filter_map(|(i, mask)| mask.is_none().then_some(i))
        .collect();
    if missing.is_empty() {
        return Ok(allowed);
    }
    let lists = futures::future::try_join_all(
        missing
            .iter()
            .map(|&i| crate::vector_locator::rows(store, &set.slabs[i], set, attribute)),
    );
    let (mask, lists) = tokio::try_join!(filtered_mask(store, state, set, at, filter), lists)?;
    for (&i, rows) in missing.iter().zip(lists) {
        let selected: RoaringBitmap = rows.iter().copied().filter(|r| mask.contains(*r)).collect();
        allowed[i] = Some(match &keys {
            Some(keys) => crate::read_cache::insert_predicate(&keys[i], selected),
            None => Arc::new(selected),
        });
    }
    Ok(allowed)
}

/// Shift exact per-segment filter results back through lineage into the tier's global rows.
/// This includes live masks and tail shadows, so a slab scan cannot return an older version.
async fn filtered_mask(
    store: &Storage,
    state: &NamespaceState,
    set: &bf_wal::SlabSet,
    at: &crate::lineage::Placement,
    filter: &Filter,
) -> Result<Arc<RoaringBitmap>, ApiError> {
    use crate::lineage::Place;
    let positions: BTreeSet<_> = at.positions().collect();
    let loaded = futures::future::try_join_all(
        positions
            .iter()
            .map(|&i| crate::snapshot::allowed(store, state, &state.segments[i], Some(filter))),
    )
    .await?;
    let allowed: BTreeMap<_, _> = positions.into_iter().zip(loaded).collect();
    let mut mask = RoaringBitmap::new();
    for (c, place) in set.covered.iter().zip(&at.places) {
        match place {
            Place::Here(i) => {
                mask.extend(
                    allowed[i]
                        .iter()
                        .filter(|r| *r < c.rows)
                        .map(|r| c.base + r),
                );
            }
            Place::Moved(parts) => {
                for (i, map) in parts {
                    mask.extend(
                        map.pairs()
                            .filter(|(_, to)| allowed[i].contains(*to))
                            .map(|(from, _)| c.base + from),
                    );
                }
            }
        }
    }
    Ok(Arc::new(mask))
}

#[cfg(test)]
mod slab_mask_tests {
    use super::*;
    use crate::{Engine, ReclusterOptions, read_cache};
    use serde_json::json;
    use std::{path::Path, time::Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn filtered_slab_masks_reuse_immutable_rows_and_bypass_shadows_and_new_live_files() {
        let store = Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let engine = Engine::with_config(
            store.clone(),
            EngineConfig {
                indexing: false,
                retention: Duration::ZERO,
                recall_sample_rate: 0.0,
                compaction_segments: 100,
                ..Default::default()
            },
        );
        for batch in 0..3 {
            let rows: Vec<_> = (batch * 600..(batch + 1) * 600)
                .map(|id| json!({"id":id,"group":id%5,"vector":[(id as f32).sin(),0.5,0.25,1.0]}))
                .collect();
            engine.write("masks", serde_json::from_value(json!({
                "distance_metric":"euclidean_squared", "schema":{"vector":{"type":"[4]f16"}},
                "upsert_rows":rows,
            })).unwrap()).await.unwrap();
            engine.index_namespace("masks", true).await.unwrap();
        }
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.scratch/d4-cache-tests");
        std::fs::create_dir_all(&base).unwrap();
        let scratch = tempfile::tempdir_in(base).unwrap();
        engine
            .recluster(
                "masks",
                &ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 2,
                    cluster_rows: 30,
                    max_slab_rows: 600,
                    scratch: scratch.path().into(),
                },
            )
            .await
            .unwrap();
        engine.collect_garbage("masks").await.unwrap();
        let (state, _) = engine
            .snapshot("masks", &Consistency::Strong)
            .await
            .unwrap();
        assert!(
            state
                .segments
                .iter()
                .all(|s| !s.files.contains_key("annv2:vector"))
        );
        let set = &state.ann_tiers["vector"][0];
        let at = crate::lineage::place(&store, &state.segments, &state.lineage, set)
            .await
            .unwrap()
            .unwrap();
        let filter: Filter = serde_json::from_value(json!(["group", "Eq", 3])).unwrap();
        let context = read_cache::ReadContext::new(engine.0.read_cache.clone());
        read_cache::scope(context, async {
            let first = filtered_slab_masks(&store, &state, set, &at, &filter, "vector")
                .await
                .unwrap();
            let second = filtered_slab_masks(&store, &state, set, &at, &filter, "vector")
                .await
                .unwrap();
            assert!(
                first
                    .iter()
                    .zip(&second)
                    .all(|(a, b)| Arc::ptr_eq(a.as_ref().unwrap(), b.as_ref().unwrap()))
            );
            let union = |masks: &[Option<Arc<RoaringBitmap>>]| -> RoaringBitmap {
                masks
                    .iter()
                    .flatten()
                    .flat_map(|mask| mask.iter())
                    .collect()
            };
            assert_eq!(union(&first).len(), 360);
            let mut shadowed = state.clone();
            shadowed.shadowed.insert(Id::Uint(3));
            let third = filtered_slab_masks(&store, &shadowed, set, &at, &filter, "vector")
                .await
                .unwrap();
            assert_eq!(
                union(&third).len(),
                359,
                "a branch or WAL shadow must bypass the cached tier mask"
            );
            // A different immutable live generation must miss, rather than reuse a valid old mask.
            let mut changed = state.clone();
            let mut segments = state.segments.to_vec();
            segments[0].live.key = "missing-new-live-generation".into();
            changed.segments = segments.into();
            assert!(
                filtered_slab_masks(&store, &changed, set, &at, &filter, "vector")
                    .await
                    .is_err()
            );
        })
        .await;
    }
}
