//! Cross-segment ANN: rank leaf clusters of every segment together, scan only the globally
//! nearest clusters, and rerank one merged candidate pool. Probing each segment separately
//! scans and reranks once per segment, so cost grew with the segment count instead of the
//! corpus size.
use crate::{EngineConfig, NamespaceState, ann_dir::Layout, segment};
use bf_storage::Storage;
use bf_types::*;
use bf_vector::RoaringBitmap;
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
    // A namespace-wide recluster (slabs) answers unfiltered queries while it covers exactly
    // the live segments; otherwise (new segments since it was built) fall through.
    if q.filters.is_none()
        && let Some(set) = state.ann_slabs.get(attribute)
        && let Some(found) = from_slabs(
            store, state, attribute, vector, config, k, columns, set, None,
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
        eprintln!("STAGE {name} {} ms", t0.elapsed().as_millis());
    }
}

/// Whether the slab set covers exactly the live segments, in order.
pub(crate) fn slabs_cover(segments: &[bf_wal::Segment], set: &bf_wal::SlabSet) -> bool {
    segments.len() == set.covered.len()
        && segments
            .iter()
            .zip(&set.covered)
            .all(|(s, c)| s.id == c.segment && s.row_count == c.rows)
}

/// Unfiltered ANN from a recluster's slabs: the same directory search, over slabs as pseudo
/// segments, with winners mapped back to their segments for hydration.
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
    prefetched: Option<&crate::ann_dir::Prefetched>,
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    if !slabs_cover(&state.segments, set) {
        return Ok(None);
    }
    let Ok((dir, _)) = crate::ann_dir::slab_directory(store, set, prefetched, vector, config).await
    else {
        return Ok(None);
    };
    let mut view = state.without_docs();
    view.segments = set.slabs.clone().into();
    if !crate::ann_dir::covers(&dir, &view.segments, attribute, Layout::Slabs) {
        return Ok(None);
    }
    // Rows deleted since the slabs were built: a mask over global row numbers.
    let changed: Vec<usize> = (0..state.segments.len())
        .filter(|i| state.segments[*i].live_generation != set.covered[*i].live_generation)
        .collect();
    let mask = if changed.is_empty() {
        None
    } else {
        let mut mask = RoaringBitmap::new();
        let loaded = futures::future::try_join_all(
            changed
                .iter()
                .map(|i| segment::live(store, &state.segments[*i])),
        )
        .await?;
        for (n, i) in changed.iter().enumerate() {
            let c = &set.covered[*i];
            mask.insert_range(c.base..c.base + c.rows);
            let mut live = RoaringBitmap::new();
            for r in &loaded[n] {
                live.insert(c.base + r);
            }
            mask -= RoaringBitmap::from_iter(c.base..c.base + c.rows);
            mask |= live;
        }
        // Unchanged segments stay fully live.
        for (i, c) in set.covered.iter().enumerate() {
            if !changed.contains(&i) {
                mask.insert_range(c.base..c.base + c.rows);
            }
        }
        Some(Arc::new(mask))
    };
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
        Some((state, &set.covered)),
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
