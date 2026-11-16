//! Consistent segment checkpoints plus a bounded, newest-wins WAL overlay.
use crate::{NamespaceState, Operation, filter, log_error, segment};
use bf_storage::Storage;
use bf_types::*;
use bf_wal::{Pending, Wal};
use futures::{FutureExt, TryFutureExt};
use roaring::RoaringBitmap;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

/// What a query will open, so a cold snapshot can fetch it in parallel with the WAL tail.
#[derive(Clone)]
pub(crate) struct OpenHint {
    context: crate::read_cache::ReadContext,
    ann: BTreeSet<String>,
    text: BTreeSet<String>,
    docs: bool,
    /// Filters of ANN sub-queries: their per-segment bitmaps are opened in the snapshot wave.
    filters: Vec<Filter>,
}
impl OpenHint {
    pub(crate) fn for_request(
        request: &QueryRequest,
        context: crate::read_cache::ReadContext,
    ) -> Self {
        let mut hint = Self {
            context,
            ann: BTreeSet::new(),
            text: BTreeSet::new(),
            docs: false,
            filters: Vec::new(),
        };
        for q in request
            .queries
            .as_deref()
            .unwrap_or(std::slice::from_ref(request))
        {
            match &q.rank_by {
                Some(RankBy::Vector {
                    attribute,
                    exact: false,
                    ..
                }) => {
                    hint.ann.insert(attribute.clone());
                    hint.filters.extend(q.filters.clone());
                }
                Some(rank) => {
                    let mut clauses = vec![];
                    crate::indexed::text_clauses(rank, &mut clauses);
                    for clause in clauses {
                        if let RankBy::Bm25 { attribute, .. } = clause {
                            hint.text.insert(attribute.clone());
                        }
                    }
                }
                None => {}
            }
            hint.docs |= q.include_attributes.is_some()
                || q.exclude_attributes.is_some()
                || q.compute_attributes.is_some()
                || q.filters.is_some();
        }
        hint
    }
    /// Open (and cache) segment metadata. Failures are ignored: the query reports them.
    async fn warm(
        &self,
        store: &Storage,
        segments: &[bf_wal::Segment],
        text: Option<&bf_wal::SegmentFile>,
        dirs: &BTreeMap<String, bf_wal::SegmentFile>,
        schema: &Schema,
    ) {
        let context = self.context.clone();
        crate::read_cache::scope(context, async {
            let mut tasks = vec![];
            // A directory replaces opening every segment's ANN metadata, ids, and docs footer.
            let by_dir = self
                .ann
                .iter()
                .all(|a| dirs.contains_key(&format!("ann:{a}")));
            for attribute in &self.ann {
                if let Some(file) = dirs.get(&format!("ann:{attribute}")) {
                    let filtered = !self.filters.is_empty();
                    tasks.push(
                        async move {
                            let dir = crate::ann_dir::load(store, file).await?;
                            if filtered {
                                // Filtered queries also need each segment's row-to-cluster map.
                                crate::ann_dir::memberships(store, segments, &dir, attribute)
                                    .await?;
                            }
                            Ok(())
                        }
                        .boxed(),
                    );
                }
            }
            for seg in segments {
                if by_dir {
                    for f in &self.filters {
                        // Populate the cache with the live mask and the filter's dictionary
                        // blocks; the query recomputes the (cheap) bitmap from them.
                        tasks.push(
                            async move {
                                let live = segment::live(store, seg).await?;
                                segment::filter_rows(store, seg, f, schema, &live)
                                    .await
                                    .map(drop)
                            }
                            .boxed(),
                        );
                    }
                }
                for attribute in self.ann.iter().filter(|_| !by_dir) {
                    if let Some(file) = seg.files.get(&format!("ann:{attribute}")) {
                        tasks.push(segment::vector_index(store, file).map_ok(drop).boxed());
                    }
                }
                for attribute in &self.text {
                    if let Some(file) = seg.files.get(&format!("fts:{attribute}")) {
                        tasks.push(
                            timed("text_index", segment::text_index(store, file))
                                .map_ok(drop)
                                .boxed(),
                        );
                    }
                }
                let small_ids = seg
                    .files
                    .get("ids")
                    .is_none_or(|f| f.size <= crate::indexed::RANGED_IDS_BYTES);
                if (!self.ann.is_empty() && !by_dir) || (!self.text.is_empty() && small_ids) {
                    tasks.push(
                        timed("ids", segment::ids(store, seg))
                            .map(|r| r.map(drop))
                            .boxed(),
                    );
                }
                if (!self.ann.is_empty() && !by_dir) || !self.text.is_empty() {
                    tasks.push(
                        segment::get_value::<RoaringBitmap>(store, &seg.live)
                            .map_ok(drop)
                            .boxed(),
                    );
                }
                if (self.docs && !(by_dir && self.text.is_empty() && !self.ann.is_empty()))
                    || (!self.text.is_empty() && !small_ids)
                {
                    tasks.push(segment::warm_docs(store, seg).map(|()| Ok(())).boxed());
                }
            }
            if !self.text.is_empty()
                && let Some(file) = text
            {
                tasks.push(
                    timed("corpus", crate::text::warm_corpus(store, file))
                        .map(|()| Ok(()))
                        .boxed(),
                );
            }
            futures::future::join_all(tasks).await;
        })
        .await;
    }
}

/// `BF_TRACE_STAGES=1` prints how long each warm-up task took (diagnostics only).
async fn timed<T>(label: &'static str, fut: impl std::future::Future<Output = T>) -> T {
    let start = Instant::now();
    let out = fut.await;
    if std::env::var_os("BF_TRACE_STAGES").is_some() {
        eprintln!("WARM {label} {} ms", start.elapsed().as_millis());
    }
    out
}

pub(crate) async fn catch_up(
    wal: &Wal,
    state: &mut NamespaceState,
    cap: Option<u64>,
    store: &Storage,
) -> Result<(), ApiError> {
    catch_up_hinted(wal, state, cap, store, None).await
}
pub(crate) async fn catch_up_hinted(
    wal: &Wal,
    state: &mut NamespaceState,
    cap: Option<u64>,
    store: &Storage,
    hint: Option<&OpenHint>,
) -> Result<(), ApiError> {
    // Always refresh the manifest before reading a potentially reclaimed tail.
    // A concurrent GC can still race this read; one fresh checkpoint retry resolves it.
    for attempt in 0..3 {
        if let Some((generation, m)) = wal
            .manifest_after(state.manifest_seq)
            .await
            .map_err(log_error)?
        {
            if m.format_version != 1 {
                return Err(ApiError::new(500, "unsupported manifest format"));
            }
            if m.indexed_through > 0 && generation > state.manifest_seq {
                *state = if m.deleted {
                    NamespaceState {
                        seq: m.indexed_through,
                        indexed_through: m.indexed_through,
                        manifest_seq: generation,
                        ..Default::default()
                    }
                } else {
                    NamespaceState {
                        seq: m.indexed_through,
                        exists: true,
                        schema: m.schema,
                        metric: m.distance_metric,
                        created_at: m.created_at,
                        updated_at: m.updated_at.unwrap_or(m.created_at),
                        segments: m.segments,
                        indexed_through: m.indexed_through,
                        manifest_seq: generation,
                        corpus: if m.text_stats.is_some() {
                            Default::default()
                        } else {
                            crate::text::checkpoint_corpus(&m.stats)
                        },
                        text_stats: m.text_stats,
                        ann_dirs: m.ann_dirs,
                        stats: m.stats.into(),
                        ..Default::default()
                    }
                };
            }
            state.manifest_seq = generation;
        }
        state.checkpoint_refreshed = Instant::now();
        let tail = wal.tail_bounded(
            state.seq,
            cap.unwrap_or(u64::MAX).saturating_sub(state.tail_bytes),
        );
        let tail = if let Some(hint) = hint.filter(|_| !state.segments.is_empty()) {
            // The checkpoint's files do not depend on the tail: open them meanwhile.
            let (segments, text) = (state.segments.clone(), state.text_stats.clone());
            let dirs = state.ann_dirs.clone();
            let schema = state.schema.clone();
            tokio::join!(
                tail,
                hint.warm(store, &segments, text.as_ref(), &dirs, &schema)
            )
            .0
        } else {
            tail.await
        };
        match tail {
            Ok(entries) => {
                for (seq, entry) in entries {
                    let size = serde_json::to_vec(&entry).expect("WAL serializes").len() as u64;
                    if cap.is_some_and(|cap| state.tail_bytes.saturating_add(size) > cap) {
                        break;
                    }
                    crate::branch::replay(store, wal, seq, &entry, state).await?;
                }
                state.refreshed = Instant::now();
                return Ok(());
            }
            Err(bf_wal::Error::Pruned | bf_wal::Error::Format) if attempt < 2 => continue,
            Err(e) => return Err(log_error(e)),
        }
    }
    unreachable!()
}
/// Remove tail-shadowed IDs before searching any immutable segment.
pub(crate) async fn allowed(
    store: &Storage,
    state: &NamespaceState,
    seg: &bf_wal::Segment,
    f: Option<&Filter>,
) -> Result<RoaringBitmap, ApiError> {
    allowed_with(store, state, seg, f, true).await
}
/// `allowed`, optionally leaving tail-shadowed rows in (the directory ANN path drops them by
/// ID from its winners instead of opening the segment's ID index).
pub(crate) async fn allowed_with(
    store: &Storage,
    state: &NamespaceState,
    seg: &bf_wal::Segment,
    f: Option<&Filter>,
    unshadow: bool,
) -> Result<RoaringBitmap, ApiError> {
    let mut live = segment::live(store, seg).await?;
    if unshadow && !state.shadowed.is_empty() {
        live -= segment::lookup_many(store, seg, &state.shadowed).await?;
    }
    if let Some(f) = f {
        filter::validate(f, &state.schema, false)?;
        live = segment::filter_rows(store, seg, f, &state.schema, &live).await?;
    }
    Ok(live)
}
pub(crate) async fn materialize(
    store: &Storage,
    state: &NamespaceState,
) -> Result<NamespaceState, ApiError> {
    let mut full = state.clone();
    for seg in &state.segments {
        let live = allowed(store, state, seg, None).await?;
        full.docs
            .extend(crate::shared(segment::documents(store, seg, &live).await?));
    }
    // Schema drops apply immediately even while older segment columns remain on disk.
    for doc in full.docs.values_mut() {
        std::sync::Arc::make_mut(doc).retain(|name, _| state.schema.contains_key(name));
    }
    Ok(full)
}
/// Point writes fetch their current IDs; by-filter and schema-drop writes use a scan.
pub(crate) async fn for_writes(
    store: &Storage,
    state: &NamespaceState,
    pending: &[Pending<Operation, Result<WriteResponse, ApiError>>],
) -> Result<NamespaceState, ApiError> {
    let mut wanted = BTreeSet::new();
    for p in pending {
        let Operation::Write(req, _) = &p.request else {
            continue;
        };
        if req.delete_by_filter.is_some() || req.patch_by_filter.is_some() {
            return materialize(store, state).await;
        }
        for row in req
            .upsert_rows
            .iter()
            .flatten()
            .chain(req.patch_rows.iter().flatten())
        {
            if let Some(value) = row.get("id")
                && let Ok(id) = serde_json::from_value::<Id>(value.clone())
            {
                wanted.insert(id);
            }
        }
        for columns in req.upsert_columns.iter().chain(req.patch_columns.iter()) {
            if let Some(ids) = columns.get("id").and_then(serde_json::Value::as_array) {
                for value in ids {
                    if let Ok(id) = serde_json::from_value::<Id>(value.clone()) {
                        wanted.insert(id);
                    }
                }
            }
        }
        wanted.extend(req.deletes.iter().flatten().cloned());
    }
    if state.schema.get("id").and_then(|s| s.data_type.as_ref())
        == Some(&AttributeType::Scalar(ScalarType::Uuid))
    {
        wanted = wanted
            .into_iter()
            .map(|id| match id {
                Id::String(s) => uuid::Uuid::parse_str(&s)
                    .map(|id| Id::String(id.to_string()))
                    .unwrap_or(Id::String(s)),
                id => id,
            })
            .collect();
    }
    // A write reads only the rows it names: tail rows by ID here, segment rows below.
    let mut full = state.without_docs();
    for id in &wanted {
        if let Some(doc) = state.docs.get(id) {
            full.docs.insert(id.clone(), doc.clone());
        }
    }
    for seg in &state.segments {
        let live = segment::live(store, seg).await?;
        let wanted = wanted.difference(&state.shadowed).cloned().collect();
        let rows = segment::lookup_many(store, seg, &wanted).await? & live;
        full.docs
            .extend(crate::shared(segment::documents(store, seg, &rows).await?));
    }
    for doc in full.docs.values_mut() {
        std::sync::Arc::make_mut(doc).retain(|name, _| state.schema.contains_key(name));
    }
    Ok(full)
}

/// Exact row and logical-byte totals without materializing unaffected segment rows.
pub(crate) async fn statistics(
    store: &Storage,
    state: &NamespaceState,
) -> Result<bf_wal::NamespaceStats, ApiError> {
    let mut stats = (*state.stats).clone();
    if !state.shadowed.is_empty() {
        for seg in &state.segments {
            let rows = segment::lookup_many(store, seg, &state.shadowed).await?
                & segment::live(store, seg).await?;
            let old = segment::documents(store, seg, &rows).await?;
            stats.rows = stats.rows.saturating_sub(old.len() as u64);
            let bytes: u64 = old
                .values()
                .map(|d| serde_json::to_vec(d).expect("document serializes").len() as u64)
                .sum();
            stats.logical_bytes = stats.logical_bytes.saturating_sub(bytes);
        }
    }
    stats.rows += state.docs.len() as u64;
    stats.logical_bytes += state
        .docs
        .values()
        .map(|d| serde_json::to_vec(&**d).expect("document serializes").len() as u64)
        .sum::<u64>();
    Ok(stats)
}
