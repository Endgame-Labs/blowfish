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
    /// BM25 clauses `(attribute, query value, last_as_prefix)`: their terms are looked up (the
    /// dictionary chunks, term records, and corpus statistics chunks) in the snapshot wave.
    bm25: Vec<(String, serde_json::Value, bool)>,
    docs: bool,
    /// Filters of ANN sub-queries: their per-segment bitmaps are opened in the snapshot wave.
    filters: Vec<Filter>,
    /// A lone unfiltered ANN query `(attribute, vector, k)`: its scan reads start in the snapshot
    /// wave too, overlapping the WAL tail read.
    scan: Option<(String, std::sync::Arc<[f32]>, usize)>,
    config: crate::EngineConfig,
}
/// Which part of the warm-up to run.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Part {
    All,
    /// Everything except text segments, statistics, and the files they need.
    WithoutText,
    OnlyText,
}
impl OpenHint {
    /// Whether the query ranks by text or filters on a text index, which read the segments' text
    /// headers from the manifest.
    fn wants_text(&self) -> bool {
        !self.text.is_empty() || !self.bm25.is_empty()
    }
    /// A hybrid query's ANN half needs only the directory, so it must not wait for the BM25
    /// term lookups (they finish a few hundred milliseconds later on a cold namespace).
    fn splits_text(&self, dirs: &BTreeMap<String, bf_wal::SegmentFile>) -> bool {
        !self.ann.is_empty()
            && !self.text.is_empty()
            && self
                .ann
                .iter()
                .all(|a| dirs.contains_key(&format!("ann:{a}")))
    }
    pub(crate) fn for_request(
        request: &QueryRequest,
        context: crate::read_cache::ReadContext,
        config: &crate::EngineConfig,
        prefetch_scan: bool,
    ) -> Self {
        let mut hint = Self {
            config: config.clone(),
            scan: None,
            context,
            ann: BTreeSet::new(),
            text: BTreeSet::new(),
            bm25: Vec::new(),
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
                        if let RankBy::Bm25 {
                            attribute,
                            query,
                            params,
                        } = clause
                        {
                            hint.text.insert(attribute.clone());
                            hint.bm25.push((
                                attribute.clone(),
                                query.clone(),
                                crate::text::prefix(params.as_ref()),
                            ));
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
        if prefetch_scan
            && request.queries.is_none()
            && let Some(RankBy::Vector {
                attribute,
                exact: false,
                vector,
                ..
            }) = &request.rank_by
            && request.filters.is_none()
            && request.limit.as_ref().and_then(Limit::per).is_none()
        {
            hint.scan = Some((
                attribute.clone(),
                vector.as_slice().into(),
                request.total().unwrap_or(10) + request.offset,
            ));
        }
        hint
    }
    /// Open (and cache) segment metadata. Failures are ignored: the query reports them.
    #[allow(clippy::too_many_arguments)]
    async fn warm(
        &self,
        part: Part,
        store: &Storage,
        segments: &[bf_wal::Segment],
        text: Option<&bf_wal::SegmentFile>,
        dirs: &BTreeMap<String, bf_wal::SegmentFile>,
        slabs: &BTreeMap<String, bf_wal::SlabSet>,
        prefetched: &BTreeMap<String, crate::ann_dir::Prefetched>,
        schema: &Schema,
    ) {
        let (with_text, with_rest) = (part != Part::WithoutText, part != Part::OnlyText);
        let context = self.context.clone();
        crate::read_cache::scope(context, async {
            let mut queries: BTreeMap<&str, Vec<bf_text::Query>> = BTreeMap::new();
            let mut terms = crate::text::QueryTerms::new();
            for (attribute, value, prefix) in &self.bm25 {
                if let Some(cfg) = schema.get(attribute).and_then(|s| s.text_config()) {
                    let tokens = crate::text::tokens(value, &cfg);
                    terms
                        .entry(attribute.clone())
                        .or_default()
                        .extend(tokens.iter().map(|t| t.text.clone()));
                    queries
                        .entry(attribute)
                        .or_default()
                        .push(bf_text::Query::new(tokens, *prefix));
                }
            }
            let queries = &queries;
            let mut tasks = vec![];
            // A directory replaces opening every segment's ANN metadata, ids, and docs footer.
            let by_dir = self
                .ann
                .iter()
                .all(|a| dirs.contains_key(&format!("ann:{a}")));
            for attribute in self.ann.iter().filter(|_| with_rest) {
                // A lone unfiltered ANN query reads the block-layout directory when there is one.
                // A recluster's slabs, when they cover exactly these segments, take the scan.
                let slab = self
                    .scan
                    .as_ref()
                    .filter(|(a, _, _)| a == attribute)
                    .filter(|_| crate::ann::slabs_enabled())
                    .and_then(|_| slabs.get(attribute))
                    .filter(|set| crate::ann::slabs_cover(segments, set));
                if let Some(set) = slab
                    && let Some((_, vector, k)) = self.scan.as_ref()
                {
                    let config = &self.config;
                    let name = crate::ann_dir::Layout::Slabs.dir_key(attribute);
                    tasks.push(
                        async move {
                            let (dir, warm) = crate::ann_dir::slab_directory(
                                store,
                                set,
                                prefetched.get(&name),
                                vector,
                                config,
                            )
                            .await?;
                            // Everything this query touches is already in memory: the scan's
                            // reads would only copy cached bytes, and the query itself reads them.
                            if warm {
                                return Ok(());
                            }
                            crate::ann_dir::prefetch_scan(
                                store,
                                &set.slabs,
                                &dir,
                                attribute,
                                crate::ann_dir::Layout::Slabs,
                                vector,
                                *k,
                                config,
                            )
                            .await
                        }
                        .boxed(),
                    );
                    continue;
                }
                let blocks = self
                    .scan
                    .as_ref()
                    .filter(|(a, _, _)| a == attribute)
                    .filter(|_| crate::ann_dir::blocks_enabled())
                    .and_then(|_| dirs.get(&crate::ann_dir::Layout::Blocks.dir_key(attribute)));
                let chosen = match blocks {
                    Some(file) => Some((
                        file,
                        crate::ann_dir::Layout::Blocks,
                        crate::ann_dir::Layout::Blocks.dir_key(attribute),
                    )),
                    None => dirs
                        .get(&format!("ann:{attribute}"))
                        .map(|f| (f, crate::ann_dir::Layout::Plain, attribute.clone())),
                };
                if let Some((file, layout, hint_name)) = chosen {
                    let filtered = !self.filters.is_empty();
                    let scan = self.scan.as_ref().filter(|(a, _, _)| a == attribute);
                    let config = &self.config;
                    tasks.push(
                        async move {
                            let dir = crate::ann_dir::load(store, file, prefetched.get(&hint_name))
                                .await?;
                            if filtered {
                                // Filtered queries also need each segment's row-to-cluster map.
                                crate::ann_dir::memberships(store, segments, &dir, attribute)
                                    .await?;
                            } else if let Some((_, vector, k)) = scan {
                                crate::ann_dir::prefetch_scan(
                                    store, segments, &dir, attribute, layout, vector, *k, config,
                                )
                                .await?;
                            }
                            Ok(())
                        }
                        .boxed(),
                    );
                }
            }
            for seg in segments {
                if by_dir && with_rest {
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
                for attribute in self.ann.iter().filter(|_| !by_dir && with_rest) {
                    if let Some(file) = seg.files.get(&format!("ann:{attribute}")) {
                        tasks.push(segment::vector_index(store, file).map_ok(drop).boxed());
                    }
                }
                for attribute in self.text.iter().filter(|_| with_text) {
                    if let Some(file) = seg.files.get(&format!("fts:{attribute}")) {
                        tasks.push(
                            timed("text_index", async move {
                                let reader = segment::text_index(store, file).await?;
                                // The query's dictionary chunks and term records.
                                for q in queries.get(attribute.as_str()).into_iter().flatten() {
                                    reader
                                        .prefetch_plan(q, None)
                                        .await
                                        .map_err(segment::index_error)?;
                                }
                                Ok(())
                            })
                            .boxed(),
                        );
                    }
                }
                let small_ids = seg
                    .files
                    .get("ids")
                    .is_none_or(|f| f.size <= crate::indexed::RANGED_IDS_BYTES);
                if (with_rest && !self.ann.is_empty() && !by_dir)
                    || (with_text && !self.text.is_empty() && small_ids)
                {
                    tasks.push(
                        timed("ids", segment::ids(store, seg))
                            .map(|r| r.map(drop))
                            .boxed(),
                    );
                }
                if (with_rest && !self.ann.is_empty() && !by_dir)
                    || (with_text && !self.text.is_empty())
                {
                    tasks.push(
                        segment::get_value::<RoaringBitmap>(store, &seg.live)
                            .map_ok(drop)
                            .boxed(),
                    );
                }
                if (with_rest
                    && self.docs
                    && !(by_dir && self.text.is_empty() && !self.ann.is_empty()))
                    || (with_text && !self.text.is_empty() && !small_ids)
                {
                    tasks.push(segment::warm_docs(store, seg).map(|()| Ok(())).boxed());
                }
            }
            if with_text
                && !self.text.is_empty()
                && let Some(file) = text
            {
                tasks.push(
                    timed("corpus", crate::text::warm_corpus(store, file, &terms))
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
/// `BF_READER_COPY=0` makes queries read the full manifest (for A/B runs).
fn reader_copy_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("BF_READER_COPY").is_ok_and(|v| v != "0")
            || std::env::var_os("BF_READER_COPY").is_none()
    })
}
/// A manifest that republishes the checkpoint this state already holds (garbage collection and
/// epoch claims do) changes nothing the state depends on, so it must not discard the resident
/// tail: rebuilding would download every unindexed WAL payload again.
fn same_checkpoint(state: &NamespaceState, m: &bf_wal::Manifest) -> bool {
    state.exists
        && !m.deleted
        && m.indexed_through == state.indexed_through
        && state.segments.len() == m.segments.len()
        && state
            .segments
            .iter()
            .zip(&m.segments)
            .all(|(a, b)| a.id == b.id && a.live.key == b.live.key)
        && state.text_stats.as_ref().map(|f| &f.key) == m.text_stats.as_ref().map(|f| &f.key)
        && state.ann_dirs.len() == m.ann_dirs.len()
        && state
            .ann_dirs
            .iter()
            .all(|(k, f)| m.ann_dirs.get(k).is_some_and(|g| g.key == f.key))
        && state.ann_slabs.len() == m.ann_slabs.len()
        && state
            .ann_slabs
            .iter()
            .all(|(k, f)| m.ann_slabs.get(k).is_some_and(|g| g.dir.key == f.dir.key))
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
        // A cold open reads the newest ANN directory's hint copy while it reads the manifest; the
        // directory's key inside it is checked against the manifest before use.
        // A query that needs no text header reads the compact reader copy of the manifest;
        // everything else (writers, indexers, text queries) wants the full one, which also
        // replaces a compact one this state holds.
        let want_full = hint.is_none_or(OpenHint::wants_text) || !reader_copy_enabled();
        let (view, known) = if !want_full {
            (bf_wal::ManifestView::Reader, state.manifest_seq)
        } else if state.root_only {
            (
                bf_wal::ManifestView::Full,
                state.manifest_seq.saturating_sub(1),
            )
        } else {
            (bf_wal::ManifestView::Full, state.manifest_seq)
        };
        let (manifest, prefetched) = tokio::join!(wal.manifest_view_after(known, view), async {
            let mut found = BTreeMap::new();
            if let Some(hint) = hint.filter(|_| state.ann_dirs.is_empty()) {
                let reads = hint.ann.iter().map(|a| async move {
                    // A lone unfiltered ANN query wants the block-layout directory: try its hint
                    // first, and the plain one when the namespace has none.
                    let mut got = Vec::new();
                    if hint.scan.is_some() && crate::ann_dir::blocks_enabled() {
                        // The recluster's paged slab directory, its flat slab directory, and
                        // the block directory are fetched together: which one answers is decided
                        // once the manifest is read. They are tried in that order, and the
                        // slower ones are dropped as soon as an earlier one is found, so a big
                        // flat directory is not downloaded in full for nothing.
                        let names = [
                            crate::ann_dir::paged_hint_name(a),
                            crate::ann_dir::Layout::Slabs.dir_key(a),
                            crate::ann_dir::Layout::Blocks.dir_key(a),
                        ];
                        let mut pending: Vec<futures::future::BoxFuture<'_, _>> = names
                            .iter()
                            .enumerate()
                            .map(|(i, n)| {
                                async move {
                                    (i, crate::ann_dir::prefetch(store, wal.namespace(), n).await)
                                }
                                .boxed()
                            })
                            .collect();
                        let mut results: [Option<Option<crate::ann_dir::Prefetched>>; 3] =
                            Default::default();
                        'priority: for want in 0..3 {
                            while results[want].is_none() {
                                let ((i, r), _, rest) = futures::future::select_all(pending).await;
                                pending = rest;
                                results[i] = Some(r);
                            }
                            if results[want].as_ref().is_some_and(Option::is_some) {
                                break 'priority;
                            }
                        }
                        drop(pending);
                        // Readers look a hint up by the directory's own name.
                        let reader_names = [
                            crate::ann_dir::Layout::Slabs.dir_key(a),
                            crate::ann_dir::Layout::Slabs.dir_key(a),
                            crate::ann_dir::Layout::Blocks.dir_key(a),
                        ];
                        let first = results
                            .into_iter()
                            .zip(reader_names)
                            .find_map(|(r, name)| r.flatten().map(|p| (name, p)));
                        if let Some((name, p)) = first {
                            // A paged slab directory's header says what the scan will read:
                            // start those reads now, while the manifest is still in flight.
                            if let Some((attribute, vector, k)) =
                                hint.scan.as_ref().filter(|(s, _, _)| s == a)
                            {
                                let (store, context, config) =
                                    (store.clone(), hint.context.clone(), hint.config.clone());
                                let (attribute, vector, k, p) =
                                    (attribute.clone(), vector.clone(), *k, p.clone());
                                tokio::spawn(crate::read_cache::scope(context, async move {
                                    crate::ann_dir::speculate_slab_scan(
                                        &store, p, &attribute, &vector, k, &config,
                                    )
                                    .await
                                }));
                            }
                            got.push((name, p));
                            return got;
                        }
                    }
                    got.extend(
                        crate::ann_dir::prefetch(store, wal.namespace(), a)
                            .await
                            .map(|p| (a.clone(), p)),
                    );
                    got
                });
                found.extend(futures::future::join_all(reads).await.into_iter().flatten());
            }
            found
        });
        if let Some((generation, m)) = manifest.map_err(log_error)? {
            if m.format_version != 1 {
                return Err(ApiError::new(500, "unsupported manifest format"));
            }
            if m.indexed_through > 0
                && generation > state.manifest_seq
                && !same_checkpoint(state, &m)
            {
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
                        segments: m.segments.into(),
                        root_only: m.root,
                        indexed_through: m.indexed_through,
                        manifest_seq: generation,
                        corpus: if m.text_stats.is_some() {
                            Default::default()
                        } else {
                            crate::text::checkpoint_corpus(&m.stats)
                        },
                        text_stats: m.text_stats,
                        ann_dirs: m.ann_dirs,
                        ann_slabs: m.ann_slabs.into(),
                        stats: m.stats.into(),
                        ..Default::default()
                    }
                };
            } else if state.root_only
                && !m.root
                && m.indexed_through > 0
                && same_checkpoint(state, &m)
            {
                // The same checkpoint in full: take the text headers the compact copy left out.
                state.segments = m.segments.into();
                state.root_only = false;
            }
            state.manifest_seq = generation.max(state.manifest_seq);
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
            let slabs = state.ann_slabs.clone();
            let schema = state.schema.clone();
            if hint.splits_text(&dirs) {
                // The text half keeps warming behind the query: its reads are single-flighted
                // with the query's own, so nothing is fetched twice.
                let (hint, store, segments, text, dirs, slabs, schema) = (
                    hint.clone(),
                    store.clone(),
                    segments.clone(),
                    text.clone(),
                    dirs.clone(),
                    slabs.clone(),
                    schema.clone(),
                );
                let none = BTreeMap::new();
                tokio::spawn(async move {
                    hint.warm(
                        Part::OnlyText,
                        &store,
                        &segments,
                        text.as_ref(),
                        &dirs,
                        &slabs,
                        &none,
                        &schema,
                    )
                    .await
                });
            }
            let part = if hint.splits_text(&dirs) {
                Part::WithoutText
            } else {
                Part::All
            };
            tokio::join!(
                tail,
                hint.warm(
                    part,
                    store,
                    &segments,
                    text.as_ref(),
                    &dirs,
                    &slabs,
                    &prefetched,
                    &schema
                )
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
    overlay: &[std::sync::Arc<bf_wal::WalEntry>],
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
    // Groups that are staged but not yet committed come first in the log: a write must see their
    // effect on the rows it names (and their schema), without applying every in-flight document.
    for entry in overlay {
        for batch in &entry.batches {
            full.apply_filtered(batch, &wanted);
        }
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
