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
    /// A cold staged search is already opening these files; avoid duplicate prefetch work.
    pub(crate) searching: bool,
    context: crate::read_cache::ReadContext,
    ann: BTreeSet<String>,
    text: BTreeSet<String>,
    /// BM25 clauses `(attribute, query value, last_as_prefix)`: their terms are looked up (the
    /// dictionary chunks, term records, and corpus statistics chunks) in the snapshot wave.
    bm25: Vec<(String, serde_json::Value, bool)>,
    docs: bool,
    /// Every sub-query can return text hits using only compact row-to-ID maps.
    only_text_ids: bool,
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

fn tiers_cover_all(
    segments: &[bf_wal::Segment],
    lineage: &[bf_wal::Lineage],
    tiers: &[bf_wal::SlabSet],
) -> bool {
    if !crate::ann::tiers_usable(segments, lineage, tiers) {
        return false;
    }
    let sources: BTreeSet<&str> = tiers
        .iter()
        .flat_map(|set| set.covered.iter().map(|c| c.segment.as_str()))
        .collect();
    segments.iter().all(|segment| {
        sources.contains(segment.id.as_str())
            || lineage.iter().any(|entry| {
                sources.contains(entry.source.as_str())
                    && entry.parts.iter().any(|p| p.target == segment.id)
            })
    })
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
                .all(|a| crate::upgrade::directory_file(dirs, a).is_some())
    }
    pub(crate) fn for_request(
        request: &QueryRequest,
        context: crate::read_cache::ReadContext,
        config: &crate::EngineConfig,
        prefetch_scan: bool,
    ) -> Self {
        let mut hint = Self {
            searching: false,
            config: config.clone(),
            scan: None,
            context,
            ann: BTreeSet::new(),
            text: BTreeSet::new(),
            bm25: Vec::new(),
            docs: false,
            only_text_ids: true,
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
            let text_ids = crate::indexed::text_ids_only(q);
            hint.only_text_ids &= text_ids;
            hint.docs |= !text_ids
                && (q.include_attributes.is_some()
                    || q.exclude_attributes.is_some()
                    || q.compute_attributes.is_some()
                    || q.filters.is_some());
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
        tiers: &BTreeMap<String, Vec<bf_wal::SlabSet>>,
        lineage: &[bf_wal::Lineage],
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
            // A fully covering, usable tier also replaces the segment directories. Legacy
            // source segments may have no namespace directory after tier installation.
            // A directory replaces opening every segment's ANN metadata, IDs, and docs footer.
            let by_dir = self.ann.iter().all(|a| {
                crate::upgrade::directory_file(dirs, a).is_some()
                    || tiers
                        .get(a)
                        .is_some_and(|sets| tiers_cover_all(segments, lineage, sets))
            });
            for attribute in self.ann.iter().filter(|_| with_rest) {
                // A lone unfiltered ANN query reads the block-layout directory when there is one.
                // A recluster's tiers take the scan of the rows they cover; the block-layout
                // directory is still loaded for the segments they don't.
                let tiered = tiers
                    .get(attribute)
                    .filter(|t| crate::ann::tiers_usable(segments, lineage, t));
                let covers_all = tiered.is_some_and(|t| tiers_cover_all(segments, lineage, t));
                for set in tiered.into_iter().flatten() {
                    let scan = self.scan.as_ref().filter(|(a, _, _)| a == attribute);
                    let filtered = !self.filters.is_empty();
                    let config = &self.config;
                    let name = crate::ann_dir::Layout::Slabs.dir_key(attribute);
                    tasks.push(
                        async move {
                            if filtered || scan.is_none() {
                                let dir = crate::ann_dir::load(store, &set.dir, None).await?;
                                if filtered {
                                    crate::ann_dir::memberships(store, &set.slabs, &dir, attribute)
                                        .await?;
                                }
                                return Ok(());
                            }
                            let (_, vector, k) = scan.unwrap();
                            let (dir, warm) = crate::ann_dir::slab_directory(
                                store,
                                set,
                                prefetched.get(&name),
                                vector,
                                config,
                                attribute,
                                segments,
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
                }
                if covers_all {
                    continue;
                }
                let chosen = crate::upgrade::directory_file(dirs, attribute)
                    .map(|(file, hint_name)| (file, crate::ann_dir::Layout::Segments, hint_name));
                if let Some((file, layout, hint_name)) = chosen {
                    let filtered = !self.filters.is_empty();
                    // With tiers, only the uncovered segments are searched through the directory:
                    // a prefetch of its plan over every segment would read the wrong clusters.
                    let scan = self
                        .scan
                        .as_ref()
                        .filter(|(a, _, _)| a == attribute)
                        .filter(|_| tiered.is_none());
                    let config = &self.config;
                    tasks.push(
                        async move {
                            let dir = crate::ann_dir::load(store, file, prefetched.get(&hint_name))
                                .await?;
                            if filtered {
                                // Filtered queries also need each segment's row-to-cluster map.
                                crate::ann_dir::memberships(
                                    store,
                                    &segments
                                        .iter()
                                        .filter(|s| crate::upgrade::files(s, attribute).is_some())
                                        .cloned()
                                        .collect::<Vec<_>>(),
                                    &dir,
                                    attribute,
                                )
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
                        // Share the immutable live predicate with query planning; WAL shadows
                        // are removed afterward, rather than entering the cache key.
                        tasks.push(
                            async move {
                                segment::filter_live(store, seg, f, schema)
                                    .await
                                    .map(drop)
                            }
                            .boxed(),
                        );
                    }
                }
                for attribute in self.ann.iter().filter(|_| !by_dir && with_rest) {
                    if let Some(file) = seg.files.get(&format!("annv2:{attribute}")) {
                        tasks.push(segment::vector_index(store, file).map_ok(drop).boxed());
                    }
                }
                let compact_ids = self.only_text_ids && crate::text_keys::inline(seg);
                if with_text && compact_ids {
                    tasks.push(
                        timed("text_keys", crate::text_keys::load(store, seg).map_ok(drop)).boxed(),
                    );
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
                    || (with_text && !self.text.is_empty() && small_ids && !compact_ids)
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
                    || (with_text && !self.text.is_empty() && !small_ids && !compact_ids)
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
    if crate::ann::trace_stages() {
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
/// A manifest that republishes the checkpoint this state already holds (garbage collection and
/// epoch claims do) changes nothing the state depends on, so it must not discard the resident
/// tail: rebuilding would download every unindexed WAL payload again.
fn same_checkpoint(state: &NamespaceState, m: &bf_wal::Manifest) -> bool {
    state.exists
        && !m.deleted
        && m.indexed_through == state.indexed_through
        && state.segments.len() == m.segments.len()
        && state.segments.iter().zip(&m.segments).all(|(a, b)| {
            a.id == b.id
                    && a.live.key == b.live.key
                    // Upgrade batches can keep the checkpoint and directory set unchanged.
                    // Adopt every immutable file replacement before a query or publication.
                    && a.files.iter().map(|(k, f)| (k, &f.key))
                        .eq(b.files.iter().map(|(k, f)| (k, &f.key)))
        })
        && state.text_stats.as_ref().map(|f| &f.key) == m.text_stats.as_ref().map(|f| &f.key)
        && state.ann_dirs.len() == m.ann_dirs.len()
        && state
            .ann_dirs
            .iter()
            .all(|(k, f)| m.ann_dirs.get(k).is_some_and(|g| g.key == f.key))
        && state.lineage.len() == m.lineage.len()
        && state.lineage.iter().zip(&m.lineage).all(|(a, b)| {
            a.source == b.source
                && a.parts
                    .iter()
                    .map(|p| &p.map.key)
                    .eq(b.parts.iter().map(|p| &p.map.key))
        })
        && state.ann_tiers.len() == m.ann_tiers.len()
        && state.ann_tiers.iter().all(|(k, f)| {
            m.ann_tiers.get(k).is_some_and(|g| {
                g.len() == f.len()
                    && g.iter().zip(f.iter()).all(|(a, b)| {
                        // Upgrade batches can replace slab objects too.
                        a.dir.key == b.dir.key
                            && a.slabs.len() == b.slabs.len()
                            && a.slabs.iter().zip(&b.slabs).all(|(x, y)| {
                                x.files
                                    .iter()
                                    .map(|(k, f)| (k, &f.key))
                                    .eq(y.files.iter().map(|(k, f)| (k, &f.key)))
                            })
                    })
            })
        })
}
/// Wait until the current manifest proves that `attr` has no slab tier. A missing hint can
/// independently release the segment fallback, so a manifest listing a tier waits here.
async fn without_tier(
    layouts: &mut tokio::sync::watch::Receiver<Option<BTreeSet<String>>>,
    attr: &str,
) {
    loop {
        let tiered = layouts.borrow().as_ref().map(|attrs| attrs.contains(attr));
        match tiered {
            Some(false) => return,
            Some(true) => std::future::pending::<()>().await,
            None => {
                if layouts.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

/// Writers and indexers nest snapshot refreshes deeply. Keep the cold read plan
/// pointer-sized in their futures, including its speculative branch.
pub(crate) fn catch_up_hinted<'a>(
    wal: &'a Wal,
    state: &'a mut NamespaceState,
    cap: Option<u64>,
    store: &'a Storage,
    hint: Option<&'a OpenHint>,
) -> futures::future::BoxFuture<'a, Result<(), ApiError>> {
    catch_up_hinted_inner(wal, state, cap, store, hint).boxed()
}

async fn catch_up_hinted_inner(
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
        let want_full = hint.is_none_or(OpenHint::wants_text);
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
        // The fresh manifest must confirm the advisory checkpoint before its tail is reused.
        let checkpoint = hint
            .filter(|h| {
                attempt == 0 && state.seq == 0 && cap.is_none() && (h.scan.is_some() || h.searching)
            })
            .and_then(|h| {
                h.context.cache.staging.as_ref().and_then(|manager| {
                    manager.checkpoint(&h.context.cache.key(""), wal.namespace())
                })
            });
        let mut speculative_tail =
            checkpoint.map(|c| wal.tail_bounded(c.indexed_through, u64::MAX).boxed().fuse());
        let mut completed_tail = None;
        // The notice exists only for a cold unfiltered open; warm queries allocate nothing here.
        let (layouts_tx, layouts_rx) =
            if state.ann_dirs.is_empty() && hint.is_some_and(|hint| hint.scan.is_some()) {
                let (tx, rx) = tokio::sync::watch::channel(None);
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };
        let manifest_read = async {
            let result = wal.manifest_view_after(known, view).await;
            if let Some(tx) = &layouts_tx {
                let tiers = result
                    .as_ref()
                    .ok()
                    .and_then(Option::as_ref)
                    .map_or(&*state.ann_tiers, |(_, m)| &m.ann_tiers);
                let attrs: BTreeSet<String> = tiers
                    .iter()
                    .filter(|(_, tiers)| !tiers.is_empty())
                    .map(|(attr, _)| attr.clone())
                    .collect();
                let _ = tx.send(Some(attrs));
            }
            result
        };
        let prefetch_read = async {
            let mut found = BTreeMap::new();
            if let Some(hint) = hint.filter(|h| state.ann_dirs.is_empty() && !h.searching) {
                let reads = hint.ann.iter().map(|a| {
                    let mut layouts = layouts_rx.clone();
                    async move {
                        // A lone unfiltered ANN query wants the block-layout directory: try its hint
                        // first, and the plain one when the namespace has none.
                        let mut got = Vec::new();
                        if hint.scan.is_some() {
                            let mut layouts =
                                layouts.take().expect("cold unfiltered layout notice");
                            // Race the paged and flat slab hints, in that priority order. A
                            // lossless upgrade also writes a segment-directory hint; downloading
                            // it here wastes megabytes when a tier answers the query. Fetch that
                            // fallback when both slab hints are absent, or as soon as the
                            // manifest proves there is no tier (preserving untiered read waves).
                            // Filtered queries still prefetch the segment hint directly.
                            let names = [
                                crate::ann_dir::paged_hint_name(a),
                                crate::ann_dir::Layout::Slabs.dir_key(a),
                            ];
                            let mut pending: Vec<futures::future::BoxFuture<'_, _>> = names
                                .iter()
                                .enumerate()
                                .map(|(i, n)| {
                                    async move {
                                        (
                                            i,
                                            crate::ann_dir::prefetch(store, wal.namespace(), n)
                                                .await,
                                        )
                                    }
                                    .boxed()
                                })
                                .collect();
                            let (force, forced) = tokio::sync::oneshot::channel();
                            let mut force = Some(force);
                            pending.push(
                                async move {
                                    tokio::select! {
                                        _ = forced => {},
                                        _ = without_tier(&mut layouts, a) => {},
                                    }
                                    (
                                        2,
                                        crate::ann_dir::prefetch(
                                            store,
                                            wal.namespace(),
                                            &crate::ann_dir::Layout::Segments.dir_key(a),
                                        )
                                        .await,
                                    )
                                }
                                .boxed(),
                            );
                            let mut results: [Option<Option<crate::ann_dir::Prefetched>>; 3] =
                                Default::default();
                            'priority: for want in 0..3 {
                                if want == 2
                                    && let Some(force) = force.take()
                                {
                                    let _ = force.send(());
                                }
                                while results[want].is_none() {
                                    let ((i, r), _, rest) =
                                        futures::future::select_all(pending).await;
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
                                crate::ann_dir::Layout::Segments.dir_key(a),
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
                            crate::ann_dir::prefetch(store, wal.namespace(), &format!("annv2:{a}"))
                                .await
                                .map(|p| (format!("annv2:{a}"), p)),
                        );
                        got
                    }
                });
                found.extend(
                    crate::read_cache::scope_hints(
                        hint.context.clone(),
                        futures::future::join_all(reads),
                    )
                    .await
                    .into_iter()
                    .flatten(),
                );
            }
            found
        };
        let (manifest, prefetched) = {
            let opening = async { tokio::join!(manifest_read, prefetch_read) };
            tokio::pin!(opening);
            if let Some(tail) = &mut speculative_tail {
                tokio::select! {
                    opened = &mut opening => opened,
                    entries = tail => {
                        completed_tail = Some(entries);
                        opening.await
                    }
                }
            } else {
                opening.await
            }
        };
        let reuse_tail = checkpoint.is_some_and(|checkpoint| {
            manifest
                .as_ref()
                .ok()
                .and_then(Option::as_ref)
                .is_some_and(|(generation, m)| {
                    !m.deleted
                        && *generation == checkpoint.generation
                        && m.indexed_through == checkpoint.indexed_through
                })
        });
        if !reuse_tail {
            // Cancel a stale scan immediately, even if its old tail is very large.
            speculative_tail = None;
            completed_tail = None;
        }
        if let Some((generation, m)) = manifest.map_err(log_error)? {
            if let Some(hint) = hint
                && let Some(manager) = &hint.context.cache.staging
            {
                manager.note_checkpoint(
                    &hint.context.cache.key(""),
                    wal.namespace(),
                    (!m.deleted && m.indexed_through > 0).then_some(crate::local_ann::Checkpoint {
                        generation,
                        indexed_through: m.indexed_through,
                    }),
                );
                manager.retain_namespace(
                    &hint.context.cache.key(""),
                    wal.namespace(),
                    &m.ann_tiers,
                );
            }
            if m.indexed_through > 0
                && generation > state.manifest_seq
                && !same_checkpoint(state, &m)
            {
                // A maintenance publication can replace segments while retaining this checkpoint,
                // and remote indexing can advance through rows already in our resident overlay.
                // Preserve their Arcs instead of downloading and decoding the complete tail again.
                if !(cap.is_none() && state.fast_forward(&m, generation, m.indexed_through)) {
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
                            text_stats: m.text_stats.map(std::sync::Arc::new),
                            ann_dirs: m.ann_dirs,
                            ann_tiers: m.ann_tiers.into(),
                            lineage: m.lineage.into(),
                            bulk_load_at: m.bulk_load_at,
                            stats: m.stats.into(),
                            ..Default::default()
                        }
                    };
                }
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
        if let Some(hint) = hint
            && let Some(manager) = &hint.context.cache.staging
        {
            manager.note_snapshot(&hint.context.cache.key(""), wal.namespace(), state);
        }
        let tail = async {
            if reuse_tail && checkpoint.is_some_and(|c| c.indexed_through == state.seq) {
                let result = match completed_tail {
                    Some(entries) => entries,
                    None => {
                        speculative_tail
                            .as_mut()
                            .expect("matching checkpoint probe")
                            .await
                    }
                };
                if let Ok(entries) = result {
                    // The probe began after this query began. A slot committed after its
                    // first miss is a concurrent write, and can belong to the next snapshot.
                    // Writes acknowledged before the query are included by the normal WAL
                    // scan, including tombstones. No negative result survives this query.
                    metrics::counter!("blowfish_wal_speculation_total", "outcome" => "reused")
                        .increment(1);
                    return Ok(entries);
                }
            }
            // A changed checkpoint or a speculative error takes the existing serial path,
            // including its reclamation retry. Speculation cannot turn an error into a miss.
            if checkpoint.is_some() {
                metrics::counter!("blowfish_wal_speculation_total", "outcome" => "fallback")
                    .increment(1);
            }
            wal.tail_bounded(
                state.seq,
                cap.unwrap_or(u64::MAX).saturating_sub(state.tail_bytes),
            )
            .await
        };
        let tail = if let Some(hint) = hint.filter(|h| !state.segments.is_empty() && !h.searching) {
            // The checkpoint's files do not depend on the tail: open them meanwhile.
            let (segments, text) = (state.segments.clone(), state.text_stats.clone());
            let dirs = state.ann_dirs.clone();
            let slabs = state.ann_tiers.clone();
            let lineage = state.lineage.clone();
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
                        text.as_deref(),
                        &dirs,
                        &slabs,
                        &[],
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
                    text.as_deref(),
                    &dirs,
                    &slabs,
                    &lineage,
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
    let mut live = if let Some(f) = f {
        filter::validate(f, &state.schema, false)?;
        segment::filter_live(store, seg, f, &state.schema)
            .await?
            .as_ref()
            .clone()
    } else {
        segment::live(store, seg).await?
    };
    if unshadow && !state.shadowed.is_empty() {
        let prepared = crate::read_cache::context()
            .and_then(|context| context.text_shadowed)
            .and_then(|masks| masks.get(&seg.id).cloned());
        if let Some(rows) = prepared {
            live -= rows;
        } else {
            live -= segment::lookup_many(store, seg, &state.shadowed).await?;
        }
    }
    Ok(live)
}
pub(crate) async fn materialize(
    store: &Storage,
    state: &NamespaceState,
) -> Result<NamespaceState, ApiError> {
    crate::vector_locator::scope(state, materialize_inner(store, state)).await
}

async fn materialize_inner(
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
/// Patches and conditions hydrate named rows; unconditional replacements need no old documents.
/// By-filter writes still use a namespace scan.
pub(crate) async fn for_writes(
    store: &Storage,
    state: &NamespaceState,
    pending: &[Pending<Operation, Result<WriteResponse, ApiError>>],
    overlay: &[std::sync::Arc<bf_wal::WalEntry>],
) -> Result<NamespaceState, ApiError> {
    crate::vector_locator::scope(state, for_writes_inner(store, state, pending, overlay)).await
}

async fn for_writes_inner(
    store: &Storage,
    state: &NamespaceState,
    pending: &[Pending<Operation, Result<WriteResponse, ApiError>>],
    overlay: &[std::sync::Arc<bf_wal::WalEntry>],
) -> Result<NamespaceState, ApiError> {
    let mut wanted = BTreeSet::new();
    let mut hydrate = BTreeSet::new();
    for p in pending {
        let Operation::Write(req, _, _activity) = &p.request else {
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
        // Replacements never inspect old rows unless they have a condition. Patches and
        // deletes need their old values (and existence), including earlier in-flight groups.
        if req.upsert_condition.is_some() {
            for row in req.upsert_rows.iter().flatten() {
                if let Some(value) = row.get("id")
                    && let Ok(id) = serde_json::from_value::<Id>(value.clone())
                {
                    hydrate.insert(id);
                }
            }
            if let Some(ids) = req
                .upsert_columns
                .as_ref()
                .and_then(|c| c.get("id"))
                .and_then(serde_json::Value::as_array)
            {
                for value in ids {
                    if let Ok(id) = serde_json::from_value::<Id>(value.clone()) {
                        hydrate.insert(id);
                    }
                }
            }
        }
        for row in req.patch_rows.iter().flatten() {
            if let Some(value) = row.get("id")
                && let Ok(id) = serde_json::from_value::<Id>(value.clone())
            {
                hydrate.insert(id);
            }
        }
        if let Some(ids) = req
            .patch_columns
            .as_ref()
            .and_then(|c| c.get("id"))
            .and_then(serde_json::Value::as_array)
        {
            for value in ids {
                if let Ok(id) = serde_json::from_value::<Id>(value.clone()) {
                    hydrate.insert(id);
                }
            }
        }
        hydrate.extend(req.deletes.iter().flatten().cloned());
        wanted.extend(req.deletes.iter().flatten().cloned());
    }
    if state.schema.get("id").and_then(|s| s.data_type.as_ref())
        == Some(&AttributeType::Scalar(ScalarType::Uuid))
    {
        for ids in [&mut wanted, &mut hydrate] {
            *ids = std::mem::take(ids)
                .into_iter()
                .map(|id| match id {
                    Id::String(s) => uuid::Uuid::parse_str(&s)
                        .map(|id| Id::String(id.to_string()))
                        .unwrap_or(Id::String(s)),
                    id => id,
                })
                .collect();
        }
    }
    // A write reads only the rows it names: tail rows by ID here, segment rows below.
    let mut full = state.without_docs();
    for id in &wanted {
        if let Some(doc) = state.docs.get(id) {
            full.docs.insert(id.clone(), doc.clone());
        }
    }
    let hydrate = hydrate
        .difference(&state.shadowed)
        .cloned()
        .collect::<BTreeSet<_>>();
    if !hydrate.is_empty() {
        for seg in &state.segments {
            let live = segment::live(store, seg).await?;
            let rows = segment::lookup_many(store, seg, &hydrate).await? & live;
            full.docs
                .extend(crate::shared(segment::documents(store, seg, &rows).await?));
        }
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
    crate::vector_locator::scope(state, statistics_inner(store, state)).await
}

async fn statistics_inner(
    store: &Storage,
    state: &NamespaceState,
) -> Result<bf_wal::NamespaceStats, ApiError> {
    let mut stats = (*state.stats).clone();
    if !state.shadowed.is_empty() {
        for seg in &state.segments {
            let rows = segment::lookup_many(store, seg, &state.shadowed).await?
                & segment::live(store, seg).await?;
            let old = crate::statistics::rows(store, seg, &rows, &state.schema, false).await?;
            stats.rows = stats.rows.saturating_sub(old.rows);
            stats.logical_bytes = stats.logical_bytes.saturating_sub(old.logical_bytes);
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

#[cfg(test)]
mod cold_tier_tests {
    use super::*;
    use crate::{Engine, EngineConfig, ReclusterOptions, read_cache};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tier_only_cold_open_skips_ids_but_uncovered_segments_keep_their_warmup() {
        let parent = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
        std::fs::create_dir_all(&parent).unwrap();
        let work = tempfile::Builder::new()
            .prefix("cold-tier-")
            .tempdir_in(parent)
            .unwrap();
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
                recall_sample_rate: 0.0,
                ..Default::default()
            },
        );
        let rows: Vec<_> = (0..4096u32)
            .map(|id| {
                serde_json::json!({
                    "id": id, "vector": [id as f32, (id % 17) as f32]
                })
            })
            .collect();
        engine
            .write(
                "t",
                serde_json::from_value(serde_json::json!({
                    "distance_metric": "euclidean_squared", "upsert_rows": rows
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("t", true).await.unwrap();
        engine
            .recluster(
                "t",
                &ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 4,
                    cluster_rows: 200,
                    max_slab_rows: 2000,
                    scratch: work.path().join("build"),
                },
            )
            .await
            .unwrap();
        let query: QueryRequest = serde_json::from_value(serde_json::json!({
            "rank_by": ["vector", "ANN", [17., 0.]], "top_k": 1,
            "include_attributes": ["id"]
        }))
        .unwrap();
        let expected = engine.query("t", query.clone()).await.unwrap();
        let wal = Wal::new(store.clone(), "t").unwrap();
        let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
        let ids = manifest.segments[0].files["ids"].clone();
        manifest.ann_dirs.clear();
        wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
            .await
            .unwrap();
        engine.drop_caches().await;
        let actual = engine.query("t", query.clone()).await.unwrap();
        assert_eq!(
            serde_json::to_value(actual.result.rows).unwrap(),
            serde_json::to_value(expected.result.rows).unwrap()
        );
        let cached_ids = || {
            let context = read_cache::ReadContext::new(engine.0.read_cache.clone());
            let ids = ids.clone();
            async move {
                read_cache::scope(context, async move {
                    read_cache::get::<segment::IdIndex>(&format!("validated:{}", ids.key), 0)
                        .is_some()
                })
                .await
            }
        };
        assert!(
            !cached_ids().await,
            "a covering tier must not load the source ID dictionary"
        );

        let sets = &manifest.ann_tiers["vector"];
        let mut uncovered = manifest.segments.clone();
        let mut extra = uncovered[0].clone();
        extra.id = "uncovered".into();
        uncovered.push(extra);
        assert!(!tiers_cover_all(&uncovered, &manifest.lineage, sets));
        let mut stale = manifest.segments.clone();
        stale[0].row_count += 1;
        assert!(!tiers_cover_all(&stale, &manifest.lineage, sets));
        assert!(!tiers_cover_all(&manifest.segments, &manifest.lineage, &[]));
        let mut moved = manifest.segments.clone();
        let source = moved[0].id.clone();
        moved[0].id = "moved".into();
        let lineage = vec![bf_wal::Lineage {
            source,
            parts: vec![bf_wal::LineagePart {
                target: moved[0].id.clone(),
                rows: moved[0].row_count,
                map: ids.clone(),
            }],
        }];
        assert!(tiers_cover_all(&moved, &lineage, sets));
        assert!(!tiers_cover_all(&moved, &[], sets));

        let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
        manifest.ann_tiers.clear();
        wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
            .await
            .unwrap();
        engine.drop_caches().await;
        engine.query("t", query).await.unwrap();
        assert!(
            cached_ids().await,
            "an uncovered ANN query still warms its ID dictionary"
        );
        assert!(engine.shutdown(std::time::Duration::from_secs(5)).await);
    }
}
