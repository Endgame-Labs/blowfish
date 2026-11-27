//! Per-segment candidate search, tail scoring, and reference-compatible result assembly.
use crate::sum_text::{self, sum_of_text};
use crate::{EngineConfig, NamespaceState, filter, query, segment, snapshot};
use bf_storage::Storage;
use bf_types::*;
use futures::{FutureExt, StreamExt, TryStreamExt};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};

pub(crate) fn rank_key(rank: &RankBy) -> Vec<u8> {
    match rank {
        RankBy::Vector {
            attribute,
            exact,
            vector,
            ..
        } => {
            // An invalid JSON prefix separates binary vector keys from every other rank.
            // Attribute length and every float bit preserve exact identity, without hashing.
            let mut key = Vec::with_capacity(10 + attribute.len() + vector.len() * 4);
            key.extend_from_slice(&[0xff, u8::from(*exact)]);
            key.extend_from_slice(&(attribute.len() as u64).to_le_bytes());
            key.extend_from_slice(attribute.as_bytes());
            for x in vector.iter() {
                key.extend_from_slice(&x.to_bits().to_le_bytes());
            }
            key
        }
        _ => rank.value().to_string().into_bytes(),
    }
}

fn filter_columns(filter: &Filter, columns: &mut BTreeSet<String>) {
    match filter {
        Filter::And(filters) | Filter::Or(filters) => {
            for filter in filters {
                filter_columns(filter, columns);
            }
        }
        Filter::Not(filter) => filter_columns(filter, columns),
        Filter::Compare { attribute, .. } => {
            if attribute != "id" {
                columns.insert(attribute.clone());
            }
        }
    }
}

pub(crate) fn execute<'a>(
    store: &'a Storage,
    state: &'a NamespaceState,
    request: &'a QueryRequest,
    config: &'a EngineConfig,
    force_exact: bool,
) -> futures::future::BoxFuture<'a, Result<QueryResponse, ApiError>> {
    crate::vector_locator::scope(
        state,
        execute_inner(store, state, request, config, force_exact),
    )
}

async fn execute_inner(
    store: &Storage,
    state: &NamespaceState,
    request: &QueryRequest,
    config: &EngineConfig,
    force_exact: bool,
) -> Result<QueryResponse, ApiError> {
    // Validate the complete request before touching index files, preserving M1 errors.
    query::validate(state, request)?;
    if crate::streaming::supports(request) {
        return crate::streaming::execute(store, state, request).await;
    }
    // Independent subqueries can each stream without building a union of all candidates.
    if let Some(queries) = &request.queries
        && request.rerank_by.is_none()
    {
        let results = futures::future::try_join_all(queries.iter().map(|q| async move {
            let mut q = q.clone();
            q.vector_encoding = request.vector_encoding;
            Ok::<_, ApiError>(execute(store, state, &q, config, force_exact).await?.result)
        }))
        .await?;
        return Ok(QueryResponse {
            result: Default::default(),
            results: Some(results),
            performance: Default::default(),
            billing: Default::default(),
        });
    }
    // Computed BM25 scores can use terms absent from the retrieval rank.
    let projected_ranks: Vec<_> = request
        .queries
        .as_deref()
        .unwrap_or(std::slice::from_ref(request))
        .iter()
        .flat_map(|q| q.compute_attributes.iter().flat_map(|attrs| attrs.values()))
        .filter_map(|value| RankBy::parse(value.clone()).ok())
        .collect();
    let mut clauses = vec![];
    for q in request
        .queries
        .as_deref()
        .unwrap_or(std::slice::from_ref(request))
    {
        if let Some(rank) = &q.rank_by {
            text_clauses(rank, &mut clauses);
        }
    }
    for rank in &projected_ranks {
        text_clauses(rank, &mut clauses);
    }
    let mut terms = crate::text::QueryTerms::new();
    if !clauses.is_empty() {
        for clause in &clauses {
            if let RankBy::Bm25 {
                attribute, query, ..
            } = clause
                && let Some(cfg) = state.schema.get(attribute).and_then(|s| s.text_config())
            {
                terms
                    .entry(attribute.clone())
                    .or_default()
                    .extend(crate::text::tokens(query, &cfg).into_iter().map(|t| t.text));
            }
        }
    }
    // Only text subqueries wait for corpus statistics. In a cold RRF request,
    // ANN can start its directory and scan reads while those chunks arrive.
    let statistics = async {
        let started = std::time::Instant::now();
        let result = crate::text::global_stats(store, state, &terms).await;
        if !clauses.is_empty() {
            crate::ann::stage(started, "bm25 global stats");
        }
        result
    }
    .shared();
    let mut candidates = result_state(state);
    // Subqueries (for example the ANN and BM25 halves of a hybrid query) read independent
    // files, so they run concurrently. Results merge in request order.
    let parts = futures::future::try_join_all(
        request
            .queries
            .as_deref()
            .unwrap_or(std::slice::from_ref(request))
            .iter()
            .map(|q| {
                let statistics = statistics.clone();
                async move {
                    let mut text = Vec::new();
                    if let Some(rank) = &q.rank_by {
                        text_clauses(rank, &mut text);
                    }
                    if text.is_empty() {
                        prepare(
                            store,
                            state,
                            q,
                            config,
                            force_exact,
                            request.queries.is_none(),
                        )
                        .await
                    } else {
                        let mut prepared = state.clone();
                        let stats = statistics.await?;
                        prepared.corpus = stats.corpus;
                        let work = prepare(
                            store,
                            &prepared,
                            q,
                            config,
                            force_exact,
                            request.queries.is_none(),
                        );
                        if (stats.shadowed.is_some() || stats.tail.is_some())
                            && let Some(mut context) = crate::read_cache::context()
                        {
                            context.text_shadowed = stats.shadowed;
                            context.text_tail = stats.tail;
                            crate::read_cache::scope(context, work).await
                        } else {
                            work.await
                        }
                    }
                }
            }),
    )
    .await?;
    // RRF clauses can project different fields on overlapping candidates.
    // Keep their common candidate hydration intact when merging the clauses.
    for part in parts {
        candidates.docs.extend(part.docs);
        for (rank, scores) in part.scores {
            candidates.scores.entry(rank).or_default().extend(scores);
        }
    }
    if !clauses.is_empty() {
        candidates.corpus = statistics.await?.corpus;
    }
    let started = std::time::Instant::now();
    let result = query::execute(&candidates, request);
    if !clauses.is_empty() {
        crate::ann::stage(started, "bm25 merge");
    }
    result
}
/// Candidate assembly needs only the evaluator's inputs. Copying the resident documents,
/// shadow set, and replay history here only to clear them multiplies tail work per query.
fn result_state(state: &NamespaceState) -> NamespaceState {
    NamespaceState {
        schema: state.schema.clone(),
        metric: state.metric,
        segments: state.segments.clone(),
        corpus: state.corpus.clone(),
        ..Default::default()
    }
}
#[derive(Clone)]
pub(crate) struct HitKey {
    pub(crate) score: f64,
    pub(crate) id: Id,
    descending: bool,
}
impl PartialEq for HitKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for HitKey {}
impl PartialOrd for HitKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HitKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self.score.total_cmp(&other.score);
        (if self.descending { cmp.reverse() } else { cmp }).then(self.id.cmp(&other.id))
    }
}
/// Offer one exact score without cloning the ID of a noncompetitive row.
pub(crate) fn offer(
    heap: &mut BinaryHeap<HitKey>,
    id: &Id,
    score: f64,
    descending: bool,
    k: usize,
) {
    let competitive = heap.len() < k
        || heap
            .peek()
            .is_some_and(|worst| improves(id, score, descending, worst));
    if competitive {
        if heap.len() == k {
            heap.pop();
        }
        heap.push(HitKey {
            score,
            id: id.clone(),
            descending,
        });
    }
}
fn improves(id: &Id, score: f64, descending: bool, worst: &HitKey) -> bool {
    let cmp = score.total_cmp(&worst.score);
    (if descending { cmp.reverse() } else { cmp })
        .then(id.cmp(&worst.id))
        .is_lt()
}
/// Keep only the globally competitive candidates while merging segment-local top-k lists.
fn trim_candidates(state: &mut NamespaceState, q: &QueryRequest) {
    if state.scores.is_empty() {
        return;
    }
    if q.limit.as_ref().and_then(Limit::per).is_some() {
        return;
    }
    let Some(rank @ (RankBy::Vector { .. } | RankBy::Bm25 { .. })) = &q.rank_by else {
        return;
    };
    let Some(scores) = state.scores.get(&rank_key(rank)) else {
        return;
    };
    let k = q.total().unwrap_or(10) + q.offset;
    let mut heap = BinaryHeap::new();
    for (id, score) in scores {
        offer(
            &mut heap,
            id,
            *score,
            matches!(rank, RankBy::Bm25 { .. }),
            k,
        );
    }
    let keep: BTreeSet<_> = heap.into_iter().map(|hit| hit.id).collect();
    state.docs.retain(|id, _| keep.contains(id));
    for scores in state.scores.values_mut() {
        scores.retain(|id, _| keep.contains(id));
    }
}
async fn prepare(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    config: &EngineConfig,
    force_exact: bool,
    narrow_columns: bool,
) -> Result<NamespaceState, ApiError> {
    let threshold = (matches!(q.rank_by, Some(RankBy::Bm25 { .. }))
        && q.limit.as_ref().and_then(Limit::per).is_none())
    .then(|| {
        std::sync::Arc::new(bf_text::SearchThreshold::new(
            q.total().unwrap_or(10) + q.offset,
        ))
    });
    if matches!(q.rank_by, Some(RankBy::MultiVector { .. })) {
        return snapshot::materialize(store, state).await;
    }
    if matches!(q.rank_by, Some(RankBy::Order(_))) {
        return crate::streaming::order_candidates(store, state, q).await;
    }
    if let Some(
        rank @ RankBy::Vector {
            attribute,
            vector,
            exact,
            ..
        },
    ) = &q.rank_by
        && !*exact
        && !force_exact
        && q.limit.as_ref().and_then(Limit::per).is_none()
        && !state.segments.is_empty()
        && !(state.metric == DistanceMetric::CosineDistance && vector.iter().all(|x| *x == 0.0))
    {
        let k = q.total().unwrap_or(10) + q.offset;
        let columns = projection_columns(state, q, narrow_columns);
        if let Some((scores, docs)) =
            crate::ann::search(store, state, q, attribute, vector, config, k, &columns).await?
        {
            let mut out = result_state(state);
            out.docs = crate::shared(docs);
            let scores = merge_tail(state, q, attribute, vector, false, scores, &mut out)?;
            out.scores.insert(rank_key(rank), scores);
            return Ok(out);
        }
    }
    let Some(context) = crate::read_cache::context()
        .filter(|_| state.segments.len() > 1 && (threshold.is_none() || crate::fan_out()))
    else {
        let mut out = prepare_one(
            store,
            state,
            q,
            config,
            force_exact,
            narrow_columns,
            threshold.as_deref(),
        )
        .await?;
        trim_candidates(&mut out, q);
        return Ok(out);
    };
    let mut base = state.without_docs();
    base.segments.clear();
    let base = std::sync::Arc::new(base);
    let parts = futures::stream::iter(state.segments.iter().cloned().map(|segment| {
        let context = context.clone();
        let store = store.clone();
        let config = config.clone();
        let q = q.clone();
        let base = base.clone();
        let threshold = threshold.clone();
        async move {
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
                let mut state = (*base).clone();
                state.segments.push(segment);
                runtime.block_on(crate::read_cache::scope(
                    context,
                    prepare_one(
                        &store,
                        &state,
                        &q,
                        &config,
                        force_exact,
                        narrow_columns,
                        threshold.as_deref(),
                    ),
                ))
            })
            .await
            .map_err(segment::index_error)?
        }
    }))
    .buffer_unordered(config.search_concurrency.max(1));
    let mut out = (*base).clone();
    futures::pin_mut!(parts);
    while let Some(part) = parts.try_next().await? {
        out.docs.extend(part.docs);
        for (rank, scores) in part.scores {
            out.scores.entry(rank).or_default().extend(scores);
        }
        trim_candidates(&mut out, q);
    }
    let mut tail = state.clone();
    tail.segments.clear();
    let part = prepare_one(
        store,
        &tail,
        q,
        config,
        force_exact,
        narrow_columns,
        threshold.as_deref(),
    )
    .await?;
    out.docs.extend(part.docs);
    for (rank, scores) in part.scores {
        out.scores.entry(rank).or_default().extend(scores);
    }
    trim_candidates(&mut out, q);
    Ok(out)
}
fn projection_columns(
    state: &NamespaceState,
    q: &QueryRequest,
    narrow_columns: bool,
) -> BTreeSet<String> {
    let mut columns: BTreeSet<String> = state
        .schema
        .iter()
        .filter(|(name, cfg)| {
            cfg.data_type
                .as_ref()
                .is_none_or(|t| t.dimensions().is_none())
                || q.compute_attributes.is_some()
                || match &q.include_attributes {
                    Some(IncludeAttributes::All(true)) => true,
                    Some(IncludeAttributes::Names(names)) => names.contains(name),
                    _ => q
                        .exclude_attributes
                        .as_ref()
                        .is_some_and(|names| !names.contains(name)),
                }
        })
        .map(|(name, _)| name.clone())
        .collect();
    // The attributes the response projects: none, or the named ones. Any other projection
    // (all attributes, exclusions, computed values, or per-group limits) keeps every column.
    let projected: Option<BTreeSet<&String>> = match &q.include_attributes {
        // `false` asks for no attributes, the same as leaving the field out.
        None | Some(IncludeAttributes::All(false)) => Some(BTreeSet::new()),
        Some(IncludeAttributes::Names(names)) => Some(names.iter().collect()),
        Some(_) => None,
    };
    if let Some(projected) = projected
        && q.exclude_attributes.is_none()
        && q.compute_attributes.is_none()
        && q.limit.as_ref().and_then(Limit::per).is_none()
    {
        match &q.filters {
            // Scoring already supplied distances: only the projected attributes need reading
            // (a top-10 query naming `id` used to parse every text column of its results).
            None => columns.retain(|name| projected.contains(name)),
            Some(filter) if narrow_columns && q.extra.is_empty() => {
                // The filter needs its attributes' document values too.
                let mut needed = BTreeSet::new();
                filter_columns(filter, &mut needed);
                columns.retain(|name| needed.contains(name) || projected.contains(name));
            }
            _ => {}
        }
    }
    // IDs are intrinsic to every hydrated row, rather than an optional attribute.
    // Naming `id` must leave the same attribute set as the default ID-only request.
    columns.remove("id");
    columns
}
async fn prepare_one(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    config: &EngineConfig,
    force_exact: bool,
    narrow_columns: bool,
    threshold: Option<&bf_text::SearchThreshold>,
) -> Result<NamespaceState, ApiError> {
    let Some(
        rank @ RankBy::Vector {
            attribute,
            vector,
            exact,
            ..
        },
    ) = &q.rank_by
    else {
        return prepare_scalar(store, state, q, threshold).await;
    };
    if state.metric == DistanceMetric::CosineDistance && vector.iter().all(|x| *x == 0.0) {
        return snapshot::materialize(store, state).await;
    }
    // Diversification can need arbitrarily deep candidates from each segment.
    let all = q.limit.as_ref().and_then(Limit::per).is_some();
    let k = q.total().unwrap_or(10) + q.offset;
    let mut out = result_state(state);
    let mut scores = BTreeMap::new();
    let columns = projection_columns(state, q, narrow_columns);
    for seg in &state.segments {
        let file = seg.files.get(&format!("annv2:{attribute}"));
        let (allowed, index, ids) = tokio::try_join!(
            snapshot::allowed(store, state, seg, q.filters.as_ref()),
            async {
                match file {
                    Some(file) => segment::vector_index(store, file).await.map(Some),
                    None => Ok(None),
                }
            },
            segment::ids(store, seg),
        )?;
        if allowed.is_empty() {
            continue;
        }
        let Some(index) = index else {
            if crate::read_cache::context()
                .and_then(|c| c.vectors)
                .is_some_and(|s| s.covers(seg, attribute))
            {
                let top = if all { allowed.len() as usize } else { k };
                let hits = crate::vector_locator::exact(
                    store,
                    seg,
                    &allowed,
                    attribute,
                    vector,
                    top,
                    state.metric,
                )
                .await?;
                let rows = hits.iter().map(|(row, _)| *row).collect();
                for (row, score) in hits {
                    scores.insert(ids[row as usize].clone(), score as f64);
                }
                out.docs.extend(crate::shared(
                    segment::projected_documents(store, seg, &rows, Some(&columns)).await?,
                ));
                continue;
            }

            // Version-one document-only segments remain readable during rollout.
            if let Some(context) = crate::read_cache::context() {
                context
                    .document_scan_rows
                    .fetch_add(allowed.len(), std::sync::atomic::Ordering::Relaxed);
            }
            let docs = segment::documents(store, seg, &allowed).await?;
            for (id, mut doc) in docs {
                doc.retain(|name, _| state.schema.contains_key(name));
                if let Some(v) = doc.get(attribute) {
                    scores.insert(
                        id.clone(),
                        query::distance(&decode_vector(v)?, vector, state.metric),
                    );
                    out.docs.insert(id, std::sync::Arc::new(doc));
                }
            }
            continue;
        };
        let top = if all { allowed.len() as usize } else { k };
        let hits = if *exact || force_exact || all {
            index.exact_search(vector, top, Some(&allowed)).await
        } else {
            index
                .search(
                    vector,
                    top,
                    bf_vector::SearchParams {
                        nprobe: config.ann_nprobe,
                        rerank_factor: config.ann_rerank_factor,
                    },
                    Some(&allowed),
                )
                .await
        }
        .map_err(segment::index_error)?;
        let rows: RoaringBitmap = hits.iter().map(|(row, _)| *row).collect();
        for (row, score) in hits {
            scores.insert(ids[row as usize].clone(), score as f64);
        }
        out.docs.extend(crate::shared(
            segment::projected_documents(store, seg, &rows, Some(&columns)).await?,
        ));
    }
    let scores = merge_tail(state, q, attribute, vector, all, scores, &mut out)?;
    out.scores.insert(rank_key(rank), scores);
    Ok(out)
}

/// Every live tail vector is scored with the reference kernel. The heap retains the best
/// `k + offset` from the union with indexed hits, using the same ID tie-break as the evaluator.
/// Dropped rows therefore cannot change top-k. Diversified limits retain every candidate.
#[allow(clippy::too_many_arguments)]
fn merge_tail(
    state: &NamespaceState,
    q: &QueryRequest,
    attribute: &str,
    vector: &[f32],
    all: bool,
    mut scores: BTreeMap<Id, f64>,
    out: &mut NamespaceState,
) -> Result<BTreeMap<Id, f64>, ApiError> {
    let start = std::time::Instant::now();
    let k = q.total().unwrap_or(10) + q.offset;
    let mut heap = BinaryHeap::new();
    if !all {
        for (id, score) in &scores {
            offer(&mut heap, id, *score, false, k);
        }
    }
    let context = crate::read_cache::context();
    let tail = context.as_ref().map(|c| (&c.cache.tail_vectors, c.admit));
    score_tail(
        state,
        q.filters.as_ref(),
        attribute,
        vector,
        all,
        k,
        tail,
        &mut heap,
        &mut scores,
    )?;
    if !all {
        scores = heap.into_iter().map(|hit| (hit.id, hit.score)).collect();
    }
    out.docs.retain(|id, _| scores.contains_key(id));
    for id in scores.keys() {
        if let Some(doc) = state.docs.get(id) {
            out.docs.insert(id.clone(), doc.clone());
        }
    }
    crate::ann::stage(start, "tail score+merge");
    Ok(scores)
}

/// Score every live tail document against `vector` and offer it to `heap`, which keeps the best
/// `k` under the evaluator's order (distance, then ID); with `all`, every score goes to `scores`
/// instead. `tail` is the decoded-vector cache and whether this work may admit into it
/// (without one, each vector is decoded for this query only). Shared by the document path's
/// [`merge_tail`] and the typed ID-only path, so both score the tail with the same kernel.
#[allow(clippy::too_many_arguments)]
pub(crate) fn score_tail(
    state: &NamespaceState,
    filters: Option<&Filter>,
    attribute: &str,
    vector: &[f32],
    all: bool,
    k: usize,
    tail: Option<(&crate::tail::VectorCache, bool)>,
    heap: &mut BinaryHeap<HitKey>,
    scores: &mut BTreeMap<Id, f64>,
) -> Result<(), ApiError> {
    let attribute_key: std::sync::Arc<str> = attribute.into();
    let admit = tail.is_some_and(|(cache, admit)| {
        admit && cache.fits(state.docs.len(), vector.len(), attribute.len())
    });
    let scorer = crate::tail::Scorer::new(state.metric, vector);
    // One prepared block per snapshot (built by the first query that needs it), so each row
    // below costs a pointer comparison and a dot product instead of a cache lookup.
    let block = tail
        .filter(|_| admit && !state.docs.is_empty())
        .and_then(|(cache, _)| {
            cache.block(
                std::sync::Arc::as_ptr(&state.stats) as usize,
                state.seq,
                &attribute_key,
                state.docs.values(),
            )
        });
    // `worst` is the distance the top `k` must beat. A row the eight-bit screen proves is
    // farther than that is not scored exactly and is not offered, as it could not enter.
    let score = |index: usize,
                 doc: &std::sync::Arc<Document>,
                 key: &std::sync::Arc<str>,
                 worst: Option<f64>|
     -> Result<Option<f64>, ApiError> {
        if let Some(row) = block.as_ref().and_then(|block| block.row(index, doc)) {
            return Ok(row.and_then(|vector| scorer.resident_below(vector, worst)));
        }
        doc.get(attribute)
            .map(|v| {
                let values = match tail {
                    Some((cache, _)) => cache.get(doc, key, admit)?,
                    None => crate::tail::Values::Transient(decode_vector(v)?),
                };
                Ok(scorer.distance(&values))
            })
            .transpose()
    };
    if !all
        && filters.is_none()
        && state.docs.len() >= 2048
        && vector.len() >= 64
        && crate::fan_out()
        && rayon::current_num_threads() > 1
    {
        let rows: Vec<_> = state.docs.iter().collect();
        let bound = heap.peek().filter(|_| heap.len() == k);
        // Each chunk retains its best k rows with the evaluator's identical total order.
        // A discarded row has k better rows in its own chunk, so it cannot win globally.
        // The initial indexed bound can only improve as tail candidates are merged.
        let chunks: Result<Vec<_>, ApiError> = rows
            .par_chunks(512)
            .enumerate()
            .map(|(chunk, rows)| {
                // Separate key allocations avoid a contended Arc counter on cache lookups.
                let key = std::sync::Arc::<str>::from(attribute);
                let mut local: BinaryHeap<HitKey> = BinaryHeap::new();
                for (offset, (id, doc)) in rows.iter().enumerate() {
                    let local_worst = local.peek().filter(|_| local.len() == k);
                    let worst = match (bound, local_worst) {
                        (Some(a), Some(b)) => Some(a.score.min(b.score)),
                        (Some(a), None) => Some(a.score),
                        (None, Some(b)) => Some(b.score),
                        (None, None) => None,
                    };
                    if let Some(distance) = score(chunk * 512 + offset, doc, &key, worst)?
                        && bound.is_none_or(|worst| improves(id, distance, false, worst))
                    {
                        offer(&mut local, id, distance, false, k);
                    }
                }
                Ok(local)
            })
            .collect();
        for chunk in chunks? {
            for hit in chunk {
                offer(heap, &hit.id, hit.score, false, k);
            }
        }
    } else {
        for (index, (id, doc)) in state.docs.iter().enumerate() {
            let worst = (!all)
                .then(|| heap.peek().filter(|_| heap.len() == k))
                .flatten()
                .map(|hit| hit.score);
            if filters
                .map(|f| filter::matches(f, Some(doc), None, &state.schema))
                .transpose()?
                .unwrap_or(true)
                && let Some(distance) = score(index, doc, &attribute_key, worst)?
            {
                if all {
                    scores.insert(id.clone(), distance);
                } else {
                    offer(heap, id, distance, false, k);
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn recall(ann: &QueryResponse, exact: &QueryResponse) -> f64 {
    let ann: BTreeSet<_> = ann
        .result
        .rows
        .iter()
        .flatten()
        .map(|row| row["id"].to_string())
        .collect();
    let exact: BTreeSet<_> = exact
        .result
        .rows
        .iter()
        .flatten()
        .map(|row| row["id"].to_string())
        .collect();
    if exact.is_empty() {
        1.0
    } else {
        ann.intersection(&exact).count() as f64 / exact.len() as f64
    }
}

/// Calibration truth without an exhaustive scan: the exact-distance top `k` of the union of
/// several approximate answers. Every answer was reranked with exact distances, so a row any of
/// them found is ranked correctly against the others; only a true neighbor that every answer
/// missed is absent. `None` when a row has no distance (the caller then falls back to the
/// exhaustive scan).
pub(crate) fn union_truth(answers: &[&QueryResponse]) -> Option<QueryResponse> {
    let k = answers
        .iter()
        .map(|a| a.result.rows.as_ref().map_or(0, Vec::len))
        .max()?;
    let mut seen = BTreeMap::new();
    for row in answers
        .iter()
        .filter_map(|a| a.result.rows.as_ref())
        .flatten()
    {
        let dist = row.get("$dist")?.as_f64()?;
        seen.entry(row["id"].to_string()).or_insert((dist, row));
    }
    let mut ranked = seen.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|a, b| a.1.0.total_cmp(&b.1.0).then_with(|| a.0.cmp(&b.0)));
    let mut out = answers[0].clone();
    out.result.rows = Some(
        ranked
            .into_iter()
            .take(k)
            .map(|(_, (_, r))| r.clone())
            .collect(),
    );
    Some(out)
}

/// ID indexes above this size are not read whole by BM25 queries.
pub(crate) const RANGED_IDS_BYTES: u64 = 128 << 10;
/// A plain BM25 query whose response needs only IDs and scores.
pub(crate) fn text_ids_only(q: &QueryRequest) -> bool {
    let ids = match &q.include_attributes {
        None | Some(IncludeAttributes::All(false)) => true,
        Some(IncludeAttributes::Names(names)) => names.iter().all(|name| name == "id"),
        _ => false,
    };
    (matches!(q.rank_by, Some(RankBy::Bm25 { .. })) || max_of_text(q.rank_by.as_ref()))
        && ids
        && q.filters.is_none()
        && q.exclude_attributes.is_none()
        && q.compute_attributes.is_none()
        && q.limit.as_ref().and_then(Limit::per).is_none()
}
/// `["Max", [...]]` whose clauses are all plain BM25. Each clause then needs only its own top
/// `k + offset` rows (plus ties at the cutoff): a row outside every clause's top list has a
/// maximum below at least `k + offset` other rows' maxima. `Sum` has no such bound.
pub(crate) fn max_of_text(rank: Option<&RankBy>) -> bool {
    matches!(rank, Some(RankBy::Max(xs)) if !xs.is_empty()
        && xs.iter().all(|x| matches!(x, RankBy::Bm25 { .. })))
}
/// True when `hits` (best first, from a search for `top > k` rows) hold every row scoring at
/// least the `k`th hit's score: the list is the whole result, or ends strictly below that score.
fn ties_complete(hits: &[bf_text::SearchHit], k: usize, top: usize) -> bool {
    k == 0
        || hits.len() < top
        || hits
            .last()
            .is_some_and(|last| last.score < hits[k - 1].score)
}
pub(crate) fn text_clauses<'a>(rank: &'a RankBy, clauses: &mut Vec<&'a RankBy>) {
    match rank {
        RankBy::Bm25 { .. } => clauses.push(rank),
        RankBy::Sum(xs) | RankBy::Max(xs) => {
            for x in xs {
                text_clauses(x, clauses);
            }
        }
        RankBy::Product { expr, .. }
        | RankBy::Saturate { expr, .. }
        | RankBy::Decay { expr, .. }
        | RankBy::Dist { expr, .. } => text_clauses(expr, clauses),
        _ => {}
    }
}
/// One segment's contribution to a scalar or BM25 query: scores and hydrated documents, in a
/// state of their own so segments run concurrently.
#[allow(clippy::too_many_arguments)]
async fn prepare_segment(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    seg: &bf_wal::Segment,
    clauses: &[&RankBy],
    all: bool,
    clause_top: bool,
    k: usize,
    columns: &BTreeSet<String>,
    threshold: Option<&bf_text::SearchThreshold>,
    preset: Option<Preset>,
    mut out: NamespaceState,
) -> Result<NamespaceState, ApiError> {
    let open_text = async {
        futures::future::try_join_all(clauses.iter().filter_map(|clause| {
            let RankBy::Bm25 { attribute, .. } = clause else {
                return None;
            };
            if !segment::seg_text_matches(seg, &state.schema, attribute) {
                return None;
            }
            let file = seg.files.get(&format!("fts:{attribute}"))?;
            Some(async move {
                Ok::<_, ApiError>((attribute.clone(), segment::text_index(store, file).await?))
            })
        }))
        .await
        .map(|entries| entries.into_iter().collect::<BTreeMap<_, _>>())
    };
    // The ID index is read whole. For a big one, BM25 hits take their IDs from the
    // (ranged) `id` column of the documents file instead, for the winning rows only (one
    // more sequential read, but bytes that no longer grow with the segment). Segments
    // whose text index cannot be used still score through documents and need the index.
    let big_ids = seg
        .files
        .get("ids")
        .is_some_and(|f| f.size > RANGED_IDS_BYTES);
    let use_keys =
        columns.is_empty() && !all && !clauses.is_empty() && crate::text_keys::inline(seg);
    let (allowed, indexes, whole_ids, keys) = tokio::try_join!(
        snapshot::allowed(store, state, seg, q.filters.as_ref()),
        open_text,
        async {
            if clauses.is_empty() || big_ids || use_keys {
                Ok(None)
            } else {
                segment::ids(store, seg).await.map(Some)
            }
        },
        async {
            if use_keys {
                crate::text_keys::load(store, seg).await.map(Some)
            } else {
                Ok(None)
            }
        },
    )?;
    if allowed.is_empty() {
        return Ok(out);
    }
    let ids = if whole_ids.is_none()
        && clauses.iter().any(
            |c| matches!(c, RankBy::Bm25 { attribute, .. } if !indexes.contains_key(attribute)),
        ) {
        Some(segment::ids(store, seg).await?)
    } else {
        whole_ids
    };
    let mut bm25_hits = Vec::new();
    let mut selected = if all {
        allowed.clone()
    } else {
        RoaringBitmap::new()
    };
    // A `Sum` that already chose its winners hydrates just those rows.
    let searched: &[&RankBy] = if preset.is_some() { &[] } else { clauses };
    if let Some(preset) = preset {
        for (_, hits) in &preset {
            for hit in hits {
                selected.insert(hit.row);
            }
        }
        bm25_hits = preset;
    }
    for &clause in searched {
        let RankBy::Bm25 {
            attribute,
            query,
            params,
        } = clause
        else {
            unreachable!()
        };
        let cfg = state.schema[attribute]
            .text_config()
            .expect("validated FTS");
        let query = bf_text::Query::new(
            crate::text::tokens(query, &cfg),
            crate::text::prefix(params.as_ref()),
        );
        let fallback = bf_text::CorpusStats::default();
        let stats = state.corpus.get(attribute).unwrap_or(&fallback);
        let top = if all { allowed.len() as usize } else { k };
        let hits = if let Some(reader) = indexes.get(attribute) {
            if clause_top {
                // Exact cutoff ties: widen until the list ends strictly below the kth score,
                // so a tied row that wins the final ID tie-break is never dropped.
                let mut top = top + 1;
                loop {
                    let hits = reader
                        .search_prefetched(&query, stats, &cfg, top, Some(&allowed))
                        .await;
                    if hits
                        .as_ref()
                        .map_or(true, |hits| ties_complete(hits, k, top))
                    {
                        break hits;
                    }
                    top = top.saturating_mul(2);
                }
            } else if let Some(threshold) = threshold {
                reader
                    .search_prefetched_shared(&query, stats, &cfg, Some(&allowed), threshold)
                    .await
            } else {
                reader
                    .search_prefetched(&query, stats, &cfg, top, Some(&allowed))
                    .await
            }
            .map_err(segment::index_error)?
        } else {
            let columns = BTreeSet::from([attribute.clone()]);
            let docs = segment::projected_documents(store, seg, &allowed, Some(&columns)).await?;
            let mut hits = vec![];
            for row in &allowed {
                let value = docs[&ids.as_ref().expect("text IDs")[row as usize]]
                    .get(attribute)
                    .unwrap_or(&serde_json::Value::Null);
                let score =
                    bf_text::score_document(&crate::text::tokens(value, &cfg), &query, stats, &cfg);
                if score > 0.0 {
                    hits.push(bf_text::SearchHit { row, score });
                }
            }
            hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.row.cmp(&b.row)));
            if !clause_top {
                hits.truncate(top);
            }
            hits
        };
        for hit in &hits {
            selected.insert(hit.row);
        }
        bm25_hits.push((rank_key(clause), hits));
    }
    if let Some(rows) =
        crate::pattern::segment_scores(store, seg, q.rank_by.as_ref(), &allowed, &mut out).await?
    {
        selected &= rows;
    }
    crate::sparse::segment_scores(store, seg, q.rank_by.as_ref(), &allowed, &mut out).await?;
    if let Some(keys) = keys {
        for (key, hits) in bm25_hits {
            let scores = out.scores.entry(key).or_default();
            for hit in hits {
                let id = keys.id(hit.row)?;
                scores.insert(id.clone(), hit.score as f64);
                out.docs.entry(id.clone()).or_insert_with(|| {
                    std::sync::Arc::new(Document::from_iter([("id".into(), id.value())]))
                });
            }
        }
        return Ok(out);
    }
    if let Some(ids) = &ids {
        for (key, hits) in bm25_hits {
            let scores = out.scores.entry(key).or_default();
            for hit in hits {
                scores.insert(ids[hit.row as usize].clone(), hit.score as f64);
            }
        }
        out.docs.extend(crate::shared(
            segment::projected_documents(store, seg, &selected, Some(columns)).await?,
        ));
        return Ok(out);
    }
    let rows = segment::projected_rows(store, seg, &selected, Some(columns)).await?;
    let mut row_ids = BTreeMap::new();
    let mut docs = BTreeMap::new();
    for (row, doc) in rows {
        let id: Id = serde_json::from_value(doc.get("id").ok_or_else(segment::corrupt)?.clone())
            .map_err(segment::index_error)?;
        row_ids.insert(row, id.clone());
        docs.insert(id, doc);
    }
    for (key, hits) in bm25_hits {
        let scores = out.scores.entry(key).or_default();
        for hit in hits {
            if let Some(id) = row_ids.get(&hit.row) {
                scores.insert(id.clone(), hit.score as f64);
            }
        }
    }
    out.docs.extend(crate::shared(docs));
    Ok(out)
}
/// Open every segment for a `Sum` of BM25 clauses. `None` when any cannot answer from lists.
async fn sum_prepare(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    clauses: &[&RankBy],
) -> Result<Option<Vec<sum_text::SegmentSum>>, ApiError> {
    let prepared = sum_clauses(state, clauses);
    let opened = futures::future::try_join_all(
        state
            .segments
            .iter()
            .map(|seg| sum_text::open(store, state, q, seg, &prepared)),
    )
    .await?;
    Ok(opened.into_iter().collect())
}
fn sum_clauses<'a>(state: &'a NamespaceState, clauses: &[&'a RankBy]) -> Vec<sum_text::Clause<'a>> {
    static FALLBACK: std::sync::LazyLock<bf_text::CorpusStats> =
        std::sync::LazyLock::new(Default::default);
    clauses
        .iter()
        .map(|clause| {
            let RankBy::Bm25 {
                attribute,
                query,
                params,
            } = clause
            else {
                unreachable!()
            };
            let cfg = state.schema[attribute]
                .text_config()
                .expect("validated FTS");
            sum_text::Clause {
                attribute,
                key: rank_key(clause),
                query: bf_text::Query::new(
                    crate::text::tokens(query, &cfg),
                    crate::text::prefix(params.as_ref()),
                ),
                cfg,
                stats: state.corpus.get(attribute).unwrap_or(&FALLBACK),
            }
        })
        .collect()
}
/// A segment's chosen winners: per clause, its rank key and the winning rows' hits.
type Preset = Vec<(Vec<u8>, Vec<bf_text::SearchHit>)>;
/// Score and hydrate every segment (`preset` carries a `Sum`'s chosen winners per segment).
#[allow(clippy::too_many_arguments)]
async fn hydrate_segments(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    clauses: &[&RankBy],
    all: bool,
    clause_top: bool,
    k: usize,
    columns: &BTreeSet<String>,
    threshold: Option<&bf_text::SearchThreshold>,
    mut out: NamespaceState,
    mut preset: Option<Vec<Preset>>,
) -> Result<NamespaceState, ApiError> {
    let parts = futures::future::try_join_all(state.segments.iter().enumerate().map(|(i, seg)| {
        let preset = preset.as_mut().map(|p| std::mem::take(&mut p[i]));
        prepare_segment(
            store,
            state,
            q,
            seg,
            clauses,
            if preset.is_some() {
                false
            } else {
                all && !clause_top
            },
            clause_top,
            k,
            columns,
            threshold,
            preset,
            out.clone(),
        )
    }))
    .await?;
    for part in parts {
        out.docs.extend(part.docs);
        for (rank, scores) in part.scores {
            out.scores.entry(rank).or_default().extend(scores);
        }
    }
    Ok(out)
}
/// Decide a `Sum` from the lists (growing them a bounded number of times), and hydrate the
/// winning rows. Falls back to scoring every row when no list length proves the answer.
#[allow(clippy::too_many_arguments)]
async fn sum_finish(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    clauses: &[&RankBy],
    segments: Vec<sum_text::SegmentSum>,
    k: usize,
    columns: &BTreeSet<String>,
    threshold: Option<&bf_text::SearchThreshold>,
    out: NamespaceState,
) -> Result<NamespaceState, ApiError> {
    let prepared = sum_clauses(state, clauses);
    // Tail documents are already scored (every clause, in `out`); their totals take part too.
    let tail: Vec<(Id, f64)> = clauses
        .first()
        .and_then(|c| out.scores.get(&rank_key(c)))
        .map(|first| {
            first
                .keys()
                .map(|id| {
                    let t = sum_text::total(clauses.iter().map(|c| {
                        out.scores
                            .get(&rank_key(c))
                            .and_then(|m| m.get(id))
                            .copied()
                            .unwrap_or(0.0) as f32
                    }));
                    (id.clone(), t)
                })
                .collect()
        })
        .unwrap_or_default();
    let mut n = sum_text::first_list(k);
    while let Some(list) = n {
        let rounds =
            futures::future::try_join_all(segments.iter().map(|s| s.candidates(&prepared, list)))
                .await?;
        let mut totals: Vec<f64> = tail.iter().map(|(_, t)| *t).collect();
        let mut bound = 0f64;
        let mut exhaustive = true;
        for r in &rounds {
            totals.extend(r.rows.iter().map(|(_, t)| *t));
            bound = bound.max(r.bound);
            exhaustive &= r.exhaustive;
        }
        if let Some(floor) = sum_text::accept(&mut totals, k, bound, exhaustive) {
            let presets =
                futures::future::try_join_all(segments.iter().zip(&rounds).map(|(s, r)| {
                    let rows: RoaringBitmap = r
                        .rows
                        .iter()
                        .filter(|(_, t)| *t > 0.0 && *t >= floor)
                        .map(|(row, _)| *row)
                        .collect();
                    let prepared = &prepared;
                    async move { s.winners(prepared, &rows).await }
                }))
                .await?;
            return hydrate_segments(
                store,
                state,
                q,
                clauses,
                false,
                false,
                k,
                columns,
                threshold,
                out,
                Some(presets),
            )
            .await;
        }
        n = sum_text::next_list(list);
    }
    hydrate_segments(
        store, state, q, clauses, true, false, k, columns, threshold, out, None,
    )
    .await
}
async fn prepare_scalar(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    threshold: Option<&bf_text::SearchThreshold>,
) -> Result<NamespaceState, ApiError> {
    let mut clauses = vec![];
    if let Some(rank) = &q.rank_by {
        text_clauses(rank, &mut clauses);
    }
    let plain_text = matches!(q.rank_by, Some(RankBy::Bm25 { .. }));
    let k = q.total().unwrap_or(10000) + q.offset;
    let per = q.limit.as_ref().and_then(Limit::per).is_some();
    let all = !plain_text || per;
    // The segments of a `Max` of BM25 clauses return each clause's top list, not every row.
    // The WAL tail still scores every live tail document under every clause.
    let clause_top = max_of_text(q.rank_by.as_ref()) && !per;
    let mut out = result_state(state);
    let mut columns: BTreeSet<_> = state
        .schema
        .iter()
        .filter(|(name, cfg)| {
            cfg.data_type
                .as_ref()
                .is_none_or(|t| t.dimensions().is_none())
                || q.compute_attributes.is_some()
                || match &q.include_attributes {
                    Some(IncludeAttributes::All(true)) => true,
                    Some(IncludeAttributes::Names(names)) => names.contains(name),
                    _ => q
                        .exclude_attributes
                        .as_ref()
                        .is_some_and(|names| !names.contains(name)),
                }
        })
        .map(|(name, _)| name.clone())
        .collect();
    if text_ids_only(q) {
        columns.clear();
    }
    for clause in &clauses {
        out.scores.entry(rank_key(clause)).or_default();
    }
    let segment_started = std::time::Instant::now();
    // A `Sum` of BM25 clauses is decided from short per-clause lists when they prove the answer
    // (see `sum_text`); the segments are hydrated after the tail has scored its documents.
    let sum_plan = if sum_of_text(q.rank_by.as_ref()) && !per && sum_text::first_list(k).is_some() {
        sum_prepare(store, state, q, &clauses).await?
    } else {
        None
    };
    if sum_plan.is_none() {
        out = hydrate_segments(
            store, state, q, &clauses, all, clause_top, k, &columns, threshold, out, None,
        )
        .await?;
    }
    crate::ann::stage(segment_started, "bm25 segments+hydrate");
    if plain_text && state.docs.is_empty() {
        return Ok(out);
    }
    let tail_started = std::time::Instant::now();
    let mut token_time = std::time::Duration::ZERO;
    let mut score_time = std::time::Duration::ZERO;
    let analyzer = crate::text_tail::Analyzer::new(&state.schema);
    let prepared_queries: Vec<_> = clauses
        .iter()
        .map(|clause| {
            let RankBy::Bm25 {
                attribute,
                query,
                params,
            } = clause
            else {
                unreachable!()
            };
            let cfg = state.schema[attribute]
                .text_config()
                .expect("validated FTS");
            (
                attribute,
                rank_key(clause),
                bf_text::Query::new(
                    crate::text::tokens(query, &cfg),
                    crate::text::prefix(params.as_ref()),
                ),
                cfg,
            )
        })
        .collect();
    let fallback = bf_text::CorpusStats::default();
    let scorers: Vec<_> = prepared_queries
        .iter()
        .map(|(attribute, key, query, cfg)| {
            (
                attribute,
                key,
                bf_text::PreparedQuery::new(
                    query,
                    state.corpus.get(*attribute).unwrap_or(&fallback),
                    cfg,
                ),
            )
        })
        .collect();
    // A plain BM25 rank needs only its global k + offset winners. Keeping every
    // tail score also cloned the rank key and allocated tree entries for misses.
    let top_key = (!all).then(|| rank_key(q.rank_by.as_ref().expect("plain BM25")));
    let mut best = BinaryHeap::new();
    if let Some(scores) = top_key.as_ref().and_then(|key| out.scores.get(key)) {
        for (id, score) in scores {
            offer(&mut best, id, *score, true, k);
        }
    }
    for (id, doc) in &state.docs {
        if !q
            .filters
            .as_ref()
            .map(|f| filter::matches(f, Some(doc), None, &state.schema))
            .transpose()?
            .unwrap_or(true)
        {
            continue;
        }
        let before = crate::ann::trace_stages().then(std::time::Instant::now);
        let summary = (!scorers.is_empty()).then(|| analyzer.tail(doc));
        if let Some(before) = before {
            token_time += before.elapsed();
        }
        let before = crate::ann::trace_stages().then(std::time::Instant::now);
        for (attribute, key, scorer) in &scorers {
            let score = summary
                .as_ref()
                .expect("text clause")
                .fields
                .get(**attribute)
                .map_or(0.0, |terms| scorer.score(terms)) as f64;
            if top_key.is_some() {
                if score > 0.0 {
                    offer(&mut best, id, score, true, k);
                }
            } else {
                out.scores
                    .entry((*key).clone())
                    .or_default()
                    .insert(id.clone(), score);
            }
        }
        if let Some(before) = before {
            score_time += before.elapsed();
        }
        if all {
            out.docs.insert(id.clone(), doc.clone());
        }
    }
    if let Some(key) = top_key {
        let scores: BTreeMap<_, _> = best.into_iter().map(|hit| (hit.id, hit.score)).collect();
        out.docs.retain(|id, _| scores.contains_key(id));
        for id in scores.keys() {
            if let Some(doc) = state.docs.get(id) {
                out.docs.insert(id.clone(), doc.clone());
            }
        }
        out.scores.insert(key, scores);
    }
    if let Some(plan) = sum_plan {
        let started = std::time::Instant::now();
        out = sum_finish(store, state, q, &clauses, plan, k, &columns, threshold, out).await?;
        crate::ann::stage(started, "bm25 sum lists+hydrate");
    }
    crate::ann::stage(tail_started, "bm25 tail total");
    if crate::ann::trace_stages() {
        eprintln!("STAGE bm25 tail tokenization {} us", token_time.as_micros());
        eprintln!("STAGE bm25 tail scoring {} us", score_time.as_micros());
    }
    crate::sparse::tail_scores(q.rank_by.as_ref(), &state.docs, &mut out);
    crate::pattern::tail_scores(q.rank_by.as_ref(), &state.docs, &mut out)?;
    Ok(out)
}

#[cfg(test)]
mod ties_tests {
    use super::*;
    fn hits(scores: &[f32]) -> Vec<bf_text::SearchHit> {
        scores
            .iter()
            .enumerate()
            .map(|(row, &score)| bf_text::SearchHit {
                row: row as u32,
                score,
            })
            .collect()
    }
    #[test]
    fn cutoff_ties_are_complete_only_when_the_list_ends_below_the_kth_score() {
        // k = 2, asked for 3: the third hit ties the second, so more rows may tie too.
        assert!(!ties_complete(&hits(&[5.0, 4.0, 4.0]), 2, 3));
        // The third hit is strictly lower: every row tied with the second is already present.
        assert!(ties_complete(&hits(&[5.0, 4.0, 3.0]), 2, 3));
        // Fewer hits than asked for: the clause is exhausted.
        assert!(ties_complete(&hits(&[5.0, 4.0, 4.0]), 2, 4));
        assert!(ties_complete(&hits(&[]), 2, 3));
        // A widened list that ends below the tie is complete, and one that ends on it is not.
        assert!(ties_complete(&hits(&[5.0, 4.0, 4.0, 4.0, 1.0, 1.0]), 2, 6));
        assert!(!ties_complete(&hits(&[4.0; 6]), 2, 6));
        assert!(ties_complete(&hits(&[4.0]), 0, 1));
    }
}

#[cfg(test)]
mod rank_key_tests {
    use super::*;

    #[test]
    fn named_id_projects_the_same_attributes_as_default_ids_only() {
        let state = NamespaceState {
            schema: serde_json::from_value(serde_json::json!({
                "id": {"type": "uint"},
                "text": {"type": "string"},
                "vector": {"type": "[2]f16", "ann": true}
            }))
            .unwrap(),
            ..Default::default()
        };
        let default: QueryRequest = serde_json::from_value(serde_json::json!({
            "rank_by": ["vector", "ANN", [0.1, 0.2]], "top_k": 10
        }))
        .unwrap();
        let mut named = default.clone();
        named.include_attributes = Some(IncludeAttributes::Names(vec!["id".into()]));
        let mut none = default.clone();
        none.include_attributes = Some(IncludeAttributes::All(false));
        for narrow in [false, true] {
            assert!(projection_columns(&state, &none, narrow).is_empty());
            assert!(projection_columns(&state, &default, narrow).is_empty());
            assert_eq!(
                projection_columns(&state, &default, narrow),
                projection_columns(&state, &named, narrow)
            );
        }
        named.include_attributes = Some(IncludeAttributes::Names(vec!["id".into(), "text".into()]));
        assert_eq!(
            projection_columns(&state, &named, true),
            BTreeSet::from(["text".into()])
        );
    }

    #[test]
    fn vector_keys_preserve_float_bits_and_unambiguous_rank_identity() {
        let values = [0.0f32, -0.0, 1.5, -3.25e-7, f32::MAX, 7.0e-45];
        let rank: RankBy =
            serde_json::from_value(serde_json::json!(["vector", "ANN", values])).unwrap();
        let key = rank_key(&rank);
        assert_eq!(&key[..2], &[0xff, 0]);
        assert_eq!(&key[2..10], &6u64.to_le_bytes());
        assert_eq!(&key[10..16], b"vector");
        let decoded: Vec<u32> = key[16..]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| u32::from_le_bytes(*b))
            .collect();
        assert_eq!(decoded, values.map(f32::to_bits));
        let shapes = [
            serde_json::json!(["v", "ANN", [0.0]]),
            serde_json::json!(["v", "ANN", [-0.0]]),
            serde_json::json!(["v", "kNN", [0.0]]),
            serde_json::json!(["v:", "ANN", [0.0]]),
            serde_json::json!(["v", "ANN", [0.0, 0.0]]),
            serde_json::json!(["id", "asc"]),
            serde_json::json!(["text", "BM25", "v"]),
        ];
        let keys: BTreeSet<_> = shapes
            .iter()
            .map(|shape| rank_key(&serde_json::from_value(shape.clone()).unwrap()))
            .collect();
        assert_eq!(keys.len(), shapes.len());
        assert_eq!(rank_key(&rank.clone()), key);
    }
}

#[cfg(test)]
mod union_truth_tests {
    use super::*;
    fn answer(rows: &[(&str, f64)]) -> QueryResponse {
        let mut r: QueryResponse = serde_json::from_value(serde_json::json!({
            "rows": [], "performance": {"cache_hit_ratio":0.0,"cache_temperature":"cold",
            "server_total_ms":0,"query_execution_ms":0,"exhaustive_search_count":0,
            "approx_namespace_size":0},
            "billing": {"billable_logical_bytes_queried":0,"billable_logical_bytes_returned":0}
        }))
        .unwrap();
        r.result.rows = Some(
            rows.iter()
                .map(|(id, d)| {
                    let mut row = Document::new();
                    row.insert("id".into(), serde_json::json!(id));
                    row.insert("$dist".into(), serde_json::json!(d));
                    row
                })
                .collect(),
        );
        r
    }
    fn ids(r: &QueryResponse) -> Vec<String> {
        r.result
            .rows
            .iter()
            .flatten()
            .map(|r| r["id"].to_string())
            .collect()
    }
    #[test]
    fn union_truth_is_the_exact_top_k_of_everything_any_answer_found() {
        let graph = answer(&[("a", 0.1), ("c", 0.3), ("d", 0.4)]);
        let ivf = answer(&[("a", 0.1), ("b", 0.2), ("e", 0.5)]);
        let truth = union_truth(&[&graph, &ivf]).unwrap();
        assert_eq!(ids(&truth), ["\"a\"", "\"b\"", "\"c\""]);
        // The graph missed `b`, which the other answer found: it loses recall; IVF misses `c`.
        assert!((recall(&graph, &truth) - 2. / 3.).abs() < 1e-12);
        assert!((recall(&ivf, &truth) - 2. / 3.).abs() < 1e-12);
    }
    #[test]
    fn ties_break_by_id_and_a_row_without_a_distance_declines() {
        let a = answer(&[("b", 0.1), ("a", 0.1)]);
        assert_eq!(ids(&union_truth(&[&a]).unwrap()), ["\"a\"", "\"b\""]);
        let mut bad = answer(&[("a", 0.1)]);
        bad.result.rows.as_mut().unwrap()[0].remove("$dist");
        assert!(union_truth(&[&bad]).is_none());
    }
}
