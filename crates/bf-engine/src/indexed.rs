//! Per-segment candidate search, tail scoring, and reference-compatible result assembly.
use crate::{EngineConfig, NamespaceState, filter, query, segment, snapshot};
use bf_storage::Storage;
use bf_types::*;
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringBitmap;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};

pub(crate) fn rank_key(rank: &RankBy) -> String {
    match rank {
        // Exact and much cheaper than serializing the query vector as JSON: the bits in hex.
        // (The prefix cannot begin a JSON value, so these keys never equal another kind's.)
        RankBy::Vector {
            attribute,
            exact,
            vector,
            ..
        } => {
            use std::fmt::Write;
            let mut key = String::with_capacity(attribute.len() + 12 + vector.len() * 8);
            let _ = write!(key, "~vector {exact} {}:", attribute.len());
            key.push_str(attribute);
            for x in vector {
                let _ = write!(key, "{:08x}", x.to_bits());
            }
            key
        }
        _ => rank.value().to_string(),
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

pub(crate) async fn execute(
    store: &Storage,
    state: &NamespaceState,
    request: &QueryRequest,
    config: &EngineConfig,
    force_exact: bool,
) -> Result<QueryResponse, ApiError> {
    // Validate the complete request before touching index files, preserving M1 errors.
    let mut empty = state.clone();
    empty.docs.clear();
    query::execute(&empty, request)?;
    if crate::streaming::supports(request) {
        return crate::streaming::execute(store, state, request).await;
    }
    // Independent subqueries can each stream without building a union of all candidates.
    if let Some(queries) = &request.queries
        && request.rerank_by.is_none()
    {
        let mut results = Vec::with_capacity(queries.len());
        for q in queries {
            let mut q = q.clone();
            q.vector_encoding = request.vector_encoding;
            results.push(
                Box::pin(execute(store, state, &q, config, force_exact))
                    .await?
                    .result,
            );
        }
        return Ok(QueryResponse {
            result: Default::default(),
            results: Some(results),
            performance: Default::default(),
            billing: Default::default(),
        });
    }
    let mut prepared = state.clone();
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
    if !clauses.is_empty() {
        let mut terms = crate::text::QueryTerms::new();
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
        prepared.corpus = crate::text::global_stats(store, state, &terms).await?;
    }
    let state = &prepared;
    let mut candidates = state.clone();
    candidates.docs.clear();
    // Subqueries (for example the ANN and BM25 halves of a hybrid query) read independent
    // files, so they run concurrently. Results merge in request order.
    let parts = futures::future::try_join_all(
        request
            .queries
            .as_deref()
            .unwrap_or(std::slice::from_ref(request))
            .iter()
            .map(|q| {
                prepare(
                    store,
                    state,
                    q,
                    config,
                    force_exact,
                    request.queries.is_none(),
                )
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
    query::execute(&candidates, request)
}
#[derive(Clone)]
struct HitKey {
    score: f64,
    id: Id,
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
/// Keep only the globally competitive candidates while merging segment-local top-k lists.
fn trim_candidates(state: &mut NamespaceState, q: &QueryRequest) {
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
        let hit = HitKey {
            id: id.clone(),
            score: *score,
            descending: matches!(rank, RankBy::Bm25 { .. }),
        };
        if heap.len() < k {
            heap.push(hit);
        } else if heap.peek().is_some_and(|worst| hit < *worst) {
            heap.pop();
            heap.push(hit);
        }
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
            let mut tail = state.clone();
            tail.segments.clear();
            let mut out = prepare_one(store, &tail, q, config, force_exact, narrow_columns).await?;
            out.docs.extend(crate::shared(docs));
            out.scores.entry(rank_key(rank)).or_default().extend(scores);
            trim_candidates(&mut out, q);
            return Ok(out);
        }
    }
    let Some(context) = crate::read_cache::context().filter(|_| state.segments.len() > 1) else {
        let mut out = prepare_one(store, state, q, config, force_exact, narrow_columns).await?;
        trim_candidates(&mut out, q);
        return Ok(out);
    };
    let mut base = state.clone();
    base.docs.clear();
    base.segments.clear();
    let base = std::sync::Arc::new(base);
    let parts = futures::stream::iter(state.segments.iter().cloned().map(|segment| {
        let context = context.clone();
        let store = store.clone();
        let config = config.clone();
        let q = q.clone();
        let base = base.clone();
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
                    prepare_one(&store, &state, &q, &config, force_exact, narrow_columns),
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
    let part = prepare_one(store, &tail, q, config, force_exact, narrow_columns).await?;
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
        None => Some(BTreeSet::new()),
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
    columns
}
async fn prepare_one(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
    config: &EngineConfig,
    force_exact: bool,
    narrow_columns: bool,
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
        return prepare_scalar(store, state, q).await;
    };
    if state.metric == DistanceMetric::CosineDistance && vector.iter().all(|x| *x == 0.0) {
        return snapshot::materialize(store, state).await;
    }
    // Diversification can need arbitrarily deep candidates from each segment.
    let all = q.limit.as_ref().and_then(Limit::per).is_some();
    let k = q.total().unwrap_or(10) + q.offset;
    let mut out = state.clone();
    out.docs.clear();
    let mut scores = BTreeMap::new();
    let columns = projection_columns(state, q, narrow_columns);
    for seg in &state.segments {
        let file = seg.files.get(&format!("ann:{attribute}"));
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
            // Version-one document-only segments remain readable during rollout.
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
    for (id, doc) in &state.docs {
        if q.filters
            .as_ref()
            .map(|f| filter::matches(f, Some(doc), None, &state.schema))
            .transpose()?
            .unwrap_or(true)
            && let Some(v) = doc.get(attribute)
        {
            scores.insert(
                id.clone(),
                query::distance(&decode_vector(v)?, vector, state.metric),
            );
            out.docs.insert(id.clone(), doc.clone());
        }
    }
    out.scores.insert(rank_key(rank), scores);
    Ok(out)
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

/// ID indexes above this size are not read whole by BM25 queries.
pub(crate) const RANGED_IDS_BYTES: u64 = 128 << 10;
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
    k: usize,
    columns: &BTreeSet<String>,
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
    let (allowed, indexes, whole_ids) = tokio::try_join!(
        snapshot::allowed(store, state, seg, q.filters.as_ref()),
        open_text,
        async {
            if clauses.is_empty() || big_ids {
                Ok(None)
            } else {
                segment::ids(store, seg).await.map(Some)
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
    let mut bm25_hits: Vec<(String, Vec<bf_text::SearchHit>)> = vec![];
    let mut selected = if all {
        allowed.clone()
    } else {
        RoaringBitmap::new()
    };
    for &clause in clauses {
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
            reader
                .search_prefetched(&query, stats, &cfg, top, Some(&allowed))
                .await
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
            hits.truncate(top);
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
async fn prepare_scalar(
    store: &Storage,
    state: &NamespaceState,
    q: &QueryRequest,
) -> Result<NamespaceState, ApiError> {
    let mut clauses = vec![];
    if let Some(rank) = &q.rank_by {
        text_clauses(rank, &mut clauses);
    }
    let plain_text = matches!(q.rank_by, Some(RankBy::Bm25 { .. }));
    let k = q.total().unwrap_or(10000) + q.offset;
    let all = !plain_text || q.limit.as_ref().and_then(Limit::per).is_some();
    let mut out = state.clone();
    out.docs.clear();
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
    if plain_text
        && q.filters.is_none()
        && q.include_attributes.is_none()
        && q.exclude_attributes.is_none()
        && q.compute_attributes.is_none()
        && q.limit.as_ref().and_then(Limit::per).is_none()
    {
        columns.clear();
    }
    for clause in &clauses {
        out.scores.entry(rank_key(clause)).or_default();
    }
    let parts = futures::future::try_join_all(state.segments.iter().map(|seg| {
        prepare_segment(
            store,
            state,
            q,
            seg,
            &clauses,
            all,
            k,
            &columns,
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
        let mut positive = false;
        for clause in &clauses {
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
            let score = bf_text::score_document(
                &crate::text::tokens(doc.get(attribute).unwrap_or(&serde_json::Value::Null), &cfg),
                &query,
                stats,
                &cfg,
            ) as f64;
            out.scores
                .entry(rank_key(clause))
                .or_default()
                .insert(id.clone(), score);
            positive |= score > 0.0;
        }
        if all || positive {
            out.docs.insert(id.clone(), doc.clone());
        }
    }
    crate::sparse::tail_scores(q.rank_by.as_ref(), &state.docs, &mut out);
    crate::pattern::tail_scores(q.rank_by.as_ref(), &state.docs, &mut out)?;
    Ok(out)
}
