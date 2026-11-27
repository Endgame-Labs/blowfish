//! The typed ID-only query path: a plain approximate vector query that the resident warm graph
//! serves, answered from the graph's `(row, distance)` hits to the response's `(ID, distance)`
//! pairs with no `Document`, attribute map, request clone, or second pass of the general result
//! evaluator. It is an exact shortcut: whenever any condition of the general path is not
//! plainly met, [`Engine::query_ids`] returns `None` and the caller runs [`Engine::query`], which
//! produces the same answer by the long way (the tests compare the two byte for byte).
use crate::{Engine, NamespaceState, lineage, query_worker, storage_error, warm_graph};
use bf_types::{
    ApiError, Consistency, DistanceMetric, IdRef, IncludeAttributes, QueryPerformance,
    QueryRequest, RankBy, VectorEncoding,
};
use std::{sync::Arc, time::Instant};

/// The answer of [`Engine::query_ids`]: the rows and the measured performance. Write it with
/// [`IdsAnswer::write_json`].
pub struct IdsAnswer {
    pub(crate) hits: Vec<(IdRef, f64)>,
    pub(crate) performance: QueryPerformance,
}
impl IdsAnswer {
    /// The `(ID, distance)` of every row, nearest first.
    pub fn rows(&self) -> &[(IdRef, f64)] {
        &self.hits
    }
    /// The measured performance.
    pub fn performance(&self) -> &QueryPerformance {
        &self.performance
    }
    /// Append the response body: the bytes [`Engine::query`]'s response serializes to.
    /// `false` (with `out` unchanged) when a distance is not a finite number.
    pub fn write_json(&self, out: &mut Vec<u8>) -> bool {
        bf_types::QueryResponse::write_id_rows(out, &self.hits, &self.performance)
    }
}

/// The parts of a plain ID-only request the typed path reads: `(attribute, vector, k)`.
fn plain_shape(request: &QueryRequest) -> Option<(&str, &[f32], usize)> {
    let Some(RankBy::Vector {
        attribute,
        exact: false,
        vector,
        ..
    }) = &request.rank_by
    else {
        return None;
    };
    let k = request.top_k?;
    let ids_only = matches!(
        request.include_attributes,
        None | Some(IncludeAttributes::All(false))
    );
    (ids_only
        && (1..=100).contains(&k)
        && request.limit.is_none()
        && request.offset == 0
        && request.filters.is_none()
        && request.exclude_attributes.is_none()
        && request.compute_attributes.is_none()
        && request.aggregate_by.is_none()
        && request.group_by.is_none()
        && request.queries.is_none()
        && request.rerank_by.is_none()
        && request.distance_metric.is_none()
        && request.extra.is_empty()
        && request.vector_encoding == VectorEncoding::Float
        && matches!(request.consistency, Consistency::Strong))
    .then_some((attribute.as_str(), vector.as_slice(), k))
}

/// The best `k` of the graph's rows and the state's tail documents, as the general path's
/// `merge_tail` and result evaluator order them (distance, then ID). `None` when the answer is
/// not plain: a tail error (the general path reports it), a winner whose ID is neither an integer nor a canonical UUID, or an ID
/// twice.
fn merge_tail(
    state: &NamespaceState,
    attribute: &str,
    vector: &[f32],
    k: usize,
    hits: Vec<(IdRef, f64)>,
    tail: &crate::tail::VectorCache,
) -> Option<Vec<(IdRef, f64)>> {
    let mut heap = std::collections::BinaryHeap::new();
    for (id, distance) in hits {
        crate::indexed::offer(&mut heap, &id.to_id(), distance, false, k);
    }
    crate::indexed::score_tail(
        state,
        None,
        attribute,
        vector,
        false,
        k,
        Some((tail, true)),
        &mut heap,
        &mut std::collections::BTreeMap::new(),
    )
    .ok()?;
    let mut winners = heap.into_vec();
    winners.sort_unstable();
    let merged = winners
        .into_iter()
        .map(|hit| IdRef::from_id(&hit.id).map(|id| (id, hit.score)))
        .collect::<Option<Vec<_>>>()?;
    let mut ids: Vec<IdRef> = merged.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    (!ids.windows(2).any(|w| w[0] == w[1])).then_some(merged)
}

/// `last_included_write_at` for a state, formatted once per timestamp on each thread.
fn write_stamp(at: chrono::DateTime<chrono::Utc>) -> String {
    thread_local! {
        static LAST: std::cell::RefCell<Option<(chrono::DateTime<chrono::Utc>, String)>> =
            const { std::cell::RefCell::new(None) };
    }
    LAST.with(|last| {
        let mut last = last.borrow_mut();
        match last.as_ref() {
            Some((when, text)) if *when == at => text.clone(),
            _ => {
                let text = at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
                *last = Some((at, text.clone()));
                text
            }
        }
    })
}

impl Engine {
    /// Answer a plain ID-only approximate vector query from the resident warm graph without
    /// building documents, or `None` when this path does not apply and [`Engine::query`] must run
    /// (another request shape, a namespace with an unindexed tail or deleted rows still shadowing
    /// the index, a graph that is not ready or not yet holding its ID table, a changed
    /// snapshot, or any decline). Never returns a different answer from `query`.
    ///
    /// It runs only on an HTTP reactor, where graph queries already run inline.
    pub async fn query_ids(
        &self,
        ns: &str,
        request: &QueryRequest,
    ) -> Result<Option<IdsAnswer>, ApiError> {
        let Some((attribute, vector, k)) = plain_shape(request) else {
            return Ok(None);
        };
        if !query_worker::is_reactor() || self.0.config.models.is_some() {
            return Ok(None);
        }
        let start = Instant::now();
        let Some(handle) = self.0.namespaces.load().get(ns).cloned() else {
            return Ok(None);
        };
        let state = handle.state.load();
        let fresh = |state: &NamespaceState| {
            state.exists
                && state.seq > 0
                && state.checkpoint_refreshed.elapsed() < self.0.config.retention.div_f64(2.0)
        };
        if !fresh(&state)
            || state.metric != DistanceMetric::CosineDistance
            || state.segments.is_empty()
        {
            return Ok(None);
        }
        let Some([set]) = state.ann_tiers.get(attribute).map(Vec::as_slice) else {
            return Ok(None);
        };
        // The WAL-next check runs while the search does, as in the general path: the search is
        // valid only if the namespace did not move meanwhile.
        let probe = async {
            let key = handle.wal_key(&self.0.store, ns, state.seq + 1)?;
            match handle.wal_probe.exists(&self.0.store, &key).await {
                Ok(false) => {
                    let current = handle.state.load();
                    Ok(current.seq == state.seq
                        && current.manifest_seq == state.manifest_seq
                        && fresh(&current))
                }
                Ok(true) => Ok(false),
                Err(error) => Err(storage_error(error)),
            }
        };
        let cpu = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let search = {
            let cpu = cpu.clone();
            let state = &state;
            async move {
                let started = warm_graph::thread_cpu();
                let result = async {
                    let at = lineage::place_memoized(&self.0.store, state, set)
                        .await
                        .ok()??;
                    if !at.covers_all(state.segments.len()) {
                        return None;
                    }
                    let mask = crate::ann::tier_mask(&self.0.store, state, set, &at)
                        .await
                        .ok()?;
                    // The walk hides the rows the unindexed tail shadows (overwritten or deleted
                    // IDs) while still navigating through them, as the general path's does.
                    let navigate =
                        crate::ann::graph_mask(&self.0.store, state, set, &at, mask.as_ref())
                            .await
                            .ok()?;
                    warm_graph::search_ids(
                        &self.0.hot_graph,
                        ns,
                        state,
                        attribute,
                        set,
                        &at,
                        vector,
                        mask.as_ref(),
                        navigate.as_ref(),
                        k,
                    )
                    .await
                }
                .await;
                let seconds = (warm_graph::thread_cpu() - started).max(0.);
                cpu.fetch_add((seconds * 1e9) as u64, std::sync::atomic::Ordering::Relaxed);
                result
            }
        };
        let (probed, found) = tokio::join!(probe, search);
        let (true, Some(hits)) = (probed?, found) else {
            return Ok(None);
        };
        let hits: Vec<(IdRef, f64)> = hits
            .into_iter()
            .map(|(id, distance)| (id, f64::from(distance)))
            .collect();
        // The unindexed tail: every live tail document is scored with the general path's
        // kernel and competes with the graph's rows, in the evaluator's order.
        let hits = if state.docs.is_empty() {
            hits
        } else {
            let tail = self.0.read_cache.tail_vectors.clone();
            let Some(merged) = merge_tail(&state, attribute, vector, k, hits, &tail) else {
                return Ok(None);
            };
            merged
        };
        metrics::counter!("blowfish_query_cpu_dispatch_total", "path" => "inline").increment(1);
        warm_graph::Cache::observe(
            self,
            ns,
            &state,
            request,
            cpu.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e9,
        );
        self.sample_recall(&state, request, || {
            Some(warm_graph::Scope::new(self.0.hot_graph.clone(), ns))
        });
        let stats = bf_storage::CacheStats::default().snapshot();
        let elapsed = start.elapsed().as_millis() as u64;
        let performance = QueryPerformance {
            cache_hit_ratio: stats.cache_hit_ratio(),
            cache_temperature: stats.temperature().into(),
            server_total_ms: elapsed,
            query_execution_ms: elapsed,
            exhaustive_search_count: state.docs.len() as u64,
            approx_namespace_size: state.stats.rows + state.docs.len() as u64,
            last_included_write_at: Some(write_stamp(state.updated_at)),
            embedding_tokens: None,
            embedding_ms: None,
            rerank_ms: None,
        };
        Ok(Some(IdsAnswer { hits, performance }))
    }
}
