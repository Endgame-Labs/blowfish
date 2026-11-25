//! Synchronous answers for resident graph queries.
//!
//! A bare top-10 ANN query against a namespace whose single recluster tier has a ready graph
//! needs no I/O: the snapshot is already published, navigation runs over RAM, the exact rerank
//! reads memory-resident source rows, and the winners' IDs sit in decoded column blocks. This
//! module answers that shape on the calling thread, with no per-query plan futures, owned
//! request copies, read contexts, or document maps. Whatever it cannot prove resident, it
//! declines, and the caller runs the ordinary plan, which reaches the same rows and distances.
use crate::{Engine, NamespaceState, lineage, query_worker, segment, warm_graph};
use bf_types::{
    ApiError, DistanceMetric, Document, Id, QueryBilling, QueryPerformance, QueryRequest,
    QueryResponse, RankBy, SingleQueryResult,
};
use serde_json::Value;
use std::{sync::Arc, time::Instant};

/// The bare ANN shape this module answers, borrowed from the request.
struct Bare<'a> {
    attribute: &'a str,
    vector: &'a [f32],
    k: usize,
}

fn bare(request: &QueryRequest) -> Option<Bare<'_>> {
    let Some(RankBy::Vector {
        attribute,
        vector,
        exact: false,
        ..
    }) = &request.rank_by
    else {
        return None;
    };
    let k = request.total()?;
    (request.extra.is_empty()
        && request.filters.is_none()
        && request.queries.is_none()
        && request.aggregate_by.is_none()
        && request.group_by.is_none()
        && request.rerank_by.is_none()
        && request.limit.is_none()
        && request.compute_attributes.is_none()
        && matches!(
            request.include_attributes,
            None | Some(bf_types::IncludeAttributes::All(false))
        )
        && request.exclude_attributes.is_none()
        && request.distance_metric.is_none()
        && request.offset == 0
        && k > 0
        && k <= 10)
        .then_some(Bare {
            attribute,
            vector,
            k,
        })
}

/// Whether the synchronous path is enabled. This is the round G6 kill-test flag: with
/// `BF_QUERY_SYNC` naming a file, the path is on while that file exists (re-checked every 256
/// queries), so one running server can be measured with the path off and on over one graph.
/// If it passes, the flag goes away and the path is the only one.
fn enabled() -> bool {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
    static ON: AtomicU64 = AtomicU64::new(0);
    static CALLS: AtomicU64 = AtomicU64::new(0);
    if cfg!(test) {
        return true;
    }
    let Some(flag) = STATE.get_or_init(|| std::env::var_os("BF_QUERY_SYNC").map(Into::into)) else {
        return false;
    };
    if CALLS.fetch_add(1, Ordering::Relaxed) % 256 == 0 {
        ON.store(u64::from(flag.exists()), Ordering::Relaxed);
    }
    ON.load(Ordering::Relaxed) == 1
}

impl Engine {
    /// Answer `request` without suspending, or `None` when it needs the ordinary plan. Only
    /// the strong-consistency log check awaits; every other step either completes at once or
    /// declines.
    pub async fn query_resident(
        &self,
        ns: &str,
        request: &QueryRequest,
    ) -> Result<Option<QueryResponse>, ApiError> {
        // Tests drive the engine without a per-core reactor.
        if !(enabled() && (query_worker::is_reactor() || cfg!(test)))
            || !matches!(request.consistency, bf_types::Consistency::Strong)
        {
            return Ok(None);
        }
        let Some(plan) = bare(request) else {
            return Ok(None);
        };
        if self.0.config.models.is_some() || self.0.read_cache.staging.is_some() {
            return Ok(None);
        }
        let Some(handle) = self.0.namespaces.load().get(ns).cloned() else {
            return Ok(None);
        };
        let state = handle.state.load();
        let fresh = self.0.config.retention.div_f64(2.0);
        if !state.exists || state.seq == 0 || state.checkpoint_refreshed.elapsed() >= fresh {
            return Ok(None);
        }
        // Everything the answer needs from the state must already be settled, so that the
        // synchronous steps below are the whole plan.
        if state.metric != DistanceMetric::CosineDistance
            || !state.docs.is_empty()
            || state.tail_bytes != 0
            || !state.shadowed.is_empty()
            || !matches!(
                state.ann_tiers.get(plan.attribute).map(|t| t.len()),
                Some(1)
            )
        {
            return Ok(None);
        }
        let set = &state.ann_tiers[plan.attribute][0];
        let Some(places) = lineage::direct(&state.segments, set) else {
            return Ok(None);
        };
        if crate::query::validate_rank(&state, request.rank_by.as_ref().expect("bare"), false)
            .is_err()
        {
            return Ok(None);
        }
        let start = Instant::now();
        // Strong consistency: the log must hold nothing after the published snapshot.
        let wal = bf_wal::Wal::new(self.0.store.clone(), ns).map_err(crate::log_error)?;
        match self.0.store.head(&wal.key(state.seq + 1)).await {
            Err(bf_storage::Error::NotFound) => {}
            Ok(_) => return Ok(None),
            Err(error) => return Err(crate::storage_error(error)),
        }
        let current = handle.state.load();
        if current.seq != state.seq
            || current.manifest_seq != state.manifest_seq
            || current.checkpoint_refreshed.elapsed() >= fresh
        {
            return Ok(None);
        }
        let _in_flight = crate::InFlight::enter();
        let cpu_started = warm_graph::thread_cpu();
        let Some(response) = self.answer_resident(ns, &state, set, &places, &plan, start) else {
            return Ok(None);
        };
        warm_graph::Cache::observe(
            self,
            ns,
            &state,
            request,
            (warm_graph::thread_cpu() - cpu_started).max(0.),
        );
        Ok(Some(response))
    }

    fn answer_resident(
        &self,
        ns: &str,
        state: &Arc<NamespaceState>,
        set: &bf_wal::SlabSet,
        places: &[usize],
        plan: &Bare<'_>,
        start: Instant,
    ) -> Option<QueryResponse> {
        let found = self.0.hot_graph.answer_resident(
            ns,
            state,
            plan.attribute,
            set,
            plan.vector,
            plan.k,
        )?;
        // Map each winner's global row to its segment and local row, then read IDs from the
        // decoded column blocks, one segment at a time.
        let mut by_segment: Vec<(usize, Vec<(u32, usize)>)> = Vec::new();
        for (n, &(row, _)) in found.iter().enumerate() {
            let p = set
                .covered
                .partition_point(|c| c.base <= row)
                .checked_sub(1)?;
            let (at, local) = (places[p], row - set.covered[p].base);
            match by_segment.iter_mut().find(|(s, _)| *s == at) {
                Some((_, rows)) => rows.push((local, n)),
                None => by_segment.push((at, vec![(local, n)])),
            }
        }
        let mut ids: Vec<Option<Id>> = vec![None; found.len()];
        for (at, mut rows) in by_segment {
            rows.sort_unstable();
            let locals: Vec<u32> = rows.iter().map(|r| r.0).collect();
            let got = segment::cached_ids(&self.0.read_cache, &state.segments[at], &locals)?;
            for ((_, n), id) in rows.into_iter().zip(got) {
                ids[n] = Some(id);
            }
        }
        let mut ranked: Vec<(f64, Id)> = Vec::with_capacity(found.len());
        for ((_, dist), id) in found.iter().zip(ids) {
            let dist = f64::from(*dist);
            if !dist.is_finite() {
                return None;
            }
            ranked.push((dist, id?));
        }
        ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        // A repeated ID would have collapsed in the ordinary plan's maps.
        if ranked
            .iter()
            .enumerate()
            .any(|(n, a)| ranked[..n].iter().any(|b| a.1 == b.1))
        {
            return None;
        }
        let rows = ranked
            .into_iter()
            .map(|(dist, id)| {
                let mut row = Document::new();
                row.insert(
                    "id".into(),
                    match id {
                        Id::Uint(n) => Value::from(n),
                        Id::String(s) => Value::from(s),
                    },
                );
                row.insert("$dist".into(), Value::from(dist));
                row
            })
            .collect();
        let elapsed = start.elapsed();
        Some(QueryResponse {
            result: SingleQueryResult {
                rows: Some(rows),
                ..Default::default()
            },
            results: None,
            performance: QueryPerformance {
                cache_hit_ratio: 1.0,
                cache_temperature: "hot".into(),
                server_total_ms: elapsed.as_millis() as u64,
                query_execution_ms: elapsed.as_millis() as u64,
                exhaustive_search_count: 0,
                approx_namespace_size: state.stats.rows,
                last_included_write_at: Some(
                    state
                        .updated_at
                        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                ),
                embedding_tokens: None,
                embedding_ms: None,
                rerank_ms: None,
            },
            billing: QueryBilling::default(),
        })
    }
}
