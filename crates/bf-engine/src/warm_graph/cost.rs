//! Measure warm serving cost in bounded query blocks, away from exhaustive scan interference.
use super::*;

const QUERIES: usize = 8;
const BYTES: usize = 64 << 10;

#[derive(Default)]
pub(super) struct Pending {
    stamp: (u64, u64, usize),
    bytes: usize,
    pub(super) queries: Vec<QueryRequest>,
}
impl Pending {
    fn push(
        &mut self,
        stamp: (u64, u64, usize),
        query: &QueryRequest,
    ) -> Option<Vec<QueryRequest>> {
        let bytes = serde_json::to_vec(query).ok()?.len();
        if bytes > BYTES {
            return None;
        }
        if self.stamp != stamp || self.bytes.saturating_add(bytes) > BYTES {
            *self = Self {
                stamp,
                ..Self::default()
            };
        }
        self.bytes += bytes;
        self.queries.push(query.clone());
        if self.queries.len() == QUERIES {
            self.bytes = 0;
            Some(std::mem::take(&mut self.queries))
        } else {
            None
        }
    }
}

/// Quality observations retain their original exact comparison. Cost uses eight distinct
/// samples together: streaming truth between every timed graph trial otherwise measures a
/// cold CPU working set, even when ordinary serving has sustained heat. The first query's
/// cold cost remains in the average; no identical-query warmup is excluded from measurement.
pub(crate) async fn measure(
    scope: &Scope,
    store: &Storage,
    state: &NamespaceState,
    query: &QueryRequest,
    config: &crate::EngineConfig,
    context: read_cache::ReadContext,
) {
    let Some(keys) = scope.used.lock().unwrap_or_else(|e| e.into_inner()).clone() else {
        return;
    };
    let [key] = keys.as_slice() else { return };
    let entry = scope
        .cache
        .entries
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(key)
        .and_then(|h| h.ready.clone());
    let Some(entry) = entry else { return };
    let ef = entry
        .calibration
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .quality
        .trial;
    // This allocation is covered by the engine's per-graph metadata allowance. Requests,
    // source generations, and exact responses are never retained beyond this bounded queue.
    let batch = entry
        .pending_cost
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((state.seq, state.manifest_seq, ef), query);
    let Some(batch) = batch else { return };
    let mut graph_cpu = 0.;
    for request in &batch {
        let trial = scope.trial();
        let trial_context = read_cache::ReadContext {
            hot_graph: Some(trial.clone()),
            ..context.clone()
        };
        let Ok((_, seconds)) = trial_plan(store, state, request, config, trial_context).await
        else {
            return;
        };
        if !trial.used_graph() {
            return;
        }
        graph_cpu += seconds;
    }
    let mut ivf_cpu = 0.;
    for request in &batch {
        let Ok((_, seconds)) =
            trial_plan(store, state, request, config, context.ivf_reference()).await
        else {
            return;
        };
        ivf_cpu += seconds;
    }
    {
        let mut c = entry.calibration.lock().unwrap_or_else(|e| e.into_inner());
        if c.quality.trial != ef {
            return; // A concurrent comparison changed effort while this batch ran.
        }
        let seconds = graph_cpu / batch.len() as f64;
        c.trial_seconds = if c.trial_seconds == 0. {
            seconds
        } else {
            c.trial_seconds * 0.8 + seconds * 0.2
        };
    }
    scope.record_ivf_cpu(ivf_cpu / batch.len() as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cost_batches_are_bounded_and_never_cross_snapshots_or_efforts() {
        let q: QueryRequest = serde_json::from_value(
            serde_json::json!({"rank_by":["vector","ANN",[1.,0.]],"top_k":10}),
        )
        .unwrap();
        let mut pending = Pending::default();
        for _ in 0..7 {
            assert!(pending.push((1, 1, 64), &q).is_none());
        }
        assert!(pending.push((2, 1, 64), &q).is_none());
        assert_eq!(pending.queries.len(), 1);
        assert!(pending.push((2, 2, 64), &q).is_none());
        assert_eq!(pending.queries.len(), 1);
        assert!(pending.push((2, 2, 80), &q).is_none());
        assert_eq!(pending.queries.len(), 1);
        for _ in 0..6 {
            assert!(pending.push((2, 2, 80), &q).is_none());
        }
        assert_eq!(pending.push((2, 2, 80), &q).unwrap().len(), 8);
        assert_eq!(pending.bytes, 0);
        let mut huge = q.clone();
        huge.extra
            .insert("padding".into(), serde_json::json!("x".repeat(BYTES)));
        assert!(pending.push((2, 2, 80), &huge).is_none());
        assert!(pending.queries.is_empty());
    }
}
