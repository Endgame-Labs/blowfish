//! Measure warm serving cost in bounded query blocks, away from exhaustive scan interference.
use super::*;

const QUERIES: usize = 8;
const BYTES: usize = 256 << 10;

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
        struct Size(usize);
        impl std::io::Write for Size {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self.0.saturating_add(bytes.len());
                if self.0 > BYTES {
                    return Err(std::io::ErrorKind::FileTooLarge.into());
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut size = Size(0);
        serde_json::to_writer(&mut size, query).ok()?;
        let bytes = size.0;
        if self.stamp != stamp {
            *self = Self {
                stamp,
                ..Self::default()
            };
        }
        if self.bytes.saturating_add(bytes) > BYTES {
            let ready = std::mem::take(&mut self.queries);
            self.bytes = bytes;
            self.queries.push(query.clone());
            // A single cold query cannot establish warm serving cost. Keep the new
            // request, and conservatively decline admission when no block fits.
            return (ready.len() >= 2).then_some(ready);
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
/// samples together (or a smaller block at the byte boundary): streaming truth between every timed graph trial otherwise measures a
/// cold CPU working set, even when ordinary serving has sustained heat. Each path then runs the block
/// twice and only the second, steady-state pass is timed, for graph and IVF alike.
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
    // Ordinary traffic keeps the IVF path's working set warm, while a graph that is not serving
    // yet is cold after every exhaustive truth scan. Timing each path's first touch compared warm
    // IVF with a cold graph, and the busier the server, the more it favored IVF. Both paths run
    // the block once untimed, so each is costed at the steady state it would serve from.
    let mut graph_cpu = 0.;
    for _ in 0..2 {
        graph_cpu = 0.;
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
    }
    let mut ivf_cpu = 0.;
    for _ in 0..2 {
        ivf_cpu = 0.;
        for request in &batch {
            let Ok((_, seconds)) =
                trial_plan(store, state, request, config, context.ivf_reference()).await
            else {
                return;
            };
            ivf_cpu += seconds;
        }
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
    #[test]
    fn real_dimension_requests_form_blocks_and_byte_pressure_flushes_instead_of_resetting() {
        let q: QueryRequest = serde_json::from_value(
            serde_json::json!({"rank_by":["vector","ANN",vec![0.1f32;768]],"top_k":10}),
        )
        .unwrap();
        let mut pending = Pending::default();
        for _ in 0..7 {
            assert!(pending.push((1, 1, 64), &q).is_none());
        }
        assert_eq!(pending.push((1, 1, 64), &q).unwrap().len(), 8);
        let mut large = q.clone();
        large
            .extra
            .insert("padding".into(), serde_json::json!("x".repeat(BYTES / 3)));
        assert!(pending.push((1, 1, 64), &large).is_none());
        assert!(pending.push((1, 1, 64), &large).is_none());
        let block = pending
            .push((1, 1, 64), &large)
            .expect("byte pressure must produce useful observations");
        assert_eq!(block.len(), 2);
        assert_eq!(pending.queries.len(), 1);
        assert!(pending.bytes <= BYTES);
    }
}
