//! Task-scoped backend accounting, independent of the process-wide metrics recorder.
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Completed backend operations, grouped by query stage and operation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackendMeasurement {
    /// Caller-supplied stage, such as `snapshot` or `search`.
    pub stage: String,
    /// Backend operation (`head`, `get_range`, `list`, and so on).
    pub operation: String,
    /// Completed requests, including not-found responses and errors.
    pub requests: u64,
    /// Successfully transferred physical bytes, including range overfetch.
    pub bytes: u64,
    /// Sum of request durations, including body consumption; parallel requests overlap.
    pub elapsed: Duration,
}
/// Share across all tasks belonging to one query. Spawned tasks must enter a scope explicitly.
#[derive(Debug, Default)]
pub struct BackendStats(Mutex<BTreeMap<(&'static str, &'static str), BackendMeasurement>>);
tokio::task_local! {
    static CURRENT: (Arc<BackendStats>, &'static str);
}
impl BackendStats {
    /// Attribute operations in `future` to this accumulator and stage.
    pub async fn scope<T>(
        self: &Arc<Self>,
        stage: &'static str,
        future: impl Future<Output = T>,
    ) -> T {
        CURRENT.scope((self.clone(), stage), future).await
    }
    /// Completed measurements in stable stage/operation order.
    pub fn snapshot(&self) -> Vec<BackendMeasurement> {
        self.0
            .lock()
            .expect("backend diagnostics lock")
            .values()
            .cloned()
            .collect()
    }
}
pub(crate) fn record(operation: &'static str, bytes: u64, elapsed: Duration) {
    let _ = CURRENT.try_with(|(stats, stage)| {
        let mut entries = stats.0.lock().expect("backend diagnostics lock");
        let entry = entries
            .entry((stage, operation))
            .or_insert_with(|| BackendMeasurement {
                stage: (*stage).into(),
                operation: operation.into(),
                ..Default::default()
            });
        entry.requests += 1;
        entry.bytes += bytes;
        entry.elapsed += elapsed;
    });
}
