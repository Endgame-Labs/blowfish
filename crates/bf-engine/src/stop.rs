//! Shutdown of background work.
//!
//! An engine's background jobs (the indexer loop with its flushes, compaction, and GC, the
//! vector, document, and text upgrades, recluster builds, and recall samples) run under one
//! [`Stop`], shared with the engine's scoped engines. Once [`Stop::begin`] is called, every job
//! is dropped at its next await point, and the CPU-bound loops of a recluster build check
//! [`Stop::stopping`] between rows. Dropping a job is crash-safe in the same way as a killed
//! process: nothing is published until a fenced manifest write, a build's record stays (so GC
//! keeps its objects until the record is stale), and the next start resumes the work.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

/// The shutdown signal, and a count of the background jobs still running.
#[derive(Debug)]
pub(crate) struct Stop {
    stopping: watch::Sender<bool>,
    running: watch::Sender<usize>,
}

impl Default for Stop {
    fn default() -> Self {
        Self {
            stopping: watch::Sender::new(false),
            running: watch::Sender::new(0),
        }
    }
}

/// A running background job; counted until dropped.
pub(crate) struct Running(Arc<Stop>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.running.send_modify(|n| *n -= 1);
    }
}

impl Stop {
    /// Whether shutdown has begun.
    pub(crate) fn stopping(&self) -> bool {
        *self.stopping.borrow()
    }

    /// Resolves once shutdown has begun.
    pub(crate) async fn stopped(&self) {
        let mut rx = self.stopping.subscribe();
        let _ = rx.wait_for(|s| *s).await;
    }

    /// Count a background job until the returned guard is dropped.
    pub(crate) fn track(self: &Arc<Self>) -> Running {
        self.running.send_modify(|n| *n += 1);
        Running(self.clone())
    }

    /// Run `job` as a background job: `None` if shutdown began first, or began while it ran (it
    /// is then dropped at its next await point).
    pub(crate) async fn run<F: Future>(self: &Arc<Self>, job: F) -> Option<F::Output> {
        let _running = self.track();
        if self.stopping() {
            return None;
        }
        tokio::select! {
            biased;
            () = self.stopped() => None,
            out = job => Some(out),
        }
    }

    /// Begin shutdown: every background job stops at its next await point.
    pub(crate) fn begin(&self) {
        self.stopping.send_replace(true);
    }

    /// Wait up to `grace` for the running jobs to stop. Whether they all did.
    pub(crate) async fn drained(&self, grace: Duration) -> bool {
        let mut rx = self.running.subscribe();
        tokio::time::timeout(grace, rx.wait_for(|n| *n == 0))
            .await
            .is_ok()
    }

    /// Background jobs running now.
    pub(crate) fn running(&self) -> usize {
        *self.running.borrow()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_job_is_dropped_at_its_next_await_once_shutdown_begins() {
        let stop = Arc::new(Stop::default());
        let job = tokio::spawn({
            let stop = stop.clone();
            async move { stop.run(std::future::pending::<()>()).await }
        });
        while stop.running() == 0 {
            tokio::task::yield_now().await;
        }
        assert!(!stop.drained(Duration::from_millis(20)).await);
        stop.begin();
        assert!(stop.drained(Duration::from_secs(5)).await);
        assert_eq!(job.await.unwrap(), None);
        // A job started after shutdown never runs.
        assert_eq!(stop.run(async { 1 }).await, None);
    }
}
