//! Hedged ranged reads: when a GET has not answered by about the recently observed p90, send a
//! duplicate and take whichever answers first. A wave of N parallel reads ends when its slowest
//! request does, so cutting the tail cuts the wave. Extra requests are capped by a token bucket
//! that earns `budget` tokens per primary request (default 10 percent).
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{
    collections::VecDeque,
    ops::Range,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Requests larger than this are never hedged (a duplicate would double the bytes).
const MAX_HEDGE_BYTES: u64 = 1 << 20;
/// Latency samples kept per size class.
const WINDOW: usize = 512;
/// Samples needed before the observed p90 replaces the prior.
const MIN_SAMPLES: usize = 24;
/// Threshold before enough samples exist.
const PRIOR: Duration = Duration::from_millis(250);
/// Never hedge sooner than this.
const FLOOR: Duration = Duration::from_millis(60);
/// Token bucket capacity, which bounds a burst of hedges in one wave.
const BURST: f64 = 64.0;

#[derive(Default)]
struct State {
    samples: VecDeque<f32>,
    tokens: f64,
}
/// Adaptive hedging policy shared by every reader of one store.
pub(crate) struct Hedger {
    budget: f64,
    state: Mutex<State>,
}
impl Hedger {
    /// `budget` is the extra-request fraction; zero disables hedging.
    pub(crate) fn new(budget: f64) -> Self {
        Self {
            budget: budget.clamp(0.0, 1.0),
            state: Mutex::default(),
        }
    }
    fn threshold(&self) -> Duration {
        let s = self.state.lock().unwrap();
        if s.samples.len() < MIN_SAMPLES {
            return PRIOR;
        }
        let mut v: Vec<f32> = s.samples.iter().copied().collect();
        let i = (v.len() * 9 / 10).min(v.len() - 1);
        let (_, p90, _) = v.select_nth_unstable_by(i, |a, b| a.total_cmp(b));
        Duration::from_secs_f32(*p90 / 1e3).max(FLOOR)
    }
    fn record(&self, elapsed: Duration) {
        let mut s = self.state.lock().unwrap();
        if s.samples.len() == WINDOW {
            s.samples.pop_front();
        }
        s.samples.push_back(elapsed.as_secs_f32() * 1e3);
    }
    fn earn(&self) {
        let mut s = self.state.lock().unwrap();
        s.tokens = (s.tokens + self.budget).min(BURST);
    }
    fn spend(&self) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.tokens >= 1.0 {
            s.tokens -= 1.0;
            true
        } else {
            false
        }
    }
    /// A ranged GET, hedged once if the first attempt outlasts the observed p90 and the budget
    /// allows. Latencies of the first answer (from either attempt) feed the threshold.
    pub(crate) async fn get_range(
        &self,
        backend: &Arc<dyn ObjectStore>,
        path: &Path,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        if self.budget <= 0.0 || range.end - range.start > MAX_HEDGE_BYTES {
            return backend.get_range(path, range).await;
        }
        self.earn();
        let start = Instant::now();
        let mut primary = Box::pin(backend.get_range(path, range.clone()));
        loop {
            let wait = self
                .threshold()
                .saturating_sub(start.elapsed())
                .clamp(Duration::from_millis(1), Duration::from_millis(25));
            tokio::select! {
                r = &mut primary => {
                    self.record(start.elapsed());
                    return r;
                }
                _ = tokio::time::sleep(wait) => {}
            }
            if start.elapsed() >= self.threshold() {
                break;
            }
        }
        if !self.spend() {
            let r = primary.await;
            self.record(start.elapsed());
            return r;
        }
        metrics::counter!("blowfish_object_store_hedges_total", "outcome" => "issued").increment(1);
        let second = Box::pin(backend.get_range(path, range));
        let (r, _) = match futures::future::select_ok([primary, second]).await {
            Ok((bytes, rest)) => (Ok(bytes), rest),
            Err(e) => (Err(e), vec![]),
        };
        self.record(start.elapsed());
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// An in-memory store whose first `slow` reads take `delay`, then answer normally.
    #[derive(Debug)]
    struct Stalling {
        inner: InMemory,
        calls: AtomicUsize,
        slow: usize,
        delay: Duration,
    }
    impl std::fmt::Display for Stalling {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("stalling")
        }
    }
    #[async_trait]
    impl ObjectStore for Stalling {
        async fn put_opts(
            &self,
            p: &Path,
            b: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(p, b, o).await
        }
        async fn put_multipart_opts(
            &self,
            p: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(p, o).await
        }
        async fn get_opts(&self, p: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            if self.calls.fetch_add(1, Ordering::SeqCst) < self.slow {
                tokio::time::sleep(self.delay).await;
            }
            self.inner.get_opts(p, o).await
        }
        fn delete_stream(
            &self,
            l: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
    }
    async fn store(slow: usize, delay: Duration) -> (Arc<Stalling>, Path) {
        let inner = InMemory::new();
        let path = Path::from("o");
        inner
            .put(&path, PutPayload::from(vec![7u8; 4096]))
            .await
            .unwrap();
        let s = Arc::new(Stalling {
            inner,
            calls: AtomicUsize::new(0),
            slow,
            delay,
        });
        (s, path)
    }

    #[tokio::test]
    async fn a_stalled_first_attempt_is_beaten_by_the_hedge() {
        let (s, path) = store(1, Duration::from_secs(30)).await;
        let backend: Arc<dyn ObjectStore> = s.clone();
        let hedger = Hedger::new(1.0);
        // Seed the latency window so the threshold is the floor, not the 250 ms prior.
        for _ in 0..MIN_SAMPLES {
            hedger.record(Duration::from_millis(5));
        }
        let start = Instant::now();
        let bytes = hedger.get_range(&backend, &path, 0..16).await.unwrap();
        assert_eq!(bytes.len(), 16);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "hedge never fired"
        );
        assert_eq!(s.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn the_budget_caps_extra_requests() {
        let (s, path) = store(usize::MAX, Duration::from_millis(150)).await;
        let backend: Arc<dyn ObjectStore> = s.clone();
        let hedger = Hedger::new(0.1);
        for _ in 0..MIN_SAMPLES {
            hedger.record(Duration::from_millis(5));
        }
        // Every request stalls past the threshold. 20 requests earn 2 tokens: at most 2 hedges.
        let reads = (0..20).map(|_| hedger.get_range(&backend, &path, 0..16));
        for r in futures::future::join_all(reads).await {
            r.unwrap();
        }
        let hedges = s.calls.load(Ordering::SeqCst) - 20;
        assert!((1..=3).contains(&hedges), "hedges {hedges}");
    }

    #[tokio::test]
    async fn disabled_and_large_reads_are_never_hedged() {
        let (s, path) = store(usize::MAX, Duration::from_millis(100)).await;
        let backend: Arc<dyn ObjectStore> = s.clone();
        Hedger::new(0.0)
            .get_range(&backend, &path, 0..16)
            .await
            .unwrap();
        assert_eq!(s.calls.load(Ordering::SeqCst), 1);
        let hedger = Hedger::new(1.0);
        for _ in 0..MIN_SAMPLES {
            hedger.record(Duration::from_millis(5));
        }
        // Larger than MAX_HEDGE_BYTES: allowed to exceed the object, in-memory clamps nothing
        // here, so use a store with a bigger object.
        let big = Path::from("big");
        s.inner
            .put(&big, PutPayload::from(vec![1u8; 2 << 20]))
            .await
            .unwrap();
        hedger
            .get_range(&backend, &big, 0..(2 << 20))
            .await
            .unwrap();
        assert_eq!(s.calls.load(Ordering::SeqCst), 2);
    }
}
