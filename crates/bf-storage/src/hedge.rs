//! Hedged ranged reads. A wave of N parallel reads ends when its slowest request does, so the
//! straggler policy looks at what the wave's own siblings are doing right now:
//!
//! * **Waves.** Reads that start within [`WAVE_WINDOW`] of each other form one wave. Once a wave
//!   of at least [`MIN_WAVE`] reads has seen [`WAVE_DONE`] of its members finish, every read still
//!   outstanding is duplicated (first response wins, the loser is dropped). A wave may add at most
//!   [`WAVE_CAP`] of its size in duplicates, and the store-wide token bucket (`budget` tokens earned
//!   per primary request) bounds the total. No history is needed, so a fresh process hedges too, and
//!   there is no size limit: coalesced slab reads are the usual stragglers.
//! * **Single reads.** A read in a wave smaller than [`MIN_WAVE`] has no siblings to compare with.
//!   It is duplicated once it outlasts the recently observed p90 (a prior until [`MIN_SAMPLES`]
//!   latencies exist), but only up to [`MAX_TIMER_HEDGE_BYTES`], since a duplicate doubles the bytes.
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{
    collections::VecDeque,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

/// Single reads larger than this are never hedged by the timer (a duplicate doubles the bytes).
const MAX_TIMER_HEDGE_BYTES: u64 = 1 << 20;
/// Latency samples kept for the single-read timer.
const WINDOW: usize = 512;
/// Samples needed before the observed p90 replaces the prior.
const MIN_SAMPLES: usize = 24;
/// Timer threshold before enough samples exist.
const PRIOR: Duration = Duration::from_millis(250);
/// The timer never hedges sooner than this.
const FLOOR: Duration = Duration::from_millis(60);
/// Token bucket capacity, which bounds a burst of hedges.
const BURST: f64 = 64.0;
/// Reads starting within this long of a wave's first read join it.
const WAVE_WINDOW: Duration = Duration::from_millis(10);
/// Smallest wave that hedges its own stragglers.
const MIN_WAVE: usize = 4;
/// Share of a wave that must finish before its remaining reads are duplicated.
const WAVE_DONE: f64 = 0.85;
/// Most duplicates one wave may add, as a share of its size (at least one).
const WAVE_CAP: f64 = 0.15;

#[derive(Default)]
struct State {
    samples: VecDeque<f32>,
    tokens: f64,
    wave: Option<Arc<Wave>>,
}

/// Parallel reads that started together.
struct Wave {
    opened: Instant,
    /// Reads still counted: joined, and neither dropped nor failed to start.
    size: AtomicUsize,
    done: AtomicUsize,
    hedged: AtomicUsize,
    triggered: AtomicBool,
    trigger: Notify,
}
impl Wave {
    fn new() -> Self {
        Self {
            opened: Instant::now(),
            size: AtomicUsize::new(0),
            done: AtomicUsize::new(0),
            hedged: AtomicUsize::new(0),
            triggered: AtomicBool::new(false),
            trigger: Notify::new(),
        }
    }
    /// Fire the trigger once the wave is large enough, closed to new members, and mostly done.
    fn check(&self) {
        let size = self.size.load(Ordering::SeqCst);
        let done = self.done.load(Ordering::SeqCst);
        if size >= MIN_WAVE
            && self.opened.elapsed() >= WAVE_WINDOW
            && done as f64 >= WAVE_DONE * size as f64
            && !self.triggered.swap(true, Ordering::SeqCst)
        {
            self.trigger.notify_waiters();
        }
    }
    fn cap(&self) -> usize {
        ((self.size.load(Ordering::SeqCst) as f64 * WAVE_CAP).ceil() as usize).max(1)
    }
}
/// One read's membership in a wave. Dropping it unfinished (a cancelled read) leaves the wave.
struct Member {
    wave: Arc<Wave>,
    finished: bool,
}
impl Member {
    fn finish(&mut self) {
        if !self.finished {
            self.finished = true;
            self.wave.done.fetch_add(1, Ordering::SeqCst);
            self.wave.check();
        }
    }
}
impl Drop for Member {
    fn drop(&mut self) {
        if !self.finished {
            self.wave.size.fetch_sub(1, Ordering::SeqCst);
            self.wave.check();
        }
    }
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
    /// Count a primary read: earn a token and join (or open) the current wave.
    fn join(&self) -> Member {
        let mut s = self.state.lock().unwrap();
        s.tokens = (s.tokens + self.budget).min(BURST);
        let wave = match &s.wave {
            Some(w) if w.opened.elapsed() < WAVE_WINDOW => w.clone(),
            _ => {
                let w = Arc::new(Wave::new());
                s.wave = Some(w.clone());
                w
            }
        };
        wave.size.fetch_add(1, Ordering::SeqCst);
        Member {
            wave,
            finished: false,
        }
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
    /// Take a hedge from the wave's allowance and the store-wide budget.
    fn allow_wave_hedge(&self, wave: &Wave) -> bool {
        if wave.hedged.fetch_add(1, Ordering::SeqCst) >= wave.cap() || !self.spend() {
            wave.hedged.fetch_sub(1, Ordering::SeqCst);
            return false;
        }
        true
    }
    /// A ranged GET, duplicated once if it straggles (see the module docs) and the budget
    /// allows. Latencies of the first answer (from either attempt) feed the single-read timer.
    pub(crate) async fn get_range(
        &self,
        backend: &Arc<dyn ObjectStore>,
        path: &Path,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        if self.budget <= 0.0 {
            return backend.get_range(path, range).await;
        }
        let mut member = self.join();
        let wave = member.wave.clone();
        let start = Instant::now();
        let mut primary = Box::pin(backend.get_range(path, range.clone()));
        let len = range.end - range.start;
        loop {
            let trigger = wave.trigger.notified();
            tokio::pin!(trigger);
            trigger.as_mut().enable();
            if wave.triggered.load(Ordering::SeqCst) {
                if self.allow_wave_hedge(&wave) {
                    break;
                }
                // The wave's allowance is spent: no further hedge for this read.
                let r = primary.await;
                self.record(start.elapsed());
                member.finish();
                return r;
            }
            let small_wave =
                wave.size.load(Ordering::SeqCst) < MIN_WAVE && wave.opened.elapsed() >= WAVE_WINDOW;
            let open = WAVE_WINDOW.saturating_sub(wave.opened.elapsed());
            let wait = if !open.is_zero() {
                // Re-check the wave the moment it closes to new members.
                open
            } else if small_wave && len <= MAX_TIMER_HEDGE_BYTES {
                self.threshold().saturating_sub(start.elapsed())
            } else {
                Duration::from_millis(25)
            }
            .clamp(Duration::from_millis(1), Duration::from_millis(25));
            tokio::select! {
                r = &mut primary => {
                    self.record(start.elapsed());
                    member.finish();
                    return r;
                }
                _ = &mut trigger => {}
                _ = tokio::time::sleep(wait) => {
                    wave.check();
                    if small_wave
                        && len <= MAX_TIMER_HEDGE_BYTES
                        && start.elapsed() >= self.threshold()
                        && self.spend()
                    {
                        break;
                    }
                }
            }
        }
        metrics::counter!("blowfish_object_store_hedges_total", "outcome" => "issued").increment(1);
        let second = Box::pin(backend.get_range(path, range));
        let r = match futures::future::select_ok([primary, second]).await {
            Ok((bytes, _loser)) => Ok(bytes),
            Err(e) => Err(e),
        };
        self.record(start.elapsed());
        member.finish();
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
                if self.delay == Duration::MAX {
                    std::future::pending::<()>().await;
                } else {
                    tokio::time::sleep(self.delay).await;
                }
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
        let (s, path) = store(1, Duration::MAX).await;
        let backend: Arc<dyn ObjectStore> = s.clone();
        let hedger = Hedger::new(1.0);
        // Seed the latency window so the threshold is the floor, not the 250 ms prior.
        for _ in 0..MIN_SAMPLES {
            hedger.record(Duration::from_millis(5));
        }
        // The primary never returns, so only the hedge can satisfy this read. The timeout
        // is a deadlock watchdog, not a latency measurement.
        let bytes = tokio::time::timeout(
            Duration::from_secs(60),
            hedger.get_range(&backend, &path, 0..16),
        )
        .await
        .expect("hedge never fired")
        .unwrap();
        assert_eq!(bytes.len(), 16);
        assert_eq!(s.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn the_budget_caps_extra_requests() {
        let (s, path) = store(3, Duration::from_millis(150)).await;
        let backend: Arc<dyn ObjectStore> = s.clone();
        let hedger = Hedger::new(0.1);
        for _ in 0..MIN_SAMPLES {
            hedger.record(Duration::from_millis(5));
        }
        // The first three of 20 stall past the wave's 85 percent mark. 20 requests earn 2 tokens:
        // at most 2 hedges, although the wave's own allowance is 3.
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
        // Larger than MAX_TIMER_HEDGE_BYTES: allowed to exceed the object, in-memory clamps nothing
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

    #[tokio::test]
    async fn a_fresh_hedger_duplicates_a_waves_stragglers() {
        // Two of 20 simultaneous reads never answer. With no latency history at all, the wave's
        // own 18 finished siblings (90 percent) are what trigger the duplicates.
        let (s, path) = store(2, Duration::MAX).await;
        let backend: Arc<dyn ObjectStore> = s.clone();
        let hedger = Hedger::new(0.15);
        let reads = (0..20).map(|_| hedger.get_range(&backend, &path, 0..16));
        let all = futures::future::join_all(reads);
        for r in tokio::time::timeout(Duration::from_secs(60), all)
            .await
            .expect("wave stragglers were never hedged")
        {
            assert_eq!(r.unwrap().len(), 16);
        }
        assert_eq!(s.calls.load(Ordering::SeqCst), 22);
    }

    #[tokio::test]
    async fn a_wave_hedges_reads_of_any_size_within_its_allowance() {
        let (s, path) = store(3, Duration::MAX).await;
        let big = Path::from("big");
        s.inner
            .put(&big, PutPayload::from(vec![1u8; 3 << 20]))
            .await
            .unwrap();
        let backend: Arc<dyn ObjectStore> = s.clone();
        let hedger = Hedger::new(1.0);
        let reads = (0..20).map(|i| {
            let (hedger, backend) = (&hedger, &backend);
            let (path, big) = (&path, &big);
            async move {
                if i < 3 {
                    hedger.get_range(backend, big, 0..(3 << 20)).await
                } else {
                    hedger.get_range(backend, path, 0..16).await
                }
            }
        });
        let all = futures::future::join_all(reads);
        for r in tokio::time::timeout(Duration::from_secs(60), all)
            .await
            .expect("a 3 MiB straggler was never hedged")
        {
            r.unwrap();
        }
        // Three stragglers fit the allowance (15 percent of 20), so all three were hedged.
        let hedges = s.calls.load(Ordering::SeqCst) - 20;
        assert!(hedges <= 3, "hedges {hedges}");
    }

    #[tokio::test]
    async fn small_waves_and_cancelled_reads_do_not_trigger_wave_hedges() {
        let (s, path) = store(usize::MAX, Duration::from_millis(80)).await;
        let backend: Arc<dyn ObjectStore> = s.clone();
        let hedger = Hedger::new(1.0);
        for _ in 0..MIN_SAMPLES {
            hedger.record(Duration::from_secs(5));
        }
        // Three reads are below MIN_WAVE, and the 5 s timer threshold never fires in 80 ms.
        let reads = (0..3).map(|_| hedger.get_range(&backend, &path, 0..16));
        for r in futures::future::join_all(reads).await {
            r.unwrap();
        }
        assert_eq!(s.calls.load(Ordering::SeqCst), 3);
        // Dropped reads leave their wave and never count as finished.
        let m = hedger.join();
        let wave = m.wave.clone();
        drop(m);
        assert_eq!(wave.size.load(Ordering::SeqCst), 0);
        assert_eq!(wave.done.load(Ordering::SeqCst), 0);
    }
}
