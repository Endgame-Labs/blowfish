//! Hedged ranged reads. A wave of parallel reads ends when its slowest request does, so the
//! straggler policy looks at what a read's own siblings are doing right now:
//!
//! * **Siblings.** The reads that started within [`SIBLING_WINDOW`] either side of a read are its
//!   siblings (a scan's reads rarely start in lockstep, so the window is wide). Once at least
//!   [`MIN_WAVE`] siblings exist, [`WAVE_DONE`] (70 percent) of them have finished, and the read has outlasted
//!   [`WAVE_DONE`] of the finished siblings' latencies, it is a straggler and is duplicated
//!   (first response wins, the loser is dropped). No history is needed, so a fresh process hedges
//!   too, and there is no size limit: coalesced slab reads are the usual stragglers.
//! * **Budget.** A token bucket earns `budget` tokens per primary request, and each duplicate
//!   spends one, so a wave adds at most about that share of extra reads.
//! * **Single reads.** A read with fewer than [`MIN_WAVE`] siblings has nothing to compare with.
//!   It is duplicated once it outlasts the recently observed p90 (a prior until [`MIN_SAMPLES`]
//!   latencies exist), but only up to [`MAX_TIMER_HEDGE_BYTES`], since a duplicate doubles the bytes.
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{
    collections::VecDeque,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

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
/// Reads that started this close to a read (before or after) are its siblings.
const SIBLING_WINDOW: Duration = Duration::from_millis(50);
/// Fewest siblings (the read included) for sibling-based hedging.
const MIN_WAVE: usize = 4;
/// Share of siblings that must have finished, and the latency quantile a straggler must exceed.
/// Round C24 swept 0.5 to 0.85 with 15 percent of budget: 0.7 was the best of the cheap settings, since
/// 0.85 left most of the budget unspent.
const WAVE_DONE: f64 = 0.7;
/// How often a read re-checks its siblings.
const TICK: Duration = Duration::from_millis(5);
/// Finished reads older than this leave the sibling list.
const RETAIN: Duration = Duration::from_secs(2);

const FLYING: u8 = 0;
const FINISHED: u8 = 1;
const GONE: u8 = 2;

/// One primary read, visible to its siblings.
struct Entry {
    start: Instant,
    state: AtomicU8,
    /// Latency in microseconds, set when the read finishes.
    latency_us: AtomicU32,
}

#[derive(Default)]
struct State {
    samples: VecDeque<f32>,
    tokens: f64,
    recent: VecDeque<Arc<Entry>>,
}

/// A registered read. Dropping it unfinished (a cancelled read) removes it from its siblings.
struct Member(Arc<Entry>);
impl Member {
    fn finish(&self) {
        let us = self.0.start.elapsed().as_micros().min(u32::MAX as u128) as u32;
        self.0.latency_us.store(us, Ordering::SeqCst);
        self.0.state.store(FINISHED, Ordering::SeqCst);
    }
}
impl Drop for Member {
    fn drop(&mut self) {
        let _ = self
            .0
            .state
            .compare_exchange(FLYING, GONE, Ordering::SeqCst, Ordering::SeqCst);
    }
}

/// What the siblings say about one read.
#[derive(PartialEq, Debug)]
enum Verdict {
    /// Too few siblings to compare with.
    Alone,
    Waiting,
    Straggler,
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
    /// Count a primary read: earn a token and become visible to siblings.
    fn join(&self) -> Member {
        let mut s = self.state.lock().unwrap();
        s.tokens = (s.tokens + self.budget).min(BURST);
        while s
            .recent
            .front()
            .is_some_and(|e| e.state.load(Ordering::SeqCst) != FLYING && e.start.elapsed() > RETAIN)
        {
            s.recent.pop_front();
        }
        let entry = Arc::new(Entry {
            start: Instant::now(),
            state: AtomicU8::new(FLYING),
            latency_us: AtomicU32::new(0),
        });
        s.recent.push_back(entry.clone());
        Member(entry)
    }
    /// Compare one in-flight read with the reads that started around it.
    fn verdict(&self, me: &Entry) -> Verdict {
        let s = self.state.lock().unwrap();
        let mut flying = 0usize;
        let mut done = Vec::new();
        for e in s.recent.iter() {
            let apart = if e.start > me.start {
                e.start - me.start
            } else {
                me.start - e.start
            };
            if apart > SIBLING_WINDOW {
                continue;
            }
            match e.state.load(Ordering::SeqCst) {
                FLYING => flying += 1,
                FINISHED => done.push(e.latency_us.load(Ordering::SeqCst)),
                _ => {}
            }
        }
        let total = flying + done.len();
        if total < MIN_WAVE {
            return Verdict::Alone;
        }
        if done.is_empty() || (done.len() as f64) < WAVE_DONE * total as f64 {
            return Verdict::Waiting;
        }
        done.sort_unstable();
        let i = ((done.len() as f64 * WAVE_DONE).ceil() as usize).clamp(1, done.len()) - 1;
        if me.start.elapsed().as_micros() > done[i] as u128 {
            Verdict::Straggler
        } else {
            Verdict::Waiting
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
        let member = self.join();
        let start = member.0.start;
        let mut primary = Box::pin(backend.get_range(path, range.clone()));
        let len = range.end - range.start;
        loop {
            let verdict = self.verdict(&member.0);
            let timer = verdict == Verdict::Alone
                && len <= MAX_TIMER_HEDGE_BYTES
                && start.elapsed() >= self.threshold();
            if (verdict == Verdict::Straggler || timer) && self.spend() {
                break;
            }
            tokio::select! {
                r = &mut primary => {
                    self.record(start.elapsed());
                    member.finish();
                    return r;
                }
                _ = tokio::time::sleep(TICK) => {}
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
        // The first three of 20 stall past the wave's 70 percent mark. 20 requests earn 2 tokens:
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
        // A full budget: a healthy read that is merely slow to be polled may also be duplicated.
        let hedger = Hedger::new(1.0);
        let reads = (0..20).map(|_| hedger.get_range(&backend, &path, 0..16));
        let all = futures::future::join_all(reads);
        for r in tokio::time::timeout(Duration::from_secs(60), all)
            .await
            .expect("wave stragglers were never hedged")
        {
            assert_eq!(r.unwrap().len(), 16);
        }
        assert!(s.calls.load(Ordering::SeqCst) >= 22);
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
        // A dropped read leaves its siblings' count and never counts as finished.
        let m = hedger.join();
        let entry = m.0.clone();
        drop(m);
        assert_eq!(entry.state.load(Ordering::SeqCst), GONE);
    }
}
