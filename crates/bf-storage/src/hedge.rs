//! Hedged ranged reads. A wave of parallel reads ends when its slowest request does, so the
//! straggler policy looks at what a read's own siblings are doing right now:
//!
//! * **Siblings.** The reads that started within [`SIBLING_WINDOW`] either side of a read are its
//!   siblings (a scan's reads rarely start in lockstep, so the window is wide). Starts are bucketed into
//!   [`SLOT`]-wide slots in a fixed ring, each holding counts and a log-scale latency histogram, so a
//!   check reads a few dozen atomics (no lock, no scan of reads) and memory is fixed whatever the
//!   arrival rate or a stuck read does. A slot is reused once it is older than the ring. Once at least
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
        atomic::{AtomicU32, AtomicU64, Ordering},
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
/// The timer's p90 is recomputed after this many new samples.
const P90_EVERY: usize = 16;
/// Token bucket capacity, which bounds a burst of hedges.
const BURST: f64 = 64.0;
/// Reads that started this close to a read (before or after) are its siblings, to within one slot.
const SIBLING_WINDOW: Duration = Duration::from_millis(50);
/// Width of one start-time slot, in microseconds.
const SLOT_US: u64 = 10_000;
/// Slots on each side of a read's own slot that count as its siblings.
const SPAN: u64 = SIBLING_WINDOW.as_micros() as u64 / SLOT_US;
/// Slots in the ring: 2.56 s of history, longer than any window needs.
const SLOTS: usize = 256;
/// Fewest siblings (the read included) for sibling-based hedging.
const MIN_WAVE: usize = 4;
/// Share of siblings that must have finished, and the latency quantile a straggler must exceed.
/// Round C24 swept 0.5 to 0.85 with 15 percent of budget: 0.7 was the best of the cheap settings, since
/// 0.85 left most of the budget unspent.
const WAVE_DONE: f64 = 0.7;
/// Shortest and longest wait before a read re-checks its siblings. Each wake re-polls the read, so
/// the wait adapts: it is as long as the verdict allows, to keep polling off the CPU.
const TICK_MIN: Duration = Duration::from_millis(5);
const TICK_MAX: Duration = Duration::from_millis(20);
/// Latency histogram: `BUCKETS` log-scale buckets, `PER_OCTAVE` per doubling, from `MIN_US` up.
const BUCKETS: usize = 128;
const PER_OCTAVE: f64 = 8.0;
const MIN_US: f64 = 256.0;

/// Histogram bucket of a latency in microseconds.
fn bucket(us: u64) -> usize {
    if (us as f64) < MIN_US {
        return 0;
    }
    (((us as f64 / MIN_US).log2() * PER_OCTAVE) as usize).min(BUCKETS - 1)
}
/// Upper edge of a bucket, in microseconds: a latency above it beats every read in the bucket.
fn bucket_top(b: usize) -> f64 {
    MIN_US * ((b + 1) as f64 / PER_OCTAVE).exp2()
}

/// Reads that started in one `SLOT_US` slot.
struct Slot {
    /// Absolute slot number (`start_us / SLOT_US` plus one; zero means never used).
    tag: AtomicU64,
    started: AtomicU32,
    finished: AtomicU32,
    hist: [AtomicU32; BUCKETS],
}
impl Slot {
    fn new() -> Self {
        Self {
            tag: AtomicU64::new(0),
            started: AtomicU32::new(0),
            finished: AtomicU32::new(0),
            hist: std::array::from_fn(|_| AtomicU32::new(0)),
        }
    }
}

#[derive(Default)]
struct State {
    samples: VecDeque<f32>,
    since_p90: usize,
    tokens: f64,
}

/// A registered read. Dropping it unfinished (a cancelled read) removes it from its siblings.
struct Member<'a> {
    hedger: &'a Hedger,
    /// Start time in microseconds since the hedger was made.
    start_us: u64,
    done: bool,
}
impl Member<'_> {
    fn finish(&mut self) {
        self.done = true;
        self.hedger.finish_at(self.start_us, self.hedger.now_us());
    }
}
impl Drop for Member<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.hedger.cancel_at(self.start_us);
        }
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

/// Reads up to this size are judged against, and paid for by, other small reads only (Round C34):
/// a rerank wave of 16 KB reads must not be compared with the scan's multi-megabyte GETs, and the
/// scan's duplicates must not spend the budget the wave needs.
const SMALL_READ_BYTES: u64 = 64 << 10;
/// Extra-request share for small reads: a duplicate costs a request, not bytes.
const SMALL_BUDGET: f64 = 0.5;

/// Hedging for one store: one [`Hedger`] for small reads and one for the rest.
pub(crate) struct Hedges {
    small: Hedger,
    large: Hedger,
}
impl Hedges {
    /// `budget` is the extra-request fraction of large reads; zero disables hedging.
    pub(crate) fn new(budget: f64) -> Self {
        let small = if budget > 0.0 {
            std::env::var("BF_HEDGE_SMALL_BUDGET")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(SMALL_BUDGET)
        } else {
            0.0
        };
        Self {
            small: Hedger::new(small),
            large: Hedger::new(budget),
        }
    }
    /// A ranged GET, hedged by the lane of its size.
    pub(crate) async fn get_range(
        &self,
        backend: &Arc<dyn ObjectStore>,
        path: &Path,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        let lane = if range.end - range.start <= SMALL_READ_BYTES {
            &self.small
        } else {
            &self.large
        };
        lane.get_range(backend, path, range).await
    }
}

/// Adaptive hedging policy shared by every reader of one store.
pub(crate) struct Hedger {
    budget: f64,
    epoch: Instant,
    state: Mutex<State>,
    /// The single-read timer's threshold in microseconds, zero until enough samples exist.
    p90_us: AtomicU64,
    ring: Box<[Slot]>,
    /// Serializes a slot's reuse; reads of the ring never take it.
    rotate: Mutex<()>,
}
impl Hedger {
    /// `budget` is the extra-request fraction; zero disables hedging.
    pub(crate) fn new(budget: f64) -> Self {
        Self {
            budget: budget.clamp(0.0, 1.0),
            epoch: Instant::now(),
            state: Mutex::default(),
            p90_us: AtomicU64::new(0),
            ring: (0..SLOTS).map(|_| Slot::new()).collect(),
            rotate: Mutex::new(()),
        }
    }
    fn now_us(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64
    }
    fn threshold(&self) -> Duration {
        match self.p90_us.load(Ordering::Relaxed) {
            0 => PRIOR,
            us => Duration::from_micros(us).max(FLOOR),
        }
    }
    fn record(&self, elapsed: Duration) {
        let mut s = self.state.lock().unwrap();
        if s.samples.len() == WINDOW {
            s.samples.pop_front();
        }
        s.samples.push_back(elapsed.as_secs_f32() * 1e3);
        s.since_p90 += 1;
        if s.samples.len() >= MIN_SAMPLES
            && (s.since_p90 >= P90_EVERY || s.samples.len() == MIN_SAMPLES)
        {
            s.since_p90 = 0;
            let mut v: Vec<f32> = s.samples.iter().copied().collect();
            let i = (v.len() * 9 / 10).min(v.len() - 1);
            let (_, p90, _) = v.select_nth_unstable_by(i, |a, b| a.total_cmp(b));
            self.p90_us.store((*p90 * 1e3) as u64, Ordering::Relaxed);
        }
    }
    /// The ring slot for absolute slot `abs`, if it still holds that slot.
    fn slot(&self, abs: u64) -> Option<&Slot> {
        let s = &self.ring[(abs % SLOTS as u64) as usize];
        (s.tag.load(Ordering::Acquire) == abs + 1).then_some(s)
    }
    /// Count a primary read that starts at `start_us`: earn a token and become visible to siblings.
    fn join_at(&self, start_us: u64) {
        {
            let mut st = self.state.lock().unwrap();
            st.tokens = (st.tokens + self.budget).min(BURST);
        }
        let abs = start_us / SLOT_US;
        let s = &self.ring[(abs % SLOTS as u64) as usize];
        if s.tag.load(Ordering::Acquire) != abs + 1 {
            let _g = self.rotate.lock().unwrap();
            if s.tag.load(Ordering::Acquire) < abs + 1 {
                // Reuse the slot of a read that began a ring ago. Its reads, if any are still in
                // flight, drop out of every count: that is what bounds memory.
                s.started.store(0, Ordering::Relaxed);
                s.finished.store(0, Ordering::Relaxed);
                for h in &s.hist {
                    h.store(0, Ordering::Relaxed);
                }
                s.tag.store(abs + 1, Ordering::Release);
            }
        }
        s.started.fetch_add(1, Ordering::AcqRel);
    }
    fn join(&self) -> Member<'_> {
        let start_us = self.now_us();
        self.join_at(start_us);
        Member {
            hedger: self,
            start_us,
            done: false,
        }
    }
    /// A read that started at `start_us` finished at `now_us`.
    fn finish_at(&self, start_us: u64, now_us: u64) {
        if let Some(s) = self.slot(start_us / SLOT_US) {
            s.hist[bucket(now_us.saturating_sub(start_us))].fetch_add(1, Ordering::AcqRel);
            s.finished.fetch_add(1, Ordering::AcqRel);
        }
    }
    /// A read that started at `start_us` was dropped unfinished.
    fn cancel_at(&self, start_us: u64) {
        if let Some(s) = self.slot(start_us / SLOT_US) {
            let _ = s
                .started
                .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
        }
    }
    /// Compare an in-flight read that started at `start_us` with the reads that started around it.
    #[cfg(test)]
    fn verdict_at(&self, start_us: u64, now_us: u64) -> Verdict {
        self.check_at(start_us, now_us).0
    }
    /// The verdict, and how long to wait before asking again: until the quantile the read must
    /// outlast when that is known, else a coarse tick for siblings to arrive or finish.
    fn check_at(&self, start_us: u64, now_us: u64) -> (Verdict, Duration) {
        let abs = start_us / SLOT_US;
        let slots: Vec<&Slot> = (abs.saturating_sub(SPAN)..=abs + SPAN)
            .filter_map(|a| self.slot(a))
            .collect();
        let total: u64 = slots
            .iter()
            .map(|s| s.started.load(Ordering::Acquire) as u64)
            .sum();
        let done: u64 = slots
            .iter()
            .map(|s| s.finished.load(Ordering::Acquire) as u64)
            .sum();
        if (total as usize) < MIN_WAVE {
            return (Verdict::Alone, TICK_MAX);
        }
        if done == 0 || (done as f64) < WAVE_DONE * total as f64 {
            return (Verdict::Waiting, TICK_MIN * 2);
        }
        // The WAVE_DONE quantile of the finished latencies, to the histogram's resolution.
        let want = ((done as f64 * WAVE_DONE).ceil() as u64).clamp(1, done);
        let mut seen = 0u64;
        for b in 0..BUCKETS {
            seen += slots
                .iter()
                .map(|s| s.hist[b].load(Ordering::Acquire) as u64)
                .sum::<u64>();
            if seen >= want {
                let top = bucket_top(b);
                let elapsed = now_us.saturating_sub(start_us) as f64;
                return if elapsed > top {
                    (Verdict::Straggler, TICK_MIN)
                } else {
                    let left = Duration::from_micros((top - elapsed) as u64 + 1);
                    (Verdict::Waiting, left.clamp(TICK_MIN, TICK_MAX))
                };
            }
        }
        // Counts raced a slot's reuse: nothing to compare with yet.
        (Verdict::Waiting, TICK_MIN)
    }
    /// (siblings, finished siblings, the quantile a straggler must exceed in microseconds) for a
    /// read that started at `start_us`: diagnostics for `BF_TRACE_READS`.
    fn snapshot(&self, start_us: u64) -> (u64, u64, u64) {
        let abs = start_us / SLOT_US;
        let slots: Vec<&Slot> = (abs.saturating_sub(SPAN)..=abs + SPAN)
            .filter_map(|a| self.slot(a))
            .collect();
        let total: u64 = slots
            .iter()
            .map(|s| s.started.load(Ordering::Acquire) as u64)
            .sum();
        let done: u64 = slots
            .iter()
            .map(|s| s.finished.load(Ordering::Acquire) as u64)
            .sum();
        let want = ((done as f64 * WAVE_DONE).ceil() as u64).clamp(1, done.max(1));
        let mut seen = 0u64;
        let mut top = 0u64;
        for b in 0..BUCKETS {
            seen += slots
                .iter()
                .map(|s| s.hist[b].load(Ordering::Acquire) as u64)
                .sum::<u64>();
            if seen >= want && done > 0 {
                top = bucket_top(b) as u64;
                break;
            }
        }
        (total, done, top)
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
        let mut member = self.join();
        let start = Instant::now();
        let trace = std::env::var_os("BF_TRACE_READS").is_some();
        let mut hedged_at: Option<(u128, (u64, u64, u64), bool)> = None;
        let mut primary = Box::pin(backend.get_range(path, range.clone()));
        let len = range.end - range.start;
        loop {
            let (verdict, mut wait) = self.check_at(member.start_us, self.now_us());
            let mut timer = false;
            if verdict == Verdict::Alone && len <= MAX_TIMER_HEDGE_BYTES {
                let left = self.threshold().saturating_sub(start.elapsed());
                timer = left.is_zero();
                wait = left.clamp(TICK_MIN, TICK_MAX);
            }
            if (verdict == Verdict::Straggler || timer) && self.spend() {
                if trace {
                    hedged_at = Some((
                        start.elapsed().as_millis(),
                        self.snapshot(member.start_us),
                        timer,
                    ));
                }
                break;
            }
            tokio::select! {
                r = &mut primary => {
                    self.record(start.elapsed());
                    member.finish();
                    if trace {
                        trace_line(path, &range, start, None, self.snapshot(member.start_us));
                    }
                    return r;
                }
                _ = tokio::time::sleep(wait) => {}
            }
        }
        metrics::counter!("blowfish_object_store_hedges_total", "outcome" => "issued").increment(1);
        let second = Box::pin(backend.get_range(path, range.clone()));
        let r = match futures::future::select_ok([primary, second]).await {
            Ok((bytes, _loser)) => Ok(bytes),
            Err(e) => Err(e),
        };
        self.record(start.elapsed());
        member.finish();
        if trace {
            trace_line(
                path,
                &range,
                start,
                hedged_at,
                self.snapshot(member.start_us),
            );
        }
        r
    }
}

/// `TRACE_HEDGE path from len end_ms elapsed_ms hedge_after_ms(-1 none) siblings done p70_us timer`
fn trace_line(
    path: &Path,
    range: &Range<u64>,
    start: Instant,
    hedged: Option<(u128, (u64, u64, u64), bool)>,
    now: (u64, u64, u64),
) {
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let (after, (t, d, q), timer) =
        hedged.map_or((-1i128, now, false), |(a, s, tm)| (a as i128, s, tm));
    eprintln!(
        "TRACE_HEDGE {} {} {} {} {} {} {} {} {} {}",
        path,
        range.start,
        range.end - range.start,
        end,
        start.elapsed().as_millis(),
        after,
        t,
        d,
        q,
        timer as u8
    );
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
    use std::sync::atomic::AtomicUsize;

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
        let start_us = m.start_us;
        let abs = start_us / SLOT_US;
        assert_eq!(hedger.slot(abs).unwrap().started.load(Ordering::SeqCst), 1);
        drop(m);
        assert_eq!(hedger.slot(abs).unwrap().started.load(Ordering::SeqCst), 0);
    }

    /// The pre-ring rule, on an explicit list of (start, latency) reads: siblings are the reads that
    /// started within 50 ms, and a read straggles once 70 percent finished and it outlasts their
    /// 70th-percentile latency. `None` latency means still in flight.
    fn old_rule(reads: &[(u64, Option<u64>)], me: u64, elapsed: u64) -> Verdict {
        let sib: Vec<_> = reads
            .iter()
            .filter(|(s, _)| s.abs_diff(me) <= 50_000)
            .collect();
        let mut done: Vec<u64> = sib.iter().filter_map(|(_, l)| *l).collect();
        if sib.len() < MIN_WAVE {
            return Verdict::Alone;
        }
        if done.is_empty() || (done.len() as f64) < WAVE_DONE * sib.len() as f64 {
            return Verdict::Waiting;
        }
        done.sort_unstable();
        let i = ((done.len() as f64 * WAVE_DONE).ceil() as usize).clamp(1, done.len()) - 1;
        if elapsed > done[i] {
            Verdict::Straggler
        } else {
            Verdict::Waiting
        }
    }

    #[test]
    fn the_ring_matches_the_old_rule_on_synthetic_waves() {
        // A tiny deterministic generator keeps the test reproducible.
        let mut x = 0x2545F4914F6CDD1Du64;
        let mut rnd = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        for _ in 0..200 {
            let hedger = Hedger::new(1.0);
            // Starts inside one 30 ms span that sits in the middle of the window of every member, so
            // slot rounding cannot change who counts as a sibling.
            let base = 1_000_000u64;
            let n = 4 + rnd(40) as usize;
            let mut reads = Vec::new();
            for _ in 0..n {
                let start = base + rnd(30_000);
                let lat = 5_000 + rnd(200_000);
                let finished = rnd(10) < 8;
                hedger.join_at(start);
                if finished {
                    hedger.finish_at(start, start + lat);
                }
                reads.push((start, finished.then_some(lat)));
            }
            let me = base + 15_000;
            for elapsed in [1_000u64, 20_000, 60_000, 120_000, 190_000, 250_000] {
                let new = hedger.verdict_at(me, me + elapsed);
                let old = old_rule(&reads, me, elapsed);
                match new {
                    // Bucket edges round the quantile up (by under 10 percent), so the ring is never
                    // sooner than the exact rule, and never later than one bucket.
                    Verdict::Straggler => assert_eq!(old, Verdict::Straggler),
                    Verdict::Alone => assert_eq!(old, Verdict::Alone),
                    Verdict::Waiting => {
                        if old == Verdict::Straggler {
                            let late = old_rule(&reads, me, (elapsed as f64 / 1.1) as u64);
                            assert_ne!(late, Verdict::Straggler, "ring waited past one bucket");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn a_stuck_read_does_not_grow_the_ring_and_reads_expire() {
        let hedger = Hedger::new(1.0);
        // One read never finishes, then 20 seconds of arrivals, 50 reads per slot.
        hedger.join_at(0);
        for slot in 1..2_000u64 {
            for k in 0..50 {
                let s = slot * SLOT_US + k;
                hedger.join_at(s);
                hedger.finish_at(s, s + 3_000);
            }
        }
        // The ring is fixed-size, the stuck read's slot was reused (its count did not survive), and
        // a read that started long ago sees nothing.
        assert_eq!(hedger.ring.len(), SLOTS);
        assert!(hedger.slot(0).is_none());
        assert_eq!(hedger.verdict_at(0, 100_000_000), Verdict::Alone);
        // Current reads still see their siblings.
        let t = 1_999 * SLOT_US;
        assert_ne!(hedger.verdict_at(t, t + 1), Verdict::Alone);
    }

    #[test]
    fn a_late_straggler_is_called_once_it_outlasts_the_wave() {
        let hedger = Hedger::new(1.0);
        for i in 0..10u64 {
            hedger.join_at(i * 1_000);
        }
        for i in 0..9u64 {
            hedger.finish_at(i * 1_000, i * 1_000 + 40_000);
        }
        // Nine of ten finished in about 40 ms; the tenth is not a straggler at 30 ms but is at 60 ms.
        assert_eq!(hedger.verdict_at(9_000, 9_000 + 30_000), Verdict::Waiting);
        assert_eq!(hedger.verdict_at(9_000, 9_000 + 60_000), Verdict::Straggler);
    }
}
