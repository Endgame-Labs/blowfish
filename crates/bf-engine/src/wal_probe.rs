//! Sharing the "is there a WAL entry after my snapshot" probe among concurrent strong queries.
//!
//! A strong query may answer from the resident snapshot only after it learns that the next WAL
//! entry does not exist. Each query used to issue its own `head`, and on a local-disk store each
//! one is a stat on the tokio blocking pool: a task spawn, a thread wakeup, and (under sixteen
//! busy HTTP reactors) a spin on the pool's one mutex. At thousands of queries a second that
//! hop cost about a tenth of the server's CPU.
//!
//! Queries now share probes without weakening the guarantee. At most one probe per namespace
//! runs at a time. A query that arrives while one is in flight cannot use it (the probe may have
//! looked before the query began, and a write acknowledged in between would be missed), so it
//! joins the next round, which starts only after the running one ends and therefore after every
//! joiner arrived. A query that finds no probe running starts one. So every query is answered
//! by a probe that began after it did, exactly as when it ran its own.
//!
//! Sharing only pays when a probe is cheap. A joiner therefore waits at most [`SHARE_BELOW`]; past
//! that it probes on its own, as it always did (a probe is only one request, and independent
//! probes overlap). A store whose probes keep taking longer than that (an object store over a
//! network) is recognized after [`SLOW_RUN`] probes and every query probes alone from then on.
use bf_storage::{Error, Storage};
use std::{
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

/// A probe that finishes within this is cheap enough to share.
const SHARE_BELOW: Duration = Duration::from_millis(2);
/// How many slow probes in a row make a store count as remote.
const SLOW_RUN: u32 = 8;

/// What one probe found. `Failed` keeps the backend error behind an `Arc`, since a round has
/// many readers.
#[derive(Clone)]
enum Outcome {
    /// The object exists.
    Present,
    /// The object does not exist.
    Missing,
    /// The probe failed.
    Failed(Arc<Error>),
    /// The query that ran the probe went away before it finished; joiners start over.
    Abandoned,
}

struct Round {
    key: String,
    outcome: OnceLock<Outcome>,
}

impl Round {
    fn new(key: &str) -> Arc<Self> {
        Arc::new(Self {
            key: key.to_owned(),
            outcome: OnceLock::new(),
        })
    }
}

#[derive(Default)]
struct State {
    /// The probe in flight.
    running: Option<Arc<Round>>,
    /// The next round: its members arrived while `running` was in flight.
    waiting: Option<Arc<Round>>,
}

/// One namespace's probe sharing.
pub(crate) struct WalProbe {
    state: Mutex<State>,
    /// Woken when a round finishes or the running slot frees.
    changed: Notify,
    /// Whether probes are cheap enough to share: false after [`SLOW_RUN`] slow probes in a row,
    /// true again at the first fast one (one stall, such as a descheduled thread, changes nothing).
    cheap: AtomicBool,
    slow: AtomicU32,
}

impl Default for WalProbe {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            changed: Notify::new(),
            cheap: AtomicBool::new(true),
            slow: AtomicU32::new(0),
        }
    }
}

/// Finishes a round the leader dropped without completing, so nothing waits on it forever.
struct Lead<'a> {
    probe: &'a WalProbe,
    round: Arc<Round>,
}
impl Drop for Lead<'_> {
    fn drop(&mut self) {
        // A completed round already holds its outcome; this does nothing then.
        let _ = self.round.outcome.set(Outcome::Abandoned);
        self.probe.release(&self.round);
    }
}

impl WalProbe {
    /// Whether the object at `key` exists, as `Storage::head` would say it, from a probe that
    /// began after this call did.
    pub(crate) async fn exists(&self, store: &Storage, key: &str) -> Result<bool, Error> {
        if !self.cheap.load(Ordering::Relaxed) {
            return self.run(store, key).await.map(|found| found.is_some());
        }
        loop {
            let step = {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                match (&state.running, &state.waiting) {
                    (None, _) => {
                        let round = Round::new(key);
                        state.running = Some(round.clone());
                        Some((round, true))
                    }
                    (Some(_), Some(next)) if next.key == key => Some((next.clone(), false)),
                    (Some(running), None) if running.key == key => {
                        let round = Round::new(key);
                        state.waiting = Some(round.clone());
                        Some((round, false))
                    }
                    // Another key (the namespace moved to a new WAL entry): probe alone.
                    _ => None,
                }
            };
            let Some((round, mut lead)) = step else {
                return self.run(store, key).await.map(|found| found.is_some());
            };
            // A joiner waits only as long as a cheap probe takes; past that it probes alone, so a
            // slow store (until the gate notices) never makes queries queue behind each other.
            let deadline = (!lead).then(|| tokio::time::Instant::now() + SHARE_BELOW);
            loop {
                if lead {
                    let guard = Lead {
                        probe: self,
                        round: round.clone(),
                    };
                    let outcome = match self.run(store, key).await {
                        Ok(Some(())) => Outcome::Present,
                        Ok(None) => Outcome::Missing,
                        Err(error) => Outcome::Failed(Arc::new(error)),
                    };
                    let _ = round.outcome.set(outcome);
                    drop(guard);
                }
                // Register for the next change before looking, so none is missed.
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                match round.outcome.get() {
                    Some(Outcome::Present) => return Ok(true),
                    Some(Outcome::Missing) => return Ok(false),
                    Some(Outcome::Failed(error)) => return Err(Error::Shared(error.clone())),
                    Some(Outcome::Abandoned) => break,
                    None => {}
                }
                // The round before ours ended: ours runs next, led by whoever gets here first.
                lead = {
                    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    if state.running.is_none()
                        && state
                            .waiting
                            .as_ref()
                            .is_some_and(|next| Arc::ptr_eq(next, &round))
                    {
                        state.running = state.waiting.take();
                        true
                    } else {
                        false
                    }
                };
                if !lead
                    && let Some(deadline) = deadline
                    && tokio::time::timeout_at(deadline, changed.as_mut())
                        .await
                        .is_err()
                {
                    return self.run(store, key).await.map(|found| found.is_some());
                }
            }
        }
    }

    /// Record how long a probe took.
    fn note(&self, elapsed: Duration) {
        if elapsed < SHARE_BELOW {
            self.slow.store(0, Ordering::Relaxed);
            self.cheap.store(true, Ordering::Relaxed);
        } else if self.slow.fetch_add(1, Ordering::Relaxed) + 1 >= SLOW_RUN {
            self.cheap.store(false, Ordering::Relaxed);
        }
    }

    /// One `head`: `Some` when the object exists.
    async fn run(&self, store: &Storage, key: &str) -> Result<Option<()>, Error> {
        let started = Instant::now();
        let result = store.head(key).await;
        self.note(started.elapsed());
        match result {
            Ok(_) => Ok(Some(())),
            Err(Error::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Free the running slot after `round` and wake everyone.
    fn release(&self, round: &Arc<Round>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .running
            .as_ref()
            .is_some_and(|running| Arc::ptr_eq(running, round))
        {
            state.running = None;
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    async fn store() -> Storage {
        Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    #[test]
    fn a_remote_store_stops_sharing_and_a_stall_does_not_stop_it() {
        let probe = WalProbe::default();
        let slow = SHARE_BELOW * 5;
        for _ in 0..SLOW_RUN - 1 {
            probe.note(slow);
        }
        assert!(
            probe.cheap.load(Ordering::Relaxed),
            "a stall is not a slow store"
        );
        probe.note(Duration::from_micros(50));
        for _ in 0..SLOW_RUN - 1 {
            probe.note(slow);
        }
        assert!(
            probe.cheap.load(Ordering::Relaxed),
            "a fast probe resets the run"
        );
        probe.note(slow);
        assert!(
            !probe.cheap.load(Ordering::Relaxed),
            "a run of slow probes means remote"
        );
        probe.note(Duration::from_micros(50));
        assert!(
            probe.cheap.load(Ordering::Relaxed),
            "one fast probe restores sharing"
        );
    }

    #[tokio::test]
    async fn answers_like_head_and_sees_a_new_object() {
        let store = store().await;
        let probe = WalProbe::default();
        assert!(!probe.exists(&store, "wal/1").await.unwrap());
        store.put("wal/1", "x").await.unwrap();
        assert!(probe.exists(&store, "wal/1").await.unwrap());
        // A different key never shares the first one's round.
        assert!(!probe.exists(&store, "wal/2").await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_queries_share_probes_and_every_probe_follows_its_caller() {
        let store = store().await;
        let probe = Arc::new(WalProbe::default());
        let started = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..64 {
            let (store, probe, started) = (store.clone(), probe.clone(), started.clone());
            tasks.push(tokio::spawn(async move {
                for _ in 0..50 {
                    started.fetch_add(1, Ordering::SeqCst);
                    assert!(!probe.exists(&store, "wal/9").await.unwrap());
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(started.load(Ordering::SeqCst), 3200);
        let state = probe.state.lock().unwrap();
        assert!(state.running.is_none() && state.waiting.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_acknowledged_before_the_query_is_never_missed() {
        // The object appears while queries keep probing; a query that starts after the put
        // returned must see it, however many probes are in flight.
        let store = store().await;
        let probe = Arc::new(WalProbe::default());
        let stop = Arc::new(AtomicBool::new(false));
        let noise: Vec<_> = (0..8)
            .map(|_| {
                let (store, probe, stop) = (store.clone(), probe.clone(), stop.clone());
                tokio::spawn(async move {
                    while !stop.load(Ordering::Relaxed) {
                        let _ = probe.exists(&store, "wal/5").await;
                    }
                })
            })
            .collect();
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        store.put("wal/5", "x").await.unwrap();
        for _ in 0..200 {
            assert!(probe.exists(&store, "wal/5").await.unwrap());
        }
        stop.store(true, Ordering::Relaxed);
        for task in noise {
            task.await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_leader_does_not_strand_joiners() {
        let store = store().await;
        let probe = Arc::new(WalProbe::default());
        for _ in 0..200 {
            let mut racers = Vec::new();
            for _ in 0..4 {
                let (store, probe) = (store.clone(), probe.clone());
                racers.push(tokio::spawn(async move {
                    probe.exists(&store, "wal/3").await.unwrap()
                }));
            }
            // Abort some mid-flight; the rest must still finish.
            racers[0].abort();
            racers[1].abort();
            for racer in racers.into_iter().skip(2) {
                assert!(!racer.await.unwrap());
            }
        }
    }
}
