//! Engine-owned, disposable warm graphs. Stored IVF remains the fallback and source of truth.
use crate::{Engine, NamespaceState, lineage::Placement, read_cache, segment};
use bf_storage::Storage;
use bf_types::{ApiError, DistanceMetric, Document, Id, QueryRequest, RankBy};
use bf_vector::{
    CosineQuery, Index, RoaringBitmap, VectorType,
    warm_graph::{HotCancellation, HotGraph, HotGraphBuilder, HotScratch},
};
use bf_wal::{SlabSet, Wal};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
const DWELL: Duration = Duration::from_secs(600);
const IDLE: Duration = Duration::from_secs(1800);
const RETRY: Duration = Duration::from_secs(1800);
/// Exact-rerank shortlist for the largest request the original graph path served.
const RERANK: usize = 30;
/// The largest `k + offset` the graph serves; larger requests use the IVF tier.
const MAX_K: usize = 100;
/// With CPU sampling on, one query in this many reads the thread CPU clock.
const SAMPLE_EVERY: u64 = 16;

/// Beam width and exact-rerank shortlist for a request of `k` rows, given the calibrated beam.
///
/// Up to [`RERANK`] rows nothing changes. Beyond that the shortlist is 1.5 x `k` and the beam
/// 2.56 x `k` (never below the calibrated beam). Round G8 measured on NQ at `k` 100: beam 256 with
/// shortlist 150 matches the IVF tier's recall@100 (0.9533 against 0.9523) at about 1/17 of its cost,
/// and a shortlist beyond 1.5 x `k` adds no recall because the beam bounds the candidates.
fn plan(k: usize, calibrated: usize) -> (usize, usize) {
    if k <= RERANK {
        return (calibrated, RERANK);
    }
    (calibrated.max((k * 128).div_ceil(50)), (k * 3).div_ceil(2))
}
pub(crate) mod cost;
mod effort;
#[cfg(all(test, target_os = "macos"))]
mod nav_probe;
static RESERVED: AtomicU64 = AtomicU64::new(0);
static FOREGROUND: AtomicU64 = AtomicU64::new(0);
static CACHES: std::sync::LazyLock<Mutex<Vec<std::sync::Weak<Cache>>>> =
    std::sync::LazyLock::new(Default::default);
struct Foreground(u64);
impl Foreground {
    fn new(bytes: u64) -> Self {
        let bytes = bytes.min(bf_alloc::memory_limit_bytes());
        FOREGROUND.fetch_add(bytes, Ordering::AcqRel);
        Self(bytes)
    }
}
impl Drop for Foreground {
    fn drop(&mut self) {
        FOREGROUND.fetch_sub(self.0, Ordering::AcqRel);
    }
}
fn overcommitted() -> bool {
    RESERVED
        .load(Ordering::Acquire)
        .saturating_add(FOREGROUND.load(Ordering::Acquire))
        > bf_alloc::memory_limit_bytes().saturating_mul(4) / 5
}

/// Unit tests that need the process-global build slot, automatic-recluster flags, or the memory
/// reservations to themselves hold this exclusively; every other recluster in a test holds it
/// shared (see `Engine::recluster`), so no other test's build can starve them of admission.
#[cfg(test)]
pub(crate) static ADMISSION_GATE: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());
static BUILDING: AtomicBool = AtomicBool::new(false);
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct Key {
    namespace: String,
    generation: chrono::DateTime<chrono::Utc>,
    attribute: String,
    tier: String,
}
impl Key {
    fn new(ns: &str, state: &NamespaceState, attribute: &str, set: &SlabSet) -> Self {
        Self {
            namespace: ns.into(),
            generation: state.created_at,
            attribute: attribute.into(),
            tier: set.dir.key.clone(),
        }
    }
}
struct Reservation(u64);
impl Reservation {
    fn new(bytes: u64, cap: u64) -> Option<Self> {
        RESERVED
            .try_update(Ordering::AcqRel, Ordering::Relaxed, |held| {
                held.checked_add(bytes).filter(|n| *n <= cap)
            })
            .ok()
            .map(|_| Self(bytes))
    }
    fn shrink(&mut self, bytes: u64) {
        if bytes < self.0 {
            RESERVED.fetch_sub(self.0 - bytes, Ordering::AcqRel);
            self.0 = bytes;
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        RESERVED.fetch_sub(self.0, Ordering::AcqRel);
    }
}
pub(crate) fn reserved_bytes() -> u64 {
    RESERVED.load(Ordering::Acquire)
}
struct BuildSlot;
impl BuildSlot {
    fn take() -> Option<Self> {
        BUILDING
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .ok()
            .map(|_| Self)
    }
}
impl Drop for BuildSlot {
    fn drop(&mut self) {
        BUILDING.store(false, Ordering::Release);
    }
}
struct Calibration {
    quality: effort::Effort,
    trial_seconds: f64,
    /// Graph CPU as a fraction of warm IVF CPU over the same query blocks, at the trial effort.
    trial_ratio: f64,
    usable: bool,
    /// The same fraction for the serving effort (0 until one is accepted). A ratio, not seconds:
    /// graph CPU timed on ordinary queries (any `top_k`, under load) compared with IVF CPU timed on
    /// calibration blocks (the sampled `top_k`, one query at a time) made a `top_k` 100 workload look
    /// like a loss against a `top_k` 10 baseline, and the graph stopped serving.
    graph_ratio: f64,
    build_seconds: f64,
}
impl Calibration {
    /// CPU seconds a graph query saves against IVF, given the warm IVF cost.
    fn saving(&self, ivf_seconds: f64) -> f64 {
        (ivf_seconds * (1. - self.graph_ratio)).max(0.)
    }
}
enum Locations {
    Dense(Vec<[u32; 3]>),
    Sparse(Vec<(u32, [u32; 3])>),
}
impl Locations {
    fn new(mut rows: Vec<(u32, [u32; 3])>) -> Self {
        rows.sort_unstable_by_key(|row| row.0);
        if rows.iter().enumerate().all(|(i, row)| i == row.0 as usize) {
            // Dense immutable tiers already encode the row ID in the array position. Drop
            // its redundant copy, including capacity retained by an in-place collect.
            let mut dense = rows.into_iter().map(|(_, at)| at).collect::<Vec<_>>();
            dense.shrink_to_fit();
            Self::Dense(dense)
        } else {
            Self::Sparse(rows)
        }
    }
    fn get(&self, row: u32) -> Option<[u32; 3]> {
        match self {
            Self::Dense(rows) => rows.get(row as usize).copied(),
            Self::Sparse(rows) => rows
                .binary_search_by_key(&row, |entry| entry.0)
                .ok()
                .map(|i| rows[i].1),
        }
    }
    fn resident_bytes(&self) -> u64 {
        match self {
            Self::Dense(rows) => (rows.capacity() * 12) as u64,
            Self::Sparse(rows) => (rows.capacity() * 16) as u64,
        }
    }
}
struct Entry {
    graph: HotGraph,
    sources: Vec<Index<segment::ObjectReader>>,
    locations: Locations,
    scratch: Mutex<Vec<HotScratch>>,
    mask_live: Mutex<Option<(std::sync::Weak<RoaringBitmap>, usize)>>,
    slots: tokio::sync::Semaphore,
    calibration: Mutex<Calibration>,
    _reservation: Reservation,
    installed_at: Instant,
    build_wall_seconds: f64,
    measured_cpu_seconds: f64,
    build_reservation_bytes: u64,
    phase_cpu: [AtomicU64; 3],
    searches: AtomicU64,
    pending_cost: Mutex<cost::Pending>,
    /// Round G12 item 5: the unsigned ID of every tier row (`u64::MAX` where the row has none
    /// now), built once in the background by the first ID-only query. Absent when any ID is a
    /// string, and until built, queries hydrate IDs from the documents as before.
    ids: std::sync::OnceLock<Vec<u64>>,
    ids_started: std::sync::atomic::AtomicBool,
}
struct Heat {
    first: Instant,
    last: Instant,
    rate: f64,
    ivf_seconds: f64,
    retry: Instant,
    building: Option<HotCancellation>,
    ready: Option<Arc<Entry>>,
}
impl Heat {
    fn new(now: Instant) -> Self {
        Self {
            first: now,
            last: now,
            rate: 0.,
            ivf_seconds: 0.,
            retry: now,
            building: None,
            ready: None,
        }
    }
    fn rate_at(&self, now: Instant) -> f64 {
        self.rate * (-now.saturating_duration_since(self.last).as_secs_f64() / 900.).exp()
    }
    fn observe(&mut self, now: Instant, seconds: f64) {
        // Concurrent completions can reach the cache lock out of timestamp order.
        let now = now.max(self.last);
        let dt = now.duration_since(self.last).as_secs_f64();
        if dt > 60. {
            self.first = now;
        }
        self.rate = self.rate_at(now) + 1. / 900.;
        self.ivf_seconds = if seconds <= 0. {
            // A query whose CPU clock was not read (sampled out) carries no cost sample.
            self.ivf_seconds
        } else if self.ivf_seconds == 0. {
            seconds
        } else {
            self.ivf_seconds * 0.99 + seconds * 0.01
        };
        self.last = now;
    }
}
/// Engine/backend ownership is implicit in this private cache. Scoped engines get separate caches.
pub(crate) struct Cache {
    entries: Mutex<BTreeMap<Key, Heat>>,
    answers: AtomicU64,
    foreground: Arc<Foreground>,
}
impl Default for Cache {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        for heat in self
            .entries
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            if let Some(c) = &heat.building {
                c.cancel();
            }
        }
    }
}
/// Cancelling an async owner must also cancel its CPU worker and release the heat marker.
struct Job {
    cache: std::sync::Weak<Cache>,
    key: Key,
    cancel: HotCancellation,
}
impl Drop for Job {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(cache) = self.cache.upgrade()
            && let Some(h) = cache
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get_mut(&self.key)
            && h.building.take().is_some()
        {
            h.retry = Instant::now() + RETRY;
        }
    }
}
#[derive(Clone)]
pub(crate) struct Scope {
    cache: Arc<Cache>,
    namespace: String,
    trial: bool,
    used: Arc<Mutex<Option<Vec<Key>>>>,
}
impl Scope {
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }
    pub(crate) fn new(cache: Arc<Cache>, namespace: &str) -> Self {
        Self {
            cache,
            namespace: namespace.into(),
            trial: false,
            used: Arc::new(Mutex::new(Some(Vec::new()))),
        }
    }
    pub(crate) fn trial(&self) -> Self {
        Self {
            trial: true,
            used: Arc::new(Mutex::new(Some(Vec::new()))),
            ..self.clone()
        }
    }
    /// A complete IVF retry must not calibrate any graph from its discarded partial plan.
    pub(crate) fn abandon(&self) {
        *self.used.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
    pub(crate) fn used_graph(&self) -> bool {
        self.used
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|used| !used.is_empty())
    }
    /// Pair the trial with ordinary IVF under the same admission, snapshot, and worker conditions.
    pub(crate) fn record_ivf_cpu(&self, seconds: f64) {
        if !seconds.is_finite() || seconds <= 0. {
            return;
        }
        let Some(mut used) = self.used.lock().unwrap_or_else(|e| e.into_inner()).clone() else {
            return;
        };
        used.sort();
        used.dedup();
        let mut entries = self.cache.entries.lock().unwrap_or_else(|e| e.into_inner());
        for key in used {
            if let Some(heat) = entries.get_mut(&key) {
                heat.ivf_seconds = if heat.ivf_seconds == 0. {
                    seconds
                } else {
                    heat.ivf_seconds * 0.8 + seconds * 0.2
                };
            }
        }
    }
    pub(crate) fn feedback(&self, recall: f64, reference: f64) {
        let mut used = {
            let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
            let Some(used) = used.as_mut() else {
                return;
            };
            std::mem::take(used)
        };
        used.sort();
        used.dedup();
        let mut entries = self.cache.entries.lock().unwrap_or_else(|e| e.into_inner());
        let mut retired = Vec::new();
        for key in used {
            if let Some(h) = entries.get_mut(&key)
                && let Some(entry) = h.ready.as_ref()
            {
                let mut c = entry.calibration.lock().unwrap_or_else(|e| e.into_inner());
                if !recall.is_finite() || !reference.is_finite() {
                    continue;
                }
                let update = c.quality.observe(recall, reference);
                if update.accepted {
                    c.graph_ratio = c.trial_ratio;
                }
                if update.changed {
                    c.trial_seconds = 0.;
                    c.trial_ratio = 0.;
                }
                let saving = c.saving(h.ivf_seconds);
                c.usable = c.quality.serving.is_some()
                    && c.graph_ratio > 0.
                    && h.rate_at(Instant::now()) * 86400. * saving >= 2. * c.build_seconds;
                let reject = update.exhausted
                    || (c.quality.settled() && c.quality.len() >= 128 && !c.usable);
                drop(c);
                if reject {
                    retired.push(h.ready.take());
                    h.first = Instant::now();
                    h.retry = Instant::now() + RETRY;
                }
            }
        }
        drop(entries);
        drop(retired);
    }
}
fn current() -> Option<Scope> {
    read_cache::context()?.hot_graph
}
impl Cache {
    pub(crate) fn new(foreground: u64) -> Self {
        Self {
            entries: Default::default(),
            answers: AtomicU64::new(0),
            foreground: Arc::new(Foreground::new(foreground)),
        }
    }
    pub(crate) fn scoped(&self) -> Self {
        Self {
            entries: Default::default(),
            answers: AtomicU64::new(0),
            foreground: self.foreground.clone(),
        }
    }

    pub(crate) fn clear(&self) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        for h in entries.values() {
            if let Some(c) = &h.building {
                c.cancel();
            }
        }
        entries.clear();
    }
    pub(crate) fn delete(&self, ns: &str) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k, h| {
                if k.namespace == ns {
                    if let Some(c) = &h.building {
                        c.cancel();
                    }
                    false
                } else {
                    true
                }
            });
    }
    fn prune(&self, ns: &str, state: &NamespaceState, now: Instant) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k, h| {
                let stale = now.duration_since(h.last) > IDLE
                    || (k.namespace == ns
                        && (k.generation != state.created_at
                            || !state
                                .ann_tiers
                                .get(&k.attribute)
                                .is_some_and(|sets| sets.iter().any(|s| s.dir.key == k.tier))));
                if stale && let Some(c) = &h.building {
                    c.cancel();
                }
                !stale
            });
    }
    /// Whether a ready, usable graph would answer this plain vector query. Used to let a query
    /// larger than the usual inline limit stay on the HTTP reactor.
    pub(crate) fn serves(&self, ns: &str, state: &NamespaceState, q: &QueryRequest) -> bool {
        let Some(RankBy::Vector {
            attribute, vector, ..
        }) = &q.rank_by
        else {
            return false;
        };
        let k = q.total().unwrap_or(10).saturating_add(q.offset);
        let Some([set]) = state.ann_tiers.get(attribute).map(Vec::as_slice) else {
            return false;
        };
        if overcommitted() {
            return false;
        }
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .get(&Key::new(ns, state, attribute, set))
            .and_then(|h| h.ready.as_ref())
            .is_some_and(|e| {
                k > 0
                    && k <= MAX_K
                    && e.graph.len() >= k
                    && vector.len() == e.graph.dims()
                    && e.calibration
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .usable
            })
    }
    pub(crate) fn observe(
        engine: &Engine,
        ns: &str,
        state: &NamespaceState,
        q: &QueryRequest,
        seconds: f64,
    ) {
        trim();
        let cache = &engine.0.hot_graph;
        let now = Instant::now();
        cache.prune(ns, state, now);
        let Some(RankBy::Vector {
            attribute,
            vector,
            exact: false,
            ..
        }) = &q.rank_by
        else {
            return;
        };
        if q.filters.is_some()
            || q.total().unwrap_or(10) == 0
            || q.total().unwrap_or(10).saturating_add(q.offset) > MAX_K
            || vector.iter().all(|x| *x == 0.)
            || state.metric != DistanceMetric::CosineDistance
        {
            return;
        }
        let Some((_, VectorType::F16)) = state.schema.get(attribute).and_then(segment::dense_type)
        else {
            return;
        };
        let Some(sets) = state.ann_tiers.get(attribute) else {
            return;
        };
        for set in sets {
            let key = Key::new(ns, state, attribute, set);
            let launch = {
                let mut entries = cache.entries.lock().unwrap_or_else(|e| e.into_inner());
                // A fixed metadata cap prevents one-query namespaces from growing an unbounded heat map.
                if entries.len() >= 256 && !entries.contains_key(&key) {
                    continue;
                }
                let h = entries.entry(key.clone()).or_insert_with(|| Heat::new(now));
                let baseline = if h.ready.is_none() {
                    seconds
                } else {
                    h.ivf_seconds
                };
                h.observe(now, baseline);
                if h.ready.is_some()
                    || h.building.is_some()
                    || now < h.retry
                    || now.duration_since(h.first) < DWELL
                {
                    None
                } else {
                    // Initial scheduling is an estimate. Installation and calibration use the
                    // actual measured construction cost and namespace query costs below.
                    let forecast = h.rate * 86400. * h.ivf_seconds * 0.5;
                    let estimate = set.rows() as f64 * 0.001;
                    if forecast < 2. * estimate {
                        None
                    } else {
                        let c = HotCancellation::default();
                        h.building = Some(c.clone());
                        Some(c)
                    }
                }
            };
            if let Some(cancel) = launch {
                let engine = engine.clone();
                let set = set.clone();
                let attribute = attribute.clone();
                let ns = ns.to_string();
                let running = engine.0.stop.track();
                tokio::spawn(async move {
                    let _running = running;
                    let _job = Job {
                        cache: Arc::downgrade(&engine.0.hot_graph),
                        key: key.clone(),
                        cancel: cancel.clone(),
                    };
                    let build = engine.construct_hot_graph(&ns, &attribute, &set, &cancel);
                    tokio::pin!(build);
                    let mut check_source = tokio::time::interval(Duration::from_secs(30));
                    let result = loop {
                        tokio::select! {
                            result = &mut build => break result,
                            _ = check_source.tick() => {
                                if engine.0.stop.stopping() { cancel.cancel(); }
                                if let Ok(wal) = Wal::new(engine.0.store.clone(), &ns)
                                    && let Ok(Some((_, manifest))) = wal.latest_manifest().await
                                    && (manifest.deleted || manifest.created_at != key.generation
                                        || !manifest.ann_tiers.get(&attribute).is_some_and(|sets| sets.iter().any(|s| s.dir.key == set.dir.key))) { cancel.cancel(); }
                            }
                        }
                    };
                    let mut entries = engine
                        .0
                        .hot_graph
                        .entries
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if let Some(h) = entries.get_mut(&key) {
                        h.building = None;
                        h.retry = Instant::now() + RETRY;
                        match result {
                            Ok(Some(entry)) if !cancel.cancelled() => {
                                h.ready = Some(Arc::new(entry));
                            }
                            Ok(_) => {}
                            Err(error) => tracing::debug!(%error,"warm graph build deferred"),
                        }
                    }
                });
            }
        }
    }
}
struct Source {
    builder: HotGraphBuilder,
    sources: Vec<Index<segment::ObjectReader>>,
    locations: Vec<(u32, [u32; 3])>,
}
struct Built {
    graph: HotGraph,
    sources: Vec<Index<segment::ObjectReader>>,
    locations: Locations,
    source_cpu: f64,
    build_cpu: f64,
    reservation: Reservation,
    _slot: BuildSlot,
    _running: crate::stop::Running,
}
async fn read_source(
    engine: &Engine,
    set: &SlabSet,
    attribute: &str,
    dims: usize,
    count: usize,
    cancel: &HotCancellation,
) -> Result<Source, ApiError> {
    let mut builder = HotGraphBuilder::new(dims, count).map_err(segment::index_error)?;
    let mut sources = Vec::new();
    let mut locations = Vec::with_capacity(count);
    for (slab_id, slab) in set.slabs.iter().enumerate() {
        if cancel.cancelled() || engine.0.stop.stopping() {
            return Err(ApiError::new(
                503,
                "warm graph source unavailable or cancelled",
            ));
        }
        let Some((file, _)) = crate::upgrade::files(slab, attribute) else {
            return Err(ApiError::new(
                503,
                "warm graph source unavailable or cancelled",
            ));
        };
        let source = Index::open_with_metadata_len(
            segment::ObjectReader::new(&engine.0.store, file).bypass_cache(),
            file.metadata_len,
        )
        .await
        .map_err(segment::index_error)?;
        if source.vector_type() != VectorType::F16
            || source.dims() != dims
            || source.metric() != bf_vector::Metric::CosineDistance
        {
            return Err(ApiError::new(
                503,
                "warm graph source unavailable or cancelled",
            ));
        }
        for cluster in 0..source.cluster_count() {
            if cancel.cancelled() || engine.0.stop.stopping() {
                return Err(ApiError::new(
                    503,
                    "warm graph source unavailable or cancelled",
                ));
            }
            if source.cluster_rows(cluster).saturating_mul(dims * 2) > 64 << 20 {
                return Err(ApiError::new(
                    503,
                    "warm graph source unavailable or cancelled",
                ));
            }
            let (ids, packed) = source
                .read_cluster_rows(cluster)
                .await
                .map_err(segment::index_error)?;
            locations.extend(
                ids.iter()
                    .enumerate()
                    .map(|(ordinal, &row)| (row, [slab_id as u32, cluster as u32, ordinal as u32])),
            );
            builder
                .append(&ids, &packed, &|| {
                    cancel.cancelled() || engine.0.stop.stopping()
                })
                .map_err(segment::index_error)?;
        }
        let context = read_cache::ReadContext {
            admit: false,
            ..read_cache::ReadContext::new(engine.0.read_cache.clone())
        };
        sources.push(
            read_cache::scope(
                context,
                Index::open_with_metadata_len(
                    segment::ObjectReader::new(&engine.0.store, file),
                    file.metadata_len,
                ),
            )
            .await
            .map_err(segment::index_error)?,
        );
    }
    Ok(Source {
        builder,
        sources,
        locations,
    })
}
// Production priority is tested separately from constructor correctness. A SCHED_IDLE
// worker can be starved indefinitely by unrelated builds on a shared Linux test host.
fn background_priority() {
    #[cfg(target_os = "linux")]
    // SAFETY: both calls affect only this worker on Linux. Idle scheduling yields
    // to serving work; nice 19 remains a fallback when a sandbox denies it.
    unsafe {
        libc::setpriority(libc::PRIO_PROCESS, 0, 19);
        let priority = libc::sched_param { sched_priority: 0 };
        libc::sched_setscheduler(0, libc::SCHED_IDLE, &priority);
    }
    #[cfg(target_os = "macos")]
    // SAFETY: the QoS call changes only the calling worker's scheduling class.
    unsafe {
        unsafe extern "C" {
            fn pthread_set_qos_class_self_np(
                class: libc::c_uint,
                priority: libc::c_int,
            ) -> libc::c_int;
        }
        // QOS_CLASS_UTILITY from Apple's sys/qos.h (the libc enum is not reexported).
        pthread_set_qos_class_self_np(0x11, 0);
    }
}

fn pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .thread_name(|n| format!("warm-graph-{n}"))
            .start_handler(|_| {
                #[cfg(not(all(test, target_os = "linux")))]
                background_priority();
            })
            .build()
            .expect("warm graph CPU pool")
    })
}
impl Engine {
    async fn construct_hot_graph(
        &self,
        ns: &str,
        attribute: &str,
        set: &SlabSet,
        cancel: &HotCancellation,
    ) -> Result<Option<Entry>, ApiError> {
        if crate::auto_recluster::building() {
            return Ok(None);
        }
        let Some(_slot) = BuildSlot::take() else {
            return Ok(None);
        };
        let start = Instant::now();
        let wal = Wal::new(self.0.store.clone(), ns).map_err(segment::index_error)?;
        let Some((_, manifest)) = wal.latest_manifest().await.map_err(segment::index_error)? else {
            return Ok(None);
        };
        if manifest.deleted
            || manifest.distance_metric != DistanceMetric::CosineDistance
            || !manifest
                .ann_tiers
                .get(attribute)
                .is_some_and(|sets| sets.iter().any(|s| s.dir.key == set.dir.key))
        {
            return Ok(None);
        }
        let Some((dims, VectorType::F16)) =
            manifest.schema.get(attribute).and_then(segment::dense_type)
        else {
            return Ok(None);
        };
        let Some(count) = set
            .slabs
            .iter()
            .try_fold(0usize, |n, s| n.checked_add(s.row_count as usize))
        else {
            return Ok(None);
        };
        let workers = self.0.config.query_concurrency.max(1);
        let Some(metadata) = set
            .slabs
            .iter()
            .filter_map(|s| crate::upgrade::files(s, attribute))
            .try_fold(0u64, |sum, (file, _)| sum.checked_add(file.metadata_len))
        else {
            return Ok(None);
        };
        let Some(estimate) = HotGraph::reservation(count, dims, workers)
            .and_then(|n| n.checked_add(metadata.saturating_mul(3)))
            .and_then(|n| n.checked_add(count as u64 * 20))
            .and_then(|n| n.checked_add(1 << 20))
        else {
            return Ok(None);
        };
        let limit = bf_alloc::memory_limit_bytes();
        let foreground = FOREGROUND.load(Ordering::Acquire);
        let cap = limit.saturating_mul(4) / 5;
        if !memory_available(estimate.saturating_add(limit / 5)) {
            return Ok(None);
        }
        let mut reserved = Reservation::new(estimate, cap.saturating_sub(foreground));
        if reserved.is_none() {
            let key = Key {
                namespace: ns.into(),
                generation: manifest.created_at,
                attribute: attribute.into(),
                tier: set.dir.key.clone(),
            };
            let benefit = self
                .0
                .hot_graph
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .map_or(0., |h| h.rate_at(Instant::now()) * h.ivf_seconds * 0.5);
            make_room(estimate, benefit / estimate.max(1) as f64);
            reserved = Reservation::new(estimate, cap.saturating_sub(foreground));
        }
        let Some(reservation) = reserved else {
            return Ok(None);
        };
        let c = cancel.clone();
        let stop = self.0.stop.clone();
        let engine = self.clone();
        let source_set = set.clone();
        let source_attribute = attribute.to_owned();
        let runtime = tokio::runtime::Handle::current();
        // Source decoding and graph construction run on the same two-worker
        // low-priority pool. The worker owns its charge even if its waiter is aborted.
        let worker_running = self.0.stop.track();
        let mut built = tokio::task::spawn_blocking(move || {
            pool().install(|| {
                let _busy = bf_alloc::busy();
                let source_started = thread_cpu();
                let source = read_source(&engine, &source_set, &source_attribute, dims, count, &c);
                let Source { builder, sources, locations } = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        () = stop.stopped() => Err(ApiError::new(503, "warm graph build cancelled")),
                        source = source => source,
                    }
                })?;
                let source_cpu = (thread_cpu() - source_started).max(0.);
                let before: f64 = pool().broadcast(|_| thread_cpu()).into_iter().sum();
                let checks = AtomicU64::new(0);
                let graph = builder.finish(&|| c.cancelled() || stop.stopping() || overcommitted()
                    || (checks.fetch_add(1, Ordering::Relaxed).is_multiple_of(16384) && !memory_available(limit / 5))).map_err(segment::index_error)?;
                let locations = Locations::new(locations);
                let after: f64 = pool().broadcast(|_| thread_cpu()).into_iter().sum();
                Ok::<_, ApiError>(Built { graph, sources, locations, source_cpu, build_cpu: (after-before).max(0.), reservation, _slot, _running: worker_running })
            })
        }).await.map_err(segment::index_error)??;
        let current = wal.latest_manifest().await.map_err(segment::index_error)?;
        if cancel.cancelled()
            || self.0.stop.stopping()
            || !current.is_some_and(|(_, m)| {
                !m.deleted
                    && m.created_at == manifest.created_at
                    && m.ann_tiers
                        .get(attribute)
                        .is_some_and(|sets| sets.iter().any(|s| s.dir.key == set.dir.key))
            })
        {
            return Ok(None);
        }
        let resident = built.graph.resident_bytes()
            + built.sources.iter().map(Index::resident_bytes).sum::<u64>()
            + built.locations.resident_bytes();
        let retained = (resident
            + (count as u64).div_ceil(32) * 8 * workers as u64
            + workers as u64 * (2 << 20))
            .saturating_mul(5)
            / 4;
        if retained > built.reservation.0 {
            return Ok(None);
        }
        built.reservation.shrink(retained);
        tracing::info!(
            rows = count,
            seconds = start.elapsed().as_secs_f64(),
            resident_bytes = resident,
            reserved_bytes = retained,
            "warm graph constructed; calibrating recall"
        );
        Ok(Some(Entry {
            graph: built.graph,
            sources: built.sources,
            locations: built.locations,
            scratch: Mutex::new(Vec::new()),
            mask_live: Mutex::new(None),
            slots: tokio::sync::Semaphore::new(workers),
            calibration: Mutex::new(Calibration {
                quality: effort::Effort::new(),
                trial_seconds: 0.,
                trial_ratio: 0.,
                usable: false,
                graph_ratio: 0.,
                // Include a conservative allowance for source metadata and CRC decoding.
                build_seconds: (built.build_cpu + built.source_cpu) * 1.1,
            }),
            _reservation: built.reservation,
            installed_at: Instant::now(),
            build_wall_seconds: start.elapsed().as_secs_f64(),
            measured_cpu_seconds: built.build_cpu + built.source_cpu,
            build_reservation_bytes: estimate,
            phase_cpu: Default::default(),
            searches: AtomicU64::new(0),
            ids: std::sync::OnceLock::new(),
            ids_started: std::sync::atomic::AtomicBool::new(false),
            pending_cost: Mutex::default(),
        }))
    }
}
pub(crate) fn available(
    state: &NamespaceState,
    attribute: &str,
    set: &SlabSet,
    vector: &[f32],
    k: usize,
) -> bool {
    if overcommitted() {
        trim();
        return false;
    }
    let Some(scope) = current() else {
        return false;
    };
    let entries = scope
        .cache
        .entries
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    entries
        .get(&Key::new(&scope.namespace, state, attribute, set))
        .and_then(|h| h.ready.as_ref())
        .is_some_and(|e| {
            k > 0
                && k <= MAX_K
                && e.graph.len() >= k
                && vector.len() == e.graph.dims()
                && vector.iter().any(|x| *x != 0.)
                && (scope.trial
                    || e.calibration
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .usable)
        })
}
pub(crate) fn mask_eligible(
    state: &NamespaceState,
    attribute: &str,
    set: &SlabSet,
    mask: Option<&Arc<RoaringBitmap>>,
    k: usize,
) -> bool {
    let Some(mask) = mask else {
        return true;
    };
    let Some(scope) = current() else {
        return false;
    };
    let key = Key::new(&scope.namespace, state, attribute, set);
    let entry = scope
        .cache
        .entries
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .and_then(|h| h.ready.clone());
    let Some(entry) = entry else {
        return false;
    };
    let live = {
        let mut cached = entry.mask_live.lock().unwrap_or_else(|e| e.into_inner());
        match cached.as_ref() {
            Some((old, live)) if old.upgrade().is_some_and(|m| Arc::ptr_eq(&m, mask)) => *live,
            _ => {
                let live = entry.graph.live_count(mask);
                *cached = Some((Arc::downgrade(mask), live));
                live
            }
        }
    };

    if live >= k && live.saturating_mul(10) >= entry.graph.len().saturating_mul(9) {
        return true;
    }
    let mut entries = scope
        .cache
        .entries
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(h) = entries.get_mut(&key) {
        h.ready = None;
        h.first = Instant::now();
        h.retry = Instant::now() + RETRY;
    }
    false
}
struct ScratchLoan<'a> {
    entry: &'a Entry,
    scratch: Option<HotScratch>,
}
impl Drop for ScratchLoan<'_> {
    fn drop(&mut self) {
        if let Some(s) = self.scratch.take() {
            self.entry
                .scratch
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(s);
        }
    }
}
#[allow(clippy::too_many_arguments)]
pub(crate) async fn search(
    store: &Storage,
    state: &NamespaceState,
    attribute: &str,
    set: &SlabSet,
    at: &Placement,
    vector: &[f32],
    mask: Option<Arc<RoaringBitmap>>,
    k: usize,
    columns: &BTreeSet<String>,
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    let Some(scope) = current() else {
        return Ok(None);
    };
    let key = Key::new(&scope.namespace, state, attribute, set);
    let entry = {
        let entries = scope
            .cache
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        entries.get(&key).and_then(|h| h.ready.clone())
    };
    let Some(entry) = entry else {
        return Ok(None);
    };
    let ef = {
        let c = entry.calibration.lock().unwrap_or_else(|e| e.into_inner());
        if !scope.trial && !c.usable {
            return Ok(None);
        }
        if scope.trial {
            c.quality.trial
        } else {
            c.quality.serving.unwrap_or(c.quality.trial)
        }
    };
    if k == 0
        || k > MAX_K
        || entry.graph.len() < k
        || vector.len() != entry.graph.dims()
        || vector.iter().all(|x| *x == 0.)
    {
        return Ok(None);
    }
    if !mask_eligible(state, attribute, set, mask.as_ref(), k) {
        return Ok(None);
    }
    let Ok(_permit) = entry.slots.try_acquire() else {
        return Ok(None);
    };
    if columns.is_empty()
        && bf_vector::g12_flags::on(bf_vector::g12_flags::LATE_IDS)
        && entry.ids.get().is_none()
        && !entry.ids_started.swap(true, Ordering::AcqRel)
    {
        tokio::spawn(build_id_table(
            store.clone(),
            state.clone(),
            set.clone(),
            at.clone(),
            entry.clone(),
        ));
    }
    let result = search_entry(store, state, set, at, vector, mask, k, columns, &entry, ef).await;
    match result {
        Ok(Some(found)) => {
            if let Some(used) = scope
                .used
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_mut()
            {
                used.push(key);
            }
            scope.cache.answers.fetch_add(1, Ordering::Relaxed);
            Ok(Some(found))
        }
        Ok(None) => Ok(None),
        Err(error) => {
            tracing::debug!(%error,"warm graph declined; using IVF");
            Ok(None)
        }
    }
}
#[allow(clippy::too_many_arguments)]
async fn search_entry(
    store: &Storage,
    state: &NamespaceState,
    set: &SlabSet,
    at: &Placement,
    vector: &[f32],
    mask: Option<Arc<RoaringBitmap>>,
    k: usize,
    columns: &BTreeSet<String>,
    entry: &Entry,
    ef: usize,
) -> Result<Option<(BTreeMap<Id, f64>, BTreeMap<Id, Document>)>, ApiError> {
    let searches = entry.searches.fetch_add(1, Ordering::Relaxed);
    // Phase counters read the thread CPU clock, a kernel call on macOS. With sampling on, one
    // search in SAMPLE_EVERY is timed and its counters are scaled to stand for all of them.
    let sampled = bf_vector::g12_flags::on(bf_vector::g12_flags::SAMPLED_CPU);
    let timed = !sampled || searches % SAMPLE_EVERY == 0;
    let weight = if sampled { SAMPLE_EVERY } else { 1 };
    let cpu_now = || if timed { thread_cpu() } else { 0. };
    let navigation_started = cpu_now();
    #[cfg(all(test, target_os = "macos"))]
    let counter_started = nav_probe::begin();
    let scratch = entry
        .scratch
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .pop()
        .unwrap_or_else(|| entry.graph.scratch());
    let mut loan = ScratchLoan {
        entry,
        scratch: Some(scratch),
    };
    let (ef, shortlist) = plan(k, ef);
    let (hits, evaluations) = entry
        .graph
        .candidates_with_norms(
            vector,
            ef,
            shortlist,
            mask.as_deref(),
            loan.scratch.as_mut().unwrap(),
        )
        .map_err(segment::index_error)?;
    drop(loan);
    #[cfg(all(test, target_os = "macos"))]
    nav_probe::end(
        counter_started,
        evaluations,
        &hits.iter().map(|h| h.0).collect::<Vec<_>>(),
    );
    #[cfg(not(all(test, target_os = "macos")))]
    let _ = evaluations;
    if timed {
        entry.phase_cpu[0].fetch_add(
            ((thread_cpu() - navigation_started).max(0.) * 1e9) as u64 * weight,
            Ordering::Relaxed,
        );
    }
    let mut by_source: BTreeMap<usize, Vec<(usize, usize, bf_vector::CosineNorm)>> =
        BTreeMap::new();
    for (row, norm) in hits {
        let [slab, cluster, ordinal] = entry.locations.get(row).ok_or_else(segment::corrupt)?;
        by_source.entry(slab as usize).or_default().push((
            cluster as usize,
            ordinal as usize,
            norm,
        ));
    }
    let prepared = CosineQuery::new(vector).map_err(segment::index_error)?;
    let mut exact = measure_phase(
        &entry.phase_cpu[1],
        timed.then_some(weight),
        futures::future::try_join_all(by_source.into_iter().map(|(slab, locations)| {
            let prepared = &prepared;
            async move {
                entry.sources[slab]
                    .rerank_cosine_candidates(prepared, k, locations)
                    .await
                    .map_err(segment::index_error)
            }
        })),
    )
    .await?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    exact.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    exact.truncate(k);
    if exact.len() < k {
        return Ok(None);
    }
    if columns.is_empty()
        && bf_vector::g12_flags::on(bf_vector::g12_flags::LATE_IDS)
        && let Some(table) = entry.ids.get()
        && let Some(found) = ids_from_table(table, &exact, mask.as_deref(), set, at, k)
    {
        return Ok(found);
    }
    let mut by_segment: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
    let mut distances = BTreeMap::new();
    for (row, distance) in exact {
        if mask.as_ref().is_some_and(|m| !m.contains(row)) {
            continue;
        }
        let Some((i, local)) = at.real(&set.covered, row) else {
            return Ok(None);
        };
        by_segment.entry(i).or_default().insert(local);
        distances.insert((i, local), distance);
    }
    let mut scores = BTreeMap::new();
    let mut documents = BTreeMap::new();
    for (i, rows) in by_segment {
        for (row, doc) in measure_phase(
            &entry.phase_cpu[2],
            timed.then_some(weight),
            segment::projected_rows(store, &state.segments[i], &rows, Some(columns)),
        )
        .await?
        {
            let id: Id =
                serde_json::from_value(doc.get("id").ok_or_else(segment::corrupt)?.clone())
                    .map_err(segment::index_error)?;
            scores.insert(
                id.clone(),
                *distances.get(&(i, row)).ok_or_else(segment::corrupt)? as f64,
            );
            documents.insert(id, doc);
        }
    }
    if scores.len() < k {
        return Ok(None);
    }
    Ok(Some((scores, documents)))
}
type Found = (BTreeMap<Id, f64>, BTreeMap<Id, Document>);
/// The answer for an ID-only query from the resident table, carrying `(row, distance)` to the end
/// and resolving each ID once, for the final rows only. `None` when the table cannot answer (a row
/// without an ID, say), so the caller takes the document path. `Some(None)` is the same decline the
/// document path gives when a row has no live home.
fn ids_from_table(
    table: &[u64],
    exact: &[(u32, f32)],
    mask: Option<&RoaringBitmap>,
    set: &SlabSet,
    at: &Placement,
    k: usize,
) -> Option<Option<Found>> {
    let mut scores = BTreeMap::new();
    let mut documents = BTreeMap::new();
    for &(row, distance) in exact {
        if mask.is_some_and(|m| !m.contains(row)) {
            continue;
        }
        if at.real(&set.covered, row).is_none() {
            return Some(None);
        }
        let id = *table.get(row as usize).filter(|id| **id != u64::MAX)?;
        let mut doc = Document::new();
        doc.insert("id".into(), serde_json::Value::from(id));
        scores.insert(Id::Uint(id), f64::from(distance));
        documents.insert(Id::Uint(id), doc);
    }
    Some((scores.len() >= k).then_some((scores, documents)))
}
/// Read every tier row's unsigned ID once, from the same documents the query path hydrates, and
/// publish the table on the entry. Any failure or non-numeric ID leaves the entry without one.
async fn build_id_table(
    store: Storage,
    state: NamespaceState,
    set: SlabSet,
    at: Placement,
    entry: Arc<Entry>,
) {
    let total = set
        .covered
        .iter()
        .map(|c| u64::from(c.base) + u64::from(c.rows))
        .max()
        .unwrap_or(0) as usize;
    let mut wanted: BTreeMap<usize, Vec<(u32, u32)>> = BTreeMap::new();
    for row in 0..total as u32 {
        if let Some((segment, local)) = at.real(&set.covered, row) {
            wanted.entry(segment).or_default().push((local, row));
        }
    }
    let mut table = vec![u64::MAX; total];
    let none = BTreeSet::new();
    for (segment, pairs) in wanted {
        let rows: RoaringBitmap = pairs.iter().map(|(local, _)| *local).collect();
        let Some(segment) = state.segments.get(segment) else {
            return;
        };
        let Ok(docs) =
            segment::projected_rows_with(&store, segment, &rows, Some(&none), true).await
        else {
            return;
        };
        for (local, row) in pairs {
            let Some(id) = docs
                .get(&local)
                .and_then(|d| d.get("id"))
                .and_then(serde_json::Value::as_u64)
            else {
                return;
            };
            table[row as usize] = id;
        }
    }
    let _ = entry.ids.set(table);
}
/// Run a calibration ANN plan through the same CPU dispatch as an ordinary warm query.
/// Exhaustive truth retains its separate blocking worker and nonadmitting context.
pub(crate) async fn trial_plan(
    store: &Storage,
    state: &NamespaceState,
    request: &QueryRequest,
    config: &crate::EngineConfig,
    mut context: read_cache::ReadContext,
) -> Result<(bf_types::QueryResponse, f64), ApiError> {
    let store = store.clone();
    let state = state.clone();
    let request = request.clone();
    let config = config.clone();
    context.cpu_nanos = Arc::new(AtomicU64::new(0));
    let cpu = context.cpu_nanos.clone();
    let query = Box::pin(async move {
        measure_cpu(
            context.cpu_nanos.clone(),
            true,
            read_cache::scope(
                context,
                crate::indexed::execute(&store, &state, &request, &config, false),
            ),
        )
        .await
    });
    let answer = crate::query_worker::run(query, false).await??;
    Ok((answer, cpu.load(Ordering::Relaxed) as f64 / 1e9))
}
fn measure_phase<F: std::future::Future>(
    cpu: &AtomicU64,
    weight: Option<u64>,
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    let mut future = Box::pin(future);
    std::future::poll_fn(move |cx| {
        let Some(weight) = weight else {
            return future.as_mut().poll(cx);
        };
        let started = thread_cpu();
        let result = future.as_mut().poll(cx);
        cpu.fetch_add(
            ((thread_cpu() - started).max(0.) * 1e9) as u64 * weight,
            Ordering::Relaxed,
        );
        result
    })
}
/// Charge each poll on its current thread, including Q2's resumed I/O continuation.
/// Async waiting and migration between worker threads do not become query CPU time.
pub(crate) fn measure_cpu<F: std::future::Future>(
    cpu: Arc<AtomicU64>,
    enabled: bool,
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    // Pin the large query plan on the heap before constructing the small poll wrapper. Keeping
    // both the input and a pin! local in an async fn doubles its debug-build stack footprint.
    let mut future = Box::pin(future);
    std::future::poll_fn(move |cx| {
        if !enabled {
            return future.as_mut().poll(cx);
        }
        let started = thread_cpu();
        let result = future.as_mut().poll(cx);
        let elapsed = (thread_cpu() - started).max(0.);
        cpu.fetch_add((elapsed * 1e9) as u64, Ordering::Relaxed);
        result
    })
}

/// Whether this ordinary query reads the thread CPU clock: always, or one in `SAMPLE_EVERY`
/// when sampling is on. Only warm-IVF cost estimates need it, and they converge from a sample.
pub(crate) fn measure_this_query() -> bool {
    static COUNT: AtomicU64 = AtomicU64::new(0);
    !bf_vector::g12_flags::on(bf_vector::g12_flags::SAMPLED_CPU)
        || COUNT.fetch_add(1, Ordering::Relaxed) % SAMPLE_EVERY == 0
}

/// Round G12: read experiment switches from the file named by `BF_G12_FLAGS` (a decimal
/// bitmask) once a second, so one process can compare paths on the same promoted graph.
fn start_flag_poller() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let Some(path) = std::env::var_os("BF_G12_FLAGS") else {
            return;
        };
        std::thread::spawn(move || {
            loop {
                if let Some(bits) = std::fs::read_to_string(&path)
                    .ok()
                    .and_then(|t| t.trim().parse::<u32>().ok())
                {
                    bf_vector::g12_flags::FLAGS.store(bits, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        });
    });
}

pub(crate) fn thread_cpu() -> f64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: the writable timespec is valid, and this clock has no process-global side effects.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } == 0 {
        ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
    } else {
        0.
    }
}

fn scratch_dir(engine: &Engine) -> std::path::PathBuf {
    engine
        .0
        .store
        .cache_limits()
        .1
        .unwrap_or_else(std::env::temp_dir)
        .join("blowfish-warm-graph")
}
pub(crate) fn start(engine: &Engine) {
    start_flag_poller();
    if let Err(error) = crate::recluster::spill::sweep(&scratch_dir(engine)) {
        tracing::debug!(%error, "warm graph spill recovery deferred");
    }
    CACHES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(Arc::downgrade(&engine.0.hot_graph));
    trim();
    let cache = Arc::downgrade(&engine.0.hot_graph);
    let stop = engine.0.stop.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {()=stop.stopped()=>break,()=tokio::time::sleep(Duration::from_secs(60))=>{}}
            let Some(cache) = cache.upgrade() else {
                break;
            };
            let now = Instant::now();
            cache
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|_, h| {
                    if now.duration_since(h.last) <= IDLE {
                        true
                    } else {
                        if let Some(c) = &h.building {
                            c.cancel();
                        }
                        false
                    }
                });
        }
    });
}
impl Engine {
    /// Read-only diagnostics for the automatic, disposable warm path. This exposes no tuning controls.
    pub fn hot_graph_status(&self, namespace: &str) -> serde_json::Value {
        let entries = self
            .0
            .hot_graph
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let graphs=entries.iter().filter(|(k,_)|k.namespace==namespace).map(|(k,h)|{
            let calibration=h.ready.as_ref().map(|e|{let c=e.calibration.lock().unwrap_or_else(|e|e.into_inner());serde_json::json!({"ef":c.quality.serving.unwrap_or(c.quality.trial),"trial_ef":c.quality.trial,"usable":c.usable,"samples":c.quality.len(),"paired_quality":c.quality.statistics(),"trial_cpu_seconds":c.trial_seconds,"build_cpu_seconds":e.measured_cpu_seconds,"build_wall_seconds":e.build_wall_seconds,"build_reservation_bytes":e.build_reservation_bytes,"reserved_bytes":e._reservation.0,"promotion_cpu_cost_seconds":c.build_seconds,"graph_cost_ratio":c.graph_ratio,"resident_bytes":e.graph.resident_bytes(),"phase_searches":e.searches.load(Ordering::Relaxed),"phase_cpu_seconds":e.phase_cpu.iter().map(|v|v.load(Ordering::Relaxed) as f64/1e9).collect::<Vec<_>>()})});
            serde_json::json!({"attribute":k.attribute,"building":h.building.is_some(),"calibration":calibration,"heat_seconds":h.first.elapsed().as_secs_f64(),"query_rate":h.rate_at(Instant::now()),"ivf_cpu_seconds":h.ivf_seconds,"retry_seconds":h.retry.saturating_duration_since(Instant::now()).as_secs_f64()})
        }).collect::<Vec<_>>();
        serde_json::json!({"graphs":graphs,"answered":self.0.hot_graph.answers.load(Ordering::Relaxed),"process_reserved_bytes":RESERVED.load(Ordering::Relaxed)})
    }
}

fn memory_available(bytes: u64) -> bool {
    #[cfg(target_os = "linux")]
    {
        let charged: Option<u64> = (|| {
            let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
            let path = cgroup.lines().find_map(|l| l.strip_prefix("0::"))?;
            let base = format!("/sys/fs/cgroup{}", path.trim_end_matches('/'));
            let max = std::fs::read_to_string(format!("{base}/memory.max"))
                .ok()?
                .trim()
                .parse::<u64>()
                .ok()?;
            let current = std::fs::read_to_string(format!("{base}/memory.current"))
                .ok()?
                .trim()
                .parse::<u64>()
                .ok()?;
            Some(max.saturating_sub(current))
        })();
        let available = charged.or_else(|| {
            let mem = std::fs::read_to_string("/proc/meminfo").ok()?;
            mem.lines().find_map(|l| {
                Some(
                    l.strip_prefix("MemAvailable:")?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()?
                        .saturating_mul(1024),
                )
            })
        });
        if let Some(available) = available {
            return available >= bytes;
        }
    }
    let _ = bytes;
    // The process-wide reservation remains authoritative where charged-memory counters are absent.
    true
}

/// Whole-graph eviction under process pressure, lowest predicted CPU benefit first. A scoped
/// engine shares its parent's foreground budgets, but owns its graph identities independently.
fn trim() {
    if !overcommitted() {
        return;
    }
    let caches = {
        let mut registry = CACHES.lock().unwrap_or_else(|e| e.into_inner());
        registry.retain(|c| c.strong_count() > 0);
        registry
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .collect::<Vec<_>>()
    };
    let mut choices = Vec::new();
    for cache in caches {
        let entries = cache.entries.lock().unwrap_or_else(|e| e.into_inner());
        for (key, heat) in entries.iter() {
            let benefit = heat.ready.as_ref().map_or(0., |entry| {
                let c = entry.calibration.lock().unwrap_or_else(|e| e.into_inner());
                heat.rate_at(Instant::now()) * c.saving(heat.ivf_seconds)
                    / entry._reservation.0.max(1) as f64
            });
            choices.push((benefit, key.clone(), cache.clone()));
        }
    }
    choices.sort_by(|a, b| a.0.total_cmp(&b.0));
    for (_, key, cache) in choices {
        if !overcommitted() {
            break;
        }
        let retired = {
            let mut entries = cache.entries.lock().unwrap_or_else(|e| e.into_inner());
            entries.get_mut(&key).and_then(|h| {
                if let Some(cancel) = &h.building {
                    cancel.cancel();
                }
                h.retry = Instant::now() + RETRY;
                h.first = Instant::now();
                h.ready.take()
            })
        };
        drop(retired);
    }
}

/// Prefer a substantially more beneficial tier only after the incumbent's minimum residence.
/// Ordinary pressure eviction remains immediate, and retired queries retain their own charge.
fn make_room(bytes: u64, benefit_per_byte: f64) {
    let cap = bf_alloc::memory_limit_bytes().saturating_mul(4) / 5;
    let fits = || {
        RESERVED
            .load(Ordering::Acquire)
            .saturating_add(FOREGROUND.load(Ordering::Acquire))
            .saturating_add(bytes)
            <= cap
    };
    if fits() || benefit_per_byte <= 0. {
        return;
    }
    let caches = CACHES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(std::sync::Weak::upgrade)
        .collect::<Vec<_>>();
    let mut choices = Vec::new();
    for cache in caches {
        let entries = cache.entries.lock().unwrap_or_else(|e| e.into_inner());
        for (key, h) in entries.iter() {
            if let Some(e) = &h.ready
                && e.installed_at.elapsed() >= Duration::from_secs(600)
            {
                let c = e.calibration.lock().unwrap_or_else(|e| e.into_inner());
                let saving = if c.graph_ratio == 0. {
                    h.ivf_seconds * 0.5
                } else {
                    c.saving(h.ivf_seconds)
                };
                let old = h.rate_at(Instant::now()) * saving / e._reservation.0.max(1) as f64;
                if benefit_per_byte > old * 2. {
                    choices.push((old, key.clone(), cache.clone()));
                }
            }
        }
    }
    choices.sort_by(|a, b| a.0.total_cmp(&b.0));
    for (_, key, cache) in choices {
        if fits() {
            break;
        }
        let retired = {
            let mut entries = cache.entries.lock().unwrap_or_else(|e| e.into_inner());
            entries.get_mut(&key).and_then(|h| {
                h.first = Instant::now();
                h.retry = Instant::now() + RETRY;
                h.ready.take()
            })
        };
        drop(retired);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod plan_tests {
    use super::*;

    #[test]
    fn small_requests_keep_the_calibrated_beam_and_shortlist() {
        for k in 1..=RERANK {
            assert_eq!(plan(k, 64), (64, RERANK));
            assert_eq!(plan(k, 48), (48, RERANK));
        }
    }

    #[test]
    fn large_requests_scale_with_k_and_never_narrow() {
        assert_eq!(plan(100, 64), (256, 150));
        assert_eq!(plan(31, 64), (80, 47));
        assert_eq!(plan(100, 400), (400, 150));
        let mut last = plan(RERANK + 1, 64);
        for k in RERANK + 2..=MAX_K {
            let now = plan(k, 64);
            assert!(now.0 >= last.0 && now.1 >= last.1 && now.0 >= now.1);
            last = now;
        }
    }
}
