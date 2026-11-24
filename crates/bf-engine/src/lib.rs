//! Immutable segment indexes with a WAL overlay and an exhaustive reference evaluator.
pub use query_worker::on_query_reactor;
mod ann;
mod ann_dir;
mod auto_recluster;
mod branch;
mod drain;
mod filter;
mod highlight;
mod indexed;
mod indexer;
#[cfg(test)]
mod ingest_benchmark;
mod lineage;
mod local_ann;
mod pattern;
mod plan;
mod read_cache;
mod segment;
mod snapshot;
mod sparse;
mod statistics;
mod stop;
mod streaming;
mod tail;
#[cfg(test)]
mod tail_benchmark;
mod text_upgrade;
pub use indexer::EngineConfig;
pub use recluster::{ReclusterOptions, ReclusterReport};
pub use reindex::{LayoutCounts, ReindexOptions, ReindexReport};
mod metadata;
mod models;
mod published;
mod query;
mod query_worker;
use published::Published;
mod recluster;
mod reindex;
mod text;
mod text_keys;
mod text_tail;
mod upgrade;
mod warm_ann;
mod write;

use bf_storage::{CreateResult, Storage};
use bf_types::*;
use bf_wal::{GroupCommit, Pending, Wal, WalEntry, WriteBatch};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};

/// One unindexed WAL entry resident in a namespace's tail.
#[derive(Clone)]
pub(crate) struct TailEntry {
    pub seq: u64,
    pub bytes: u64,
    pub at: DateTime<Utc>,
    /// A copy or branch install: its effect is not captured by `docs` and `shadowed`.
    pub copy: bool,
}
/// A checkpoint's segments, shared: cloning a namespace state (a query does it several times)
/// must not copy thousands of segment records. Mutation copies on write.
#[derive(Clone, Default)]
pub(crate) struct Segments(Arc<Vec<bf_wal::Segment>>);
impl std::ops::Deref for Segments {
    type Target = Vec<bf_wal::Segment>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Segments {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}
impl Segments {
    /// Drop every segment without copying the shared list.
    pub(crate) fn clear(&mut self) {
        self.0 = Arc::default();
    }
}
impl<'a> IntoIterator for &'a Segments {
    type Item = &'a bf_wal::Segment;
    type IntoIter = std::slice::Iter<'a, bf_wal::Segment>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
impl From<Vec<bf_wal::Segment>> for Segments {
    fn from(v: Vec<bf_wal::Segment>) -> Self {
        Self(Arc::new(v))
    }
}
#[derive(Clone)]
pub(crate) struct NamespaceState {
    pub seq: u64,
    pub exists: bool,
    pub schema: Schema,
    pub metric: DistanceMetric,
    /// Only the unindexed tail stays resident. Materialized views are query-local.
    pub docs: BTreeMap<Id, std::sync::Arc<Document>>,
    pub shadowed: BTreeSet<Id>,
    /// The last tail entry that wrote or deleted each tail ID, so a publication can drop
    /// exactly the IDs it indexed without re-reading the WAL.
    pub marks: BTreeMap<Id, u64>,
    pub entries: Vec<TailEntry>,
    pub segments: Segments,
    /// The segments came from the compact reader copy of the manifest, which has no text headers
    /// (see `bf_wal::Manifest::reader_copy`); a writer or a text query reads the full manifest first.
    pub root_only: bool,
    pub indexed_through: u64,
    pub manifest_seq: u64,
    pub tail_bytes: u64,
    pub tail_since: Option<DateTime<Utc>>,
    pub stats: Arc<bf_wal::NamespaceStats>,
    /// Object holding the checkpoint's per-term text statistics (loaded only for BM25 queries
    /// and by the indexer), when the manifest keeps them out of line.
    pub text_stats: Option<Arc<bf_wal::SegmentFile>>,
    /// Namespace ANN directories by `annv2:<attribute>`, valid for exactly `segments`.
    pub ann_dirs: BTreeMap<String, bf_wal::SegmentFile>,
    /// Recluster tiers by vector attribute (see `bf_wal::Manifest::ann_tiers`).
    pub ann_tiers: Arc<BTreeMap<String, Vec<bf_wal::SlabSet>>>,
    /// Where merged tier-covered segments' rows went (see `bf_wal::Manifest::lineage`).
    pub lineage: Arc<Vec<bf_wal::Lineage>>,
    /// When the checkpoint's indexer last flushed a bulk-load-sized tail (see
    /// `bf_wal::Manifest::bulk_load_at`).
    pub bulk_load_at: Option<DateTime<Utc>>,
    pub scores: BTreeMap<Vec<u8>, BTreeMap<Id, f64>>,
    pub corpus: Arc<BTreeMap<String, bf_text::CorpusStats>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub refreshed: Instant,
    pub checkpoint_refreshed: Instant,
}
impl Default for NamespaceState {
    fn default() -> Self {
        Self {
            seq: 0,
            exists: false,
            schema: Schema::new(),
            metric: DistanceMetric::default(),
            docs: BTreeMap::new(),
            shadowed: BTreeSet::new(),
            marks: BTreeMap::new(),
            entries: vec![],
            segments: Segments::default(),
            root_only: false,
            indexed_through: 0,
            manifest_seq: 0,
            tail_bytes: 0,
            tail_since: None,
            stats: Default::default(),
            text_stats: None,
            ann_dirs: BTreeMap::new(),
            ann_tiers: Arc::default(),
            lineage: Arc::default(),
            bulk_load_at: None,
            scores: BTreeMap::new(),
            corpus: Arc::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            refreshed: Instant::now(),
            checkpoint_refreshed: Instant::now(),
        }
    }
}
/// Shared documents: cloning a namespace state must not deep-copy its tail.
pub(crate) fn shared(docs: BTreeMap<Id, Document>) -> BTreeMap<Id, Arc<Document>> {
    docs.into_iter()
        .map(|(id, doc)| (id, Arc::new(doc)))
        .collect()
}
impl NamespaceState {
    /// Everything but the tail documents, which writes pick out by ID.
    pub(crate) fn without_docs(&self) -> Self {
        Self {
            seq: self.seq,
            exists: self.exists,
            schema: self.schema.clone(),
            metric: self.metric,
            docs: BTreeMap::new(),
            shadowed: self.shadowed.clone(),
            marks: BTreeMap::new(),
            entries: self.entries.clone(),
            segments: self.segments.clone(),
            root_only: self.root_only,
            indexed_through: self.indexed_through,
            manifest_seq: self.manifest_seq,
            tail_bytes: self.tail_bytes,
            tail_since: self.tail_since,
            stats: self.stats.clone(),
            text_stats: self.text_stats.clone(),
            ann_dirs: self.ann_dirs.clone(),
            ann_tiers: self.ann_tiers.clone(),
            lineage: self.lineage.clone(),
            bulk_load_at: self.bulk_load_at,
            scores: BTreeMap::new(),
            corpus: self.corpus.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            refreshed: self.refreshed,
            checkpoint_refreshed: self.checkpoint_refreshed,
        }
    }
    fn apply(&mut self, b: &WriteBatch, seq: u64) {
        self.exists = true;
        if self.schema.keys().any(|name| !b.schema.contains_key(name)) {
            for doc in self.docs.values_mut() {
                Arc::make_mut(doc).retain(|name, _| b.schema.contains_key(name));
            }
        }
        self.schema = b.schema.clone();
        self.metric = b.distance_metric;
        self.created_at = b.created_at;
        self.updated_at = b.updated_at;
        for id in &b.deletes {
            self.docs.remove(id);
            self.shadowed.insert(id.clone());
            if seq > 0 {
                self.marks.insert(id.clone(), seq);
            }
        }
        for doc in &b.upserts {
            let id: Id = serde_json::from_value(doc["id"].clone()).expect("validated ID");
            self.shadowed.insert(id.clone());
            if seq > 0 {
                self.marks.insert(id.clone(), seq);
            }
            self.docs.insert(id, Arc::new(doc.clone()));
        }
    }
    /// Apply a not-yet-committed batch for write evaluation: its schema and metric, and only the
    /// deletes and upserts that name an ID in `wanted`.
    pub(crate) fn apply_filtered(
        &mut self,
        b: &WriteBatch,
        wanted: &std::collections::BTreeSet<Id>,
    ) {
        self.exists = true;
        if self.schema.keys().any(|name| !b.schema.contains_key(name)) {
            for doc in self.docs.values_mut() {
                Arc::make_mut(doc).retain(|name, _| b.schema.contains_key(name));
            }
        }
        self.schema = b.schema.clone();
        self.metric = b.distance_metric;
        self.created_at = b.created_at;
        self.updated_at = b.updated_at;
        for id in b.deletes.iter().filter(|id| wanted.contains(id)) {
            self.docs.remove(id);
        }
        for doc in &b.upserts {
            if let Ok(id) = serde_json::from_value::<Id>(doc["id"].clone())
                && wanted.contains(&id)
            {
                self.docs.insert(id, Arc::new(doc.clone()));
            }
        }
    }
    fn replay(&mut self, seq: u64, entry: &WalEntry) {
        if entry.tombstone {
            let manifest_seq = self.manifest_seq;
            *self = Self::default();
            self.manifest_seq = manifest_seq;
        }
        for batch in &entry.batches {
            self.apply(batch, seq);
        }
        self.tail_since.get_or_insert(entry.committed_at);
        self.seq = seq;
        let bytes = serde_json::to_vec(entry).expect("WAL serializes").len() as u64;
        self.tail_bytes += bytes;
        self.entries.push(TailEntry {
            seq,
            bytes,
            at: entry.committed_at,
            copy: false,
        });
    }
    /// Adopt a checkpoint covered by this resident WAL history, retaining later document Arcs.
    /// Maintenance can replace segments without advancing `through`. Gaps, copies, and older
    /// checkpoints return false without changing state, so the caller can replay from storage.
    pub(crate) fn fast_forward(
        &mut self,
        manifest: &bf_wal::Manifest,
        generation: u64,
        through: u64,
    ) -> bool {
        if manifest.deleted
            || generation < self.manifest_seq
            || self.indexed_through > through
            || self.seq < through
            || self.entries.iter().any(|e| e.copy)
            || self
                .entries
                .first()
                .is_some_and(|e| e.seq > self.indexed_through + 1)
            || self.entries.iter().filter(|e| e.seq <= through).count() as u64
                != through - self.indexed_through
        {
            return false;
        }
        let marks = std::mem::take(&mut self.marks);
        self.docs
            .retain(|id, _| marks.get(id).is_some_and(|m| *m > through));
        self.shadowed
            .retain(|id| marks.get(id).is_some_and(|m| *m > through));
        self.marks = marks.into_iter().filter(|(_, m)| *m > through).collect();
        self.entries.retain(|e| e.seq > through);
        self.tail_bytes = self.entries.iter().map(|e| e.bytes).sum();
        self.tail_since = self.entries.first().map(|e| e.at);
        self.scores.clear();
        self.segments = manifest.segments.clone().into();
        self.root_only = manifest.root;
        self.indexed_through = through;
        self.manifest_seq = generation;
        self.corpus = if manifest.text_stats.is_some() {
            Default::default()
        } else {
            text::checkpoint_corpus(&manifest.stats)
        };
        self.text_stats = manifest.text_stats.clone().map(Arc::new);
        self.ann_dirs = manifest.ann_dirs.clone();
        self.ann_tiers = manifest.ann_tiers.clone().into();
        self.lineage = manifest.lineage.clone().into();
        self.bulk_load_at = manifest.bulk_load_at;
        self.stats = manifest.stats.clone().into();
        self.checkpoint_refreshed = Instant::now();
        true
    }
}
/// Query-local diagnostics; backend durations sum overlapping requests.
#[derive(Clone, Debug)]
pub struct QueryDiagnostics {
    /// Time spent obtaining a consistent namespace snapshot.
    pub snapshot_elapsed: std::time::Duration,
    /// Time spent queuing, searching, and assembling results.
    pub execution_elapsed: std::time::Duration,
    /// Backend requests by stage and operation.
    pub backend: Vec<bf_storage::BackendMeasurement>,
    /// Logical cache-byte accounting, separate from physical backend bytes.
    pub cache: bf_storage::CacheStatsSnapshot,
    /// Rows in the leaf code blocks or exact ANN vector ranges visited by the query.
    pub ann_rows_scanned: u64,
    /// Rows whose document vectors were read because their ANN index was unavailable.
    pub document_scan_rows: u64,
}
struct Handle {
    state: Arc<Published<NamespaceState>>,
    commits: GroupCommit<Operation, Result<WriteResponse, ApiError>>,
    backpressure: Arc<auto_recluster::LoadBackpressure>,
}
struct Inner {
    store: Storage,
    read_cache: Arc<read_cache::DecodedCache>,
    queries: Arc<tokio::sync::Semaphore>,
    recall_sampler: Arc<RecallSampler>,
    config: EngineConfig,
    owner: uuid::Uuid,
    namespaces: Published<HashMap<String, Arc<Handle>>>,
    /// The automatic recluster's builds and the tiers waiting to be installed.
    jobs: auto_recluster::Jobs,
    /// Shutdown of background work, shared with scoped engines.
    stop: Arc<stop::Stop>,
}
/// Queries executing right now (holding a query permit), across every engine in the process.
static QUERIES_IN_FLIGHT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Whether a query should fan its work out over other threads (one task per probed segment or
/// slab, the rayon pool for ranking). Fanning out cuts a lone query's latency, but each hop costs
/// a handoff and a wakeup; once enough queries run to keep the cores busy on their own, a query
/// runs its pieces one after another on its own thread instead, which raises throughput. The
/// threshold is half the CPUs this process may use (`BF_FANOUT_QUERIES` overrides it).
pub(crate) fn fan_out() -> bool {
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    let limit = *LIMIT.get_or_init(|| {
        std::env::var("BF_FANOUT_QUERIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map_or(1, |n| n.get() / 2)
                    .max(1)
            })
    });
    QUERIES_IN_FLIGHT.load(std::sync::atomic::Ordering::Relaxed) <= limit
}

struct InFlight;
impl InFlight {
    fn enter() -> Self {
        QUERIES_IN_FLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self
    }
}
impl Drop for InFlight {
    fn drop(&mut self) {
        QUERIES_IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Paces the background recall sampler. An exhaustive comparison reads every vector of the
/// namespace (5.5 GB at 2.7M f16 rows of 1,024 dimensions), so a per-query rate alone would let
/// it take most of the machine under load. One runs at a time; the first comes one interval after
/// the engine starts, not with the first queries after every restart; and the next waits at least
/// one interval, and at least [`RECALL_SAMPLE_DUTY`] times as long as the last one took, so it
/// never holds more than about 2% of one core, whatever the namespace's size.
struct RecallSampler {
    busy: Arc<std::sync::atomic::AtomicBool>,
    not_before: Arc<std::sync::Mutex<Instant>>,
}
const RECALL_SAMPLE_DUTY: u32 = 50;
impl Default for RecallSampler {
    fn default() -> Self {
        Self {
            busy: Arc::default(),
            not_before: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
    }
}
/// Held while one recall comparison runs.
struct Sampling {
    busy: Arc<std::sync::atomic::AtomicBool>,
    not_before: Arc<std::sync::Mutex<Instant>>,
    started: Instant,
}
impl Drop for Sampling {
    fn drop(&mut self) {
        let took = self.started.elapsed();
        let mut not_before = self.not_before.lock().unwrap_or_else(|e| e.into_inner());
        *not_before = (*not_before).max(Instant::now() + took * RECALL_SAMPLE_DUTY);
        self.busy.store(false, std::sync::atomic::Ordering::Release);
    }
}
impl RecallSampler {
    /// A sampler whose first comparison may start `interval` from now.
    fn new(interval: std::time::Duration) -> Self {
        let sampler = Self::default();
        *sampler.not_before.lock().expect("recall sampler lock") = Instant::now() + interval;
        sampler
    }
    fn start(&self, interval: std::time::Duration) -> Option<Sampling> {
        use std::sync::atomic::Ordering;
        let mut not_before = self.not_before.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if now < *not_before {
            return None;
        }
        if self.busy.swap(true, Ordering::AcqRel) {
            return None;
        }
        *not_before = now + interval;
        Some(Sampling {
            busy: self.busy.clone(),
            not_before: self.not_before.clone(),
            started: now,
        })
    }
}
/// Cloneable namespace engine. Separate instances safely share a CAS-capable bucket.
#[derive(Clone)]
pub struct Engine(Arc<Inner>);
enum Operation {
    Write(
        Box<WriteRequest>,
        Option<Box<branch::Source>>,
        auto_recluster::PendingWrite,
    ),
    Delete(auto_recluster::PendingWrite),
}
impl Engine {
    /// Use an already-probed store. No writable local state is required.
    pub fn new(store: Storage) -> Self {
        Self::with_config(store, EngineConfig::from_env())
    }
    /// Configure indexing, tail limits, retention, and ANN behavior explicitly.
    pub fn with_config(store: Storage, config: EngineConfig) -> Self {
        let stop: Arc<stop::Stop> = Arc::default();
        let engine = Self(Arc::new(Inner {
            read_cache: Arc::new(
                read_cache::DecodedCache::new(
                    config.decoded_cache_bytes,
                    config.search_concurrency,
                )
                .with_store(&store, stop.clone()),
            ),
            queries: Arc::new(tokio::sync::Semaphore::new(config.query_concurrency.max(1))),
            recall_sampler: Arc::new(RecallSampler::new(config.recall_sample_interval)),
            store,
            config,
            owner: uuid::Uuid::new_v4(),
            namespaces: Published::new(HashMap::new()),
            jobs: Default::default(),
            stop,
        }));
        bf_alloc::adaptive_decay();
        auto_recluster::sweep_spills(&engine);
        indexer::start(&engine);
        engine
    }
    /// Drop query caches and resident namespace state, retaining provisioned immutable ANN
    /// files. The next query has cold query caches on a provisioned node. For benchmarks only.
    pub async fn drop_caches(&self) {
        self.0.read_cache.clear();
        self.0.store.drop_caches();
        self.0.namespaces.lock().await.clear();
    }

    /// An engine over a sub-prefix of this one's storage, such as a project's namespaces. It
    /// shares the storage backend, caches' byte budgets, and query concurrency with this
    /// engine, but its namespaces are separate, with their own handles and background indexing.
    pub fn scoped(&self, sub: &str) -> Self {
        let engine = Self(Arc::new(Inner {
            store: self.0.store.scoped(sub),
            read_cache: Arc::new(self.0.read_cache.scoped(sub)),
            queries: self.0.queries.clone(),
            recall_sampler: self.0.recall_sampler.clone(),
            config: self.0.config.clone(),
            owner: uuid::Uuid::new_v4(),
            namespaces: Published::new(HashMap::new()),
            jobs: Default::default(),
            stop: self.0.stop.clone(),
        }));
        indexer::start(&engine);
        engine
    }
    /// Signal this engine's background work to stop, without waiting (see [`Engine::shutdown`]).
    pub fn begin_shutdown(&self) {
        self.0.stop.begin();
    }
    /// Stop this engine's background work, and its scoped engines': indexing, compaction, GC,
    /// upgrades, recluster builds, and recall samples stop at their next await point, and a
    /// build's CPU-bound passes between rows. Waits up to `grace` for them, and says whether they
    /// all stopped. Requests keep being served. Everything stopped is crash-safe and resumes on
    /// the next start (an interrupted build's record stays until it is stale, then GC reclaims
    /// its objects).
    pub async fn shutdown(&self, grace: std::time::Duration) -> bool {
        self.begin_shutdown();
        let stopped = self.0.stop.drained(grace).await;
        if !stopped {
            tracing::warn!(
                running = self.0.stop.running(),
                "background jobs still running at shutdown"
            );
        }
        stopped
    }
    /// Access the underlying store for operational inspection.
    pub fn storage(&self) -> &Storage {
        &self.0.store
    }
    /// Namespaces with an in-memory handle on this node, sorted for operational inspection.
    pub async fn cached_namespaces(&self) -> Vec<String> {
        let mut names: Vec<_> = self.0.namespaces.lock().await.keys().cloned().collect();
        names.sort();
        names
    }
    async fn handle(&self, ns: &str) -> Result<Arc<Handle>, ApiError> {
        validate_namespace(ns)?;
        if let Some(handle) = self.0.namespaces.load().get(ns) {
            return Ok(handle.clone());
        }
        let mut handles = self.0.namespaces.lock().await;
        if let Some(handle) = handles.get(ns) {
            return Ok(handle.clone());
        }
        let (commits, receiver) = GroupCommit::channel(1024);
        let state = Arc::new(Published::new(NamespaceState::default()));
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        let backpressure = Arc::new(auto_recluster::LoadBackpressure::default());
        let handle = Arc::new(Handle {
            state: state.clone(),
            commits,
            backpressure: backpressure.clone(),
        });
        handles.insert(ns.into(), handle.clone());
        let context = read_cache::ReadContext::new(self.0.read_cache.clone());
        query_worker::shared_runtime().spawn(read_cache::scope(
            context,
            commit_worker(
                wal,
                state,
                receiver,
                self.0.store.clone(),
                self.0.config.clone(),
                backpressure,
            ),
        ));
        Ok(handle)
    }
    async fn snapshot(
        &self,
        ns: &str,
        consistency: &Consistency,
    ) -> Result<(NamespaceState, bool), ApiError> {
        self.snapshot_hinted(ns, consistency, None, false).await
    }
    /// A snapshot that, when it has to read a newer checkpoint, opens the files the query
    /// will need while the WAL tail is being read.
    async fn snapshot_hinted(
        &self,
        ns: &str,
        consistency: &Consistency,
        hint: Option<snapshot::OpenHint>,
        refresh_pending_checkpoint: bool,
    ) -> Result<(NamespaceState, bool), ApiError> {
        let handle = self.handle(ns).await?;
        let mut state = handle.state.lock().await;
        let warm = state.seq > 0;
        let refresh = match consistency {
            Consistency::Strong => true,
            Consistency::Eventual {
                max_staleness_seconds,
            } => {
                if !(1..=3600).contains(max_staleness_seconds) {
                    return Err(ApiError::bad(
                        "max_staleness_seconds must be between 1 and 3600",
                    ));
                }
                state.seq == 0
                    || state.refreshed.elapsed().as_secs() >= *max_staleness_seconds
                    || state.refreshed.elapsed() >= self.0.config.retention.div_f64(2.0)
            }
        };
        let cap = matches!(consistency, Consistency::Eventual { .. })
            .then_some(self.0.config.eventual_tail_bytes);
        let over_cap = cap.is_some_and(|cap| state.tail_bytes > cap);
        if over_cap {
            *state = NamespaceState::default();
        }
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        // A retained checkpoint remains valid independently of newer index publications.
        // Bound its age separately from WAL freshness, so repeated strong reads cannot
        // keep a reclaimed checkpoint alive indefinitely.
        let checkpoint_valid = state.seq > 0
            && state.checkpoint_refreshed.elapsed() < self.0.config.retention.div_f64(2.0)
            // Metadata polling must see remote index publications even without a new write.
            // Finished checkpoints retain the ordinary strong-read fast path.
            && !(refresh_pending_checkpoint && (state.tail_bytes > 0 || !state.docs.is_empty()));
        let unchanged = if matches!(consistency, Consistency::Strong) && checkpoint_valid {
            let observed = (state.seq, state.manifest_seq);
            // Keep independent consistency requests off the namespace state lock.
            // Reconcile after the HEAD so a concurrent commit cannot be overwritten.
            drop(state);
            let head = self.0.store.head(&wal.key(observed.0 + 1)).await;
            state = handle.state.lock().await;
            match head {
                Err(bf_storage::Error::NotFound)
                    if (state.seq, state.manifest_seq) == observed
                        && state.checkpoint_refreshed.elapsed()
                            < self.0.config.retention.div_f64(2.0) =>
                {
                    state.refreshed = Instant::now();
                    true
                }
                Err(bf_storage::Error::NotFound) | Ok(_) => false,
                Err(error) => return Err(storage_error(error)),
            }
        } else {
            false
        };
        if (refresh || over_cap) && !unchanged {
            snapshot::catch_up_hinted(
                &Wal::new(self.0.store.clone(), ns).map_err(log_error)?,
                &mut state,
                cap,
                &self.0.store,
                hint.as_ref(),
            )
            .await?;
        }
        if !state.exists {
            if cap.is_some()
                && Wal::new(self.0.store.clone(), ns)
                    .map_err(log_error)?
                    .head()
                    .await
                    .map_err(log_error)?
                    > state.seq
            {
                return Err(ApiError::new(
                    409,
                    "eventual WAL tail exceeds the scan limit; wait for indexing",
                ));
            }
            return Err(ApiError::not_found(ns));
        }
        Ok((state.clone(), warm))
    }
    async fn query_snapshot(
        &self,
        ns: &str,
        consistency: &Consistency,
        hint: snapshot::OpenHint,
        backend: &Arc<bf_storage::BackendStats>,
    ) -> Result<(Arc<NamespaceState>, bool), ApiError> {
        if matches!(consistency, Consistency::Strong) {
            let handle = self.handle(ns).await?;
            let state = handle.state.load();
            if state.exists
                && state.seq > 0
                && state.checkpoint_refreshed.elapsed() < self.0.config.retention.div_f64(2.0)
            {
                let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
                match self.0.store.head(&wal.key(state.seq + 1)).await {
                    Err(bf_storage::Error::NotFound) => {
                        let current = handle.state.load();
                        if current.seq == state.seq
                            && current.manifest_seq == state.manifest_seq
                            && current.checkpoint_refreshed.elapsed()
                                < self.0.config.retention.div_f64(2.0)
                        {
                            return Ok((current, true));
                        }
                    }
                    Ok(_) => {}
                    Err(error) => return Err(storage_error(error)),
                }
            }
        }
        if query_worker::is_reactor() {
            let (engine, ns, consistency) = (self.clone(), ns.to_owned(), consistency.clone());
            let backend = backend.clone();
            return query_worker::run_io(query_worker::shared_runtime(), async move {
                backend
                    .scope(
                        "snapshot",
                        engine.snapshot_hinted(&ns, &consistency, Some(hint), false),
                    )
                    .await
                    .map(|(state, warm)| (Arc::new(state), warm))
            })
            .await?;
        }
        self.snapshot_hinted(ns, consistency, Some(hint), false)
            .await
            .map(|(state, warm)| (Arc::new(state), warm))
    }

    /// Atomically validate, group, and durably commit all operations in one request.
    pub async fn write(
        &self,
        ns: &str,
        mut request: WriteRequest,
    ) -> Result<WriteResponse, ApiError> {
        if self.0.store.is_read_only() {
            return Err(ApiError::new(403, "server is read-only"));
        }
        let _busy = bf_alloc::busy();
        self.check_writable(ns).await?;
        let embedded = self.embed_for_write(ns, &mut request).await?;
        let handle = self.handle(ns).await?;
        let copy = if let Some(source) = request
            .copy_from_namespace
            .as_ref()
            .or(request.branch_from_namespace.as_ref())
        {
            let source = match source {
                CopyFrom::Name(s) => s,
                CopyFrom::Config {
                    source_namespace,
                    options,
                } => {
                    if options.keys().any(|k| {
                        matches!(
                            k.as_str(),
                            "source_api_key" | "source_region" | "dest_encryption"
                        )
                    }) {
                        return Err(ApiError::bad(
                            "cross-deployment copy and custom encryption are not supported by blowfish yet",
                        ));
                    }
                    source_namespace
                }
            };
            if source == ns {
                return Err(ApiError::bad("cannot copy a namespace to itself"));
            }
            Some(Box::new(
                self.copy_source(source, ns, request.branch_from_namespace.is_some())
                    .await?,
            ))
        } else {
            None
        };
        let inherited_read_only = if let Some(source) = &copy {
            self.settings_for_copy(&request, &source.state).await?
        } else {
            false
        };
        let response = handle
            .commits
            .submit(Operation::Write(
                Box::new(request),
                copy,
                handle.backpressure.begin(),
            ))
            .await??;
        if inherited_read_only {
            self.patch_metadata(ns, json!({"read_only":true})).await?;
        }
        let mut response = response;
        if let Some((tokens, ms)) = embedded {
            response.performance.embedding_tokens = Some(tokens);
            response.performance.embedding_ms = Some(ms);
        }
        Ok(response)
    }
    /// Search a consistent segment-and-tail snapshot, including multi-query RRF.
    pub async fn query(&self, ns: &str, request: QueryRequest) -> Result<QueryResponse, ApiError> {
        self.query_with_diagnostics(ns, request)
            .await
            .map(|(response, _)| response)
    }
    /// Search with query-local backend counts, transferred bytes, and stage timings.
    /// Diagnostics are separate from the turbopuffer wire response.
    pub async fn query_with_diagnostics(
        &self,
        ns: &str,
        request: QueryRequest,
    ) -> Result<(QueryResponse, QueryDiagnostics), ApiError> {
        if query_worker::is_reactor() {
            let resident = self.0.config.models.is_none()
                && self.0.namespaces.load().get(ns).is_some_and(|handle| {
                    let state = handle.state.load();
                    state.exists
                        && state.seq > 0
                        && state.checkpoint_refreshed.elapsed()
                            < self.0.config.retention.div_f64(2.0)
                        && warm_ann::inline_plan(
                            &self.0.read_cache,
                            &state,
                            &request,
                            &self.0.config,
                        )
                });
            if !resident {
                let start = Instant::now();
                let (engine, ns) = (self.clone(), ns.to_owned());
                let result = query_worker::run_io(
                    query_worker::shared_runtime(),
                    Box::pin(async move { engine.query_configured(&ns, request).await }),
                )
                .await?;
                return result.map(|(mut response, diagnostics)| {
                    response.performance.server_total_ms = start.elapsed().as_millis() as u64;
                    (response, diagnostics)
                });
            }
        }
        self.query_configured(ns, request).await
    }
    async fn query_configured(
        &self,
        ns: &str,
        request: QueryRequest,
    ) -> Result<(QueryResponse, QueryDiagnostics), ApiError> {
        match self.0.config.models.clone() {
            Some(models) => self.query_with_models(models, ns, request).await,
            None => self.query_inner(ns, request).await,
        }
    }
    async fn query_inner(
        &self,
        ns: &str,
        request: QueryRequest,
    ) -> Result<(QueryResponse, QueryDiagnostics), ApiError> {
        let start = Instant::now();
        let backend = Arc::new(bf_storage::BackendStats::default());
        let stats = Arc::new(bf_storage::CacheStats::default());
        let context = read_cache::ReadContext {
            stats: stats.clone(),
            backend: backend.clone(),
            ..read_cache::ReadContext::new(self.0.read_cache.clone())
        };
        let ann_rows = context.ann_rows.clone();
        let local_ann_bytes = context.local_ann_bytes.clone();
        let document_scan_rows = context.document_scan_rows.clone();
        let cached = if matches!(request.consistency, Consistency::Strong) {
            let handle = self.handle(ns).await?;
            let state = handle.state.load();
            (state.exists
                && state.seq > 0
                && state.checkpoint_refreshed.elapsed() < self.0.config.retention.div_f64(2.0))
            .then_some(state)
        } else {
            None
        };
        let hint = snapshot::OpenHint::for_request(
            &request,
            context.clone(),
            &self.0.config,
            // With a resident checkpoint the query searches speculatively, concurrently with the
            // snapshot: prefetching its scan as well would read the same ranges twice.
            cached.is_none(),
        );
        let snapshot = async {
            let result = backend
                .scope(
                    "snapshot",
                    self.query_snapshot(ns, &request.consistency, hint, &backend),
                )
                .await;
            (result, start.elapsed())
        };
        // Search immutable files while checking the WAL. Only accept speculative
        // results when the refreshed snapshot has the same WAL and manifest version.
        // A concurrent write, index publication, or deletion discards even errors
        // from the old search, and executes against the refreshed state instead.
        let (state, mut response, execution_elapsed, snapshot_elapsed) =
            if let Some(cached) = cached {
                let ((snapshot, elapsed), searched) = tokio::join!(
                    snapshot,
                    self.search_snapshot(&cached, &request, context.clone(), true)
                );
                let (state, _) = snapshot?;
                let (response, execution) =
                    if state.seq == cached.seq && state.manifest_seq == cached.manifest_seq {
                        searched?
                    } else {
                        self.search_snapshot(&state, &request, context, true)
                            .await?
                    };
                (state, response, execution, elapsed)
            } else {
                let (snapshot, elapsed) = snapshot.await;
                let (state, warm) = snapshot?;
                let (response, execution) = self
                    .search_snapshot(&state, &request, context, warm)
                    .await?;
                (state, response, execution, elapsed)
            };
        if matches!(request.rank_by, Some(RankBy::Vector { exact: false, .. }))
            && self.0.config.recall_sample_rate > 0.0
            && (uuid::Uuid::new_v4().as_u128() % 1_000_000) as f64 / 1_000_000.0
                < self.0.config.recall_sample_rate
            && let Some(sampling) = self
                .0
                .recall_sampler
                .start(self.0.config.recall_sample_interval)
        {
            let store = self.0.store.clone();
            // The exhaustive search reads every vector: one segment at a time, so it holds one
            // core rather than all of them, and without filling the block caches, so it does not
            // evict what live queries reuse.
            let mut config = self.0.config.clone();
            config.search_concurrency = 1;
            let sample = state.clone();
            let ann = response.clone();
            let cache = self.0.read_cache.clone();
            let queries = self.0.queries.clone();
            let stop = self.0.stop.clone();
            query_worker::shared_runtime().spawn(async move {
                let Ok(permit) = queries.acquire_owned().await else {
                    return;
                };
                let runtime = tokio::runtime::Handle::current();
                let context = read_cache::ReadContext {
                    admit: false,
                    ..read_cache::ReadContext::new(cache)
                };
                let _ = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let _sampling = sampling;
                    runtime.block_on(read_cache::scope(context, async move {
                        if let Some(Ok(exact)) = stop
                            .run(indexed::execute(&store, &sample, &request, &config, true))
                            .await
                        {
                            metrics::histogram!("blowfish_ann_recall")
                                .record(indexed::recall(&ann, &exact));
                        }
                    }));
                })
                .await;
            });
        }
        let mut cache_stats = stats.snapshot();
        cache_stats.disk_bytes += local_ann_bytes.load(std::sync::atomic::Ordering::Relaxed);
        response.performance = QueryPerformance {
            cache_hit_ratio: cache_stats.cache_hit_ratio(),
            cache_temperature: cache_stats.temperature().into(),
            server_total_ms: start.elapsed().as_millis() as u64,
            query_execution_ms: execution_elapsed.as_millis() as u64,
            exhaustive_search_count: state.docs.len() as u64,
            approx_namespace_size: state.stats.rows + state.docs.len() as u64,
            last_included_write_at: Some(
                state
                    .updated_at
                    .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            ),
            embedding_tokens: None,
            embedding_ms: None,
            rerank_ms: None,
        };
        if crate::ann::trace_stages() {
            eprintln!(
                "QUERY snapshot={}us exec={}us total={}us",
                snapshot_elapsed.as_micros(),
                execution_elapsed.as_micros(),
                start.elapsed().as_micros()
            );
        }
        let diagnostics = QueryDiagnostics {
            snapshot_elapsed,
            execution_elapsed,
            backend: backend.snapshot(),
            cache: cache_stats,
            ann_rows_scanned: ann_rows.load(std::sync::atomic::Ordering::Relaxed),
            document_scan_rows: document_scan_rows.load(std::sync::atomic::Ordering::Relaxed),
        };
        Ok((response, diagnostics))
    }
    async fn search_snapshot(
        &self,
        state: &Arc<NamespaceState>,
        request: &QueryRequest,
        context: read_cache::ReadContext,
        warm: bool,
    ) -> Result<(QueryResponse, std::time::Duration), ApiError> {
        let start = Instant::now();
        let store = self.0.store.clone();
        let config = self.0.config.clone();
        let t_clone = Instant::now();
        let inline = query_worker::is_reactor()
            && warm_ann::inline_plan(&self.0.read_cache, state, request, &config);
        let state = state.clone();
        if crate::ann::trace_stages() {
            eprintln!("CLONE {}us", t_clone.elapsed().as_micros());
        }
        let request = request.clone();
        let permit = self
            .0
            .queries
            .clone()
            .acquire_owned()
            .await
            .map_err(segment::index_error)?;
        // The full indexed plan is large. Keep the dispatch alternatives pointer-sized so a
        // saved I/O continuation does not multiply its stack footprint in the caller's future.
        let query = Box::pin(async move {
            let _permit = permit;
            let _in_flight = InFlight::enter();
            read_cache::scope(
                context,
                indexed::execute(&store, &state, &request, &config, false),
            )
            .await
        });
        let response = if warm {
            query_worker::run(query, inline).await??
        } else {
            metrics::counter!("blowfish_query_cpu_dispatch_total", "path" => "cold").increment(1);
            query_worker::run_io(tokio::runtime::Handle::current(), query).await??
        };
        Ok((response, start.elapsed()))
    }
    /// Run the exhaustive reference evaluator on the same persistent snapshot machinery.
    pub async fn query_exhaustive(
        &self,
        ns: &str,
        request: QueryRequest,
    ) -> Result<QueryResponse, ApiError> {
        let (state, _) = self.snapshot(ns, &request.consistency).await?;
        let mut full = snapshot::materialize(&self.0.store, &state).await?;
        text::reference_stats(&mut full);
        query::execute(&full, &request)
    }
    /// Namespace metadata, with fully expanded schema settings and current WAL indexing progress.
    pub async fn metadata(&self, ns: &str) -> Result<Value, ApiError> {
        let (s, _) = self
            .snapshot_hinted(ns, &Consistency::Strong, None, true)
            .await?;
        let totals = snapshot::statistics(&self.0.store, &s).await?;
        let mut metadata = json!({"schema":s.schema,"approx_logical_bytes":totals.logical_bytes,"approx_row_count":totals.rows,"created_at":s.created_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),"updated_at":s.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),"last_write_at":s.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),"encryption":{"mode":"default"},"index":{"status":"up-to-date"}});
        // Every live tail row is searched exhaustively, including scalar-only namespaces.
        // Deletes and schema-only writes leave bytes to checkpoint even with zero tail rows.
        // Built segments (including small flat indexes) and recluster tiers are already indexed.
        if s.tail_bytes > 0 || !s.docs.is_empty() {
            metadata["index"] = json!({"status":"updating","unindexed_bytes":s.tail_bytes,"unindexed_rows":s.docs.len() as u64});
        }
        if let Some(lineage) = branch::lineage(&self.0.store, ns, s.created_at).await? {
            metadata["branching"] = serde_json::to_value(lineage).expect("lineage serializes");
        }
        self.decorate_metadata(ns, &s.created_at.to_rfc3339(), &mut metadata)
            .await?;
        Ok(metadata)
    }
    /// Schema endpoint representation, including nullable settings and `ann: true`.
    pub async fn schema(&self, ns: &str) -> Result<Value, ApiError> {
        let (s, _) = self.snapshot(ns, &Consistency::Strong).await?;
        Ok(Value::Object(
            s.schema
                .iter()
                .map(|(k, v)| (k.clone(), v.schema_value()))
                .collect(),
        ))
    }
    /// Atomically apply a schema update and return the expanded schema.
    pub async fn update_schema(&self, ns: &str, schema: SchemaUpdate) -> Result<Value, ApiError> {
        self.write(
            ns,
            WriteRequest {
                schema: Some(schema),
                create_namespace: Some(false),
                ..Default::default()
            },
        )
        .await?;
        self.schema(ns).await
    }
    /// Delete via a serialized tombstone. Its immutable slot fences stale writers.
    pub async fn delete_namespace(&self, ns: &str) -> Result<(), ApiError> {
        let handle = self.handle(ns).await?;
        handle
            .commits
            .submit(Operation::Delete(handle.backpressure.begin()))
            .await??;
        Ok(())
    }
    /// Sorted, consistent namespace listing, using opaque base64 cursors.
    pub async fn list_namespaces(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        page_size: usize,
    ) -> Result<Value, ApiError> {
        use base64::Engine as _;
        if !(1..=1000).contains(&page_size) {
            return Err(ApiError::bad("page_size must be between 1 and 1000"));
        }
        let after = cursor
            .map(|c| {
                base64::prelude::BASE64_STANDARD
                    .decode(c)
                    .ok()
                    .and_then(|b| String::from_utf8(b).ok())
                    .ok_or_else(|| ApiError::bad("invalid namespace cursor"))
            })
            .transpose()?;
        let mut names = std::collections::BTreeSet::new();
        for obj in self.0.store.list("ns/").await.map_err(storage_error)? {
            if let Some(ns) = obj
                .key
                .strip_prefix("ns/")
                .and_then(|p| p.split('/').next())
                && ns.starts_with(prefix)
                && after.as_ref().is_none_or(|a| ns > a.as_str())
            {
                names.insert(ns.to_owned());
            }
        }
        let mut found = Vec::new();
        for ns in names {
            match self.snapshot(&ns, &Consistency::Strong).await {
                Ok(_) => {
                    found.push(ns);
                    if found.len() > page_size {
                        break;
                    }
                }
                Err(e) if e.status == 404 => {}
                Err(e) => return Err(e),
            }
        }
        let next = if found.len() > page_size {
            found.pop();
            Some(base64::prelude::BASE64_STANDARD.encode(found.last().expect("nonzero page")))
        } else {
            None
        };
        Ok(
            json!({"namespaces":found.iter().map(|ns|json!({"id":ns})).collect::<Vec<_>>(),"next_cursor":next}),
        )
    }
    /// Prefetch the manifest, WAL tail, and schema into the node cache.
    pub async fn warm(&self, ns: &str) -> Result<(), ApiError> {
        self.snapshot(ns, &Consistency::Strong).await?;
        Ok(())
    }
    /// Describe the indexed snapshot plan after validating the query.
    pub async fn explain(&self, ns: &str, request: QueryRequest) -> Result<Value, ApiError> {
        self.query(ns, request.clone()).await?;
        let (state, _) = self.snapshot(ns, &request.consistency).await?;
        Ok(json!({"plan_text":plan::explain(&state,&request,&self.0.config)}))
    }
    /// Compare ANN with exact search on one snapshot and optionally return ground-truth rows.
    pub async fn recall(&self, ns: &str, request: Value) -> Result<Value, ApiError> {
        let engine = self.clone();
        let ns = ns.to_owned();
        let context = read_cache::ReadContext::new(self.0.read_cache.clone());
        let permit = self
            .0
            .queries
            .clone()
            .acquire_owned()
            .await
            .map_err(segment::index_error)?;
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            runtime.block_on(read_cache::scope(
                context,
                query::recall(&engine, &ns, request),
            ))
        })
        .await
        .map_err(segment::index_error)?
    }
}
fn storage_error(e: bf_storage::Error) -> ApiError {
    tracing::error!(error=?e,"storage operation failed");
    match e {
        e @ bf_storage::Error::BucketFormat { .. } => ApiError::new(500, e.to_string()),
        bf_storage::Error::ReadOnly => ApiError::new(403, "server is read-only"),
        _ => ApiError::new(503, "object storage is unavailable"),
    }
}
fn log_error(e: bf_wal::Error) -> ApiError {
    tracing::error!(error=?e,"WAL operation failed");
    match e {
        bf_wal::Error::Manifest(_) => ApiError::new(
            500,
            "unsupported or corrupt manifest; upgrade blowfish if the bucket uses a newer format",
        ),
        bf_wal::Error::Storage(e) => storage_error(e),
        bf_wal::Error::Api(e) => e,
        _ => ApiError::new(500, "stored WAL is corrupt or unsupported"),
    }
}
fn record_stage(stage: &'static str, since: Instant) {
    metrics::histogram!("blowfish_commit_stage_seconds", "stage" => stage)
        .record(since.elapsed().as_secs_f64());
}
async fn catch_up(wal: &Wal, state: &mut NamespaceState, store: &Storage) -> Result<(), ApiError> {
    snapshot::catch_up(wal, state, None, store).await
}
/// The original one-group-at-a-time commit: validate, append, and replay under the state lock.
/// Deletes, copies, and filter writes use it, after the pipeline has drained.
async fn commit_sync(
    wal: &Wal,
    state: &Published<NamespaceState>,
    store: &Storage,
    config: &EngineConfig,
    deleting: bool,
    pending: &[Op],
    backpressure: &auto_recluster::LoadBackpressure,
) -> Result<Vec<Result<WriteResponse, ApiError>>, ApiError> {
    let mut current = state.lock().await;
    // A state this node just committed or refreshed is current unless another writer
    // has since appended. The conditional create of the next WAL slot detects exactly
    // that (Conflict), and the retry then catches up, so the common single-writer
    // bulk-load path skips a LIST round trip before every commit.
    let mut trusted =
        current.seq > 0 && current.refreshed.elapsed() < std::time::Duration::from_secs(2);
    let committed = loop {
        let stage = Instant::now();
        if !std::mem::take(&mut trusted)
            && let Err(e) = catch_up(wal, &mut current, store).await
        {
            break Err(e);
        }
        record_stage("catch_up", stage);
        if deleting {
            if !current.exists {
                break Err(ApiError::not_found(
                    wal.key(0).split('/').nth(1).unwrap_or("unknown"),
                ));
            }
            let mut entry = WalEntry::new(vec![]);
            entry.tombstone = true;
            match wal.append(current.seq + 1, &entry).await {
                Ok(CreateResult::Conflict) => continue,
                Ok(_) => {
                    let seq = current.seq + 1;
                    current.replay(seq, &entry);
                    if let Err(e) = wal.cleanup_deleted(seq).await {
                        tracing::warn!(error=?e,"namespace cleanup deferred");
                    }
                    break Ok(vec![Ok(WriteResponse::default())]);
                }
                Err(e) => break Err(log_error(e)),
            }
        }
        if current.tail_bytes >= config.max_unindexed_bytes {
            let retry_ms = drain::retry_after_ms(current.tail_bytes);
            backpressure.record(current.created_at, retry_ms);
            break Err(
                ApiError::new(429, "unindexed WAL exceeds backpressure limit")
                    .with_retry_after_ms(retry_ms),
            );
        }
        let stage = Instant::now();
        let mut working = match snapshot::for_writes(store, &current, pending, &[]).await {
            Ok(s) => s,
            Err(e) => break Err(e),
        };
        record_stage("for_writes", stage);
        let stage = Instant::now();
        let mut batches = vec![];
        let mut results = vec![];
        for p in pending {
            let Operation::Write(req, copy, _activity) = &p.request else {
                unreachable!()
            };
            match write::prepare(&working, req, copy.as_deref()) {
                Ok((batch, result)) => {
                    working.apply(&batch, 0);
                    if let Some(source) = copy {
                        branch::install_source(&mut working, source);
                    }
                    batches.push(batch);
                    results.push(Ok(result));
                }
                Err(e) => results.push(Err(e)),
            }
        }
        record_stage("prepare", stage);
        if batches.is_empty() {
            break Ok(results);
        }
        let mut entry = WalEntry::new(batches);
        if let Operation::Write(_, Some(source), _activity) = &pending[0].request
            && let Err(e) = branch::stage(store, wal, &mut entry, source).await
        {
            break Err(e);
        }
        let stage = Instant::now();
        let appended = wal.append(current.seq + 1, &entry).await;
        record_stage("append", stage);
        match appended {
            Ok(CreateResult::Conflict) => continue,
            Ok(_) => {
                let seq = current.seq + 1;
                let stage = Instant::now();
                if let Err(e) = branch::replay(store, wal, seq, &entry, &mut current).await {
                    break Err(e);
                }
                record_stage("replay", stage);
                current.refreshed = Instant::now();
                break Ok(results);
            }
            Err(e) => break Err(log_error(e)),
        }
    };
    drop(current);
    committed
}
type Op = Pending<Operation, Result<WriteResponse, ApiError>>;
type WriteResult = Result<WriteResponse, ApiError>;

/// A group whose payload is uploading (or uploaded) and whose slot is not yet committed.
struct Flight {
    seq: u64,
    entry: Arc<WalEntry>,
    upload: tokio::task::JoinHandle<Result<bf_wal::Staged, bf_wal::Error>>,
    pending: Vec<Op>,
    results: Vec<WriteResult>,
    start: Instant,
}

/// Operations that cannot be evaluated against in-flight groups: deletes, copies, and writes
/// that scan the whole namespace.
fn is_barrier(op: &Operation) -> bool {
    match op {
        Operation::Delete(_activity) | Operation::Write(_, Some(_), _activity) => true,
        Operation::Write(req, None, _activity) => {
            req.delete_by_filter.is_some() || req.patch_by_filter.is_some()
        }
    }
}

/// Commit the oldest flight's slot (its upload has finished with `staged`) and reply to its
/// writers. A failure fails every later flight too (their payloads were staged for slots that
/// will not be used): after a lost race their writers are queued again in `backlog` after a
/// resync, and after a storage error they get the error. No writer is acknowledged before its
/// own slot commits, and slots commit strictly in order.
async fn commit_front(
    wal: &Wal,
    state: &Published<NamespaceState>,
    store: &Storage,
    flights: &mut VecDeque<Flight>,
    staged: Result<Result<bf_wal::Staged, bf_wal::Error>, tokio::task::JoinError>,
    backlog: &mut VecDeque<Op>,
) {
    let Some(front) = flights.pop_front() else {
        return;
    };
    let staged = match staged {
        Ok(Ok(staged)) => Ok(staged),
        Ok(Err(e)) => Err(log_error(e)),
        Err(e) => Err(segment::index_error(e)),
    };
    let mut retry = false;
    let failure = match staged {
        Err(e) => {
            // A failed stage can have an ambiguous PUT outcome. Its task has finished, so
            // attempt cleanup even though it could not return a Staged value.
            let _ = wal
                .discard(front.seq, &front.entry, &bf_wal::Staged::maybe_uploaded())
                .await;
            Some(e)
        }
        Ok(staged) => {
            let mut current = state.lock().await;
            if current.seq + 1 != front.seq {
                // The state moved (a catch-up replayed someone else's slot): resync and redo.
                retry = true;
                let _ = wal.discard(front.seq, &front.entry, &staged).await;
                Some(ApiError::new(503, "WAL position changed"))
            } else {
                let stage = Instant::now();
                let committed = wal.commit(front.seq, &front.entry, &staged).await;
                bf_wal::record_commit(&committed, front.start);
                match committed {
                    Ok(CreateResult::Conflict) => {
                        retry = true;
                        Some(ApiError::new(503, "WAL slot lost"))
                    }
                    Ok(_) => {
                        record_stage("append", stage);
                        let stage = Instant::now();
                        match branch::replay(store, wal, front.seq, &front.entry, &mut current)
                            .await
                        {
                            Ok(()) => {
                                record_stage("replay", stage);
                                current.refreshed = Instant::now();
                                drop(current);
                                for (p, result) in front.pending.into_iter().zip(front.results) {
                                    let result = result.map(|mut r| {
                                        r.performance.server_total_ms =
                                            front.start.elapsed().as_millis() as u64;
                                        r
                                    });
                                    let _ = p.reply.send(result);
                                }
                                return;
                            }
                            Err(e) => {
                                // Committed but not applied locally: resync from the log.
                                current.refreshed = Instant::now()
                                    .checked_sub(std::time::Duration::from_secs(60))
                                    .unwrap_or(current.refreshed);
                                Some(e)
                            }
                        }
                    }
                    Err(e) => {
                        let _ = wal.discard(front.seq, &front.entry, &staged).await;
                        Some(log_error(e))
                    }
                }
            }
        }
    };
    let error = failure.expect("every path that did not return failed");
    {
        // Force a catch-up before the next group is evaluated.
        let mut current = state.lock().await;
        current.refreshed = Instant::now()
            .checked_sub(std::time::Duration::from_secs(60))
            .unwrap_or(current.refreshed);
    }
    let mut failed = vec![front];
    failed.extend(flights.drain(..));
    for (i, f) in failed.into_iter().enumerate().rev() {
        if i > 0 {
            // Later flights never reached their slots. Let each upload finish before deleting:
            // aborting its task does not stop a running poll or a remote PUT, which could land
            // after the delete. Cleanup owns the handle, independently of the failed writers.
            let (wal, seq, entry, upload) = (wal.clone(), f.seq, f.entry, f.upload);
            tokio::spawn(async move {
                let _ = upload.await;
                let staged = bf_wal::Staged::maybe_uploaded();
                if let Err(error) = wal.discard(seq, &entry, &staged).await {
                    tracing::warn!(%error, seq, "abandoned WAL payload cleanup deferred to GC");
                }
            });
        }
        if retry {
            for p in f.pending.into_iter().rev() {
                backlog.push_front(p);
            }
        } else {
            for p in f.pending {
                let _ = p.reply.send(Err(error.clone()));
            }
        }
    }
}

/// Commit every flight, oldest first.
async fn drain(
    wal: &Wal,
    state: &Published<NamespaceState>,
    store: &Storage,
    flights: &mut VecDeque<Flight>,
    backlog: &mut VecDeque<Op>,
) {
    while let Some(front) = flights.front_mut() {
        let staged = (&mut front.upload).await;
        commit_front(wal, state, store, flights, staged, backlog).await;
    }
}

async fn commit_worker(
    wal: Wal,
    state: Arc<Published<NamespaceState>>,
    mut receiver: tokio::sync::mpsc::Receiver<Op>,
    store: Storage,
    config: EngineConfig,
    backpressure: Arc<auto_recluster::LoadBackpressure>,
) {
    let depth = config.wal_pipeline.max(1);
    let mut backlog: VecDeque<Op> = VecDeque::new();
    let mut flights: VecDeque<Flight> = VecDeque::new();
    loop {
        let first = if let Some(p) = backlog.pop_front() {
            p
        } else if flights.is_empty() {
            let Some(p) = receiver.recv().await else {
                break;
            };
            p
        } else {
            // Commit the oldest slot as soon as its upload lands; otherwise take new work.
            let full = flights.len() >= depth;
            let front = flights.front_mut().expect("non-empty");
            let event = tokio::select! {
                biased;
                staged = &mut front.upload => Err(staged),
                p = receiver.recv(), if !full => Ok(p),
            };
            match event {
                Err(staged) => {
                    commit_front(&wal, &state, &store, &mut flights, staged, &mut backlog).await;
                    continue;
                }
                Ok(Some(p)) => p,
                Ok(None) => {
                    drain(&wal, &state, &store, &mut flights, &mut backlog).await;
                    if backlog.is_empty() {
                        break;
                    }
                    continue;
                }
            }
        };
        let start = Instant::now();
        if is_barrier(&first.request) {
            drain(&wal, &state, &store, &mut flights, &mut backlog).await;
            if !backlog.is_empty() {
                backlog.push_back(first);
                continue;
            }
            let deleting = matches!(first.request, Operation::Delete(_));
            let pending = vec![first];
            let committed = commit_sync(
                &wal,
                &state,
                &store,
                &config,
                deleting,
                &pending,
                &backpressure,
            )
            .await;
            reply_all(pending, committed, start);
            continue;
        }
        let mut pending = vec![first];
        tokio::task::yield_now().await;
        while pending.len() < 128 {
            let next = match backlog.pop_front() {
                Some(p) => p,
                None => match receiver.try_recv() {
                    Ok(p) => p,
                    Err(_) => break,
                },
            };
            if is_barrier(&next.request) {
                backlog.push_front(next);
                break;
            }
            pending.push(next);
        }
        let mut current = state.lock().await;
        if flights.is_empty() {
            // A state this node just committed or refreshed is current unless another writer
            // has since appended. The conditional create of the next WAL slot detects exactly
            // that (Conflict), and the retry then catches up, so the common single-writer
            // bulk-load path skips a LIST round trip before every commit.
            let trusted =
                current.seq > 0 && current.refreshed.elapsed() < std::time::Duration::from_secs(2);
            let stage = Instant::now();
            if !trusted && let Err(e) = catch_up(&wal, &mut current, &store).await {
                drop(current);
                reply_all(pending, Err(e), start);
                continue;
            }
            record_stage("catch_up", stage);
        }
        if current.tail_bytes >= config.max_unindexed_bytes {
            let retry_ms = drain::retry_after_ms(current.tail_bytes);
            backpressure.record(current.created_at, retry_ms);
            let e = ApiError::new(429, "unindexed WAL exceeds backpressure limit")
                .with_retry_after_ms(retry_ms);
            drop(current);
            reply_all(pending, Err(e), start);
            continue;
        }
        let overlay: Vec<Arc<WalEntry>> = flights.iter().map(|f| f.entry.clone()).collect();
        let stage = Instant::now();
        let mut working = match snapshot::for_writes(&store, &current, &pending, &overlay).await {
            Ok(s) => s,
            Err(e) => {
                drop(current);
                reply_all(pending, Err(e), start);
                continue;
            }
        };
        record_stage("for_writes", stage);
        let stage = Instant::now();
        let mut batches = vec![];
        let mut results = vec![];
        for p in &pending {
            let Operation::Write(req, None, _activity) = &p.request else {
                unreachable!("barriers never reach the pipeline")
            };
            match write::prepare(&working, req, None) {
                Ok((batch, result)) => {
                    working.apply(&batch, 0);
                    batches.push(batch);
                    results.push(Ok(result));
                }
                Err(e) => results.push(Err(e)),
            }
        }
        record_stage("prepare", stage);
        let seq = current.seq + flights.len() as u64 + 1;
        drop(current);
        if batches.is_empty() {
            reply_all(pending, Ok(results), start);
            continue;
        }
        let entry = Arc::new(WalEntry::new(batches));
        let upload = {
            let (wal, entry) = (wal.clone(), entry.clone());
            tokio::spawn(async move { wal.stage(seq, &entry).await })
        };
        flights.push_back(Flight {
            seq,
            entry,
            upload,
            pending,
            results,
            start,
        });
    }
}

fn reply_all(pending: Vec<Op>, committed: Result<Vec<WriteResult>, ApiError>, start: Instant) {
    match committed {
        Ok(results) => {
            for (p, result) in pending.into_iter().zip(results) {
                let result = result.map(|mut r| {
                    r.performance.server_total_ms = start.elapsed().as_millis() as u64;
                    r
                });
                let _ = p.reply.send(result);
            }
        }
        Err(e) => {
            for p in pending {
                let _ = p.reply.send(Err(e.clone()));
            }
        }
    }
}

#[cfg(test)]
mod tail_tests {
    use super::*;
    use bf_wal::{Manifest, WalEntry, WriteBatch};
    use serde_json::json;

    fn entry(upserts: &[u64], deletes: &[u64]) -> WalEntry {
        let schema: Schema =
            serde_json::from_value(json!({"id":{"type":"uint"},"n":{"type":"int"}})).unwrap();
        WalEntry::new(vec![WriteBatch {
            upserts: upserts
                .iter()
                .map(|id| {
                    serde_json::from_value(json!({"id":id,"n":*id as i64})).expect("document")
                })
                .collect(),
            deletes: deletes.iter().map(|id| Id::Uint(*id)).collect(),
            schema,
            distance_metric: DistanceMetric::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }])
    }

    fn manifest(through: u64) -> Manifest {
        Manifest {
            format_version: 1,
            schema: Schema::new(),
            distance_metric: DistanceMetric::default(),
            id_type: AttributeType::Scalar(ScalarType::Uint),
            created_at: Utc::now(),
            indexed_through: through,
            indexer_epoch: 1,
            deleted: false,
            segments: vec![],
            stats: Default::default(),
            text_stats: None,
            ann_dirs: Default::default(),
            ann_tiers: Default::default(),
            lineage: Default::default(),
            updated_at: None,
            retired: vec![],
            checkpoints: vec![],
            pruned_through: 0,
            bulk_load_at: None,
            root: false,
        }
    }

    /// Publishing a checkpoint drops exactly the tail IDs whose last write it indexed, and
    /// keeps IDs written again afterwards, without re-reading any entry.
    #[test]
    fn fast_forward_drops_only_indexed_tail_ids() {
        let mut state = NamespaceState::default();
        let entries = [
            entry(&[1, 2, 3], &[]),
            entry(&[4], &[2]),
            entry(&[3, 5], &[]),
            entry(&[6], &[1]),
        ];
        for (i, e) in entries.iter().enumerate() {
            state.replay(i as u64 + 1, e);
        }
        let last_two: u64 = state.entries[2..].iter().map(|e| e.bytes).sum();
        // The indexer saw entries 1 and 2; entries 3 and 4 arrived while it built.
        assert!(state.fast_forward(&manifest(2), 7, 2));
        assert_eq!(state.indexed_through, 2);
        assert_eq!(state.manifest_seq, 7);
        assert_eq!(state.seq, 4);
        let docs: Vec<_> = state.docs.keys().cloned().collect();
        assert_eq!(docs, vec![Id::Uint(3), Id::Uint(5), Id::Uint(6)]);
        let shadowed: Vec<_> = state.shadowed.iter().cloned().collect();
        assert_eq!(
            shadowed,
            vec![Id::Uint(1), Id::Uint(3), Id::Uint(5), Id::Uint(6)]
        );
        assert_eq!(state.tail_bytes, last_two);
        assert_eq!(state.entries.len(), 2);
        // A second publication of an older checkpoint is refused.
        assert!(!state.fast_forward(&manifest(1), 8, 1));
        // Indexing everything leaves an empty tail.
        assert!(state.fast_forward(&manifest(4), 9, 4));
        assert!(state.docs.is_empty() && state.shadowed.is_empty() && state.entries.is_empty());
        assert_eq!(state.tail_bytes, 0);
        assert!(state.tail_since.is_none());
    }

    #[test]
    fn maintenance_adoption_preserves_arcs_and_rejects_stale_generations_and_copies() {
        let mut state = NamespaceState::default();
        state.replay(1, &entry(&[1], &[]));
        assert!(state.fast_forward(&manifest(1), 2, 1));
        state.replay(2, &entry(&[1, 2], &[]));
        let arc = state.docs[&Id::Uint(1)].clone();
        let bytes = state.tail_bytes;
        let mut newer = manifest(1);
        newer.root = true;
        assert!(state.fast_forward(&newer, 3, 1));
        assert!(Arc::ptr_eq(&arc, &state.docs[&Id::Uint(1)]));
        assert_eq!(state.tail_bytes, bytes);
        assert_eq!(state.seq, 2);
        assert_eq!(state.shadowed.len(), 2);
        assert!(state.root_only);
        assert!(
            !state.fast_forward(&manifest(1), 2, 1),
            "never regress to an older publication"
        );
        assert_eq!(state.manifest_seq, 3);
        state.entries.last_mut().unwrap().copy = true;
        assert!(
            !state.fast_forward(&manifest(1), 4, 1),
            "copy overlays need descriptor replay"
        );
        state.entries.last_mut().unwrap().copy = false;
        state.entries[0].seq = 3;
        assert!(
            !state.fast_forward(&manifest(1), 4, 1),
            "a missing tail prefix cannot be reused"
        );
    }

    #[tokio::test]
    async fn changed_checkpoint_files_retain_a_known_tail_but_respect_eventual_caps() {
        let store = Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let engine = Engine::with_config(
            store.clone(),
            EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                ..Default::default()
            },
        );
        engine.write("adopt",serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[{"id":1,"pad":"base"}]})).unwrap()).await.unwrap();
        engine.index_namespace("adopt", true).await.unwrap();
        engine
            .write(
                "adopt",
                serde_json::from_value(
                    json!({"upsert_rows":[{"id":1,"pad":"x".repeat(70000)},{"id":2,"pad":"new"}]}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let handle = engine.handle("adopt").await.unwrap();
        let before = handle.state.lock().await.clone();
        let wal = Wal::new(store.clone(), "adopt").unwrap();
        let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
        let file = m.segments[0].files.get_mut("docs").unwrap();
        let bytes = store.get(&file.key).await.unwrap();
        file.key = "ns/adopt/maintenance-docs.bfc".into();
        store.create_unique(&file.key, bytes).await.unwrap();
        wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
        let mut uncapped = before.clone();
        snapshot::catch_up(&wal, &mut uncapped, None, &store)
            .await
            .unwrap();
        for id in [Id::Uint(1), Id::Uint(2)] {
            assert!(Arc::ptr_eq(&before.docs[&id], &uncapped.docs[&id]));
        }
        assert_eq!(uncapped.entries.len(), before.entries.len());
        assert_eq!(uncapped.tail_bytes, before.tail_bytes);
        assert_eq!(
            uncapped.segments[0].files["docs"].key,
            "ns/adopt/maintenance-docs.bfc"
        );
        let mut capped = before;
        snapshot::catch_up(&wal, &mut capped, Some(1), &store)
            .await
            .unwrap();
        assert!(capped.docs.is_empty());
        assert_eq!(capped.seq, m.indexed_through);
        let rows = engine
            .query(
                "adopt",
                serde_json::from_value(
                    json!({"rank_by":["id","asc"],"top_k":10,"include_attributes":true}),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .result
            .rows
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["pad"], "x".repeat(70000));
    }
}

#[cfg(test)]
mod recall_sampler_tests {
    use super::{RECALL_SAMPLE_DUTY, RecallSampler};
    use std::time::{Duration, Instant};

    #[test]
    fn one_comparison_at_a_time_and_one_per_interval() {
        let sampler = RecallSampler::default();
        let first = sampler.start(Duration::ZERO).expect("first sample starts");
        assert!(
            sampler.start(Duration::ZERO).is_none(),
            "one runs at a time"
        );
        drop(first);
        // Past the few microseconds of pacing the instant comparison earned.
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            sampler.start(Duration::ZERO).is_some(),
            "free again once it ends"
        );
        let paced = RecallSampler::default();
        drop(
            paced
                .start(Duration::from_secs(3600))
                .expect("first sample starts"),
        );
        assert!(
            paced.start(Duration::from_secs(3600)).is_none(),
            "within the interval"
        );
    }

    #[test]
    fn the_first_comparison_waits_an_interval_and_long_ones_wait_longer() {
        // A restart does not compare at once.
        assert!(
            RecallSampler::new(Duration::from_secs(3600))
                .start(Duration::ZERO)
                .is_none()
        );
        // A comparison that took about 20 ms holds off the next for 50 times as long, past the
        // interval. Sleeps can overrun on a busy machine, so wait out a measured upper bound.
        let sampler = RecallSampler::new(Duration::ZERO);
        let before = Instant::now();
        let sampling = sampler.start(Duration::ZERO).expect("starts");
        std::thread::sleep(Duration::from_millis(20));
        drop(sampling);
        let hold_off = before.elapsed() * RECALL_SAMPLE_DUTY;
        assert!(sampler.start(Duration::ZERO).is_none());
        std::thread::sleep(hold_off + Duration::from_millis(50));
        assert!(sampler.start(Duration::ZERO).is_some());
    }
}

#[cfg(test)]
mod text_benchmark;

#[cfg(test)]
mod filtered_recall_tests;
