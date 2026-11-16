//! Immutable segment indexes with a WAL overlay and an exhaustive reference evaluator.
mod ann;
mod branch;
mod filter;
mod highlight;
mod indexed;
mod indexer;
mod pattern;
mod plan;
mod read_cache;
mod segment;
mod snapshot;
mod sparse;
mod streaming;
pub use indexer::EngineConfig;
mod metadata;
mod query;
mod text;
mod write;

use bf_storage::{CreateResult, Storage};
use bf_types::*;
use bf_wal::{GroupCommit, Pending, Wal, WalEntry, WriteBatch};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Instant,
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct NamespaceState {
    pub seq: u64,
    pub exists: bool,
    pub schema: Schema,
    pub metric: DistanceMetric,
    /// Only the unindexed tail stays resident. Materialized views are query-local.
    pub docs: BTreeMap<Id, Document>,
    pub shadowed: BTreeSet<Id>,
    pub segments: Vec<bf_wal::Segment>,
    pub indexed_through: u64,
    pub manifest_seq: u64,
    pub tail_bytes: u64,
    pub tail_since: Option<DateTime<Utc>>,
    pub stats: Arc<bf_wal::NamespaceStats>,
    /// Object holding the checkpoint's per-term text statistics (loaded only for BM25 queries
    /// and by the indexer), when the manifest keeps them out of line.
    pub text_stats: Option<bf_wal::SegmentFile>,
    pub scores: BTreeMap<String, BTreeMap<Id, f64>>,
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
            segments: vec![],
            indexed_through: 0,
            manifest_seq: 0,
            tail_bytes: 0,
            tail_since: None,
            stats: Default::default(),
            text_stats: None,
            scores: BTreeMap::new(),
            corpus: Arc::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            refreshed: Instant::now(),
            checkpoint_refreshed: Instant::now(),
        }
    }
}
impl NamespaceState {
    fn apply(&mut self, b: &WriteBatch) {
        self.exists = true;
        if self.schema.keys().any(|name| !b.schema.contains_key(name)) {
            for doc in self.docs.values_mut() {
                doc.retain(|name, _| b.schema.contains_key(name));
            }
        }
        self.schema = b.schema.clone();
        self.metric = b.distance_metric;
        self.created_at = b.created_at;
        self.updated_at = b.updated_at;
        for id in &b.deletes {
            self.docs.remove(id);
            self.shadowed.insert(id.clone());
        }
        for doc in &b.upserts {
            let id: Id = serde_json::from_value(doc["id"].clone()).expect("validated ID");
            self.shadowed.insert(id.clone());
            self.docs.insert(id, doc.clone());
        }
    }
    fn replay(&mut self, seq: u64, entry: &WalEntry) {
        if entry.tombstone {
            let manifest_seq = self.manifest_seq;
            *self = Self::default();
            self.manifest_seq = manifest_seq;
        }
        for batch in &entry.batches {
            self.apply(batch);
        }
        self.tail_since.get_or_insert(entry.committed_at);
        self.seq = seq;
        self.tail_bytes += serde_json::to_vec(entry).expect("WAL serializes").len() as u64;
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
}
struct Handle {
    state: Arc<Mutex<NamespaceState>>,
    commits: GroupCommit<Operation, Result<WriteResponse, ApiError>>,
}
struct Inner {
    store: Storage,
    read_cache: Arc<read_cache::DecodedCache>,
    queries: Arc<tokio::sync::Semaphore>,
    config: EngineConfig,
    owner: uuid::Uuid,
    namespaces: Mutex<HashMap<String, Arc<Handle>>>,
}
/// Cloneable namespace engine. Separate instances safely share a CAS-capable bucket.
#[derive(Clone)]
pub struct Engine(Arc<Inner>);
enum Operation {
    Write(Box<WriteRequest>, Option<Box<branch::Source>>),
    Delete,
}
impl Engine {
    /// Use an already-probed store. No writable local state is required.
    pub fn new(store: Storage) -> Self {
        Self::with_config(store, EngineConfig::from_env())
    }
    /// Configure indexing, tail limits, retention, and ANN behavior explicitly.
    pub fn with_config(store: Storage, config: EngineConfig) -> Self {
        let engine = Self(Arc::new(Inner {
            read_cache: Arc::new(read_cache::DecodedCache::new(
                config.decoded_cache_bytes,
                config.search_concurrency,
            )),
            queries: Arc::new(tokio::sync::Semaphore::new(config.query_concurrency.max(1))),
            store,
            config,
            owner: uuid::Uuid::new_v4(),
            namespaces: Mutex::new(HashMap::new()),
        }));
        indexer::start(&engine);
        engine
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
        let mut handles = self.0.namespaces.lock().await;
        if let Some(handle) = handles.get(ns) {
            return Ok(handle.clone());
        }
        let (commits, receiver) = GroupCommit::channel(1024);
        let state = Arc::new(Mutex::new(NamespaceState::default()));
        let wal = Wal::new(self.0.store.clone(), ns).map_err(log_error)?;
        let handle = Arc::new(Handle {
            state: state.clone(),
            commits,
        });
        handles.insert(ns.into(), handle.clone());
        let context = read_cache::ReadContext {
            cache: self.0.read_cache.clone(),
            stats: Default::default(),
            backend: Default::default(),
        };
        tokio::spawn(read_cache::scope(
            context,
            commit_worker(
                wal,
                state,
                receiver,
                self.0.store.clone(),
                self.0.config.clone(),
            ),
        ));
        Ok(handle)
    }
    async fn snapshot(
        &self,
        ns: &str,
        consistency: &Consistency,
    ) -> Result<(NamespaceState, bool), ApiError> {
        self.snapshot_hinted(ns, consistency, None).await
    }
    /// A snapshot that, when it has to read a newer checkpoint, opens the files the query
    /// will need while the WAL tail is being read.
    async fn snapshot_hinted(
        &self,
        ns: &str,
        consistency: &Consistency,
        hint: Option<snapshot::OpenHint>,
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
            && state.checkpoint_refreshed.elapsed() < self.0.config.retention.div_f64(2.0);
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
    /// Atomically validate, group, and durably commit all operations in one request.
    pub async fn write(&self, ns: &str, request: WriteRequest) -> Result<WriteResponse, ApiError> {
        if self.0.store.is_read_only() {
            return Err(ApiError::new(403, "server is read-only"));
        }
        self.check_writable(ns).await?;
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
            .submit(Operation::Write(Box::new(request), copy))
            .await??;
        if inherited_read_only {
            self.patch_metadata(ns, json!({"read_only":true})).await?;
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
        let start = Instant::now();
        let backend = Arc::new(bf_storage::BackendStats::default());
        let stats = Arc::new(bf_storage::CacheStats::default());
        let context = read_cache::ReadContext {
            cache: self.0.read_cache.clone(),
            stats: stats.clone(),
            backend: backend.clone(),
        };
        let cached = if matches!(request.consistency, Consistency::Strong) {
            let handle = self.handle(ns).await?;
            let state = handle.state.lock().await;
            (state.exists
                && state.seq > 0
                && state.checkpoint_refreshed.elapsed() < self.0.config.retention.div_f64(2.0))
            .then(|| state.clone())
        } else {
            None
        };
        let hint = snapshot::OpenHint::for_request(&request, context.clone());
        let snapshot = async {
            let result = backend
                .scope(
                    "snapshot",
                    self.snapshot_hinted(ns, &request.consistency, Some(hint)),
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
                    self.search_snapshot(&cached, &request, context.clone())
                );
                let (state, _) = snapshot?;
                let (response, execution) =
                    if state.seq == cached.seq && state.manifest_seq == cached.manifest_seq {
                        searched?
                    } else {
                        self.search_snapshot(&state, &request, context).await?
                    };
                (state, response, execution, elapsed)
            } else {
                let (snapshot, elapsed) = snapshot.await;
                let (state, _) = snapshot?;
                let (response, execution) = self.search_snapshot(&state, &request, context).await?;
                (state, response, execution, elapsed)
            };
        if matches!(request.rank_by, Some(RankBy::Vector { exact: false, .. }))
            && (uuid::Uuid::new_v4().as_u128() % 1_000_000) as f64 / 1_000_000.0
                < self.0.config.recall_sample_rate
        {
            let store = self.0.store.clone();
            let config = self.0.config.clone();
            let sample = state.clone();
            let ann = response.clone();
            let cache = self.0.read_cache.clone();
            let queries = self.0.queries.clone();
            tokio::spawn(async move {
                let Ok(permit) = queries.acquire_owned().await else {
                    return;
                };
                let runtime = tokio::runtime::Handle::current();
                let context = read_cache::ReadContext {
                    cache,
                    stats: Arc::default(),
                    backend: Arc::default(),
                };
                let _ = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    runtime.block_on(read_cache::scope(context, async move {
                        if let Ok(exact) =
                            indexed::execute(&store, &sample, &request, &config, true).await
                        {
                            metrics::histogram!("blowfish_ann_recall")
                                .record(indexed::recall(&ann, &exact));
                        }
                    }));
                })
                .await;
            });
        }
        response.performance = QueryPerformance {
            cache_hit_ratio: stats.cache_hit_ratio(),
            cache_temperature: stats.temperature().into(),
            server_total_ms: start.elapsed().as_millis() as u64,
            query_execution_ms: execution_elapsed.as_millis() as u64,
            exhaustive_search_count: state.docs.len() as u64,
            approx_namespace_size: state.stats.rows + state.docs.len() as u64,
            last_included_write_at: Some(
                state
                    .updated_at
                    .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            ),
        };
        if std::env::var_os("BF_TRACE_STAGES").is_some() {
            eprintln!(
                "QUERY snapshot={}ms exec={}ms total={}ms",
                snapshot_elapsed.as_millis(),
                execution_elapsed.as_millis(),
                start.elapsed().as_millis()
            );
        }
        let diagnostics = QueryDiagnostics {
            snapshot_elapsed,
            execution_elapsed,
            backend: backend.snapshot(),
            cache: stats.snapshot(),
        };
        Ok((response, diagnostics))
    }
    async fn search_snapshot(
        &self,
        state: &NamespaceState,
        request: &QueryRequest,
        context: read_cache::ReadContext,
    ) -> Result<(QueryResponse, std::time::Duration), ApiError> {
        let start = Instant::now();
        let store = self.0.store.clone();
        let config = self.0.config.clone();
        let t_clone = Instant::now();
        let state = state.clone();
        if std::env::var_os("BF_TRACE_STAGES").is_some() {
            eprintln!("CLONE {}ms", t_clone.elapsed().as_millis());
        }
        let request = request.clone();
        let runtime = tokio::runtime::Handle::current();
        let permit = self
            .0
            .queries
            .clone()
            .acquire_owned()
            .await
            .map_err(segment::index_error)?;
        let response = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            runtime.block_on(read_cache::scope(
                context,
                indexed::execute(&store, &state, &request, &config, false),
            ))
        })
        .await
        .map_err(segment::index_error)??;
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
    /// Namespace metadata, with fully expanded schema settings.
    pub async fn metadata(&self, ns: &str) -> Result<Value, ApiError> {
        let (s, _) = self.snapshot(ns, &Consistency::Strong).await?;
        let totals = snapshot::statistics(&self.0.store, &s).await?;
        let mut metadata = json!({"schema":s.schema,"approx_logical_bytes":totals.logical_bytes,"approx_row_count":totals.rows,"created_at":s.created_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),"updated_at":s.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),"last_write_at":s.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),"encryption":{"mode":"default"},"index":{"status":"up-to-date"}});
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
        handle.commits.submit(Operation::Delete).await??;
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
        let context = read_cache::ReadContext {
            cache: self.0.read_cache.clone(),
            stats: Arc::default(),
            backend: Arc::default(),
        };
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
        bf_storage::Error::ReadOnly => ApiError::new(403, "server is read-only"),
        _ => ApiError::new(503, "object storage is unavailable"),
    }
}
fn log_error(e: bf_wal::Error) -> ApiError {
    tracing::error!(error=?e,"WAL operation failed");
    match e {
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
async fn commit_worker(
    wal: Wal,
    state: Arc<Mutex<NamespaceState>>,
    mut receiver: tokio::sync::mpsc::Receiver<Pending<Operation, Result<WriteResponse, ApiError>>>,
    store: Storage,
    config: EngineConfig,
) {
    let mut carry = None;
    loop {
        let first = if let Some(p) = carry.take() {
            p
        } else {
            let Some(p) = receiver.recv().await else {
                break;
            };
            p
        };
        let deleting = matches!(first.request, Operation::Delete);
        let barrier = deleting || matches!(first.request, Operation::Write(_, Some(_)));
        let mut pending = vec![first];
        if !barrier {
            tokio::task::yield_now().await;
            while pending.len() < 128 {
                match receiver.try_recv() {
                    Ok(p)
                        if matches!(
                            p.request,
                            Operation::Delete | Operation::Write(_, Some(_))
                        ) =>
                    {
                        carry = Some(p);
                        break;
                    }
                    Ok(p) => pending.push(p),
                    Err(_) => break,
                }
            }
        }
        let start = Instant::now();
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
                && let Err(e) = catch_up(&wal, &mut current, &store).await
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
                break Err(ApiError::new(
                    429,
                    "unindexed WAL exceeds backpressure limit",
                ));
            }
            let stage = Instant::now();
            let mut working = match snapshot::for_writes(&store, &current, &pending).await {
                Ok(s) => s,
                Err(e) => break Err(e),
            };
            record_stage("for_writes", stage);
            let stage = Instant::now();
            let mut batches = vec![];
            let mut results = vec![];
            for p in &pending {
                let Operation::Write(req, copy) = &p.request else {
                    unreachable!()
                };
                match write::prepare(&working, req, copy.as_deref()) {
                    Ok((batch, result)) => {
                        working.apply(&batch);
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
            if let Operation::Write(_, Some(source)) = &pending[0].request
                && let Err(e) = branch::stage(&store, &wal, &mut entry, source).await
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
                    if let Err(e) = branch::replay(&store, &wal, seq, &entry, &mut current).await {
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
}
