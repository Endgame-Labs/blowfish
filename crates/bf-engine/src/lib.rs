//! Exhaustive reference engine. Object storage is authoritative; memory is a replay cache.
mod filter;
mod query;
pub mod text_fallback;
mod write;

use bf_storage::{CreateResult, Storage};
use bf_types::*;
use bf_wal::{GroupCommit, Manifest, Pending, Wal, WalEntry, WriteBatch};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
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
    pub docs: BTreeMap<Id, Document>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub refreshed: Instant,
}
impl Default for NamespaceState {
    fn default() -> Self {
        Self {
            seq: 0,
            exists: false,
            schema: Schema::new(),
            metric: DistanceMetric::default(),
            docs: BTreeMap::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            refreshed: Instant::now(),
        }
    }
}
impl NamespaceState {
    fn apply(&mut self, b: &WriteBatch) {
        self.exists = true;
        self.schema = b.schema.clone();
        self.metric = b.distance_metric;
        self.created_at = b.created_at;
        self.updated_at = b.updated_at;
        for id in &b.deletes {
            self.docs.remove(id);
        }
        for doc in &b.upserts {
            let id = serde_json::from_value(doc["id"].clone()).expect("validated ID");
            self.docs.insert(id, doc.clone());
        }
    }
    fn replay(&mut self, seq: u64, entry: &WalEntry) {
        if entry.tombstone {
            *self = Self::default();
        }
        for batch in &entry.batches {
            self.apply(batch);
        }
        self.seq = seq;
    }
}
struct Handle {
    state: Arc<Mutex<NamespaceState>>,
    commits: GroupCommit<Operation, Result<WriteResponse, ApiError>>,
}
struct Inner {
    store: Storage,
    namespaces: Mutex<HashMap<String, Arc<Handle>>>,
}
/// Cloneable namespace engine. Separate instances safely share a CAS-capable bucket.
#[derive(Clone)]
pub struct Engine(Arc<Inner>);
enum Operation {
    Write(Box<WriteRequest>, Option<Box<NamespaceState>>),
    Delete,
}
impl Engine {
    /// Use an already-probed store. No writable local state is required.
    pub fn new(store: Storage) -> Self {
        Self(Arc::new(Inner {
            store,
            namespaces: Mutex::new(HashMap::new()),
        }))
    }
    /// Access the underlying store for operational inspection.
    pub fn storage(&self) -> &Storage {
        &self.0.store
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
        tokio::spawn(commit_worker(wal, state, receiver));
        Ok(handle)
    }
    async fn snapshot(
        &self,
        ns: &str,
        consistency: &Consistency,
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
                state.seq == 0 || state.refreshed.elapsed().as_secs() >= *max_staleness_seconds
            }
        };
        if refresh {
            catch_up(
                &Wal::new(self.0.store.clone(), ns).map_err(log_error)?,
                &mut state,
            )
            .await?;
        }
        if !state.exists {
            return Err(ApiError::not_found(ns));
        }
        Ok((state.clone(), warm))
    }
    /// Atomically validate, group, and durably commit all operations in one request.
    pub async fn write(&self, ns: &str, request: WriteRequest) -> Result<WriteResponse, ApiError> {
        if self.0.store.is_read_only() {
            return Err(ApiError::new(403, "server is read-only"));
        }
        let handle = self.handle(ns).await?;
        let copy = if let Some(source) = &request.copy_from_namespace {
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
                self.snapshot(source, &Consistency::Strong).await?.0,
            ))
        } else {
            None
        };
        handle
            .commits
            .submit(Operation::Write(Box::new(request), copy))
            .await?
    }
    /// Query a consistent snapshot using exhaustive evaluation, including multi-query RRF.
    pub async fn query(&self, ns: &str, request: QueryRequest) -> Result<QueryResponse, ApiError> {
        let start = Instant::now();
        let (state, warm) = self.snapshot(ns, &request.consistency).await?;
        let execution = Instant::now();
        let mut response = query::execute(&state, &request)?;
        response.performance = QueryPerformance {
            cache_hit_ratio: if warm { 1.0 } else { 0.0 },
            cache_temperature: if warm { "hot" } else { "cold" }.into(),
            server_total_ms: start.elapsed().as_millis() as u64,
            query_execution_ms: execution.elapsed().as_millis() as u64,
            exhaustive_search_count: state.docs.len() as u64,
            approx_namespace_size: state.docs.len() as u64,
            last_included_write_at: Some(
                state
                    .updated_at
                    .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            ),
        };
        Ok(response)
    }
    /// Namespace metadata, with fully expanded schema settings.
    pub async fn metadata(&self, ns: &str) -> Result<Value, ApiError> {
        let (s, _) = self.snapshot(ns, &Consistency::Strong).await?;
        Ok(
            json!({"schema":s.schema,"approx_logical_bytes":logical_bytes(&s),"approx_row_count":s.docs.len(),"created_at":s.created_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),"updated_at":s.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),"last_write_at":s.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),"encryption":{"mode":"default"},"index":{"status":"up-to-date"}}),
        )
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
    /// Prefetch the WAL and schema into the node cache.
    pub async fn warm(&self, ns: &str) -> Result<(), ApiError> {
        self.snapshot(ns, &Consistency::Strong).await?;
        Ok(())
    }
    /// Describe the exhaustive reference plan after validating the query.
    pub async fn explain(&self, ns: &str, request: QueryRequest) -> Result<Value, ApiError> {
        self.query(ns, request).await?;
        Ok(
            json!({"plan_text":"Object-store WAL replay → exhaustive namespace scan → filter → exact rank/aggregate → projection"}),
        )
    }
    /// Exact-search recall is one; optionally return the sampled ground-truth rows.
    pub async fn recall(&self, ns: &str, request: Value) -> Result<Value, ApiError> {
        query::recall(self, ns, request).await
    }
}
fn logical_bytes(s: &NamespaceState) -> usize {
    s.docs
        .values()
        .map(|d| serde_json::to_vec(d).expect("document serializes").len())
        .sum()
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
async fn catch_up(wal: &Wal, state: &mut NamespaceState) -> Result<(), ApiError> {
    for (seq, entry) in wal.tail(state.seq).await.map_err(log_error)? {
        state.replay(seq, &entry);
    }
    state.refreshed = Instant::now();
    Ok(())
}
async fn commit_worker(
    wal: Wal,
    state: Arc<Mutex<NamespaceState>>,
    mut receiver: tokio::sync::mpsc::Receiver<Pending<Operation, Result<WriteResponse, ApiError>>>,
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
        let mut pending = vec![first];
        if !deleting {
            tokio::task::yield_now().await;
            while pending.len() < 128 {
                match receiver.try_recv() {
                    Ok(p) if matches!(p.request, Operation::Delete) => {
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
        let committed = loop {
            if let Err(e) = catch_up(&wal, &mut current).await {
                break Err(e);
            }
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
            let mut working = current.clone();
            let mut batches = vec![];
            let mut results = vec![];
            for p in &pending {
                let Operation::Write(req, copy) = &p.request else {
                    unreachable!()
                };
                match write::prepare(&working, req, copy.as_deref()) {
                    Ok((batch, result)) => {
                        working.apply(&batch);
                        batches.push(batch);
                        results.push(Ok(result));
                    }
                    Err(e) => results.push(Err(e)),
                }
            }
            if batches.is_empty() {
                break Ok(results);
            }
            let entry = WalEntry::new(batches);
            match wal.append(current.seq + 1, &entry).await {
                Ok(CreateResult::Conflict) => continue,
                Ok(_) => {
                    let seq = current.seq + 1;
                    current.replay(seq, &entry);
                    current.refreshed = Instant::now();
                    if let Some(id_type) =
                        current.schema.get("id").and_then(|s| s.data_type.clone())
                    {
                        let manifest = Manifest {
                            format_version: 1,
                            schema: current.schema.clone(),
                            distance_metric: current.metric,
                            id_type,
                            created_at: current.created_at,
                            indexed_through: 0,
                            indexer_epoch: 0,
                            segments: vec![],
                        };
                        if let Err(e) = wal.publish_manifest(seq, &manifest).await {
                            tracing::warn!(error=?e,"manifest publication deferred; WAL remains authoritative");
                        }
                    }
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
