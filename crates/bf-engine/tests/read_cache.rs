use async_trait::async_trait;
use bf_engine::{Engine, EngineConfig};
use bf_storage::{CacheConfig, Storage};
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult, path::Path,
};
use serde_json::json;
use std::{
    fmt,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Debug, Default)]
struct Counting {
    inner: object_store::memory::InMemory,
    ranges: Mutex<Vec<Range<u64>>>,
    waves: AtomicUsize,
    heads: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    delay: AtomicBool,
    fail: AtomicBool,
    short: AtomicBool,
    pause_head: AtomicBool,
    head_started: tokio::sync::Notify,
    resume_head: tokio::sync::Notify,
}
impl fmt::Display for Counting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("counting store")
    }
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl ObjectStore for Counting {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> StoreResult<GetResult> {
        if opts.head
            && path.as_ref().contains("/wal/")
            && self.pause_head.swap(false, Ordering::SeqCst)
        {
            self.head_started.notify_one();
            self.resume_head.notified().await;
        }
        if !path.as_ref().contains("/segments/") {
            return self.inner.get_opts(path, opts).await;
        }
        if opts.head {
            self.heads.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(object_store::GetRange::Bounded(range)) = &opts.range {
            self.ranges.lock().unwrap().push(range.clone());
        }
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        if active == 1 {
            self.waves.fetch_add(1, Ordering::SeqCst);
        }
        let _active = Active(&self.active);
        if self.delay.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "counting",
                source: "injected failure".into(),
            });
        }
        let head = opts.head;
        let mut result = self.inner.get_opts(path, opts).await?;
        if !head && self.short.swap(false, Ordering::SeqCst) {
            result.payload = object_store::GetResultPayload::Stream(
                futures::stream::once(async { Ok(Bytes::new()) }).boxed(),
            );
        }
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn config() -> EngineConfig {
    EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        ..Default::default()
    }
}
async fn store(backend: Arc<Counting>) -> Storage {
    Storage::from_backend_with_cache(
        backend,
        Path::default(),
        true,
        false,
        CacheConfig {
            memory_bytes: 64 << 20,
            block_size: 1024,
            max_coalesce_blocks: 256,
            fetch_concurrency: 512,
            ..Default::default()
        },
    )
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_ann_has_three_read_stages_no_heads_and_warm_queries_hit_cache() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    let docs: Vec<_> = (0..5000)
        .map(|id| {
            let vector: Vec<_> = (0..64)
                .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
                .collect();
            json!({"id":id,"vector":vector,"text":format!("term{} search engine", id % 64)})
        })
        .collect();
    writer
        .write(
            "stages",
            serde_json::from_value(json!({
                "schema":{"text":{"type":"string","full_text_search":true}},
                "distance_metric":"cosine_distance","upsert_rows":docs
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    writer.index_namespace("stages", true).await.unwrap();
    let engine = Engine::with_config(store(backend.clone()).await, config());
    engine.schema("stages").await.unwrap();
    backend.waves.store(0, Ordering::SeqCst);
    backend.heads.store(0, Ordering::SeqCst);
    backend.ranges.lock().unwrap().clear();
    backend.delay.store(true, Ordering::SeqCst);
    let request =
        serde_json::from_value(json!({"rank_by":["vector","ANN",docs[0]["vector"]],"top_k":10}))
            .unwrap();
    let cold = engine.query("stages", request).await.unwrap();
    assert_eq!(cold.result.rows.as_ref().unwrap().len(), 10);
    assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
    assert_eq!(backend.waves.load(Ordering::SeqCst), 3);
    assert!(cold.performance.cache_hit_ratio < 1.0);
    let reads = backend.ranges.lock().unwrap().len();
    let request =
        serde_json::from_value(json!({"rank_by":["vector","ANN",docs[0]["vector"]],"top_k":10}))
            .unwrap();
    let (warm, diagnostics) = engine
        .query_with_diagnostics("stages", request)
        .await
        .unwrap();
    assert_eq!(diagnostics.backend.len(), 1, "{diagnostics:?}");
    assert_eq!(diagnostics.backend[0].operation, "head");
    assert_eq!(diagnostics.backend[0].requests, 1);
    assert_eq!(diagnostics.backend[0].bytes, 0);
    assert_eq!(cold.result.rows, warm.result.rows);
    assert_eq!(backend.ranges.lock().unwrap().len(), reads);
    assert_eq!(warm.performance.cache_hit_ratio, 1.0);
    assert_eq!(warm.performance.cache_temperature, "hot");
    let eventual = serde_json::from_value(json!({
        "rank_by":["vector","ANN",docs[0]["vector"]],"top_k":10,
        "consistency":{"level":"eventual"}
    }))
    .unwrap();
    let (response, diagnostics) = engine
        .query_with_diagnostics("stages", eventual)
        .await
        .unwrap();
    assert_eq!(warm.result.rows, response.result.rows);
    assert!(diagnostics.backend.is_empty(), "{diagnostics:?}");
    // A fresh engine and storage handle also keep the text metadata stage together.
    let engine = Engine::with_config(store(backend.clone()).await, config());
    backend.waves.store(0, Ordering::SeqCst);
    let request =
        serde_json::from_value(json!({"rank_by":["text","BM25","search engine term1"],"top_k":10}))
            .unwrap();
    let cold = engine.query("stages", request).await.unwrap();
    assert_eq!(cold.result.rows.as_ref().unwrap().len(), 10);
    assert!(backend.waves.load(Ordering::SeqCst) <= 3);
    let reads = backend.ranges.lock().unwrap().len();
    let request =
        serde_json::from_value(json!({"rank_by":["text","BM25","search engine term1"],"top_k":10}))
            .unwrap();
    let warm = engine.query("stages", request).await.unwrap();
    assert_eq!(cold.result.rows, warm.result.rows);
    assert_eq!(backend.ranges.lock().unwrap().len(), reads);
    assert_eq!(warm.performance.cache_hit_ratio, 1.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_wal_write_discards_speculative_results() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    writer
        .write(
            "race",
            serde_json::from_value(json!({"upsert_rows":[{"id":1,"n":1}]})).unwrap(),
        )
        .await
        .unwrap();
    writer.index_namespace("race", true).await.unwrap();
    let reader = Engine::with_config(store(backend.clone()).await, config());
    let request: bf_types::QueryRequest =
        serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":1,"include_attributes":true}))
            .unwrap();
    reader.query("race", request.clone()).await.unwrap();
    backend.pause_head.store(true, Ordering::SeqCst);
    let searching = reader.clone();
    let task = tokio::spawn(async move { searching.query("race", request).await });
    tokio::time::timeout(Duration::from_secs(10), backend.head_started.notified())
        .await
        .unwrap();
    writer
        .write(
            "race",
            serde_json::from_value(json!({"patch_rows":[{"id":1,"n":2}]})).unwrap(),
        )
        .await
        .unwrap();
    backend.resume_head.notify_one();
    let response = task.await.unwrap().unwrap();
    assert_eq!(response.result.rows.unwrap()[0]["n"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_strong_checks_do_not_extend_checkpoint_retention() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    writer
        .write(
            "expiry",
            serde_json::from_value(json!({"upsert_rows":[{"id":1}]})).unwrap(),
        )
        .await
        .unwrap();
    writer.index_namespace("expiry", true).await.unwrap();
    let reader = Engine::with_config(
        store(backend).await,
        EngineConfig {
            retention: Duration::from_millis(100),
            ..config()
        },
    );
    let request: bf_types::QueryRequest =
        serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":1})).unwrap();
    reader.query("expiry", request.clone()).await.unwrap();
    let mut refreshed = false;
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (_, measured) = reader
            .query_with_diagnostics("expiry", request.clone())
            .await
            .unwrap();
        refreshed |= measured
            .backend
            .iter()
            .any(|entry| entry.operation == "list");
    }
    assert!(refreshed, "WAL freshness must not reset checkpoint age");
}
