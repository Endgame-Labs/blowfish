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
    calls: AtomicUsize,
    ranges: Mutex<Vec<Range<u64>>>,
    paths: Mutex<Vec<(String, Option<Range<u64>>)>>,
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
    wal_active: AtomicUsize,
    wal_max_active: AtomicUsize,
    /// Downloads of WAL payload objects.
    payload_reads: AtomicUsize,
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
        self.calls.fetch_add(1, Ordering::SeqCst);
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !opts.head && path.as_ref().contains("/wal-data/") {
            self.payload_reads.fetch_add(1, Ordering::SeqCst);
        }
        if opts.head
            && path.as_ref().contains("/wal/")
            && self.pause_head.swap(false, Ordering::SeqCst)
        {
            self.head_started.notify_one();
            self.resume_head.notified().await;
        }
        if !path.as_ref().contains("/segments/") {
            if opts.head && path.as_ref().contains("/wal/") {
                let active = self.wal_active.fetch_add(1, Ordering::SeqCst) + 1;
                self.wal_max_active.fetch_max(active, Ordering::SeqCst);
                let _active = Active(&self.wal_active);
                if self.delay.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                return self.inner.get_opts(path, opts).await;
            }
            return self.inner.get_opts(path, opts).await;
        }
        if opts.head {
            self.heads.fetch_add(1, Ordering::SeqCst);
        }
        self.paths.lock().unwrap().push((
            path.to_string(),
            match &opts.range {
                Some(object_store::GetRange::Bounded(r)) => Some(r.clone()),
                _ => None,
            },
        ));
        if let Some(object_store::GetRange::Bounded(range)) = &opts.range {
            self.ranges.lock().unwrap().push(range.clone());
        }
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        if active == 1 {
            self.waves.fetch_add(1, Ordering::SeqCst);
        }
        self.paths.lock().unwrap().last_mut().unwrap().0 +=
            &format!(" w{}", self.waves.load(Ordering::SeqCst));
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
        self.calls.fetch_add(1, Ordering::SeqCst);
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
async fn cold_open_reads_the_directory_hint_alongside_the_manifest() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable.clone(), config());
    let docs: Vec<_> = (0..3000)
        .map(|id| {
            let vector: Vec<_> = (0..64)
                .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
                .collect();
            json!({"id":id,"vector":vector})
        })
        .collect();
    writer
        .write(
            "hinted",
            serde_json::from_value(json!({"distance_metric":"cosine_distance","upsert_rows":docs}))
                .unwrap(),
        )
        .await
        .unwrap();
    writer.index_namespace("hinted", true).await.unwrap();
    let cold_waves = |backend: Arc<Counting>| async move {
        let engine = Engine::with_config(store(backend.clone()).await, config());
        backend.waves.store(0, Ordering::SeqCst);
        backend.delay.store(true, Ordering::SeqCst);
        let request =
            serde_json::from_value(json!({"rank_by":["vector","ANN",docs_vector()],"top_k":10}))
                .unwrap();
        let rows = engine.query("hinted", request).await.unwrap().result.rows;
        backend.delay.store(false, Ordering::SeqCst);
        (backend.waves.load(Ordering::SeqCst), rows)
    };
    fn docs_vector() -> Vec<f32> {
        (0..64)
            .map(|d| ((d * 104729) as f64).sin() as f32)
            .collect()
    }
    let (with_hint, rows) = cold_waves(backend.clone()).await;
    let hints: Vec<_> = writable
        .list("ns/hinted/segments/")
        .await
        .unwrap()
        .into_iter()
        .filter(|o| o.key.contains("ann-dir-latest"))
        .collect();
    assert_eq!(hints.len(), 1, "one hint per vector attribute");
    for hint in &hints {
        writable.delete(&hint.key).await.unwrap();
    }
    let (without_hint, rows_without) = cold_waves(backend.clone()).await;
    assert_eq!(rows, rows_without);
    assert!(
        with_hint < without_hint,
        "the hint saves a wave: {with_hint} against {without_hint}"
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_strong_queries_check_wal_concurrently() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    writer
        .write(
            "parallel",
            serde_json::from_value(json!({"upsert_rows":[{"id":1}]})).unwrap(),
        )
        .await
        .unwrap();
    writer.index_namespace("parallel", true).await.unwrap();
    let reader = Engine::with_config(store(backend.clone()).await, config());
    let request: bf_types::QueryRequest =
        serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":1})).unwrap();
    reader.query("parallel", request.clone()).await.unwrap();
    backend.delay.store(true, Ordering::SeqCst);
    backend.wal_max_active.store(0, Ordering::SeqCst);
    futures::future::try_join_all((0..8).map(|_| reader.query("parallel", request.clone())))
        .await
        .unwrap();
    assert!(
        backend.wal_max_active.load(Ordering::SeqCst) >= 2,
        "strong checks must not serialize under the namespace lock"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materializing_columns_fetches_bounded_block_batches_concurrently() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    writer.write("bulk", serde_json::from_value(json!({
        "distance_metric":"cosine_distance",
        "upsert_rows":(0..4096).map(|id|json!({"id":id,"vector":(0..64).map(|d| (id+d) as f32).collect::<Vec<_>>()})).collect::<Vec<_>>()
    })).unwrap()).await.unwrap();
    writer.index_namespace("bulk", true).await.unwrap();
    let cold = Engine::with_config(store(backend.clone()).await, config());
    backend.max_active.store(0, Ordering::SeqCst);
    backend.delay.store(true, Ordering::SeqCst);
    let result = cold
        .query_exhaustive(
            "bulk",
            serde_json::from_value(json!({
                "rank_by":["id","desc"],"top_k":3,"include_attributes":true
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result.result.rows.as_ref().unwrap()[0]["id"], 4095);
    assert_eq!(
        result.result.rows.as_ref().unwrap()[0]["vector"]
            .as_array()
            .unwrap()
            .len(),
        64
    );
    assert!(backend.max_active.load(Ordering::SeqCst) > 1);
}

#[tokio::test]
async fn idle_indexing_skips_io_but_force_compaction_and_remote_polling_still_work() {
    let backend = Arc::new(Counting::default());
    let storage = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let tuning = EngineConfig {
        indexing: false,
        index_rows: 1,
        index_age: Duration::from_secs(2),
        compaction_segments: 2,
        ..config()
    };
    let local = Engine::with_config(storage.clone(), tuning.clone());
    let remote = Engine::with_config(storage.clone(), tuning);
    local
        .write(
            "idle",
            serde_json::from_value(json!({"upsert_rows":[{"id":1,"n":1}]})).unwrap(),
        )
        .await
        .unwrap();
    local.index_namespace("idle", true).await.unwrap();
    let before = backend.calls.load(Ordering::SeqCst);
    assert!(!local.index_namespace("idle", false).await.unwrap());
    assert_eq!(backend.calls.load(Ordering::SeqCst), before);
    remote
        .write(
            "idle",
            serde_json::from_value(json!({"upsert_rows":[{"id":2,"n":2}]})).unwrap(),
        )
        .await
        .unwrap();
    let before = backend.calls.load(Ordering::SeqCst);
    assert!(!local.index_namespace("idle", false).await.unwrap());
    assert_eq!(backend.calls.load(Ordering::SeqCst), before);
    assert!(local.index_namespace("idle", true).await.unwrap());
    // Two fresh one-row segments still compact immediately, without waiting for age.
    assert!(local.index_namespace("idle", false).await.unwrap());
    let wal = bf_wal::Wal::new(storage, "idle").unwrap();
    assert_eq!(
        wal.latest_manifest()
            .await
            .unwrap()
            .unwrap()
            .1
            .segments
            .len(),
        1
    );
    remote
        .write(
            "idle",
            serde_json::from_value(json!({"upsert_rows":[{"id":3,"n":3}]})).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert!(local.index_namespace("idle", false).await.unwrap());
    assert_eq!(
        wal.latest_manifest().await.unwrap().unwrap().1.stats.rows,
        3
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_ann_over_twenty_segments_uses_the_directory() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    let vector = |id: usize| -> Vec<f32> {
        (0..128)
            .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
            .collect()
    };
    for batch in 0..20 {
        let docs: Vec<_> = (0..1200)
            .map(|n| {
                let id = batch * 1200 + n;
                json!({"id":id,"vector":vector(id),"label":format!("doc{id}")})
            })
            .collect();
        writer
            .write(
                "many",
                serde_json::from_value(json!({
                    "distance_metric":"euclidean_squared","upsert_rows":docs
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        writer.index_namespace("many", true).await.unwrap();
    }
    let segments = backend
        .inner
        .list(None)
        .filter(|o| {
            futures::future::ready(
                o.as_ref()
                    .is_ok_and(|o| o.location.as_ref().ends_with(".bfa")),
            )
        })
        .count()
        .await;
    assert!(segments >= 20, "expected 20 segments, found {segments}");
    let query = |ids_only: bool| -> bf_types::QueryRequest {
        let mut q = json!({"rank_by":["vector","ANN",vector(777)],"top_k":10});
        if !ids_only {
            q["include_attributes"] = json!(["label"]);
        }
        serde_json::from_value(q).unwrap()
    };
    for (ids_only, max_waves) in [(true, 3), (false, 4)] {
        let engine = Engine::with_config(store(backend.clone()).await, config());
        backend.waves.store(0, Ordering::SeqCst);
        backend.ranges.lock().unwrap().clear();
        backend.paths.lock().unwrap().clear();
        backend.delay.store(true, Ordering::SeqCst);
        let cold = engine.query("many", query(ids_only)).await.unwrap();
        backend.delay.store(false, Ordering::SeqCst);
        let rows = cold.result.rows.as_ref().unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0]["id"], 777);
        let waves = backend.waves.load(Ordering::SeqCst);
        let requests = backend.ranges.lock().unwrap().len();
        let metadata_reads = backend
            .paths
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, range)| path.ends_with(".bfa") && range.is_none())
            .count();
        eprintln!(
            "ids_only={ids_only} waves={waves} requests={requests} metadata={metadata_reads}"
        );
        if std::env::var_os("TRACE_PATHS").is_some() {
            for (path, range) in backend.paths.lock().unwrap().iter() {
                eprintln!("  {} {:?}", path.rsplit('/').next().unwrap(), range);
            }
        }
        assert!(waves <= max_waves, "{waves} waves");
        assert_eq!(metadata_reads, 0, "no ANN object is fetched whole");
        assert!(requests < 100, "{requests} requests");
        let exact = engine
            .query_exhaustive(
                "many",
                serde_json::from_value(json!({"rank_by":["vector","ANN",vector(777)],"top_k":10}))
                    .unwrap(),
            )
            .await
            .unwrap();
        let want: std::collections::BTreeSet<_> = exact
            .result
            .rows
            .unwrap()
            .iter()
            .map(|r| r["id"].clone().to_string())
            .collect();
        let got = rows
            .iter()
            .filter(|r| want.contains(&r["id"].to_string()))
            .count();
        assert!(got >= 9, "recall {got}/10");
    }
}

/// Cold ANN waves and requests, and the reads a publication needs, do not grow with the number
/// of segments (a 1M-equivalent namespace has dozens of them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_cost_and_publication_do_not_grow_with_segment_count() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    let vector = |id: usize| -> Vec<f32> {
        (0..64)
            .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
            .collect()
    };
    let query = || -> bf_types::QueryRequest {
        serde_json::from_value(json!({"rank_by":["vector","ANN",vector(77)],"top_k":10})).unwrap()
    };
    let mut cold = Vec::new();
    let mut publish_reads = Vec::new();
    for batch in 0..60usize {
        let docs: Vec<_> = (0..400)
            .map(|n| {
                let id = batch * 400 + n;
                json!({"id":id,"vector":vector(id)})
            })
            .collect();
        writer
            .write(
                "grow",
                serde_json::from_value(json!({
                    "distance_metric":"euclidean_squared","upsert_rows":docs
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        backend.paths.lock().unwrap().clear();
        writer.index_namespace("grow", true).await.unwrap();
        let reads: Vec<String> = backend
            .paths
            .lock()
            .unwrap()
            .iter()
            .map(|(p, _)| p.rsplit('/').next().unwrap().to_string())
            .collect();
        let count = |ext: &str| reads.iter().filter(|p| p.contains(ext)).count();
        // Publishing opens no existing segment's index, and reads only new fragments.
        publish_reads.push((batch, count(".bfa"), count(".bfd")));
        if batch + 1 == 6 || batch + 1 == 60 {
            let engine = Engine::with_config(store(backend.clone()).await, config());
            backend.waves.store(0, Ordering::SeqCst);
            backend.ranges.lock().unwrap().clear();
            backend.delay.store(true, Ordering::SeqCst);
            let result = engine.query("grow", query()).await.unwrap();
            backend.delay.store(false, Ordering::SeqCst);
            assert_eq!(result.result.rows.as_ref().unwrap()[0]["id"], 77);
            cold.push((
                batch + 1,
                backend.waves.load(Ordering::SeqCst),
                backend.ranges.lock().unwrap().len(),
            ));
        }
    }
    eprintln!("cold (segments, waves, requests): {cold:?}");
    eprintln!("publication reads (batch, bfa, bfd): {publish_reads:?}");
    assert!(cold.iter().all(|c| c.1 <= 3), "waves {cold:?}");
    // Requests follow the number of probed clusters, not the segment count: ten times the
    // segments (and rows) stays well under ten times the requests.
    assert!(cold[1].2 <= cold[0].2 * 6, "requests grew: {cold:?}");
    assert!(cold[1].2 < 100, "requests {cold:?}");
    for (batch, bfa, bfd) in &publish_reads {
        // Only the new object and the compacted object contribute resident metadata.
        assert!(*bfa <= 2, "batch {batch} read {bfa} ANN objects");
        assert!(*bfd <= 1, "batch {batch} read {bfd} directories");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtered_ann_uses_the_directory_without_opening_id_indexes() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    let vector = |id: usize| -> Vec<f32> {
        (0..64)
            .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
            .collect()
    };
    for batch in 0..12usize {
        let docs: Vec<_> = (0..400)
            .map(|n| {
                let id = batch * 400 + n;
                json!({"id":id,"vector":vector(id),"tag":(id % 50) as i64})
            })
            .collect();
        writer
            .write(
                "filt",
                serde_json::from_value(json!({
                    "schema":{"tag":{"type":"int","filterable":true}},
                    "distance_metric":"euclidean_squared","upsert_rows":docs
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        writer.index_namespace("filt", true).await.unwrap();
    }
    let distance = |id: usize| -> f32 {
        vector(id)
            .iter()
            .zip(vector(123))
            .map(|(a, b)| (a - b) * (a - b))
            .sum()
    };
    // (filter, whether the filtered set is small enough to need an exact answer)
    for (filter, tag_ok, exact) in [
        (
            json!(["tag", "Eq", 7]),
            (|t: usize| t == 7) as fn(usize) -> bool,
            true,
        ),
        (json!(["tag", "Lt", 25]), |t: usize| t < 25, false),
    ] {
        let mut expected: Vec<_> = (0..4800).filter(|id| tag_ok(id % 50)).collect();
        expected.sort_by(|a, b| distance(*a).total_cmp(&distance(*b)));
        expected.truncate(10);
        // The exact route fetches every allowed row; with a zero budget the second filter
        // goes through code scans of the nearest clusters instead.
        let engine = Engine::with_config(
            store(backend.clone()).await,
            EngineConfig {
                exact_fetch_rows: if exact { 8192 } else { 0 },
                ..config()
            },
        );
        backend.waves.store(0, Ordering::SeqCst);
        backend.paths.lock().unwrap().clear();
        backend.delay.store(true, Ordering::SeqCst);
        let request = serde_json::from_value(json!({
            "rank_by":["vector","ANN",vector(123)],"top_k":10,"filters":filter
        }))
        .unwrap();
        let result = engine.query("filt", request).await.unwrap();
        backend.delay.store(false, Ordering::SeqCst);
        let got: Vec<usize> = result
            .result
            .rows
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_u64().unwrap() as usize)
            .collect();
        assert!(got.iter().all(|id| tag_ok(id % 50)), "{got:?}");
        let hits = got.iter().filter(|id| expected.contains(id)).count();
        assert!(
            if exact { hits == 10 } else { hits >= 9 },
            "{filter}: {hits} of 10, got {got:?} want {expected:?}"
        );
        let reads: Vec<String> = backend
            .paths
            .lock()
            .unwrap()
            .iter()
            .map(|(p, _)| p.clone())
            .collect();
        assert!(
            !reads.iter().any(|p| p.contains(".bfi")),
            "filtered ANN read an ID index: {reads:?}"
        );
        assert!(backend.waves.load(Ordering::SeqCst) <= 5);
    }
}

/// An unindexed tail (new rows, overwrites of segment rows, and deletes) keeps the directory
/// path: no ID index is opened, shadowed rows are dropped by ID, and answers stay exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unindexed_tail_keeps_the_directory_path() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    let vector = |id: usize| -> Vec<f32> {
        (0..64)
            .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
            .collect()
    };
    for batch in 0..12usize {
        let docs: Vec<_> = (0..400)
            .map(|n| {
                let id = batch * 400 + n;
                json!({"id":id,"vector":vector(id)})
            })
            .collect();
        writer
            .write(
                "tail",
                serde_json::from_value(json!({
                    "distance_metric":"euclidean_squared","upsert_rows":docs
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        writer.index_namespace("tail", true).await.unwrap();
    }
    // The query's own nearest neighbours are deleted or overwritten in the tail, so shadowed
    // rows really do rank in the segments' top results.
    let target = vector(123);
    let distance =
        |v: &[f32]| -> f32 { v.iter().zip(&target).map(|(a, b)| (a - b) * (a - b)).sum() };
    let mut logical: std::collections::BTreeMap<usize, Vec<f32>> =
        (0..4800).map(|id| (id, vector(id))).collect();
    let mut by_distance: Vec<usize> = logical.keys().copied().collect();
    by_distance.sort_by(|a, b| distance(&logical[a]).total_cmp(&distance(&logical[b])));
    let doomed: Vec<usize> = by_distance[..3].to_vec();
    let moved: Vec<usize> = by_distance[3..6].to_vec();
    let mut tail_docs = Vec::new();
    for id in &moved {
        // Moved far away from the query.
        let far: Vec<f32> = vector(*id).iter().map(|x| x + 50.0).collect();
        logical.insert(*id, far.clone());
        tail_docs.push(json!({"id":id,"vector":far}));
    }
    for id in 5000..5050usize {
        logical.insert(id, vector(id));
        tail_docs.push(json!({"id":id,"vector":vector(id)}));
    }
    for id in &doomed {
        logical.remove(id);
    }
    writer
        .write(
            "tail",
            serde_json::from_value(json!({"upsert_rows":tail_docs,"deletes":doomed})).unwrap(),
        )
        .await
        .unwrap();
    let mut expected: Vec<usize> = logical.keys().copied().collect();
    expected.sort_by(|a, b| distance(&logical[a]).total_cmp(&distance(&logical[b])));
    expected.truncate(10);
    let engine = Engine::with_config(store(backend.clone()).await, config());
    backend.waves.store(0, Ordering::SeqCst);
    backend.paths.lock().unwrap().clear();
    backend.delay.store(true, Ordering::SeqCst);
    let request = serde_json::from_value(json!({
        "rank_by":["vector","ANN",target],"top_k":10
    }))
    .unwrap();
    let result = engine.query("tail", request).await.unwrap();
    backend.delay.store(false, Ordering::SeqCst);
    let got: Vec<usize> = result
        .result
        .rows
        .as_ref()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_u64().unwrap() as usize)
        .collect();
    assert!(
        !got.iter()
            .any(|id| doomed.contains(id) || moved.contains(id)),
        "shadowed rows leaked: {got:?}"
    );
    let hits = got.iter().filter(|id| expected.contains(id)).count();
    assert!(hits >= 9, "{hits} of 10: got {got:?} want {expected:?}");
    let reads: Vec<String> = backend
        .paths
        .lock()
        .unwrap()
        .iter()
        .map(|(p, _)| p.clone())
        .collect();
    assert!(
        !reads.iter().any(|p| p.contains(".bfi")),
        "an unindexed tail opened an ID index: {reads:?}"
    );
    assert!(backend.waves.load(Ordering::SeqCst) <= 4);
}

/// While writes keep arriving, a bulk load accumulates its tail into a large segment instead
/// of flushing a small one every cycle; once the limit is reached (or the writes stop) it
/// flushes, and a forced index always flushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_writes_accumulate_into_one_large_segment() {
    let backend = Arc::new(Counting::default());
    let engine = Engine::with_config(
        Storage::from_backend(backend.clone(), Path::default(), false, false)
            .await
            .unwrap(),
        EngineConfig {
            index_rows: 10,
            bulk_flush_rows: 1000,
            ..config()
        },
    );
    let write = |from: usize, rows: usize| {
        let docs: Vec<_> = (from..from + rows)
            .map(|id| json!({"id":id,"vector":[id as f32, 1.0]}))
            .collect();
        engine.write(
            "bulk",
            serde_json::from_value(json!({
                "distance_metric":"euclidean_squared","upsert_rows":docs
            }))
            .unwrap(),
        )
    };
    write(0, 300).await.unwrap();
    // Writes are still hot and the tail is under the limit: nothing is built.
    assert!(!engine.index_namespace("bulk", false).await.unwrap());
    write(300, 300).await.unwrap();
    assert!(!engine.index_namespace("bulk", false).await.unwrap());
    // Past the row limit the whole tail becomes one segment.
    write(600, 500).await.unwrap();
    assert!(engine.index_namespace("bulk", false).await.unwrap());
    let fresh = Engine::with_config(store(backend.clone()).await, config());
    let described = fresh
        .query(
            "bulk",
            serde_json::from_value(json!({"rank_by":["vector","ANN",[5.0,1.0]],"top_k":3}))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(described.result.rows.as_ref().unwrap().len(), 3);
    let segments = backend
        .inner
        .list(None)
        .filter(|o| {
            futures::future::ready(
                o.as_ref()
                    .is_ok_and(|o| o.location.as_ref().ends_with("docs.bfc")),
            )
        })
        .count()
        .await;
    assert_eq!(segments, 1);
    // A forced index flushes even a small hot tail.
    write(2000, 5).await.unwrap();
    assert!(engine.index_namespace("bulk", true).await.unwrap());
}

/// Garbage collection republishes the manifest without advancing the checkpoint. A node that
/// holds the unindexed tail in memory must keep it, not download the WAL payloads again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_republication_keeps_the_resident_tail() {
    let backend = Arc::new(Counting::default());
    let engine = Engine::with_config(
        Storage::from_backend(backend.clone(), Path::default(), false, false)
            .await
            .unwrap(),
        EngineConfig {
            retention: std::time::Duration::from_millis(1),
            ..config()
        },
    );
    let write = |from: usize, rows: usize| {
        let docs: Vec<_> = (from..from + rows)
            .map(|id| json!({"id":id,"vector":[id as f32, 1.0],"pad":"x".repeat(200)}))
            .collect();
        engine.write(
            "gc",
            serde_json::from_value(json!({
                "distance_metric":"euclidean_squared","upsert_rows":docs
            }))
            .unwrap(),
        )
    };
    write(0, 1000).await.unwrap();
    engine.index_namespace("gc", true).await.unwrap();
    write(1000, 1000).await.unwrap();
    engine.index_namespace("gc", true).await.unwrap();
    // An unindexed tail, big enough to live in its own payload object.
    write(2000, 1000).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    backend.payload_reads.store(0, Ordering::SeqCst);
    let removed = engine.collect_garbage("gc").await.unwrap();
    assert!(removed > 0, "garbage collection published nothing");
    let request =
        serde_json::from_value(json!({"rank_by":["vector","ANN",[2500.0, 1.0]],"top_k":3}))
            .unwrap();
    let result = engine.query("gc", request).await.unwrap();
    assert_eq!(result.result.rows.as_ref().unwrap()[0]["id"], 2500);
    write(3000, 10).await.unwrap();
    let payload_reads = backend.payload_reads.load(Ordering::SeqCst);
    assert_eq!(payload_reads, 0, "the tail was downloaded again");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_bm25_reads_term_chunks_not_whole_dictionaries() {
    let backend = Arc::new(Counting::default());
    let writable = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let writer = Engine::with_config(writable, config());
    let segments = 6;
    for s in 0..segments {
        // Every document carries six terms that occur nowhere else, so each segment's
        // dictionary has about 9,000 terms (several chunks).
        let docs: Vec<_> = (0..1500)
            .map(|i| {
                let n = s * 1500 + i;
                let unique: Vec<_> = (0..6).map(|t| format!("u{n}x{t}")).collect();
                json!({"id":n,"text":format!("common {}", unique.join(" "))})
            })
            .collect();
        writer
            .write(
                "chunks",
                serde_json::from_value(json!({
                    "schema":{"text":{"type":"string","full_text_search":true}},
                    "upsert_rows":docs
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        writer.index_namespace("chunks", true).await.unwrap();
    }
    let engine = Engine::with_config(store(backend.clone()).await, config());
    engine.schema("chunks").await.unwrap();
    backend.paths.lock().unwrap().clear();
    let request =
        serde_json::from_value(json!({"rank_by":["text","BM25","u7x3"],"top_k":10})).unwrap();
    let cold = engine.query("chunks", request).await.unwrap();
    assert_eq!(cold.result.rows.as_ref().unwrap().len(), 1);
    // Bytes requested from FTS and statistics objects, against the whole of those objects.
    let reads: u64 = backend
        .paths
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _)| p.contains(".bft") || p.contains("text-stats"))
        .map(|(_, r)| r.as_ref().map_or(0, |r| r.end - r.start))
        .sum();
    let total: u64 = {
        let mut sum = 0;
        let mut list = backend.inner.list(None);
        while let Some(meta) = list.next().await {
            let meta = meta.unwrap();
            if meta.location.as_ref().contains(".bft")
                || meta.location.as_ref().contains("text-stats")
            {
                sum += meta.size;
            }
        }
        sum
    };
    eprintln!("cold BM25 read {reads} of {total} FTS and statistics bytes");
    assert!(reads * 20 < total, "read {reads} of {total}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_text_keys_remove_winner_id_column_reads() {
    let backend = Arc::new(Counting::default());
    let storage = Storage::from_backend_with_cache(
        backend.clone(),
        Path::default(),
        false,
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
    .unwrap();
    let e = Engine::with_config(storage.clone(), config());
    let rows: Vec<_> = (0..30000u64)
        .map(|row| {
            let filler = format!("{:016x}", row.wrapping_mul(0x9e3779b97f4a7c15)).repeat(4);
            let text = if row % 3000 == 0 {
                format!("needle {filler}")
            } else {
                filler
            };
            json!({"id":1000000000+row,"text":text})
        })
        .collect();
    e.write(
        "keys",
        serde_json::from_value(json!({
            "schema":{"text":{"type":"string","full_text_search":true}},"upsert_rows":rows
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    e.index_namespace("keys", true).await.unwrap();
    let wal = bf_wal::Wal::new(storage.clone(), "keys").unwrap();
    let (seq, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert_eq!(manifest.segments.len(), 1);
    assert!(manifest.segments[0].files["ids"].size > 128 << 10);
    assert!(manifest.segments[0].files["text_keys"].size < 128);
    let query =
        || serde_json::from_value(json!({"rank_by":["text","BM25","needle"],"top_k":10})).unwrap();
    e.drop_caches().await;
    backend.paths.lock().unwrap().clear();
    let with_keys = e.query("keys", query()).await.unwrap().result.rows.unwrap();
    let fast_paths = backend.paths.lock().unwrap().clone();
    assert!(
        fast_paths
            .iter()
            .any(|(path, _)| path.contains("text-keys."))
    );
    let fast_docs = fast_paths
        .iter()
        .filter(|(path, _)| path.contains("docs.bfc"))
        .count();
    // The compact map replaces both the footer prefetch and winner-ID column reads.
    assert_eq!(fast_docs, 0);
    manifest.segments[0].files.remove("text_keys");
    wal.publish_fenced(seq, manifest.indexer_epoch, &manifest)
        .await
        .unwrap();
    e.drop_caches().await;
    backend.paths.lock().unwrap().clear();
    let without_keys = e.query("keys", query()).await.unwrap().result.rows.unwrap();
    let slow_docs = backend
        .paths
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _)| path.contains("docs.bfc"))
        .count();
    assert_eq!(with_keys, without_keys);
    assert!(
        slow_docs > fast_docs,
        "ranged IDs need a later document-column read"
    );
    // The ordinary background pass backfills the optional file and then reaches a no-op.
    assert!(e.index_namespace("keys", false).await.unwrap());
    assert!(!e.index_namespace("keys", true).await.unwrap());
}
