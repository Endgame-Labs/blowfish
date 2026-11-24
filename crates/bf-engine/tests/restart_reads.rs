//! A restart of an up-to-date namespace reads no ANN rows in the background: the upgrade survey
//! reads nothing of a current ANN object, idle indexing and GC read none, and the recall sampler
//! does not compare exhaustively with the first queries after every start.
use async_trait::async_trait;
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::Storage;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult, path::Path,
};
use serde_json::json;
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

/// Counts the bytes read from ANN objects (`.bfa`).
#[derive(Debug, Default)]
struct Counted {
    inner: object_store::memory::InMemory,
    ann_bytes: AtomicU64,
}
impl fmt::Display for Counted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ANN-read counting store")
    }
}
#[async_trait]
impl ObjectStore for Counted {
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
        let result = self.inner.get_opts(path, opts).await?;
        if path.as_ref().ends_with(".bfa") {
            self.ann_bytes
                .fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
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

fn vector(id: usize) -> Vec<f32> {
    (0..32)
        .map(|d| ((id * 7919 + d * 104_729) as f64).sin() as f32)
        .collect()
}

/// A fresh process on the bucket: its own caches over the same objects.
async fn open(counted: &Arc<Counted>) -> Storage {
    Storage::from_backend(counted.clone(), Path::default(), false, false)
        .await
        .unwrap()
}

async fn ann_queries(engine: &Engine) {
    for q in [7usize, 1234, 2500, 4321, 5999] {
        let request = serde_json::from_value(
            json!({"rank_by": ["vector", "ANN", vector(q)], "top_k": 3, "include_attributes": ["id"]}),
        )
        .unwrap();
        let rows = engine
            .query("t", request)
            .await
            .unwrap()
            .result
            .rows
            .unwrap();
        assert_eq!(rows[0]["id"], json!(q));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restarting_an_upgraded_namespace_reads_no_ann_rows_in_the_background() {
    let counted = Arc::new(Counted::default());
    let writer = Engine::with_config(
        open(&counted).await,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    for batch in 0..3usize {
        let rows: Vec<_> = (batch * 2000..(batch + 1) * 2000)
            .map(|n| json!({"id": n, "vector": vector(n), "text": format!("row {n} of batch {batch}")}))
            .collect();
        writer
            .write(
                "t",
                serde_json::from_value(json!({
                    "distance_metric": "euclidean_squared",
                    "schema": {"text": {"type": "string", "full_text_search": true}},
                    "upsert_rows": rows,
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        writer.index_namespace("t", true).await.unwrap();
    }
    let scratch = tempfile::tempdir().unwrap();
    writer
        .recluster(
            "t",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 3,
                cluster_rows: 50,
                max_slab_rows: 1500,
                scratch: scratch.path().to_owned(),
            },
        )
        .await
        .unwrap();
    assert!(writer.shutdown(Duration::from_secs(5)).await);

    // What the queries below read on their own, in a fresh process with sampling off.
    let config = EngineConfig {
        index_interval: Duration::from_millis(10),
        retention: Duration::ZERO,
        recall_sample_rate: 1.0,
        ..Default::default()
    };
    counted.ann_bytes.store(0, Ordering::Relaxed);
    let unsampled = Engine::with_config(
        open(&counted).await,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..config.clone()
        },
    );
    ann_queries(&unsampled).await;
    let plain = counted.ann_bytes.swap(0, Ordering::Relaxed);
    assert!(plain > 0);

    // Restart with background indexing, upgrades, and GC on, and every query sampled for
    // recall. Idle, nothing reads an ANN object: discovery, the upgrade survey, recluster
    // checks, and GC (due every second here) all run meanwhile.
    let restarted = Engine::with_config(open(&counted).await, config);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(restarted.cached_namespaces().await, vec!["t".to_string()]);
    assert_eq!(counted.ann_bytes.load(Ordering::Relaxed), 0);

    // The first queries read no more than they read with sampling off: no exhaustive
    // comparison rides along with them.
    ann_queries(&restarted).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let sampled = counted.ann_bytes.load(Ordering::Relaxed);
    assert!(sampled <= plain, "{sampled} > {plain}");
    assert!(restarted.shutdown(Duration::from_secs(5)).await);
}
