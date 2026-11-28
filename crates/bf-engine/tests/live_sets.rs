//! A segment at live generation 0 has every row live and no query reads its live set; a segment
//! with a later generation still reads and applies its set, for every filter operator.
use async_trait::async_trait;
use bf_engine::{Engine, EngineConfig};
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

/// Counts reads of live-set objects.
#[derive(Debug, Default)]
struct Counted {
    inner: object_store::memory::InMemory,
    live_reads: AtomicU64,
}
impl fmt::Display for Counted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("live-read counting store")
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
        if path.as_ref().contains("/live.") {
            self.live_reads.fetch_add(1, Ordering::Relaxed);
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

async fn open(counted: &Arc<Counted>) -> Storage {
    Storage::from_backend(counted.clone(), Path::default(), false, false)
        .await
        .unwrap()
}
fn config() -> EngineConfig {
    EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        retention: Duration::ZERO,
        compaction_segments: 100,
        ..Default::default()
    }
}
async fn ids(engine: &Engine, filters: serde_json::Value) -> Vec<u64> {
    let request = serde_json::from_value(
        json!({"filters": filters, "rank_by": ["id", "asc"], "top_k": 1000, "include_attributes": ["id"]}),
    )
    .unwrap();
    engine
        .query("t", request)
        .await
        .unwrap()
        .result
        .rows
        .unwrap()
        .iter()
        .map(|row| row["id"].as_u64().unwrap())
        .collect()
}
type Shape = (serde_json::Value, fn(u64) -> bool);
fn shapes() -> Vec<Shape> {
    vec![
        (json!(["group", "Eq", 2]), |id| id % 5 == 2),
        (json!(["group", "NotEq", 2]), |id| id % 5 != 2),
        (json!(["Not", ["group", "Eq", 2]]), |id| id % 5 != 2),
        (json!(["id", "Lt", 100]), |id| id < 100),
        (json!(["text", "ContainsAllTokens", "pond"]), |_| true),
        (
            json!(["Or", [["group", "Eq", 1], ["group", "Eq", 3]]]),
            |id| id % 5 == 1 || id % 5 == 3,
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generation_zero_reads_no_live_set_and_later_generations_apply_theirs() {
    let counted = Arc::new(Counted::default());
    let writer = Engine::with_config(open(&counted).await, config());
    let rows: Vec<_> = (0..600u64)
        .map(|n| json!({"id": n, "group": n % 5, "text": format!("fish pond {n}")}))
        .collect();
    writer
        .write(
            "t",
            serde_json::from_value(json!({
                "schema": {"text": {"type": "string", "full_text_search": true}},
                "upsert_rows": rows,
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    writer.index_namespace("t", true).await.unwrap();
    assert!(writer.shutdown(Duration::from_secs(5)).await);

    // Generation 0, in a fresh process: no live object is read.
    counted.live_reads.store(0, Ordering::Relaxed);
    let fresh = Engine::with_config(open(&counted).await, config());
    for (filter, keep) in shapes() {
        let want: Vec<u64> = (0..600).filter(|id| keep(*id)).collect();
        assert_eq!(ids(&fresh, filter.clone()).await, want, "{filter}");
    }
    assert_eq!(counted.live_reads.load(Ordering::Relaxed), 0);
    assert!(fresh.shutdown(Duration::from_secs(5)).await);

    // Deleting rows writes a later generation, which a fresh process reads and applies.
    let writer = Engine::with_config(open(&counted).await, config());
    writer
        .write(
            "t",
            serde_json::from_value(json!({"delete_by_filter": ["group", "Eq", 2]})).unwrap(),
        )
        .await
        .unwrap();
    writer.index_namespace("t", true).await.unwrap();
    writer
        .write(
            "t",
            serde_json::from_value(json!({"deletes": [0, 1, 2, 3, 4, 5]})).unwrap(),
        )
        .await
        .unwrap();
    writer.index_namespace("t", true).await.unwrap();
    assert!(writer.shutdown(Duration::from_secs(5)).await);
    counted.live_reads.store(0, Ordering::Relaxed);
    let fresh = Engine::with_config(open(&counted).await, config());
    let gone = |id: u64| id % 5 != 2 && id > 5;
    for (filter, keep) in shapes() {
        let want: Vec<u64> = (0..600).filter(|id| keep(*id) && gone(*id)).collect();
        assert_eq!(ids(&fresh, filter.clone()).await, want, "{filter}");
    }
    assert!(counted.live_reads.load(Ordering::Relaxed) > 0);
    assert!(fresh.shutdown(Duration::from_secs(5)).await);
}
