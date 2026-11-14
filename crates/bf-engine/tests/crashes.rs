//! Fail reads, listings, and mutations, including ambiguous successful PUTs and GC deletes.
use async_trait::async_trait;
use bf_engine::{Engine, EngineConfig};
use bf_storage::Storage;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult, path::Path,
};
use serde_json::json;
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Debug, Default)]
struct Faults {
    inner: object_store::memory::InMemory,
    operation: AtomicUsize,
    fail_at: AtomicUsize,
    after: AtomicBool,
    pause_copy: AtomicBool,
    copy_paused: tokio::sync::Notify,
    resume_copy: tokio::sync::Notify,
}
impl Faults {
    fn arm(&self, at: usize, after: bool) {
        self.operation.store(0, Ordering::SeqCst);
        self.fail_at.store(at, Ordering::SeqCst);
        self.after.store(after, Ordering::SeqCst);
    }
    fn hit(&self) -> bool {
        self.operation.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at.load(Ordering::SeqCst)
    }
}
fn failure<T>() -> StoreResult<T> {
    Err(object_store::Error::Generic {
        store: "injected",
        source: "crash boundary".into(),
    })
}
impl fmt::Display for Faults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("crash-injection store")
    }
}
#[async_trait]
impl ObjectStore for Faults {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        if path.as_ref().starts_with("ns/paused/wal/")
            && self.pause_copy.swap(false, Ordering::SeqCst)
        {
            self.copy_paused.notify_one();
            self.resume_copy.notified().await;
        }
        let fail = self.hit();
        if fail && !self.after.load(Ordering::SeqCst) {
            return failure();
        }
        let result = self.inner.put_opts(path, payload, opts).await?;
        if fail {
            return failure();
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> StoreResult<GetResult> {
        let fail = self.hit();
        if fail && !self.after.load(Ordering::SeqCst) {
            return failure();
        }
        let result = self.inner.get_opts(path, opts).await?;
        if fail {
            return failure();
        }
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        let fail = self.hit();
        if fail && !self.after.load(Ordering::SeqCst) {
            return futures::stream::once(async { failure() }).boxed();
        }
        let stream = self.inner.delete_stream(paths);
        if fail {
            stream.map(|_| failure()).boxed()
        } else {
            stream
        }
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        if self.hit() {
            futures::stream::once(async { failure() }).boxed()
        } else {
            self.inner.list(prefix)
        }
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
        retention: Duration::ZERO,
        compaction_segments: 2,
        ..Default::default()
    }
}
async fn write(e: &Engine, id: u64) -> Result<(), bf_types::ApiError> {
    e.write(
        "crash",
        serde_json::from_value(json!({"upsert_rows":[{"id":id,"n":id}]})).unwrap(),
    )
    .await
    .map(|_| ())
}
async fn rows(e: &Engine) -> Vec<u64> {
    e.query(
        "crash",
        serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":100})).unwrap(),
    )
    .await
    .unwrap()
    .result
    .rows
    .unwrap()
    .iter()
    .map(|d| d["id"].as_u64().unwrap())
    .collect()
}
#[tokio::test]
async fn acknowledged_writes_survive_every_commit_index_compact_and_gc_mutation() {
    for stage in 0..4 {
        for after in [false, true] {
            let mut boundaries = 1;
            let mut position = 0;
            while position <= boundaries {
                let faults = Arc::new(Faults::default());
                let store = Storage::from_backend(faults.clone(), Path::default(), false, false)
                    .await
                    .unwrap();
                let e = Engine::with_config(store.clone(), config());
                write(&e, 1).await.unwrap();
                if stage >= 2 {
                    e.index_namespace("crash", true).await.unwrap();
                    write(&e, 2).await.unwrap();
                    e.index_namespace("crash", true).await.unwrap();
                }
                if stage == 3 {
                    e.index_namespace("crash", true).await.unwrap();
                }
                faults.arm(position, after);
                let acknowledged = match stage {
                    0 => write(&e, 2).await.is_ok(),
                    1 | 2 => {
                        let _ = e.index_namespace("crash", true).await;
                        false
                    }
                    _ => {
                        let _ = e.collect_garbage("crash").await;
                        false
                    }
                };
                if position == 0 {
                    boundaries = faults.operation.load(Ordering::SeqCst);
                }
                faults.arm(0, false);
                drop(e);
                let recovered = Engine::with_config(store, config());
                let found = rows(&recovered).await;
                assert!(
                    found.contains(&1),
                    "stage {stage}, boundary {position}, after {after}"
                );
                if acknowledged || stage >= 2 {
                    assert_eq!(found, vec![1, 2]);
                } else {
                    assert!(found == vec![1] || found == vec![1, 2]);
                }
                recovered.index_namespace("crash", true).await.unwrap();
                recovered.collect_garbage("crash").await.unwrap();
                assert_eq!(found, rows(&recovered).await);
                position += 1;
            }
        }
    }
}

// A timeout-only pin collector would delete these segments while the copier is
// suspended after source-epoch validation and before the destination WAL CAS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_copier_survives_source_deletion_and_crashing_collectors() {
    for after in [false, true] {
        for boundary in [0, 1, 4, 8, 16, 32] {
            let faults = Arc::new(Faults::default());
            let store = Storage::from_backend(faults.clone(), Path::default(), false, false)
                .await
                .unwrap();
            let engine = Engine::with_config(store.clone(), config());
            write(&engine, 1).await.unwrap();
            engine.index_namespace("crash", true).await.unwrap();
            faults.pause_copy.store(true, Ordering::SeqCst);
            let copier = engine.clone();
            let copy = tokio::spawn(async move {
                copier
                    .write(
                        "paused",
                        serde_json::from_value(json!({"branch_from_namespace":"crash"})).unwrap(),
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(10), faults.copy_paused.notified())
                .await
                .unwrap();
            engine.delete_namespace("crash").await.unwrap();
            faults.arm(boundary, after);
            let _ = engine.collect_garbage("crash").await;
            faults.arm(0, false);
            // A collector restart must still retain the suspended copy's pin.
            let recovered = Engine::with_config(store.clone(), config());
            recovered.collect_garbage("crash").await.unwrap();
            faults.resume_copy.notify_one();
            copy.await.unwrap().unwrap();
            let cold = Engine::with_config(store, config());
            let result = cold
                .query(
                    "paused",
                    serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":10})).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                result.result.rows.unwrap()[0]["id"],
                1,
                "boundary={boundary}, after={after}"
            );
        }
    }
}
