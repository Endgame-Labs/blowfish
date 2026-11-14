use async_trait::async_trait;
use bf_storage::{CreateResult, Storage, StorageMode};
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    path::Path,
};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
#[derive(Debug)]
struct FaultStore {
    inner: object_store::memory::InMemory,
    ignore_create: bool,
    reject_create: bool,
    lose_response: AtomicBool,
}
impl FaultStore {
    fn new(ignore_create: bool, reject_create: bool, lose_response: bool) -> Self {
        Self {
            inner: Default::default(),
            ignore_create,
            reject_create,
            lose_response: AtomicBool::new(lose_response),
        }
    }
}
impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fault-injection store")
    }
}
#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> StoreResult<PutResult> {
        if self.reject_create && matches!(opts.mode, PutMode::Create) {
            return Err(object_store::Error::NotSupported {
                source: "conditional create unavailable".into(),
            });
        }
        if self.ignore_create {
            opts.mode = PutMode::Overwrite;
        }
        let result = self.inner.put_opts(path, payload, opts).await?;
        if path.as_ref() == "data/object" && self.lose_response.swap(false, Ordering::AcqRel) {
            return Err(object_store::Error::Generic {
                store: "fault",
                source: "response lost after durable put".into(),
            });
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
        self.inner.get_opts(path, opts).await
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
#[tokio::test]
async fn probe_detects_ignored_and_rejected_conditional_creates() {
    for backend in [
        FaultStore::new(true, false, false),
        FaultStore::new(false, true, false),
    ] {
        let backend = Arc::new(backend);
        let store = Storage::from_backend(backend.clone(), Path::default(), false, false)
            .await
            .unwrap();
        assert_eq!(store.mode(), StorageMode::SingleWriter);
        assert_eq!(
            store.create("data/object", "first").await.unwrap(),
            CreateResult::Created
        );
        assert_eq!(
            store.create("data/object", "second").await.unwrap(),
            CreateResult::Conflict
        );
        assert_eq!(store.get("data/object").await.unwrap(), "first");
        assert!(
            Storage::from_backend(backend, Path::default(), false, false)
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn lost_success_response_is_resolved_by_write_identity() {
    let backend = Arc::new(FaultStore::new(false, false, true));
    let store = Storage::from_backend(backend, Path::default(), false, false)
        .await
        .unwrap();
    assert_eq!(
        store.create("data/object", "durable").await.unwrap(),
        CreateResult::AlreadyCommitted
    );
    assert_eq!(store.get("data/object").await.unwrap(), "durable");
}
#[tokio::test]
async fn reserved_header_and_empty_objects_roundtrip() {
    let store = Storage::from_backend(
        Arc::new(object_store::memory::InMemory::new()),
        Path::default(),
        false,
        false,
    )
    .await
    .unwrap();
    let raw = Bytes::from_static(b"BFPUT001an arbitrary binary payload");
    store.put("binary", raw.clone()).await.unwrap();
    assert_eq!(store.get("binary").await.unwrap(), raw);
    assert_eq!(store.get_range("binary", 0..8).await.unwrap(), "BFPUT001");
    store.put("empty", Bytes::new()).await.unwrap();
    assert_eq!(store.get_range("empty", 0..0).await.unwrap(), Bytes::new());
}

#[tokio::test]
async fn lost_redaction_response_preserves_the_permanent_retry_fence() {
    let backend = Arc::new(FaultStore::new(false, false, false));
    let store = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let id = uuid::Uuid::new_v4();
    store
        .create_with_id("data/object", "sensitive", id)
        .await
        .unwrap();
    assert!(
        !store
            .redact_with_id("data/object", "redacted", uuid::Uuid::new_v4())
            .await
            .unwrap()
    );
    assert_eq!(store.get("data/object").await.unwrap(), "sensitive");
    assert!(
        !store
            .redact_with_id("missing", "redacted", id)
            .await
            .unwrap()
    );
    backend.lose_response.store(true, Ordering::SeqCst);
    assert!(
        store
            .redact_with_id("data/object", "redacted", id)
            .await
            .unwrap()
    );
    assert_eq!(store.get("data/object").await.unwrap(), "redacted");
    assert_eq!(
        store
            .create_with_id("data/object", "sensitive", id)
            .await
            .unwrap(),
        CreateResult::AlreadyCommitted
    );
    assert_eq!(
        store.create("data/object", "another writer").await.unwrap(),
        CreateResult::Conflict
    );
    assert_eq!(store.get("data/object").await.unwrap(), "redacted");
}
