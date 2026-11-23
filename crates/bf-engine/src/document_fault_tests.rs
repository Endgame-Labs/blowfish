//! The publication reply and its immediate verification can both fail after a durable write.
use super::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult, path::Path,
};
use std::{
    fmt,
    sync::{Arc, Mutex},
};

#[derive(Debug, Default)]
struct Faults {
    inner: object_store::memory::InMemory,
    /// Number of manifest writes to skip, and whether to fail after writing.
    rule: Mutex<Option<(usize, bool)>>,
    blocked_get: Mutex<Option<Path>>,
}
fn failure<T>() -> StoreResult<T> {
    Err(object_store::Error::Generic {
        store: "document-upgrade-test",
        source: "injected crash boundary".into(),
    })
}
impl fmt::Display for Faults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("document publication fault store")
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
        let fail = {
            let mut rule = self.rule.lock().unwrap();
            if path.as_ref().starts_with("ns/t/manifest/") {
                if let Some((skip, after)) = rule.as_mut() {
                    if *skip == 0 {
                        let after = *after;
                        *rule = None;
                        Some(after)
                    } else {
                        *skip -= 1;
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };
        match fail {
            Some(after) => {
                if after {
                    self.inner.put_opts(path, payload, opts).await?;
                    *self.blocked_get.lock().unwrap() = Some(path.clone());
                }
                failure()
            }
            None => self.inner.put_opts(path, payload, opts).await,
        }
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> StoreResult<GetResult> {
        let blocked = {
            let mut blocked = self.blocked_get.lock().unwrap();
            if blocked.as_ref() == Some(path) {
                *blocked = None;
                true
            } else {
                false
            }
        };
        if blocked {
            failure()
        } else {
            self.inner.get_opts(path, opts).await
        }
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
async fn failed_and_unresolved_ambiguous_document_publications_preserve_vectors() {
    for after in [false, true] {
        let faults = Arc::new(Faults::default());
        let store = Storage::from_backend(faults.clone(), Path::default(), false, false)
            .await
            .unwrap();
        let (_, engine, wal) = setup_with(store.clone()).await;
        let before = answers(&engine).await;
        let (seq, mut m) = wal.latest_manifest().await.unwrap().unwrap();
        for seg in &mut m.segments {
            segment::upgrade::make_legacy(&store, seg).await.unwrap();
        }
        wal.publish_fenced(seq, m.indexer_epoch, &m).await.unwrap();
        // The claim succeeds; the document publication fails before its write, or after the
        // write with a second failure in the storage layer's verification GET.
        *faults.rule.lock().unwrap() = Some((1, after));
        assert!(engine.reindex("t", &Default::default()).await.is_err());
        let (_, m) = wal.latest_manifest().await.unwrap().unwrap();
        for seg in &m.segments {
            for role in ["docs", "ids", "annv2:vector"] {
                assert!(
                    store.head(&seg.files[role].key).await.is_ok(),
                    "{role} must survive an ambiguous reply"
                );
            }
        }
        assert_eq!(answers(&engine).await, before);
        engine.reindex("t", &Default::default()).await.unwrap();
        assert_eq!(answers(&engine).await, before);
        let again = engine.reindex("t", &Default::default()).await.unwrap();
        assert_eq!(again.bytes_written, 0);
    }
}
