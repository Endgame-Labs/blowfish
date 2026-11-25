//! An unneeded speculative column block may stall forever without holding a query response.
use super::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use std::{
    fmt,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Debug, Default)]
struct Delayed {
    inner: object_store::memory::InMemory,
    reads: Mutex<Vec<(Path, Range<u64>)>>,
    blocked: Mutex<Option<(Path, Range<u64>)>>,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    canceled: AtomicUsize,
}
impl fmt::Display for Delayed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("delayed columns")
    }
}
struct Pending<'a>(&'a AtomicUsize);
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Release(Arc<Delayed>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.blocked.lock().unwrap() = None;
        self.0.resume.notify_waiters();
    }
}
#[async_trait]
impl ObjectStore for Delayed {
    async fn put_opts(
        &self,
        p: &Path,
        data: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(p, data, opts).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, opts).await
    }
    async fn get_opts(&self, p: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        if let Some(object_store::GetRange::Bounded(range)) = &opts.range {
            self.reads.lock().unwrap().push((p.clone(), range.clone()));
            let (armed, blocked) = {
                let block = self.blocked.lock().unwrap();
                (
                    block.is_some(),
                    block.as_ref().is_some_and(|(key, b)| {
                        key == p && range.start < b.end && b.start < range.end
                    }),
                )
            };
            if blocked {
                let resumed = self.resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                let _pending = Pending(&self.canceled);
                self.started.notify_one();
                if self.blocked.lock().unwrap().is_some() {
                    resumed.await;
                }
            }
            // Let the speculative tasks start while the query awaits its exact vectors.
            if armed && p.as_ref().ends_with(".bfa") {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        self.inner.get_opts(p, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn vector(id: u32) -> Vec<f32> {
    let mut seed = id as u64 + 1;
    (0..16)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed >> 40) as f32 / (1u32 << 24) as f32 - 0.5
        })
        .collect()
}
fn title(id: u32) -> String {
    let mut seed = id as u64 + 1;
    (0..384)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            char::from(b'!' + (seed % 90) as u8)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_nonwinning_projection_is_canceled_after_winner_hydration() {
    let backend = Arc::new(Delayed::default());
    let store = Storage::from_backend_with_cache(
        backend.clone(),
        Path::default(),
        false,
        false,
        bf_storage::CacheConfig {
            memory_bytes: 16 << 20,
            block_size: 512,
            max_coalesce_blocks: 8,
            hedge_budget: 0.0,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let engine = crate::Engine::with_config(
        store.clone(),
        crate::EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    let rows: Vec<_> = (0..4096u32)
        .map(|id| serde_json::json!({"id":id,"vector":vector(id),"title":title(id)}))
        .collect();
    engine
        .write(
            "t",
            serde_json::from_value(serde_json::json!({
                "distance_metric":"euclidean_squared", "upsert_rows":rows
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    engine.index_namespace("t", true).await.unwrap();
    let (_, manifest) = bf_wal::Wal::new(store.clone(), "t")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.segments.len(), 1);
    let docs = &manifest.segments[0].files["docs"];
    let directory = directory(&ObjectReader::new(&store, docs)).await.unwrap();
    let path = Path::from(docs.key.as_str());

    let mut chosen = None;
    for q in [0, 1007, 2049, 3011] {
        engine.drop_caches().await;
        backend.reads.lock().unwrap().clear();
        let request: bf_types::QueryRequest = serde_json::from_value(serde_json::json!({
            "rank_by":["vector","ANN",vector(q)],"top_k":10,"include_attributes":["title"]
        }))
        .unwrap();
        let expected = engine
            .query("t", request.clone())
            .await
            .unwrap()
            .result
            .rows
            .unwrap();
        let winners: std::collections::BTreeSet<_> = expected
            .iter()
            .map(|r| r["id"].as_u64().unwrap() as usize / BLOCK_ROWS)
            .collect();
        // Choose a block the real planner fetched for speculation, but none of the final
        // winning rows uses. Exclude the footer read's 64 KiB lookbehind.
        for (n, b) in directory.columns["title"].blocks.iter().enumerate() {
            if winners.contains(&n) || b.offset + b.length >= docs.size.saturating_sub(65536) {
                continue;
            }
            // Block one interior cache block, far enough from neighboring document blocks
            // that coalescing cannot make its GET necessary for a winning row.
            assert!(b.length > 8192);
            let middle = (b.offset + b.length / 2) / 512 * 512;
            let range = middle + 24..middle + 512 + 24;
            let read = backend
                .reads
                .lock()
                .unwrap()
                .iter()
                .any(|(p, r)| *p == path && r.start < range.end && range.start < r.end);
            if read {
                chosen = Some((request, expected, range));
                break;
            }
        }
        if chosen.is_some() {
            break;
        }
    }
    let (request, expected, range) = chosen.expect("fixture must exercise nonwinning speculation");
    engine.drop_caches().await;
    *backend.blocked.lock().unwrap() = Some((path, range));
    let _release = Release(backend.clone());
    let reader = engine.clone();
    let mut query = tokio::spawn(async move { reader.query("t", request).await });
    tokio::time::timeout(Duration::from_secs(10), backend.started.notified())
        .await
        .unwrap();
    let actual = tokio::time::timeout(Duration::from_secs(10), &mut query)
        .await
        .expect("an unneeded speculative read held the response")
        .unwrap()
        .unwrap();
    assert_eq!(actual.result.rows.unwrap(), expected);
    tokio::time::timeout(Duration::from_secs(10), async {
        while backend.canceled.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(engine.shutdown(Duration::from_secs(5)).await);
}
