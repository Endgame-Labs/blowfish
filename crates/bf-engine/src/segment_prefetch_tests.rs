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

#[tokio::test]
async fn scattered_column_blocks_use_small_reads_and_keep_decoded_cache_and_crc_checks() {
    for compressed in [false, true] {
        let backend = Arc::new(Delayed::default());
        let store = Storage::from_backend_with_cache(
            backend.clone(),
            Path::default(),
            false,
            false,
            bf_storage::CacheConfig {
                memory_bytes: 8 << 20,
                hedge_budget: 0.0,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut body = Vec::new();
        let ids: Vec<_> = (0..BLOCK_ROWS as u64)
            .map(|n| Some(Value::from(n * 7 + 3)))
            .collect();
        let id_block = block(&mut body, &encode(&ids).unwrap());
        body.resize(1 << 20, 0);
        let titles: Vec<_> = (0..BLOCK_ROWS as u32)
            .map(|n| (n != 1).then(|| Value::from(title(n))))
            .collect();
        let raw = encode(&titles).unwrap();
        let mut title_block = block(
            &mut body,
            &if compressed {
                zstd::bulk::compress(&raw, 3).unwrap()
            } else {
                raw.clone()
            },
        );
        title_block.uncompressed_length = compressed.then_some(raw.len());
        // Keep the directory's lookbehind clear of both selected data blocks.
        body.resize(3 << 20, 0);
        let dir = Directory {
            columns: BTreeMap::from([
                (
                    "id".into(),
                    Column {
                        compressed: false,
                        blocks: vec![id_block],
                        vector: None,
                        vector_i8: false,
                    },
                ),
                (
                    "title".into(),
                    Column {
                        compressed,
                        blocks: vec![title_block.clone()],
                        vector: None,
                        vector_i8: false,
                    },
                ),
            ]),
            ..Default::default()
        };
        let (bytes, meta) = finish(body, &dir).unwrap();
        let file = put_file(&store, "docs.bfc".into(), bytes.clone(), meta)
            .await
            .unwrap();
        let segment = Segment {
            id: "s".into(),
            wal_start: 0,
            wal_end: 1,
            row_count: BLOCK_ROWS as u32,
            schema: Default::default(),
            files: BTreeMap::from([("docs".into(), file.clone())]),
            live: file,
            live_generation: 0,
        };
        let context =
            read_cache::ReadContext::new(Arc::new(read_cache::DecodedCache::new(8 << 20, 1)));
        read_cache::scope(
            context.clone(),
            directory(&ObjectReader::new(&store, &segment.files["docs"])),
        )
        .await
        .unwrap();
        backend.reads.lock().unwrap().clear();
        let rows = RoaringBitmap::from_iter([1, 255]);
        let columns = std::collections::BTreeSet::from(["title".into()]);
        let got = read_cache::scope(
            context.clone(),
            hydrated_rows(&store, &segment, &rows, &columns),
        )
        .await
        .unwrap();
        assert_eq!(
            got[&1],
            Document::from_iter([("id".into(), Value::from(10))])
        );
        assert_eq!(got[&255]["id"], Value::from(255 * 7 + 3));
        assert_eq!(got[&255]["title"], titles[255].clone().unwrap());
        {
            let reads = backend.reads.lock().unwrap();
            assert!(!reads.is_empty());
            assert!(
                reads.iter().all(|(_, r)| r.end - r.start <= 128 << 10),
                "small column blocks must not fetch 1 MiB"
            );
            assert!(reads.iter().map(|(_, r)| r.end - r.start).sum::<u64>() < 256 << 10);
        }
        backend.reads.lock().unwrap().clear();
        let warm = read_cache::scope(context, hydrated_rows(&store, &segment, &rows, &columns))
            .await
            .unwrap();
        assert_eq!(got, warm);
        assert!(backend.reads.lock().unwrap().is_empty());

        // A new immutable object cannot reuse the decoded block, and still checks its CRC.
        let mut bad = bytes;
        bad[title_block.offset as usize] ^= 1;
        let mut corrupt_segment = segment;
        corrupt_segment.files.insert(
            "docs".into(),
            put_file(&store, "corrupt.bfc".into(), bad, meta)
                .await
                .unwrap(),
        );
        assert!(
            hydrated_rows(&store, &corrupt_segment, &rows, &columns)
                .await
                .is_err()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_reranks_hydrate_only_the_winning_attribute_block() {
    let parent = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
    std::fs::create_dir_all(&parent).unwrap();
    let work = tempfile::Builder::new()
        .prefix("staged-hydration-")
        .tempdir_in(parent)
        .unwrap();
    let backend = Arc::new(Delayed::default());
    let store = Storage::from_backend_with_cache(
        backend.clone(),
        Path::default(),
        false,
        false,
        bf_storage::CacheConfig {
            memory_bytes: 16 << 20,
            disk_bytes: 32 << 20,
            block_size: 4096,
            max_coalesce_blocks: 256,
            disk_dir: Some(work.path().join("cache")),
            namespace: Some(uuid::Uuid::new_v4().to_string()),
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
        .map(|id| {
            serde_json::json!({
                "id":id, "vector":vector(id), "title":title(id)
            })
        })
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
    engine
        .recluster(
            "t",
            &crate::ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 32,
                max_slab_rows: 1024,
                scratch: work.path().join("build"),
            },
        )
        .await
        .unwrap();
    let request: bf_types::QueryRequest = serde_json::from_value(serde_json::json!({
        "rank_by":["vector","ANN",vector(1007)], "top_k":1, "include_attributes":["title"]
    }))
    .unwrap();
    let expected = engine
        .query("t", request.clone())
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    assert_eq!(expected[0]["id"], 1007);
    let (_, manifest) = bf_wal::Wal::new(store.clone(), "t")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    let set = &manifest.ann_tiers["vector"][0];
    let dir = bf_vector::Directory::decode(&store.get(&set.dir.key).await.unwrap()).unwrap();
    let mut vector_ends: BTreeMap<usize, u64> = BTreeMap::new();
    for entry in dir.entries() {
        vector_ends
            .entry(entry.segment)
            .and_modify(|end| *end = (*end).max(entry.vectors_range().end))
            .or_insert(entry.vectors_range().end);
    }
    let context = read_cache::ReadContext::new(engine.0.read_cache.clone());
    // Start provisioning independently of the first query's plan.
    read_cache::scope(context.clone(), async {
        crate::local_ann::observe(
            &store,
            set,
            dir.dims(),
            dir.entries().len() as u64,
            "vector",
            &|| crate::local_ann::hydration_extras(set, &manifest.segments),
        );
    })
    .await;
    let mut coverage: Vec<bool> = Vec::new();
    let ready = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            coverage = read_cache::scope(context.clone(), async {
                vector_ends
                    .iter()
                    .map(|(&i, &end)| crate::local_ann::covers(&dir.segments()[i].key, 0..end))
                    .collect()
            })
            .await;
            if coverage.iter().all(|v| *v) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    assert!(ready.is_ok(), "stage coverage: {coverage:?}");
    let segment = &manifest.segments[0];
    let docs = &segment.files["docs"];
    let directory = directory(&ObjectReader::new(&store, docs)).await.unwrap();
    let winner = &directory.columns["title"].blocks[1007 / BLOCK_ROWS];
    let id = &directory.columns["id"].blocks[1007 / BLOCK_ROWS];
    let metadata = docs.size.saturating_sub(docs.metadata_len.max(65536));
    engine.drop_caches().await;
    backend.reads.lock().unwrap().clear();
    let got = engine
        .query("t", request.clone())
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    assert_eq!(got, expected);
    {
        let reads = backend.reads.lock().unwrap();
        let docs_reads: Vec<_> = reads
            .iter()
            .filter(|(path, _)| path.as_ref() == docs.key)
            .collect();
        assert!(!docs_reads.is_empty(), "the winning column is not staged");
        for (_, range) in docs_reads {
            assert!(
                [winner, id]
                    .iter()
                    .any(|b| range.start < b.offset + b.length + 24 && b.offset + 24 < range.end)
                    || range.end > metadata + 24,
                "a local rerank fetched a nonwinning column block: {range:?}"
            );
        }
        assert!(
            !reads
                .iter()
                .any(|(path, _)| path.as_ref().contains("/slabs/")),
            "exact vectors must come from the local stage"
        );
    }
    // Removing the stage retains the remote speculation path and identical results.
    engine
        .0
        .read_cache
        .staging
        .as_ref()
        .unwrap()
        .remove_namespace("", "t");
    engine.drop_caches().await;
    let remote = engine
        .query("t", request)
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    assert_eq!(remote, expected);
    assert!(engine.shutdown(Duration::from_secs(5)).await);
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
