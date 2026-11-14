use async_trait::async_trait;
use bf_storage::{CacheConfig, CacheStats, Error, Storage};
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    path::Path,
};
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
    heads: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    delay: AtomicBool,
    fail: AtomicBool,
    short: AtomicBool,
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
        if opts.head {
            self.heads.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(object_store::GetRange::Bounded(range)) = &opts.range {
            self.ranges.lock().unwrap().push(range.clone());
        }
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        let _active = Active(&self.active);
        if self.delay.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "counting",
                source: "injected failure".into(),
            });
        }
        let mut result = self.inner.get_opts(path, opts).await?;
        if self.short.swap(false, Ordering::SeqCst) {
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
fn config() -> CacheConfig {
    CacheConfig {
        memory_bytes: 4096,
        block_size: 64,
        ..Default::default()
    }
}
async fn setup(config: CacheConfig) -> (Storage, Arc<Counting>, Bytes) {
    let backend = Arc::new(Counting::default());
    let data = Bytes::from((0..1027).map(|i| (i * 37 % 251) as u8).collect::<Vec<_>>());
    backend
        .put(&Path::from("object"), data.clone().into())
        .await
        .unwrap();
    let store =
        Storage::from_backend_with_cache(backend.clone(), Path::default(), true, false, config)
            .await
            .unwrap();
    (store, backend, data)
}
fn ranges(backend: &Counting) -> Vec<Range<u64>> {
    backend.ranges.lock().unwrap().clone()
}

#[tokio::test]
async fn random_ranges_match_direct_reads() {
    let (store, backend, data) = setup(config()).await;
    let reader = store.range_reader("object").await.unwrap();
    assert_eq!(reader.len(), data.len() as u64);
    let mut seed = 67u64;
    for _ in 0..400 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let a = seed % (data.len() as u64 + 1);
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let b = seed % (data.len() as u64 + 1);
        let range = a.min(b)..a.max(b);
        let direct = if range.is_empty() {
            Bytes::new()
        } else {
            backend
                .inner
                .get_range(&Path::from("object"), range.clone())
                .await
                .unwrap()
        };
        assert_eq!(reader.read(range).await.unwrap(), direct);
    }
    assert_eq!(backend.heads.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn batch_aligns_coalesces_and_fetches_only_missing_blocks() {
    let (store, backend, data) = setup(config()).await;
    let reader = store
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap();
    reader.read(64..128).await.unwrap();
    backend.ranges.lock().unwrap().clear();
    let stats = CacheStats::default();
    let result = reader
        .read_batch_with_stats(&[0..193, 511..513, 1024..1027, 64..64], &stats)
        .await
        .unwrap();
    assert_eq!(
        result,
        vec![
            data.slice(0..193),
            data.slice(511..513),
            data.slice(1024..),
            Bytes::new()
        ]
    );
    let mut fetched = ranges(&backend);
    fetched.sort_by_key(|r| r.start);
    assert_eq!(fetched, vec![0..64, 128..256, 448..576, 1024..1027]);
    assert_eq!(stats.snapshot().memory_bytes, 64);
    assert_eq!(stats.snapshot().missed_bytes, 134);
    assert_eq!(stats.temperature(), "warm");
    assert!((stats.cache_hit_ratio() - 64.0 / 198.0).abs() < 1e-9);
    assert!(reader.read(0..1028).await.is_err());
    let backwards = Range { start: 7, end: 2 };
    assert!(reader.read(backwards).await.is_err());
    assert_eq!(reader.read(1027..1027).await.unwrap(), Bytes::new());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_readers_share_fetches_and_obey_global_limit() {
    let (store, backend, data) = setup(CacheConfig {
        fetch_concurrency: 2,
        max_coalesce_blocks: 2,
        ..config()
    })
    .await;
    backend.delay.store(true, Ordering::SeqCst);
    let reader = store
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap();
    let tasks: Vec<_> = (0..24)
        .map(|_| {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(0..1027).await.unwrap() })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), data);
    }
    assert_eq!(ranges(&backend).len(), 9);
    assert!(backend.max_active.load(Ordering::SeqCst) <= 2);
}
#[tokio::test]
async fn metadata_is_single_flight_and_missing_objects_are_not_cached() {
    let (store, backend, _) = setup(config()).await;
    backend.delay.store(true, Ordering::SeqCst);
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move { store.range_reader("object").await.unwrap() })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(backend.heads.load(Ordering::SeqCst), 1);
    assert_eq!(ranges(&backend), vec![0..24]);
    assert!(matches!(
        store.range_reader("new").await,
        Err(Error::NotFound)
    ));
    backend
        .put(&Path::from("new"), "arrived".into())
        .await
        .unwrap();
    assert_eq!(store.get_cached("new").await.unwrap(), "arrived");
}
#[tokio::test]
async fn failed_short_and_cancelled_fetches_are_retryable() {
    let (store, backend, data) = setup(config()).await;
    let reader = store
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap();
    backend.fail.store(true, Ordering::SeqCst);
    assert!(reader.read(0..64).await.is_err());
    backend.short.store(true, Ordering::SeqCst);
    assert!(reader.read(0..64).await.is_err());
    backend.delay.store(true, Ordering::SeqCst);
    let task = {
        let reader = reader.clone();
        tokio::spawn(async move { reader.read(0..128).await })
    };
    while backend.active.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), reader.read(0..128))
            .await
            .unwrap()
            .unwrap(),
        data.slice(0..128)
    );
}
#[tokio::test]
async fn eviction_and_zero_memory_budget_preserve_correctness() {
    for budget in [0, 64, 128] {
        let (store, backend, data) = setup(CacheConfig {
            memory_bytes: budget,
            ..config()
        })
        .await;
        let reader = store
            .range_reader_with_length("object", data.len() as u64, false)
            .unwrap();
        for i in 0..16 {
            assert_eq!(
                reader.read(i * 64..(i + 1) * 64).await.unwrap(),
                data.slice(i as usize * 64..(i as usize + 1) * 64)
            );
        }
        let stats = CacheStats::default();
        assert_eq!(reader.read_with_stats(0..1027, &stats).await.unwrap(), data);
        assert!(stats.snapshot().missed_bytes >= 1027 - budget);
        assert!(ranges(&backend).len() > 16);
    }
}
#[tokio::test]
async fn disk_reopens_evicts_and_isolates_backend_namespaces() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = CacheConfig {
        disk_dir: Some(dir.path().to_owned()),
        disk_bytes: 208,
        memory_bytes: 0,
        namespace: Some("test-backend".into()),
        ..config()
    };
    let (store, backend, data) = setup(cfg.clone()).await;
    let reader = store
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap();
    reader.read(0..192).await.unwrap(); // Three 104-byte files; only two fit.
    assert!(
        Storage::from_backend_with_cache(
            backend.clone(),
            Path::default(),
            true,
            false,
            cfg.clone()
        )
        .await
        .is_err()
    );
    drop(reader);
    drop(store);
    backend.ranges.lock().unwrap().clear();
    let reopened = Storage::from_backend_with_cache(
        backend.clone(),
        Path::default(),
        true,
        false,
        cfg.clone(),
    )
    .await
    .unwrap();
    let reader = reopened
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap();
    let stats = CacheStats::default();
    assert_eq!(
        reader.read_with_stats(64..192, &stats).await.unwrap(),
        data.slice(64..192)
    );
    assert_eq!(stats.snapshot().disk_bytes, 128);
    assert_eq!(stats.temperature(), "warm");
    assert!(ranges(&backend).is_empty());
    reader.read(0..64).await.unwrap();
    assert_eq!(ranges(&backend), vec![0..64]);
    let other = Storage::from_backend_with_cache(
        backend.clone(),
        Path::default(),
        true,
        false,
        CacheConfig {
            namespace: Some("different-backend".into()),
            ..cfg
        },
    )
    .await
    .unwrap();
    other
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap()
        .read(64..128)
        .await
        .unwrap();
    assert_eq!(ranges(&backend).len(), 2);
}
#[tokio::test]
async fn corrupt_disk_blocks_fall_back_to_source() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = CacheConfig {
        disk_dir: Some(dir.path().to_owned()),
        disk_bytes: 4096,
        memory_bytes: 0,
        namespace: Some("corruption".into()),
        ..config()
    };
    let (store, backend, data) = setup(cfg).await;
    let reader = store
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap();
    reader.read(0..64).await.unwrap();
    let subdir = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let file = std::fs::read_dir(subdir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.file_name().unwrap() != "lock")
        .unwrap();
    let mut corrupt = std::fs::read(&file).unwrap();
    corrupt[45] ^= 1;
    std::fs::write(file, corrupt).unwrap();
    assert_eq!(reader.read(0..64).await.unwrap(), data.slice(0..64));
    assert_eq!(ranges(&backend), vec![0..64, 0..64]);
}
#[tokio::test]
async fn headers_whole_objects_and_mutable_reads() {
    let backend = Arc::new(Counting::default());
    let store =
        Storage::from_backend_with_cache(backend.clone(), Path::default(), false, false, config())
            .await
            .unwrap();
    let payload = Bytes::from_static(b"BFPUT001arbitrary logical binary payload");
    store.create("encoded", payload.clone()).await.unwrap();
    assert_eq!(store.get_cached("encoded").await.unwrap(), payload);
    assert_eq!(
        store
            .range_reader_with_length("encoded", payload.len() as u64, true)
            .unwrap()
            .read(0..8)
            .await
            .unwrap(),
        "BFPUT001"
    );
    let count = ranges(&backend).len();
    let stats = CacheStats::default();
    assert_eq!(
        store
            .get_cached_with_stats("encoded", &stats)
            .await
            .unwrap(),
        payload
    );
    assert_eq!(stats.cache_hit_ratio(), 1.0);
    assert_eq!(stats.temperature(), "hot");
    assert_eq!(ranges(&backend).len(), count);
    store.put("mutable", "old").await.unwrap();
    assert_eq!(store.get("mutable").await.unwrap(), "old");
    store.put("mutable", "new").await.unwrap();
    assert_eq!(store.get("mutable").await.unwrap(), "new");
    store.create("empty", Bytes::new()).await.unwrap();
    assert!(store.range_reader("empty").await.unwrap().is_empty());
    assert_eq!(store.get_cached("empty").await.unwrap(), Bytes::new());
    assert_eq!(CacheStats::default().temperature(), "hot");
}
#[tokio::test]
#[allow(clippy::single_range_in_vec_init)]
async fn background_warming_is_bounded_and_populates_cache() {
    let (store, backend, data) = setup(CacheConfig {
        prefetch_concurrency: 1,
        ..config()
    })
    .await;
    backend.delay.store(true, Ordering::SeqCst);
    let warming = store.prefetch("object", vec![0..193]).unwrap();
    assert!(matches!(
        store.prefetch("object", vec![0..64]),
        Err(Error::PrefetchBusy)
    ));
    warming.await.unwrap().unwrap();
    let before = ranges(&backend).len();
    let stats = CacheStats::default();
    assert_eq!(
        store
            .range_reader("object")
            .await
            .unwrap()
            .read_with_stats(0..193, &stats)
            .await
            .unwrap(),
        data.slice(0..193)
    );
    assert_eq!(ranges(&backend).len(), before);
    assert_eq!(stats.cache_hit_ratio(), 1.0);
    store
        .prefetch("object", vec![0..1])
        .unwrap()
        .await
        .unwrap()
        .unwrap();
}
