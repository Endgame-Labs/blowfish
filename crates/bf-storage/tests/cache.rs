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
    stall_last: AtomicBool,
    last_started: tokio::sync::Notify,
    resume_last: tokio::sync::Notify,
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
        if self.stall_last.load(Ordering::SeqCst)
            && matches!(&opts.range, Some(object_store::GetRange::Bounded(r)) if r.start >= 512)
        {
            self.last_started.notify_one();
            self.resume_last.notified().await;
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
async fn direct_provisioning_ranges_are_exact_uncached_and_bounded() {
    let (store, backend, data) = setup(CacheConfig {
        fetch_concurrency: 2,
        ..config()
    })
    .await;
    // A portable create header must stay invisible to provisioning callers.
    let writable =
        Storage::from_backend_with_cache(backend.clone(), Path::default(), false, false, config())
            .await
            .unwrap();
    writable
        .create_unique("portable", data.clone())
        .await
        .unwrap();
    drop(writable);
    let reader = store
        .range_reader_with_length("portable", data.len() as u64, true)
        .unwrap();
    backend.ranges.lock().unwrap().clear();
    backend.max_active.store(0, Ordering::SeqCst);
    backend.delay.store(true, Ordering::SeqCst);
    let output = futures::future::try_join_all((0..8).map(|_| reader.read_direct(7..93)))
        .await
        .unwrap();
    assert!(output.iter().all(|b| b == &data.slice(7..93)));
    assert_eq!(ranges(&backend), vec![31..117; 8]);
    assert_eq!(backend.max_active.load(Ordering::SeqCst), 2);
    assert!(!reader.is_memory_resident(7..93));
    assert!(
        reader
            .read_direct(Range { start: 9, end: 8 })
            .await
            .is_err()
    );
    assert!(reader.read_direct(0..data.len() as u64 + 1).await.is_err());
    assert!(reader.read_direct(5..5).await.unwrap().is_empty());
    assert_eq!(ranges(&backend).len(), 8);
    // The identity comes from the same response, matching a HEAD, with no extra request.
    let (bytes, identity) = reader.read_direct_identified(7..93).await.unwrap();
    assert_eq!(bytes, data.slice(7..93));
    assert_eq!(identity, store.object_identity("portable").await.unwrap());
    assert!(reader.read_direct_identified(5..5).await.is_err());
    backend.ranges.lock().unwrap().pop();
    backend.short.store(true, Ordering::SeqCst);
    assert!(reader.read_direct(7..93).await.is_err());
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
    assert_eq!(backend.max_active.load(Ordering::SeqCst), 2);
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
    store.flush_cache().await;
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
    store.flush_cache().await;
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

#[tokio::test]
async fn a_waiter_finishes_shared_fetch_after_the_original_reader_cancels() {
    let (store, backend, data) = setup(config()).await;
    backend.delay.store(true, Ordering::SeqCst);
    let reader = store
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap();
    let original = {
        let reader = reader.clone();
        tokio::spawn(async move { reader.read(0..192).await })
    };
    while backend.active.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let stats = Arc::new(CacheStats::default());
    let waiter = {
        let reader = reader.clone();
        let stats = stats.clone();
        tokio::spawn(async move { reader.read_with_stats(128..320, &stats).await })
    };
    while stats.snapshot().missed_bytes == 0 {
        tokio::task::yield_now().await;
    }
    original.abort();
    assert!(original.await.unwrap_err().is_cancelled());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        data.slice(128..320)
    );
    let mut got = ranges(&backend);
    got.sort_by_key(|r| r.start);
    assert_eq!(got, vec![0..192, 192..320]);
}

#[tokio::test]
async fn metadata_header_failures_are_not_retained_and_length_errors_are_safe() {
    let (store, backend, _) = setup(config()).await;
    backend.fail.store(true, Ordering::SeqCst);
    assert!(store.range_reader("object").await.is_err());
    backend.short.store(true, Ordering::SeqCst);
    assert!(store.range_reader("object").await.is_err());
    let reader = store.range_reader("object").await.unwrap();
    assert_eq!(reader.len(), 1027);
    assert!(reader.read_batch(&[]).await.unwrap().is_empty());
    assert!(
        store
            .range_reader_with_length("object", u64::MAX, true)
            .is_err()
    );
    assert!(matches!(
        store
            .range_reader_with_length("missing", 1, false)
            .unwrap()
            .read(0..1)
            .await,
        Err(Error::NotFound)
    ));
    let backend = Arc::new(Counting::default());
    for cfg in [
        CacheConfig {
            block_size: 0,
            ..config()
        },
        CacheConfig {
            fetch_concurrency: 0,
            ..config()
        },
        CacheConfig {
            prefetch_concurrency: usize::MAX,
            ..config()
        },
        CacheConfig {
            max_coalesce_blocks: usize::MAX,
            ..config()
        },
        CacheConfig {
            disk_dir: Some(tempfile::tempdir().unwrap().path().to_owned()),
            namespace: None,
            ..config()
        },
    ] {
        assert!(
            Storage::from_backend_with_cache(backend.clone(), Path::default(), true, false, cfg)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn fresh_memory_backends_never_reuse_persistent_cache_identity() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = bf_storage::StorageConfig {
        url: "memory://".into(),
        cache: CacheConfig {
            disk_dir: Some(dir.path().to_owned()),
            ..config()
        },
        ..Default::default()
    };
    let first = Storage::open(&cfg).await.unwrap();
    first.create("same", "first").await.unwrap();
    assert_eq!(first.get_cached("same").await.unwrap(), "first");
    let second = Storage::open(&cfg).await.unwrap();
    second.create("same", "other").await.unwrap();
    assert_eq!(second.get_cached("same").await.unwrap(), "other");
}

#[tokio::test]
async fn unavailable_disk_writes_do_not_fail_source_reads() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _, data) = setup(CacheConfig {
        disk_dir: Some(dir.path().to_owned()),
        namespace: Some("unavailable-disk".into()),
        memory_bytes: 0,
        ..config()
    })
    .await;
    let subdir = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::rename(&subdir, dir.path().join("moved")).unwrap();
    let reader = store
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap();
    assert_eq!(reader.read(0..128).await.unwrap(), data.slice(0..128));
}

#[tokio::test]
async fn gap_coalescing_merges_nearby_blocks_into_one_get() {
    let (store, backend, data) = setup(CacheConfig {
        block_size: 64,
        max_coalesce_blocks: 16,
        coalesce_gap_blocks: 2,
        ..config()
    })
    .await;
    let reader = store
        .range_reader_with_length("object", data.len() as u64, false)
        .unwrap();
    // Blocks 0, 2, and 5 are separated by gaps of 1 and 2 blocks: one GET covers them.
    let got = reader
        .read_batch(&[0..10, 130..140, 330..340])
        .await
        .unwrap();
    assert_eq!(got[0], data.slice(0..10));
    assert_eq!(got[1], data.slice(130..140));
    assert_eq!(got[2], data.slice(330..340));
    assert_eq!(ranges(&backend).len(), 1);
    // Block 9 is more than two blocks away from block 5: a second GET.
    reader.read(600..610).await.unwrap();
    assert_eq!(ranges(&backend).len(), 2);
}

#[tokio::test]
async fn a_reserved_batch_coalesces_but_does_not_hold_ready_leaf_readers() {
    let (store, backend, data) = setup(config()).await;
    let reader = store.range_reader("object").await.unwrap();
    backend.ranges.lock().unwrap().clear();
    backend.stall_last.store(true, Ordering::SeqCst);
    let (ready, reserved) = tokio::sync::oneshot::channel();
    let bulk = reader.clone();
    let task = tokio::spawn(async move {
        bulk.read_batch_notifying(&[0..64, 64..128, 512..576], &CacheStats::default(), ready)
            .await
    });
    reserved.await.unwrap();
    backend.last_started.notified().await;
    let bytes = tokio::time::timeout(Duration::from_secs(2), reader.read(0..128))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes, data.slice(0..128));
    assert!(
        !task.is_finished(),
        "one slow range must not hold an independent leaf"
    );
    let tail_reader = reader.clone();
    let tail = tokio::spawn(async move { tail_reader.read(512..576).await });
    // Poll the tail once so it owns a shared flight before canceling the planner.
    tokio::task::yield_now().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    backend.resume_last.notify_one();
    assert_eq!(tail.await.unwrap().unwrap(), data.slice(512..576));
    let mut got = ranges(&backend);
    got.sort_by_key(|r| r.start);
    assert_eq!(got, vec![0..128, 512..576]);
}

#[tokio::test]
async fn reserved_batches_dispatch_in_input_priority_order() {
    let (store, backend, data) = setup(CacheConfig {
        fetch_concurrency: 1,
        ..config()
    })
    .await;
    let reader = store.range_reader("object").await.unwrap();
    backend.ranges.lock().unwrap().clear();
    let (ready, reserved) = tokio::sync::oneshot::channel();
    let result = reader
        .read_batch_notifying(&[512..576, 0..64, 64..128], &CacheStats::default(), ready)
        .await
        .unwrap();
    reserved.await.unwrap();
    assert_eq!(
        result,
        vec![data.slice(512..576), data.slice(0..64), data.slice(64..128)]
    );
    assert_eq!(ranges(&backend), vec![512..576, 0..128]);
}

#[tokio::test]
async fn memory_residency_is_advisory_and_per_range() {
    let (store, backend, _) = setup(config()).await;
    let reader = store.range_reader("object").await.unwrap();
    backend.ranges.lock().unwrap().clear();
    assert!(reader.is_memory_resident(0..0));
    assert!(!reader.is_memory_resident(0..64));
    assert!(!reader.is_memory_resident(1027..1028));
    assert!(ranges(&backend).is_empty());
    reader.read(0..64).await.unwrap();
    assert!(reader.is_memory_resident(0..64));
    assert!(!reader.is_memory_resident(0..65));
}

#[tokio::test]
async fn exact_ranges_fetch_only_their_bytes_merge_small_gaps_and_repeat_from_cache() {
    let (store, backend, data) = setup(CacheConfig {
        memory_bytes: 1 << 20,
        ..config()
    })
    .await;
    let reader = store.range_reader("object").await.unwrap();
    backend.ranges.lock().unwrap().clear();
    let stats = CacheStats::default();
    // 100..130 and 140..160 are 10 apart (merged under a gap of 16); 400..410 is far; the
    // duplicate and the empty range come back in input order.
    let wanted = [140..160, 400..410, 100..130, 400..410, 7..7];
    let got = reader
        .read_exact_batch_with_stats(&wanted, 16, 1 << 20, &stats)
        .await
        .unwrap();
    for (range, bytes) in wanted.iter().zip(&got) {
        assert_eq!(bytes, &data.slice(range.start as usize..range.end as usize));
    }
    let mut fetched = ranges(&backend);
    fetched.sort_by_key(|r| r.start);
    assert_eq!(fetched, vec![100..160, 400..410]);
    assert_eq!(stats.snapshot().missed_bytes, 30 + 20 + 10);
    // The same read again, and any sub-merge of it, is served from the exact pieces.
    let again = CacheStats::default();
    let got2 = reader
        .read_exact_batch_with_stats(&[100..130, 140..160], 0, 1 << 20, &again)
        .await
        .unwrap();
    assert_eq!(got2[0], data.slice(100..130));
    assert_eq!(got2[1], data.slice(140..160));
    assert_eq!(ranges(&backend).len(), 2);
    assert_eq!(again.snapshot().missed_bytes, 0);
    assert_eq!(again.snapshot().memory_bytes, 50);
    // A merged read never exceeds `max`.
    backend.ranges.lock().unwrap().clear();
    reader
        .read_exact_batch_with_stats(&[600..620, 630..650, 660..680], 16, 50, &stats)
        .await
        .unwrap();
    let mut fetched = ranges(&backend);
    fetched.sort_by_key(|r| r.start);
    assert_eq!(fetched, vec![600..650, 660..680]);
    assert!(
        reader
            .read_exact_batch_with_stats(
                std::slice::from_ref(&(0..data.len() as u64 + 1)),
                0,
                1,
                &stats
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn exact_ranges_use_blocks_that_are_already_in_memory() {
    let (store, backend, data) = setup(CacheConfig {
        memory_bytes: 1 << 20,
        ..config()
    })
    .await;
    let reader = store.range_reader("object").await.unwrap();
    reader.read(0..256).await.unwrap();
    backend.ranges.lock().unwrap().clear();
    let got = reader
        .read_exact_batch_with_stats(
            std::slice::from_ref(&(10..200)),
            0,
            1 << 20,
            &CacheStats::default(),
        )
        .await
        .unwrap();
    assert_eq!(got[0], data.slice(10..200));
    assert!(ranges(&backend).is_empty());
}

#[tokio::test]
async fn concurrent_exact_reads_of_one_range_share_one_request() {
    let (store, backend, data) = setup(CacheConfig {
        memory_bytes: 1 << 20,
        ..config()
    })
    .await;
    let reader = store.range_reader("object").await.unwrap();
    backend.ranges.lock().unwrap().clear();
    backend.delay.store(true, Ordering::SeqCst);
    let (ready, reserved) = tokio::sync::oneshot::channel();
    let (stats_a, stats_b) = (CacheStats::default(), CacheStats::default());
    let wanted = [300..420, 500..520];
    let first = reader.read_exact_batch_notifying(&wanted, 0, 1 << 20, &stats_a, ready);
    let second = async {
        // Starts after the first reserved its misses: it must attach to them, not refetch.
        reserved.await.unwrap();
        reader
            .read_exact_batch_with_stats(&[500..520, 300..420], 0, 1 << 20, &stats_b)
            .await
    };
    let (a, b) = tokio::join!(first, second);
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a[0], data.slice(300..420));
    assert_eq!(b[0], data.slice(500..520));
    assert_eq!(b[1], data.slice(300..420));
    let mut fetched = ranges(&backend);
    fetched.sort_by_key(|r| r.start);
    assert_eq!(fetched, vec![300..420, 500..520]);
    assert!(reader.is_memory_resident(300..420));
}
