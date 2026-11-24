//! Pipelined WAL commits: payloads upload concurrently, slots commit in order, and no writer is
//! acknowledged before its own slot exists.
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
    collections::BTreeSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

/// An in-memory store where payload uploads finish in reverse order of their start, and a chosen
/// slot's commit can be made to fail once.
#[derive(Debug, Default)]
struct Slow {
    inner: object_store::memory::InMemory,
    uploads: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    fail_slot: AtomicU64,
}
impl fmt::Display for Slow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("slow store")
    }
}
#[async_trait]
impl ObjectStore for Slow {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let name = path.as_ref();
        if name.contains("/wal-data/") {
            let n = self.uploads.fetch_add(1, Ordering::SeqCst);
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(5 + 12 * (7 - (n % 8)) as u64)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        } else if name.contains("/wal/") {
            let seq: u64 = name
                .rsplit('/')
                .next()
                .and_then(|n| n.strip_suffix(".wal"))
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            if seq != 0
                && self
                    .fail_slot
                    .compare_exchange(seq, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Err(object_store::Error::Generic {
                    store: "slow",
                    source: "injected slot failure".into(),
                });
            }
        }
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

fn config() -> EngineConfig {
    EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        ..Default::default()
    }
}
async fn engine(backend: &Arc<Slow>) -> (Engine, Storage) {
    let store = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    (Engine::with_config(store.clone(), config()), store)
}
/// A row big enough that its group's payload is staged as its own object (over 64 KiB).
fn row(id: u64, extra: serde_json::Value) -> serde_json::Value {
    let mut row = json!({"id": id, "pad": "x".repeat(70_000)});
    row.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    row
}
async fn ids(engine: &Engine, ns: &str) -> BTreeSet<u64> {
    let response = engine
        .query(
            ns,
            serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":1000})).unwrap(),
        )
        .await
        .unwrap();
    response
        .result
        .rows
        .unwrap()
        .iter()
        .map(|r| r.get("id").and_then(|v| v.as_u64()).unwrap())
        .collect()
}
async fn payload_objects(store: &Storage, ns: &str) -> usize {
    store
        .list(&format!("ns/{ns}/wal-data/"))
        .await
        .unwrap()
        .len()
}
async fn slots(store: &Storage, ns: &str) -> usize {
    store.list(&format!("ns/{ns}/wal/")).await.unwrap().len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_groups_upload_together_and_commit_in_order() {
    let backend = Arc::new(Slow::default());
    let (engine, store) = engine(&backend).await;
    let mut tasks = vec![];
    for i in 0..24u64 {
        // Staggered, so later writes form new groups while earlier payloads are still uploading.
        tokio::time::sleep(Duration::from_millis(10)).await;
        tasks.push({
            let engine = engine.clone();
            tokio::spawn(async move {
                engine
                    .write(
                        "ordered",
                        serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(i, json!({}))]})).unwrap(),
                    )
                    .await
            })
        });
    }
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    assert!(
        backend.max_in_flight.load(Ordering::SeqCst) > 1,
        "payload uploads overlapped"
    );
    // A fresh node replays the log: every acknowledged write is there, with contiguous slots.
    let (fresh, _) = engine_fresh(&backend).await;
    assert_eq!(ids(&fresh, "ordered").await, (0..24).collect());
    let n = slots(&store, "ordered").await;
    assert_eq!(payload_objects(&store, "ordered").await, n);
}
async fn engine_fresh(backend: &Arc<Slow>) -> (Engine, Storage) {
    engine(backend).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_writes_see_groups_that_are_still_in_flight() {
    let backend = Arc::new(Slow::default());
    let (engine, _) = engine(&backend).await;
    engine
        .write(
            "cond",
            serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(1, json!({"v": -1}))]})).unwrap(),
        )
        .await
        .unwrap();
    // Each write replaces the row only when its version is higher than the stored one. Whatever
    // the interleaving, the highest version must win, which needs every write to be evaluated
    // against the effect of the groups ahead of it, committed or not.
    let tasks: Vec<_> = (0..20i64)
        .map(|v| {
            let engine = engine.clone();
            tokio::spawn(async move {
                engine
                    .write(
                        "cond",
                        serde_json::from_value(json!({
                            "schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(1, json!({"v": v}))],
                            "upsert_condition":["v","Lt",{"$ref_new":"v"}]
                        }))
                        .unwrap(),
                    )
                    .await
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    for node in [engine.clone(), engine_fresh(&backend).await.0] {
        let rows = node
            .query(
                "cond",
                serde_json::from_value(
                    json!({"rank_by":["id","asc"],"top_k":1,"include_attributes":["v"]}),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .result
            .rows
            .unwrap();
        assert_eq!(rows[0].get("v").and_then(|v| v.as_i64()), Some(19));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_slot_fails_every_later_group_and_leaves_no_visible_or_orphaned_writes() {
    let backend = Arc::new(Slow::default());
    let (engine, store) = engine(&backend).await;
    engine
        .write(
            "crash",
            serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(1000, json!({}))]})).unwrap(),
        )
        .await
        .unwrap();
    // The commit of the first slot of the burst fails once, while later groups are in flight.
    backend.fail_slot.store(2, Ordering::SeqCst);
    let mut tasks = vec![];
    for i in 0..12u64 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        tasks.push({
            let engine = engine.clone();
            tokio::spawn(async move {
                let r = engine
                    .write(
                        "crash",
                        serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(i, json!({}))]})).unwrap(),
                    )
                    .await;
                (i, r.is_ok())
            })
        });
    }
    let mut acked = BTreeSet::from([1000u64]);
    let mut failed = 0;
    for t in tasks {
        let (i, ok) = t.await.unwrap();
        if ok {
            acked.insert(i);
        } else {
            failed += 1;
        }
    }
    assert!(failed >= 1, "the injected failure reached a writer");
    let (fresh, _) = engine_fresh(&backend).await;
    assert_eq!(
        ids(&fresh, "crash").await,
        acked,
        "exactly the acknowledged writes are visible"
    );
    assert_eq!(ids(&engine, "crash").await, acked, "the live node resynced");
    // The node keeps working afterwards.
    engine
        .write(
            "crash",
            serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(2000, json!({}))]})).unwrap(),
        )
        .await
        .unwrap();
    // Abandoned payloads are removed in the background; poll with a deadline instead of a fixed
    // sleep, so a busy machine doesn't fail the test.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let (payloads, slots) = (
            payload_objects(&store, "crash").await,
            slots(&store, "crash").await,
        );
        if payloads == slots || tokio::time::Instant::now() >= deadline {
            assert_eq!(
                payloads, slots,
                "abandoned payloads of the failed groups were removed"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_writers_racing_lose_nothing() {
    let backend = Arc::new(Slow::default());
    let (a, _) = engine(&backend).await;
    let (b, _) = engine(&backend).await;
    a.write(
        "race2",
        serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(9999, json!({}))]})).unwrap(),
    )
    .await
    .unwrap();
    let mut tasks = vec![];
    for i in 0..16u64 {
        for (side, node) in [(0u64, a.clone()), (1, b.clone())] {
            tasks.push(tokio::spawn(async move {
                node.write(
                    "race2",
                    serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(side * 100 + i, json!({}))]}))
                        .unwrap(),
                )
                .await
            }));
        }
    }
    let mut acked = BTreeSet::from([9999u64]);
    for (n, t) in tasks.into_iter().enumerate() {
        let i = (n / 2) as u64;
        let side = (n % 2) as u64;
        if t.await.unwrap().is_ok() {
            acked.insert(side * 100 + i);
        }
    }
    let (fresh, _) = engine_fresh(&backend).await;
    assert_eq!(ids(&fresh, "race2").await, acked);
    assert!(acked.len() > 20, "most writes succeed: {}", acked.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncommitted_staged_payload_is_reclaimed_once_its_slot_is_checkpointed() {
    let backend = Arc::new(Slow::default());
    let (engine, store) = engine(&backend).await;
    engine
        .write(
            "orphan",
            serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(1, json!({}))]})).unwrap(),
        )
        .await
        .unwrap();
    // A crashed pipeline leaves a payload staged for slot 2, which another write then takes.
    let wal = bf_wal::Wal::new(store.clone(), "orphan").unwrap();
    let lost = bf_wal::WalEntry::new(vec![]);
    wal.stage(2, &{
        let mut e = lost.clone();
        e.batches = wal_batch();
        e
    })
    .await
    .unwrap();
    engine
        .write(
            "orphan",
            serde_json::from_value(json!({"schema":{"pad":{"type":"string","filterable":false}},"upsert_rows":[row(2, json!({}))]})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        payload_objects(&store, "orphan").await,
        3,
        "two live payloads and one orphan"
    );
    wal.prune_payloads(2).await.unwrap();
    assert_eq!(payload_objects(&store, "orphan").await, 0);
}
fn wal_batch() -> Vec<bf_wal::WriteBatch> {
    use bf_types::*;
    vec![bf_wal::WriteBatch {
        upserts: vec![serde_json::from_value(json!({"id": 5, "pad": "y".repeat(70_000)})).unwrap()],
        deletes: vec![],
        schema: Schema::new(),
        distance_metric: DistanceMetric::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }]
}
