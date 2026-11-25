//! Deterministic cold-snapshot races. The stage is metadata-only: these tests exercise
//! checkpoint validation and WAL replay, independently of ANN scoring and file decoding.
use super::*;
use crate::{
    NamespaceState,
    snapshot::{OpenHint, catch_up_hinted},
};
use async_trait::async_trait;
use bf_wal::{Manifest, Wal, WalEntry, WriteBatch};
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    path::Path as ObjectPath,
};
use serde_json::json;
use std::{fmt, sync::atomic::AtomicUsize};
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct Gated {
    inner: object_store::memory::InMemory,
    block_manifest: AtomicBool,
    manifest_entered: Notify,
    release_manifest: Notify,
    slots: AtomicUsize,
    slot_finished: Notify,
    fail_slot: AtomicBool,
    fail_manifest: AtomicBool,
    block_slot: Mutex<Option<String>>,
    slot_entered: Notify,
    release_slot: Notify,
}
impl fmt::Display for Gated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("cold WAL race store")
    }
}
#[async_trait]
impl ObjectStore for Gated {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &ObjectPath, opts: GetOptions) -> StoreResult<GetResult> {
        if path.as_ref().ends_with("/manifest-latest")
            && self.block_manifest.swap(false, Ordering::SeqCst)
        {
            self.manifest_entered.notify_one();
            self.release_manifest.notified().await;
        }
        if self.fail_manifest.load(Ordering::SeqCst)
            && (path.as_ref().contains("/manifest/") || path.as_ref().ends_with("/manifest-latest"))
        {
            return Err(object_store::Error::Generic {
                store: "injected",
                source: "manifest unavailable".into(),
            });
        }
        let slot = path.as_ref().contains("/wal/") && !opts.head;
        let blocked = slot && self.block_slot.lock().unwrap().as_deref() == Some(path.as_ref());
        if blocked {
            self.slot_entered.notify_one();
            self.release_slot.notified().await;
        }
        let result = if slot && self.fail_slot.swap(false, Ordering::SeqCst) {
            Err(object_store::Error::Generic {
                store: "injected",
                source: "transient speculative failure".into(),
            })
        } else {
            self.inner.get_opts(path, opts).await
        };
        if slot {
            self.slots.fetch_add(1, Ordering::SeqCst);
            self.slot_finished.notify_one();
        }
        result
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, StoreResult<ObjectPath>>,
    ) -> BoxStream<'static, StoreResult<ObjectPath>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        opts: CopyOptions,
    ) -> StoreResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}
impl Gated {
    fn arm(&self) {
        self.slots.store(0, Ordering::SeqCst);
        self.block_manifest.store(true, Ordering::SeqCst);
    }
    async fn two_slots(&self) {
        while self.slots.load(Ordering::SeqCst) < 2 {
            self.slot_finished.notified().await;
        }
    }
}
struct Fixture {
    backend: Arc<Gated>,
    store: Storage,
    manager: Arc<Manager>,
    wal: Wal,
    manifest: Manifest,
    _work: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
        fs::create_dir_all(&parent).unwrap();
        let work = tempfile::Builder::new()
            .prefix("cold-wal-")
            .tempdir_in(parent)
            .unwrap();
        let backend = Arc::new(Gated::default());
        let store = Storage::from_backend(backend.clone(), ObjectPath::default(), false, false)
            .await
            .unwrap();
        let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
        let dir = SegmentFile {
            key: "ns/test/directory".into(),
            size: 0,
            metadata_len: 0,
            stats_index: None,
            text_header: None,
        };
        let set = SlabSet {
            covered: vec![],
            slabs: vec![],
            dir,
            pages: None,
            format: bf_wal::TIER_FORMAT_VERSION,
            coarse: None,
            coarse_rows: 0,
        };
        let manifest = Manifest {
            format_version: bf_wal::MANIFEST_FORMAT_VERSION,
            schema: Default::default(),
            distance_metric: Default::default(),
            id_type: "uint".parse().unwrap(),
            created_at: chrono::Utc::now(),
            indexed_through: 1,
            indexer_epoch: 0,
            deleted: false,
            segments: vec![],
            stats: Default::default(),
            text_stats: None,
            ann_dirs: Default::default(),
            ann_tiers: BTreeMap::from([("vector".into(), vec![set.clone()])]),
            lineage: vec![],
            updated_at: None,
            retired: vec![],
            checkpoints: vec![],
            pruned_through: 0,
            bulk_load_at: None,
            root: false,
        };
        let wal = Wal::new(store.clone(), "test").unwrap();
        wal.publish_manifest(1, &manifest).await.unwrap();
        let path = work.path().join("metadata-stage");
        fs::create_dir(&path).unwrap();
        let stage = Stage {
            path,
            files: HashMap::new(),
            bad: AtomicBool::new(false),
            _lease: manager.reserve(0, 0).unwrap(),
            durable: false,
        };
        Manager::install(
            &mut manager.state.lock().unwrap(),
            stage_key("", &set.dir.key, false),
            "",
            stage,
        );
        manager.note_checkpoint(
            "",
            "test",
            Some(Checkpoint {
                generation: 1,
                indexed_through: 1,
            }),
        );
        Self {
            backend,
            store,
            manager,
            wal,
            manifest,
            _work: work,
        }
    }
    async fn snapshot(&self, cap: Option<u64>) -> NamespaceState {
        let mut cache = read_cache::DecodedCache::new(1 << 20, 1);
        cache.staging = Some(self.manager.clone());
        let context = read_cache::ReadContext::new(Arc::new(cache));
        let request =
            serde_json::from_value(json!({"rank_by": ["vector", "ANN", [1., 0.]], "top_k": 1}))
                .unwrap();
        let hint = OpenHint::for_request(
            &request,
            context.clone(),
            &crate::EngineConfig::default(),
            true,
        );
        let mut state = NamespaceState::default();
        let snapshot = catch_up_hinted(&self.wal, &mut state, cap, &self.store, Some(&hint));
        assert!(
            std::mem::size_of_val(&snapshot) <= 32,
            "nested snapshot futures must stay pointer-sized"
        );
        tokio::time::timeout(Duration::from_secs(5), read_cache::scope(context, snapshot))
            .await
            .unwrap()
            .unwrap();
        state
    }
    async fn append(&self, seq: u64, tombstone: bool) {
        let mut entry = WalEntry::new(if tombstone {
            vec![]
        } else {
            vec![WriteBatch {
                upserts: vec![serde_json::from_value(json!({"id": seq, "value": "new"})).unwrap()],
                deletes: vec![],
                schema: Default::default(),
                distance_metric: Default::default(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            }]
        });
        entry.tombstone = tombstone;
        self.wal.append(seq, &entry).await.unwrap();
    }
}

#[tokio::test]
async fn empty_tail_checks_finish_while_manifest_is_blocked() {
    let f = Fixture::new().await;
    f.backend.arm();
    let (state, ()) = tokio::join!(f.snapshot(None), async {
        f.backend.manifest_entered.notified().await;
        f.backend.two_slots().await;
        f.backend.release_manifest.notify_one();
    });
    assert_eq!(state.seq, 1);
    assert!(state.exists);
    assert_eq!(
        f.backend.slots.load(Ordering::SeqCst),
        2,
        "confirmed speculation must not repeat the checks"
    );
}

#[tokio::test]
async fn acknowledged_write_and_delete_are_replayed_from_the_speculative_tail() {
    for deleted in [false, true] {
        let f = Fixture::new().await;
        f.append(2, deleted).await;
        let state = f.snapshot(None).await;
        assert_eq!(state.seq, 2);
        assert_eq!(state.exists, !deleted);
        assert_eq!(state.docs.contains_key(&bf_types::Id::Uint(2)), !deleted);
    }
}

#[tokio::test]
async fn slot_appearing_between_the_probe_and_manifest_is_seen_on_the_next_query() {
    for deleted in [false, true] {
        let f = Fixture::new().await;
        f.backend.arm();
        let (state, ()) = tokio::join!(f.snapshot(None), async {
            f.backend.manifest_entered.notified().await;
            f.backend.two_slots().await;
            // The query has already observed the missing slot, but has not got its manifest.
            f.append(2, deleted).await;
            f.backend.release_manifest.notify_one();
        });
        assert_eq!(
            state.seq, 1,
            "the concurrent write can follow this snapshot's linearization point"
        );
        assert!(state.exists);
        let next = f.snapshot(None).await;
        assert_eq!(
            next.seq, 2,
            "negative probes must never be reused across queries"
        );
        assert_eq!(next.exists, !deleted);
        assert_eq!(next.docs.contains_key(&bf_types::Id::Uint(2)), !deleted);
    }
}

#[tokio::test]
async fn changed_generation_or_checkpoint_discards_completed_probes() {
    for advanced in [false, true] {
        let f = Fixture::new().await;
        let mut manifest = f.manifest.clone();
        manifest.indexed_through = if advanced { 3 } else { 1 };
        f.wal.publish_manifest(2, &manifest).await.unwrap();
        f.backend.arm();
        let seq = manifest.indexed_through + 1;
        let (state, ()) = tokio::join!(f.snapshot(None), async {
            f.backend.manifest_entered.notified().await;
            f.backend.two_slots().await;
            f.append(seq, false).await;
            f.backend.release_manifest.notify_one();
        });
        assert_eq!(state.manifest_seq, 2);
        assert_eq!(
            state.seq, seq,
            "fallback must read after the authoritative checkpoint"
        );
        assert!(state.docs.contains_key(&bf_types::Id::Uint(seq)));
        assert!(f.backend.slots.load(Ordering::SeqCst) >= 4);
    }
}

#[tokio::test]
async fn indexed_through_must_match_even_when_the_generation_matches() {
    let f = Fixture::new().await;
    f.manager.note_checkpoint(
        "",
        "test",
        Some(Checkpoint {
            generation: 1,
            indexed_through: 9,
        }),
    );
    f.backend.arm();
    let (state, ()) = tokio::join!(f.snapshot(None), async {
        f.backend.manifest_entered.notified().await;
        f.backend.two_slots().await;
        f.append(2, false).await;
        f.backend.release_manifest.notify_one();
    });
    assert_eq!(state.seq, 2);
    assert!(state.docs.contains_key(&bf_types::Id::Uint(2)));
}

#[tokio::test]
async fn deleted_manifest_discards_a_stale_stage_and_its_negative_probe() {
    let f = Fixture::new().await;
    let mut manifest = f.manifest.clone();
    manifest.deleted = true;
    manifest.indexed_through = 2;
    manifest.ann_tiers.clear();
    f.wal.publish_manifest(2, &manifest).await.unwrap();
    let state = f.snapshot(None).await;
    assert!(!state.exists);
    assert_eq!(state.seq, 2);
    assert!(f.manager.checkpoint("", "test").is_none());
    assert!(f.manager.state.lock().unwrap().ready.is_empty());
}

#[tokio::test]
async fn speculative_read_failure_falls_back_instead_of_becoming_a_miss() {
    let f = Fixture::new().await;
    f.append(2, false).await;
    f.backend.arm();
    f.backend.fail_slot.store(true, Ordering::SeqCst);
    let (state, ()) = tokio::join!(f.snapshot(None), async {
        f.backend.manifest_entered.notified().await;
        f.backend.slot_finished.notified().await;
        f.backend.release_manifest.notify_one();
    });
    assert_eq!(state.seq, 2);
    assert!(state.docs.contains_key(&bf_types::Id::Uint(2)));
}

#[tokio::test]
async fn no_stage_and_eventual_reads_keep_the_serial_path() {
    for capped in [false, true] {
        let f = Fixture::new().await;
        if !capped {
            f.manager.remove_namespace("", "test");
        }
        f.backend.arm();
        let (state, ()) = tokio::join!(f.snapshot(capped.then_some(1 << 20)), async {
            f.backend.manifest_entered.notified().await;
            assert_eq!(f.backend.slots.load(Ordering::SeqCst), 0);
            f.backend.release_manifest.notify_one();
        });
        assert_eq!(state.seq, 1);
        assert_eq!(f.backend.slots.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn checkpoint_hints_are_bounded_and_do_not_cross_scopes() {
    let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
    fs::create_dir_all(&parent).unwrap();
    let work = tempfile::Builder::new()
        .prefix("wal-hints-")
        .tempdir_in(parent)
        .unwrap();
    let manager = Manager::open(work.path(), Some(1 << 20), Arc::default()).unwrap();
    let c = Checkpoint {
        generation: 4,
        indexed_through: 7,
    };
    manager.note_checkpoint("tenant\0", "test", Some(c));
    manager.note_snapshot(
        "tenant\0",
        "test",
        &NamespaceState {
            exists: true,
            seq: c.indexed_through,
            indexed_through: c.indexed_through,
            manifest_seq: c.generation,
            ..Default::default()
        },
    );
    assert_eq!(manager.state.lock().unwrap().snapshots.len(), 1);
    assert!(manager.snapshot("", "test", "vector").is_none());
    assert_eq!(manager.known_checkpoint("tenant\0", "test"), Some(c));
    assert_eq!(manager.known_checkpoint("", "test"), None);
    assert_eq!(
        manager.checkpoint("tenant\0", "test"),
        None,
        "metadata alone does not enable speculation"
    );
    manager.note_checkpoint(
        "tenant\0",
        "test",
        Some(Checkpoint {
            generation: 3,
            indexed_through: 6,
        }),
    );
    assert_eq!(manager.known_checkpoint("tenant\0", "test"), Some(c));
    for n in 0..MAX_STAGES {
        manager.note_checkpoint("", &n.to_string(), Some(c));
    }
    assert_eq!(manager.state.lock().unwrap().checkpoints.len(), MAX_STAGES);
    assert_eq!(manager.known_checkpoint("tenant\0", "test"), None);
    assert!(manager.state.lock().unwrap().snapshots.is_empty());
}

/// Real ANN files and a durable stage, with deterministic manifest and WAL gates.
struct SearchFixture {
    backend: Arc<Gated>,
    engine: crate::Engine,
    manager: Arc<Manager>,
    wal: Wal,
    request: bf_types::QueryRequest,
    _work: tempfile::TempDir,
}
impl SearchFixture {
    async fn new() -> Self {
        let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
        fs::create_dir_all(&parent).unwrap();
        let work = tempfile::Builder::new()
            .prefix("cold-search-")
            .tempdir_in(parent)
            .unwrap();
        let backend = Arc::new(Gated::default());
        let store = Storage::from_backend_with_cache(
            backend.clone(),
            ObjectPath::default(),
            false,
            false,
            bf_storage::CacheConfig {
                memory_bytes: 8 << 20,
                disk_bytes: 32 << 20,
                disk_dir: Some(work.path().join("cache")),
                namespace: Some(uuid::Uuid::new_v4().to_string()),
                block_size: 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let wal = Wal::new(store.clone(), "test").unwrap();
        let engine = crate::Engine::with_config(
            store,
            crate::EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                query_concurrency: 1,
                decoded_cache_bytes: 16 << 20,
                ..Default::default()
            },
        );
        let rows: Vec<_> = (0..128)
            .map(|id| {
                json!({
                    "id": id, "vector": [id as f32, 1.0], "category": id % 2
                })
            })
            .collect();
        engine
            .write(
                "test",
                serde_json::from_value(json!({
                    "distance_metric": "euclidean_squared", "upsert_rows": rows
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("test", true).await.unwrap();
        engine
            .recluster(
                "test",
                &crate::recluster::ReclusterOptions {
                    attribute: "vector".into(),
                    cells: 4,
                    cluster_rows: 16,
                    max_slab_rows: 64,
                    scratch: work.path().join("build"),
                },
            )
            .await
            .unwrap();
        let request: bf_types::QueryRequest = serde_json::from_value(json!({
            "rank_by": ["vector", "ANN", [17.0, 1.0]], "top_k": 5,
            "include_attributes": false
        }))
        .unwrap();
        engine.drop_caches().await;
        engine.query("test", request.clone()).await.unwrap();
        let manager = engine.0.read_cache.staging.clone().unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let done = {
                    let state = manager.state.lock().unwrap();
                    !state.ready.is_empty() && state.pending.is_empty()
                };
                if done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        engine.drop_caches().await;
        assert!(manager.snapshot("", "test", "vector").is_some());
        Self {
            backend,
            engine,
            manager,
            wal,
            request,
            _work: work,
        }
    }
    async fn query(&self) -> Result<bf_types::QueryResponse, bf_types::ApiError> {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.engine.query("test", self.request.clone()),
        )
        .await
        .unwrap()
    }
    async fn search_started(&self) {
        let state = self.manager.snapshot("", "test", "vector").unwrap();
        let key = &state.ann_tiers["vector"][0].dir.key;
        tokio::time::timeout(Duration::from_secs(5), async {
            while self
                .engine
                .0
                .read_cache
                .get::<bf_vector::Directory>(key)
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("routing must start while the manifest is blocked");
    }
}
fn rows(response: &bf_types::QueryResponse) -> serde_json::Value {
    serde_json::to_value(&response.result.rows).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_search_routes_before_manifest_and_matches_the_confirmed_snapshot() {
    let f = SearchFixture::new().await;
    f.backend.arm();
    let (result, ()) = tokio::join!(f.query(), async {
        f.backend.manifest_entered.notified().await;
        f.search_started().await;
        f.backend.two_slots().await;
        f.backend.release_manifest.notify_one();
    });
    let expected = f.query().await.unwrap();
    assert_eq!(rows(&result.unwrap()), rows(&expected));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_search_discards_stale_rows_for_writes_deletes_and_filters() {
    for during in [false, true] {
        let mut f = SearchFixture::new().await;
        f.request.filters = Some(serde_json::from_value(json!(["category", "Eq", 1])).unwrap());
        // Publish through a separate engine so the reader remains cold.
        let writer = crate::Engine::with_config(
            f.engine.storage().clone(),
            crate::EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                ..Default::default()
            },
        );
        let write = || {
            writer.write(
                "test",
                serde_json::from_value(json!({
                    "upsert_rows": [{"id": 1000, "vector": [17.0, 1.0], "category": 1},
                                    {"id": 17, "vector": [500.0, 1.0], "category": 0}],
                    "deletes": [19]
                }))
                .unwrap(),
            )
        };
        if !during {
            write().await.unwrap();
        }
        f.backend.arm();
        let (result, ()) = tokio::join!(f.query(), async {
            f.backend.manifest_entered.notified().await;
            if during {
                // Hold the speculative WAL before it observes the concurrent write.
                // The read already dispatched at t=0 may miss; either linearization is valid.
                write().await.unwrap();
            }
            f.backend.release_manifest.notify_one();
        });
        let result = result.unwrap();
        f.engine.drop_caches().await;
        let next = f.query().await.unwrap();
        if !during
            || result
                .result
                .rows
                .iter()
                .flatten()
                .any(|r| r["id"] == json!(1000))
        {
            assert_eq!(rows(&result), rows(&next));
        }
        assert!(
            next.result
                .rows
                .iter()
                .flatten()
                .any(|r| r["id"] == json!(1000))
        );
        assert!(
            next.result
                .rows
                .iter()
                .flatten()
                .all(|r| r["id"] != json!(17) && r["id"] != json!(19))
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_search_revalidates_generation_checkpoint_and_speculative_errors() {
    for change in ["generation", "checkpoint", "error"] {
        let f = SearchFixture::new().await;
        let mut stale = (*f.manager.snapshot("", "test", "vector").unwrap()).clone();
        if change == "generation" {
            let (seq, manifest) = f.wal.latest_manifest().await.unwrap().unwrap();
            f.wal.publish_manifest(seq + 1, &manifest).await.unwrap();
        } else if change == "checkpoint" {
            // Independently corrupt the advisory indexed-through field at the same generation.
            stale.seq += 10;
            stale.indexed_through += 10;
            f.manager.note_checkpoint(
                "",
                "test",
                Some(Checkpoint {
                    generation: stale.manifest_seq,
                    indexed_through: stale.indexed_through,
                }),
            );
            f.manager.note_snapshot("", "test", &stale);
        } else {
            // An obsolete schema can make speculative validation fail. The confirmed search wins.
            stale.schema.clear();
            assert!(
                f.engine
                    .search_snapshot(
                        &Arc::new(stale.clone()),
                        &f.request,
                        read_cache::ReadContext::new(f.engine.0.read_cache.clone()),
                        false,
                        true
                    )
                    .await
                    .is_err()
            );
            f.manager.note_snapshot("", "test", &stale);
        }
        let result = f.query().await.unwrap();
        f.engine.drop_caches().await;
        let expected = f.query().await.unwrap();
        assert_eq!(rows(&result), rows(&expected), "{change}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_namespace_and_manifest_failures_never_return_speculative_hits() {
    for deleted in [false, true] {
        let f = SearchFixture::new().await;
        if deleted {
            let (seq, mut manifest) = f.wal.latest_manifest().await.unwrap().unwrap();
            manifest.deleted = true;
            manifest.indexed_through += 1;
            manifest.ann_tiers.clear();
            f.wal.publish_manifest(seq + 1, &manifest).await.unwrap();
        } else {
            f.backend.fail_manifest.store(true, Ordering::SeqCst);
        }
        let error = f
            .query()
            .await
            .expect_err("a speculative reply cannot hide snapshot failure");
        assert_eq!(error.status, if deleted { 404 } else { 503 });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_cold_search_releases_worker_permits_and_stage_leases() {
    let f = SearchFixture::new().await;
    f.backend.arm();
    let engine = f.engine.clone();
    let request = f.request.clone();
    let task = tokio::spawn(async move { engine.query("test", request).await });
    f.backend.manifest_entered.notified().await;
    f.search_started().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        let _permit = f.engine.0.queries.acquire().await.unwrap();
        loop {
            let active = {
                let state = f.manager.state.lock().unwrap();
                state
                    .ready
                    .values()
                    .any(|(_, stage)| Arc::strong_count(stage) > 1)
            };
            if !active {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The blocked manifest future was dropped, so the namespace lock is usable again.
    f.backend.release_manifest.notify_one();
    f.query().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_blocked_io_drops_the_actual_worker_future() {
    let f = Fixture::new().await;
    let lease = f.manager.reserve(19, 1).unwrap();
    let permit = Arc::new(tokio::sync::Semaphore::new(1))
        .acquire_owned()
        .await
        .unwrap();
    let permits = permit.semaphore().clone();
    let started = Arc::new(Notify::new());
    let notify = started.clone();
    let task = tokio::spawn(crate::query_worker::run_io_cancellable(
        tokio::runtime::Handle::current(),
        async move {
            let (_lease, _permit) = (lease, permit);
            notify.notify_one();
            std::future::pending::<()>().await;
        },
    ));
    started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        let _permit = permits.acquire().await.unwrap();
        while f.manager.state.lock().unwrap().used != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_writes_and_tombstones_before_or_during_search_override_speculative_hits() {
    for during in [false, true] {
        for tombstone in [false, true] {
            let f = SearchFixture::new().await;
            let state = f.manager.snapshot("", "test", "vector").unwrap();
            let seq = state.seq + 1;
            let append = || async {
                let mut entry = WalEntry::new(if tombstone {
                    vec![]
                } else {
                    vec![WriteBatch {
                        upserts: vec![
                            serde_json::from_value(json!({
                                "id": 1000, "vector": [17.0, 1.0], "category": 1
                            }))
                            .unwrap(),
                        ],
                        deletes: vec![bf_types::Id::Uint(17)],
                        schema: state.schema.clone(),
                        distance_metric: state.metric,
                        created_at: state.created_at,
                        updated_at: chrono::Utc::now(),
                    }]
                });
                entry.tombstone = tombstone;
                f.wal.append(seq, &entry).await.unwrap();
            };
            if !during {
                append().await;
            }
            f.backend.arm();
            if during {
                *f.backend.block_slot.lock().unwrap() = Some(f.wal.key(seq));
            }
            let (result, ()) = tokio::join!(f.query(), async {
                f.backend.manifest_entered.notified().await;
                f.search_started().await;
                if during {
                    f.backend.slot_entered.notified().await;
                    append().await;
                    *f.backend.block_slot.lock().unwrap() = None;
                    f.backend.release_slot.notify_waiters();
                }
                f.backend.release_manifest.notify_one();
            });
            if tombstone {
                assert_eq!(result.err().unwrap().status, 404);
                assert!(f.manager.snapshot("", "test", "vector").is_none());
            } else {
                let result = result.unwrap();
                let found = result.result.rows.as_ref().unwrap();
                assert_eq!(found[0]["id"], json!(1000));
                assert!(found.iter().all(|row| row["id"] != json!(17)));
                f.engine.drop_caches().await;
                assert_eq!(rows(&result), rows(&f.query().await.unwrap()));
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_error_cancels_a_search_waiting_for_a_worker_slot() {
    let f = SearchFixture::new().await;
    let permit = f.engine.0.queries.acquire().await.unwrap();
    f.backend.fail_manifest.store(true, Ordering::SeqCst);
    assert_eq!(f.query().await.unwrap_err().status, 503);
    drop(permit);
    assert_eq!(f.engine.0.queries.available_permits(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn speculative_snapshot_requires_the_requested_healthy_stage_and_storage_scope() {
    let f = SearchFixture::new().await;
    assert!(f.manager.snapshot("other\0", "test", "vector").is_none());
    assert!(f.manager.snapshot("", "test", "unstaged-vector").is_none());
    for (_, stage) in f.manager.state.lock().unwrap().ready.values() {
        stage.bad.store(true, Ordering::Relaxed);
    }
    assert!(f.manager.snapshot("", "test", "vector").is_none());
    f.manager.remove_namespace("", "test");
    assert!(f.manager.state.lock().unwrap().snapshots.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_on_the_http_reactor_also_cancels_the_nested_speculative_worker() {
    let f = SearchFixture::new().await;
    f.backend.arm();
    let engine = f.engine.clone();
    let request = f.request.clone();
    let io = tokio::runtime::Handle::current();
    let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
    let reactor = tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(crate::query_worker::on_query_reactor(io, async move {
                tokio::select! {
                    result = engine.query("test", request) => panic!("query finished before cancellation: {result:?}"),
                    _ = cancelled => {}
                }
            }));
    });
    f.backend.manifest_entered.notified().await;
    f.search_started().await;
    cancel.send(()).unwrap();
    reactor.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let _permit = f.engine.0.queries.acquire().await.unwrap();
        let handle = f.engine.handle("test").await.unwrap();
        let _state = handle.state.lock().await;
    })
    .await
    .expect("both nested blocking futures must release their resources");
    f.backend.release_manifest.notify_one();
    f.query().await.unwrap();
}
