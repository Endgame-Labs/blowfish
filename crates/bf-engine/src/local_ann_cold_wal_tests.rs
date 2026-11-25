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
        let slot = path.as_ref().contains("/wal/") && !opts.head;
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
}
