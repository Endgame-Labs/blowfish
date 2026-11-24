use bf_storage::{CreateResult, Storage, StorageConfig};
use bf_wal::*;

#[tokio::test]
async fn versioned_slots_conflicts_tail_and_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StorageConfig {
        url: format!("file://{}", dir.path().display()),
        ..Default::default()
    };
    let a = Wal::new(Storage::open(&cfg).await.unwrap(), "shared").unwrap();
    let b = Wal::new(Storage::open(&cfg).await.unwrap(), "shared").unwrap();
    let first = WalEntry::new(vec![]);
    let second = WalEntry::new(vec![]);
    let (x, y) = tokio::join!(a.append(1, &first), b.append(1, &second));
    assert!(matches!(
        (x.unwrap(), y.unwrap()),
        (CreateResult::Created, CreateResult::Conflict)
            | (CreateResult::Conflict, CreateResult::Created)
    ));
    assert_eq!(a.head().await.unwrap(), 1);
    let entry = a.read(1).await.unwrap().unwrap();
    assert_eq!(
        a.append(1, &entry).await.unwrap(),
        CreateResult::AlreadyCommitted
    );
    assert_eq!(
        a.append(2, &WalEntry::new(vec![])).await.unwrap(),
        CreateResult::Created
    );
    assert_eq!(b.tail(0).await.unwrap().len(), 2);
    assert!(WalEntry::decode(&[99]).is_err());
    assert_eq!(
        WalEntry::decode(&entry.encode().unwrap()).unwrap().write_id,
        entry.write_id
    );
    let manifest = Manifest {
        format_version: 1,
        schema: Default::default(),
        distance_metric: Default::default(),
        id_type: "uint".parse().unwrap(),
        created_at: chrono::Utc::now(),
        indexed_through: 0,
        indexer_epoch: 0,
        deleted: false,
        segments: vec![],
        stats: Default::default(),
        text_stats: None,
        ann_dirs: Default::default(),
        ann_tiers: Default::default(),
        lineage: Default::default(),
        updated_at: None,
        retired: vec![],
        checkpoints: vec![],
        pruned_through: 0,
        bulk_load_at: None,
        root: false,
    };
    a.publish_manifest(1, &manifest).await.unwrap();
    assert_eq!(b.latest_manifest().await.unwrap().unwrap().0, 1);
    assert!(b.manifest_after(1).await.unwrap().is_none());
    let store = Storage::open(&cfg).await.unwrap();
    let copy_of_one = store.get("ns/shared/manifest-latest").await.unwrap();
    a.publish_manifest(2, &manifest).await.unwrap();
    assert_eq!(b.manifest_after(1).await.unwrap().unwrap().0, 2);
    assert!(b.manifest_after(2).await.unwrap().is_none());
    // A cold read finds the newest manifest from the fixed-key copy plus the listing after it, and
    // a copy that lags the listing (a publish whose copy write was lost) is not believed.
    assert_eq!(b.latest_manifest().await.unwrap().unwrap().0, 2);
    store
        .put("ns/shared/manifest-latest", copy_of_one)
        .await
        .unwrap();
    assert_eq!(b.latest_manifest().await.unwrap().unwrap().0, 2);
    a.publish_manifest(3, &manifest).await.unwrap();
    assert_eq!(b.latest_manifest().await.unwrap().unwrap().0, 3);
}

#[tokio::test]
async fn deletion_erases_payloads_but_fences_old_slots() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let wal = Wal::new(store.clone(), "deleted").unwrap();
    let first = WalEntry::new(vec![]);
    wal.append(1, &first).await.unwrap();
    assert_eq!(store.list("ns/deleted/wal-data/").await.unwrap().len(), 0);
    let mut tombstone = WalEntry::new(vec![]);
    tombstone.tombstone = true;
    wal.append(2, &tombstone).await.unwrap();
    // A concurrent recreation can commit before cleanup; it must survive.
    wal.append(3, &WalEntry::new(vec![])).await.unwrap();
    wal.cleanup_deleted(2).await.unwrap();
    assert!(matches!(wal.read(1).await, Err(Error::Pruned)));
    assert_eq!(store.list("ns/deleted/wal-data/").await.unwrap().len(), 0);
    let tail = wal.tail(0).await.unwrap();
    assert_eq!(tail.len(), 2);
    assert!(tail[0].1.tombstone);
    assert_eq!(tail[1].0, 3);
    assert_eq!(
        wal.append(1, &WalEntry::new(vec![])).await.unwrap(),
        CreateResult::Conflict
    );
    assert_eq!(store.list("ns/deleted/wal-data/").await.unwrap().len(), 0);
}

#[tokio::test]
async fn epoch_claims_fence_stalled_indexers_and_checkpoint_regression() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let wal = Wal::new(store, "epochs").unwrap();
    let mut manifest = Manifest {
        format_version: 1,
        schema: Default::default(),
        distance_metric: Default::default(),
        id_type: "uint".parse().unwrap(),
        created_at: chrono::Utc::now(),
        indexed_through: 0,
        indexer_epoch: 1,
        deleted: false,
        segments: vec![],
        stats: Default::default(),
        text_stats: None,
        ann_dirs: Default::default(),
        ann_tiers: Default::default(),
        lineage: Default::default(),
        updated_at: None,
        retired: vec![],
        checkpoints: vec![],
        pruned_through: 0,
        bulk_load_at: None,
        root: false,
    };
    assert_eq!(wal.publish_fenced(0, 0, &manifest).await.unwrap(), 1);
    let stale = manifest.clone();
    manifest.indexer_epoch = 2;
    assert_eq!(wal.publish_fenced(1, 1, &manifest).await.unwrap(), 2);
    assert!(matches!(
        wal.publish_fenced(1, 1, &stale).await,
        Err(Error::Fenced)
    ));
    assert!(matches!(
        wal.publish_fenced(2, 1, &stale).await,
        Err(Error::Fenced)
    ));
    manifest.indexed_through = 10;
    wal.publish_fenced(2, 2, &manifest).await.unwrap();
    manifest.indexed_through = 9;
    assert!(matches!(
        wal.publish_fenced(3, 2, &manifest).await,
        Err(Error::Fenced)
    ));
}

#[tokio::test]
async fn checkpoint_gc_collects_abandoned_payloads_only_for_fenced_slots() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let wal = Wal::new(store.clone(), "orphan").unwrap();
    wal.append(1, &WalEntry::new(vec![])).await.unwrap();
    let abandoned = WalEntry::new(vec![]);
    let orphan = format!("ns/orphan/wal-data/{:020}-{}.wal", 1, abandoned.write_id);
    store
        .create_with_id(&orphan, abandoned.encode().unwrap(), abandoned.write_id)
        .await
        .unwrap();
    let future = WalEntry::new(vec![]);
    let pending = format!("ns/orphan/wal-data/{:020}-{}.wal", 2, future.write_id);
    store
        .create_with_id(&pending, future.encode().unwrap(), future.write_id)
        .await
        .unwrap();
    wal.prune_payloads(1).await.unwrap();
    assert!(matches!(
        store.get(&orphan).await,
        Err(bf_storage::Error::NotFound)
    ));
    assert!(store.get(&pending).await.is_ok());
    assert_eq!(
        wal.append(1, &abandoned).await.unwrap(),
        CreateResult::Conflict
    );
    assert_eq!(wal.append(2, &future).await.unwrap(), CreateResult::Created);
    assert_eq!(
        wal.read(2).await.unwrap().unwrap().write_id,
        future.write_id
    );
}

fn document_entry(text: String) -> WalEntry {
    let now = chrono::Utc::now();
    WalEntry::new(vec![WriteBatch {
        upserts: vec![serde_json::from_value(serde_json::json!({"id":1,"text":text})).unwrap()],
        deletes: vec![],
        schema: Default::default(),
        distance_metric: Default::default(),
        created_at: now,
        updated_at: now,
    }])
}
#[tokio::test]
async fn inline_redaction_is_monotonic_under_retries_and_concurrent_gc() {
    for single_writer in [false, true] {
        let store = Storage::open(&StorageConfig {
            url: "memory://".into(),
            single_writer,
            ..Default::default()
        })
        .await
        .unwrap();
        let wal = Wal::new(store.clone(), "inline").unwrap();
        let entry = document_entry("erase-this-document".into());
        assert_eq!(wal.append(1, &entry).await.unwrap(), CreateResult::Created);
        assert!(store.list("ns/inline/wal-data/").await.unwrap().is_empty());
        assert_eq!(
            wal.read(1).await.unwrap().unwrap().batches[0].upserts[0]["text"],
            "erase-this-document"
        );
        let competitor = document_entry("must-never-win".into());
        for _ in 0..20 {
            let (a, b, retry, conflict) = tokio::join!(
                wal.prune_payloads(1),
                wal.prune_payloads(1),
                wal.append(1, &entry),
                wal.append(1, &competitor)
            );
            a.unwrap();
            b.unwrap();
            assert_eq!(retry.unwrap(), CreateResult::AlreadyCommitted);
            assert_eq!(conflict.unwrap(), CreateResult::Conflict);
            assert!(matches!(wal.read(1).await, Err(Error::Pruned)));
        }
        let bytes = store.get(&wal.key(1)).await.unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("erase-this-document"));
        assert_eq!(store.list("ns/inline/wal/").await.unwrap().len(), 1);
        assert!(store.list("ns/inline/wal-data/").await.unwrap().is_empty());
    }
}
#[tokio::test]
async fn large_external_and_legacy_v2_payloads_remain_readable_and_erasable() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let wal = Wal::new(store.clone(), "formats").unwrap();
    let large = document_entry("x".repeat(INLINE_PAYLOAD_BYTES + 1));
    wal.append(1, &large).await.unwrap();
    assert_eq!(store.list("ns/formats/wal-data/").await.unwrap().len(), 1);
    assert_eq!(wal.read(1).await.unwrap().unwrap().write_id, large.write_id);
    let legacy = document_entry("legacy document".into());
    let payload = format!("ns/formats/wal-data/{:020}-{}.wal", 2, legacy.write_id);
    store
        .create_with_id(&payload, legacy.encode().unwrap(), legacy.write_id)
        .await
        .unwrap();
    let mut record = vec![2];
    record.extend(serde_json::to_vec(&serde_json::json!({"write_id":legacy.write_id,"payload_seq":2,"committed_at":legacy.committed_at,"tombstone":false})).unwrap());
    store
        .create_with_id(&wal.key(2), record, legacy.write_id)
        .await
        .unwrap();
    assert_eq!(
        wal.read(2).await.unwrap().unwrap().write_id,
        legacy.write_id
    );
    wal.prune_payloads(2).await.unwrap();
    assert!(matches!(wal.read(1).await, Err(Error::Pruned)));
    assert!(matches!(wal.read(2).await, Err(Error::Pruned)));
    assert!(store.list("ns/formats/wal-data/").await.unwrap().is_empty());
    assert_eq!(store.list("ns/formats/wal/").await.unwrap().len(), 2);
}

#[tokio::test]
async fn prune_from_skips_already_pruned_sequences_and_prunes_many_concurrently() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let wal = Wal::new(store.clone(), "range").unwrap();
    for seq in 1..=100u64 {
        let entry = document_entry(format!("doc-{seq}"));
        assert_eq!(
            wal.append(seq, &entry).await.unwrap(),
            CreateResult::Created
        );
    }
    wal.prune_payloads_from(0, 60).await.unwrap();
    for seq in 1..=60 {
        assert!(matches!(wal.read(seq).await, Err(Error::Pruned)), "{seq}");
    }
    assert!(wal.read(61).await.unwrap().is_some());
    // A later pass over (60, 90] leaves the earlier range alone and prunes the new one.
    wal.prune_payloads_from(60, 90).await.unwrap();
    assert!(matches!(wal.read(90).await, Err(Error::Pruned)));
    assert!(wal.read(91).await.unwrap().is_some());
    // The whole-range form is the same as starting at zero.
    wal.prune_payloads(100).await.unwrap();
    assert!(matches!(wal.read(100).await, Err(Error::Pruned)));
}

/// A manifest with many segments carrying text headers gets a compact reader copy at the fixed key:
/// a reader that does not rank by text takes it, a full read (and a writer) never does, and
/// `write_reader_copy` upgrades a namespace published without one.
#[tokio::test]
async fn compact_reader_copy_omits_text_headers_and_full_reads_ignore_it() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let wal = Wal::new(store.clone(), "compact").unwrap();
    let file = |key: String, header: bool| SegmentFile {
        key,
        size: 10,
        metadata_len: 1,
        stats_index: None,
        text_header: header.then(|| TextHeaderRef {
            footer: "00".repeat(72),
            // Distinct, hard-to-compress terms, so the headers dominate the encoded size.
            chunks: (0..60)
                .map(|i| {
                    (
                        format!("{:x}", uuid::Uuid::new_v4().as_u128()),
                        i * 100,
                        99,
                        rand_crc(i),
                    )
                })
                .collect(),
        }),
    };
    let segments: Vec<Segment> = (0..400)
        .map(|i| Segment {
            id: format!("seg{i:04}"),
            wal_start: 1,
            wal_end: 2,
            row_count: 10,
            schema: Default::default(),
            files: [
                ("docs".to_string(), file(format!("s/{i}/docs"), false)),
                ("fts:text".to_string(), file(format!("s/{i}/fts"), true)),
            ]
            .into(),
            live: file(String::new(), false),
            live_generation: 0,
        })
        .collect();
    let mut manifest = Manifest {
        format_version: 1,
        schema: Default::default(),
        distance_metric: Default::default(),
        id_type: "uint".parse().unwrap(),
        created_at: chrono::Utc::now(),
        indexed_through: 2,
        indexer_epoch: 0,
        deleted: false,
        segments,
        stats: Default::default(),
        text_stats: None,
        ann_dirs: Default::default(),
        ann_tiers: Default::default(),
        lineage: Default::default(),
        updated_at: None,
        retired: vec![],
        checkpoints: vec![],
        pruned_through: 0,
        bulk_load_at: None,
        root: false,
    };
    wal.publish_manifest(1, &manifest).await.unwrap();
    let copy = store.get("ns/compact/manifest-latest").await.unwrap();
    let slot = store
        .get("ns/compact/manifest/00000000000000000001.json")
        .await
        .unwrap();
    assert!(
        copy.len() < slot.len() / 2,
        "{} vs {}",
        copy.len(),
        slot.len()
    );

    let (seq, reader) = wal
        .manifest_view_after(0, ManifestView::Reader)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seq, 1);
    assert!(reader.root);
    assert_eq!(reader.segments.len(), 400);
    assert!(
        reader
            .segments
            .iter()
            .all(|s| s.files.values().all(|f| f.text_header.is_none()))
    );
    // Everything else is the same.
    assert_eq!(reader.segments[7].files["docs"].key, "s/7/docs");
    assert_eq!(reader.segments[7].files["fts:text"].key, "s/7/fts");

    for full in [
        wal.latest_manifest().await.unwrap().unwrap(),
        wal.manifest_after(0).await.unwrap().unwrap(),
    ] {
        assert!(!full.1.root);
        assert!(
            full.1
                .segments
                .iter()
                .all(|s| s.files["fts:text"].text_header.is_some())
        );
    }
    // A reader copy can never be published as a manifest.
    manifest.root = true;
    assert!(wal.publish_manifest(2, &manifest).await.is_err());
    manifest.root = false;

    // A namespace whose copy is a full manifest (published before compact copies) is upgraded in place.
    store
        .put("ns/compact/manifest-latest", {
            let mut p = 1u64.to_be_bytes().to_vec();
            p.extend_from_slice(&slot);
            p
        })
        .await
        .unwrap();
    let (_, before) = wal
        .manifest_view_after(0, ManifestView::Reader)
        .await
        .unwrap()
        .unwrap();
    assert!(!before.root);
    let (seq, bytes) = wal.write_reader_copy().await.unwrap().unwrap();
    assert_eq!(seq, 1);
    assert_eq!(bytes, copy.len());
    let (_, after) = wal
        .manifest_view_after(0, ManifestView::Reader)
        .await
        .unwrap()
        .unwrap();
    assert!(after.root);
}

fn rand_crc(i: u64) -> u32 {
    (i.wrapping_mul(2_654_435_761) & 0xffff_ffff) as u32
}
