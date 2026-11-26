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

async fn memory_wal(name: &str) -> (Storage, Wal) {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let wal = Wal::new(store.clone(), name).unwrap();
    (store, wal)
}

fn external_entry(tag: usize) -> WalEntry {
    document_entry(format!("{tag}-{}", "x".repeat(INLINE_PAYLOAD_BYTES + 1)))
}

fn tag_of(entry: &WalEntry) -> usize {
    let text = entry.batches[0].upserts[0]["text"].as_str().unwrap();
    text.split('-').next().unwrap().parse().unwrap()
}

/// More out-of-line payloads than the fetch window still come back in WAL order, mixed with inline ones.
#[tokio::test]
async fn tail_returns_many_external_payloads_in_wal_order() {
    let (store, wal) = memory_wal("ordered").await;
    let total = 40u64;
    for seq in 1..=total {
        let entry = if seq % 5 == 0 {
            document_entry(format!("{seq}-small"))
        } else {
            external_entry(seq as usize)
        };
        assert_eq!(
            wal.append(seq, &entry).await.unwrap(),
            CreateResult::Created
        );
    }
    assert_eq!(store.list("ns/ordered/wal-data/").await.unwrap().len(), 32);
    let tail = wal.tail(0).await.unwrap();
    assert_eq!(tail.len(), total as usize);
    for (i, (seq, entry)) in tail.iter().enumerate() {
        assert_eq!(*seq, i as u64 + 1);
        assert_eq!(tag_of(entry), i + 1);
    }
    let from = wal.tail(30).await.unwrap();
    assert_eq!(from.first().unwrap().0, 31);
    assert_eq!(from.len(), 10);
}

/// The replay starts at the last tombstone; payloads before it are never needed.
#[tokio::test]
async fn tail_starts_at_a_tombstone_in_the_middle() {
    let (_store, wal) = memory_wal("middle").await;
    for seq in 1..=12u64 {
        let entry = if seq == 6 {
            let mut t = WalEntry::new(vec![]);
            t.tombstone = true;
            t
        } else {
            external_entry(seq as usize)
        };
        wal.append(seq, &entry).await.unwrap();
    }
    // Reclaim the payloads before the tombstone, as deletion cleanup does.
    wal.cleanup_deleted(6).await.unwrap();
    let tail = wal.tail(0).await.unwrap();
    let seqs: Vec<u64> = tail.iter().map(|(s, _)| *s).collect();
    assert_eq!(seqs, (6..=12).collect::<Vec<_>>());
    assert!(tail[0].1.tombstone);
    for (seq, entry) in &tail[1..] {
        assert_eq!(tag_of(entry), *seq as usize);
    }
}

/// A reclaimed payload without a tombstone is still reported as corruption, and a budget still
/// stops the replay at the same entry, as before fetches ran concurrently.
#[tokio::test]
async fn tail_reports_reclaimed_payloads_and_honors_the_byte_budget() {
    let (_store, wal) = memory_wal("reclaimed").await;
    for seq in 1..=12u64 {
        wal.append(seq, &external_entry(seq as usize))
            .await
            .unwrap();
    }
    wal.prune_payloads(6).await.unwrap();
    assert!(matches!(wal.tail(0).await, Err(Error::Format)));
    let tail = wal.tail(6).await.unwrap();
    assert_eq!(tail.len(), 6);
    // Entry sizes differ by a few bytes (`committed_at` serializes a variable number of
    // fractional digits), so budget exactly the first two entries.
    let size = |entry: &bf_wal::WalEntry| serde_json::to_vec(entry).unwrap().len() as u64;
    let bounded = wal
        .tail_bounded(6, size(&tail[0].1) + size(&tail[1].1))
        .await
        .unwrap();
    assert_eq!(
        bounded.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        vec![7, 8]
    );
}

/// Cleanup racing a reader: the reader either sees every payload or restarts at the tombstone.
#[tokio::test]
async fn tail_survives_payloads_reclaimed_mid_read() {
    for round in 0..10 {
        let (_store, wal) = memory_wal(&format!("race{round}")).await;
        for seq in 1..=12u64 {
            wal.append(seq, &external_entry(seq as usize))
                .await
                .unwrap();
        }
        let mut tombstone = WalEntry::new(vec![]);
        tombstone.tombstone = true;
        let deleter = async {
            wal.append(13, &tombstone).await.unwrap();
            wal.cleanup_deleted(13).await.unwrap();
        };
        let (tail, ()) = tokio::join!(wal.tail(0), deleter);
        let tail = tail.unwrap();
        let first = tail.first().unwrap().0;
        assert!(first == 1 || first == 13, "{first}");
        let seqs: Vec<u64> = tail.iter().map(|(s, _)| *s).collect();
        // A reader that scanned before the tombstone committed ends at 12.
        let last = *seqs.last().unwrap();
        assert!(last == 12 || last == 13, "{last}");
        assert_eq!(seqs, (first..=last).collect::<Vec<_>>());
    }
}

/// An empty or fully indexed tail is cheap and empty.
#[tokio::test]
async fn tail_of_nothing_is_empty() {
    let (_store, wal) = memory_wal("empty").await;
    assert!(wal.tail(0).await.unwrap().is_empty());
    wal.append(1, &external_entry(1)).await.unwrap();
    assert!(wal.tail(1).await.unwrap().is_empty());
    assert_eq!(wal.tail(0).await.unwrap().len(), 1);
}

/// Reads a slot's JSON record (after the version byte).
async fn slot_json(store: &Storage, wal: &Wal, seq: u64) -> serde_json::Value {
    let bytes = store.get(&wal.key(seq)).await.unwrap();
    serde_json::from_slice(&bytes[1..]).unwrap()
}

/// A new slot records its payload's stored size; inline slots record none.
#[tokio::test]
async fn slots_record_the_stored_payload_size() {
    let (store, wal) = memory_wal("sizes").await;
    wal.append(1, &external_entry(1)).await.unwrap();
    wal.append(2, &document_entry("tiny".into())).await.unwrap();
    let payload = store.list("ns/sizes/wal-data/").await.unwrap();
    assert_eq!(payload.len(), 1);
    let recorded = slot_json(&store, &wal, 1).await["payload_bytes"].as_u64();
    // The object carries a small storage envelope beyond the encoded payload.
    let recorded = recorded.unwrap();
    assert!(recorded <= payload[0].size && payload[0].size - recorded <= 64);
    assert!(
        slot_json(&store, &wal, 2)
            .await
            .get("payload_bytes")
            .is_none()
    );
}

/// Records written before sizes existed (no field) still replay, in order, one fetch at a time.
#[tokio::test]
async fn tail_reads_records_without_payload_sizes() {
    let (store, wal) = memory_wal("nosize").await;
    for seq in 1..=12u64 {
        wal.append(seq, &external_entry(seq as usize))
            .await
            .unwrap();
    }
    for seq in 1..=12u64 {
        let mut record = slot_json(&store, &wal, seq).await;
        record.as_object_mut().unwrap().remove("payload_bytes");
        let mut bytes = vec![3];
        bytes.extend(serde_json::to_vec(&record).unwrap());
        store.put(&wal.key(seq), bytes).await.unwrap();
    }
    let tail = wal.tail(0).await.unwrap();
    assert_eq!(tail.len(), 12);
    for (i, (_, entry)) in tail.iter().enumerate() {
        assert_eq!(tag_of(entry), i + 1);
    }
}

/// One payload larger than the in-flight budget, between small ones, replays in order.
#[tokio::test]
async fn tail_with_a_payload_over_the_budget_keeps_order() {
    let (_store, wal) = memory_wal("huge").await;
    // Incompressible text: the stored payload exceeds the 64 MiB budget.
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let noise: String = (0..80 * 1024 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'a' + (state % 26) as u8) as char
        })
        .collect();
    for seq in 1..=5u64 {
        let entry = if seq == 3 {
            document_entry(format!("3-{noise}"))
        } else {
            external_entry(seq as usize)
        };
        wal.append(seq, &entry).await.unwrap();
    }
    let tail = wal.tail(0).await.unwrap();
    assert_eq!(
        tail.iter().map(|(_, e)| tag_of(e)).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
}

mod listing {
    //! `tail` finds a long tail with one listing plus one read wave. The listing only guides the
    //! reads: the conditional slot reads still decide where the tail ends.
    use super::*;
    use async_trait::async_trait;
    use futures::stream::{self, BoxStream, StreamExt};
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
        path::Path,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    type Hide = Arc<dyn Fn(&str) -> bool + Send + Sync>;
    type After = Box<dyn FnOnce() + Send>;

    /// An in-memory store whose WAL listings can hide keys (a stale listing), fail, or run a
    /// side effect after the listing was taken (a write racing the listing). Counts listings.
    struct Listing {
        inner: Arc<InMemory>,
        lists: AtomicUsize,
        hide: Mutex<Option<Hide>>,
        fail: bool,
        after: Mutex<Option<After>>,
    }
    impl std::fmt::Debug for Listing {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("listing")
        }
    }
    impl std::fmt::Display for Listing {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("listing")
        }
    }
    #[async_trait]
    impl ObjectStore for Listing {
        async fn put_opts(
            &self,
            p: &Path,
            b: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(p, b, o).await
        }
        async fn put_multipart_opts(
            &self,
            p: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(p, o).await
        }
        async fn get_opts(&self, p: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            self.inner.get_opts(p, o).await
        }
        fn delete_stream(
            &self,
            l: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let is_wal = p.is_some_and(|p| p.as_ref().ends_with("/wal"));
            let (inner, prefix) = (self.inner.clone(), p.cloned());
            let (fail, hide) = (self.fail && is_wal, self.hide.lock().unwrap().clone());
            let after = if is_wal {
                self.lists.fetch_add(1, Ordering::SeqCst);
                self.after.lock().unwrap().take()
            } else {
                None
            };
            stream::once(async move {
                if fail {
                    return stream::iter(vec![Err(object_store::Error::Generic {
                        store: "listing",
                        source: "listing failed".into(),
                    })])
                    .boxed();
                }
                let mut all: Vec<_> = inner.list(prefix.as_ref()).collect().await;
                if let (true, Some(h)) = (is_wal, hide) {
                    all.retain(|m| !m.as_ref().is_ok_and(|m| h(m.location.as_ref())));
                }
                if let Some(after) = after {
                    after();
                }
                stream::iter(all).boxed()
            })
            .flatten()
            .boxed()
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
    }

    struct Fixture {
        inner: Arc<InMemory>,
        backend: Arc<Listing>,
        wal: Wal,
    }

    async fn fixture(fail: bool, hide: Option<Hide>) -> Fixture {
        let inner = Arc::new(InMemory::new());
        let backend = Arc::new(Listing {
            inner: inner.clone(),
            lists: AtomicUsize::new(0),
            hide: Mutex::new(hide),
            fail,
            after: Mutex::new(None),
        });
        let store = Storage::from_backend(backend.clone(), Path::from(""), false, false)
            .await
            .unwrap();
        let wal = Wal::new(store, "ns").unwrap();
        Fixture {
            inner,
            backend,
            wal,
        }
    }

    async fn append(wal: &Wal, range: std::ops::RangeInclusive<u64>) {
        for seq in range {
            let entry = if seq % 3 == 0 {
                external_entry(seq as usize)
            } else {
                document_entry(format!("{seq}-small"))
            };
            assert_eq!(
                wal.append(seq, &entry).await.unwrap(),
                CreateResult::Created
            );
        }
    }

    fn assert_contiguous(tail: &[(u64, WalEntry)], first: u64, last: u64) {
        let seqs: Vec<u64> = tail.iter().map(|(s, _)| *s).collect();
        assert_eq!(seqs, (first..=last).collect::<Vec<_>>());
        for (seq, entry) in tail {
            assert_eq!(tag_of(entry), *seq as usize);
        }
    }

    fn slot_number(key: &str) -> u64 {
        key.rsplit('/')
            .next()
            .and_then(|n| n.strip_suffix(".wal"))
            .and_then(|n| n.parse().ok())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn a_long_tail_is_listed_once_and_a_tiny_one_not_at_all() {
        let f = fixture(false, None).await;
        append(&f.wal, 1..=40).await;
        assert_contiguous(&f.wal.tail(0).await.unwrap(), 1, 40);
        assert_eq!(f.backend.lists.load(Ordering::SeqCst), 1);
        // A tail after slot 10 lists once; a tail of none or one slot never lists.
        assert_contiguous(&f.wal.tail(10).await.unwrap(), 11, 40);
        assert_eq!(f.backend.lists.load(Ordering::SeqCst), 2);
        assert_contiguous(&f.wal.tail(39).await.unwrap(), 40, 40);
        assert!(f.wal.tail(40).await.unwrap().is_empty());
        assert_eq!(f.backend.lists.load(Ordering::SeqCst), 2);
        // Two to five slots start a listing alongside the second wave, but the reads decide.
        for after in [35, 36, 37, 38] {
            assert_contiguous(&f.wal.tail(after).await.unwrap(), after + 1, 40);
        }
        assert_eq!(f.backend.lists.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn an_empty_tail_never_lists() {
        let f = fixture(false, None).await;
        assert!(f.wal.tail(0).await.unwrap().is_empty());
        append(&f.wal, 1..=1).await;
        assert!(f.wal.tail(1).await.unwrap().is_empty());
        assert_eq!(f.backend.lists.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_gap_ends_the_tail_even_when_later_slots_are_listed() {
        let f = fixture(false, None).await;
        append(&f.wal, 1..=40).await;
        f.inner
            .delete(&Path::from(f.wal.key(20).as_str()))
            .await
            .unwrap();
        assert_contiguous(&f.wal.tail(0).await.unwrap(), 1, 19);
        // A gap right after the second wave.
        f.inner
            .delete(&Path::from(f.wal.key(7).as_str()))
            .await
            .unwrap();
        assert_contiguous(&f.wal.tail(0).await.unwrap(), 1, 6);
        // A gap on the very first slot.
        f.inner
            .delete(&Path::from(f.wal.key(1).as_str()))
            .await
            .unwrap();
        assert!(f.wal.tail(0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_stale_listing_missing_the_newest_slots_loses_nothing() {
        let hide: Hide = Arc::new(|k| slot_number(k) > 33);
        let f = fixture(false, Some(hide)).await;
        append(&f.wal, 1..=40).await;
        assert_contiguous(&f.wal.tail(0).await.unwrap(), 1, 40);
        assert_eq!(f.backend.lists.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_listing_with_holes_or_nothing_loses_nothing() {
        let hide: Hide = Arc::new(|k| slot_number(k).is_multiple_of(7));
        let f = fixture(false, Some(hide)).await;
        append(&f.wal, 1..=45).await;
        assert_contiguous(&f.wal.tail(0).await.unwrap(), 1, 45);
        *f.backend.hide.lock().unwrap() = Some(Arc::new(|_| true));
        assert_contiguous(&f.wal.tail(0).await.unwrap(), 1, 45);
        assert_contiguous(&f.wal.tail(5).await.unwrap(), 6, 45);
    }

    #[tokio::test]
    async fn a_failed_listing_falls_back_to_probing() {
        let f = fixture(true, None).await;
        append(&f.wal, 1..=40).await;
        assert_contiguous(&f.wal.tail(0).await.unwrap(), 1, 40);
        assert_eq!(f.backend.lists.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn slots_appended_during_the_listing_are_read() {
        let f = fixture(false, None).await;
        append(&f.wal, 1..=40).await;
        // The listing was taken with 40 slots; two more commit before the tail's reads finish.
        let inner = f.inner.clone();
        let late = {
            let store = Storage::from_backend(
                Arc::new(Listing {
                    inner: inner.clone(),
                    lists: AtomicUsize::new(0),
                    hide: Mutex::new(None),
                    fail: false,
                    after: Mutex::new(None),
                }),
                Path::from(""),
                false,
                false,
            )
            .await
            .unwrap();
            Wal::new(store, "ns").unwrap()
        };
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        *f.backend.after.lock().unwrap() = Some(Box::new(move || tx.send(()).unwrap()));
        let tail = tokio::spawn({
            let wal = f.wal.clone();
            async move { wal.tail(0).await.unwrap() }
        });
        // Commit 41 and 42 as soon as the listing has been taken.
        tokio::task::spawn_blocking(move || rx.recv().unwrap())
            .await
            .unwrap();
        append(&late, 41..=42).await;
        let tail = tail.await.unwrap();
        // Whatever the interleaving, the replay is a gapless prefix that holds all 40 slots.
        assert!(tail.len() >= 40 && tail.len() <= 42, "{}", tail.len());
        assert_contiguous(&tail, 1, tail.len() as u64);
        assert_contiguous(&f.wal.tail(0).await.unwrap(), 1, 42);
    }
}

mod speculative {
    //! `tail` starts payload body reads from a listing of the payload prefix while the slot reads
    //! are still confirming the run. A body counts only for the slot record that names its key.
    use super::*;
    use async_trait::async_trait;
    use futures::stream::{self, BoxStream, StreamExt};
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    type Hide = Arc<dyn Fn(&str) -> bool + Send + Sync>;

    /// An in-memory store that logs payload reads, tracks how many run at once (each takes a few
    /// milliseconds), and can hide keys from, or fail, the payload listing.
    struct Spy {
        inner: Arc<InMemory>,
        gets: Mutex<Vec<String>>,
        active: AtomicUsize,
        peak: AtomicUsize,
        hide: Mutex<Option<Hide>>,
        fail: bool,
    }
    impl std::fmt::Debug for Spy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("spy")
        }
    }
    impl std::fmt::Display for Spy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("spy")
        }
    }
    #[async_trait]
    impl ObjectStore for Spy {
        async fn put_opts(
            &self,
            p: &Path,
            b: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(p, b, o).await
        }
        async fn put_multipart_opts(
            &self,
            p: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(p, o).await
        }
        async fn get_opts(&self, p: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            if p.as_ref().contains("/wal-data/") {
                self.gets.lock().unwrap().push(p.to_string());
                let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                let result = self.inner.get_opts(p, o).await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                return result;
            }
            self.inner.get_opts(p, o).await
        }
        fn delete_stream(
            &self,
            l: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let is_payloads = p.is_some_and(|p| p.as_ref().ends_with("/wal-data"));
            let (inner, prefix) = (self.inner.clone(), p.cloned());
            let (fail, hide) = (self.fail && is_payloads, self.hide.lock().unwrap().clone());
            stream::once(async move {
                if fail {
                    return stream::iter(vec![Err(object_store::Error::Generic {
                        store: "spy",
                        source: "listing failed".into(),
                    })])
                    .boxed();
                }
                let mut all: Vec<_> = inner.list(prefix.as_ref()).collect().await;
                if let (true, Some(h)) = (is_payloads, hide) {
                    all.retain(|m| !m.as_ref().is_ok_and(|m| h(m.location.as_ref())));
                }
                stream::iter(all).boxed()
            })
            .flatten()
            .boxed()
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
    }

    struct Fixture {
        inner: Arc<InMemory>,
        spy: Arc<Spy>,
        wal: Wal,
    }

    async fn fixture(fail: bool, hide: Option<Hide>) -> Fixture {
        let inner = Arc::new(InMemory::new());
        let spy = Arc::new(Spy {
            inner: inner.clone(),
            gets: Mutex::new(vec![]),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            hide: Mutex::new(hide),
            fail,
        });
        let store = Storage::from_backend(spy.clone(), Path::from(""), false, false)
            .await
            .unwrap();
        Fixture {
            inner,
            spy,
            wal: Wal::new(store, "ns").unwrap(),
        }
    }

    async fn append_external(wal: &Wal, range: std::ops::RangeInclusive<u64>) {
        for seq in range {
            let created = wal.append(seq, &external_entry(seq as usize)).await;
            assert_eq!(created.unwrap(), CreateResult::Created);
        }
    }

    /// The tail must equal what reading slot by slot (no speculation) returns.
    async fn assert_same_as_serial(wal: &Wal, last: u64) -> Vec<(u64, WalEntry)> {
        let tail = wal.tail(0).await.unwrap();
        let mut serial = vec![];
        for seq in 1..=last {
            let entry = wal.read(seq).await.unwrap().unwrap();
            if entry.tombstone {
                serial.clear();
            }
            serial.push((seq, entry));
        }
        assert_eq!(tail.len(), serial.len());
        for ((a, x), (b, y)) in tail.iter().zip(&serial) {
            assert_eq!((a, x.write_id, x.tombstone), (b, y.write_id, y.tombstone));
        }
        tail
    }

    fn gets_of(f: &Fixture, needle: &str) -> usize {
        let gets = f.spy.gets.lock().unwrap();
        gets.iter().filter(|k| k.contains(needle)).count()
    }

    #[tokio::test]
    async fn every_listed_body_is_fetched_once_and_the_tail_is_unchanged() {
        let f = fixture(false, None).await;
        append_external(&f.wal, 1..=40).await;
        f.spy.gets.lock().unwrap().clear();
        let tail = f.wal.tail(0).await.unwrap();
        assert_eq!(tail.len(), 40);
        for (seq, entry) in &tail {
            assert_eq!(tag_of(entry), *seq as usize);
        }
        // The speculative GETs were used: no payload was read twice.
        assert_eq!(f.spy.gets.lock().unwrap().len(), 40);
        // And the bound on fetches in flight holds with them.
        let peak = f.spy.peak.load(Ordering::SeqCst);
        assert!((2..=8).contains(&peak), "peak {peak}");
        assert_same_as_serial(&f.wal, 40).await;
    }

    #[tokio::test]
    async fn a_stale_or_failed_payload_listing_loses_nothing() {
        for (fail, hide) in [
            (
                false,
                Some(Arc::new(|k: &str| k.contains("0000000000000000003")) as Hide),
            ),
            (false, Some(Arc::new(|_: &str| true) as Hide)),
            (true, None),
        ] {
            let f = fixture(fail, hide).await;
            append_external(&f.wal, 1..=40).await;
            f.spy.gets.lock().unwrap().clear();
            let tail = f.wal.tail(0).await.unwrap();
            assert_eq!(tail.len(), 40);
            assert!(tail.iter().all(|(s, e)| tag_of(e) == *s as usize));
            assert_eq!(f.spy.gets.lock().unwrap().len(), 40, "once each");
        }
    }

    #[tokio::test]
    async fn a_losing_writes_body_under_a_reused_slot_is_never_accepted() {
        let f = fixture(false, None).await;
        append_external(&f.wal, 1..=4).await;
        // A loser staged a body for slot 5 (and 9) that never committed; the winner took the slot.
        let loser = external_entry(999);
        let staged = f.wal.stage(5, &loser).await.unwrap();
        f.wal.stage(9, &loser).await.unwrap();
        append_external(&f.wal, 5..=40).await;
        let tail = f.wal.tail(0).await.unwrap();
        assert_eq!(tail.len(), 40);
        for (seq, entry) in &tail {
            assert_eq!(tag_of(entry), *seq as usize, "slot {seq}");
            assert_ne!(entry.write_id, loser.write_id);
        }
        // The loser's own writer discards it; the tail is the same.
        f.wal.discard(5, &loser, &staged).await.unwrap();
        assert_eq!(f.wal.tail(0).await.unwrap().len(), 40);
    }

    #[tokio::test]
    async fn a_body_staged_for_an_empty_slot_is_not_an_entry() {
        let f = fixture(false, None).await;
        append_external(&f.wal, 1..=40).await;
        f.wal.stage(41, &external_entry(41)).await.unwrap();
        f.wal.stage(60, &external_entry(60)).await.unwrap();
        let tail = f.wal.tail(0).await.unwrap();
        assert_eq!(tail.len(), 40);
        assert_eq!(tail.last().unwrap().0, 40);
    }

    /// Slot bytes in the record shape older releases wrote: no `payload_bytes`.
    async fn put_sizeless_slot(f: &Fixture, seq: u64, entry: &WalEntry, size: Option<u64>) {
        let mut record = vec![3u8];
        let mut json = serde_json::json!({"write_id":entry.write_id,"payload_seq":seq,"committed_at":entry.committed_at,"tombstone":false});
        if let Some(size) = size {
            json["payload_bytes"] = size.into();
        }
        record.extend(serde_json::to_vec(&json).unwrap());
        f.wal.stage(seq, entry).await.unwrap();
        f.inner
            .put_opts(
                &Path::from(f.wal.key(seq).as_str()),
                PutPayload::from(record),
                PutOptions::default(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn records_without_a_size_are_accepted_by_key_and_a_wrong_size_is_not() {
        let f = fixture(false, None).await;
        for seq in 1..=40u64 {
            let entry = external_entry(seq as usize);
            // Slot 7 claims a size its listed body does not have.
            let size = (seq == 7).then_some(1);
            put_sizeless_slot(&f, seq, &entry, size).await;
        }
        f.spy.gets.lock().unwrap().clear();
        let tail = f.wal.tail(0).await.unwrap();
        assert_eq!(tail.len(), 40);
        assert!(tail.iter().all(|(s, e)| tag_of(e) == *s as usize));
        let gets = f.spy.gets.lock().unwrap().len();
        // Sizeless slots use their listed bodies. Slot 7's was dropped for its size, so it is read
        // again (its dropped speculative read may or may not have reached the store first).
        assert!((40..=41).contains(&gets), "{gets} reads");
    }

    #[tokio::test]
    async fn tombstones_and_conflicts_replay_like_serial_reads() {
        let f = fixture(false, None).await;
        append_external(&f.wal, 1..=20).await;
        let mut tombstone = WalEntry::new(vec![]);
        tombstone.tombstone = true;
        assert_eq!(
            f.wal.append(21, &tombstone).await.unwrap(),
            CreateResult::Created
        );
        // A losing attempt at the occupied slot leaves nothing behind that the replay accepts.
        let loser = external_entry(777);
        assert_eq!(
            f.wal.append(21, &loser).await.unwrap(),
            CreateResult::Conflict
        );
        append_external(&f.wal, 22..=45).await;
        let tail = assert_same_as_serial(&f.wal, 45).await;
        assert_eq!(tail.len(), 25);
        assert!(tail[0].1.tombstone);
        // Reclaimed payloads before the tombstone are never fetched.
        f.spy.gets.lock().unwrap().clear();
        assert_eq!(f.wal.tail(0).await.unwrap().len(), 25);
        assert!(gets_of(&f, "wal-data/0000000000000000000") <= 25 + 1);
    }
}
