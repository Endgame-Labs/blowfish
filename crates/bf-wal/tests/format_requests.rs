use bf_storage::{BackendStats, Storage, StorageConfig};
use bf_wal::{INLINE_PAYLOAD_BYTES, Manifest, Wal, WalEntry, WriteBatch};
use std::sync::Arc;

#[tokio::test]
async fn steady_state_write_request_count() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let wal = Wal::new(store, "requests").unwrap();
    for sequence in 1..=6 {
        let external = sequence > 3;
        let entry = if external {
            let now = chrono::Utc::now();
            WalEntry::new(vec![WriteBatch {
                upserts: vec![
                    serde_json::from_value(serde_json::json!({
                        "id": 1, "text": "x".repeat(INLINE_PAYLOAD_BYTES + 1)
                    }))
                    .unwrap(),
                ],
                deletes: vec![],
                schema: Default::default(),
                distance_metric: Default::default(),
                created_at: now,
                updated_at: now,
            }])
        } else {
            WalEntry::new(vec![])
        };
        let stats = Arc::new(BackendStats::default());
        stats
            .scope("write", wal.append(sequence, &entry))
            .await
            .unwrap();
        let counts: Vec<_> = stats
            .snapshot()
            .into_iter()
            .map(|m| (m.operation, m.requests))
            .collect();
        eprintln!("write {sequence} (external={external}): {counts:?}");
        assert_eq!(counts, vec![("put".into(), if external { 2 } else { 1 })]);
    }
}

#[tokio::test]
async fn steady_state_publication_request_count() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let wal = Wal::new(store, "publication").unwrap();
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
        updated_at: None,
        retired: vec![],
        checkpoints: vec![],
        pruned_through: 0,
        bulk_load_at: None,
        root: false,
    };
    for sequence in 1..=3 {
        let stats = Arc::new(BackendStats::default());
        stats
            .scope("publication", wal.publish_manifest(sequence, &manifest))
            .await
            .unwrap();
        let counts: Vec<_> = stats
            .snapshot()
            .into_iter()
            .map(|m| (m.operation, m.requests))
            .collect();
        eprintln!("publication {sequence}: {counts:?}");
        assert_eq!(
            counts,
            vec![("get".into(), 1), ("list".into(), 1), ("put".into(), 2)]
        );
    }
}
