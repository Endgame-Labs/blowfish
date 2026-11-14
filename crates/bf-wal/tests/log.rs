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
        segments: vec![],
    };
    a.publish_manifest(1, &manifest).await.unwrap();
    assert_eq!(b.latest_manifest().await.unwrap().unwrap().0, 1);
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
    assert_eq!(store.list("ns/deleted/wal-data/").await.unwrap().len(), 1);
    let mut tombstone = WalEntry::new(vec![]);
    tombstone.tombstone = true;
    wal.append(2, &tombstone).await.unwrap();
    // A concurrent recreation can commit before cleanup; it must survive.
    wal.append(3, &WalEntry::new(vec![])).await.unwrap();
    wal.cleanup_deleted(2).await.unwrap();
    assert!(matches!(wal.read(1).await, Err(Error::Pruned)));
    assert_eq!(store.list("ns/deleted/wal-data/").await.unwrap().len(), 1);
    let tail = wal.tail(0).await.unwrap();
    assert_eq!(tail.len(), 2);
    assert!(tail[0].1.tombstone);
    assert_eq!(tail[1].0, 3);
    assert_eq!(
        wal.append(1, &WalEntry::new(vec![])).await.unwrap(),
        CreateResult::Conflict
    );
    assert_eq!(store.list("ns/deleted/wal-data/").await.unwrap().len(), 1);
}
