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
        updated_at: None,
        retired: vec![],
        checkpoints: vec![],
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
        updated_at: None,
        retired: vec![],
        checkpoints: vec![],
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
