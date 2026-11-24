#![cfg(unix)]
#![allow(clippy::single_range_in_vec_init)]
use bf_storage::{CacheConfig, CacheStats, Error, Storage, StorageConfig};
use bytes::Bytes;

async fn store(root: &std::path::Path) -> Storage {
    Storage::open(&StorageConfig {
        url: url::Url::from_file_path(root).unwrap().to_string(),
        cache: CacheConfig {
            memory_bytes: 64,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn mapped_ranges_share_pages_and_keep_unlinked_bytes_alive() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path()).await;
    assert_eq!(store.cache_limits().0, 0);
    let data = Bytes::from_static(b"immutable mapped bytes");
    let key = "nested/percent%/unicode-é";
    store.create(key, data.clone()).await.unwrap();
    let reader = store.range_reader(key).await.unwrap();
    let stats = CacheStats::default();
    let got = reader
        .read_batch_with_stats(&[3..10, 0..4, 4..4], &stats)
        .await
        .unwrap();
    assert_eq!(got, vec![data.slice(3..10), data.slice(0..4), Bytes::new()]);
    assert_eq!(stats.snapshot().missed_bytes, 11);
    let retained = got[0].clone();
    let sparse = reader.clone().sparse(512);
    let retained_sparse = sparse.read(3..10).await.unwrap();
    assert_eq!(retained_sparse, retained);
    store.drop_caches();
    assert_eq!(retained, data.slice(3..10));
    store.delete(key).await.unwrap();
    assert_eq!(retained, data.slice(3..10));
    let fresh = Bytes::from(vec![b'x'; data.len()]);
    store.create(key, fresh.clone()).await.unwrap();
    assert_eq!(
        store
            .range_reader(key)
            .await
            .unwrap()
            .read(0..data.len() as u64)
            .await
            .unwrap(),
        fresh
    );
    assert_eq!(retained, data.slice(3..10));
    assert_eq!(retained_sparse, data.slice(3..10));
    assert_eq!(sparse.read(0..fresh.len() as u64).await.unwrap(), fresh);
    assert!(reader.read(0..1000).await.is_err());
}

#[tokio::test]
async fn mapped_scopes_empty_objects_and_missing_files_preserve_range_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path()).await;
    let alpha = store.scoped("alpha");
    let beta = store.scoped("beta");
    alpha.create("same", "alpha").await.unwrap();
    beta.create("same", "beta!").await.unwrap();
    assert_eq!(
        alpha
            .range_reader("same")
            .await
            .unwrap()
            .read(0..5)
            .await
            .unwrap(),
        "alpha"
    );
    assert_eq!(
        beta.range_reader("same")
            .await
            .unwrap()
            .read(0..5)
            .await
            .unwrap(),
        "beta!"
    );
    std::fs::write(dir.path().join("empty"), []).unwrap();
    let empty = store.range_reader("empty").await.unwrap();
    assert!(empty.read(0..0).await.unwrap().is_empty());
    assert!(empty.read(0..1).await.is_err());
    let missing = store.range_reader_with_length("absent", 8, false).unwrap();
    assert!(matches!(
        missing.read(0..1).await.unwrap_err(),
        Error::NotFound
    ));
}

#[tokio::test]
async fn sequential_truncation_is_reported_before_accessing_a_cached_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path()).await;
    store.create("short", "a complete payload").await.unwrap();
    let reader = store.range_reader("short").await.unwrap();
    assert_eq!(reader.read(0..1).await.unwrap(), "a");
    std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(dir.path().join("short"))
        .unwrap();
    assert!(matches!(
        reader.read(0..1).await.unwrap_err(),
        Error::Config(_)
    ));
}

#[tokio::test]
async fn bulk_and_sparse_views_invalidate_together_without_invalidating_held_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path()).await;
    let first = Bytes::from(vec![b'a'; 128 << 10]);
    store.create("both-views", first.clone()).await.unwrap();
    let reader = store.range_reader("both-views").await.unwrap();
    // Even a sparse hint as large as the block size must keep the random-access view.
    let sparse = reader.clone().sparse(1 << 20);
    let bulk_bytes = reader.read(0..first.len() as u64).await.unwrap();
    let sparse_bytes = sparse.read(0..first.len() as u64).await.unwrap();
    assert_eq!(bulk_bytes, first);
    assert_eq!(sparse_bytes, first);
    store.delete("both-views").await.unwrap();
    let second = Bytes::from(vec![b'b'; first.len()]);
    store.create("both-views", second.clone()).await.unwrap();
    assert_eq!(reader.read(0..second.len() as u64).await.unwrap(), second);
    assert_eq!(sparse.read(0..second.len() as u64).await.unwrap(), second);
    store.drop_caches();
    assert_eq!(bulk_bytes, first);
    assert_eq!(sparse_bytes, first);
}
