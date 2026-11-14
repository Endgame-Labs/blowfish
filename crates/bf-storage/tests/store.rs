use bf_storage::*;
use bytes::Bytes;
use uuid::Uuid;

async fn exercise(store: Storage) {
    let key = "data/object";
    let id = Uuid::new_v4();
    assert_eq!(
        store.create_with_id(key, "hello world", id).await.unwrap(),
        CreateResult::Created
    );
    assert_eq!(
        store.create_with_id(key, "hello world", id).await.unwrap(),
        CreateResult::AlreadyCommitted
    );
    assert_eq!(
        store.create(key, "replacement").await.unwrap(),
        CreateResult::Conflict
    );
    assert_eq!(store.get(key).await.unwrap(), "hello world");
    assert_eq!(store.get_range(key, 6..11).await.unwrap(), "world");
    assert!(store.head(key).await.unwrap().size >= 11);
    assert_eq!(store.list("data").await.unwrap().len(), 1);
    store
        .put("data/plain", Bytes::from_static(b"raw"))
        .await
        .unwrap();
    assert_eq!(store.get_range("data/plain", 0..3).await.unwrap(), "raw");
    store.delete(key).await.unwrap();
    store.delete("data/plain").await.unwrap();
    assert!(store.list("data").await.unwrap().is_empty());
}
#[tokio::test]
async fn local_contract() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", dir.path().display()),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(store.mode(), StorageMode::Conditional);
    exercise(store).await;
}
#[tokio::test]
async fn memory_contract() {
    exercise(
        Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap(),
    )
    .await;
}
#[tokio::test]
async fn single_writer_heartbeat_refuses_second_process() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        url: format!("file://{}", dir.path().display()),
        single_writer: true,
        ..Default::default()
    };
    let first = Storage::open(&config).await.unwrap();
    assert_eq!(first.mode(), StorageMode::SingleWriter);
    assert!(matches!(
        Storage::open(&config).await,
        Err(Error::WriterActive)
    ));
    exercise(first).await;
    let readonly = Storage::open(&StorageConfig {
        read_only: true,
        ..config
    })
    .await
    .unwrap();
    assert!(matches!(
        readonly.create("x", "y").await,
        Err(Error::ReadOnly)
    ));
}
/// Run explicitly with BF_TEST_STORAGE_URL, BF_TEST_STORAGE_ENDPOINT, and BF_TEST_STORAGE_PROFILE.
#[tokio::test]
#[ignore = "requires an explicitly configured disposable S3 prefix"]
async fn s3_contract() {
    let url = std::env::var("BF_TEST_STORAGE_URL").unwrap();
    assert!(url.contains("/dev/core-tests/"));
    let store = Storage::open(&StorageConfig {
        url,
        endpoint: std::env::var("BF_TEST_STORAGE_ENDPOINT").ok(),
        region: Some(std::env::var("BF_TEST_STORAGE_REGION").unwrap_or("auto".into())),
        profile: std::env::var("BF_TEST_STORAGE_PROFILE").ok(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(store.mode(), StorageMode::Conditional);
    exercise(store.clone()).await;
    assert!(store.list("").await.unwrap().is_empty());
}
