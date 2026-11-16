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

#[tokio::test]
async fn create_unique_uploads_large_objects_in_parts_and_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", dir.path().display()),
        ..Default::default()
    })
    .await
    .unwrap();
    // Above the multipart threshold, and not a multiple of the part size.
    let value: Vec<u8> = (0..(40usize << 20) + 12_345)
        .map(|i| (i % 251) as u8)
        .collect();
    assert_eq!(
        store
            .create_unique("big/object", value.clone())
            .await
            .unwrap(),
        CreateResult::Created
    );
    let reader = store.range_reader("big/object").await.unwrap();
    assert_eq!(reader.len(), value.len() as u64);
    let tail = reader
        .read(value.len() as u64 - 5000..value.len() as u64)
        .await
        .unwrap();
    assert_eq!(&tail[..], &value[value.len() - 5000..]);
    let middle = reader.read((17 << 20) - 10..(17 << 20) + 10).await.unwrap();
    assert_eq!(&middle[..], &value[(17 << 20) - 10..(17 << 20) + 10]);
    // Small values keep conditional-create semantics.
    assert_eq!(
        store.create_unique("small", "a").await.unwrap(),
        CreateResult::Created
    );
    assert_eq!(
        store.create_unique("small", "b").await.unwrap(),
        CreateResult::Conflict
    );
}

#[tokio::test]
async fn scoped_views_are_isolated_but_share_the_backend() {
    let root = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let a = root.scoped("projects/a");
    let b = root.scoped("projects/b");
    a.create("ns/docs/x", "from a").await.unwrap();
    b.create("ns/docs/x", "from b").await.unwrap();
    assert_eq!(a.get("ns/docs/x").await.unwrap(), "from a");
    assert_eq!(b.get("ns/docs/x").await.unwrap(), "from b");
    // The shared range cache keys by full location, so same-named objects never collide.
    for (store, want) in [(&a, "from a"), (&b, "from b")] {
        assert_eq!(store.get_cached("ns/docs/x").await.unwrap(), want);
        let reader = store.range_reader("ns/docs/x").await.unwrap();
        assert_eq!(reader.read(0..6).await.unwrap(), want);
    }
    assert_eq!(a.list("ns/").await.unwrap().len(), 1);
    assert_eq!(a.list("ns/").await.unwrap()[0].key, "ns/docs/x");
    // The root sees the projects only by their full path.
    assert!(root.list("ns/").await.unwrap().is_empty());
    assert_eq!(root.get("projects/a/ns/docs/x").await.unwrap(), "from a");
    assert_eq!(
        root.scoped("projects")
            .scoped("a")
            .get("ns/docs/x")
            .await
            .unwrap(),
        "from a"
    );
}
