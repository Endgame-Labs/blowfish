use bf_storage::{BUCKET_FORMAT_VERSION, Error, Storage, StorageConfig};

#[tokio::test]
async fn generations_are_monotonic_root_scoped_and_checked_before_startup_writes() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        url: format!("file://{}", dir.path().display()),
        ..Default::default()
    };
    let store = Storage::open(&config).await.unwrap();
    let marker = format!("_system/format/{BUCKET_FORMAT_VERSION}.json");
    let original = store.get(&marker).await.unwrap();
    // A legacy generation arriving late never lowers the marker.
    store.create("_system/format/1.json", "{}").await.unwrap();
    let scoped = store.scoped("projects/test");
    scoped.ensure_bucket_format().await.unwrap();
    assert_eq!(
        scoped.check_bucket_format().await.unwrap(),
        BUCKET_FORMAT_VERSION
    );
    assert_eq!(store.get(&marker).await.unwrap(), original);
    assert!(scoped.list("_system/format/").await.unwrap().is_empty());
    store.create("_system/format/12.json", "{}").await.unwrap();
    assert!(matches!(
        scoped.ensure_bucket_format().await,
        Err(Error::BucketFormat { required: 12, .. })
    ));
    assert!(matches!(
        store.create("unrelated", "blocked").await,
        Err(Error::BucketFormat { required: 12, .. })
    ));
    let before = store
        .list("")
        .await
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect::<Vec<_>>();
    for read_only in [false, true] {
        let error = match Storage::open(&StorageConfig {
            read_only,
            ..config.clone()
        })
        .await
        {
            Ok(_) => panic!("a newer bucket must refuse startup"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("requires format version 12"));
    }
    assert_eq!(
        before,
        store
            .list("")
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn read_only_legacy_startup_does_not_create_a_marker() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        url: format!("file://{}", dir.path().display()),
        read_only: true,
        ..Default::default()
    };
    let reader = Storage::open(&config).await.unwrap();
    assert_eq!(reader.check_bucket_format().await.unwrap(), 1);
    assert!(reader.list("").await.unwrap().is_empty());
    let writable = StorageConfig {
        read_only: false,
        ..config
    };
    let (a, b) = tokio::join!(Storage::open(&writable), Storage::open(&writable));
    let writer = a.unwrap();
    b.unwrap();
    assert_eq!(
        writer.check_bucket_format().await.unwrap(),
        BUCKET_FORMAT_VERSION
    );
    assert_eq!(writer.list("_system/format/").await.unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn background_refresh_is_delayed_shared_and_permanently_fails_closed() {
    use object_store::{ObjectStoreExt, path::Path};
    use std::{sync::Arc, time::Duration};

    let backend = Arc::new(object_store::memory::InMemory::new());
    let store = Storage::from_backend(backend.clone(), Path::default(), false, false)
        .await
        .unwrap();
    let scoped = store.scoped("projects/test");
    // Arm the background timer. Clones and scoped views do not start extra refreshers.
    tokio::task::yield_now().await;
    let marker = Path::from("_system/format/4.json");
    backend.put(&marker, "{}".into()).await.unwrap();
    tokio::time::advance(Duration::from_secs(59)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        scoped.cached_bucket_format().unwrap(),
        BUCKET_FORMAT_VERSION
    );
    scoped.create("before-refresh", "ok").await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        scoped.cached_bucket_format(),
        Err(Error::BucketFormat { required: 4, .. })
    ));
    backend.delete(&marker).await.unwrap();
    // Even an explicit successful re-list cannot lower the process's observed generation.
    assert!(matches!(
        store.check_bucket_format().await,
        Err(Error::BucketFormat { required: 4, .. })
    ));
    assert!(scoped.put("after-refresh", "blocked").await.is_err());
    assert!(store.delete("projects/test/before-refresh").await.is_err());
}
