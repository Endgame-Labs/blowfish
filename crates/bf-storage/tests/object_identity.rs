use bf_storage::{Storage, StorageConfig};

#[tokio::test]
async fn durable_identity_separates_backends_and_detects_same_size_replacement() {
    let config = StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    };
    let left = Storage::open(&config).await.unwrap();
    let right = Storage::open(&config).await.unwrap();
    assert_ne!(left.cache_identity(), right.cache_identity());
    assert_eq!(left.cache_identity(), left.clone().cache_identity());
    assert_eq!(
        left.cache_identity(),
        left.scoped("tenant").cache_identity()
    );

    left.create_unique("immutable", b"first".to_vec())
        .await
        .unwrap();
    let initial = left.object_identity("immutable").await.unwrap();
    assert_eq!(initial, left.object_identity("immutable").await.unwrap());
    assert!(initial.etag.is_some() || initial.version.is_some());
    left.delete("immutable").await.unwrap();
    left.create_unique("immutable", b"other".to_vec())
        .await
        .unwrap();
    let replaced = left.object_identity("immutable").await.unwrap();
    assert_eq!(initial.size, replaced.size);
    assert_ne!(initial, replaced);
    assert!(right.object_identity("immutable").await.is_err());
    assert!(
        left.scoped("tenant")
            .object_identity("immutable")
            .await
            .is_err()
    );
}
