use bf_storage::{BackendStats, Storage, StorageConfig};
use std::sync::Arc;

#[tokio::test]
async fn scopes_isolate_queries_and_account_for_missing_requests_and_physical_bytes() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    store.create("object", b"payload".to_vec()).await.unwrap();
    let first = Arc::new(BackendStats::default());
    let second = Arc::new(BackendStats::default());
    let (_, _) = tokio::join!(
        first.scope("snapshot", async {
            assert!(store.head("missing").await.is_err());
            store.list("").await.unwrap();
        }),
        second.scope("search", async {
            assert_eq!(store.get("object").await.unwrap().as_ref(), b"payload");
        })
    );
    let measured = first.snapshot();
    assert_eq!(measured.len(), 2);
    assert_eq!(measured[0].operation, "head");
    assert_eq!(measured[1].operation, "list");
    assert!(
        measured
            .iter()
            .all(|entry| entry.requests == 1 && entry.bytes == 0 && entry.stage == "snapshot")
    );
    let measured = second.snapshot();
    assert_eq!(measured.len(), 1);
    assert_eq!(measured[0].operation, "get");
    assert_eq!(measured[0].stage, "search");
    assert_eq!(measured[0].requests, 1);
    assert_eq!(measured[0].bytes, 24 + 7);
    // Work outside either scope never leaks into their measurements.
    store.get("object").await.unwrap();
    assert_eq!(second.snapshot(), measured);
}
