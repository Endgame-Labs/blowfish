use bf_engine::Engine;
use bf_storage::{Storage, StorageConfig};
use bf_types::*;
use serde_json::json;

async fn node(config: &StorageConfig) -> Engine {
    Engine::new(Storage::open(config).await.unwrap())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_nodes_group_commit_without_lost_or_duplicated_writes() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        url: format!("file://{}", dir.path().display()),
        ..Default::default()
    };
    let a = node(&config).await;
    let b = node(&config).await;
    let mut tasks = vec![];
    for id in 1..=128 {
        let engine = if id % 2 == 0 { a.clone() } else { b.clone() };
        tasks.push(tokio::spawn(async move {
            let response = engine
                .write(
                    "shared",
                    serde_json::from_value(json!({"upsert_rows":[{"id":id,"n":id}]})).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.rows_affected, 1);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let q: QueryRequest = serde_json::from_value(
        json!({"rank_by":["id","asc"],"top_k":1000,"include_attributes":true}),
    )
    .unwrap();
    for engine in [&a, &b, &node(&config).await] {
        let result = engine.query("shared", q.clone()).await.unwrap();
        let rows = result.result.rows.unwrap();
        assert_eq!(rows.len(), 128);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row["id"], i + 1);
            assert_eq!(row["n"], i + 1);
        }
    }
    let wal = bf_wal::Wal::new(a.storage().clone(), "shared").unwrap();
    let entries = wal.tail(0).await.unwrap();
    assert!(
        entries.iter().any(|(_, e)| e.batches.len() > 1),
        "concurrent requests should share a commit"
    );
    assert_eq!(
        entries.iter().map(|(_, e)| e.batches.len()).sum::<usize>(),
        128
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditions_are_revalidated_after_losing_a_slot() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        url: format!("file://{}", dir.path().display()),
        ..Default::default()
    };
    let a = node(&config).await;
    let b = node(&config).await;
    a.write(
        "conditional",
        serde_json::from_value(json!({"upsert_rows":[{"id":1,"n":0}]})).unwrap(),
    )
    .await
    .unwrap();
    b.warm("conditional").await.unwrap();
    let request = |n| {
        serde_json::from_value::<WriteRequest>(
            json!({"patch_rows":[{"id":1,"n":n}],"patch_condition":["n","Eq",0]}),
        )
        .unwrap()
    };
    let (left, right) = tokio::join!(
        a.write("conditional", request(1)),
        b.write("conditional", request(2))
    );
    assert_eq!(
        left.unwrap().rows_affected + right.unwrap().rows_affected,
        1
    );
    let r = a
        .query(
            "conditional",
            serde_json::from_value(
                json!({"rank_by":["id","asc"],"limit":1,"include_attributes":true}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!([json!(1), json!(2)].contains(&r.result.rows.unwrap()[0]["n"]));
}
#[tokio::test]
async fn eventual_snapshot_is_bounded_and_strong_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        url: format!("file://{}", dir.path().display()),
        ..Default::default()
    };
    let a = node(&config).await;
    let b = node(&config).await;
    a.write(
        "stale",
        serde_json::from_value(json!({"upsert_rows":[{"id":1}]})).unwrap(),
    )
    .await
    .unwrap();
    b.warm("stale").await.unwrap();
    a.write(
        "stale",
        serde_json::from_value(json!({"upsert_rows":[{"id":2}]})).unwrap(),
    )
    .await
    .unwrap();
    let req = |level| {
        serde_json::from_value::<QueryRequest>(
            json!({"aggregate_by":{"c":["Count"]},"consistency":{"level":level}}),
        )
        .unwrap()
    };
    assert_eq!(
        b.query("stale", req("eventual"))
            .await
            .unwrap()
            .result
            .aggregations
            .unwrap()["c"],
        1
    );
    assert_eq!(
        b.query("stale", req("strong"))
            .await
            .unwrap()
            .result
            .aggregations
            .unwrap()["c"],
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_strong_reads_include_every_previously_acknowledged_write() {
    use bf_engine::EngineConfig;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.,
        ..Default::default()
    };
    let writer = Engine::with_config(store.clone(), config.clone());
    let reader = Engine::with_config(store, config);
    writer
        .write(
            "linear",
            serde_json::from_value(json!({
                "upsert_rows":[{"id":1,"revision":0}],
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let request: QueryRequest = serde_json::from_value(json!({
        "rank_by":["id","asc"], "top_k":1, "include_attributes":true,
    }))
    .unwrap();
    reader.query("linear", request.clone()).await.unwrap();
    let acknowledged = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let (reader, request, acknowledged, barrier) = (
            reader.clone(),
            request.clone(),
            acknowledged.clone(),
            barrier.clone(),
        );
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..256 {
                let before = acknowledged.load(Ordering::Acquire);
                let response = reader.query("linear", request.clone()).await.unwrap();
                let revision = response.result.rows.unwrap()[0]["revision"]
                    .as_u64()
                    .unwrap();
                assert!(
                    revision >= before,
                    "strong read {revision} predates acknowledged write {before}"
                );
                tokio::task::yield_now().await;
            }
        }));
    }
    barrier.wait().await;
    for revision in 1..=128 {
        writer
            .write(
                "linear",
                serde_json::from_value(json!({
                    "patch_rows":[{"id":1,"revision":revision}],
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        acknowledged.store(revision, Ordering::Release);
        tokio::task::yield_now().await;
    }
    for task in tasks {
        task.await.unwrap();
    }
    let response = reader.query("linear", request).await.unwrap();
    assert_eq!(response.result.rows.unwrap()[0]["revision"], 128);
}
