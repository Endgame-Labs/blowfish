use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_wal::Wal;
use serde_json::{Value, json};
use std::time::Duration;
async fn node() -> Engine {
    Engine::with_config(
        Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap(),
        config(),
    )
}
fn config() -> EngineConfig {
    EngineConfig {
        indexing: false,
        retention: Duration::ZERO,
        compaction_segments: 3,
        ..Default::default()
    }
}
async fn write(e: &Engine, ns: &str, body: Value) -> bf_types::WriteResponse {
    e.write(ns, serde_json::from_value(body).unwrap())
        .await
        .unwrap()
}
async fn rows(e: &Engine, ns: &str) -> Value {
    serde_json::to_value(
        e.query(
            ns,
            serde_json::from_value(
                json!({"rank_by":["id","asc"],"top_k":100,"include_attributes":true}),
            )
            .unwrap(),
        )
        .await
        .unwrap(),
    )
    .unwrap()["rows"]
        .clone()
}
#[tokio::test]
async fn copy_and_branch_share_segments_replay_tail_and_survive_source_gc() {
    let e = node().await;
    write(
        &e,
        "source",
        json!({"upsert_rows":[{"id":1,"s":"first"},{"id":2,"s":"second"},{"id":3,"s":"third"}]}),
    )
    .await;
    e.index_namespace("source", true).await.unwrap();
    let original = Wal::new(e.storage().clone(), "source")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap()
        .1;
    write(&e,"source",json!({"patch_rows":[{"id":1,"s":"updated"}],"deletes":[2],"upsert_rows":[{"id":4,"s":"fourth"}]})).await;
    let expected = rows(&e, "source").await;
    let r = write(&e, "branch", json!({"branch_from_namespace":"source"})).await;
    assert_eq!(
        e.metadata("branch").await.unwrap()["branching"],
        json!({"parent":"source","depth":1})
    );
    assert_eq!(r.rows_affected, 0);
    assert_eq!(r.message, "namespace branch successful");
    let r = write(
        &e,
        "copy",
        json!({"copy_from_namespace":{"source_namespace":"source"}}),
    )
    .await;
    assert!(e.metadata("copy").await.unwrap().get("branching").is_none());
    assert_eq!(r.rows_affected, 3);
    assert!(
        e.storage()
            .list("ns/branch/segments/")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(rows(&e, "branch").await, expected);
    assert_eq!(rows(&e, "copy").await, expected);
    let cold = Engine::with_config(e.storage().clone(), config());
    assert_eq!(rows(&cold, "branch").await, expected);
    assert_eq!(rows(&cold, "copy").await, expected);
    e.index_namespace("branch", true).await.unwrap();
    e.index_namespace("copy", true).await.unwrap();
    let branched = Wal::new(e.storage().clone(), "branch")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap()
        .1;
    assert!(
        branched
            .segments
            .iter()
            .any(|seg| seg.files["docs"].key == original.segments[0].files["docs"].key)
    );
    assert!(
        branched
            .segments
            .iter()
            .filter(|seg| seg.id == original.segments[0].id)
            .all(|seg| seg.live.key.starts_with("ns/branch/"))
    );
    write(
        &e,
        "branch",
        json!({"patch_rows":[{"id":3,"s":"branch only"}]}),
    )
    .await;
    write(
        &e,
        "source",
        json!({"patch_rows":[{"id":3,"s":"source only"}]}),
    )
    .await;
    assert_eq!(rows(&e, "copy").await, expected);
    assert_eq!(rows(&e, "branch").await[1]["s"], "branch only");
    e.delete_namespace("source").await.unwrap();
    e.collect_garbage("source").await.unwrap();
    e.collect_garbage("source").await.unwrap();
    let cold = Engine::with_config(e.storage().clone(), config());
    assert_eq!(rows(&cold, "copy").await, expected);
    assert_eq!(rows(&cold, "branch").await[1]["s"], "branch only");
    e.index_namespace("branch", true).await.unwrap();
    e.collect_garbage("branch").await.unwrap();
    assert_eq!(rows(&cold, "copy").await, expected);
    let before = rows(&e, "copy").await;
    assert_eq!(
        e.write(
            "copy",
            serde_json::from_value(json!({"branch_from_namespace":{"source_namespace":"branch"}}))
                .unwrap()
        )
        .await
        .unwrap_err()
        .status,
        400
    );
    assert_eq!(rows(&e, "copy").await, before);
}
#[tokio::test]
async fn branch_of_branch_is_independent_after_ancestors_are_deleted() {
    let e = node().await;
    write(
        &e,
        "root",
        json!({"upsert_rows":[{"id":1,"s":"first"},{"id":2,"s":"second"}]}),
    )
    .await;
    e.index_namespace("root", true).await.unwrap();
    write(
        &e,
        "child",
        json!({"branch_from_namespace":{"source_namespace":"root"}}),
    )
    .await;
    write(&e, "child", json!({"deletes":[2]})).await;
    e.index_namespace("child", true).await.unwrap();
    write(&e, "grandchild", json!({"branch_from_namespace":"child"})).await;
    assert_eq!(
        e.metadata("grandchild").await.unwrap()["branching"],
        json!({"parent":"child","depth":2})
    );
    e.delete_namespace("root").await.unwrap();
    e.collect_garbage("root").await.unwrap();
    e.delete_namespace("child").await.unwrap();
    e.collect_garbage("child").await.unwrap();
    let cold = Engine::with_config(e.storage().clone(), config());
    assert_eq!(
        rows(&cold, "grandchild").await,
        json!([{"id":1,"s":"first"}])
    );
}
