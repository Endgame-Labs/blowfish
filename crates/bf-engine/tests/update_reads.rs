//! Replacements do not need old vectors; patches and conditional writes still read old rows.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_wal::Wal;
use serde_json::json;

#[tokio::test]
async fn unconditional_replacements_do_not_read_the_old_document_or_vector_objects() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    engine.write("u",serde_json::from_value(json!({"schema":{"vector":{"type":"[3]f16"}},"distance_metric":"cosine_distance","upsert_rows":[{"id":1,"vector":[1.,0.,0.],"title":"old"}]})).unwrap()).await.unwrap();
    engine.index_namespace("u", true).await.unwrap();
    let (_, m) = Wal::new(store.clone(), "u")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    let mut saved = Vec::new();
    for role in ["docs", "annv2:vector"] {
        let key = &m.segments[0].files[role].key;
        saved.push((key.clone(), store.get(key).await.unwrap()));
        store.delete(key).await.unwrap();
    }
    engine
        .write(
            "u",
            serde_json::from_value(
                json!({"upsert_rows":[{"id":1,"vector":[0.,1.,0.],"title":"new"}]}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    for (key, bytes) in saved {
        store.create_unique(&key, bytes).await.unwrap();
    }
    let rows = engine
        .query(
            "u",
            serde_json::from_value(
                json!({"rank_by":["id","asc"],"top_k":10,"include_attributes":true}),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["title"], "new");
    engine.index_namespace("u", true).await.unwrap();
    let before = engine.metadata("u").await.unwrap();
    let response = engine.write("u",serde_json::from_value(json!({"upsert_rows":[{"id":1,"vector":[0.,0.,1.],"title":"rejected"}],"upsert_condition":["title","Eq","old"]})).unwrap()).await.unwrap();
    assert_eq!(response.rows_affected, 0);
    engine.write("u",serde_json::from_value(json!({"patch_rows":[{"id":1,"title":"patched"}],"patch_condition":["title","Eq","new"]})).unwrap()).await.unwrap();
    let during = engine.metadata("u").await.unwrap();
    engine.index_namespace("u", true).await.unwrap();
    let after = engine.metadata("u").await.unwrap();
    assert_eq!(before["approx_row_count"], 1);
    assert_eq!(during["approx_row_count"], 1);
    assert_eq!(
        during["approx_logical_bytes"],
        after["approx_logical_bytes"]
    );
    let rows = engine
        .query(
            "u",
            serde_json::from_value(
                json!({"rank_by":["id","asc"],"top_k":10,"include_attributes":true}),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    assert_eq!(rows[0]["title"], "patched");
}
