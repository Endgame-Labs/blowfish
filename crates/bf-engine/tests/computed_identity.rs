use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::QueryRequest;
use serde_json::json;

#[tokio::test]
async fn computed_attributes_cannot_replace_identity_or_panic_rrf() {
    let e = Engine::with_config(
        Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap(),
        EngineConfig {
            indexing: false,
            ..Default::default()
        },
    );
    e.write(
        "identity",
        serde_json::from_value(json!({"upsert_rows":[{"id":1,"n":2}]})).unwrap(),
    )
    .await
    .unwrap();
    for indexed in [false, true] {
        if indexed {
            e.index_namespace("identity", true).await.unwrap();
        }
        let single = json!({"rank_by":["id","asc"],"top_k":1,"compute_attributes":{"id":0.5}});
        for value in [
            single.clone(),
            json!({"queries":[single.clone(),single],"rerank_by":["RRF"],"limit":1}),
        ] {
            let q: QueryRequest = serde_json::from_value(value).unwrap();
            assert_eq!(
                e.query("identity", q.clone()).await.unwrap_err().status,
                400
            );
            assert_eq!(
                e.query_exhaustive("identity", q).await.unwrap_err().status,
                400
            );
        }
    }
}
