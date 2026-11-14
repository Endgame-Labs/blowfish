use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::json;

async fn engine() -> Engine {
    Engine::with_config(
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
    )
}
#[tokio::test]
async fn cartesian_group_expansion_is_bounded_in_both_evaluators() {
    let e = engine().await;
    e.write(
        "groups",
        serde_json::from_value(json!({"upsert_rows":[{
            "id":1, "a":(0..101).collect::<Vec<_>>(), "b":(0..100).collect::<Vec<_>>()
        }]}))
        .unwrap(),
    )
    .await
    .unwrap();
    let q: bf_types::QueryRequest = serde_json::from_value(json!({"aggregate_by":{"n":["Count"]},
        "group_by":[{"a":["ForEachUnique","a"]},{"b":["ForEachUnique","b"]}],
        "limit":1}))
    .unwrap();
    for indexed in [false, true] {
        if indexed {
            e.index_namespace("groups", true).await.unwrap();
        }
        assert_eq!(e.query("groups", q.clone()).await.unwrap_err().status, 400);
        assert_eq!(
            e.query_exhaustive("groups", q.clone())
                .await
                .unwrap_err()
                .status,
            400
        );
    }
    let q: bf_types::QueryRequest = serde_json::from_value(json!({"aggregate_by":{"n":["Count"]},
        "group_by":(0..33).map(|i| json!({format!("g{i}"):["ForEachUnique","a"]})).collect::<Vec<_>>()
    })).unwrap();
    assert_eq!(e.query("groups", q).await.unwrap_err().status, 400);
}
