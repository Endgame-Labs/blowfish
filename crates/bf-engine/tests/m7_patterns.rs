use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::{Value, json};

async fn node() -> Engine {
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
async fn write(e: &Engine, value: Value) {
    e.write("patterns", serde_json::from_value(value).unwrap())
        .await
        .unwrap();
}
async fn rows(e: &Engine, filter: Value) -> Value {
    serde_json::to_value(
        e.query(
            "patterns",
            serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":100,"filters":filter}))
                .unwrap(),
        )
        .await
        .unwrap(),
    )
    .unwrap()["rows"]
        .clone()
}
#[tokio::test]
async fn fuzzy_substrings_thresholds_case_arrays_and_index_overlay() {
    let e = node().await;
    write(&e, json!({"schema":{"s":{"type":"string","fuzzy":true},"a":{"type":"[]string","fuzzy":true}},
        "upsert_rows":[{"id":1,"s":"prefix turbopuffer suffix","a":["turbo","puffer"]},{"id":2,"s":"Turbopufer","a":["a turbopuffer"]},{"id":3,"s":"túrbopuffer"},{"id":4}]})).await;
    let params = json!({"max_edit_distance":[{"min_query_chars":3,"distance":0},{"min_query_chars":6,"distance":1}],"case_sensitive":false});
    let tests = [
        (
            json!(["s", "Fuzzy", "turbopufer", params]),
            json!([{"id":1},{"id":2}]),
        ),
        (
            json!(["s", "Fuzzy", "turbopuffer", params]),
            json!([{"id":1},{"id":2},{"id":3}]),
        ),
        (json!(["s", "Fuzzy", "tu", params]), json!([])),
        (
            json!(["a", "Fuzzy", "turbopuffer", params]),
            json!([{"id":2}]),
        ),
        (
            json!(["Not", ["s", "Fuzzy", "turbopuffer", params]]),
            json!([{"id":4}]),
        ),
    ];
    for (filter, expected) in &tests {
        assert_eq!(rows(&e, filter.clone()).await, *expected);
    }
    e.index_namespace("patterns", true).await.unwrap();
    assert!(
        e.storage()
            .list("ns/patterns/segments/")
            .await
            .unwrap()
            .iter()
            .any(|f| f.key.contains("trigram."))
    );
    for (filter, expected) in &tests {
        assert_eq!(rows(&e, filter.clone()).await, *expected);
    }
    write(
        &e,
        json!({"patch_rows":[{"id":1,"s":"unrelated"}],"deletes":[2]}),
    )
    .await;
    assert_eq!(
        rows(&e, json!(["s", "Fuzzy", "turbopuffer", params])).await,
        json!([{"id":3}])
    );
    e.index_namespace("patterns", true).await.unwrap();
    assert_eq!(
        rows(&e, json!(["s", "Fuzzy", "turbopuffer", params])).await,
        json!([{"id":3}])
    );
}
#[tokio::test]
async fn online_fuzzy_schema_rebuild_and_validation() {
    let e = node().await;
    write(&e, json!({"upsert_rows":[{"id":1,"s":"turbopuffer"}]})).await;
    e.index_namespace("patterns", true).await.unwrap();
    write(&e, json!({"schema":{"s":{"fuzzy":true}}})).await;
    let q = json!({"rank_by":["id","asc"],"top_k":10,"filters":["s","Fuzzy","turbopufer",{"max_edit_distance":[{"min_query_chars":6,"distance":1}]}]});
    assert_eq!(
        e.query("patterns", serde_json::from_value(q.clone()).unwrap())
            .await
            .unwrap_err()
            .status,
        409
    );
    e.index_namespace("patterns", true).await.unwrap();
    assert_eq!(
        e.query("patterns", serde_json::from_value(q).unwrap())
            .await
            .unwrap()
            .result
            .rows
            .unwrap()
            .len(),
        1
    );
    for params in [
        json!({}),
        json!({"max_edit_distance":[]}),
        json!({"max_edit_distance":[{"min_query_chars":3,"distance":1}]}),
        json!({"max_edit_distance":[{"min_query_chars":12,"distance":3}]}),
    ] {
        assert_eq!(e.query("patterns",serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":10,"filters":["s","Fuzzy","turbopuffer",params]})).unwrap()).await.unwrap_err().status,400);
    }
}
