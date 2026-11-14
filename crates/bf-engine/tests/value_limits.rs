use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::{Value, json};

async fn write(e: &Engine, v: Value) -> Result<bf_types::WriteResponse, bf_types::ApiError> {
    e.write("sizes", serde_json::from_value(v).unwrap()).await
}
#[tokio::test]
async fn value_caps_apply_atomically_to_rows_columns_and_filter_patches() {
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
    write(&e, json!({"upsert_rows":[{"id":1,"s":"a".repeat(4096),"tags":["a".repeat(4096),"b".repeat(4096)]}]})).await.unwrap();
    for request in [
        json!({"upsert_rows":[{"id":2,"s":"ok"},{"id":3,"s":"é".repeat(2049)}]}),
        json!({"upsert_columns":{"id":[2],"s":["x".repeat(4097)]}}),
        json!({"patch_rows":[{"id":1,"tags":["x".repeat(4097)]}]}),
        json!({"patch_by_filter":{"filters":["id","Eq",1],"patch":{"s":"x".repeat(4097)}}}),
        json!({"schema":{"large":{"type":"string","filterable":false}},"upsert_rows":[{"id":4,"large":"x".repeat(8*1024*1024+1)}]}),
    ] {
        assert_eq!(write(&e, request).await.unwrap_err().status, 400);
    }
    assert_eq!(e.metadata("sizes").await.unwrap()["approx_row_count"], 1);
    let q = serde_json::from_value(
        json!({"rank_by":["id","asc"],"top_k":10,"include_attributes":true}),
    )
    .unwrap();
    let rows = e.query("sizes", q).await.unwrap().result.rows.unwrap();
    assert_eq!(rows[0]["s"], "a".repeat(4096));
}

#[tokio::test]
async fn merged_patch_cannot_exceed_document_cap() {
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
    let schema: serde_json::Map<_, _> = (0..9)
        .map(|i| (format!("a{i}"), json!({"type":"string","filterable":false})))
        .collect();
    let mut row = serde_json::Map::from_iter([("id".into(), json!(1))]);
    for i in 0..7 {
        row.insert(format!("a{i}"), json!("x".repeat(8 * 1024 * 1024)));
    }
    write(&e, json!({"schema":schema,"upsert_rows":[row]}))
        .await
        .unwrap();
    assert_eq!(write(&e, json!({"patch_rows":[{"id":1,"a7":"x".repeat(8*1024*1024),"a8":"x".repeat(8*1024*1024)}]})).await.unwrap_err().status, 400);
}
