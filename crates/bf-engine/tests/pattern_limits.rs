use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::json;

#[tokio::test]
async fn expensive_text_filters_and_highlights_return_errors() {
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
    e.write("text", serde_json::from_value(json!({
        "schema":{"text":{"type":"string","full_text_search":true,"fuzzy":true,"regex":true,"glob":true}},
        "upsert_rows":[{"id":1,"text":"fish. ".repeat(12000)}]
    })).unwrap()).await.unwrap();
    for op in ["Regex", "Glob", "Fuzzy", "ContainsAllTokens"] {
        let q = serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":1,
            "filters":["text",op,"x".repeat(8193)]}))
        .unwrap();
        assert_eq!(e.query("text", q).await.unwrap_err().status, 400);
    }
    let fuzzy =
        json!(["text","Fuzzy","fish",{"max_edit_distance":[{"min_query_chars":3,"distance":0}]}]);
    let q =
        serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":1,"filters":fuzzy})).unwrap();
    assert_eq!(e.query("text", q).await.unwrap_err().status, 400);
    let q = serde_json::from_value(json!({"rank_by":["text","BM25","fish"],"top_k":1,
        "compute_attributes":{"snippet":["Highlight","text",{"fragment_by":"word","fragment_limit":1}]}})).unwrap();
    assert_eq!(e.query("text", q).await.unwrap_err().status, 400);
}
