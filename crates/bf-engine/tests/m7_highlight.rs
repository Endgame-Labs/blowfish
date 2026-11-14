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
async fn query(e: &Engine, q: Value) -> Value {
    serde_json::to_value(
        e.query("highlight", serde_json::from_value(q).unwrap())
            .await
            .unwrap(),
    )
    .unwrap()["rows"]
        .clone()
}
#[tokio::test]
async fn highlights_fragments_offsets_arrays_fuzzy_and_indexed_projection() {
    let e = node().await;
    e.write("highlight",serde_json::from_value(json!({"schema":{"s":{"type":"string","full_text_search":true,"fuzzy":true},"a":{"type":"[]string","full_text_search":true}},"upsert_rows":[
        {"id":1,"s":"🐡 Red fox.\nThe fox runs.\n\nA bird flies.","a":["A fox. Next bird.","More fox fox."]},{"id":2,"s":"nothing"}]})).unwrap()).await.unwrap();
    let queries = [
        json!({"rank_by":["s","BM25","fox"],"top_k":10,"compute_attributes":{"h":["Highlight","s",{"include_offsets":"utf-8"}]}}),
        json!({"rank_by":["s","BM25","fox"],"top_k":10,"compute_attributes":{"h":["Highlight","s",{"fragment_by":"none","include_offsets":"utf-16"}]}}),
        json!({"rank_by":["s","BM25","fox"],"top_k":10,"compute_attributes":{"h":["Highlight","s",{"fragment_by":"word","include_offsets":"codepoints"}]}}),
        json!({"rank_by":["a","BM25","fox"],"top_k":10,"compute_attributes":{"h":["Highlight","a",{"include_offsets":"utf-8"}]}}),
        json!({"rank_by":["s","Fuzzy","fox",{"max_edit_distance":[{"min_query_chars":3,"distance":0}]}],"top_k":10,"compute_attributes":{"h":["Highlight","s",{"fragment_by":"none","include_offsets":"utf-8"}]}}),
        json!({"rank_by":["id","asc"],"top_k":10,"compute_attributes":{"h":["Highlight","s",{"rank_fragments_by":["$fragment","BM25","bird"],"include_offsets":"utf-8"}]}}),
    ];
    let mut expected = vec![];
    for q in &queries {
        expected.push(query(&e, q.clone()).await)
    }
    assert_eq!(expected[1][0]["h"][0]["fragment_range"], json!([0, 40]));
    assert_eq!(
        expected[1][0]["h"][0]["match_ranges"],
        json!([[7, 10], [16, 19]])
    );
    assert_eq!(expected[3][0]["h"][0]["array_index"], 1);
    assert_eq!(
        expected[3][0]["h"][0]["match_ranges"],
        json!([[5, 8], [9, 12]])
    );
    assert_eq!(
        expected[4][0]["h"][0]["match_ranges"],
        json!([[9, 12], [18, 21]])
    );
    assert_eq!(expected[5][1]["h"], json!([]));
    e.index_namespace("highlight", true).await.unwrap();
    for (i, (q, want)) in queries.iter().zip(expected).enumerate() {
        assert_eq!(query(&e, q.clone()).await, want, "indexed query {i}")
    }
    for options in [
        json!({"fragment_limit":0}),
        json!({"fragment_limit":1025}),
        json!({"include_offsets":"bytes"}),
        json!({"fragment_by":"token"}),
    ] {
        let q = json!({"rank_by":["s","BM25","fox"],"top_k":10,"compute_attributes":{"h":["Highlight","s",options]}});
        assert_eq!(
            e.query("highlight", serde_json::from_value(q).unwrap())
                .await
                .unwrap_err()
                .status,
            400
        );
    }
}
