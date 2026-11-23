use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::json;
#[tokio::test]
async fn explain_identifies_real_indexes_segment_count_nprobe_and_tail() {
    let e = Engine::with_config(
        Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap(),
        EngineConfig {
            indexing: false,
            ann_nprobe: 7,
            ..Default::default()
        },
    );
    e.write("plan",serde_json::from_value(json!({"distance_metric":"cosine_distance","schema":{"s":{"type":"string","regex":true}},"upsert_rows":[{"id":1,"vector":[1,0],"s":"turbopuffer"},{"id":2,"vector":[0,1],"s":"other"}]})).unwrap()).await.unwrap();
    let q = json!({"rank_by":["vector","ANN",[1,0]],"top_k":1,"filters":["s","Regex","turbo"]});
    let plan = e
        .explain("plan", serde_json::from_value(q.clone()).unwrap())
        .await
        .unwrap()["plan_text"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(plan.contains("Segments touched: 0"));
    assert!(plan.contains("WAL tail rows: 2"));
    e.index_namespace("plan", true).await.unwrap();
    e.write(
        "plan",
        serde_json::from_value(json!({"patch_rows":[{"id":1,"s":"turbopuffer updated"}]})).unwrap(),
    )
    .await
    .unwrap();
    let plan = e
        .explain("plan", serde_json::from_value(q).unwrap())
        .await
        .unwrap()["plan_text"]
        .as_str()
        .unwrap()
        .to_owned();
    for text in [
        "Segments touched: 1",
        "WAL tail rows: 1",
        "nprobe=7",
        "annv2:vector",
        "trigram:s",
        "indexes used",
    ] {
        assert!(plan.contains(text), "missing {text}: {plan}");
    }
}

#[tokio::test]
async fn hosted_model_features_fail_with_clear_runtime_errors() {
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
    e.write("models",serde_json::from_value(json!({"distance_metric":"cosine_distance","upsert_rows":[{"id":1,"vector":[1,0],"s":"fox"}]})).unwrap()).await.unwrap();
    let queries = [
        json!({"rank_by":["vector","ANN",["Embed","fox"]],"top_k":1}),
        json!({"rank_by":["id","asc"],"top_k":1,"rerank_by":{"model":"unknown","attribute":"s","query":"fox"}}),
        json!({"queries":[{"rank_by":["id","asc"],"top_k":1}],"rerank_by":{"model":"unknown","attribute":"s","query":"fox"},"limit":1}),
        json!({"rank_by":["id","asc"],"top_k":1,"compute_attributes":{"e":["Embed","fox"]}}),
    ];
    for q in queries {
        let error = e
            .query("models", serde_json::from_value(q).unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.status, 400);
        assert!(error.message.contains("not supported by blowfish yet"));
        assert!(error.message.contains("Embed") || error.message.contains("rerank_by"));
    }
    let error = e
        .write(
            "models",
            serde_json::from_value(json!({"upsert_rows":[{"id":2,"vector":["Embed","fox"]}]}))
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.status, 400);
    assert!(error.message.contains("Embed"));
}
