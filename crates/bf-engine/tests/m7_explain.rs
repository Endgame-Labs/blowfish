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
        "ann:vector",
        "trigram:s",
        "indexes used",
    ] {
        assert!(plan.contains(text), "missing {text}: {plan}");
    }
}
