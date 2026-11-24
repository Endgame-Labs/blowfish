#![cfg(feature = "experimental-hot-graph")]
//! The experimental path preserves masks, WAL shadowing, delta tiers, and filtered fallback.
use bf_engine::{
    Engine, EngineConfig, ReclusterOptions,
    hot_graph::{self, HotGraphOptions},
};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use bf_wal::Wal;
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        hot_graph::evict();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
async fn query(e: &Engine, vector: &[f32], filter: Option<Value>) -> Value {
    let mut body =
        json!({"rank_by":["vector","ANN",vector],"top_k":10,"include_attributes":["group"]});
    if let Some(filter) = filter {
        body["filters"] = filter;
    }
    let q: QueryRequest = serde_json::from_value(body).unwrap();
    serde_json::to_value(
        tokio::time::timeout(Duration::from_secs(15), e.query("g3", q))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}
async fn write(e: &Engine, body: Value) {
    e.write("g3", serde_json::from_value::<WriteRequest>(body).unwrap())
        .await
        .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_tier_graph_preserves_query_semantics() {
    hot_graph::evict();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.scratch")
        .join(format!("g3-hot-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    let _cleanup = Cleanup(path.clone());
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let e = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            ..Default::default()
        },
    );
    let mut seed = 7u64;
    let vectors: Vec<Vec<f32>> = (0..2500)
        .map(|_| {
            (0..16)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    ((seed >> 40) as f32 / 16_777_216.) * 2. - 1.
                })
                .collect()
        })
        .collect();
    let rows: Vec<_> = vectors
        .iter()
        .enumerate()
        .map(|(id, v)| json!({"id":id,"vector":v,"group":id%3}))
        .collect();
    write(&e,json!({"distance_metric":"cosine_distance","schema":{"vector":{"type":"[16]f16","ann":true}},"upsert_rows":rows})).await;
    e.index_namespace("g3", true).await.unwrap();
    let opts = ReclusterOptions {
        attribute: "vector".into(),
        cells: 2,
        cluster_rows: 32,
        max_slab_rows: 5000,
        scratch: path.clone(),
    };
    e.recluster("g3", &opts).await.unwrap();
    assert!(
        e.build_hot_graph(
            "g3",
            "vector",
            HotGraphOptions {
                build_budget_bytes: 1024,
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_none()
    );
    let report = e
        .build_hot_graph(
            "g3",
            "vector",
            HotGraphOptions {
                query_ef: 256,
                rerank: 80,
                build_budget_bytes: 64 * 1024 * 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert!(report.installed);
    assert_eq!(report.rows, 2500);
    let before = hot_graph::answered_queries();
    let answer = query(&e, &vectors[17], None).await;
    assert_eq!(answer["rows"][0]["id"], 17);
    assert!(hot_graph::answered_queries() > before);
    write(&e, json!({"deletes":[17]})).await;
    let answer = query(&e, &vectors[17], None).await;
    assert!(
        answer["rows"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["id"] != 17)
    );
    write(
        &e,
        json!({"upsert_rows":[{"id":5000,"vector":vectors[17],"group":1}]}),
    )
    .await;
    assert_eq!(query(&e, &vectors[17], None).await["rows"][0]["id"], 5000);
    e.index_namespace("g3", true).await.unwrap();
    assert_eq!(query(&e, &vectors[17], None).await["rows"][0]["id"], 5000);
    e.recluster("g3", &opts).await.unwrap();
    let (_, manifest) = Wal::new(store, "g3")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    assert!(
        manifest.ann_tiers["vector"].len() > 1,
        "exercise the delta-tier path"
    );
    let before = hot_graph::answered_queries();
    assert_eq!(query(&e, &vectors[17], None).await["rows"][0]["id"], 5000);
    assert!(hot_graph::answered_queries() > before);
    let before = hot_graph::answered_queries();
    let filtered = query(&e, &vectors[17], Some(json!(["group", "Eq", 1]))).await;
    assert!(
        filtered["rows"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["group"] == 1)
    );
    assert_eq!(
        hot_graph::answered_queries(),
        before,
        "filters retain the existing planner"
    );
    let _ = query(&e, &[0.; 16], None).await;
    assert_eq!(
        hot_graph::answered_queries(),
        before,
        "zero queries retain the existing planner"
    );
}
