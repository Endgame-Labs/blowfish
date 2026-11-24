//! Immutable predicate caching across WAL shadows, live generations, and compaction lineage.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use bf_wal::{Manifest, Wal};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

fn config() -> EngineConfig {
    EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        retention: Duration::ZERO,
        ann_nprobe: 256,
        compaction_segments: 100,
        ..Default::default()
    }
}
fn engine(store: &Storage) -> Engine {
    Engine::with_config(store.clone(), config())
}
fn vector(id: usize) -> Vec<f32> {
    (0..16)
        .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
        .collect()
}
async fn write(e: &Engine, ns: &str, body: Value) {
    e.write(ns, serde_json::from_value(body).unwrap())
        .await
        .unwrap();
}
async fn query(e: &Engine, ns: &str, body: Value) -> Vec<bf_types::Document> {
    e.query(ns, serde_json::from_value(body).unwrap())
        .await
        .unwrap()
        .result
        .rows
        .unwrap()
}
async fn manifest(store: &Storage) -> Manifest {
    Wal::new(store.clone(), "t")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap()
        .1
}
fn scratch() -> tempfile::TempDir {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.scratch/d4-tests");
    std::fs::create_dir_all(&path).unwrap();
    tempfile::tempdir_in(path).unwrap()
}
async fn setup(store: &Storage, path: &Path) -> (Engine, Vec<bf_wal::Segment>) {
    let e = engine(store);
    for batch in 0..3 {
        let docs: Vec<_> = (batch*600..(batch+1)*600).map(|id| {
            json!({"id":id,"vector":vector(id),"group":id%5,"text":format!("fish pond {id}"),"optional":vector(id+7)})
        }).collect();
        write(&e,"t",json!({"distance_metric":"euclidean_squared","schema":{"vector":{"type":"[16]f16"},"optional":{"type":"[16]f32","ann":false},"text":{"type":"string","full_text_search":true}},"upsert_rows":docs})).await;
        e.index_namespace("t", true).await.unwrap();
    }
    let own = manifest(store).await.segments;
    e.recluster(
        "t",
        &ReclusterOptions {
            attribute: "vector".into(),
            cells: 3,
            cluster_rows: 30,
            max_slab_rows: 800,
            scratch: path.into(),
        },
    )
    .await
    .unwrap();
    (e, own)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_predicates_follow_tail_shadows_live_generations_and_lineage() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let scratch = scratch();
    let (e, _) = setup(&store, scratch.path()).await;
    e.collect_garbage("t").await.unwrap();
    let filtered = json!({"rank_by":["vector","ANN",vector(853)],"filters":["group","Lt",4],"include_attributes":true,"top_k":10});
    let mut exact = filtered.clone();
    exact["rank_by"][1] = json!("kNN");
    let want = query(&e, "t", exact.clone()).await;
    for _ in 0..2 {
        assert_eq!(query(&e, "t", filtered.clone()).await, want);
    }
    // A cached immutable predicate must never admit a tail-shadowed row. Tail rows are
    // evaluated separately, and the live generation will change when the tail is flushed.
    write(
        &e,
        "t",
        json!({"patch_rows":[{"id":853,"group":4}],"deletes":[858],"upsert_rows":[{"id":1801,"group":3,"vector":vector(853),"optional":vector(860)}]}),
    )
    .await;
    let changed = query(&e, "t", exact.clone()).await;
    assert_eq!(changed[0]["id"], 1801);
    assert!(
        changed
            .iter()
            .all(|doc| doc["id"] != 853 && doc["id"] != 858)
    );
    assert_eq!(query(&e, "t", filtered.clone()).await, changed);
    e.index_namespace("t", true).await.unwrap();
    assert_eq!(query(&e, "t", filtered.clone()).await, changed);
    let merging = Engine::with_config(
        store.clone(),
        EngineConfig {
            compaction_segments: 2,
            max_segment_rows: 3000,
            ..config()
        },
    );
    merging.index_namespace("t", true).await.unwrap();
    merging.collect_garbage("t").await.unwrap();
    for _ in 0..2 {
        assert_eq!(query(&e, "t", filtered.clone()).await, changed);
    }
    e.drop_caches().await;
    assert_eq!(query(&e, "t", filtered).await, changed);
    assert_eq!(query(&engine(&store), "t", exact).await, changed);
}
