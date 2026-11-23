//! One ANN object serves filtered and unfiltered queries, and directory rebuilds reuse it.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use bf_wal::Wal;
use serde_json::{Value, json};

fn vector(id: usize) -> Vec<f32> {
    (0..64)
        .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
        .collect()
}
fn engine(store: &Storage) -> Engine {
    Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    )
}
async fn ids(e: &Engine, q: usize) -> Vec<Value> {
    let request: QueryRequest =
        serde_json::from_value(json!({"rank_by":["vector","ANN",vector(q)],"top_k":10})).unwrap();
    let rows = e.query("t", request).await.unwrap().result.rows.unwrap();
    rows.iter().map(|r| r["id"].clone()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_ann_objects_serve_queries_and_survive_directory_rebuilds() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let writer = engine(&store);
    for batch in 0..3usize {
        let docs: Vec<_> = (0..3000)
            .map(|n| json!({"id":batch * 3000 + n,"vector":vector(batch * 3000 + n)}))
            .collect();
        writer
            .write(
                "t",
                serde_json::from_value::<WriteRequest>(
                    json!({"distance_metric":"euclidean_squared","upsert_rows":docs}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        writer.index_namespace("t", true).await.unwrap();
    }
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (sequence, mut manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(manifest.segments.len() >= 2);
    assert!(manifest.ann_dirs.contains_key("annv2:vector"));
    assert!(
        manifest
            .segments
            .iter()
            .all(|s| s.files.contains_key("annv2:vector")
                && !s.files.keys().any(|k| k.starts_with("annfrag")))
    );
    let queries = [5usize, 1234, 4321, 8000];
    let mut with_blocks = Vec::new();
    for q in queries {
        let rows = ids(&engine(&store), q).await;
        assert_eq!(rows[0], json!(q));
        with_blocks.push(rows);
    }

    // Losing a directory does not lose vectors: the next publication rebuilds metadata only.
    manifest.ann_dirs.clear();
    let epoch = manifest.indexer_epoch;
    manifest.indexer_epoch += 1;
    wal.publish_fenced(sequence, epoch, &manifest)
        .await
        .unwrap();
    for (q, want) in queries.iter().zip(&with_blocks) {
        assert_eq!(&ids(&engine(&store), *q).await, want, "document fallback");
    }

    // A new document triggers publication and rebuilds the directory without rewriting ANN objects.
    let upgrader = engine(&store);
    upgrader
        .write(
            "t",
            serde_json::from_value::<WriteRequest>(
                json!({"upsert_rows":[{"id":100_000,"vector":vec![1000.;64],"note":"publish"}]}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(upgrader.index_namespace("t", true).await.unwrap());
    let (_, upgraded) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(upgraded.ann_dirs.contains_key("annv2:vector"));
    assert!(
        upgraded
            .segments
            .iter()
            .all(|s| s.files.contains_key("annv2:vector")
                && !s.files.keys().any(|k| k.starts_with("annfrag")))
    );
    for (q, want) in queries.iter().zip(&with_blocks) {
        assert_eq!(&ids(&engine(&store), *q).await, want, "rebuilt directory");
    }
    for bound in [500, 4500] {
        let request: QueryRequest = serde_json::from_value(json!({
            "rank_by":["vector","ANN",vector(5)], "top_k":10,
            "filters":["id","Lt",bound]
        }))
        .unwrap();
        let response = engine(&store).query("t", request).await.unwrap();
        let rows = response.result.rows.unwrap();
        assert_eq!(rows[0]["id"], json!(5));
        assert!(rows.iter().all(|r| r["id"].as_u64().unwrap() < bound));
    }
    assert!(!upgrader.index_namespace("t", false).await.unwrap());
}
