//! Block fragments (codes beside blobs) answer unfiltered ANN queries like the plain ones, and
//! segments built before them are upgraded by the next indexing pass.
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
async fn block_layout_matches_plain_fragments_and_old_segments_are_upgraded() {
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
    assert!(manifest.ann_dirs.contains_key("ann:vector"));
    assert!(manifest.ann_dirs.contains_key("ann2:vector"));
    assert!(
        manifest
            .segments
            .iter()
            .all(|s| s.files.contains_key("annfrag2:vector"))
    );
    let queries = [5usize, 1234, 4321, 8000];
    let mut with_blocks = Vec::new();
    for q in queries {
        let rows = ids(&engine(&store), q).await;
        assert_eq!(rows[0], json!(q));
        with_blocks.push(rows);
    }

    // A namespace written before block fragments: drop them from the manifest.
    for seg in &mut manifest.segments {
        seg.files.remove("annfrag2:vector");
    }
    manifest.ann_dirs.remove("ann2:vector");
    let epoch = manifest.indexer_epoch;
    manifest.indexer_epoch += 1;
    wal.publish_fenced(sequence, epoch, &manifest)
        .await
        .unwrap();
    for (q, want) in queries.iter().zip(&with_blocks) {
        assert_eq!(&ids(&engine(&store), *q).await, want, "plain fragments");
    }

    // The next indexing pass notices and builds them, though there is no tail to flush.
    let upgrader = engine(&store);
    assert!(upgrader.index_namespace("t", false).await.unwrap());
    let (_, upgraded) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(upgraded.ann_dirs.contains_key("ann2:vector"));
    assert!(
        upgraded
            .segments
            .iter()
            .all(|s| s.files.contains_key("annfrag2:vector"))
    );
    for (q, want) in queries.iter().zip(&with_blocks) {
        assert_eq!(
            &ids(&engine(&store), *q).await,
            want,
            "upgraded block fragments"
        );
    }
    assert!(!upgrader.index_namespace("t", false).await.unwrap());
}
