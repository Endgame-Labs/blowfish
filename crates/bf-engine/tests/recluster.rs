//! A recluster's slabs answer unfiltered ANN like the segments do, honor later deletes and tail
//! writes, and keep answering for their rows once new segments appear: one plan searches the tier
//! and the segments it does not cover, and compaction leaves covered segments alone.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
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
async fn write(e: &Engine, body: Value) {
    e.write("t", serde_json::from_value::<WriteRequest>(body).unwrap())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slabs_answer_like_segments_and_keep_answering_beside_new_segments() {
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
        write(
            &writer,
            json!({"distance_metric":"euclidean_squared","upsert_rows":docs}),
        )
        .await;
        writer.index_namespace("t", true).await.unwrap();
    }
    let queries = [5usize, 1234, 4321, 8000];
    let mut before = Vec::new();
    for q in queries {
        before.push(ids(&engine(&store), q).await);
    }
    let scratch = std::env::temp_dir().join(format!("bf-recluster-{}", std::process::id()));
    let report = writer
        .recluster(
            "t",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 60,
                max_slab_rows: 5000,
                scratch: scratch.clone(),
            },
        )
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&scratch);
    assert_eq!(report.rows, 9000);
    let wal = Wal::new(store.clone(), "t").unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(manifest.ann_tiers.contains_key("vector"));
    for (q, want) in queries.iter().zip(&before) {
        let got = ids(&engine(&store), *q).await;
        assert_eq!(got[0], json!(q), "the query's own row is first");
        let overlap = got.iter().filter(|g| want.contains(g)).count();
        assert!(
            overlap >= 8,
            "slab results overlap the segments' ({overlap}/10)"
        );
    }
    // A delete after the build is honored through the slabs.
    write(&writer, json!({"deletes":[1234]})).await;
    writer.index_namespace("t", true).await.unwrap();
    let after = ids(&engine(&store), 1234).await;
    assert!(!after.contains(&json!(1234)));
    // New segments after the build stay outside the tier, which keeps answering for its rows:
    // one plan searches both, so new rows and old ones rank together.
    let covered: Vec<String> = manifest.ann_tiers["vector"][0]
        .covered
        .iter()
        .map(|c| c.segment.clone())
        .collect();
    for round in 0..6usize {
        let docs: Vec<_> = (0..100)
            .map(|n| 9000 + round * 100 + n)
            .map(|n| json!({"id":n,"vector":vector(n)}))
            .collect();
        write(&writer, json!({"upsert_rows":docs})).await;
        writer.index_namespace("t", true).await.unwrap();
    }
    // An update moves a covered row into a new segment: only the new version answers.
    write(
        &writer,
        json!({"upsert_rows":[{"id":5,"vector":vector(9999)}]}),
    )
    .await;
    writer.index_namespace("t", true).await.unwrap();
    // Forced passes compact the new segments together, never the covered ones.
    for _ in 0..4 {
        writer.index_namespace("t", true).await.unwrap();
    }
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    let tier = &manifest.ann_tiers["vector"][0];
    assert_eq!(
        tier.covered
            .iter()
            .map(|c| c.segment.clone())
            .collect::<Vec<_>>(),
        covered
    );
    for id in &covered {
        assert!(manifest.segments.iter().any(|s| &s.id == id));
    }
    assert!(
        manifest.segments.len() < covered.len() + 6,
        "the uncovered segments were compacted"
    );
    let fresh = engine(&store);
    assert_eq!(ids(&fresh, 9050).await[0], json!(9050));
    assert_eq!(ids(&fresh, 9550).await[0], json!(9550));
    assert_eq!(ids(&fresh, 4321).await[0], json!(4321));
    assert_eq!(ids(&fresh, 9999).await[0], json!(5));
    assert!(!ids(&fresh, 5).await.contains(&json!(5)));
    assert!(!ids(&fresh, 1234).await.contains(&json!(1234)));
    // A covered segment whose rows are all deleted stays listed, masked, while the tier lives.
    let first: Vec<usize> = (0..3000).filter(|n| *n != 5 && *n != 1234).collect();
    write(&writer, json!({"deletes": first})).await;
    writer.index_namespace("t", true).await.unwrap();
    let (_, manifest) = wal.latest_manifest().await.unwrap().unwrap();
    assert!(manifest.segments.iter().any(|s| s.id == covered[0]));
    assert_eq!(manifest.ann_tiers["vector"][0].covered.len(), 3);
    let fresh = engine(&store);
    assert_eq!(ids(&fresh, 4321).await[0], json!(4321));
    assert!(
        ids(&fresh, 100)
            .await
            .iter()
            .all(|id| id.as_u64().unwrap() >= 3000)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slab_winners_shadowed_by_tail_match_an_exhaustive_reference() {
    let open = || async {
        Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap()
    };
    let store = open().await;
    let writer = engine(&store);
    let reference = engine(&open().await);
    let vector = |id: usize| {
        let mut vector = vec![0.0f32; 64];
        vector[0] = 1.0;
        vector[1] = id as f32 / 100.0;
        vector
    };
    let initial = json!({"distance_metric":"euclidean_squared",
        "upsert_rows":(0..512).map(|id|json!({"id":id,"version":0,"vector":vector(id)})).collect::<Vec<_>>()});
    write(&writer, initial.clone()).await;
    write(&reference, initial).await;
    writer.index_namespace("t", true).await.unwrap();
    let scratch = tempfile::tempdir().unwrap();
    writer
        .recluster(
            "t",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 64,
                max_slab_rows: 512,
                scratch: scratch.path().into(),
            },
        )
        .await
        .unwrap();
    for hidden in [1, 31, 96] {
        let update = json!({"deletes":(0..hidden).filter(|id|id%2==0).collect::<Vec<_>>(),
            "upsert_rows":(0..hidden).filter(|id|id%2==1)
                .map(|id|json!({"id":id,"version":hidden,"vector":vector(1000+id)})).collect::<Vec<_>>()});
        write(&writer, update.clone()).await;
        write(&reference, update).await;
        let request: QueryRequest = serde_json::from_value(json!({
            "rank_by":["vector","ANN",vector(0)],"top_k":10,"include_attributes":["version"]
        }))
        .unwrap();
        assert_eq!(
            writer
                .query("t", request.clone())
                .await
                .unwrap()
                .result
                .rows,
            reference.query("t", request).await.unwrap().result.rows,
            "{hidden} nearest slab rows shadowed by replacements and deletes"
        );
    }
}
