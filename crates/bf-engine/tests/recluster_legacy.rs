//! Rebuilding from legacy ANN vectors preserves the current builder's output, without a
//! prerequisite rewrite of source ANN objects, document columns, or the previous slab tier.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use bf_wal::{SegmentFile, Wal};
use serde_json::json;
use std::path::PathBuf;

fn vector(id: usize) -> Vec<f32> {
    (0..32)
        .map(|d| ((id * 7919 + d * 104729) as f64).sin() as f32)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_row_major_sources_and_current_sources_build_the_same_tier() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.scratch");
    std::fs::create_dir_all(&root).unwrap();
    let work = tempfile::Builder::new()
        .prefix("recluster-legacy-")
        .tempdir_in(root)
        .unwrap();
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let engine = Engine::with_config(
        store.clone(),
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    for batch in 0..2 {
        let rows: Vec<_> = (batch * 3000..(batch + 1) * 3000)
            .map(|id| json!({"id":id,"v":vector(id)}))
            .collect();
        engine
            .write(
                "modern",
                serde_json::from_value(json!({"distance_metric":"euclidean_squared",
            "schema":{"v":{"type":"[32]f16","ann":true}},"upsert_rows":rows}))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("modern", true).await.unwrap();
    }
    engine
        .write(
            "modern",
            serde_json::from_value(json!({"deletes":(0..6000).step_by(17).collect::<Vec<_>>()}))
                .unwrap(),
        )
        .await
        .unwrap();
    engine.index_namespace("modern", true).await.unwrap();
    let modern = Wal::new(store.clone(), "modern").unwrap();
    let (_, mut manifest) = modern.latest_manifest().await.unwrap().unwrap();
    let mut old_sources = Vec::new();
    for segment in &mut manifest.segments {
        let current = segment.files.remove("annv2:v").unwrap();
        let bytes = store.get(&current.key).await.unwrap();
        let legacy = bf_vector::downgrade_layout(&bytes).unwrap().unwrap();
        let metadata_len = bf_vector::metadata_len(&legacy).unwrap();
        let key = format!("ns/legacy/source/{}.bfv", segment.id);
        store.create_unique(&key, legacy.clone()).await.unwrap();
        segment.files.insert(
            "ann:v".into(),
            SegmentFile {
                key: key.clone(),
                size: legacy.len() as u64,
                metadata_len,
                stats_index: None,
                text_header: None,
            },
        );
        old_sources.push((key, legacy));
    }
    manifest.ann_dirs.clear();
    manifest.ann_tiers.clear();
    let legacy = Wal::new(store.clone(), "legacy").unwrap();
    legacy.publish_manifest(1, &manifest).await.unwrap();
    let mut reports = Vec::new();
    for ns in ["modern", "legacy"] {
        reports.push(
            engine
                .recluster(
                    ns,
                    &ReclusterOptions {
                        attribute: "v".into(),
                        cells: 4,
                        cluster_rows: 60,
                        max_slab_rows: 2000,
                        scratch: work.path().join(ns),
                    },
                )
                .await
                .unwrap_or_else(|error| panic!("{ns}: {error:?}")),
        );
    }
    assert_eq!(reports[0].rows, reports[1].rows);
    assert_eq!(reports[0].clusters, reports[1].clusters);
    assert_eq!(reports[0].slabs, reports[1].slabs);
    let (_, a) = modern.latest_manifest().await.unwrap().unwrap();
    let (_, b) = legacy.latest_manifest().await.unwrap().unwrap();
    let a = bf_vector::Directory::decode(&store.get(&a.ann_tiers["v"][0].dir.key).await.unwrap())
        .unwrap();
    let b = bf_vector::Directory::decode(&store.get(&b.ann_tiers["v"][0].dir.key).await.unwrap())
        .unwrap();
    for id in [1, 333, 1024, 4242, 5800] {
        let q = vector(id);
        assert_eq!(
            a.center_distances(&q).unwrap(),
            b.center_distances(&q).unwrap()
        );
        let request: bf_types::QueryRequest = serde_json::from_value(
            json!({"rank_by":["v","ANN",q],"top_k":10,"include_attributes":["id"]}),
        )
        .unwrap();
        let modern = engine.query("modern", request.clone()).await.unwrap();
        let legacy = engine.query("legacy", request).await.unwrap();
        assert_eq!(modern.result.rows, legacy.result.rows);
    }
    for (key, bytes) in old_sources {
        assert_eq!(store.get(&key).await.unwrap().as_ref(), bytes.as_slice());
    }
}
