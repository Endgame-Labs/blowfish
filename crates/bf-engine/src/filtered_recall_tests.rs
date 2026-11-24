//! Selective scalar masks keep ANN recall without changing the vector format.
use crate::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::Document;
use bf_wal::Wal;
use serde_json::{Value, json};
use std::time::Duration;

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
fn bucket(id: u64) -> u64 {
    mix(id ^ 0xa0761d6478bd642f) % 100
}
fn vector(id: u64) -> Vec<f32> {
    (0u64..64)
        .map(|d| {
            let n = mix(id ^ (d + 1).wrapping_mul(0xe7037ed1a0b428db));
            ((n >> 40) as f32 / 8_388_608.0) - 1.0
        })
        .collect()
}
async fn query(engine: &Engine, body: Value) -> Vec<Document> {
    Box::pin(engine.query("recall", serde_json::from_value(body).unwrap()))
        .await
        .unwrap()
        .result
        .rows
        .unwrap()
}
fn overlap(actual: &[Document], exact: &[Document]) -> usize {
    actual
        .iter()
        .filter(|row| exact.iter().any(|other| other["id"] == row["id"]))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selective_distributed_masks_preserve_recall_and_format_four() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        retention: Duration::ZERO,
        max_segment_rows: 5000,
        compaction_segments: 1000,
        ann_nprobe: 12,
        ann_nprobe_explicit: true,
        ann_rerank_factor: 64,
        exact_fetch_rows: 0,
        decoded_cache_bytes: 128 << 20,
        ..Default::default()
    };
    let engine = Engine::with_config(store.clone(), config.clone());
    for start in (0..40_000).step_by(5000) {
        let rows: Vec<_> = (start..start + 5000)
            .map(|id| {
                let v = vector(id);
                json!({"id":id,"bucket":bucket(id),"vector":v})
            })
            .collect();
        Box::pin(
            engine.write(
                "recall",
                serde_json::from_value(json!({
                    "distance_metric":"cosine_distance",
                    "schema":{"vector":{"type":"[64]f16"}},
                    "upsert_rows":rows
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        Box::pin(engine.index_namespace("recall", true))
            .await
            .unwrap();
    }
    let (_, manifest) = Wal::new(store.clone(), "recall")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.format_version, 4);
    assert_eq!(manifest.segments.len(), 8);
    assert!(manifest.ann_tiers.is_empty());
    assert!(
        manifest.segments.iter().all(|s| {
            s.files.contains_key("annv2:vector") && !s.files.contains_key("vector_rows")
        })
    );
    let capped = Engine::with_config(
        store.clone(),
        EngineConfig {
            legacy_three_filter_test: true,
            ..config
        },
    );
    let mut selective = (0usize, 0usize, 0usize);
    for attribute in ["vector"] {
        for percent in [1, 10] {
            let mut found = 0;
            for i in 0..8 {
                let q = vector(137 + i * 4999);
                let body = json!({"rank_by":[attribute,"ANN",q],"filters":["bucket","Lt",percent],"top_k":5,"include_attributes":false});
                let mut exact = body.clone();
                exact["rank_by"][1] = json!("kNN");
                let expected = query(&engine, exact).await;
                let actual = query(&engine, body.clone()).await;
                assert_eq!(actual.len(), 5);
                assert!(actual.iter().all(|row| {
                    let id = row["id"].as_u64().unwrap();
                    bucket(id) < percent
                }));
                found += overlap(&actual, &expected);
                if attribute == "vector" && percent == 1 {
                    selective.0 += overlap(&actual, &expected);
                    selective.1 += overlap(&query(&capped, body).await, &expected);
                    selective.2 += expected.len();
                }
            }
            assert!(
                found >= 39,
                "attribute={attribute}, percent={percent}, found={found}/40"
            );
        }
    }
    assert!(
        selective.0 > selective.1,
        "uncapped={}, capped={}, total={}",
        selective.0,
        selective.1,
        selective.2
    );
    assert!(
        query(
            &engine,
            json!({"rank_by":["vector","ANN",vector(1)],"filters":["bucket","Lt",0],"top_k":5})
        )
        .await
        .is_empty()
    );
}
