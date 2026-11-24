//! Resident slab batches preserve source row mappings and tied ANN winners.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use bf_wal::Wal;
use serde_json::{Value, json};

async fn write(e: &Engine, body: Value) {
    e.write("t", serde_json::from_value::<WriteRequest>(body).unwrap())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resident_batches_preserve_rows_and_tied_winners() {
    let mut config = StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    };
    config.cache.memory_bytes = 64 << 20;
    let store = Storage::open(&config).await.unwrap();
    let tuning = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.,
        // Keep the normal partial-index reader instead of a compiled tier.
        decoded_cache_bytes: 0,
        ann_nprobe: 512,
        ann_nprobe_explicit: true,
        ann_rerank_factor: 256,
        ..Default::default()
    };
    let writer = Engine::with_config(store.clone(), tuning.clone());
    let vector = |n: usize| -> Vec<f32> {
        (0..64)
            .map(|d| if (n >> (d % 9)) & 1 == 0 { -1. } else { 1. })
            .collect()
    };
    // Repeated, exactly representable vectors make the nearest tied IDs unambiguous.
    let docs: Vec<_> = (0..100_000)
        .map(|n| json!({"id":n,"vector":vector(n % 509)}))
        .collect();
    write(
        &writer,
        json!({"distance_metric":"euclidean_squared",
            "schema":{"vector":{"type":"[64]f16","ann":true}},
            "upsert_rows":docs}),
    )
    .await;
    writer.index_namespace("t", true).await.unwrap();
    let scratch = tempfile::tempdir().unwrap();
    writer
        .recluster(
            "t",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 1,
                cluster_rows: 200,
                max_slab_rows: 250_000,
                scratch: scratch.path().into(),
            },
        )
        .await
        .unwrap();
    let (_, manifest) = Wal::new(store.clone(), "t")
        .unwrap()
        .latest_manifest()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.ann_tiers["vector"][0].slabs.len(), 1);
    let reader = Engine::with_config(store, tuning);
    let ids = |value: Value| {
        value["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>()
    };
    for pattern in [0, 17, 251, 508] {
        let request: QueryRequest = serde_json::from_value(json!({
            "rank_by":["vector","ANN",vector(pattern)],
            "top_k":10,"include_attributes":false
        }))
        .unwrap();
        reader.query("t", request.clone()).await.unwrap();
        let (answer, diagnostic) = reader
            .query_with_diagnostics("t", request.clone())
            .await
            .unwrap();
        assert_eq!(diagnostic.ann_rows_scanned, 100_000);
        assert_eq!(diagnostic.document_scan_rows, 0);
        let exhaustive = reader.query_exhaustive("t", request).await.unwrap();
        let expected: Vec<_> = (0..10).map(|n| json!(pattern + 509 * n)).collect();
        assert_eq!(ids(serde_json::to_value(answer).unwrap()), expected);
        assert_eq!(ids(serde_json::to_value(exhaustive).unwrap()), expected);
    }
}
