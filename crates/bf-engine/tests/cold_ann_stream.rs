//! The cold streaming path also reads current ANN objects, where blobs and codes share an
//! object, and maps independently scanned batches back to their document segments.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{CacheConfig, Storage};
use object_store::{memory::InMemory, path::Path};
use serde_json::json;
use std::sync::Arc;

fn vector(id: usize) -> Vec<f32> {
    let mut state = id as u64 + 1;
    (0..16)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / (1u32 << 24) as f32) - 0.5
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_batches_preserve_warm_results_projection_and_live_masks() {
    let backend = Arc::new(InMemory::new());
    let store = Storage::from_backend_with_cache(
        backend.clone(),
        Path::default(),
        false,
        false,
        CacheConfig {
            memory_bytes: 32 << 20,
            block_size: 1024,
            max_coalesce_blocks: 256,
            fetch_concurrency: 32,
            hedge_budget: 0.0,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let engine = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            decoded_cache_bytes: 16 << 20,
            ..Default::default()
        },
    );
    for batch in 0..10 {
        let rows: Vec<_> = (batch * 10_000..(batch + 1) * 10_000)
            .map(|id| json!({"id":id,"vector":vector(id),"title":format!("row {id}")}))
            .collect();
        engine
            .write(
                "stream",
                serde_json::from_value(json!({
                    "distance_metric":"euclidean_squared", "upsert_rows":rows
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        engine.index_namespace("stream", true).await.unwrap();
    }
    let scratch = tempfile::tempdir().unwrap();
    let built = engine
        .recluster(
            "stream",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 128,
                max_slab_rows: 30_000,
                scratch: scratch.path().join("build"),
            },
        )
        .await
        .unwrap();
    assert_eq!(built.rows, 100_000);
    let uncached_store = Storage::from_backend_with_cache(
        backend,
        Path::default(),
        true,
        false,
        CacheConfig {
            memory_bytes: 0,
            block_size: 1024,
            max_coalesce_blocks: 256,
            fetch_concurrency: 32,
            hedge_budget: 0.0,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let uncached = Engine::with_config(
        uncached_store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            decoded_cache_bytes: 16 << 20,
            ..Default::default()
        },
    );
    for id in [0, 107, 1007, 49_001, 99_999] {
        engine.drop_caches().await;
        let request = json!({
            "rank_by":["vector","ANN",vector(id)], "top_k":10,
            "include_attributes":["title"]
        });
        let cold = engine
            .query("stream", serde_json::from_value(request.clone()).unwrap())
            .await
            .unwrap();
        let warm = engine
            .query("stream", serde_json::from_value(request.clone()).unwrap())
            .await
            .unwrap();
        assert_eq!(cold.result.rows, warm.result.rows);
        // A zero-capacity reader cannot retain speculative seed reads. Its per-slab path
        // must return the same IDs, distances, and projections as the cached streaming path.
        uncached.drop_caches().await;
        let no_cache = uncached
            .query("stream", serde_json::from_value(request).unwrap())
            .await
            .unwrap();
        assert_eq!(cold.result.rows, no_cache.result.rows);
        let rows = cold.result.rows.unwrap();
        assert_eq!(rows[0]["id"], json!(id));
        for row in rows {
            assert_eq!(row["title"], format!("row {}", row["id"]));
            assert!(row.get("vector").is_none());
        }
    }
    engine
        .write(
            "stream",
            serde_json::from_value(json!({"deletes":[1007]})).unwrap(),
        )
        .await
        .unwrap();
    engine.index_namespace("stream", true).await.unwrap();
    engine.drop_caches().await;
    let response = engine
        .query(
            "stream",
            serde_json::from_value(json!({"rank_by":["vector","ANN",vector(1007)],"top_k":10}))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        response
            .result
            .rows
            .unwrap()
            .iter()
            .all(|r| r["id"] != 1007)
    );
}
