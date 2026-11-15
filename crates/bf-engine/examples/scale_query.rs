//! Query-local backend measurements against an already ingested scale namespace.
//! Usage: scale_query storage.json workloads.json namespace concurrency [warmup]
//! Storage configuration and query vectors are supplied externally, never embedded.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::QueryRequest;
use futures::{StreamExt, TryStreamExt};
use serde_json::{Value, json};
use std::{error::Error, time::Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 5 {
        return Err("expected storage.json workloads.json namespace concurrency [warmup]".into());
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let requests: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[2])?)?;
    let concurrency: usize = args[4].parse()?;
    let store = Storage::open(&config).await?;
    let tuning = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        ..EngineConfig::from_env()
    };
    let engine = Engine::with_config(store, tuning.clone());
    if args.get(5).is_some_and(|s| s == "warmup") {
        for request in &requests {
            engine
                .query(
                    &args[3],
                    serde_json::from_value(request["request"].clone())?,
                )
                .await?;
        }
    }
    let start = Instant::now();
    let mut results = futures::stream::iter(requests.iter().enumerate().map(|(i, input)| {
        let engine = engine.clone();
        let ns = args[3].clone();
        async move {
            let request: QueryRequest = serde_json::from_value(input["request"].clone())?;
            let tick = Instant::now();
            let (response, diagnostic) = engine.query_with_diagnostics(&ns, request).await?;
            Ok::<_, Box<dyn Error>>(json!({
                "i":i,"kind":input["kind"],"query_id":input["query_id"],
                "ms":tick.elapsed().as_secs_f64()*1000.0,
                "snapshot_ms":diagnostic.snapshot_elapsed.as_secs_f64()*1000.0,
                "execution_ms":diagnostic.execution_elapsed.as_secs_f64()*1000.0,
                "backend":diagnostic.backend.iter().map(|b|json!({"stage":b.stage,
                    "operation":b.operation,"requests":b.requests,"bytes":b.bytes,
                    "ms":b.elapsed.as_secs_f64()*1000.0})).collect::<Vec<_>>(),
                "cache":{"memory_bytes":diagnostic.cache.memory_bytes,"disk_bytes":diagnostic.cache.disk_bytes,
                    "missed_bytes":diagnostic.cache.missed_bytes},
                "response":response
            }))
        }
    })).buffer_unordered(concurrency.max(1));
    let mut count = 0;
    while let Some(result) = results.try_next().await? {
        count += 1;
        println!("{result}");
    }
    eprintln!(
        "{}",
        json!({"queries":count,"seconds":start.elapsed().as_secs_f64(),
        "qps":count as f64/start.elapsed().as_secs_f64(),"concurrency":concurrency,
        "decoded_cache_bytes":tuning.decoded_cache_bytes,"ann_nprobe":tuning.ann_nprobe,
        "ann_rerank_factor":tuning.ann_rerank_factor})
    );
    Ok(())
}
