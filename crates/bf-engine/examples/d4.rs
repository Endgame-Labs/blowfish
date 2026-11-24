//! Paired D4 object-store measurements. All configuration and output paths are supplied by the caller.
//! Usage: d4 STORAGE_JSON MODE OUTPUT [WORKLOAD_JSON]; modes: index, recluster, gc, and measure.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use bf_types::QueryRequest;
use serde_json::json;
use std::{
    error::Error,
    path::PathBuf,
    time::{Duration, Instant},
};

fn percentiles(mut times: Vec<f64>) -> serde_json::Value {
    times.sort_by(f64::total_cmp);
    let nth = |fraction: f64| times[((times.len() - 1) as f64 * fraction).round() as usize];
    json!({"queries":times.len(),"p50_ms":nth(0.5),"p90_ms":nth(0.9),"qps":times.len() as f64/(times.iter().sum::<f64>()/1000.0)})
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let store = Storage::open(&config).await?;
    let mut tuning = EngineConfig::from_env();
    tuning.indexing = false;
    tuning.retention = Duration::ZERO;
    tuning.recall_sample_rate = 0.0;
    if args[1] == "index" {
        // Isolate the retirement publication from compaction in the paired snapshot.
        tuning.max_segment_bytes = 1;
    }
    let nprobe = tuning.ann_nprobe;
    let engine = Engine::with_config(store.clone(), tuning);
    let mut result = json!({"mode":args[1],"nprobe":nprobe,"manifest_format":bf_wal::MANIFEST_FORMAT_VERSION,"bucket_format":bf_storage::BUCKET_FORMAT_VERSION});
    let t0 = Instant::now();
    match args[1].as_str() {
        "index" => {
            for _ in 0..100 {
                if !engine.index_namespace("nq", true).await? {
                    break;
                }
            }
        }
        "recluster" => {
            let scratch = PathBuf::from(&args[2])
                .parent()
                .ok_or("output needs a parent directory")?
                .join("recluster");
            std::fs::create_dir_all(&scratch)?;
            let report = engine
                .recluster(
                    "nq",
                    &ReclusterOptions {
                        attribute: "vector".into(),
                        cells: 32,
                        cluster_rows: 200,
                        max_slab_rows: 100_000,
                        scratch: scratch.clone(),
                    },
                )
                .await?;
            result["rows"] = json!(report.rows);
            std::fs::remove_dir_all(scratch)?;
        }
        "gc" => {
            result["deleted"] = json!(engine.collect_garbage("nq").await?);
        }
        "measure" => {
            let requests: Vec<QueryRequest> = serde_json::from_slice(&std::fs::read(&args[3])?)?;
            if requests.is_empty() {
                return Err("empty workload".into());
            }
            let mut cold = Vec::new();
            for q in requests.iter().take(100) {
                engine.drop_caches().await;
                let start = Instant::now();
                engine.query("nq", q.clone()).await?;
                cold.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            result["cache_cold"] = percentiles(cold);
            for q in requests.iter().take(1000) {
                engine.query("nq", q.clone()).await?;
            }
            let mut times = Vec::new();
            let mut rows = Vec::new();
            for (i, q) in requests.iter().enumerate() {
                let start = Instant::now();
                let reply = engine.query("nq", q.clone()).await?;
                if i < 1000 {
                    times.push(start.elapsed().as_secs_f64() * 1000.0);
                }
                rows.push(reply.result.rows);
            }
            result["warm"] = percentiles(times);
            result["rows"] = json!(rows);
            let requests: Vec<_> = requests.into_iter().take(2000).collect();
            for q in &requests {
                engine.query("nq", q.clone()).await?;
            }
            let start = Instant::now();
            let mut tasks = tokio::task::JoinSet::new();
            for client in 0..16 {
                let (engine, requests) = (engine.clone(), requests.clone());
                tasks.spawn(async move {
                    for q in requests.into_iter().skip(client).step_by(16) {
                        engine.query("nq", q).await?;
                    }
                    Ok::<_, bf_types::ApiError>(())
                });
            }
            while let Some(done) = tasks.join_next().await {
                done??;
            }
            result["qps16"] = json!(requests.len() as f64 / start.elapsed().as_secs_f64());
        }
        _ => return Err("unknown mode".into()),
    }
    result["seconds"] = json!(t0.elapsed().as_secs_f64());
    let objects = store.list("ns/nq/").await?;
    result["object_bytes"] = json!(objects.iter().map(|o| o.size).sum::<u64>());
    result["objects"] = json!(objects.len());
    std::fs::write(&args[2], serde_json::to_vec(&result)?)?;
    eprintln!(
        "{}",
        json!({"mode":args[1],"seconds":t0.elapsed().as_secs_f64()})
    );
    Ok(())
}
