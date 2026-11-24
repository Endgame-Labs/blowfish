//! Paired D4 object-store measurements. All configuration and output paths are supplied by the caller.
//! Usage: d4 STORAGE_JSON MODE OUTPUT [WORKLOAD_JSON] [QUERY_INDEX].
//! Modes: index, recluster, gc, measure, short (64 queries), first (one fresh-process query),
//! warm (2,000-query prewarm, then serial and concurrent queries), and quality (rows only).
use bf_engine::{Engine, EngineConfig, QueryDiagnostics, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use bf_types::QueryRequest;
use serde_json::json;
use std::io::{BufRead, Write};
use std::{
    collections::BTreeMap,
    error::Error,
    path::PathBuf,
    time::{Duration, Instant},
};

fn percentiles(mut times: Vec<f64>) -> serde_json::Value {
    times.sort_by(f64::total_cmp);
    let nth = |fraction: f64| times[((times.len() - 1) as f64 * fraction).round() as usize];
    json!({"queries":times.len(),"p50_ms":nth(0.5),"p90_ms":nth(0.9),"qps":times.len() as f64/(times.iter().sum::<f64>()/1000.0)})
}
#[derive(Default)]
struct Diagnostics {
    snapshot: Duration,
    execution: Duration,
    backend: BTreeMap<String, (u64, u64, Duration)>,
    ann_rows: u64,
}
impl Diagnostics {
    fn add(&mut self, d: QueryDiagnostics) {
        self.snapshot += d.snapshot_elapsed;
        self.execution += d.execution_elapsed;
        self.ann_rows += d.ann_rows_scanned;
        for b in d.backend {
            let total = self
                .backend
                .entry(format!("{}:{}", b.stage, b.operation))
                .or_default();
            total.0 += b.requests;
            total.1 += b.bytes;
            total.2 += b.elapsed;
        }
    }
    fn merge(&mut self, other: Self) {
        self.snapshot += other.snapshot;
        self.execution += other.execution;
        self.ann_rows += other.ann_rows;
        for (key, (requests, bytes, elapsed)) in other.backend {
            let total = self.backend.entry(key).or_default();
            total.0 += requests;
            total.1 += bytes;
            total.2 += elapsed;
        }
    }
    fn json(self, queries: usize) -> serde_json::Value {
        json!({
            "snapshot_mean_ms":self.snapshot.as_secs_f64()*1000.0/queries as f64,
            "execution_mean_ms":self.execution.as_secs_f64()*1000.0/queries as f64,
            "ann_rows_per_query":self.ann_rows as f64/queries as f64,
            "backend":self.backend.into_iter().map(|(key,(requests,bytes,elapsed))|
                json!({"stage_operation":key,"requests":requests,"bytes":bytes,
                    "elapsed_ms":elapsed.as_secs_f64()*1000.0})).collect::<Vec<_>>(),
        })
    }
}
// perf stat --delay=-1 --control=fifo:CONTROL,ACK can exclude startup and warm-up.
// An acknowledgement ensures counters change before crossing the phase boundary.
fn counters(command: &str) -> Result<(), Box<dyn Error>> {
    if let Ok(path) = std::env::var("BF_D4_PERF_CONTROL") {
        let ack = std::env::var("BF_D4_PERF_ACK")?;
        let mut control = std::fs::OpenOptions::new().write(true).open(path)?;
        let mut ack = std::io::BufReader::new(std::fs::File::open(ack)?);
        writeln!(control, "{command}")?;
        control.flush()?;
        let mut line = String::new();
        if ack.read_line(&mut line)? == 0 {
            return Err("perf control closed before acknowledging the phase".into());
        }
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let mut config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    if !matches!(args[1].as_str(), "index" | "recluster" | "gc") {
        // Reader controls must never advance the baseline bucket marker.
        config.read_only = true;
    }
    let store = Storage::open(&config).await?;
    let mut tuning = EngineConfig::from_env();
    // The engine's positive-threshold parser ignores zero; this benchmark also needs an
    // explicit zero-retention query profile, independently of maintenance and immediate GC.
    if let Ok(seconds) = std::env::var("BF_RETENTION_SECS") {
        tuning.retention = Duration::from_secs(seconds.parse()?);
    }
    tuning.indexing = false;
    if matches!(args[1].as_str(), "index" | "recluster" | "gc") {
        tuning.retention = Duration::ZERO;
    }
    tuning.recall_sample_rate = 0.0;
    if args[1] == "index" {
        // Isolate the retirement publication from compaction in the paired snapshot.
        tuning.max_segment_bytes = 1;
    }
    let nprobe = tuning.ann_nprobe;
    let retention = tuning.retention.as_secs_f64();
    let decoded = tuning.decoded_cache_bytes;
    let engine = Engine::with_config(store.clone(), tuning);
    let mut result = json!({
        "mode":args[1], "nprobe":nprobe, "storage_read_only":config.read_only, "retention_secs":retention,
        "decoded_cache_bytes":decoded, "block_cache_bytes":config.cache.memory_bytes,
        "disk_cache_bytes":config.cache.disk_bytes,
        "manifest_format":bf_wal::MANIFEST_FORMAT_VERSION,
        "bucket_format":bf_storage::BUCKET_FORMAT_VERSION,
    });
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
        "quality" => {
            let requests: Vec<QueryRequest> = serde_json::from_slice(&std::fs::read(&args[3])?)?;
            let mut rows = Vec::new();
            for request in requests {
                rows.push(engine.query("nq", request).await?.result.rows);
            }
            result["rows"] = json!(rows);
        }
        "first" => {
            let requests: Vec<QueryRequest> = serde_json::from_slice(&std::fs::read(&args[3])?)?;
            let i: usize = args.get(4).map_or(Ok(0), |i| i.parse())?;
            let start = Instant::now();
            let answer = engine.query("nq", requests[i].clone()).await?;
            result["first_ms"] = json!(start.elapsed().as_secs_f64() * 1000.0);
            result["rows"] = json!(answer.result.rows);
        }
        "warm" => {
            let mut requests: Vec<QueryRequest> =
                serde_json::from_slice(&std::fs::read(&args[3])?)?;
            requests.truncate(2000);
            if requests.is_empty() {
                return Err("empty workload".into());
            }
            for q in &requests {
                engine.query("nq", q.clone()).await?;
            }
            counters("enable")?;
            let mut times = Vec::new();
            let mut rows = Vec::new();
            let mut diagnostics = Diagnostics::default();
            for q in requests.iter().take(1000) {
                let start = Instant::now();
                let (reply, d) = engine.query_with_diagnostics("nq", q.clone()).await?;
                diagnostics.add(d);
                times.push(start.elapsed().as_secs_f64() * 1000.0);
                rows.push(reply.result.rows);
            }
            result["warm_diagnostics"] = diagnostics.json(times.len());
            result["warm"] = percentiles(times);
            result["rows"] = json!(rows);
            let start = Instant::now();
            let mut tasks = tokio::task::JoinSet::new();
            for client in 0..16 {
                let (engine, requests) = (engine.clone(), requests.clone());
                tasks.spawn(async move {
                    let mut diagnostics = Diagnostics::default();
                    for q in requests.into_iter().skip(client).step_by(16) {
                        let (_, d) = engine.query_with_diagnostics("nq", q).await?;
                        diagnostics.add(d);
                    }
                    Ok::<_, bf_types::ApiError>(diagnostics)
                });
            }
            let mut diagnostics = Diagnostics::default();
            while let Some(done) = tasks.join_next().await {
                diagnostics.merge(done??);
            }
            result["qps16"] = json!(requests.len() as f64 / start.elapsed().as_secs_f64());
            counters("disable")?;
            result["concurrent_diagnostics"] = diagnostics.json(requests.len());
            result["counted_queries"] = json!(requests.len().min(1000) + requests.len());
        }
        "measure" | "short" => {
            let mut requests: Vec<QueryRequest> =
                serde_json::from_slice(&std::fs::read(&args[3])?)?;
            let short = args[1] == "short";
            if short {
                requests.truncate(64);
            }
            if requests.is_empty() {
                return Err("empty workload".into());
            }
            let mut cold = Vec::new();
            for q in requests.iter().take(if short { 20 } else { 100 }) {
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
            if requests.len() > 1000 {
                for q in &requests {
                    engine.query("nq", q.clone()).await?;
                }
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
