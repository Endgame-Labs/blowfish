//! Reproduce filtered ANN recall, stage profiles, and warm CPU measurements on a frozen NQ fixture.
//! Usage: filtered_bench STORAGE_JSON MODE OUTPUT [WORKLOAD_JSON].
//! Modes: flush, recluster, quality, profile (prewarm, then trace), and warm.
//! Warm cycles the supplied queries through 2,000 prewarm, 1,000 serial, and 2,000 16-client calls.
//! perf stat FIFO control excludes startup and prewarm (BF_FILTER_BENCH_PERF_CONTROL and _ACK).
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
    if let Ok(path) = std::env::var("BF_FILTER_BENCH_PERF_CONTROL") {
        let ack = std::env::var("BF_FILTER_BENCH_PERF_ACK")?;
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
    if args.len() < 3
        || !matches!(
            args[1].as_str(),
            "flush" | "recluster" | "quality" | "profile" | "warm"
        )
    {
        return Err(
            "expected STORAGE_JSON {flush|recluster|quality|profile|warm} OUTPUT [WORKLOAD_JSON]"
                .into(),
        );
    }
    if matches!(args[1].as_str(), "quality" | "profile" | "warm") && args.len() < 4 {
        return Err("query modes require WORKLOAD_JSON".into());
    }
    let mut config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    if !matches!(args[1].as_str(), "flush" | "recluster") {
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
    if matches!(args[1].as_str(), "flush" | "recluster") {
        tuning.retention = Duration::ZERO;
    }
    tuning.recall_sample_rate = 0.0;
    if args[1] == "flush" {
        // Keep this fixture's existing segment geometry while checkpointing its final overlay.
        // A high row floor, unlimited byte cap, and unreachable group count disable compaction.
        tuning.max_segment_rows = u32::MAX as usize;
        tuning.max_segment_bytes = u64::MAX;
        tuning.compaction_segments = usize::MAX;
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
        "flush" => {
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
        "quality" | "profile" => {
            let requests: Vec<QueryRequest> = serde_json::from_slice(&std::fs::read(&args[3])?)?;
            if args[1] == "profile" {
                for request in &requests {
                    engine.query("nq", request.clone()).await?;
                }
                eprintln!("FILTER_PROFILE_BEGIN");
            }
            let mut rows = Vec::new();
            for request in requests {
                rows.push(engine.query("nq", request).await?.result.rows);
            }
            result["rows"] = json!(rows);
        }
        "warm" => {
            let requests: Vec<QueryRequest> = serde_json::from_slice(&std::fs::read(&args[3])?)?;
            if requests.is_empty() {
                return Err("empty workload".into());
            }
            for q in requests.iter().cycle().take(2000) {
                engine.query("nq", q.clone()).await?;
            }
            counters("enable")?;
            let mut times = Vec::new();
            let mut rows = Vec::new();
            let mut diagnostics = Diagnostics::default();
            for q in requests.iter().cycle().take(1000) {
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
                    for i in (client..2000).step_by(16) {
                        let q = requests[i % requests.len()].clone();
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
            result["qps16"] = json!(2000.0 / start.elapsed().as_secs_f64());
            counters("disable")?;
            result["concurrent_diagnostics"] = diagnostics.json(2000);
            result["counted_queries"] = json!(3000);
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
