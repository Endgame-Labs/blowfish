//! Deterministic, offline PGO training through the production loopback HTTP server.
//!
//! Invoked by `scripts/pgo-build.sh`; no downloaded corpus or vendor service is used.
//! The trainer's own counters are discarded: only child server profiles are merged.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use serde_json::{Value, json};
use std::{error::Error, path::Path, process::Child, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(600))
        .build()?)
}
const DIMS: usize = 1024;
const ROWS: usize = 32_768;
const TERMS: [&str; 8] = [
    "history of the world",
    "united states president",
    "science and technology",
    "river city",
    "film music",
    "university school",
    "first world war",
    "national park",
];

fn random(seed: &mut u64) -> f32 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((*seed >> 32) as u32 as f64 / u32::MAX as f64 * 2.0 - 1.0) as f32
}
fn vector(id: usize) -> Vec<f32> {
    let mut center = (id % 256) as u64 + 123;
    let mut noise = id as u64 + 9_876_543;
    (0..DIMS)
        .map(|_| random(&mut center) + random(&mut noise) * 0.15)
        .collect()
}
fn row(id: usize) -> Value {
    json!({"id":id,"vector":vector(id),"category":id%8,
        "text":format!("{} {} passage {}", TERMS[id%8], TERMS[(id/8)%8], id)})
}
async fn fixture(root: &Path) -> Result<()> {
    let store = root.join("store");
    std::fs::create_dir(&store)?;
    let engine = Engine::with_config(
        Storage::open(&StorageConfig {
            url: url::Url::from_directory_path(&store)
                .map_err(|()| "invalid store path")?
                .into(),
            ..Default::default()
        })
        .await?,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            compaction_segments: 1024,
            ..Default::default()
        },
    );
    for (namespace, rows) in [("vectors", ROWS), ("small", 10_000)] {
        for start in (0..rows).step_by(2048) {
            engine.write(namespace, serde_json::from_value(json!({
                "distance_metric":"cosine_distance",
                "schema":{"vector":{"type":"[1024]f16"},
                    "category":{"type":"uint"},"text":{"type":"string","full_text_search":true}},
                "upsert_rows":(start..(start+2048).min(rows)).map(row).collect::<Vec<_>>()
            }))?).await?;
            engine.index_namespace(namespace, true).await?;
        }
    }
    let report = engine
        .recluster(
            "vectors",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 8,
                cluster_rows: 200,
                max_slab_rows: 8192,
                scratch: root.to_owned(),
            },
        )
        .await?;
    if report.rows != ROWS as u64 || report.clusters < 64 {
        return Err("training fixture did not build the expected IVF tier".into());
    }
    engine.shutdown(Duration::from_secs(5)).await;
    eprintln!(
        "PGO fixture: {} rows, {} slabs, {} leaves",
        report.rows, report.slabs, report.clusters
    );
    Ok(())
}

struct Server(Child);
impl Server {
    fn stop(&mut self) -> Result<()> {
        if self.0.try_wait()?.is_none() {
            // The child is owned by this process. SIGTERM flushes LLVM's counters.
            unsafe {
                libc::kill(self.0.id() as i32, libc::SIGTERM);
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.0.try_wait()? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("training server exited: {status}").into())
                };
            }
            if std::time::Instant::now() > deadline {
                self.0.kill()?;
                self.0.wait()?;
                return Err("training server failed to stop gracefully".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
async fn start(root: &Path, profile: &str, writable: bool) -> Result<(Server, String)> {
    // Reserve an available loopback port. Startup detects a competing binder as an error.
    let socket = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = socket.local_addr()?;
    let store = if writable {
        root.join("writes")
    } else {
        root.join("store")
    };
    std::fs::create_dir_all(&store)?;
    let log = std::fs::File::create(root.join(if writable {
        "write-server.log"
    } else {
        "server.log"
    }))?;
    let mut command = std::process::Command::new(std::env::var("BF_PGO_BINARY")?);
    command
        .env_clear()
        .args([
            "serve",
            "--listen",
            &address.to_string(),
            "--storage-url",
            url::Url::from_directory_path(store)
                .map_err(|()| "invalid store path")?
                .as_str(),
            "--indexing",
            "false",
            "--recall-sample-rate",
            "0",
            "--ann-nprobe",
            "48",
            "--ann-rerank-factor",
            "32",
            "--cache-memory-bytes",
            "1073741824",
            "--cache-disk-bytes",
            "0",
        ])
        .env("LLVM_PROFILE_FILE", profile)
        .env("TOKIO_WORKER_THREADS", "8")
        .env("RAYON_NUM_THREADS", "8")
        .env("BF_DECODED_CACHE_BYTES", "1073741824")
        .env("TMPDIR", root)
        .stdout(log.try_clone()?)
        .stderr(log);
    if !writable {
        command.arg("--read-only");
    }
    drop(socket);
    let mut server = Server(command.spawn()?);
    let base = format!("http://{address}");
    let client = client()?;
    for _ in 0..600 {
        if server.0.try_wait()?.is_some() {
            return Err("training server failed; inspect server.log".into());
        }
        if let Ok(response) = client.get(&base).send().await
            && response.status().is_success()
        {
            return Ok((server, base));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("training server readiness timeout".into())
}
fn query(i: usize, workload: &str) -> Value {
    if workload == "bm25" {
        return json!({"rank_by":["text","BM25",TERMS[i%8]],"top_k":10,"include_attributes":["id"]});
    }
    let mut body = json!({"rank_by":["vector","ANN",vector(ROWS+i%512)],"top_k":10,"include_attributes":["id"]});
    if workload == "filtered" {
        body["filters"] = json!(["category", "Eq", i % 8]);
    }
    body
}
async fn queries(base: &str, workload: &'static str, count: usize) -> Result<()> {
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..16 {
        let base = base.to_owned();
        workers.spawn(async move {
            let client = client()?;
            let namespace = if workload == "vector" {
                "vectors"
            } else {
                "small"
            };
            let bodies: Vec<_> = (0..512).map(|i| query(i, workload)).collect();
            for i in (worker..count).step_by(16) {
                let result: Value = client
                    .post(format!("{base}/v2/namespaces/{namespace}/query"))
                    .json(&bodies[i % 512])
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if result["rows"].as_array().is_none_or(|r| r.len() != 10)
                    || result["performance"]["exhaustive_search_count"] != 0
                {
                    return Err("training query missed indexed rows".into());
                }
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        });
    }
    while let Some(result) = workers.join_next().await {
        result??;
    }
    Ok(())
}
async fn train(root: &Path, profile: &str) -> Result<()> {
    fixture(root).await?;
    let (mut server, base) = start(root, profile, false).await?;
    queries(&base, "vector", 20_712).await?;
    let metrics = client()?
        .get(format!("{base}/metrics"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    if !metrics.lines().any(|line| {
        line.strip_prefix("blowfish_ann_compiled_queries_total ")
            .and_then(|n| n.parse::<u64>().ok())
            .is_some_and(|n| n > 0)
    }) {
        return Err("trainer did not exercise the compiled IVF scorer".into());
    }
    queries(&base, "filtered", 1024).await?;
    queries(&base, "bm25", 1024).await?;
    server.stop()?;
    let (mut server, base) = start(root, profile, true).await?;
    let client = client()?;
    for start in (0..512).step_by(16) {
        let result: Value = client.post(format!("{base}/v2/namespaces/writes"))
            .json(&json!({"distance_metric":"cosine_distance",
                "schema":{"vector":{"type":"[1024]f16"},"text":{"type":"string","full_text_search":true}},
                "upsert_rows":(start..start+16).map(row).collect::<Vec<_>>()}))
            .send().await?.error_for_status()?.json().await?;
        if result["rows_upserted"] != 16 {
            return Err("training write did not insert rows".into());
        }
    }
    server.stop()?;
    // Fresh processes exercise startup, disk decoding, and cache-cold query paths.
    for i in 0..8 {
        let (mut server, base) = start(root, profile, false).await?;
        queries(&base, "vector", 16 + i).await?;
        server.stop()?;
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("expected NEW_SCRATCH_DIRECTORY SERVER_PROFILE_PATTERN".into());
    }
    let root = Path::new(&args[1]);
    let parent = root
        .parent()
        .ok_or("missing scratch parent")?
        .canonicalize()?;
    if !parent.components().any(|c| c.as_os_str() == ".scratch") {
        return Err("trainer requires .scratch".into());
    }
    std::fs::create_dir(root)?;
    let root = root.canonicalize()?;
    let started = std::time::Instant::now();
    let result = train(&root, &args[2]).await;
    if result.is_err() {
        for name in ["server.log", "write-server.log"] {
            if let Ok(log) = std::fs::read_to_string(root.join(name)) {
                eprintln!("{name}:\n{log}");
            }
        }
    }
    std::fs::remove_dir_all(&root)?;
    result?;
    eprintln!(
        "PGO training completed in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixture_is_deterministic_and_queries_cover_the_wire_contract() {
        assert_eq!(vector(42), vector(42));
        assert_ne!(vector(42), vector(43));
        assert_eq!(vector(42).len(), DIMS);
        assert!(vector(42).iter().all(|v| v.is_finite()));
        assert_eq!(
            query(9, "filtered")["filters"],
            json!(["category", "Eq", 1])
        );
        assert_eq!(query(9, "bm25")["rank_by"][1], "BM25");
    }
}
