//! Round C17: time local ANN staging from a recluster's manifest commit.
//!
//! Usage: c17_stage storage.json namespace workload.json scratch_dir out.json
//! The storage URL must use an owned `tmp/` prefix, `cache.disk_dir` must be under `.scratch/`,
//! and the workload is the `[{"request": ...}]` list `scale_query` reads. The process runs one
//! recluster, then queries until the tier's stage and its companion pack are on disk, and writes
//! the timings and every reply to `out.json`. `BF_PRESTAGE=0` is the control (the tier is
//! fetched back from object storage after the commit).
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use bf_types::QueryRequest;
use serde_json::{Value, json};
use std::{
    error::Error,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// (stages published, packs published) under a staging area.
fn published(disk: &Path) -> (usize, usize) {
    let (mut stages, mut packs) = (0, 0);
    let Ok(roots) = std::fs::read_dir(disk.join("ann-staging-v2")) else {
        return (0, 0);
    };
    for root in roots.flatten() {
        let Ok(dirs) = std::fs::read_dir(root.path()) else {
            continue;
        };
        for dir in dirs.flatten() {
            if dir.path().join("complete").exists() {
                stages += 1;
                packs += usize::from(dir.path().join("pack").exists());
            }
        }
    }
    (stages, packs)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 5 {
        return Err("expected storage.json namespace workload.json scratch_dir out.json".into());
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let prefix = config
        .url
        .strip_prefix("s3://")
        .and_then(|s| s.split_once('/'));
    if config.read_only || !prefix.is_some_and(|(_, p)| p.starts_with("tmp/") && !p.contains(".."))
    {
        return Err("needs writable S3 storage under an owned tmp/ prefix".into());
    }
    let disk: PathBuf = config
        .cache
        .disk_dir
        .clone()
        .ok_or("cache.disk_dir is required")?;
    if !disk.components().any(|c| c.as_os_str() == ".scratch") {
        return Err("cache.disk_dir must be beneath .scratch/".into());
    }
    let requests: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[2])?)?;
    let scratch = PathBuf::from(&args[3]);
    std::fs::create_dir_all(&scratch)?;
    let scratch = scratch.canonicalize()?;
    let engine = Engine::with_config(
        Storage::open(&config).await?,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..EngineConfig::from_env()
        },
    );
    let ns = args[1].clone();
    let started = Instant::now();
    let report = engine
        .recluster(
            &ns,
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 32,
                cluster_rows: 0,
                max_slab_rows: 250_000,
                scratch,
            },
        )
        .await?;
    let commit = Instant::now();
    let (before_stages, _) = published(&disk);
    // Queries run one at a time from the commit on; the first one starts staging.
    let query = |i: usize| {
        let (engine, ns) = (engine.clone(), ns.clone());
        let request = requests[i % requests.len()]["request"].clone();
        async move {
            let request: QueryRequest = serde_json::from_value(request)?;
            let tick = Instant::now();
            let response = engine.query(&ns, request).await?;
            Ok::<_, Box<dyn Error>>((tick.elapsed().as_secs_f64() * 1000.0, response))
        }
    };
    let (mut codes_ms, mut full_ms) = (None, None);
    let mut early = Vec::new();
    let mut i = 0;
    while full_ms.is_none() && commit.elapsed() < Duration::from_secs(900) {
        let poll = async {
            loop {
                let (stages, packs) = published(&disk);
                let now = commit.elapsed().as_secs_f64() * 1000.0;
                if codes_ms.is_none() && stages > before_stages {
                    codes_ms = Some(now);
                }
                if packs > 0 {
                    full_ms = Some(now);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        };
        let (ms, response) = tokio::select! {
            r = query(i) => r?,
            () = poll => break,
        };
        early.push(json!({"i": i % requests.len(), "ms": ms, "response": response}));
        i += 1;
        // Poll between queries too, so a slow query cannot hide a finished stage.
        let (stages, packs) = published(&disk);
        let now = commit.elapsed().as_secs_f64() * 1000.0;
        if codes_ms.is_none() && stages > before_stages {
            codes_ms = Some(now);
        }
        if packs > 0 {
            full_ms = Some(now);
        }
    }
    // Steady replies, with the stage fully local.
    let mut steady = Vec::new();
    for n in 0..requests.len().min(100) {
        let (ms, response) = query(n).await?;
        steady.push(json!({"i": n, "ms": ms, "response": response}));
    }
    let bytes: u64 = walk(&disk.join("ann-staging-v2"));
    std::fs::write(
        &args[4],
        serde_json::to_vec(&json!({
            "prestage": std::env::var("BF_PRESTAGE").unwrap_or_default() != "0",
            "recluster_seconds": report.seconds,
            "wall_seconds": started.elapsed().as_secs_f64(),
            "slabs": report.slabs,
            "codes_usable_ms": codes_ms,
            "fully_staged_ms": full_ms,
            "staged_bytes": bytes,
            "early": early,
            "steady": steady,
        }))?,
    )?;
    println!(
        "recluster {:.1}s codes {:?} ms full {:?} ms staged {} bytes",
        report.seconds, codes_ms, full_ms, bytes
    );
    Ok(())
}

fn walk(dir: &Path) -> u64 {
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries
            .flatten()
            .map(|e| match e.metadata() {
                Ok(m) if m.is_dir() => walk(&e.path()),
                Ok(m) => m.len(),
                Err(_) => 0,
            })
            .sum()
    })
}
