//! Opt-in disposable object-store benchmark; endpoints and credentials stay in the environment.
use bf_storage::{CacheConfig, Storage, StorageConfig};
use bytes::Bytes;
use std::{error::Error, time::Instant};
use uuid::Uuid;

fn summarize(name: &str, mut times: Vec<f64>) {
    times.sort_by(f64::total_cmp);
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    println!(
        "{name}: n={}, mean={mean:.3} ms, p50={:.3} ms, p95={:.3} ms",
        times.len(),
        times[times.len() / 2],
        times[(times.len() * 95 / 100).min(times.len() - 1)]
    );
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("BF_CACHE_BENCH_URL")?;
    let parsed = url::Url::parse(&url)?;
    if parsed.scheme() != "s3"
        || !parsed
            .path()
            .trim_end_matches('/')
            .ends_with("/dev/cache-bench")
    {
        return Err("BF_CACHE_BENCH_URL must be s3://<bucket>/dev/cache-bench".into());
    }
    let dir = tempfile::tempdir()?;
    let disk_only = std::env::var_os("BF_CACHE_BENCH_DISK_ONLY").is_some();
    let store = Storage::open(&StorageConfig {
        url: format!("{}/{}", url.trim_end_matches('/'), Uuid::new_v4()),
        endpoint: std::env::var("BF_CACHE_BENCH_ENDPOINT").ok(),
        profile: std::env::var("BF_CACHE_BENCH_PROFILE").ok(),
        region: Some(std::env::var("BF_CACHE_BENCH_REGION").unwrap_or_else(|_| "auto".into())),
        cache: CacheConfig {
            memory_bytes: if disk_only { 0 } else { 16 << 20 },
            disk_bytes: 16 << 20,
            disk_dir: Some(dir.path().to_owned()),
            ..Default::default()
        },
        ..Default::default()
    })
    .await?;
    let size = 1 << 20;
    let data = Bytes::from(
        (0..8 * size)
            .map(|i| (i * 37 % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let result: Result<(), Box<dyn Error>> = async {
        store.create("blocks", data.clone()).await?;
        // Length is known by an index reader; exclude metadata discovery from payload timings.
        let reader = store.range_reader_with_length("blocks", data.len() as u64, true)?;
        let mut cold = Vec::new();
        let mut warm = Vec::new();
        for pass in 0..6 {
            for index in 0..8 {
                let start = index * size;
                let now = Instant::now();
                let got = reader.read(start as u64..(start + size) as u64).await?;
                let elapsed = now.elapsed().as_secs_f64() * 1000.0;
                if got != data.slice(start..start + size) {
                    return Err("benchmark payload mismatch".into());
                }
                if pass == 0 {
                    cold.push(elapsed);
                } else {
                    warm.push(elapsed);
                }
            }
        }
        summarize("cold 1 MiB (object store, including cache fill)", cold);
        summarize(
            if disk_only {
                "warm 1 MiB (disk)"
            } else {
                "warm 1 MiB (memory)"
            },
            warm,
        );
        Ok(())
    }
    .await;
    // Clean even when a timed read fails. Each invocation owns a unique disposable prefix.
    let cleanup = store.delete("blocks").await;
    result?;
    cleanup?;
    assert!(store.list("").await?.is_empty());
    println!("Disposable benchmark objects cleaned up.");
    Ok(())
}
