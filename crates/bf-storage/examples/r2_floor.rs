//! Object-store latency floor: single ranged GET distributions and the wall time of a wave of N
//! parallel ranged GETs, measured through the same `object_store` client blowfish uses.
//!
//! Usage: `cargo run --release -p bf-storage --example r2_floor -- CONFIG.json KEY [flags]`
//! CONFIG.json is `bench/scale/config.local` (`endpoint`, `region`, `profile`, `storage_url`).
//! KEY is the path of an existing large object, relative to the bucket (at least 64 MiB).
//! Flags: `--http2` (allow HTTP/2), `--pool N` (pool_max_idle_per_host), `--reps N`,
//! `--max-n N` (largest wave), `--sizes 4096,65536,...`, `--ns 1,8,32,...`, `--fresh` (a new
//! client per rep: includes DNS, TCP, and TLS setup, which a cold server also pays).
//! Reads only; writes nothing.
use futures::{StreamExt, stream};
use object_store::{ClientOptions, ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path};
use std::{sync::Arc, time::Instant};

fn credentials(profile: &str) -> (String, String) {
    let home = std::env::var("HOME").unwrap();
    let text = std::fs::read_to_string(format!("{home}/.aws/credentials")).unwrap();
    let (mut on, mut a, mut s) = (false, String::new(), String::new());
    for line in text.lines().map(str::trim) {
        if line.starts_with('[') {
            on = line == format!("[{profile}]");
        } else if on && let Some((k, v)) = line.split_once('=') {
            match k.trim() {
                "aws_access_key_id" => a = v.trim().into(),
                "aws_secret_access_key" => s = v.trim().into(),
                _ => {}
            }
        }
    }
    (a, s)
}
fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 * p) as usize).min(v.len() - 1)]
}
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |n: &str| {
        args.iter()
            .position(|a| a == n)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let has = |n: &str| args.iter().any(|a| a == n);
    let cfg: serde_json::Value = serde_json::from_slice(&std::fs::read(&args[0]).unwrap()).unwrap();
    let key = args[1].clone();
    let url = url::Url::parse(cfg["storage_url"].as_str().unwrap()).unwrap();
    let bucket = url.host_str().unwrap().to_string();
    let (ak, sk) = credentials(cfg["profile"].as_str().unwrap());
    let reps: usize = flag("--reps").map_or(7, |v| v.parse().unwrap());
    let list = |name: &str, default: &str| -> Vec<usize> {
        flag(name)
            .unwrap_or(default.into())
            .split(',')
            .map(|v| v.parse().unwrap())
            .collect()
    };
    let sizes = list("--sizes", "4096,65536,262144,1048576,4194304");
    let ns = list("--ns", "1,8,32,128,256,1024");
    let build = || -> Arc<dyn ObjectStore> {
        let mut o = ClientOptions::new();
        if has("--http2") {
            o = o.with_allow_http2();
        }
        if let Some(p) = flag("--pool") {
            o = o.with_config(object_store::ClientConfigKey::PoolMaxIdlePerHost, p);
        }
        Arc::new(
            AmazonS3Builder::new()
                .with_bucket_name(&bucket)
                .with_endpoint(cfg["endpoint"].as_str().unwrap())
                .with_region(cfg["region"].as_str().unwrap())
                .with_access_key_id(&ak)
                .with_secret_access_key(&sk)
                .with_client_options(o)
                .build()
                .unwrap(),
        )
    };
    let path = Path::from(key.as_str());
    let size = build().head(&path).await.unwrap().size;
    println!(
        "object {key}: {size} bytes; http2={} pool={:?} fresh={}",
        has("--http2"),
        flag("--pool"),
        has("--fresh")
    );
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let mut offset = move |len: usize| (rng.next() % (size - len as u64 - 1)).max(1);
    let fresh = has("--fresh");
    let shared = build();
    // Warm the shared pool with one request (TLS, DNS) so single-GET numbers exclude setup.
    shared.get_range(&path, 0..16).await.unwrap();

    println!("\n## single ranged GET (sequential, warm connection): ms");
    println!("| size | n | p50 | p90 | p99 | max |");
    println!("| ---: | ---: | ---: | ---: | ---: | ---: |");
    for &sz in &sizes {
        let mut v = vec![];
        for _ in 0..(reps * 10).max(40) {
            let o = offset(sz);
            let t = Instant::now();
            shared.get_range(&path, o..o + sz as u64).await.unwrap();
            v.push(t.elapsed().as_secs_f64() * 1e3);
        }
        let m = v.iter().cloned().fold(0., f64::max);
        println!(
            "| {sz} | {} | {:.0} | {:.0} | {:.0} | {:.0} |",
            v.len(),
            pct(&mut v.clone(), 0.5),
            pct(&mut v.clone(), 0.9),
            pct(&mut v, 0.99),
            m
        );
    }

    println!(
        "\n## wave of N parallel ranged GETs ({} reps), wall ms; per-request p50/p99 inside the wave",
        reps
    );
    println!("| size | N | wave p50 | wave max | req p50 | req p99 | MB/s (p50) |");
    println!("| ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for &sz in &sizes {
        for &n in &ns {
            if sz as u64 * n as u64 > 600 << 20 {
                continue;
            }
            let (mut walls, mut reqs) = (vec![], vec![]);
            for _ in 0..reps {
                let store = if fresh { build() } else { shared.clone() };
                let ranges: Vec<_> = (0..n).map(|_| offset(sz)).collect();
                let t = Instant::now();
                let times: Vec<f64> = stream::iter(ranges)
                    .map(|o| {
                        let (s, p) = (store.clone(), path.clone());
                        async move {
                            let t = Instant::now();
                            s.get_range(&p, o..o + sz as u64).await.unwrap();
                            t.elapsed().as_secs_f64() * 1e3
                        }
                    })
                    .buffer_unordered(n)
                    .collect()
                    .await;
                walls.push(t.elapsed().as_secs_f64() * 1e3);
                reqs.extend(times);
            }
            let mut w = walls.clone();
            let w50 = pct(&mut w, 0.5);
            let wmax = walls.iter().cloned().fold(0., f64::max);
            println!(
                "| {sz} | {n} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} |",
                w50,
                wmax,
                pct(&mut reqs.clone(), 0.5),
                pct(&mut reqs, 0.99),
                (sz * n) as f64 / 1e6 / (w50 / 1e3)
            );
        }
    }
}
