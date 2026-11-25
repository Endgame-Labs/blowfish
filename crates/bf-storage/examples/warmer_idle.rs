//! Cold-pool wave latency after an idle gap, through the real `Storage` and its connection warmer.
//!
//! Usage: `cargo run --release -p bf-storage --example warmer_idle -- s3://BUCKET/PREFIX REGION KEY GAPS [ROUNDS]`
//! KEY is an existing object (at least 64 MiB) relative to the prefix, and GAPS is a comma list of
//! idle seconds, for example `5,12,25,35,43`. ROUNDS (default 3) repeats the list. Each round idles for the gap and then issues one wave
//! of 84 random 16 KiB ranged GETs, printing the wave's wall time and the slowest GET. The first wave
//! raises the warmer's pool target; it is not reported. S3 closes idle connections after 20 to 40
//! seconds, so waves after a gap longer than the warmer's refresh interval find closed connections.
//! Reads only.
use bf_storage::{Storage, StorageConfig};
use futures::future::join_all;
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let config: StorageConfig = serde_json::from_value(serde_json::json!({
            "url": args[0], "region": args[1], "read_only": true,
        }))
        .unwrap();
        let store = Storage::open(&config).await.unwrap();
        let key = args[2].as_str();
        let size = store.head(key).await.unwrap().size;
        let gaps: Vec<u64> = args[3].split(',').map(|g| g.parse().unwrap()).collect();
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut wave = async |store: &Storage| {
            let reads = (0..84).map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let at = (seed % ((size - 16384) / 4096)) * 4096;
                let store = store.clone();
                async move {
                    let t = Instant::now();
                    store.get_range(key, at..at + 16384).await.unwrap();
                    t.elapsed().as_secs_f64() * 1e3
                }
            });
            let t = Instant::now();
            let each = join_all(reads).await;
            (
                t.elapsed().as_secs_f64() * 1e3,
                each.into_iter().fold(0.0, f64::max),
            )
        };
        wave(&store).await;
        let rounds: usize = args.get(4).map_or(3, |r| r.parse().unwrap());
        for round in 0..rounds {
            for &gap in &gaps {
                tokio::time::sleep(Duration::from_secs(gap)).await;
                let (total, slowest) = wave(&store).await;
                println!("round={round} gap_s={gap} wave_ms={total:.1} slowest_ms={slowest:.1}");
            }
        }
    });
}
