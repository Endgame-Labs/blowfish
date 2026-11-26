//! Measurement harness for `Wal::tail_bounded` (Round W7).
//!
//! `tail_rss write URL NS COUNT BYTES` appends COUNT external payloads of about BYTES of
//! incompressible text. `tail_rss read URL NS MAX_BYTES REPS` replays the tail REPS times against
//! a fresh handle each time and prints the p50 latency and this process's peak RSS (`VmHWM`).
use bf_storage::{Storage, StorageConfig};
use bf_wal::{Wal, WalEntry, WriteBatch};

fn entry(seq: u64, bytes: usize) -> WalEntry {
    let mut state = 0x9e37_79b9_7f4a_7c15u64 ^ seq;
    let text: String = (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'a' + (state % 26) as u8) as char
        })
        .collect();
    let now = chrono::Utc::now();
    WalEntry::new(vec![WriteBatch {
        upserts: vec![serde_json::from_value(serde_json::json!({"id":seq,"text":text})).unwrap()],
        deletes: vec![],
        schema: Default::default(),
        distance_metric: Default::default(),
        created_at: now,
        updated_at: now,
    }])
}

fn hwm_mib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap_or(0);
    kb / 1024
}

async fn open(url: &str, ns: &str) -> Wal {
    let store = Storage::open(&StorageConfig {
        url: url.into(),
        ..Default::default()
    })
    .await
    .unwrap();
    Wal::new(store, ns).unwrap()
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a[1].as_str() {
        "write" => {
            let wal = open(&a[2], &a[3]).await;
            let (count, bytes): (u64, usize) = (a[4].parse().unwrap(), a[5].parse().unwrap());
            for seq in 1..=count {
                wal.append(seq, &entry(seq, bytes)).await.unwrap();
            }
        }
        "list" => {
            // LIST latency: `tail_rss list URL NS REPS` lists the WAL prefix REPS times.
            let reps: usize = a[4].parse().unwrap();
            let wal = open(&a[2], &a[3]).await;
            let mut ms = vec![];
            for _ in 0..reps {
                let t = std::time::Instant::now();
                wal.head().await.unwrap();
                ms.push(t.elapsed().as_micros());
            }
            ms.sort();
            println!("list_p50_us={} min_us={}", ms[ms.len() / 2], ms[0]);
        }
        _ => {
            let max: u64 = a[4].parse().unwrap();
            let reps: usize = a[5].parse().unwrap();
            let mut ms = vec![];
            let mut n = 0;
            for _ in 0..reps {
                let wal = open(&a[2], &a[3]).await;
                let t = std::time::Instant::now();
                n = wal.tail_bounded(0, max).await.unwrap().len();
                ms.push(t.elapsed().as_millis());
            }
            ms.sort();
            println!(
                "entries={n} p50_ms={} min_ms={} max_ms={} peak_rss_mib={}",
                ms[ms.len() / 2],
                ms[0],
                ms[ms.len() - 1],
                hwm_mib()
            );
        }
    }
}
