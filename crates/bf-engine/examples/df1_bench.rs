//! Round DF1: time filtered deletes and patches on a local namespace with vectors.
//! Usage: df1_bench build STORE_DIR ROWS DIMS | run STORE_DIR CASE
//! CASE: delete:ATTR | patch:ATTR | mix (ATTR is b1000, b100, or b10: rows where ATTR == 0).
//! Prints one JSON line: wall time, VmHWM before and after (KiB), and bytes read (rchar).
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::WriteRequest;
use serde_json::json;
use std::time::Instant;

fn status(key: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}
fn rchar() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find(|l| l.starts_with("rchar"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}
fn req(v: serde_json::Value) -> WriteRequest {
    serde_json::from_value(v).unwrap()
}
#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let store = Storage::open(&StorageConfig {
        url: format!(
            "file://{}",
            std::fs::canonicalize(&args[2]).unwrap().display()
        ),
        ..Default::default()
    })
    .await
    .unwrap();
    let build = args[1] == "build";
    let engine = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    );
    if build {
        let rows: u64 = args[3].parse().unwrap();
        let dims: usize = args[4].parse().unwrap();
        let mut next = 0;
        while next < rows {
            let end = (next + 20_000).min(rows);
            let batch: Vec<_> = (next..end)
                .map(|id| {
                    let v: Vec<f32> = (0..dims)
                        .map(|d| ((id * 31 + d as u64 * 7) % 1000) as f32 / 1000.0)
                        .collect();
                    json!({"id": id, "vector": v, "b1000": id % 1000, "b100": id % 100,
                        "b10": id % 10, "n": 0, "text": format!("row {id}")})
                })
                .collect();
            engine
                .write(
                    "df1",
                    req(json!({"distance_metric": "cosine_distance", "upsert_rows": batch})),
                )
                .await
                .unwrap();
            next = end;
        }
        engine.index_namespace("df1", true).await.unwrap();
        // A small WAL tail on top of the segments.
        engine
            .write("df1", req(json!({"upsert_rows": [
                {"id": 0, "vector": vec![0.5f32; dims], "b1000": 0, "b100": 0, "b10": 0, "n": 0, "text": "tail"}]})))
            .await
            .unwrap();
        println!("built");
        engine.shutdown(std::time::Duration::from_secs(10)).await;
        return;
    }
    // Open the namespace (loads the manifest and tail) before the idle reading.
    let _ = engine
        .query(
            "df1",
            serde_json::from_value(
                json!({"rank_by":["id","asc"],"filters":["id","Eq",0],"top_k":1}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let case = args[3].as_str();
    let (idle, io0) = (status("VmHWM"), rchar());
    let t = Instant::now();
    let (kind, attr) = case.split_once(':').unwrap_or((case, "b1000"));
    let out = match kind {
        "delete" => req(json!({"delete_by_filter": [attr, "Eq", 0]})),
        "patch" => req(json!({"patch_by_filter": {"filters": [attr, "Eq", 0], "patch": {"n": 7}}})),
        "patchself" => {
            req(json!({"patch_by_filter": {"filters": [attr, "Eq", 0], "patch": {attr: 5}}}))
        }
        _ => panic!("case"),
    };
    let resp = engine.write("df1", out).await.unwrap();
    let wall = t.elapsed();
    println!(
        "{}",
        json!({"case": case, "wall_ms": wall.as_secs_f64() * 1000.0,
        "rss_idle_kib": idle, "rss_peak_kib": status("VmHWM"), "bytes_read": rchar() - io0,
        "rows_affected": resp.rows_affected})
    );
    engine.shutdown(std::time::Duration::from_secs(10)).await;
}
