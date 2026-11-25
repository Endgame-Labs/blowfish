//! Warm vector and BM25 micro-benchmark at 1 and 16 clients, for paired A/B builds.
//! Usage: t7_ab DIR [ROWS] [QUERIES_PER_ARM]. Prints one JSON line per arm.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::json;
use std::{error::Error, sync::Arc, time::Instant};

fn vector(id: usize) -> Vec<f32> {
    let mut s = id as u64 * 2654435761 + 17;
    (0..64)
        .map(|_| {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((s >> 33) as f32 / (1u64 << 31) as f32) - 0.5
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let dir = &args[1];
    let rows: usize = args.get(2).map_or(20_000, |v| v.parse().unwrap());
    let per_arm: usize = args.get(3).map_or(400, |v| v.parse().unwrap());
    let storage = Storage::open(&StorageConfig {
        url: format!("file://{dir}"),
        ..Default::default()
    })
    .await?;
    let config = EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        ..Default::default()
    };
    let engine = Engine::with_config(storage, config);
    for first in (0..rows).step_by(5000) {
        let docs: Vec<_> = (first..(first + 5000).min(rows))
            .map(|id| json!({"id":id,"vector":vector(id),"text":format!("term{} term{} search engine",id%512,(id*17+7)%512)}))
            .collect();
        let request = if first == 0 {
            json!({"distance_metric":"cosine_distance","schema":{"text":{"type":"string","full_text_search":true},"vector":{"type":"[64]f32","ann":true}},"upsert_rows":docs})
        } else {
            json!({"upsert_rows":docs})
        };
        engine.write("t7", serde_json::from_value(request)?).await?;
        engine.index_namespace("t7", true).await?;
    }
    while engine.index_namespace("t7", true).await? {}
    let engine = Arc::new(engine);
    let request = |kind: &str, i: usize| {
        let body = if kind == "vector" {
            json!({"rank_by":["vector","ANN",vector(i * 7919 % rows)],"top_k":10})
        } else {
            json!({"rank_by":["text","BM25",format!("term{} term{}", i % 512, (i * 3) % 512)],"top_k":10})
        };
        serde_json::from_value::<bf_types::QueryRequest>(body).unwrap()
    };
    for kind in ["vector", "bm25"] {
        for clients in [1usize, 16] {
            for i in 0..200 {
                engine.query("t7", request(kind, i)).await?;
            }
            let start = Instant::now();
            let mut tasks = vec![];
            for c in 0..clients {
                let (engine, kind) = (engine.clone(), kind.to_string());
                let reqs: Vec<_> = (0..per_arm / clients)
                    .map(|i| request(&kind, i * clients + c))
                    .collect();
                tasks.push(tokio::spawn(async move {
                    let mut ms = vec![];
                    for r in reqs {
                        let t = Instant::now();
                        engine.query("t7", r).await.unwrap();
                        ms.push(t.elapsed().as_secs_f64() * 1000.0);
                    }
                    ms
                }));
            }
            let mut ms = vec![];
            for t in tasks {
                ms.extend(t.await?);
            }
            let wall = start.elapsed().as_secs_f64();
            ms.sort_by(f64::total_cmp);
            let q = |f: f64| ms[((ms.len() - 1) as f64 * f).round() as usize];
            println!(
                "{}",
                json!({"kind":kind,"clients":clients,"n":ms.len(),"p50_ms":q(0.5),"p90_ms":q(0.9),"qps":ms.len() as f64/wall})
            );
        }
    }
    Ok(())
}
