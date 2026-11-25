//! Prepare an untiered, frozen NQ checkpoint for the RR6 paired experiment.
//! Usage: prepare_hot_graph_nq storage.json DATA_DIR namespace SCRATCH_DIR
#[allow(dead_code)]
#[path = "../../bf-vector/examples/g3/data.rs"]
mod data;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::WriteRequest;
use rayon::prelude::*;
use serde_json::json;
use std::{error::Error, path::Path, time::Instant};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 5 {
        return Err(
            "usage: prepare_hot_graph_nq storage.json DATA_DIR namespace SCRATCH_DIR".into(),
        );
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    if !config.url.starts_with("file://") || !config.url.contains("/.scratch/") {
        return Err("RR6 preparation requires an owned file store beneath .scratch".into());
    }
    let store = Storage::open(&config).await?;
    let engine = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            retention: std::time::Duration::from_secs(5),
            ..EngineConfig::from_env()
        },
    );
    let start = Instant::now();
    {
        let matrix = data::Matrix::load(
            &Path::new(&args[2]).join("corpus.f16"),
            "<f2",
            2,
            usize::MAX,
        )?;
        let width = matrix.cols * 2;
        for first in (0..matrix.rows).step_by(1000) {
            let end = (first + 1000).min(matrix.rows);
            let encoded: Vec<_> = matrix.bytes[first * width..end * width]
                .par_chunks_exact(width)
                .map(|row| {
                    let mut values = Vec::with_capacity(matrix.cols * 4);
                    for b in row.as_chunks::<2>().0 {
                        values.extend_from_slice(
                            &half::f16::from_bits(u16::from_le_bytes(*b))
                                .to_f32()
                                .to_le_bytes(),
                        );
                    }
                    STANDARD.encode(values)
                })
                .collect();
            let mut body =
                json!({"upsert_columns":{"id":(first..end).collect::<Vec<_>>(),"vector":encoded}});
            if first == 0 {
                body["distance_metric"] = json!("cosine_distance");
                body["schema"] =
                    json!({"vector":{"type":format!("[{}]f16",matrix.cols),"ann":true}});
            }
            engine
                .write(&args[3], serde_json::from_value::<WriteRequest>(body)?)
                .await?;
            let ack_start = Instant::now();
            if end == matrix.rows {
                eprintln!("RR6_FINAL_ACK elapsed={:.6}", start.elapsed().as_secs_f64());
            }
            if end % 50_000 == 0 || end == matrix.rows {
                engine.index_namespace(&args[3], true).await?;
                if end == matrix.rows {
                    eprintln!("RR6_TAIL_SECONDS {:.6}", ack_start.elapsed().as_secs_f64());
                }
                eprintln!(
                    "prepared rows={end} seconds={:.1}",
                    start.elapsed().as_secs_f64()
                );
            }
        }
    }
    eprintln!(
        "RR6_SOURCE_READY seconds={:.3}",
        start.elapsed().as_secs_f64()
    );
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    engine.collect_garbage(&args[3]).await?;
    engine.shutdown(std::time::Duration::from_secs(5)).await;
    Ok(())
}
