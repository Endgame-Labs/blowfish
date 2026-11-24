//! Paired cold-query gate over an existing, read-only NQ fixture.
//! Usage: cold_graph_gate storage.json NQ_DATA namespace count offset
//! Run baseline, candidate, candidate, and baseline with identical arguments and an owned prefix.
#[allow(dead_code)]
#[path = "../../bf-vector/examples/g3/data.rs"]
mod data;
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::QueryRequest;
use serde_json::json;
use std::{error::Error, path::Path, time::Instant};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 6 {
        return Err("usage: cold_graph_gate storage.json NQ_DATA namespace count offset".into());
    }
    let mut storage: StorageConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    storage.read_only = true;
    let e = Engine::with_config(
        Storage::open(&storage).await?,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            ..EngineConfig::from_env()
        },
    );
    let queries = data::Matrix::load(
        &Path::new(&args[2]).join("queries.npy"),
        "<f4",
        4,
        usize::MAX,
    )?
    .floats();
    let truth =
        data::Matrix::load(&Path::new(&args[2]).join("truth.npy"), "<i4", 4, usize::MAX)?.truth();
    let count = args[4].parse::<usize>()?;
    let offset = args[5].parse::<usize>()?;
    if count == 0 || offset.checked_add(count).is_none_or(|n| n > queries.len()) {
        return Err("invalid query slice".into());
    }
    // Establish the store's connections independently of the timed queries.
    let _ = e.metadata(&args[3]).await?;
    for i in offset..offset + count {
        e.drop_caches().await;
        let request: QueryRequest = serde_json::from_value(
            json!({"rank_by":["vector","ANN",queries[i]],"top_k":10,"include_attributes":false}),
        )?;
        let start = Instant::now();
        let (response, diagnostics) = e.query_with_diagnostics(&args[3], request).await?;
        let ms = start.elapsed().as_secs_f64() * 1000.;
        let rows = serde_json::to_value(response)?["rows"].clone();
        let overlap = rows
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| {
                r["id"]
                    .as_u64()
                    .is_some_and(|id| truth[i].contains(&(id as u32)))
            })
            .count();
        let backend = diagnostics
            .backend
            .iter()
            .map(
                |b| json!({"stage":b.stage,"op":b.operation,"requests":b.requests,"bytes":b.bytes}),
            )
            .collect::<Vec<_>>();
        println!(
            "{}",
            json!({"query":i,"ms":ms,"recall":overlap as f64/10.,"backend":backend,"rows":rows})
        );
    }
    e.shutdown(std::time::Duration::from_secs(5)).await;
    Ok(())
}
