//! Build a namespace-wide ANN layout (see `recluster.rs`) for an ingested namespace.
//! Usage: recluster storage.json namespace attribute scratch_dir [cells] [cluster_rows] [max_slab_rows]
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let a: Vec<_> = std::env::args().collect();
    if a.len() < 5 {
        return Err("expected storage.json namespace attribute scratch_dir [cells] [cluster_rows] [max_slab_rows]".into());
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&a[1])?)?;
    let store = Storage::open(&config).await?;
    let engine = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.0,
            ..EngineConfig::from_env()
        },
    );
    let num = |i: usize, d: usize| a.get(i).and_then(|s| s.parse().ok()).unwrap_or(d);
    let report = engine
        .recluster(
            &a[2],
            &ReclusterOptions {
                attribute: a[3].clone(),
                cells: num(5, 32),
                cluster_rows: num(6, 200),
                max_slab_rows: num(7, 700_000),
                scratch: a[4].clone().into(),
            },
        )
        .await?;
    println!("{report:?}");
    Ok(())
}
