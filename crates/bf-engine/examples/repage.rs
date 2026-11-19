//! Write a paged copy of a namespace's slab directory (no slab is rebuilt).
//! Usage: repage storage.json namespace attribute
use bf_alloc as _;
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let a: Vec<_> = std::env::args().collect();
    if a.len() < 4 {
        return Err("expected storage.json namespace attribute".into());
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
    println!("{:?}", engine.page_slab_directory(&a[2], &a[3]).await?);
    Ok(())
}
