//! Build a disposable 10K-row, two-attribute namespace for query-wrapper measurements.
//! Usage: server_tax_fixture STORE ROWS_F32 DIMENSIONS
//! ROWS_F32 contains at least 10,000 headerless little-endian f32 vectors.
use bf_engine::{Engine, EngineConfig, ReclusterOptions};
use bf_storage::{Storage, StorageConfig};
use serde_json::json;
use std::{error::Error, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("expected STORE ROWS_F32 DIMENSIONS".into());
    }
    let root = PathBuf::from(&args[1]).canonicalize()?;
    if !root.components().any(|part| part.as_os_str() == ".scratch") {
        return Err("fixture store must be under .scratch".into());
    }
    let dims: usize = args[3].parse()?;
    let bytes = std::fs::read(&args[2])?;
    let required = dims.checked_mul(40_000).ok_or("dimensions overflow")?;
    if dims == 0 || bytes.len() < required {
        return Err("expected at least 10,000 vectors".into());
    }
    let store = Storage::open(&StorageConfig {
        url: format!("file://{}", root.display()),
        ..Default::default()
    })
    .await?;
    let engine = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            compaction_segments: 1024,
            ..Default::default()
        },
    );
    for batch in 0..5 {
        let docs: Vec<_> = (batch * 2000..(batch + 1) * 2000)
            .map(|id| {
                let vector: Vec<_> = bytes[id * dims * 4..(id + 1) * dims * 4]
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|value| f32::from_le_bytes(*value))
                    .collect();
                json!({"id":id,"vector":vector,"category":id%8,"label":format!("group-{}",id%32)})
            })
            .collect();
        engine.write("small", serde_json::from_value(json!({
            "distance_metric":"cosine_distance", "schema":{"vector":{"type":format!("[{dims}]f16")}},
            "upsert_rows":docs,
        }))?).await?;
        engine.index_namespace("small", true).await?;
    }
    let scratch = tempfile::Builder::new()
        .prefix("small-build-")
        .tempdir_in(root.parent().ok_or("missing scratch parent")?)?;
    let report = engine
        .recluster(
            "small",
            &ReclusterOptions {
                attribute: "vector".into(),
                cells: 4,
                cluster_rows: 100,
                max_slab_rows: 10_000,
                scratch: scratch.path().to_owned(),
            },
        )
        .await?;
    println!("Built {} rows with category and label", report.rows);
    Ok(())
}
