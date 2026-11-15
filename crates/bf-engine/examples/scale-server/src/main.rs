//! Benchmark entrypoint using the production HTTP router and explicit engine tuning.
//! Usage: blowfish-scale-server storage.json [127.0.0.1:8300]
//! Supply private storage configuration externally; this server disables authentication.
use bf_auth::KeyStore;
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use blowfish::{ServerOptions, prometheus_handle, router};
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("expected storage.json [127.0.0.1:8300]")?;
    let address = args.next().unwrap_or_else(|| "127.0.0.1:8300".into());
    let address: std::net::SocketAddr = address.parse()?;
    if !address.ip().is_loopback() {
        return Err("benchmark server requires a loopback address".into());
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let tuning = EngineConfig {
        recall_sample_rate: 0.0,
        ..EngineConfig::from_env()
    };
    eprintln!(
        "{}",
        serde_json::json!({"decoded_cache_bytes":tuning.decoded_cache_bytes,
            "ann_nprobe":tuning.ann_nprobe,"ann_rerank_factor":tuning.ann_rerank_factor})
    );
    let _ = prometheus_handle();
    let storage = Storage::open(&config).await?;
    // Match the normal binary's refresh interval, including its background LISTs.
    let keys = KeyStore::from_storage(storage.clone(), None, Duration::from_secs(5)).await?;
    let app = router(
        Engine::with_config(storage, tuning),
        keys,
        ServerOptions {
            auth_enabled: false,
            storage_url: config.url,
        },
    );
    axum::serve(tokio::net::TcpListener::bind(address).await?, app).await?;
    Ok(())
}
