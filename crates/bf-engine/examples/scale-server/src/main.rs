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
    blowfish::start_alloc_monitor();
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
    // Authentication is disabled here, so the key set is refreshed rarely.
    let keys = KeyStore::from_storage(storage.clone(), None, Duration::from_secs(300)).await?;
    let engine = Engine::with_config(storage, tuning);
    let dropper = engine.clone();
    let app = router(
        engine,
        keys,
        ServerOptions {
            auth_enabled: false,
            storage_url: config.url,
        },
    );
    // Bench-only (loopback, auth disabled): empty every cache on a warm process.
    let app = app.route(
        "/_bench/drop_caches",
        axum::routing::post(move || {
            let engine = dropper.clone();
            async move {
                engine.drop_caches().await;
                "ok"
            }
        }),
    );
    blowfish::reactors::serve(tokio::net::TcpListener::bind(address).await?, app, std::future::pending()).await?;
    Ok(())
}
