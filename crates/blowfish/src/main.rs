//! Standalone blowfish server.
use bf_engine::Engine;
use bf_storage::{Storage, StorageConfig};
use blowfish::{AdminKeyAuthorizer, router};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};

#[derive(Parser)]
#[command(
    version,
    about = "Self-hosted, object-store-backed search with the turbopuffer API"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Serve the turbopuffer-compatible HTTP API.
    Serve {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, env = "BLOWFISH_STORAGE_URL")]
        storage_url: Option<String>,
        #[arg(long, env = "BLOWFISH_STORAGE_ENDPOINT")]
        storage_endpoint: Option<String>,
        #[arg(long, env = "BLOWFISH_STORAGE_REGION")]
        storage_region: Option<String>,
        #[arg(long, env = "BLOWFISH_STORAGE_PROFILE")]
        storage_profile: Option<String>,
        #[arg(long, env = "BLOWFISH_LISTEN")]
        listen: Option<String>,
        #[arg(long, env = "BLOWFISH_ADMIN_KEY")]
        admin_key: Option<String>,
        #[arg(long, env = "BLOWFISH_READ_ONLY")]
        read_only: bool,
        #[arg(long, env = "BLOWFISH_SINGLE_WRITER")]
        single_writer: bool,
    },
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    storage: StorageConfig,
    storage_url: Option<String>,
    listen: Option<String>,
    admin_key: Option<String>,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "blowfish=info,bf_engine=info".into()),
        )
        .init();
    let Command::Serve {
        config,
        storage_url,
        storage_endpoint,
        storage_region,
        storage_profile,
        listen,
        admin_key,
        read_only,
        single_writer,
    } = Cli::parse().command;
    let mut config: Config = if let Some(path) = config {
        toml::from_str(&std::fs::read_to_string(path)?)?
    } else {
        Config::default()
    };
    if let Some(url) = storage_url.or(config.storage_url) {
        config.storage.url = url;
    }
    if let Some(endpoint) = storage_endpoint {
        config.storage.endpoint = Some(endpoint);
    }
    if let Some(region) = storage_region {
        config.storage.region = Some(region);
    }
    if let Some(profile) = storage_profile {
        config.storage.profile = Some(profile);
    }
    config.storage.read_only |= read_only;
    config.storage.single_writer |= single_writer;
    let storage = Storage::open(&config.storage).await?;
    let listen = listen
        .or(config.listen)
        .unwrap_or_else(|| "0.0.0.0:8000".into());
    let app = router(
        Engine::new(storage.clone()),
        Arc::new(AdminKeyAuthorizer::new(admin_key.or(config.admin_key))),
    );
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!(listen,mode=?storage.mode(),read_only=storage.is_read_only(),"blowfish is ready 🐡");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}
async fn shutdown() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    {
        let terminate = async {
            if let Ok(mut signal) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                signal.recv().await;
            }
        };
        tokio::select! {_ = ctrl_c=>{},_ = terminate=>{}}
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}
