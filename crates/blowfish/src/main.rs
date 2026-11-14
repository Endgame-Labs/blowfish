//! Standalone blowfish server.
use bf_auth::KeyStore;
use bf_engine::Engine;
use bf_storage::{Storage, StorageConfig};
use blowfish::{ServerOptions, metrics_router, router};
mod keys;
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};

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
    /// Manage API keys on a running server.
    Keys(keys::Keys),
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
        #[arg(long, env = "BLOWFISH_ADMIN_KEY", hide_env_values = true)]
        admin_key: Option<String>,
        #[arg(long, env = "BLOWFISH_READ_ONLY")]
        read_only: bool,
        #[arg(long, env = "BLOWFISH_REQUIRE_AUTH")]
        require_auth: bool,
        #[arg(long, env = "BLOWFISH_METRICS_LISTEN")]
        metrics_listen: Option<String>,
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
    require_auth: bool,
    metrics_listen: Option<String>,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "blowfish=info,bf_engine=info".into()),
        )
        .init();
    let command = Cli::parse().command;
    let command = match command {
        Command::Keys(keys) => return keys.run().await,
        command => command,
    };
    let Command::Serve {
        config,
        storage_url,
        storage_endpoint,
        storage_region,
        storage_profile,
        listen,
        admin_key,
        read_only,
        require_auth,
        metrics_listen,
        single_writer,
    } = command
    else {
        unreachable!()
    };
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
    let admin_key = admin_key.or(config.admin_key).filter(|s| !s.is_empty());
    if admin_key.is_none() {
        if require_auth || config.require_auth {
            return Err(
                "--require-auth needs a nonempty BLOWFISH_ADMIN_KEY or admin_key config".into(),
            );
        }
        tracing::warn!(
            "AUTHENTICATION IS DISABLED: all API, admin, and metrics routes are publicly accessible; development mode only"
        );
    }
    let storage = Storage::open(&config.storage).await?;
    let keys = KeyStore::from_storage(
        storage.clone(),
        admin_key.as_deref(),
        Duration::from_secs(5),
    )
    .await?;
    let listen = listen
        .or(config.listen)
        .unwrap_or_else(|| "0.0.0.0:8000".into());
    let app = router(
        Engine::new(storage.clone()),
        keys,
        ServerOptions {
            auth_enabled: admin_key.is_some(),
            storage_url: config.storage.url,
        },
    );
    let metrics_task = if let Some(address) = metrics_listen.or(config.metrics_listen) {
        let listener = tokio::net::TcpListener::bind(&address).await?;
        tracing::info!(listen = address, "unauthenticated metrics listener enabled");
        Some(tokio::spawn(async move {
            axum::serve(listener, metrics_router()).await
        }))
    } else {
        None
    };
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!(listen,mode=?storage.mode(),read_only=storage.is_read_only(),"blowfish is ready 🐡");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    if let Some(task) = metrics_task {
        task.abort();
    }
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
