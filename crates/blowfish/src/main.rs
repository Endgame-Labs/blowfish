//! Standalone blowfish server.
use bf_auth::KeyStore;
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use blowfish::{ServerOptions, metrics_router, prometheus_handle, router};
mod crash;
mod fdlimit;
mod keys;
use clap::{Args, Parser, Subcommand};
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
    /// Upgrade a namespace's ANN and document formats now. A running
    /// server does this in the background by itself. Safe beside a running server.
    Reindex {
        /// Namespace to upgrade (or use --all).
        namespace: Option<String>,
        /// Upgrade every namespace in the bucket.
        #[arg(long, conflicts_with = "namespace")]
        all: bool,
        /// Report ANN layouts and legacy document counts and bytes, and write nothing.
        #[arg(long)]
        dry_run: bool,
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
    },
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
        #[command(flatten)]
        tuning: Box<TuningArgs>,
    },
}
#[derive(Args, Default, Debug)]
struct TuningArgs {
    #[arg(long, env = "BLOWFISH_INDEXING")]
    indexing: Option<bool>,
    #[arg(long, env = "BLOWFISH_INDEX_ROWS")]
    index_rows: Option<usize>,
    #[arg(long, env = "BLOWFISH_INDEX_BYTES")]
    index_bytes: Option<u64>,
    #[arg(long, env = "BLOWFISH_INDEX_AGE_MS")]
    index_age_ms: Option<u64>,
    #[arg(long, env = "BLOWFISH_INDEX_INTERVAL_MS")]
    index_interval_ms: Option<u64>,
    #[arg(long, env = "BLOWFISH_COMPACTION_SEGMENTS")]
    compaction_segments: Option<usize>,
    #[arg(long, env = "BLOWFISH_RETENTION_MS")]
    retention_ms: Option<u64>,
    #[arg(long, env = "BLOWFISH_LEASE_DURATION_MS")]
    lease_duration_ms: Option<u64>,
    #[arg(long, env = "BLOWFISH_MAX_UNINDEXED_BYTES")]
    max_unindexed_bytes: Option<u64>,
    #[arg(long, env = "BLOWFISH_EVENTUAL_TAIL_BYTES")]
    eventual_tail_bytes: Option<u64>,
    #[arg(long, env = "BLOWFISH_ANN_NPROBE")]
    ann_nprobe: Option<usize>,
    #[arg(long, env = "BLOWFISH_ANN_RERANK_FACTOR")]
    ann_rerank_factor: Option<usize>,
    #[arg(long, env = "BLOWFISH_RECALL_SAMPLE_RATE")]
    recall_sample_rate: Option<f64>,
    #[arg(long, env = "BLOWFISH_CACHE_MEMORY_BYTES")]
    cache_memory_bytes: Option<u64>,
    #[arg(long, env = "BLOWFISH_CACHE_DISK_BYTES")]
    cache_disk_bytes: Option<u64>,
    #[arg(long, env = "BLOWFISH_CACHE_DISK_DIR")]
    cache_disk_dir: Option<PathBuf>,
    #[arg(long, env = "BLOWFISH_CACHE_NAMESPACE")]
    cache_namespace: Option<String>,
    #[arg(long, env = "BLOWFISH_CACHE_BLOCK_SIZE")]
    cache_block_size: Option<usize>,
    #[arg(long, env = "BLOWFISH_CACHE_MAX_COALESCE_BLOCKS")]
    cache_max_coalesce_blocks: Option<usize>,
    #[arg(long, env = "BLOWFISH_CACHE_COALESCE_GAP_BLOCKS")]
    cache_coalesce_gap_blocks: Option<usize>,
    #[arg(long, env = "BLOWFISH_CACHE_FETCH_CONCURRENCY")]
    cache_fetch_concurrency: Option<usize>,
    #[arg(long, env = "BLOWFISH_CACHE_PREFETCH_CONCURRENCY")]
    cache_prefetch_concurrency: Option<usize>,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct EngineFile {
    indexing: Option<bool>,
    index_rows: Option<usize>,
    index_bytes: Option<u64>,
    index_age_ms: Option<u64>,
    index_interval_ms: Option<u64>,
    compaction_segments: Option<usize>,
    retention_ms: Option<u64>,
    lease_duration_ms: Option<u64>,
    max_unindexed_bytes: Option<u64>,
    eventual_tail_bytes: Option<u64>,
    ann_nprobe: Option<usize>,
    ann_rerank_factor: Option<usize>,
    recall_sample_rate: Option<f64>,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    engine: EngineFile,
    storage: StorageConfig,
    storage_url: Option<String>,
    listen: Option<String>,
    admin_key: Option<String>,
    require_auth: bool,
    metrics_listen: Option<String>,
    /// Embedding and reranking models, keyed by the model name clients use.
    models: bf_models::ModelsConfig,
}
/// After SIGTERM or Ctrl-C: how long requests in flight and background jobs get to finish.
/// systemd's default stop timeout is 90 s; this stays well inside it.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    // Dropping a runtime waits for every blocking task, such as a build step still on the CPU.
    // Background work is crash-safe wherever it stops, so it gets a moment, and then the process
    // exits without it.
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    crash::install();
    blowfish::start_alloc_monitor();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "blowfish=info,bf_engine=info".into()),
        )
        .init();
    match fdlimit::raise() {
        Some((before, after)) if after > before => {
            tracing::info!(before, after, "raised the open-file limit")
        }
        Some(_) => {}
        None => tracing::warn!("could not raise the open-file limit"),
    }
    let command = Cli::parse().command;
    let command = match command {
        Command::Keys(keys) => return keys.run().await,
        command => command,
    };
    if let Command::Reindex {
        namespace,
        all,
        dry_run,
        config,
        storage_url,
        storage_endpoint,
        storage_region,
        storage_profile,
    } = command
    {
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
        // A survey must not raise the bucket fence and disable an older writer.
        if dry_run {
            config.storage.read_only = true;
        }
        let storage = Storage::open(&config.storage).await?;
        let engine = Engine::with_config(
            storage,
            EngineConfig {
                indexing: false,
                recall_sample_rate: 0.0,
                ..EngineConfig::from_env()
            },
        );
        let opts = bf_engine::ReindexOptions {
            dry_run,
            ..Default::default()
        };
        let names = match (namespace, all) {
            (Some(ns), false) => vec![ns],
            (None, true) => engine.namespace_names().await?,
            _ => return Err("give a namespace, or --all".into()),
        };
        for ns in names {
            match engine.reindex(&ns, &opts).await {
                Ok(r) => {
                    let line = |name: &str, c: &bf_engine::LayoutCounts| {
                        println!(
                            "{ns} {name}: legacy row-major {} ({} bytes), legacy fast-scan {} ({} bytes), legacy other {} ({} bytes), single-object {} ({} bytes)",
                            c.row_major.0,
                            c.row_major.1,
                            c.fast_scan.0,
                            c.fast_scan.1,
                            c.other.0,
                            c.other.1,
                            c.single_object.0,
                            c.single_object.1
                        )
                    };
                    line("segments", &r.segments);
                    line("slabs", &r.slabs);
                    println!(
                        "{ns} documents: legacy {} ({} bytes)",
                        r.documents.0, r.documents.1
                    );
                    if !dry_run {
                        println!(
                            "{ns}: rewrote {} segment indexes, {} slabs, and {} document/ID pairs ({} bytes written, {} superseded) in {} publications, {:.1}s",
                            r.converted_segments,
                            r.converted_slabs,
                            r.converted_documents,
                            r.bytes_written,
                            r.superseded,
                            r.publications,
                            r.seconds
                        );
                    }
                }
                Err(e) => eprintln!("{ns}: skipped: {e:?}"),
            }
        }
        return Ok(());
    }
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
        tuning,
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
    // Environment overrides first, so every `BF_*` engine knob works under `blowfish serve`; CLI flags and TOML keys
    // applied below take precedence over them.
    let mut engine_config = EngineConfig::from_env();
    if let Some(value) = tuning.indexing.or(config.engine.indexing) {
        engine_config.indexing = value;
    }
    if let Some(value) = tuning
        .index_rows
        .or_else(|| {
            std::env::var("BF_INDEX_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|v| *v > 0)
        })
        .or(config.engine.index_rows)
    {
        engine_config.index_rows = value;
    }
    if let Some(value) = tuning.index_bytes.or(config.engine.index_bytes) {
        engine_config.index_bytes = value;
    }
    if let Some(value) = tuning
        .index_age_ms
        .or_else(|| {
            std::env::var("BF_INDEX_AGE_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|v| *v > 0)
        })
        .or(config.engine.index_age_ms)
    {
        engine_config.index_age = Duration::from_millis(value);
    }
    if let Some(value) = tuning
        .index_interval_ms
        .or_else(|| {
            std::env::var("BF_INDEX_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|v| *v > 0)
        })
        .or(config.engine.index_interval_ms)
    {
        engine_config.index_interval = Duration::from_millis(value);
    }
    if let Some(value) = tuning
        .compaction_segments
        .or(config.engine.compaction_segments)
    {
        engine_config.compaction_segments = value;
    }
    if let Some(value) = tuning.retention_ms.or(config.engine.retention_ms) {
        engine_config.retention = Duration::from_millis(value);
    }
    if let Some(value) = tuning.lease_duration_ms.or(config.engine.lease_duration_ms) {
        engine_config.lease_duration = Duration::from_millis(value);
    }
    if let Some(value) = tuning
        .max_unindexed_bytes
        .or_else(|| {
            std::env::var("BF_MAX_UNINDEXED_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|v| *v > 0)
        })
        .or(config.engine.max_unindexed_bytes)
    {
        engine_config.max_unindexed_bytes = value;
        // A bulk flush follows the limit (three quarters of it) unless set on its own.
        if std::env::var_os("BF_BULK_FLUSH_BYTES").is_none() {
            engine_config.bulk_flush_bytes = value / 4 * 3;
        }
    }
    if let Some(value) = tuning
        .eventual_tail_bytes
        .or(config.engine.eventual_tail_bytes)
    {
        engine_config.eventual_tail_bytes = value;
    }
    if let Some(value) = tuning
        .ann_nprobe
        .or_else(|| {
            std::env::var("BF_ANN_NPROBE")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|v| *v > 0)
        })
        .or(config.engine.ann_nprobe)
    {
        engine_config.ann_nprobe = value;
        engine_config.ann_nprobe_explicit = true;
    }
    if let Some(value) = tuning.ann_rerank_factor.or(config.engine.ann_rerank_factor) {
        engine_config.ann_rerank_factor = value;
    }
    if let Some(value) = tuning
        .recall_sample_rate
        .or(config.engine.recall_sample_rate)
    {
        engine_config.recall_sample_rate = value;
    }
    if let Some(value) = tuning.cache_memory_bytes {
        config.storage.cache.memory_bytes = value;
    }
    if let Some(value) = tuning.cache_disk_bytes {
        config.storage.cache.disk_bytes = value;
    }
    if let Some(value) = tuning.cache_disk_dir {
        config.storage.cache.disk_dir = Some(value);
    }
    if let Some(value) = tuning.cache_namespace {
        config.storage.cache.namespace = Some(value);
    }
    if let Some(value) = tuning.cache_block_size {
        config.storage.cache.block_size = value;
    }
    if let Some(value) = tuning.cache_coalesce_gap_blocks {
        config.storage.cache.coalesce_gap_blocks = value;
    }
    if let Some(value) = tuning.cache_max_coalesce_blocks {
        config.storage.cache.max_coalesce_blocks = value;
    }
    if let Some(value) = tuning.cache_fetch_concurrency {
        config.storage.cache.fetch_concurrency = value;
    }
    if let Some(value) = tuning.cache_prefetch_concurrency {
        config.storage.cache.prefetch_concurrency = value;
    }
    if let Ok(json) = std::env::var("BLOWFISH_MODELS_JSON")
        && !json.trim().is_empty()
    {
        bf_models::apply_env(&mut config.models, &json)?;
    }
    if !config.models.is_empty() {
        let models = bf_models::Models::new(std::mem::take(&mut config.models))?;
        let (embedding, rerank) = models.names();
        tracing::info!(
            ?embedding,
            ?rerank,
            "native embedding and reranking models configured"
        );
        engine_config.models = Some(models);
    }
    if !(0.0..=1.0).contains(&engine_config.recall_sample_rate) {
        return Err("recall_sample_rate must be between 0 and 1".into());
    }
    let _ = prometheus_handle();
    let storage = Storage::open(&config.storage).await?;
    // With authentication disabled no request consults the key set, so refresh it rarely
    // instead of LISTing the bucket every few seconds.
    let key_refresh = if admin_key.is_some() {
        Duration::from_secs(5)
    } else {
        Duration::from_secs(300)
    };
    let keys = KeyStore::from_storage(storage.clone(), admin_key.as_deref(), key_refresh).await?;
    let listen = listen
        .or(config.listen)
        .unwrap_or_else(|| "0.0.0.0:8000".into());
    let engine = Engine::with_config(storage.clone(), engine_config);
    let app = router(
        engine.clone(),
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
    // On the signal, the server stops accepting connections and background jobs (indexing,
    // compaction, GC, upgrades, and recluster builds) begin stopping at once. Requests in
    // flight and the jobs share one grace period; whatever is left after it is dropped, which is
    // crash-safe (an interrupted build's record stays, and GC reclaims its objects later).
    let signalled = std::sync::Arc::new(tokio::sync::Notify::new());
    let server = axum::serve(listener, app).with_graceful_shutdown({
        let (signalled, engine) = (signalled.clone(), engine.clone());
        async move {
            shutdown().await;
            tracing::info!("shutting down");
            signalled.notify_one();
            engine.begin_shutdown();
        }
    });
    let mut server = std::pin::pin!(server.into_future());
    let deadline = tokio::select! {
        served = &mut server => {
            served?;
            tokio::time::Instant::now() + SHUTDOWN_GRACE
        }
        () = signalled.notified() => {
            let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
            if tokio::time::timeout_at(deadline, &mut server).await.is_err() {
                tracing::warn!("requests still in flight at shutdown were dropped");
            }
            deadline
        }
    };
    if let Some(task) = metrics_task {
        task.abort();
    }
    let stopped = engine
        .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
        .await;
    tracing::info!(background_jobs_stopped = stopped, "stopped");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_configuration_parses() {
        let config: Config =
            toml::from_str(include_str!("../../../blowfish.example.toml")).unwrap();
        assert_eq!(config.engine.index_rows, Some(1000));
        assert_eq!(config.storage.cache.block_size, 1 << 20);
    }

    #[test]
    fn example_models_parse_when_uncommented() {
        let example = include_str!("../../../blowfish.example.toml");
        let tail = &example[example.find("# [models.").unwrap()..];
        let uncommented: String = tail
            .lines()
            .map(|l| l.strip_prefix("# ").or(l.strip_prefix('#')).unwrap_or(l))
            .collect::<Vec<_>>()
            .join("\n");
        let config: Config = toml::from_str(&uncommented).unwrap();
        assert_eq!(config.models.len(), 2);
        assert!(config.models.contains_key("google/embeddinggemma-2"));
    }

    #[test]
    fn tuning_flags_accept_values() {
        let cli = Cli::try_parse_from([
            "blowfish",
            "serve",
            "--indexing",
            "false",
            "--index-rows",
            "2",
            "--cache-disk-dir",
            "/tmp/cache",
            "--recall-sample-rate",
            "0.5",
        ])
        .unwrap();
        let Command::Serve { tuning, .. } = cli.command else {
            panic!("expected serve")
        };
        assert_eq!(tuning.indexing, Some(false));
        assert_eq!(tuning.index_rows, Some(2));
        assert_eq!(tuning.cache_disk_dir, Some(PathBuf::from("/tmp/cache")));
        assert_eq!(tuning.recall_sample_rate, Some(0.5));
    }
}
