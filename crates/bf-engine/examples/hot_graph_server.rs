//! Loopback-only measurement server using the automatic production graph lifecycle.
//! Usage: hot_graph_server storage.json namespace ready_marker port
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, QueryResponse};
use serde_json::{Value, json};
type Failure = (StatusCode, Json<Value>);
fn failure(e: bf_types::ApiError) -> Failure {
    (
        StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(json!({"status":"error","error":e.message})),
    )
}
async fn query(
    State(e): State<Engine>,
    Path(ns): Path<String>,
    Json(q): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, Failure> {
    e.query(&ns, q).await.map(Json).map_err(failure)
}
async fn recall(
    State(e): State<Engine>,
    Path(ns): Path<String>,
    Json(q): Json<Value>,
) -> Result<Json<Value>, Failure> {
    e.recall(&ns, q).await.map(Json).map_err(failure)
}
async fn status(State(e): State<Engine>, Path(ns): Path<String>) -> Json<Value> {
    let mut status = e.hot_graph_status(&ns);
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes this writable output on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } == 0 {
        // SAFETY: the successful call initialized the complete structure.
        let usage = unsafe { usage.assume_init() };
        #[cfg(target_os = "macos")]
        let peak_rss_bytes = usage.ru_maxrss as u64;
        #[cfg(not(target_os = "macos"))]
        let peak_rss_bytes = usage.ru_maxrss as u64 * 1024;
        status["process"] = json!({"peak_rss_bytes":peak_rss_bytes,"swaps":usage.ru_nswap});
    }
    Json(status)
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        return Err("usage: hot_graph_server storage.json namespace ready_marker port".into());
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let e = Engine::with_config(
        Storage::open(&config).await?,
        EngineConfig {
            indexing: false,
            ..EngineConfig::from_env()
        },
    );
    let app = Router::new()
        .route("/v2/namespaces/{ns}/query", post(query))
        .route("/v1/namespaces/{ns}/_debug/recall", post(recall))
        .route("/measure/{ns}/status", get(status))
        .with_state(e);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", args[4].parse::<u16>()?)).await?;
    std::fs::write(&args[3], b"ready\n")?;
    axum::serve(listener, app).await?;
    Ok(())
}
