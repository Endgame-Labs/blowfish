//! Loopback-only G3 HTTP prototype over the real engine query path.
//! Usage: hot_graph_server storage.json namespace ready_marker port ef rerank budget_gib
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::post,
};
use bf_engine::{
    Engine, EngineConfig,
    hot_graph::{self, HotGraphOptions},
};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, QueryResponse};
use serde_json::{Value, json};
use std::{error::Error, path::PathBuf};
async fn query(
    State(engine): State<Engine>,
    Path(ns): Path<String>,
    Json(q): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<Value>)> {
    engine.query(&ns, q).await.map(Json).map_err(|e| {
        (
            StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(json!({"status":"error","error":e.message})),
        )
    })
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 8 {
        return Err(
            "usage: hot_graph_server storage.json namespace ready_marker port ef rerank budget_gib"
                .into(),
        );
    }
    let config: StorageConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let store = Storage::open(&config).await?;
    let engine = Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            ..EngineConfig::from_env()
        },
    );
    let builder = engine.clone();
    let namespace = args[2].clone();
    let ready = PathBuf::from(&args[3]);
    let options = HotGraphOptions {
        query_ef: args[5].parse()?,
        rerank: args[6].parse()?,
        build_budget_bytes: args[7].parse::<u64>()? * 1024 * 1024 * 1024,
        sparse_rerank: std::env::var_os("G3_RETAIN_EXACT").is_none(),
        ..Default::default()
    };
    tokio::spawn(async move {
        if options.query_ef == 0 {
            let _ = std::fs::write(ready, b"ivf\n");
            return;
        }
        match builder.build_hot_graph(&namespace, "vector", options).await {
            Ok(Some(r)) if r.installed => {
                eprintln!(
                    "HOT_GRAPH_READY rows={} estimated_build_bytes={} resident_bytes={} seconds={:.3}",
                    r.rows, r.estimated_build_bytes, r.resident_bytes, r.seconds
                );
                if let Err(e) = std::fs::write(ready, b"ready\n") {
                    eprintln!("ready marker: {e}");
                }
            }
            other => eprintln!("HOT_GRAPH_UNAVAILABLE {other:?}"),
        }
    });
    let app = Router::new()
        .route("/v2/namespaces/{ns}/query", post(query))
        .route(
            "/g3/status",
            axum::routing::get(|| async {
                Json(json!({"answered":hot_graph::answered_queries()}))
            }),
        )
         .route("/g3/budget",post(|Json(v):Json<Value>|async move {
        Json(json!({"ok":hot_graph::query_budget(v["ef"].as_u64().unwrap_or(0) as usize,v["rerank"].as_u64().unwrap_or(0) as usize)}))
    })).with_state(engine);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", args[4].parse::<u16>()?)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
