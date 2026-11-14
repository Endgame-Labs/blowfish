//! HTTP adapter for the exhaustive engine, with replaceable authorization.
mod legacy;
mod operations;

use axum::{
    Extension, Json, Router,
    extract::{
        DefaultBodyLimit, MatchedPath, Path, Query, Request, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bf_auth::{Action, AuthError, KeyStore, Principal};
use bf_engine::Engine;
use bf_types::*;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    sync::OnceLock,
    time::{Duration, Instant},
};
use tower_http::{
    compression::CompressionLayer, decompression::RequestDecompressionLayer, trace::TraceLayer,
};

/// Runtime options for authentication and operational endpoints.
#[derive(Clone, Default)]
pub struct ServerOptions {
    /// Enforce bearer authentication. Disable only for local development.
    pub auth_enabled: bool,
    /// Storage URL to report after removing credentials, query parameters, and fragments.
    pub storage_url: String,
}
#[derive(Clone)]
struct AppState {
    engine: Engine,
    auth: KeyStore,
    options: ServerOptions,
    started: Instant,
}
/// Install one process-wide recorder shared by every crate using the metrics facade.
pub fn prometheus_handle() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            let handle = PrometheusBuilder::new()
                .install_recorder()
                .expect("install Prometheus recorder before another global recorder");
            let upkeep = handle.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    upkeep.run_upkeep();
                }
            });
            handle
        })
        .clone()
}
/// Unauthenticated metrics listener; bind it only to a trusted monitoring network.
pub fn metrics_router() -> Router {
    Router::new().route("/metrics", get(metrics_endpoint))
}
async fn metrics_endpoint() -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        prometheus_handle().render(),
    )
}
/// Construct API, admin, and metrics routes with JSON errors and bearer authorization.
pub fn router(engine: Engine, auth: KeyStore, options: ServerOptions) -> Router {
    prometheus_handle();
    let state = AppState {
        engine,
        auth,
        options,
        started: Instant::now(),
    };
    Router::new()
        .route("/", get(|| async { Json(json!({"status":"🐡"})) }))
        .route("/metrics", get(metrics_endpoint))
        .route("/_blowfish/v1/status", get(status))
        .route("/_blowfish/v1/keys", get(list_keys).post(create_key))
        .route("/_blowfish/v1/keys/{id}", get(get_key).delete(revoke_key))
        .route("/v1/namespaces", get(list))
        .route("/v2/namespaces/{ns}", post(write).delete(delete_namespace))
        .route("/v2/namespaces/{ns}/async", post(operations::start_copy))
        .route(
            "/v1/namespaces/{ns}/operations/{token}",
            get(operations::poll),
        )
        .route(
            "/v1/namespaces/{ns}",
            post(legacy::write)
                .get(legacy::export)
                .head(legacy::head)
                .delete(legacy::delete),
        )
        .route("/v1/namespaces/{ns}/query", post(legacy::query))
        .route("/v2/namespaces/{ns}/query", post(query))
        .route("/v2/namespaces/{ns}/metadata", get(metadata))
        .route(
            "/v1/namespaces/{ns}/metadata",
            get(metadata).patch(patch_metadata),
        )
        .route(
            "/v1/namespaces/{ns}/schema",
            get(schema).post(update_schema),
        )
        .route("/v1/namespaces/{ns}/hint_cache_warm", get(warm))
        .route("/v1/namespaces/{ns}/_debug/recall", post(recall))
        .route("/v2/namespaces/{ns}/explain_query", post(explain))
        .fallback(|| async { HttpError(ApiError::new(404, "unknown route")) })
        .method_not_allowed_fallback(|| async {
            HttpError(ApiError::new(405, "method is not allowed"))
        })
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .layer(RequestDecompressionLayer::new())
        .layer(middleware::from_fn(json_errors))
        .layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state)
}
impl From<AuthError> for HttpError {
    fn from(e: AuthError) -> Self {
        Self(ApiError::new(e.status_code(), e.to_string()))
    }
}
/// Remove credentials and signed URL data before operational display.
pub fn redacted_storage_url(value: &str) -> String {
    let Ok(mut url) = url::Url::parse(value) else {
        return "<invalid storage URL>".into();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}
async fn status(State(s): State<AppState>) -> Json<Value> {
    let store = s.engine.storage();
    let mode = if store.is_read_only() {
        "read_only"
    } else {
        match store.mode() {
            bf_storage::StorageMode::Conditional => "multi_writer",
            bf_storage::StorageMode::SingleWriter => "single_writer",
        }
    };
    Json(
        json!({"version":env!("CARGO_PKG_VERSION"), "storage_url":redacted_storage_url(&s.options.storage_url), "storage_mode":mode, "uptime_seconds":s.started.elapsed().as_secs_f64(), "namespaces_cached":s.engine.cached_namespaces().await}),
    )
}
async fn create_key(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    input: Result<Json<bf_auth::CreateKeyRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<bf_auth::CreateKeyResponse>), HttpError> {
    let (key, token) = s.auth.create_key(body(input)?, &p).await?;
    Ok((
        StatusCode::CREATED,
        Json(bf_auth::CreateKeyResponse { key, token }),
    ))
}
async fn list_keys(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
) -> Result<Json<bf_auth::ListKeysResponse>, HttpError> {
    Ok(Json(bf_auth::ListKeysResponse {
        keys: s.auth.list_keys(&p).await?,
    }))
}
async fn get_key(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<bf_auth::KeyInfo>, HttpError> {
    Ok(Json(s.auth.get_key(&p, &id).await?))
}
async fn revoke_key(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, HttpError> {
    s.auth.revoke_key(&p, &id).await?;
    Ok(Json(json!({"status":"OK"})))
}
/// HTTP representation of the shared error, without coupling bf-types to axum.
pub struct HttpError(pub ApiError);
impl From<ApiError> for HttpError {
    fn from(e: ApiError) -> Self {
        Self(e)
    }
}
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.0.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut response = (status, Json(self.0.body())).into_response();
        if self.0.status == 500 {
            response
                .headers_mut()
                .insert("x-should-retry", HeaderValue::from_static("false"));
        }
        response
    }
}
fn body<T>(result: Result<Json<T>, JsonRejection>) -> Result<T, HttpError> {
    result.map(|Json(v)| v).map_err(|e| {
        let detail = e.body_text();
        if matches!(e, JsonRejection::JsonSyntaxError(_)) {
            return HttpError(ApiError::bad(format!("💔 {detail}")));
        }
        if matches!(e, JsonRejection::MissingJsonContentType(_)) {
            return HttpError(ApiError::new(
                415,
                "💔 Expected request with `Content-Type: application/json`",
            ));
        }
        if detail.contains("is not supported by blowfish yet")
            || detail.contains("weight must be non-negative")
        {
            return HttpError(ApiError::bad(detail));
        }
        HttpError(ApiError::new(
            if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
                413
            } else {
                422
            },
            "failed to deserialize the JSON body into the target type",
        ))
    })
}
// An RAII guard keeps the gauge correct when a client cancels a request.
struct InFlight;
impl Drop for InFlight {
    fn drop(&mut self) {
        metrics::gauge!("blowfish_http_requests_in_flight").decrement(1.0);
    }
}
async fn authorize(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    let started = Instant::now();
    metrics::gauge!("blowfish_http_requests_in_flight").increment(1.0);
    let _guard = InFlight;
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str())
        .unwrap_or("unmatched")
        .to_owned();
    let method = request.method().to_string();
    let principal = if state.options.auth_enabled {
        state
            .auth
            .authenticate(bearer_token(request.headers()).unwrap_or(""))
            .await
    } else {
        Ok(Principal::development())
    };
    let key_name = principal
        .as_ref()
        .map(|p| p.name())
        .unwrap_or("unauthenticated")
        .to_owned();
    let namespace = if route.contains("{ns}") {
        request.uri().path().split('/').nth(3).and_then(|ns| {
            percent_encoding::percent_decode_str(ns)
                .decode_utf8()
                .ok()
                .map(|s| s.into_owned())
        })
    } else {
        None
    };
    let response = match principal {
        Err(e) => HttpError::from(e).into_response(),
        Ok(p) => {
            let action = match route.as_str() {
                "/metrics" => Some(Action::ReadMetrics),
                "/_blowfish/v1/status" => Some(Action::ReadMetrics),
                "/_blowfish/v1/keys" | "/_blowfish/v1/keys/{id}" => Some(Action::ManageKeys),
                "/v1/namespaces" => Some(Action::ListNamespaces),
                "/v1/namespaces/{ns}" | "/v2/namespaces/{ns}"
                    if request.method() == Method::DELETE =>
                {
                    Some(Action::DeleteNamespace)
                }
                "/v1/namespaces/{ns}" if request.method() == Method::POST => Some(Action::Write),
                "/v1/namespaces/{ns}" if request.method() == Method::HEAD => {
                    Some(Action::ReadMetadata)
                }
                "/v1/namespaces/{ns}"
                | "/v1/namespaces/{ns}/query"
                | "/v1/namespaces/{ns}/operations/{token}" => Some(Action::Query),
                "/v2/namespaces/{ns}" | "/v2/namespaces/{ns}/async" => Some(Action::Write),
                "/v2/namespaces/{ns}/query" => Some(Action::Query),
                "/v1/namespaces/{ns}/metadata" if request.method() == Method::PATCH => {
                    Some(Action::WriteSchema)
                }
                "/v1/namespaces/{ns}/metadata" | "/v2/namespaces/{ns}/metadata" => {
                    Some(Action::ReadMetadata)
                }
                "/v1/namespaces/{ns}/schema" if request.method() == Method::POST => {
                    Some(Action::WriteSchema)
                }
                "/v1/namespaces/{ns}/schema" => Some(Action::ReadSchema),
                "/v1/namespaces/{ns}/hint_cache_warm" => Some(Action::WarmCache),
                "/v1/namespaces/{ns}/_debug/recall" => Some(Action::Recall),
                "/v2/namespaces/{ns}/explain_query" => Some(Action::ExplainQuery),
                _ => None,
            };
            let allowed = action
                .map(|a| state.auth.authorize(&p, a, namespace.as_deref()))
                .unwrap_or(Ok(()));
            let allowed = if route == "/_blowfish/v1/status" && allowed.is_err() {
                state.auth.authorize(&p, Action::ManageKeys, None)
            } else {
                allowed
            };
            match allowed {
                Err(e) => HttpError::from(e).into_response(),
                Ok(()) => {
                    request.extensions_mut().insert(p);
                    next.run(request).await
                }
            }
        }
    };
    let labels = [
        ("route", route),
        ("method", method),
        ("status", response.status().as_u16().to_string()),
        ("key_name", key_name),
    ];
    metrics::counter!("blowfish_http_requests_total", &labels).increment(1);
    metrics::histogram!("blowfish_http_request_duration_seconds", &labels)
        .record(started.elapsed().as_secs_f64());
    response
}
async fn json_errors(request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    let json = response
        .headers()
        .get("content-type")
        .is_some_and(|v| v.as_bytes().starts_with(b"application/json"));
    if !json && (response.status().is_client_error() || response.status().is_server_error()) {
        HttpError(ApiError::new(
            response.status().as_u16(),
            response.status().canonical_reason().unwrap_or("HTTP error"),
        ))
        .into_response()
    } else {
        response
    }
}
async fn write(
    State(s): State<AppState>,
    Path(ns): Path<String>,
    Extension(p): Extension<Principal>,
    headers: HeaderMap,
    input: Result<Json<WriteRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let request = body(input)?;
    operations::authorize_copy(&s, &p, &request)?;
    if request.copy_from_namespace.is_some()
        && headers.get_all("prefer").iter().any(|v| {
            v.to_str().is_ok_and(|v| {
                v.split(',')
                    .any(|p| p.trim().eq_ignore_ascii_case("respond-async"))
            })
        })
    {
        return operations::start(s, ns, request, true).await;
    }
    let response = s.engine.write(&ns, request).await?;
    for (kind, count) in [
        ("upserted", response.rows_upserted),
        ("patched", response.rows_patched),
        ("deleted", response.rows_deleted),
    ] {
        metrics::counter!("blowfish_write_rows_total", "operation" => kind)
            .increment(count.unwrap_or(0) as u64);
    }
    Ok(Json(response).into_response())
}
async fn query(
    State(s): State<AppState>,
    Path(ns): Path<String>,
    input: Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Json<QueryResponse>, HttpError> {
    let request = body(input)?;
    let response = s.engine.query(&ns, request.clone()).await?;
    record_query(&request);
    metrics::counter!("blowfish_query_rows_scanned_total")
        .increment(response.performance.exhaustive_search_count);
    Ok(Json(response))
}
fn record_query(request: &QueryRequest) {
    if let Some(queries) = &request.queries {
        for query in queries {
            record_query(query);
        }
        return;
    }
    let kind = if request.aggregate_by.is_some() {
        "aggregate"
    } else {
        match &request.rank_by {
            Some(RankBy::Vector { exact: true, .. }) => "knn",
            Some(RankBy::Vector { exact: false, .. }) => "ann",
            Some(RankBy::Bm25 { .. }) => "bm25",
            Some(RankBy::Order(_)) | None => "order",
            _ => "score",
        }
    };
    metrics::counter!("blowfish_query_requests_total", "kind" => kind).increment(1);
}
async fn delete_namespace(
    State(s): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<Value>, HttpError> {
    s.engine.delete_namespace(&ns).await?;
    Ok(Json(json!({"status":"OK"})))
}
async fn metadata(
    State(s): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(s.engine.metadata(&ns).await?))
}
async fn patch_metadata(
    State(s): State<AppState>,
    Path(ns): Path<String>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(s.engine.patch_metadata(&ns, body(input)?).await?))
}
async fn schema(
    State(s): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(s.engine.schema(&ns).await?))
}
async fn update_schema(
    State(s): State<AppState>,
    Path(ns): Path<String>,
    input: Result<Json<SchemaUpdate>, JsonRejection>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(s.engine.update_schema(&ns, body(input)?).await?))
}
async fn warm(
    State(s): State<AppState>,
    Path(ns): Path<String>,
) -> Result<(StatusCode, Json<Value>), HttpError> {
    s.engine.warm(&ns).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"status":"ACCEPTED","message":"cache warm hint accepted"})),
    ))
}
async fn recall(
    State(s): State<AppState>,
    Path(ns): Path<String>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, HttpError> {
    let request = body(input)?;
    if !request.is_object() {
        return Err(ApiError::malformed("recall requires a JSON object").into());
    }
    Ok(Json(s.engine.recall(&ns, request).await?))
}
async fn explain(
    State(s): State<AppState>,
    Path(ns): Path<String>,
    input: Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(s.engine.explain(&ns, body(input)?).await?))
}
#[derive(Deserialize)]
struct ListParams {
    #[serde(default)]
    prefix: String,
    cursor: Option<String>,
    #[serde(default = "default_page_size")]
    page_size: usize,
}
fn default_page_size() -> usize {
    100
}
async fn list(
    State(s): State<AppState>,
    Extension(principal): Extension<Principal>,
    params: Result<Query<ListParams>, QueryRejection>,
) -> Result<Json<Value>, HttpError> {
    let Query(params) = params.map_err(|_| ApiError::bad("invalid namespace list parameters"))?;
    // Preserve the engine's opaque cursor while skipping inaccessible pages. Fetch one
    // result at a time so a cursor never leaks an inaccessible namespace name.
    if !(1..=1000).contains(&params.page_size) {
        return Err(ApiError::bad("page_size must be between 1 and 1000").into());
    }
    let mut cursor = params.cursor;
    let mut visible = Vec::new();
    let mut next_cursor = None;
    loop {
        let page = s
            .engine
            .list_namespaces(&params.prefix, cursor.as_deref(), 1)
            .await?;
        for ns in page["namespaces"].as_array().into_iter().flatten() {
            if ns["id"]
                .as_str()
                .is_some_and(|ns| principal.allows_namespace(ns))
            {
                if visible.len() == params.page_size {
                    return Ok(Json(
                        json!({"namespaces":visible,"next_cursor":next_cursor}),
                    ));
                }
                visible.push(ns.clone());
                next_cursor = page["next_cursor"].as_str().map(str::to_owned);
            }
        }
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    Ok(Json(json!({"namespaces":visible,"next_cursor":null})))
}
/// Extract bearer auth for integrations that provide their own HTTP routing.
pub fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}
