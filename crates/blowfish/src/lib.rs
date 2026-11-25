//! HTTP adapter for the exhaustive engine, with replaceable authorization.
mod admin;
mod legacy;
mod operations;
pub mod reactors;

use axum::{
    Extension, Json, Router,
    extract::{
        DefaultBodyLimit, FromRequest, MatchedPath, Path, Query, Request, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bf_alloc as _; // installs jemalloc as the global allocator
use bf_auth::{Action, AuthError, KeyStore, Principal};
use bf_engine::Engine;
use bf_types::*;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tower_http::{
    compression::CompressionLayer, decompression::RequestDecompressionLayer,
    limit::RequestBodyLimitLayer, trace::TraceLayer,
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
    projects: Arc<ProjectEngines>,
}
/// One engine per project, created on first use. `default` is the root engine itself.
struct ProjectEngines {
    root: Engine,
    engines: std::sync::Mutex<HashMap<String, Engine>>,
}
impl ProjectEngines {
    fn get(&self, project: &str) -> Engine {
        if project == bf_auth::DEFAULT_PROJECT {
            return self.root.clone();
        }
        self.engines
            .lock()
            .expect("project engine registry")
            .entry(project.to_owned())
            .or_insert_with(|| self.root.scoped(&format!("projects/{project}")))
            .clone()
    }
    fn remove(&self, project: &str) {
        self.engines
            .lock()
            .expect("project engine registry")
            .remove(project);
    }
}
/// Keep an engine running for every project, so its background indexing resumes after a
/// restart without waiting for a first request, and drop engines of deleted projects.
fn sync_project_engines(state: &AppState) {
    let weak = Arc::downgrade(&state.projects);
    let auth = state.auth.clone();
    tokio::spawn(async move {
        loop {
            let Some(projects) = weak.upgrade() else {
                break;
            };
            if let Ok(ids) = auth.project_ids().await {
                for id in &ids {
                    projects.get(id);
                }
                projects
                    .engines
                    .lock()
                    .expect("project engine registry")
                    .retain(|id, _| ids.contains(id));
            }
            drop(projects);
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
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
/// Start the allocator monitor configured by `BF_MEM_LOG_SECS` and `BF_HEAP_DUMP_DIR` (see `bf-alloc`).
pub fn start_alloc_monitor() {
    bf_alloc::start_monitor();
}
/// Unauthenticated metrics listener; bind it only to a trusted monitoring network.
pub fn metrics_router() -> Router {
    Router::new().route("/metrics", get(metrics_endpoint))
}
async fn metrics_endpoint() -> impl IntoResponse {
    bf_alloc::publish_gauges();
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        prometheus_handle().render(),
    )
}
/// Construct API, admin, and metrics routes with JSON errors and bearer authorization.
pub fn router(engine: Engine, auth: KeyStore, options: ServerOptions) -> Router {
    router_with_body_limit(engine, auth, options, 512 * 1024 * 1024)
}
fn router_with_body_limit(
    engine: Engine,
    auth: KeyStore,
    options: ServerOptions,
    body_limit: usize,
) -> Router {
    prometheus_handle();
    let state = AppState {
        auth,
        options,
        started: Instant::now(),
        projects: Arc::new(ProjectEngines {
            root: engine.clone(),
            engines: Default::default(),
        }),
        engine,
    };
    sync_project_engines(&state);
    Router::new()
        .route("/", get(|| async { Json(json!({"status":"🐡"})) }))
        .route("/metrics", get(metrics_endpoint))
        .route("/_blowfish/v1/status", get(status))
        .route("/_blowfish/v1/keys", get(list_keys).post(create_key))
        .route("/_blowfish/v1/keys/{id}", get(get_key).delete(revoke_key))
        .route("/_blowfish/v1/whoami", get(whoami))
        .route(
            "/_blowfish/v1/projects",
            get(admin::list_projects).post(admin::create_project),
        )
        .route(
            "/_blowfish/v1/projects/{id}",
            get(admin::get_project)
                .patch(admin::rename_project)
                .delete(admin::delete_project),
        )
        .route(
            "/_blowfish/v1/projects/{id}/keys",
            get(admin::list_project_keys).post(admin::create_project_key),
        )
        .route(
            "/_blowfish/v1/projects/{id}/keys/{key_id}",
            get(admin::get_project_key).delete(admin::revoke_project_key),
        )
        .route(
            "/_blowfish/v1/org/keys",
            get(admin::list_org_keys).post(admin::create_org_key),
        )
        .route(
            "/_blowfish/v1/org/keys/{key_id}",
            get(admin::get_org_key).delete(admin::revoke_org_key),
        )
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
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .layer(RequestDecompressionLayer::new())
        // Bound compressed input as well as the decompressed JSON extractor body.
        .layer(RequestBodyLimitLayer::new(body_limit))
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
    let namespaces = s.engine.cached_namespaces().await;
    let hot_graphs = namespaces
        .iter()
        .map(|namespace| (namespace.clone(), s.engine.hot_graph_status(namespace)))
        .collect::<std::collections::BTreeMap<_, _>>();
    Json(
        json!({"version":env!("CARGO_PKG_VERSION"), "storage_url":redacted_storage_url(&s.options.storage_url), "storage_mode":mode, "uptime_seconds":s.started.elapsed().as_secs_f64(), "namespaces_cached":namespaces, "hot_graphs":hot_graphs}),
    )
}
async fn whoami(Extension(p): Extension<Principal>) -> Json<bf_auth::WhoAmI> {
    Json(p.whoami())
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
/// Retry delay sent on a 429 or 503 that carries no estimate of its own.
const DEFAULT_RETRY_AFTER_MS: u64 = 1000;
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
        let headers = response.headers_mut();
        // The official SDKs read `retry-after-ms`, then `Retry-After` (seconds), and retry 408,
        // 409, 429, and 5xx unless `x-should-retry` says otherwise.
        if matches!(self.0.status, 429 | 503) {
            let ms = self.0.retry_after_ms.unwrap_or(DEFAULT_RETRY_AFTER_MS);
            headers.insert("retry-after", HeaderValue::from(ms.div_ceil(1000).max(1)));
            headers.insert("retry-after-ms", HeaderValue::from(ms));
        }
        let retry = self.0.should_retry.or(match self.0.status {
            408 | 409 | 429 | 502 | 503 | 504 => Some(true),
            500 => Some(false),
            _ => None,
        });
        if let Some(retry) = retry {
            headers.insert(
                "x-should-retry",
                HeaderValue::from_static(if retry { "true" } else { "false" }),
            );
        }
        response
    }
}
fn body<T>(result: Result<Json<T>, JsonRejection>) -> Result<T, HttpError> {
    result.map(|Json(v)| v).map_err(|e| {
        // A streaming compressed-input limit can be wrapped by both tower and axum.
        // Inspect the source chain so all body-size failures retain HTTP 413.
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&e);
        while let Some(error) = cause {
            if error.is::<http_body_util::LengthLimitError>() {
                return HttpError(ApiError::new(413, "request body exceeds size limit"));
            }
            cause = error.source();
        }
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
    let namespaced = route.contains("{ns}") || route == "/v1/namespaces";
    let response = match principal {
        Err(e) => HttpError::from(e).into_response(),
        Ok(p) => {
            // Resolve the project first, so another project's namespace is a 404 before any
            // role check could distinguish "exists" from "doesn't".
            let scoped = if namespaced {
                let header = request
                    .headers()
                    .get("x-blowfish-project")
                    .and_then(|v| v.to_str().ok());
                match state.auth.effective_project(&p, header).await {
                    Ok(project) => {
                        let mut scoped = state.clone();
                        scoped.engine = state.projects.get(&project);
                        Some(scoped)
                    }
                    Err(e) => {
                        return finish(
                            HttpError::from(e).into_response(),
                            route,
                            method,
                            key_name,
                            started,
                        );
                    }
                }
            } else {
                None
            };
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
                    if let Some(scoped) = scoped {
                        request.extensions_mut().insert(scoped);
                    }
                    next.run(request).await
                }
            }
        }
    };
    finish(response, route, method, key_name, started)
}
fn finish(
    response: Response,
    route: String,
    method: String,
    key_name: String,
    started: Instant,
) -> Response {
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
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
    Extension(p): Extension<Principal>,
    headers: HeaderMap,
    input: Result<FastJson<WriteRequest, true>, JsonRejection>,
) -> Result<Response, HttpError> {
    let request = body(input.map(|FastJson(v)| Json(v)))?;
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
fn trace_query_stages() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("BF_TRACE_STAGES").is_some())
}
/// `Json<T>`, with the same rejections, that parses a valid body without
/// `serde_path_to_error`'s per-value path tracking (several percent of a vector query's CPU).
/// Only a body that fails to parse is parsed again through `Json`, for its error path.
struct FastJson<T, const STAGES: bool = false>(T);
impl<T, S, const STAGES: bool> FromRequest<S> for FastJson<T, STAGES>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = JsonRejection;
    async fn from_request(req: Request, state: &S) -> Result<Self, JsonRejection> {
        // As `Json` decides it, so every other body gets `Json`'s own rejection.
        let json = req
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<mime::Mime>().ok())
            .is_some_and(|m| {
                m.type_() == "application"
                    && (m.subtype() == "json" || m.suffix().is_some_and(|s| s == "json"))
            });
        if !json {
            return Json::<T>::from_request(req, state)
                .await
                .map(|Json(v)| Self(v));
        }
        let started = std::time::Instant::now();
        let bytes = axum::body::Bytes::from_request(req, state).await?;
        let collected = started.elapsed();
        let parsed = serde_json::from_slice(&bytes);
        if STAGES {
            let seconds = |d: std::time::Duration| d.as_secs_f64();
            metrics::histogram!("blowfish_commit_stage_seconds", "stage" => "http_receive")
                .record(seconds(collected));
            metrics::histogram!("blowfish_commit_stage_seconds", "stage" => "http_parse")
                .record(seconds(started.elapsed() - collected));
        }
        match parsed {
            Ok(v) => {
                if trace_query_stages() {
                    eprintln!(
                        "HTTP collect={}us decode={}us bytes={}",
                        collected.as_micros(),
                        (started.elapsed() - collected).as_micros(),
                        bytes.len()
                    );
                }
                Ok(Self(v))
            }
            Err(_) => Json::<T>::from_bytes(&bytes).map(|Json(v)| Self(v)),
        }
    }
}
async fn query(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
    input: Result<FastJson<QueryRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let request = body(input.map(|FastJson(v)| Json(v)))?;
    let response = s.engine.query(&ns, request.clone()).await?;
    record_query(&request);
    metrics::counter!("blowfish_query_rows_scanned_total")
        .increment(response.performance.exhaustive_search_count);
    let started = std::time::Instant::now();
    let response = Json(response).into_response();
    if trace_query_stages() {
        eprintln!("HTTP response={}us", started.elapsed().as_micros());
    }
    Ok(response)
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
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<Value>, HttpError> {
    s.engine.delete_namespace(&ns).await?;
    Ok(Json(json!({"status":"OK"})))
}
async fn metadata(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(s.engine.metadata(&ns).await?))
}
async fn patch_metadata(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(s.engine.patch_metadata(&ns, body(input)?).await?))
}
async fn schema(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(s.engine.schema(&ns).await?))
}
async fn update_schema(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
    input: Result<Json<SchemaUpdate>, JsonRejection>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(s.engine.update_schema(&ns, body(input)?).await?))
}
async fn warm(
    Extension(s): Extension<AppState>,
    Path(ns): Path<String>,
) -> Result<(StatusCode, Json<Value>), HttpError> {
    s.engine.warm(&ns).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"status":"ACCEPTED","message":"cache warm hint accepted"})),
    ))
}
async fn recall(
    Extension(s): Extension<AppState>,
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
    Extension(s): Extension<AppState>,
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
    Extension(s): Extension<AppState>,
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

#[cfg(test)]
mod body_limit_tests {
    use super::*;
    use axum::body::Body;
    use flate2::{Compression, write::GzEncoder};
    use http_body_util::BodyExt;
    use std::io::Write;
    use tower::ServiceExt;

    #[tokio::test]
    async fn bounds_compressed_and_decompressed_bodies_with_json_errors() {
        let store = bf_storage::Storage::open(&bf_storage::StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let keys = KeyStore::from_storage(store.clone(), None, Duration::from_secs(5))
            .await
            .unwrap();
        let app = router_with_body_limit(Engine::new(store), keys, ServerOptions::default(), 1024);
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(format!("{{\"x\":\"{}\"}}", "a".repeat(2048)).as_bytes())
            .unwrap();
        let bomb = encoder.finish().unwrap();
        assert!(bomb.len() < 1024);
        for (payload, encoding, length) in [
            (bomb, "gzip", None),
            (vec![b' '; 1025], "identity", None),
            (vec![], "gzip", Some("1025")),
        ] {
            let mut request = axum::http::Request::builder()
                .method("POST")
                .uri("/v2/namespaces/test")
                .header("content-type", "application/json")
                .header("content-encoding", encoding);
            if let Some(length) = length {
                request = request.header("content-length", length);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::from(payload)).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let error: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(error["status"], "error");
        }
    }
}
