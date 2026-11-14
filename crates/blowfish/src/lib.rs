//! HTTP adapter for the exhaustive engine, with replaceable authorization.
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, Request, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bf_engine::Engine;
use bf_types::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use tower_http::{
    compression::CompressionLayer, decompression::RequestDecompressionLayer, trace::TraceLayer,
};

/// Authorization seam for bf-auth or an application-specific authorizer.
pub trait Authorizer: Send + Sync + 'static {
    /// Authorize a method and path using the bearer token, if supplied.
    fn authorize(&self, token: Option<&str>, method: &Method, path: &str) -> Result<(), ApiError>;
}
/// Development authorizer: allow all when unset; otherwise require the configured admin key.
pub struct AdminKeyAuthorizer {
    key: Option<String>,
}
impl AdminKeyAuthorizer {
    /// Configure a bearer admin key without storing it in object storage.
    pub fn new(key: Option<String>) -> Self {
        Self { key }
    }
}
impl Authorizer for AdminKeyAuthorizer {
    fn authorize(&self, token: Option<&str>, _: &Method, _: &str) -> Result<(), ApiError> {
        if self
            .key
            .as_ref()
            .is_some_and(|key| token != Some(key.as_str()))
        {
            Err(ApiError::new(401, "invalid or missing bearer API key"))
        } else {
            Ok(())
        }
    }
}
#[derive(Clone)]
struct AppState {
    engine: Engine,
    auth: Arc<dyn Authorizer>,
}
/// Construct all M1 routes, JSON errors, compression, and authorization middleware.
pub fn router(engine: Engine, auth: Arc<dyn Authorizer>) -> Router {
    let state = AppState { engine, auth };
    Router::new()
        .route("/", get(|| async { Json(json!({"status":"🐡"})) }))
        .route("/v1/namespaces", get(list))
        .route("/v2/namespaces/{ns}", post(write).delete(delete_namespace))
        .route("/v2/namespaces/{ns}/query", post(query))
        .route("/v2/namespaces/{ns}/metadata", get(metadata))
        .route("/v1/namespaces/{ns}/metadata", get(metadata))
        .route(
            "/v1/namespaces/{ns}/schema",
            get(schema).post(update_schema),
        )
        .route("/v1/namespaces/{ns}/hint_cache_warm", get(warm))
        .route("/v1/namespaces/{ns}/_debug/recall", post(recall))
        .route("/v2/namespaces/{ns}/explain_query", post(explain))
        .fallback(|| async { HttpError(ApiError::new(404, "route was not found")) })
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
async fn authorize(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let token = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match state
        .auth
        .authorize(token, request.method(), request.uri().path())
    {
        Ok(()) => next.run(request).await,
        Err(e) => HttpError(e).into_response(),
    }
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
    input: Result<Json<WriteRequest>, JsonRejection>,
) -> Result<Json<WriteResponse>, HttpError> {
    Ok(Json(s.engine.write(&ns, body(input)?).await?))
}
async fn query(
    State(s): State<AppState>,
    Path(ns): Path<String>,
    input: Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Json<QueryResponse>, HttpError> {
    Ok(Json(s.engine.query(&ns, body(input)?).await?))
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
    params: Result<Query<ListParams>, QueryRejection>,
) -> Result<Json<Value>, HttpError> {
    let Query(params) = params.map_err(|_| ApiError::bad("invalid namespace list parameters"))?;
    Ok(Json(
        s.engine
            .list_namespaces(&params.prefix, params.cursor.as_deref(), params.page_size)
            .await?,
    ))
}
/// Extract bearer auth for integrations that provide their own HTTP routing.
pub fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}
