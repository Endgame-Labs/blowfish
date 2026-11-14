//! Durable async-copy status, shared by every node through immutable bucket objects.
use super::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct OperationRecord {
    namespace: String,
    state: Value,
}
fn storage_error(error: bf_storage::Error) -> HttpError {
    tracing::error!(?error, "operation storage failed");
    ApiError::new(503, "object storage is unavailable").into()
}
fn decode(bytes: &[u8]) -> Result<OperationRecord, HttpError> {
    serde_json::from_slice(bytes)
        .map_err(|_| ApiError::new(500, "stored operation is corrupt").into())
}
pub(super) fn authorize_copy(
    s: &AppState,
    p: &Principal,
    request: &WriteRequest,
) -> Result<(), HttpError> {
    if let Some(source) = &request.copy_from_namespace {
        let source = match source {
            CopyFrom::Name(name) => name,
            CopyFrom::Config {
                source_namespace, ..
            } => source_namespace,
        };
        s.auth.authorize(p, Action::Query, Some(source))?;
    }
    Ok(())
}
pub(super) async fn start(
    s: AppState,
    ns: String,
    request: WriteRequest,
    apply_preference: bool,
) -> Result<Response, HttpError> {
    validate_namespace(&ns)?;
    if request.copy_from_namespace.is_none() {
        return Err(ApiError::bad("async endpoint requires copy_from_namespace").into());
    }
    let token = Uuid::new_v4().to_string();
    let prefix = format!("_system/operations/{token}");
    let start_time = Utc::now().to_rfc3339();
    let running = OperationRecord {
        namespace: ns.clone(),
        state: json!({"status":"running","start_time":start_time}),
    };
    s.engine
        .storage()
        .create(
            &format!("{prefix}/running.json"),
            serde_json::to_vec(&running).expect("operation serializes"),
        )
        .await
        .map_err(storage_error)?;
    let location = format!(
        "/v1/namespaces/{}/operations/{token}",
        percent_encoding::utf8_percent_encode(&ns, percent_encoding::NON_ALPHANUMERIC)
    );
    tokio::spawn(async move {
        let result = match s.engine.write(&ns, request).await {
            Ok(response) => json!({"success":response}),
            Err(error) => json!({"error":{"status_code":error.status,"detail":error.body()}}),
        };
        let finished = OperationRecord {
            namespace: ns,
            state: json!({"status":"finished","start_time":start_time,"finish_time":Utc::now().to_rfc3339(),"result":result}),
        };
        let bytes = serde_json::to_vec(&finished).expect("operation serializes");
        // Keep one identity across ambiguous retries of the immutable completion object.
        let put_id = Uuid::new_v4();
        for attempt in 0..6 {
            match s
                .engine
                .storage()
                .create_with_id(&format!("{prefix}/finished.json"), bytes.clone(), put_id)
                .await
            {
                Ok(_) => return,
                Err(error) => tracing::error!(?error, attempt, "persist async completion failed"),
            }
            tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
        }
    });
    let mut response = (StatusCode::ACCEPTED, Json(json!({"token":token}))).into_response();
    response.headers_mut().insert(
        "location",
        HeaderValue::from_str(&location).expect("encoded location"),
    );
    if apply_preference {
        response.headers_mut().insert(
            "preference-applied",
            HeaderValue::from_static("respond-async"),
        );
    }
    Ok(response)
}
pub(super) async fn start_copy(
    State(s): State<AppState>,
    Path(ns): Path<String>,
    Extension(p): Extension<Principal>,
    input: Result<Json<WriteRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let request = body(input)?;
    authorize_copy(&s, &p, &request)?;
    start(s, ns, request, false).await
}
pub(super) async fn poll(
    State(s): State<AppState>,
    Path((ns, token)): Path<(String, String)>,
) -> Result<Json<Value>, HttpError> {
    validate_namespace(&ns)?;
    if Uuid::parse_str(&token).is_err() {
        return Err(ApiError::new(404, "operation was not found").into());
    }
    let prefix = format!("_system/operations/{token}");
    let store = s.engine.storage();
    let missing = || HttpError(ApiError::new(404, "operation was not found"));
    let finished = store.get(&format!("{prefix}/finished.json")).await;
    let record = match finished {
        Ok(bytes) => {
            let record = decode(&bytes)?;
            if record.namespace != ns {
                return Err(missing());
            }
            let finish = record.state["finish_time"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .ok_or_else(|| HttpError(ApiError::new(500, "stored operation is corrupt")))?;
            if Utc::now().signed_duration_since(finish).num_seconds() >= 3600 {
                // Expired results are inaccessible even when best-effort cleanup fails.
                for suffix in ["running.json", "finished.json"] {
                    let _ = store.delete(&format!("{prefix}/{suffix}")).await;
                }
                return Err(missing());
            }
            record
        }
        Err(bf_storage::Error::NotFound) => {
            let bytes =
                store
                    .get(&format!("{prefix}/running.json"))
                    .await
                    .map_err(|e| match e {
                        bf_storage::Error::NotFound => missing(),
                        e => storage_error(e),
                    })?;
            decode(&bytes)?
        }
        Err(error) => return Err(storage_error(error)),
    };
    if record.namespace != ns {
        return Err(missing());
    }
    Ok(Json(record.state))
}
