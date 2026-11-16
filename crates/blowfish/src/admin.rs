//! Admin API: projects, project keys, and org superadmin keys.
//!
//! Authorization lives in [`bf_auth::KeyStore`], which answers 403 for a key that lacks the
//! authority and 404 for a project that is not the caller's own.
use super::*;
use bf_auth::{
    CreateKeyResponse, CreateKindKeyRequest, CreateProjectRequest, KeyInfo, Project,
    UpdateProjectRequest,
};

#[derive(Deserialize, Default)]
pub(super) struct DeleteParams {
    #[serde(default)]
    force: bool,
}

pub(super) async fn list_projects(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
) -> Result<Json<Value>, HttpError> {
    Ok(Json(json!({"projects": s.auth.list_projects(&p).await?})))
}
pub(super) async fn create_project(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    input: Result<Json<CreateProjectRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<Project>), HttpError> {
    let project = s.auth.create_project(&p, body(input)?).await?;
    Ok((StatusCode::CREATED, Json(project)))
}
pub(super) async fn get_project(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Project>, HttpError> {
    Ok(Json(s.auth.get_project(&p, &id).await?))
}
pub(super) async fn rename_project(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
    input: Result<Json<UpdateProjectRequest>, JsonRejection>,
) -> Result<Json<Project>, HttpError> {
    Ok(Json(
        s.auth.rename_project(&p, &id, &body(input)?.name).await?,
    ))
}
pub(super) async fn delete_project(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
    params: Result<Query<DeleteParams>, QueryRejection>,
) -> Result<Json<Value>, HttpError> {
    let Query(params) = params.map_err(|_| ApiError::bad("invalid delete parameters"))?;
    s.auth.get_project(&p, &id).await?;
    if id == bf_auth::DEFAULT_PROJECT {
        return Err(ApiError::bad("the default project is built in and can't be deleted").into());
    }
    let engine = s.projects.get(&id);
    let mut remaining = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = engine.list_namespaces("", cursor.as_deref(), 1000).await?;
        remaining.extend(
            page["namespaces"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|n| n["id"].as_str().map(str::to_owned)),
        );
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    if !remaining.is_empty() {
        if !params.force {
            return Err(ApiError::new(
                409,
                format!(
                    "project has {} namespace(s); delete them first, or pass ?force=true",
                    remaining.len()
                ),
            )
            .into());
        }
        for ns in &remaining {
            engine.delete_namespace(ns).await?;
        }
    }
    s.auth.delete_project(&p, &id).await?;
    s.projects.remove(&id);
    Ok(Json(json!({"status":"OK"})))
}

pub(super) async fn list_project_keys(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<bf_auth::ListKeysResponse>, HttpError> {
    Ok(Json(bf_auth::ListKeysResponse {
        keys: s.auth.list_project_keys(&p, &id).await?,
    }))
}
pub(super) async fn create_project_key(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path(id): Path<String>,
    input: Result<Json<CreateKindKeyRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CreateKeyResponse>), HttpError> {
    let (key, token) = s.auth.create_project_key(&p, &id, body(input)?).await?;
    Ok((StatusCode::CREATED, Json(CreateKeyResponse { key, token })))
}
pub(super) async fn get_project_key(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path((id, key_id)): Path<(String, String)>,
) -> Result<Json<KeyInfo>, HttpError> {
    Ok(Json(s.auth.get_project_key(&p, &id, &key_id).await?))
}
pub(super) async fn revoke_project_key(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path((id, key_id)): Path<(String, String)>,
) -> Result<Json<Value>, HttpError> {
    s.auth.revoke_project_key(&p, &id, &key_id).await?;
    Ok(Json(json!({"status":"OK"})))
}

pub(super) async fn list_org_keys(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
) -> Result<Json<bf_auth::ListKeysResponse>, HttpError> {
    Ok(Json(bf_auth::ListKeysResponse {
        keys: s.auth.list_org_keys(&p).await?,
    }))
}
pub(super) async fn create_org_key(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    input: Result<Json<CreateKindKeyRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CreateKeyResponse>), HttpError> {
    let (key, token) = s.auth.create_org_key(&p, body(input)?).await?;
    Ok((StatusCode::CREATED, Json(CreateKeyResponse { key, token })))
}
pub(super) async fn get_org_key(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path(key_id): Path<String>,
) -> Result<Json<KeyInfo>, HttpError> {
    Ok(Json(s.auth.get_org_key(&p, &key_id).await?))
}
pub(super) async fn revoke_org_key(
    State(s): State<AppState>,
    Extension(p): Extension<Principal>,
    Path(key_id): Path<String>,
) -> Result<Json<Value>, HttpError> {
    s.auth.revoke_org_key(&p, &key_id).await?;
    Ok(Json(json!({"status":"OK"})))
}
