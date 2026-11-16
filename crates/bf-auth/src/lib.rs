//! API key authentication and authorization backed by immutable objects.
//!
//! Middleware should call [`KeyStore::authenticate`] for each request with the bearer token, then
//! [`KeyStore::authorize`] for the endpoint and namespace. For namespace listing,
//! filter each result with [`Principal::allows_namespace`]. Callers must check
//! both source and destination namespaces for operations involving two namespaces.

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, RwLock};

/// Permissions that can be attached to a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Admin,
    Write,
    Read,
    List,
    Metrics,
    Keys,
}

/// What a key is for. Keys with no kind are legacy role-and-prefix keys, which act in the
/// `default` project.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyKind {
    /// Org superadmin: every project and every key.
    Superadmin,
    /// Project admin: all namespace operations, and `read_write` and `read` keys of its project.
    Admin,
    /// Project writer: writes, schema updates, and queries.
    ReadWrite,
    /// Project reader: queries and metadata.
    Read,
}
impl KeyKind {
    /// The wire name, such as `read_write`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Superadmin => "superadmin",
            Self::Admin => "admin",
            Self::ReadWrite => "read_write",
            Self::Read => "read",
        }
    }
    fn roles(self) -> Vec<Role> {
        match self {
            Self::Superadmin => vec![],
            Self::Admin => vec![Role::Admin],
            Self::ReadWrite => vec![Role::Write],
            Self::Read => vec![Role::Read],
        }
    }
}
/// The built-in project that maps to the storage root, where pre-project data lives.
pub const DEFAULT_PROJECT: &str = "default";
/// A project: its own namespaces and its own keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}
/// Body for `POST /_blowfish/v1/projects`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateProjectRequest {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub created_by: Option<String>,
}
/// Body for `PATCH /_blowfish/v1/projects/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateProjectRequest {
    pub name: String,
}
/// Body for creating a project key or an org superadmin key. Org keys ignore `kind`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateKindKeyRequest {
    pub name: String,
    #[serde(default)]
    pub kind: Option<KeyKind>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    /// Attribution label, such as `webapp:alice`. Honored only from superadmin callers.
    #[serde(default)]
    pub created_by: Option<String>,
}
/// Response to `GET /_blowfish/v1/whoami`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WhoAmI {
    /// `superadmin`, `admin`, `read_write`, `read`, or `legacy`.
    pub kind: String,
    /// The key's project, or `null` for a superadmin (who selects one per request).
    pub project: Option<String>,
    pub key_id: String,
    pub name: String,
    pub expires_at: Option<DateTime<Utc>>,
}

/// An operation enforced by the HTTP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Write,
    Query,
    ReadMetadata,
    ReadSchema,
    WriteSchema,
    DeleteNamespace,
    ListNamespaces,
    ReadMetrics,
    ManageKeys,
    WarmCache,
    Recall,
    ExplainQuery,
}

impl Role {
    /// Whether this role grants an action, before namespace scope checks.
    pub fn allows(self, action: Action) -> bool {
        use Action::*;
        match self {
            Self::Admin => !matches!(action, ReadMetrics | ManageKeys),
            Self::Write => matches!(
                action,
                Write
                    | Query
                    | ReadMetadata
                    | ReadSchema
                    | WriteSchema
                    | WarmCache
                    | Recall
                    | ExplainQuery
            ),
            Self::Read => matches!(
                action,
                Query | ReadMetadata | ReadSchema | WarmCache | Recall | ExplainQuery
            ),
            Self::List => matches!(action, ListNamespaces),
            Self::Metrics => matches!(action, ReadMetrics),
            Self::Keys => matches!(action, ManageKeys),
        }
    }
}

/// A key's public metadata. No secret or secret hash is exposed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeyInfo {
    pub id: String,
    pub name: String,
    pub roles: Vec<Role>,
    /// `None` means all namespaces; an empty list grants none.
    pub namespace_prefixes: Option<Vec<String>>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_by: String,
    /// `None` for legacy keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<KeyKind>,
    /// The project a project key belongs to; `None` for legacy and superadmin keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Most recent use observed by this process, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<DateTime<Utc>>,
}

/// Body for `POST /_blowfish/v1/keys`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateKeyRequest {
    pub name: String,
    pub roles: Vec<Role>,
    #[serde(default)]
    pub namespace_prefixes: Option<Vec<String>>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}
/// Response to key creation; `token` is returned only once.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKeyResponse {
    pub key: KeyInfo,
    pub token: String,
}
/// Response to listing keys.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListKeysResponse {
    pub keys: Vec<KeyInfo>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredKey {
    info: KeyInfo,
    secret_hash: [u8; 32],
}

/// An authenticated identity, including the namespace restrictions to enforce.
#[derive(Debug, Clone)]
pub struct Principal {
    id: String,
    name: String,
    roles: Vec<Role>,
    namespace_prefixes: Option<Vec<String>>,
    expires_at: Option<DateTime<Utc>>,
    super_key: bool,
    kind: Option<KeyKind>,
    project: Option<String>,
}
impl Principal {
    /// Display name used in HTTP metrics.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Unrestricted identity for explicitly auth-disabled development servers.
    pub fn development() -> Self {
        Self {
            id: "development".into(),
            name: "development".into(),
            roles: vec![],
            namespace_prefixes: None,
            expires_at: None,
            super_key: true,
            kind: Some(KeyKind::Superadmin),
            project: None,
        }
    }
    /// Whether this is an org superadmin (including the break-glass key).
    pub fn is_superadmin(&self) -> bool {
        self.super_key
    }
    /// The key's kind; `None` for legacy keys.
    pub fn kind(&self) -> Option<KeyKind> {
        self.kind
    }
    /// The project this key belongs to; `None` for superadmin and legacy keys.
    pub fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }
    /// Describe this identity for `whoami`.
    pub fn whoami(&self) -> WhoAmI {
        WhoAmI {
            kind: self.kind.map_or("legacy", KeyKind::as_str).into(),
            project: match self.kind {
                Some(KeyKind::Superadmin) => None,
                _ => Some(
                    self.project
                        .clone()
                        .unwrap_or_else(|| DEFAULT_PROJECT.into()),
                ),
            },
            key_id: self.id.clone(),
            name: self.name.clone(),
            expires_at: self.expires_at,
        }
    }
    /// Stable identifier, or `super` for the configured super key.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Namespace prefixes, or `None` when unrestricted.
    pub fn namespace_prefixes(&self) -> Option<&[String]> {
        self.namespace_prefixes.as_deref()
    }
    /// Whether this principal's scope includes a namespace.
    pub fn allows_namespace(&self, namespace: &str) -> bool {
        self.super_key
            || self
                .namespace_prefixes
                .as_ref()
                .is_none_or(|prefixes| prefixes.iter().any(|p| namespace.starts_with(p)))
    }
}

/// Authentication, authorization, validation, or storage failure.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing or invalid API key")]
    Unauthorized,
    #[error("insufficient role or namespace scope")]
    Forbidden,
    #[error("invalid key request: {0}")]
    InvalidRequest(&'static str),
    #[error("key not found")]
    NotFound,
    #[error("project not found")]
    ProjectNotFound,
    #[error("project already exists")]
    Conflict,
    #[error("key authorization data is temporarily unavailable")]
    Unavailable,
    #[error("key storage error")]
    Storage(#[source] object_store::Error),
    #[error("key storage error")]
    SharedStorage(#[from] bf_storage::Error),
    #[error("invalid key data")]
    Data(#[source] serde_json::Error),
}
impl AuthError {
    /// HTTP status code suitable for the turbopuffer error envelope.
    pub fn status_code(&self) -> u16 {
        match self {
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::InvalidRequest(_) => 400,
            Self::NotFound | Self::ProjectNotFound => 404,
            Self::Conflict => 409,
            Self::Unavailable => 503,
            Self::SharedStorage(bf_storage::Error::ReadOnly) => 403,
            Self::Storage(_) | Self::Data(_) | Self::SharedStorage(_) => 500,
        }
    }
    /// JSON-compatible error envelope.
    pub fn response(&self) -> ErrorResponse {
        ErrorResponse {
            status: "error".into(),
            error: self.to_string(),
        }
    }
}
/// Standard turbopuffer-style error body.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub status: String,
    pub error: String,
}

#[derive(Default)]
struct Cache {
    keys: HashMap<String, StoredKey>,
    projects: HashMap<String, Project>,
    revoked: HashSet<String>,
    negative: HashMap<String, Instant>,
    last_used: HashMap<String, DateTime<Utc>>,
    refreshed_at: Option<Instant>,
}
enum Backend {
    Object(Arc<dyn ObjectStore>),
    Shared(bf_storage::Storage),
}
impl Backend {
    async fn list(&self, prefix: &Path) -> Result<Vec<Path>, AuthError> {
        match self {
            Self::Object(store) => Ok(store
                .list(Some(prefix))
                .try_collect::<Vec<_>>()
                .await
                .map_err(AuthError::Storage)?
                .into_iter()
                .map(|o| o.location)
                .collect()),
            Self::Shared(store) => Ok(store
                .list(prefix.as_ref())
                .await?
                .into_iter()
                .map(|o| Path::from(o.key))
                .collect()),
        }
    }
    async fn get(&self, path: &Path) -> Result<bytes::Bytes, AuthError> {
        match self {
            Self::Object(store) => store
                .get(path)
                .await
                .map_err(AuthError::Storage)?
                .bytes()
                .await
                .map_err(AuthError::Storage),
            Self::Shared(store) => Ok(store.get(path.as_ref()).await?),
        }
    }
    async fn put(&self, path: &Path, bytes: Vec<u8>) -> Result<(), AuthError> {
        match self {
            Self::Object(store) => {
                store
                    .put(path, bytes.into())
                    .await
                    .map_err(AuthError::Storage)?;
                Ok(())
            }
            Self::Shared(store) => Ok(store.put(path.as_ref(), bytes).await?),
        }
    }
    async fn delete(&self, path: &Path) -> Result<(), AuthError> {
        match self {
            Self::Object(store) => match store.delete(path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(AuthError::Storage(e)),
            },
            Self::Shared(store) => Ok(store.delete(path.as_ref()).await?),
        }
    }
    async fn create(&self, path: &Path, bytes: Vec<u8>) -> Result<(), AuthError> {
        match self {
            Self::Object(store) => match store
                .put_opts(
                    path,
                    bytes.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_)
                | Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                ) => Ok(()),
                Err(e) => Err(AuthError::Storage(e)),
            },
            Self::Shared(store) => {
                store.create(path.as_ref(), bytes).await?;
                Ok(())
            }
        }
    }
}
struct Inner {
    store: Backend,
    base: String,
    projects_base: String,
    super_hash: Option<[u8; 32]>,
    interval: Duration,
    cache: RwLock<Cache>,
    mutation: Mutex<()>,
}
/// Shared key store. Dropping all clones stops its background refresh task.
#[derive(Clone)]
pub struct KeyStore {
    inner: Arc<Inner>,
}
impl KeyStore {
    /// Load keys and start periodic refresh. Requires a Tokio runtime and a nonzero interval.
    pub async fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        super_key: Option<&str>,
        interval: Duration,
    ) -> Result<Self, AuthError> {
        Self::open(Backend::Object(store), prefix, super_key, interval).await
    }
    /// Use the server's already-probed storage, including its prefix and write protections.
    pub async fn from_storage(
        store: bf_storage::Storage,
        super_key: Option<&str>,
        interval: Duration,
    ) -> Result<Self, AuthError> {
        Self::open(Backend::Shared(store), "", super_key, interval).await
    }
    async fn open(
        store: Backend,
        prefix: &str,
        super_key: Option<&str>,
        interval: Duration,
    ) -> Result<Self, AuthError> {
        if interval.is_zero() {
            return Err(AuthError::InvalidRequest(
                "refresh interval must be nonzero",
            ));
        }
        let prefix = prefix.trim_matches('/');
        let (base, projects_base) = if prefix.is_empty() {
            ("_system/keys".to_string(), "_system/projects".to_string())
        } else {
            (
                format!("{prefix}/_system/keys"),
                format!("{prefix}/_system/projects"),
            )
        };
        let inner = Arc::new(Inner {
            store,
            base,
            projects_base,
            super_hash: super_key.filter(|s| !s.is_empty()).map(hash),
            interval,
            cache: RwLock::new(Cache::default()),
            mutation: Mutex::new(()),
        });
        let this = Self { inner };
        this.refresh().await?;
        tokio::spawn(refresh_loop(Arc::downgrade(&this.inner), interval));
        Ok(this)
    }
    fn path(&self, id: &str, ext: &str) -> Path {
        Path::from(format!("{}/{}.{}", self.inner.base, id, ext))
    }
    /// Reload the key set from storage. Failed reloads leave the previous cache intact.
    pub async fn refresh(&self) -> Result<(), AuthError> {
        let _guard = self.inner.mutation.lock().await;
        let prefix = Path::from(self.inner.base.clone());
        let objects = self.inner.store.list(&prefix).await?;
        let mut keys = HashMap::new();
        let mut revoked = HashSet::new();
        for obj in objects {
            let Some(file) = obj.filename() else {
                continue;
            };
            if let Some(id) = file.strip_suffix(".revoked") {
                if valid_id(id) {
                    revoked.insert(id.to_owned());
                }
            } else if let Some(id) = file.strip_suffix(".json") {
                if !valid_id(id) {
                    continue;
                }
                let bytes = self.inner.store.get(&obj).await?;
                let key: StoredKey = serde_json::from_slice(&bytes).map_err(AuthError::Data)?;
                if key.info.id != id {
                    return Err(AuthError::InvalidRequest("stored key ID mismatch"));
                }
                keys.insert(id.to_owned(), key);
            }
        }
        let mut projects = HashMap::new();
        for obj in self
            .inner
            .store
            .list(&Path::from(self.inner.projects_base.clone()))
            .await?
        {
            let Some(id) = obj.filename().and_then(|f| f.strip_suffix(".json")) else {
                continue;
            };
            if !valid_project_id(id) {
                continue;
            }
            let bytes = self.inner.store.get(&obj).await?;
            let project: Project = serde_json::from_slice(&bytes).map_err(AuthError::Data)?;
            if project.id == id {
                projects.insert(id.to_owned(), project);
            }
        }
        let mut cache = self.inner.cache.write().await;
        cache.keys = keys;
        cache.projects = projects;
        cache.revoked = revoked;
        cache.negative.clear();
        cache.refreshed_at = Some(Instant::now());
        Ok(())
    }
    /// Authenticate a raw bearer token. Pass an empty string for a missing token.
    pub async fn authenticate(&self, bearer: &str) -> Result<Principal, AuthError> {
        if let Some(expected) = self.inner.super_hash
            && bool::from(hash(bearer).ct_eq(&expected))
        {
            return Ok(Principal {
                id: "super".into(),
                name: "super".into(),
                roles: vec![],
                namespace_prefixes: None,
                expires_at: None,
                super_key: true,
                kind: Some(KeyKind::Superadmin),
                project: None,
            });
        }
        let (id, secret) = parse_token(bearer).ok_or(AuthError::Unauthorized)?;
        let mut cache = self.inner.cache.write().await;
        // A failed refresh must not preserve a revoked credential indefinitely.
        // Allow one missed refresh; the configured super key remains a recovery path.
        if cache
            .refreshed_at
            .is_none_or(|at| at.elapsed() > self.inner.interval.saturating_mul(2))
        {
            return Err(AuthError::Unavailable);
        }
        if cache.revoked.contains(id) {
            return Err(AuthError::Unauthorized);
        }
        if cache
            .negative
            .get(id)
            .is_some_and(|at| at.elapsed() < self.inner.interval)
        {
            return Err(AuthError::Unauthorized);
        }
        let Some(key) = cache.keys.get(id) else {
            cache
                .negative
                .retain(|_, at| at.elapsed() < self.inner.interval);
            if cache.negative.len() < 1024 {
                cache.negative.insert(id.to_owned(), Instant::now());
            }
            return Err(AuthError::Unauthorized);
        };
        if key.info.expires_at.is_some_and(|t| t <= Utc::now())
            || !bool::from(hash(secret).ct_eq(&key.secret_hash))
        {
            return Err(AuthError::Unauthorized);
        }
        if let Some(project) = &key.info.project
            && project != DEFAULT_PROJECT
            && !cache.projects.contains_key(project)
        {
            return Err(AuthError::Unauthorized);
        }
        let principal = Principal {
            id: id.to_owned(),
            name: key.info.name.clone(),
            roles: key.info.roles.clone(),
            namespace_prefixes: key.info.namespace_prefixes.clone(),
            expires_at: key.info.expires_at,
            super_key: key.info.kind == Some(KeyKind::Superadmin),
            kind: key.info.kind,
            project: key.info.project.clone(),
        };
        cache.last_used.insert(id.to_owned(), Utc::now());
        Ok(principal)
    }
    /// Enforce an action and, for namespace actions, the namespace scope.
    pub fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        namespace: Option<&str>,
    ) -> Result<(), AuthError> {
        if principal.expires_at.is_some_and(|t| t <= Utc::now()) {
            return Err(AuthError::Unauthorized);
        }
        let scoped = matches!(
            action,
            Action::Write
                | Action::Query
                | Action::ReadMetadata
                | Action::ReadSchema
                | Action::WriteSchema
                | Action::DeleteNamespace
                | Action::WarmCache
                | Action::Recall
                | Action::ExplainQuery
        );
        if scoped && namespace.is_none() {
            return Err(AuthError::Forbidden);
        }
        if !principal.super_key && !principal.roles.iter().any(|r| r.allows(action)) {
            return Err(AuthError::Forbidden);
        }
        if scoped && !principal.allows_namespace(namespace.unwrap_or_default()) {
            return Err(AuthError::Forbidden);
        }
        Ok(())
    }
    /// Create a key without exceeding the creator's roles or namespace scope.
    pub async fn create_key(
        &self,
        spec: CreateKeyRequest,
        creator: &Principal,
    ) -> Result<(KeyInfo, String), AuthError> {
        self.authorize(creator, Action::ManageKeys, None)?;
        if spec.name.trim().is_empty() || spec.roles.is_empty() {
            return Err(AuthError::InvalidRequest("name and roles are required"));
        }
        if spec.expires_at.is_some_and(|t| t <= Utc::now()) {
            return Err(AuthError::InvalidRequest("expiry must be in the future"));
        }
        if !creator.super_key {
            if let Some(expiry) = creator.expires_at
                && spec.expires_at.is_none_or(|child| child > expiry)
            {
                return Err(AuthError::Forbidden);
            }
            if spec.roles.iter().any(|role| !creator.roles.contains(role)) {
                return Err(AuthError::Forbidden);
            }
            if let Some(parent) = &creator.namespace_prefixes {
                let Some(child) = &spec.namespace_prefixes else {
                    return Err(AuthError::Forbidden);
                };
                if child
                    .iter()
                    .any(|c| !parent.iter().any(|p| c.starts_with(p)))
                {
                    return Err(AuthError::Forbidden);
                }
            }
        }
        let _guard = self.inner.mutation.lock().await;
        let id = random_hex(16);
        let secret = random_hex(32);
        let info = KeyInfo {
            id: id.clone(),
            name: spec.name,
            roles: spec.roles,
            namespace_prefixes: spec.namespace_prefixes,
            created_at: Utc::now(),
            expires_at: spec.expires_at,
            created_by: creator.id.clone(),
            kind: None,
            project: None,
            last_used_at: None,
        };
        let key = StoredKey {
            info: info.clone(),
            secret_hash: hash(&secret),
        };
        let bytes = serde_json::to_vec(&key).map_err(AuthError::Data)?;
        self.inner
            .store
            .create(&self.path(&id, "json"), bytes)
            .await?;
        let mut cache = self.inner.cache.write().await;
        cache.negative.remove(&id);
        cache.keys.insert(id.clone(), key);
        Ok((info, format!("bf_{id}_{secret}")))
    }
    // Key management must obey the same authority boundary as delegation. Otherwise
    // a tenant's key manager can enumerate and revoke another tenant's keys.
    fn can_manage(principal: &Principal, key: &KeyInfo) -> bool {
        principal.super_key
            || (key.roles.iter().all(|role| principal.roles.contains(role))
                && principal.namespace_prefixes.as_ref().is_none_or(|parent| {
                    key.namespace_prefixes.as_ref().is_some_and(|child| {
                        child
                            .iter()
                            .all(|c| parent.iter().any(|p| c.starts_with(p)))
                    })
                })
                && principal
                    .expires_at
                    .is_none_or(|expiry| key.expires_at.is_some_and(|child| child <= expiry)))
    }
    /// Return active key metadata; usage timestamps are local to this process.
    pub async fn list_keys(&self, principal: &Principal) -> Result<Vec<KeyInfo>, AuthError> {
        self.authorize(principal, Action::ManageKeys, None)?;
        let cache = self.inner.cache.read().await;
        let mut result: Vec<_> = cache
            .keys
            .values()
            .filter(|k| {
                k.info.kind.is_none()
                    && !cache.revoked.contains(&k.info.id)
                    && Self::can_manage(principal, &k.info)
            })
            .map(|k| {
                let mut info = k.info.clone();
                info.last_used_at = cache.last_used.get(&info.id).copied();
                info
            })
            .collect();
        result.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(result)
    }
    /// Return active key metadata by ID.
    pub async fn get_key(&self, principal: &Principal, id: &str) -> Result<KeyInfo, AuthError> {
        self.list_keys(principal)
            .await?
            .into_iter()
            .find(|k| k.id == id)
            .ok_or(AuthError::NotFound)
    }
    /// Revoke a key immediately on this node and on other nodes after refresh.
    pub async fn revoke_key(&self, principal: &Principal, id: &str) -> Result<(), AuthError> {
        self.authorize(principal, Action::ManageKeys, None)?;
        if !valid_id(id) {
            return Err(AuthError::NotFound);
        }
        let _guard = self.inner.mutation.lock().await;
        {
            let cache = self.inner.cache.read().await;
            if cache.revoked.contains(id)
                || cache.keys.get(id).is_none_or(|key| {
                    key.info.kind.is_some() || !Self::can_manage(principal, &key.info)
                })
            {
                return Err(AuthError::NotFound);
            }
        }
        self.inner
            .store
            .create(&self.path(id, "revoked"), Vec::new())
            .await?;
        self.inner.cache.write().await.revoked.insert(id.to_owned());
        Ok(())
    }
}
/// Projects, project keys, and org superadmin keys.
impl KeyStore {
    fn project_path(&self, id: &str) -> Path {
        Path::from(format!("{}/{id}.json", self.inner.projects_base))
    }
    fn require_superadmin(principal: &Principal) -> Result<(), AuthError> {
        if principal.expires_at.is_some_and(|t| t <= Utc::now()) {
            return Err(AuthError::Unauthorized);
        }
        if principal.super_key {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }
    /// Which project a request acts on. A superadmin picks one with the `X-Blowfish-Project`
    /// header (default: `default`); every other key is pinned to its own project, and a header
    /// naming another project is a 404, so it can't be used to probe for projects.
    pub async fn effective_project(
        &self,
        principal: &Principal,
        header: Option<&str>,
    ) -> Result<String, AuthError> {
        if principal.super_key {
            let project = header.unwrap_or(DEFAULT_PROJECT);
            if project != DEFAULT_PROJECT
                && !self.inner.cache.read().await.projects.contains_key(project)
            {
                return Err(AuthError::ProjectNotFound);
            }
            return Ok(project.to_owned());
        }
        let own = principal.project.as_deref().unwrap_or(DEFAULT_PROJECT);
        match header {
            Some(h) if h != own => Err(AuthError::ProjectNotFound),
            _ => Ok(own.to_owned()),
        }
    }
    /// IDs of every project, `default` included. Not an authorization check.
    pub async fn project_ids(&self) -> Result<Vec<String>, AuthError> {
        let cache = self.inner.cache.read().await;
        let mut ids: Vec<_> = cache.projects.keys().cloned().collect();
        ids.push(DEFAULT_PROJECT.into());
        Ok(ids)
    }
    /// The built-in `default` project, which has no stored record.
    fn default_project() -> Project {
        Project {
            id: DEFAULT_PROJECT.into(),
            name: "Default".into(),
            created_at: DateTime::UNIX_EPOCH,
            created_by: "system".into(),
        }
    }
    /// All projects, with `default` first. Superadmin only.
    pub async fn list_projects(&self, principal: &Principal) -> Result<Vec<Project>, AuthError> {
        Self::require_superadmin(principal)?;
        let cache = self.inner.cache.read().await;
        let mut rest: Vec<_> = cache.projects.values().cloned().collect();
        rest.sort_by(|a, b| a.id.cmp(&b.id));
        let mut all = vec![Self::default_project()];
        all.extend(rest);
        Ok(all)
    }
    /// One project. Superadmin only.
    pub async fn get_project(&self, principal: &Principal, id: &str) -> Result<Project, AuthError> {
        Self::require_superadmin(principal)?;
        if id == DEFAULT_PROJECT {
            return Ok(Self::default_project());
        }
        self.inner
            .cache
            .read()
            .await
            .projects
            .get(id)
            .cloned()
            .ok_or(AuthError::ProjectNotFound)
    }
    /// Create a project. Superadmin only.
    pub async fn create_project(
        &self,
        principal: &Principal,
        spec: CreateProjectRequest,
    ) -> Result<Project, AuthError> {
        Self::require_superadmin(principal)?;
        if !valid_project_id(&spec.id) {
            return Err(AuthError::InvalidRequest(
                "project id must be 1 to 40 characters of a-z, 0-9, and -",
            ));
        }
        validate_name(&spec.name)?;
        let _guard = self.inner.mutation.lock().await;
        if spec.id == DEFAULT_PROJECT
            || self
                .inner
                .cache
                .read()
                .await
                .projects
                .contains_key(&spec.id)
        {
            return Err(AuthError::Conflict);
        }
        let project = Project {
            id: spec.id,
            name: spec.name.trim().to_owned(),
            created_at: Utc::now(),
            created_by: attribution(principal, spec.created_by),
        };
        let bytes = serde_json::to_vec(&project).map_err(AuthError::Data)?;
        // Creation must not overwrite a concurrent creation on another node.
        let path = self.project_path(&project.id);
        if self.inner.store.get(&path).await.is_ok() {
            return Err(AuthError::Conflict);
        }
        self.inner.store.create(&path, bytes).await?;
        self.inner
            .cache
            .write()
            .await
            .projects
            .insert(project.id.clone(), project.clone());
        Ok(project)
    }
    /// Rename a project. Superadmin only; `default` is built in and can't be renamed.
    pub async fn rename_project(
        &self,
        principal: &Principal,
        id: &str,
        name: &str,
    ) -> Result<Project, AuthError> {
        Self::require_superadmin(principal)?;
        validate_name(name)?;
        let _guard = self.inner.mutation.lock().await;
        let mut project = {
            let cache = self.inner.cache.read().await;
            if id == DEFAULT_PROJECT {
                return Err(AuthError::InvalidRequest(
                    "the default project is built in and can't be renamed",
                ));
            }
            cache
                .projects
                .get(id)
                .cloned()
                .ok_or(AuthError::ProjectNotFound)?
        };
        project.name = name.trim().to_owned();
        let bytes = serde_json::to_vec(&project).map_err(AuthError::Data)?;
        self.inner.store.put(&self.project_path(id), bytes).await?;
        self.inner
            .cache
            .write()
            .await
            .projects
            .insert(id.to_owned(), project.clone());
        Ok(project)
    }
    /// Remove a project's record and revoke all of its keys. The caller removes its namespaces
    /// first. Superadmin only; `default` can't be deleted.
    pub async fn delete_project(&self, principal: &Principal, id: &str) -> Result<(), AuthError> {
        Self::require_superadmin(principal)?;
        if id == DEFAULT_PROJECT {
            return Err(AuthError::InvalidRequest(
                "the default project is built in and can't be deleted",
            ));
        }
        let _guard = self.inner.mutation.lock().await;
        let key_ids: Vec<String> = {
            let cache = self.inner.cache.read().await;
            if !cache.projects.contains_key(id) {
                return Err(AuthError::ProjectNotFound);
            }
            cache
                .keys
                .values()
                .filter(|k| {
                    k.info.project.as_deref() == Some(id) && !cache.revoked.contains(&k.info.id)
                })
                .map(|k| k.info.id.clone())
                .collect()
        };
        for key in &key_ids {
            self.inner
                .store
                .create(&self.path(key, "revoked"), Vec::new())
                .await?;
        }
        self.inner.store.delete(&self.project_path(id)).await?;
        let mut cache = self.inner.cache.write().await;
        cache.projects.remove(id);
        cache.revoked.extend(key_ids);
        Ok(())
    }
    /// Whether a project exists (`default` always does).
    pub async fn project_exists(&self, id: &str) -> bool {
        id == DEFAULT_PROJECT || self.inner.cache.read().await.projects.contains_key(id)
    }
    /// Check that `principal` may manage keys of `project`, returning whether it is a superadmin.
    /// Other projects' admins get a 404, so project IDs can't be probed.
    async fn project_key_scope(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<bool, AuthError> {
        if principal.expires_at.is_some_and(|t| t <= Utc::now()) {
            return Err(AuthError::Unauthorized);
        }
        if principal.super_key {
            if !self.project_exists(project).await {
                return Err(AuthError::ProjectNotFound);
            }
            return Ok(true);
        }
        match (principal.kind, principal.project.as_deref()) {
            (Some(_), Some(own)) if own != project => Err(AuthError::ProjectNotFound),
            (Some(KeyKind::Admin), Some(_)) => Ok(false),
            _ => Err(AuthError::Forbidden),
        }
    }
    fn visible_to_project_admin(info: &KeyInfo) -> bool {
        matches!(info.kind, Some(KeyKind::ReadWrite | KeyKind::Read))
    }
    async fn insert_key(
        &self,
        name: &str,
        kind: KeyKind,
        project: Option<String>,
        expires_at: Option<DateTime<Utc>>,
        created_by: String,
    ) -> Result<(KeyInfo, String), AuthError> {
        validate_name(name)?;
        let _guard = self.inner.mutation.lock().await;
        let id = random_hex(16);
        let secret = random_hex(32);
        let info = KeyInfo {
            id: id.clone(),
            name: name.trim().to_owned(),
            roles: kind.roles(),
            namespace_prefixes: None,
            created_at: Utc::now(),
            expires_at,
            created_by,
            kind: Some(kind),
            project,
            last_used_at: None,
        };
        let key = StoredKey {
            info: info.clone(),
            secret_hash: hash(&secret),
        };
        let bytes = serde_json::to_vec(&key).map_err(AuthError::Data)?;
        self.inner
            .store
            .create(&self.path(&id, "json"), bytes)
            .await?;
        let mut cache = self.inner.cache.write().await;
        cache.negative.remove(&id);
        cache.keys.insert(id.clone(), key);
        Ok((info, format!("bf_{id}_{secret}")))
    }
    /// Create a key in a project. A superadmin can create any kind but `superadmin`; a
    /// project admin can create only `read_write` and `read` keys, in its own project.
    pub async fn create_project_key(
        &self,
        principal: &Principal,
        project: &str,
        spec: CreateKindKeyRequest,
    ) -> Result<(KeyInfo, String), AuthError> {
        let superadmin = self.project_key_scope(principal, project).await?;
        let kind = spec
            .kind
            .ok_or(AuthError::InvalidRequest("kind is required"))?;
        match kind {
            KeyKind::Superadmin => {
                return Err(AuthError::InvalidRequest(
                    "superadmin keys belong to the org: use org/keys",
                ));
            }
            KeyKind::Admin if !superadmin => return Err(AuthError::Forbidden),
            _ => {}
        }
        check_expiry(principal, spec.expires_at)?;
        let label = if superadmin { spec.created_by } else { None };
        self.insert_key(
            &spec.name,
            kind,
            Some(project.to_owned()),
            spec.expires_at,
            attribution(principal, label),
        )
        .await
    }
    /// List a project's active keys. Project admins don't see `admin` keys.
    pub async fn list_project_keys(
        &self,
        principal: &Principal,
        project: &str,
    ) -> Result<Vec<KeyInfo>, AuthError> {
        let superadmin = self.project_key_scope(principal, project).await?;
        let cache = self.inner.cache.read().await;
        let mut keys: Vec<_> = cache
            .keys
            .values()
            .filter(|k| !cache.revoked.contains(&k.info.id))
            .filter(|k| in_project(&k.info, project, superadmin))
            .filter(|k| superadmin || Self::visible_to_project_admin(&k.info))
            .map(|k| {
                let mut info = k.info.clone();
                info.last_used_at = cache.last_used.get(&info.id).copied();
                info
            })
            .collect();
        keys.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
        Ok(keys)
    }
    /// One active key of a project.
    pub async fn get_project_key(
        &self,
        principal: &Principal,
        project: &str,
        id: &str,
    ) -> Result<KeyInfo, AuthError> {
        self.list_project_keys(principal, project)
            .await?
            .into_iter()
            .find(|k| k.id == id)
            .ok_or(AuthError::NotFound)
    }
    /// Revoke a key of a project.
    pub async fn revoke_project_key(
        &self,
        principal: &Principal,
        project: &str,
        id: &str,
    ) -> Result<(), AuthError> {
        self.get_project_key(principal, project, id).await?;
        self.revoke_checked(id).await
    }
    async fn revoke_checked(&self, id: &str) -> Result<(), AuthError> {
        let _guard = self.inner.mutation.lock().await;
        if self.inner.cache.read().await.revoked.contains(id) {
            return Err(AuthError::NotFound);
        }
        self.inner
            .store
            .create(&self.path(id, "revoked"), Vec::new())
            .await?;
        self.inner.cache.write().await.revoked.insert(id.to_owned());
        Ok(())
    }
    /// Create an org superadmin key. Superadmin only.
    pub async fn create_org_key(
        &self,
        principal: &Principal,
        spec: CreateKindKeyRequest,
    ) -> Result<(KeyInfo, String), AuthError> {
        Self::require_superadmin(principal)?;
        check_expiry(principal, spec.expires_at)?;
        self.insert_key(
            &spec.name,
            KeyKind::Superadmin,
            None,
            spec.expires_at,
            attribution(principal, spec.created_by),
        )
        .await
    }
    /// List active org superadmin keys. Superadmin only.
    pub async fn list_org_keys(&self, principal: &Principal) -> Result<Vec<KeyInfo>, AuthError> {
        Self::require_superadmin(principal)?;
        let cache = self.inner.cache.read().await;
        let mut keys: Vec<_> = cache
            .keys
            .values()
            .filter(|k| {
                k.info.kind == Some(KeyKind::Superadmin) && !cache.revoked.contains(&k.info.id)
            })
            .map(|k| {
                let mut info = k.info.clone();
                info.last_used_at = cache.last_used.get(&info.id).copied();
                info
            })
            .collect();
        keys.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
        Ok(keys)
    }
    /// One active org superadmin key. Superadmin only.
    pub async fn get_org_key(&self, principal: &Principal, id: &str) -> Result<KeyInfo, AuthError> {
        self.list_org_keys(principal)
            .await?
            .into_iter()
            .find(|k| k.id == id)
            .ok_or(AuthError::NotFound)
    }
    /// Revoke an org superadmin key. Superadmin only.
    pub async fn revoke_org_key(&self, principal: &Principal, id: &str) -> Result<(), AuthError> {
        self.get_org_key(principal, id).await?;
        self.revoke_checked(id).await
    }
}
fn in_project(info: &KeyInfo, project: &str, include_legacy: bool) -> bool {
    match (&info.kind, &info.project) {
        (Some(KeyKind::Superadmin), _) => false,
        (Some(_), Some(p)) => p == project,
        // Legacy keys act in `default`, and only a superadmin manages them here.
        (None, _) => include_legacy && project == DEFAULT_PROJECT,
        (Some(_), None) => false,
    }
}
fn attribution(creator: &Principal, label: Option<String>) -> String {
    match label {
        Some(label) if creator.super_key && !label.trim().is_empty() => {
            label.chars().take(200).collect()
        }
        _ => creator.id.clone(),
    }
}
fn check_expiry(creator: &Principal, expires_at: Option<DateTime<Utc>>) -> Result<(), AuthError> {
    if expires_at.is_some_and(|t| t <= Utc::now()) {
        return Err(AuthError::InvalidRequest("expiry must be in the future"));
    }
    if let Some(parent) = creator.expires_at
        && expires_at.is_none_or(|child| child > parent)
    {
        return Err(AuthError::Forbidden);
    }
    Ok(())
}
fn validate_name(name: &str) -> Result<(), AuthError> {
    if name.trim().is_empty() || name.len() > 200 {
        return Err(AuthError::InvalidRequest(
            "name is required and at most 200 bytes",
        ));
    }
    Ok(())
}
/// Project IDs are slugs: 1 to 40 characters of `a-z`, `0-9`, and `-`.
pub fn valid_project_id(id: &str) -> bool {
    (1..=40).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
async fn refresh_loop(inner: Weak<Inner>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(inner) = inner.upgrade() else { break };
        let _ = (KeyStore { inner }).refresh().await;
    }
}
fn hash(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}
fn random_hex(bytes: usize) -> String {
    let mut data = vec![0; bytes];
    rand::rng().fill_bytes(&mut data);
    data.iter().map(|b| format!("{b:02x}")).collect()
}
fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn parse_token(token: &str) -> Option<(&str, &str)> {
    let (id, secret) = token.strip_prefix("bf_")?.split_once('_')?;
    if valid_id(id)
        && secret.len() == 64
        && secret
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        Some((id, secret))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{local::LocalFileSystem, memory::InMemory};

    fn spec(roles: Vec<Role>, prefixes: Option<Vec<&str>>) -> CreateKeyRequest {
        CreateKeyRequest {
            name: "test".into(),
            roles,
            namespace_prefixes: prefixes.map(|v| v.into_iter().map(String::from).collect()),
            expires_at: None,
        }
    }
    async fn setup(store: Arc<dyn ObjectStore>) -> (KeyStore, Principal) {
        let ks = KeyStore::new(
            store,
            "test",
            Some("super-secret"),
            Duration::from_millis(50),
        )
        .await
        .unwrap();
        let principal = ks.authenticate("super-secret").await.unwrap();
        (ks, principal)
    }
    #[tokio::test]
    async fn shared_storage_prefix_persistence_and_read_only() {
        let backend = Arc::new(InMemory::new());
        let storage = bf_storage::Storage::from_backend(
            backend.clone(),
            Path::from("deployment"),
            false,
            false,
        )
        .await
        .unwrap();
        let keys = KeyStore::from_storage(storage, Some("test-super"), Duration::from_secs(5))
            .await
            .unwrap();
        let principal = keys.authenticate("test-super").await.unwrap();
        let (key, token) = keys
            .create_key(spec(vec![Role::Read], None), &principal)
            .await
            .unwrap();
        let objects = backend
            .list(Some(&Path::from("deployment/_system/keys")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(objects.len(), 1);
        let stored = backend
            .get(&objects[0].location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&stored).contains(&token));
        let readonly =
            bf_storage::Storage::from_backend(backend, Path::from("deployment"), true, false)
                .await
                .unwrap();
        let replica = KeyStore::from_storage(readonly, Some("test-super"), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(replica.authenticate(&token).await.unwrap().name(), "test");
        assert_eq!(
            replica
                .revoke_key(&principal, &key.id)
                .await
                .unwrap_err()
                .status_code(),
            403
        );
        assert_eq!(
            replica
                .create_key(spec(vec![Role::Read], None), &principal)
                .await
                .unwrap_err()
                .status_code(),
            403
        );
        keys.revoke_key(&principal, &key.id).await.unwrap();
        replica.refresh().await.unwrap();
        assert!(matches!(
            replica.authenticate(&token).await,
            Err(AuthError::Unauthorized)
        ));
    }
    #[tokio::test]
    async fn scoped_managers_cannot_inspect_or_revoke_more_powerful_keys() {
        let (ks, root) = setup(Arc::new(InMemory::new())).await;
        let (_, token) = ks
            .create_key(
                spec(vec![Role::Keys, Role::Read], Some(vec!["tenant-"])),
                &root,
            )
            .await
            .unwrap();
        let manager = ks.authenticate(&token).await.unwrap();
        for request in [
            spec(vec![Role::Read], None),
            spec(vec![Role::Read], Some(vec!["other-"])),
            spec(vec![Role::Admin], Some(vec!["tenant-"])),
            spec(vec![Role::Keys], Some(vec![""])),
        ] {
            let (key, token) = ks.create_key(request, &root).await.unwrap();
            assert!(
                !ks.list_keys(&manager)
                    .await
                    .unwrap()
                    .iter()
                    .any(|k| k.id == key.id)
            );
            assert!(matches!(
                ks.get_key(&manager, &key.id).await,
                Err(AuthError::NotFound)
            ));
            assert!(matches!(
                ks.revoke_key(&manager, &key.id).await,
                Err(AuthError::NotFound)
            ));
            assert!(ks.authenticate(&token).await.is_ok());
        }
        let (child, token) = ks
            .create_key(spec(vec![Role::Read], Some(vec!["tenant-child"])), &manager)
            .await
            .unwrap();
        assert!(ks.get_key(&manager, &child.id).await.is_ok());
        ks.revoke_key(&manager, &child.id).await.unwrap();
        assert!(ks.authenticate(&token).await.is_err());
    }

    #[tokio::test]
    async fn failed_refresh_cannot_authorize_stale_keys_indefinitely() {
        let backend = Arc::new(InMemory::new());
        let (ks, root) = setup(backend.clone()).await;
        let (key, token) = ks
            .create_key(spec(vec![Role::Read], None), &root)
            .await
            .unwrap();
        // Simulate unreadable authorization data while another node may revoke a key.
        backend
            .put(
                &ks.path(&key.id, "json"),
                bytes::Bytes::from_static(b"invalid JSON").into(),
            )
            .await
            .unwrap();
        assert!(ks.refresh().await.is_err());
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(matches!(
            ks.authenticate(&token).await,
            Err(AuthError::Unavailable)
        ));
        assert!(ks.authenticate("super-secret").await.is_ok());
    }

    #[test]
    fn role_matrix() {
        use Action::*;
        let cases = [
            (
                Role::Admin,
                vec![
                    Write,
                    Query,
                    ReadMetadata,
                    ReadSchema,
                    WriteSchema,
                    DeleteNamespace,
                    ListNamespaces,
                    WarmCache,
                    Recall,
                    ExplainQuery,
                ],
            ),
            (
                Role::Write,
                vec![
                    Write,
                    Query,
                    ReadMetadata,
                    ReadSchema,
                    WriteSchema,
                    WarmCache,
                    Recall,
                    ExplainQuery,
                ],
            ),
            (
                Role::Read,
                vec![
                    Query,
                    ReadMetadata,
                    ReadSchema,
                    WarmCache,
                    Recall,
                    ExplainQuery,
                ],
            ),
            (Role::List, vec![ListNamespaces]),
            (Role::Metrics, vec![ReadMetrics]),
            (Role::Keys, vec![ManageKeys]),
        ];
        let actions = [
            Write,
            Query,
            ReadMetadata,
            ReadSchema,
            WriteSchema,
            DeleteNamespace,
            ListNamespaces,
            ReadMetrics,
            ManageKeys,
            WarmCache,
            Recall,
            ExplainQuery,
        ];
        for (role, expected) in cases {
            for action in actions {
                assert_eq!(
                    role.allows(action),
                    expected.contains(&action),
                    "{role:?} {action:?}"
                );
            }
        }
    }
    #[tokio::test]
    async fn scopes_delegation_expiry_and_malformed() {
        let (ks, root) = setup(Arc::new(InMemory::new())).await;
        let (_, token) = ks
            .create_key(
                spec(vec![Role::Keys, Role::Read], Some(vec!["team-"])),
                &root,
            )
            .await
            .unwrap();
        let user = ks.authenticate(&token).await.unwrap();
        assert!(ks.authorize(&user, Action::Query, Some("team-a")).is_ok());
        assert!(ks.authorize(&user, Action::Query, Some("other")).is_err());
        assert!(ks.authorize(&user, Action::Query, None).is_err());
        assert!(
            ks.create_key(spec(vec![Role::Read], Some(vec!["team-a"])), &user)
                .await
                .is_ok()
        );
        assert!(matches!(
            ks.create_key(spec(vec![Role::Write], Some(vec!["team-a"])), &user)
                .await,
            Err(AuthError::Forbidden)
        ));
        assert!(matches!(
            ks.create_key(spec(vec![Role::Read], None), &user).await,
            Err(AuthError::Forbidden)
        ));
        assert!(matches!(
            ks.create_key(spec(vec![Role::Read], Some(vec!["team"])), &user)
                .await,
            Err(AuthError::Forbidden)
        ));
        for token in ["", "bf_", "bf_a_b", "Bearer super-secret", "bf_../../x_foo"] {
            assert!(matches!(
                ks.authenticate(token).await,
                Err(AuthError::Unauthorized)
            ));
        }
        let mut expiring = spec(vec![Role::Read], None);
        expiring.expires_at = Some(Utc::now() + chrono::Duration::milliseconds(40));
        let (_, token) = ks.create_key(expiring, &root).await.unwrap();
        assert!(ks.authenticate(&token).await.is_ok());
        tokio::time::sleep(Duration::from_millis(70)).await;
        assert!(matches!(
            ks.authenticate(&token).await,
            Err(AuthError::Unauthorized)
        ));
        let mut parent = spec(vec![Role::Keys, Role::Read], None);
        parent.expires_at = Some(Utc::now() + chrono::Duration::minutes(1));
        let (_, token) = ks.create_key(parent, &root).await.unwrap();
        let parent = ks.authenticate(&token).await.unwrap();
        assert!(matches!(
            ks.create_key(spec(vec![Role::Read], None), &parent).await,
            Err(AuthError::Forbidden)
        ));
    }
    async fn propagation(store: Arc<dyn ObjectStore>) {
        let (a, root) = setup(store.clone()).await;
        let (b, _) = setup(store).await;
        let (info, token) = a
            .create_key(spec(vec![Role::Read], None), &root)
            .await
            .unwrap();
        assert!(a.authenticate(&token).await.is_ok());
        assert!(b.authenticate(&token).await.is_err());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if b.authenticate(&token).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            a.get_key(&root, &info.id)
                .await
                .unwrap()
                .last_used_at
                .is_some()
        );
        a.revoke_key(&root, &info.id).await.unwrap();
        assert!(a.authenticate(&token).await.is_err());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if b.authenticate(&token).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(a.get_key(&root, &info.id).await.is_err());
    }
    #[tokio::test]
    async fn memory_propagation() {
        propagation(Arc::new(InMemory::new())).await;
    }
    #[tokio::test]
    async fn filesystem_propagation() {
        let dir = tempfile::tempdir().unwrap();
        propagation(Arc::new(
            LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        ))
        .await;
    }
}
