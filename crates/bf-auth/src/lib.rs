//! API key authentication and authorization backed by immutable objects.
//!
//! Middleware should call [`KeyStore::authenticate`] for each request with the bearer token, then
//! [`KeyStore::authorize`] for the endpoint and namespace. For namespace listing,
//! filter each result with [`Principal::allows_namespace`]. Callers must check
//! both source and destination namespaces for operations involving two namespaces.

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::{ObjectStore, PutMode, PutOptions, path::Path};
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
    roles: Vec<Role>,
    namespace_prefixes: Option<Vec<String>>,
    expires_at: Option<DateTime<Utc>>,
    super_key: bool,
}
impl Principal {
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
    #[error("key storage error")]
    Storage(#[source] object_store::Error),
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
            Self::NotFound => 404,
            Self::Storage(_) | Self::Data(_) => 500,
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
    revoked: HashSet<String>,
    negative: HashMap<String, Instant>,
    last_used: HashMap<String, DateTime<Utc>>,
}
struct Inner {
    store: Arc<dyn ObjectStore>,
    base: String,
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
        if interval.is_zero() {
            return Err(AuthError::InvalidRequest(
                "refresh interval must be nonzero",
            ));
        }
        let prefix = prefix.trim_matches('/');
        let base = if prefix.is_empty() {
            "_system/keys".to_string()
        } else {
            format!("{prefix}/_system/keys")
        };
        let inner = Arc::new(Inner {
            store,
            base,
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
        let objects = self
            .inner
            .store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(AuthError::Storage)?;
        let mut keys = HashMap::new();
        let mut revoked = HashSet::new();
        for obj in objects {
            let Some(file) = obj.location.filename() else {
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
                let bytes = self
                    .inner
                    .store
                    .get(&obj.location)
                    .await
                    .map_err(AuthError::Storage)?
                    .bytes()
                    .await
                    .map_err(AuthError::Storage)?;
                let key: StoredKey = serde_json::from_slice(&bytes).map_err(AuthError::Data)?;
                if key.info.id != id {
                    return Err(AuthError::InvalidRequest("stored key ID mismatch"));
                }
                keys.insert(id.to_owned(), key);
            }
        }
        let mut cache = self.inner.cache.write().await;
        cache.keys = keys;
        cache.revoked = revoked;
        cache.negative.clear();
        Ok(())
    }
    /// Authenticate a raw bearer token. Pass an empty string for a missing token.
    pub async fn authenticate(&self, bearer: &str) -> Result<Principal, AuthError> {
        if let Some(expected) = self.inner.super_hash
            && bool::from(hash(bearer).ct_eq(&expected))
        {
            return Ok(Principal {
                id: "super".into(),
                roles: vec![],
                namespace_prefixes: None,
                expires_at: None,
                super_key: true,
            });
        }
        let (id, secret) = parse_token(bearer).ok_or(AuthError::Unauthorized)?;
        let mut cache = self.inner.cache.write().await;
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
        let principal = Principal {
            id: id.to_owned(),
            roles: key.info.roles.clone(),
            namespace_prefixes: key.info.namespace_prefixes.clone(),
            expires_at: key.info.expires_at,
            super_key: false,
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
            last_used_at: None,
        };
        let key = StoredKey {
            info: info.clone(),
            secret_hash: hash(&secret),
        };
        let bytes = serde_json::to_vec(&key).map_err(AuthError::Data)?;
        self.inner
            .store
            .put_opts(
                &self.path(&id, "json"),
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(AuthError::Storage)?;
        let mut cache = self.inner.cache.write().await;
        cache.negative.remove(&id);
        cache.keys.insert(id.clone(), key);
        Ok((info, format!("bf_{id}_{secret}")))
    }
    /// Return active key metadata; usage timestamps are local to this process.
    pub async fn list_keys(&self, principal: &Principal) -> Result<Vec<KeyInfo>, AuthError> {
        self.authorize(principal, Action::ManageKeys, None)?;
        let cache = self.inner.cache.read().await;
        let mut result: Vec<_> = cache
            .keys
            .values()
            .filter(|k| !cache.revoked.contains(&k.info.id))
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
            if !cache.keys.contains_key(id) || cache.revoked.contains(id) {
                return Err(AuthError::NotFound);
            }
        }
        match self
            .inner
            .store
            .put_opts(
                &self.path(id, "revoked"),
                Vec::new().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(e) => return Err(AuthError::Storage(e)),
        }
        self.inner.cache.write().await.revoked.insert(id.to_owned());
        Ok(())
    }
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
