#![doc = include_str!("../README.md")]
mod cache;
mod diagnostics;
mod disk;
mod hedge;
pub use diagnostics::{BackendMeasurement, BackendStats};
mod observed;
use bytes::Bytes;
pub use cache::{CacheConfig, CacheStats, CacheStatsSnapshot, RangeReader};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use serde::{Deserialize, Serialize};
use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"BFPUT001";
const HEADER: usize = 24;
/// Safe storage errors. Backend messages are retained as sources, not displayed to HTTP clients.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cache prefetch concurrency limit reached")]
    PrefetchBusy,
    #[error("shared cache fetch failed")]
    Shared(#[source] Arc<Error>),
    #[error("object not found")]
    NotFound,
    #[error("object already exists")]
    AlreadyExists,
    #[error("storage configuration: {0}")]
    Config(&'static str),
    #[error("another live writer holds the storage prefix")]
    WriterActive,
    #[error("storage is read-only or its writer heartbeat failed")]
    ReadOnly,
    #[error("object-store request failed")]
    Backend(#[source] object_store::Error),
    #[error("local storage I/O failed")]
    Io(#[from] std::io::Error),
}
impl Error {
    fn from_shared(error: Arc<Self>) -> Self {
        match error.as_ref() {
            Self::NotFound => Self::NotFound,
            Self::AlreadyExists => Self::AlreadyExists,
            Self::Config(message) => Self::Config(message),
            _ => Self::Shared(error),
        }
    }
}
impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        match e {
            object_store::Error::NotFound { .. } => Self::NotFound,
            object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. } => Self::AlreadyExists,
            e => Self::Backend(e),
        }
    }
}
/// Result of a create-only operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateResult {
    Created,
    AlreadyCommitted,
    Conflict,
}
/// Coordination mode selected by startup probing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageMode {
    Conditional,
    SingleWriter,
}
/// Runtime storage options; credentials may come from environment or an AWS profile.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub url: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub read_only: bool,
    pub single_writer: bool,
    /// Budgets and concurrency limits for explicitly cached immutable reads.
    pub cache: CacheConfig,
    /// Idle object-store connections to keep open (S3 only; 0 disables; default 256).
    /// `BF_WARM_CONNECTIONS` overrides it.
    pub warm_connections: usize,
}
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            endpoint: None,
            region: None,
            profile: None,
            read_only: false,
            single_writer: false,
            cache: CacheConfig::default(),
            warm_connections: 256,
        }
    }
}
struct Inner {
    backend: Arc<dyn ObjectStore>,
    cache: cache::RangeCache,
    prefix: Path,
    mode: StorageMode,
    read_only: bool,
    healthy: AtomicBool,
    writer: Uuid,
    serial: Mutex<()>,
    activity: Arc<observed::Activity>,
}
/// Cloneable store handle. Creates carry a portable body header with a retry identity.
#[derive(Clone)]
pub struct Storage(Arc<Inner>, Arc<str>);
/// Object metadata; size includes the portable conditional-create header, if present.
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
}
#[derive(Serialize, Deserialize)]
struct Heartbeat {
    writer: String,
    updated_at: DateTime<Utc>,
}
impl Storage {
    /// Open a local or S3-compatible URL, probe create semantics, and acquire a fallback heartbeat.
    pub async fn open(config: &StorageConfig) -> Result<Self, Error> {
        let url = url::Url::parse(&config.url).map_err(|_| Error::Config("invalid storage URL"))?;
        let mut namespace = config.url.clone();
        let (backend, prefix): (Arc<dyn ObjectStore>, Path) = match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| Error::Config("invalid file URL"))?;
                std::fs::create_dir_all(&path)?;
                (
                    Arc::new(object_store::local::LocalFileSystem::new_with_prefix(path)?),
                    Path::default(),
                )
            }
            "s3" => {
                let bucket = url
                    .host_str()
                    .ok_or(Error::Config("S3 URL needs a bucket"))?;
                // Parallel ranged reads each need a connection (HTTP/1), and a new TLS connection
                // costs about 65 ms, so keep idle connections for ten minutes instead of 90 seconds.
                let mut b = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_client_options(
                        object_store::ClientOptions::new()
                            .with_pool_idle_timeout(Duration::from_secs(600)),
                    );
                if let Some(endpoint) = &config.endpoint {
                    b = b
                        .with_endpoint(endpoint)
                        .with_allow_http(endpoint.starts_with("http://"));
                }
                if let Some(region) = &config.region {
                    b = b.with_region(region);
                }
                if let Some(profile) = &config.profile {
                    let credentials = profile_credentials(profile)?;
                    b = b
                        .with_access_key_id(&credentials.0)
                        .with_secret_access_key(&credentials.1);
                    if let Some(token) = credentials.2 {
                        b = b.with_token(token);
                    }
                }
                use object_store::aws::AmazonS3ConfigKey;
                namespace = serde_json::to_string(&(
                    &config.url,
                    b.get_config_value(&AmazonS3ConfigKey::S3Endpoint)
                        .or_else(|| b.get_config_value(&AmazonS3ConfigKey::Endpoint)),
                    b.get_config_value(&AmazonS3ConfigKey::Region),
                ))
                .expect("cache identity serializes");
                (
                    Arc::new(b.build()?),
                    Path::from_url_path(url.path())
                        .map_err(|_| Error::Config("invalid S3 prefix"))?,
                )
            }
            "memory" => {
                // Each open creates a different ephemeral backend, even with the same URL.
                namespace = format!("memory:{}", Uuid::new_v4());
                (
                    Arc::new(object_store::memory::InMemory::new()),
                    Path::default(),
                )
            }
            _ => {
                return Err(Error::Config(
                    "supported storage schemes are file, s3, and memory",
                ));
            }
        };
        let mut cache = config.cache.clone();
        if cache.namespace.is_none() {
            cache.namespace = Some(namespace);
        }
        let store = Self::from_backend_with_cache(
            backend,
            prefix,
            config.read_only,
            config.single_writer,
            cache,
        )
        .await?;
        if url.scheme() == "s3" {
            let fixed = std::env::var("BF_WARM_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok());
            let floor = std::env::var("BF_WARM_FLOOR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(16);
            store.spawn_connection_warmer(PoolSizer::new(
                fixed,
                config.warm_connections,
                floor,
                1024,
                Duration::from_secs(600),
            ));
        }
        Ok(store)
    }
    /// Keep idle connections open to the object store: a cold wave of N parallel reads otherwise
    /// pays a TLS handshake per request (measured at about 65 ms each, and about half the wall
    /// time of a 250-request wave). The pool follows the largest recent wave (see `PoolSizer`),
    /// grows within seconds of a bigger wave, and decays to a small floor after an idle period, so
    /// an idle server does not pay for hundreds of keepalive HEADs. Refreshes before the pool's
    /// idle timeout.
    fn spawn_connection_warmer(&self, mut sizer: PoolSizer) {
        if sizer.fixed == Some(0) {
            return;
        }
        let weak = Arc::downgrade(&self.0);
        tokio::spawn(async move {
            let mut warmed = 0usize;
            let mut last_refresh: Option<std::time::Instant> = None;
            loop {
                let Some(inner) = weak.upgrade() else { break };
                let path = Path::from(format!("{}/.warm", inner.prefix));
                let backend = inner.backend.clone();
                let peak = inner.activity.take_peak();
                drop(inner);
                let now = std::time::Instant::now();
                let target = sizer.observe(peak, now);
                let due =
                    last_refresh.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(45));
                if target > 0 && (due || target > warmed) {
                    // HEAD of a missing key answers 404 on a connection that then returns to the pool.
                    futures::future::join_all((0..target).map(|_| backend.head(&path))).await;
                    warmed = target;
                    last_refresh = Some(std::time::Instant::now());
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }
    /// Wrap an object_store backend. Read-only instances do not run a mutating CAS probe.
    pub async fn from_backend(
        backend: Arc<dyn ObjectStore>,
        prefix: Path,
        read_only: bool,
        force_single: bool,
    ) -> Result<Self, Error> {
        Self::from_backend_with_cache(
            backend,
            prefix,
            read_only,
            force_single,
            CacheConfig::default(),
        )
        .await
    }
    /// Wrap a backend with explicit cache budgets. Disk caching requires a stable, unique
    /// `cache.namespace` identifying the bucket/backend; the storage prefix is added here.
    pub async fn from_backend_with_cache(
        backend: Arc<dyn ObjectStore>,
        prefix: Path,
        read_only: bool,
        force_single: bool,
        mut cache: CacheConfig,
    ) -> Result<Self, Error> {
        if let Some(namespace) = &mut cache.namespace {
            *namespace = serde_json::to_string(&(&namespace, prefix.as_ref()))
                .expect("cache identity serializes");
        }
        let cache = cache::RangeCache::open(cache).await?;
        let activity = Arc::new(observed::Activity::default());
        let backend: Arc<dyn ObjectStore> = Arc::new(observed::Observed(backend, activity.clone()));
        let conditional = if read_only {
            true
        } else {
            probe_backend(&*backend, &prefix).await?
        };
        let mode = if conditional && !force_single {
            StorageMode::Conditional
        } else {
            StorageMode::SingleWriter
        };
        let store = Self(
            Arc::new(Inner {
                backend,
                cache,
                prefix,
                mode,
                read_only,
                healthy: AtomicBool::new(true),
                writer: Uuid::new_v4(),
                serial: Mutex::new(()),
                activity,
            }),
            Arc::from(""),
        );
        if mode == StorageMode::SingleWriter && !read_only {
            store.acquire_heartbeat().await?;
            let weak = Arc::downgrade(&store.0);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let Some(inner) = weak.upgrade() else { break };
                    let store = Storage(inner, Arc::from(""));
                    if store.refresh_heartbeat().await.is_err() {
                        store.0.healthy.store(false, Ordering::Release);
                        break;
                    }
                }
            });
        }
        Ok(store)
    }
    /// Selected write coordination mode.
    pub fn mode(&self) -> StorageMode {
        self.0.mode
    }
    /// Whether writes are disabled.
    pub fn is_read_only(&self) -> bool {
        self.0.read_only
    }
    fn path(&self, key: &str) -> Path {
        if self.1.is_empty() {
            Path::from(format!("{}/{key}", self.0.prefix))
        } else {
            Path::from(format!("{}/{}/{key}", self.0.prefix, self.1))
        }
    }
    /// Identity of a key within the shared range cache: unique across scoped views.
    fn cache_key(&self, key: &str) -> String {
        if self.1.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.1)
        }
    }
    /// A view of this store rooted at a sub-prefix, such as `projects/acme`. It shares the
    /// backend, caches, write mode, and health with the original, and sees only keys beneath
    /// the sub-prefix. System state such as the writer heartbeat stays with the root store.
    pub fn scoped(&self, sub: &str) -> Self {
        let sub = sub.trim_matches('/');
        let joined = if self.1.is_empty() {
            sub.to_owned()
        } else {
            format!("{}/{sub}", self.1)
        };
        Self(self.0.clone(), Arc::from(joined))
    }
    fn writable(&self) -> Result<(), Error> {
        if self.0.read_only || !self.0.healthy.load(Ordering::Acquire) {
            Err(Error::ReadOnly)
        } else {
            Ok(())
        }
    }
    /// Read an entire object, removing any portable create header.
    pub async fn get(&self, key: &str) -> Result<Bytes, Error> {
        let bytes = self.0.backend.get(&self.path(key)).await?.bytes().await?;
        Ok(if bytes.starts_with(MAGIC) && bytes.len() >= HEADER {
            bytes.slice(HEADER..)
        } else {
            bytes
        })
    }
    /// Read an exact logical byte range (header offsets are hidden).
    pub async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, Error> {
        let path = self.path(key);
        let meta = self.0.backend.head(&path).await?;
        if meta.size == 0 {
            return if range == (0..0) {
                Ok(Bytes::new())
            } else {
                Err(Error::Config("invalid object range"))
            };
        }
        let prefix = self
            .0
            .backend
            .get_range(&path, 0..meta.size.min(HEADER as u64))
            .await?;
        let header = if prefix.starts_with(MAGIC) && prefix.len() >= HEADER {
            HEADER as u64
        } else {
            0
        };
        if range.start > range.end || range.end > meta.size - header {
            return Err(Error::Config("invalid object range"));
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        Ok(self
            .0
            .backend
            .get_range(&path, range.start + header..range.end + header)
            .await?)
    }
    /// Read object metadata without downloading the payload.
    pub async fn head(&self, key: &str) -> Result<ObjectInfo, Error> {
        let m = self.0.backend.head(&self.path(key)).await?;
        Ok(ObjectInfo {
            key: key.into(),
            size: m.size,
            last_modified: m.last_modified,
        })
    }
    /// Wait for disk-cache writes that were started behind reads (they never delay a read).
    pub async fn flush_cache(&self) {
        self.0.cache.flush().await;
    }
    /// Replace an object. Only heartbeats and explicit mutable system objects should use this.
    pub async fn put(&self, key: &str, value: impl Into<Bytes>) -> Result<(), Error> {
        self.writable()?;
        let value = value.into();
        // Escape the reserved prefix so arbitrary overwrite payloads round-trip.
        let value = if value.starts_with(MAGIC) {
            let mut encoded = Vec::with_capacity(HEADER + value.len());
            encoded.extend_from_slice(MAGIC);
            encoded.extend_from_slice(&[0; 16]);
            encoded.extend_from_slice(&value);
            Bytes::from(encoded)
        } else {
            value
        };
        self.0.backend.put(&self.path(key), value.into()).await?;
        Ok(())
    }
    /// Create with a random retry identity. Conflicts never overwrite an existing object.
    pub async fn create(&self, key: &str, value: impl Into<Bytes>) -> Result<CreateResult, Error> {
        self.create_with_id(key, value, Uuid::new_v4()).await
    }
    /// Upload an immutable object under a key no other writer can use (for example a segment
    /// file under a fresh UUID prefix). Large values go up as concurrent multipart parts,
    /// which is several times faster than one PUT stream. Multipart uploads cannot be
    /// conditional, so unlike [`Self::create`] this overwrites: callers must guarantee key
    /// uniqueness. Small values and single-writer stores use [`Self::create`].
    pub async fn create_unique(
        &self,
        key: &str,
        value: impl Into<Bytes>,
    ) -> Result<CreateResult, Error> {
        self.create_unique_sized(key, value, 32 << 20, 16 << 20)
            .await
    }
    /// Whether the store supports conditional creates, and so multipart uploads here.
    pub fn is_conditional(&self) -> bool {
        self.0.mode == StorageMode::Conditional
    }
    /// [`Self::create_unique`] with explicit thresholds: values of at least `multipart_min`
    /// bytes go up in parts of `part` bytes (object stores need at least 5 MiB per part).
    pub async fn create_unique_sized(
        &self,
        key: &str,
        value: impl Into<Bytes>,
        multipart_min: usize,
        part: usize,
    ) -> Result<CreateResult, Error> {
        const MIN_PART: usize = 5 << 20;
        let (min, part) = (multipart_min, part.max(MIN_PART));
        let value = value.into();
        if value.len() < min || self.0.mode != StorageMode::Conditional {
            return self.create(key, value).await;
        }
        self.writable()?;
        let path = self.path(key);
        let upload = self.0.backend.put_multipart(&path).await?;
        let mut writer = object_store::WriteMultipart::new_with_chunk_size(upload, part);
        let mut header = Vec::with_capacity(HEADER);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(Uuid::new_v4().as_bytes());
        writer.put(header.into());
        let mut offset = 0;
        let result = async {
            while offset < value.len() {
                writer.wait_for_capacity(8).await?;
                let end = (offset + part).min(value.len());
                writer.put(value.slice(offset..end));
                offset = end;
            }
            Ok::<_, object_store::Error>(())
        }
        .await;
        match result {
            Ok(()) => {
                writer.finish().await?;
                Ok(CreateResult::Created)
            }
            Err(e) => {
                let _ = writer.abort().await;
                Err(e.into())
            }
        }
    }
    /// Create with a caller-retained identity. A retry of an already committed put succeeds.
    pub async fn create_with_id(
        &self,
        key: &str,
        value: impl Into<Bytes>,
        write_id: Uuid,
    ) -> Result<CreateResult, Error> {
        self.writable()?;
        let _guard = if self.0.mode == StorageMode::SingleWriter {
            Some(self.0.serial.lock().await)
        } else {
            None
        };
        let path = self.path(key);
        let value = value.into();
        let mut payload = Vec::with_capacity(HEADER + value.len());
        payload.extend_from_slice(MAGIC);
        payload.extend_from_slice(write_id.as_bytes());
        payload.extend_from_slice(&value);
        if self.0.mode == StorageMode::SingleWriter {
            match self.0.backend.get(&path).await {
                Ok(found) => return Ok(identity(&found.bytes().await?, write_id)),
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        let mode = if self.0.mode == StorageMode::Conditional {
            PutMode::Create
        } else {
            PutMode::Overwrite
        };
        match self
            .0
            .backend
            .put_opts(
                &path,
                Bytes::from(payload).into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(CreateResult::Created),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(identity(
                &self.0.backend.get(&path).await?.bytes().await?,
                write_id,
            )),
            Err(e) => {
                // Resolve a lost response before reporting failure; retry once with the same identity.
                match self.0.backend.get(&path).await {
                    Ok(found) => Ok(identity(&found.bytes().await?, write_id)),
                    Err(object_store::Error::NotFound { .. }) => Err(e.into()),
                    Err(_) => Err(e.into()),
                }
            }
        }
    }
    /// Atomically redact an occupied permanent fence, retaining its create/retry identity.
    ///
    /// This is NOT a general compare-and-swap operation: every concurrent caller for
    /// this key must supply the identical, final redacted value. The key must never
    /// be deleted or reused. An overwrite never exposes an absent slot, so stalled
    /// create-only writers remain fenced, including during concurrent redactions.
    /// Returns false for a missing fence or a different identity.
    pub async fn redact_with_id(
        &self,
        key: &str,
        value: impl Into<Bytes>,
        write_id: Uuid,
    ) -> Result<bool, Error> {
        self.writable()?;
        let _guard = if self.0.mode == StorageMode::SingleWriter {
            Some(self.0.serial.lock().await)
        } else {
            None
        };
        let path = self.path(key);
        let current = match self.0.backend.get(&path).await {
            Ok(found) => found.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        if identity(&current, write_id) != CreateResult::AlreadyCommitted {
            return Ok(false);
        }
        let value = value.into();
        let mut encoded = Vec::with_capacity(HEADER + value.len());
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(write_id.as_bytes());
        encoded.extend_from_slice(&value);
        let encoded = Bytes::from(encoded);
        if current == encoded {
            return Ok(true);
        }
        match self.0.backend.put(&path, encoded.clone().into()).await {
            Ok(_) => Ok(true),
            Err(e) => {
                // A lost overwrite response is successful only when the final body is visible.
                if let Ok(found) = self.0.backend.get(&path).await
                    && found.bytes().await.is_ok_and(|bytes| bytes == encoded)
                {
                    return Ok(true);
                }
                Err(e.into())
            }
        }
    }
    /// List objects under a relative prefix, sorted by key.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Error> {
        let base = if self.1.is_empty() {
            self.0.prefix.to_string()
        } else if self.0.prefix.as_ref().is_empty() {
            self.1.to_string()
        } else {
            format!("{}/{}", self.0.prefix, self.1)
        };
        let base = base.as_str();
        let mut objects = self
            .0
            .backend
            .list(Some(&self.path(prefix)))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|m| ObjectInfo {
                key: if base.is_empty() {
                    m.location.to_string()
                } else {
                    m.location
                        .as_ref()
                        .strip_prefix(&format!("{base}/"))
                        .unwrap_or(m.location.as_ref())
                        .into()
                },
                size: m.size,
                last_modified: m.last_modified,
            })
            .collect::<Vec<_>>();
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(objects)
    }
    /// Delete one key; a missing key is already deleted.
    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        self.writable()?;
        match self.0.backend.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
    async fn acquire_heartbeat(&self) -> Result<(), Error> {
        match self.get("_system/writer.json").await {
            Ok(bytes) => {
                let h: Heartbeat =
                    serde_json::from_slice(&bytes).map_err(|_| Error::WriterActive)?;
                if Utc::now().signed_duration_since(h.updated_at).num_seconds() < 30 {
                    return Err(Error::WriterActive);
                }
            }
            Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        self.write_heartbeat().await
    }
    async fn refresh_heartbeat(&self) -> Result<(), Error> {
        let bytes = self.get("_system/writer.json").await?;
        let h: Heartbeat = serde_json::from_slice(&bytes).map_err(|_| Error::WriterActive)?;
        if h.writer != self.0.writer.to_string() {
            return Err(Error::WriterActive);
        }
        self.write_heartbeat().await
    }
    async fn write_heartbeat(&self) -> Result<(), Error> {
        let bytes = serde_json::to_vec(&Heartbeat {
            writer: self.0.writer.to_string(),
            updated_at: Utc::now(),
        })
        .expect("heartbeat serializes");
        self.put("_system/writer.json", bytes).await
    }
}
fn identity(bytes: &[u8], id: Uuid) -> CreateResult {
    if bytes.len() >= HEADER && bytes.starts_with(MAGIC) && &bytes[8..HEADER] == id.as_bytes() {
        CreateResult::AlreadyCommitted
    } else {
        CreateResult::Conflict
    }
}
async fn probe_backend(backend: &dyn ObjectStore, prefix: &Path) -> Result<bool, Error> {
    let path = Path::from(format!("{prefix}/_system/probe/{}", Uuid::new_v4()));
    match backend
        .put_opts(
            &path,
            Bytes::from_static(b"first").into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => {}
        Err(
            object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. },
        ) => return Ok(false),
        Err(e) => return Err(e.into()),
    }
    let second = backend
        .put_opts(
            &path,
            Bytes::from_static(b"second").into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await;
    let answer = match second {
        Err(
            object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. },
        ) => Ok(true),
        Ok(_)
        | Err(
            object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. },
        ) => Ok(false),
        Err(e) => Err(e.into()),
    };
    backend.delete(&path).await?;
    answer
}
fn profile_credentials(profile: &str) -> Result<(String, String, Option<String>), Error> {
    let path = std::env::var_os("AWS_SHARED_CREDENTIALS_FILE")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".aws/credentials"))
        })
        .ok_or(Error::Config("AWS profile credentials file unavailable"))?;
    let content = std::fs::read_to_string(path)?;
    let mut active = false;
    let mut access = None;
    let mut secret = None;
    let mut token = None;
    for line in content.lines().map(str::trim) {
        if line.starts_with('[') {
            active = line == format!("[{profile}]");
            continue;
        }
        if active && let Some((name, value)) = line.split_once('=') {
            let value = value.trim().to_owned();
            match name.trim() {
                "aws_access_key_id" => access = Some(value),
                "aws_secret_access_key" => secret = Some(value),
                "aws_session_token" => token = Some(value),
                _ => {}
            }
        }
    }
    Ok((
        access.ok_or(Error::Config("AWS profile access key missing"))?,
        secret.ok_or(Error::Config("AWS profile secret missing"))?,
        token,
    ))
}

/// Chooses how many idle connections to keep: 1.25 times the largest wave of parallel reads seen in
/// the last `window`, between `floor` and `ceiling`. It starts from `initial` so the first query
/// after startup finds a warm pool, and falls to `floor` once the window passes without a wave that
/// big. `fixed` (from `BF_WARM_CONNECTIONS`) overrides all of it.
#[derive(Debug)]
pub(crate) struct PoolSizer {
    fixed: Option<usize>,
    floor: usize,
    ceiling: usize,
    window: Duration,
    demand: usize,
    demand_at: std::time::Instant,
}
impl PoolSizer {
    pub(crate) fn new(
        fixed: Option<usize>,
        initial: usize,
        floor: usize,
        ceiling: usize,
        window: Duration,
    ) -> Self {
        Self {
            fixed,
            floor,
            ceiling,
            window,
            demand: initial.min(ceiling),
            demand_at: std::time::Instant::now(),
        }
    }
    /// Record the peak concurrency of the last tick and return the pool size to hold.
    pub(crate) fn observe(&mut self, peak: usize, now: std::time::Instant) -> usize {
        if let Some(n) = self.fixed {
            return n;
        }
        let scaled = peak + peak / 4;
        if scaled >= self.demand || now.duration_since(self.demand_at) > self.window {
            self.demand = scaled;
            self.demand_at = now;
        }
        self.demand.clamp(self.floor, self.ceiling)
    }
}
#[cfg(test)]
mod pool_sizer_tests {
    use super::*;
    #[test]
    fn follows_the_largest_wave_and_decays_when_idle() {
        let t0 = std::time::Instant::now();
        let mut s = PoolSizer::new(None, 256, 16, 1024, Duration::from_secs(600));
        assert_eq!(s.observe(0, t0), 256, "starts warm");
        assert_eq!(s.observe(900, t0), 1024, "grows to the wave, capped");
        assert_eq!(
            s.observe(10, t0 + Duration::from_secs(300)),
            1024,
            "keeps it inside the window"
        );
        assert_eq!(
            s.observe(10, t0 + Duration::from_secs(700)),
            16,
            "decays to the floor"
        );
        assert_eq!(s.observe(200, t0 + Duration::from_secs(710)), 250);
        let mut f = PoolSizer::new(Some(64), 256, 16, 1024, Duration::from_secs(1));
        assert_eq!(f.observe(900, t0), 64, "override wins");
    }
}
