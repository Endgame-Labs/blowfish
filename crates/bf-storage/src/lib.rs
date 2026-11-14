#![doc = include_str!("../README.md")]
mod cache;
mod disk;
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
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub url: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub read_only: bool,
    pub single_writer: bool,
    pub cache: CacheConfig,
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
}
/// Cloneable store handle. Creates carry a portable body header with a retry identity.
#[derive(Clone)]
pub struct Storage(Arc<Inner>);
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
                let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
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
                (
                    Arc::new(b.build()?),
                    Path::from_url_path(url.path())
                        .map_err(|_| Error::Config("invalid S3 prefix"))?,
                )
            }
            "memory" => (
                Arc::new(object_store::memory::InMemory::new()),
                Path::default(),
            ),
            _ => {
                return Err(Error::Config(
                    "supported storage schemes are file, s3, and memory",
                ));
            }
        };
        let mut cache = config.cache.clone();
        if cache.namespace.is_none() {
            cache.namespace = Some(format!(
                "{}|{}",
                config.url,
                config.endpoint.as_deref().unwrap_or("")
            ));
        }
        Self::from_backend_with_cache(
            backend,
            prefix,
            config.read_only,
            config.single_writer,
            cache,
        )
        .await
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
            namespace.push_str(&format!("|{prefix}"));
        }
        let cache = cache::RangeCache::open(cache).await?;
        let backend: Arc<dyn ObjectStore> = Arc::new(observed::Observed(backend));
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
        let store = Self(Arc::new(Inner {
            backend,
            cache,
            prefix,
            mode,
            read_only,
            healthy: AtomicBool::new(true),
            writer: Uuid::new_v4(),
            serial: Mutex::new(()),
        }));
        if mode == StorageMode::SingleWriter && !read_only {
            store.acquire_heartbeat().await?;
            let weak = Arc::downgrade(&store.0);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let Some(inner) = weak.upgrade() else { break };
                    let store = Storage(inner);
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
        Path::from(format!("{}/{key}", self.0.prefix))
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
    /// List objects under a relative prefix, sorted by key.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, Error> {
        let base = self.0.prefix.as_ref();
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
