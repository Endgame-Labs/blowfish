//! Versioned, immutable, contiguous WAL slots and extensible namespace manifests.
use bf_storage::{CreateResult, Storage};
use bf_types::{ApiError, AttributeType, DistanceMetric, Document, Id, Schema, validate_namespace};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Current WAL encoding: one version byte followed by zstd-compressed JSON.
pub const FORMAT_VERSION: u8 = 1;
/// A fully validated, deterministic write batch. Conditions are resolved before persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteBatch {
    pub upserts: Vec<Document>,
    pub deletes: Vec<Id>,
    pub schema: Schema,
    pub distance_metric: DistanceMetric,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
/// A slot can group many validated atomic requests, or delete a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub write_id: Uuid,
    pub committed_at: DateTime<Utc>,
    pub batches: Vec<WriteBatch>,
    #[serde(default)]
    pub tombstone: bool,
}
impl WalEntry {
    /// A new group-commit entry with a stable identity for ambiguous retries.
    pub fn new(batches: Vec<WriteBatch>) -> Self {
        Self {
            write_id: Uuid::new_v4(),
            committed_at: Utc::now(),
            batches,
            tombstone: false,
        }
    }
    /// Serialize with a version header and bounded decompression on reads.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let json = serde_json::to_vec(self)?;
        let mut out = vec![FORMAT_VERSION];
        out.extend(zstd::stream::encode_all(json.as_slice(), 3)?);
        Ok(out)
    }
    /// Read a supported WAL encoding; corrupt and future versions fail closed.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        use std::io::Read;
        if bytes.first() != Some(&FORMAT_VERSION) {
            return Err(Error::Format);
        }
        let mut out = Vec::new();
        zstd::stream::read::Decoder::new(&bytes[1..])?
            .take(1024 * 1024 * 1024 + 1)
            .read_to_end(&mut out)?;
        if out.len() > 1024 * 1024 * 1024 {
            return Err(Error::Format);
        }
        Ok(serde_json::from_slice(&out)?)
    }
}
/// A create-only manifest snapshot, extensible by later indexers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u8,
    pub schema: Schema,
    pub distance_metric: DistanceMetric,
    pub id_type: AttributeType,
    pub created_at: DateTime<Utc>,
    pub indexed_through: u64,
    pub indexer_epoch: u64,
    #[serde(default)]
    pub segments: Vec<serde_json::Value>,
}
/// Durable log errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Storage(#[from] bf_storage::Error),
    #[error("invalid WAL JSON")]
    Json(#[from] serde_json::Error),
    #[error("invalid WAL compression")]
    Io(#[from] std::io::Error),
    #[error("unsupported or corrupt WAL format")]
    Format,
    #[error(transparent)]
    Api(#[from] ApiError),
}
/// WAL operations scoped to one validated namespace.
#[derive(Clone)]
pub struct Wal {
    store: Storage,
    namespace: String,
}
impl Wal {
    /// Open a namespace log without creating it.
    pub fn new(store: Storage, namespace: &str) -> Result<Self, Error> {
        validate_namespace(namespace)?;
        Ok(Self {
            store,
            namespace: namespace.into(),
        })
    }
    /// Deterministic contiguous-slot object key.
    pub fn key(&self, seq: u64) -> String {
        format!("ns/{}/wal/{seq:020}.wal", self.namespace)
    }
    /// Attempt a slot. Callers must catch up and revalidate after `Conflict`.
    pub async fn append(&self, seq: u64, entry: &WalEntry) -> Result<CreateResult, Error> {
        Ok(self
            .store
            .create_with_id(&self.key(seq), entry.encode()?, entry.write_id)
            .await?)
    }
    /// Read a slot, with a missing slot represented as None.
    pub async fn read(&self, seq: u64) -> Result<Option<WalEntry>, Error> {
        match self.store.get(&self.key(seq)).await {
            Ok(bytes) => Ok(Some(WalEntry::decode(&bytes)?)),
            Err(bf_storage::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    /// Replay a contiguous tail after `after`, stopping at the first missing slot.
    pub async fn tail(&self, after: u64) -> Result<Vec<(u64, WalEntry)>, Error> {
        let mut entries = Vec::new();
        let mut seq = after + 1;
        while let Some(entry) = self.read(seq).await? {
            entries.push((seq, entry));
            seq += 1;
        }
        Ok(entries)
    }
    /// Discover the head with one sorted listing on cold start.
    pub async fn head(&self) -> Result<u64, Error> {
        Ok(self
            .store
            .list(&format!("ns/{}/wal/", self.namespace))
            .await?
            .iter()
            .filter_map(|o| {
                o.key
                    .rsplit('/')
                    .next()?
                    .strip_suffix(".wal")?
                    .parse::<u64>()
                    .ok()
            })
            .max()
            .unwrap_or(0))
    }
    /// Persist an immutable manifest version.
    pub async fn publish_manifest(
        &self,
        seq: u64,
        manifest: &Manifest,
    ) -> Result<CreateResult, Error> {
        Ok(self
            .store
            .create(
                &format!("ns/{}/manifest/{seq:020}.json", self.namespace),
                serde_json::to_vec(manifest)?,
            )
            .await?)
    }
    /// Read the most recent manifest if one exists.
    pub async fn latest_manifest(&self) -> Result<Option<(u64, Manifest)>, Error> {
        let objects = self
            .store
            .list(&format!("ns/{}/manifest/", self.namespace))
            .await?;
        let Some(last) = objects.last() else {
            return Ok(None);
        };
        let seq = last
            .key
            .rsplit('/')
            .next()
            .and_then(|s| s.strip_suffix(".json"))
            .and_then(|s| s.parse().ok())
            .ok_or(Error::Format)?;
        Ok(Some((
            seq,
            serde_json::from_slice(&self.store.get(&last.key).await?)?,
        )))
    }
    /// Delete obsolete non-WAL objects after a tombstone is committed. WAL slots remain fences.
    ///
    /// Immutable WAL fences prevent a stale writer from recreating a previously committed slot.
    /// Physical WAL reclamation requires the generation/retention protocol implemented with GC.
    pub async fn cleanup_deleted(&self) -> Result<(), Error> {
        for object in self.store.list(&format!("ns/{}/", self.namespace)).await? {
            if !object.key.contains("/wal/") {
                self.store.delete(&object.key).await?;
            }
        }
        Ok(())
    }
}

/// Request queue for group commit. A leader drains available requests into one WAL entry.
pub struct GroupCommit<T, R> {
    sender: tokio::sync::mpsc::Sender<Pending<T, R>>,
}
/// One request and its durable-commit reply channel.
pub struct Pending<T, R> {
    pub request: T,
    pub reply: tokio::sync::oneshot::Sender<R>,
}
impl<T, R> Clone for GroupCommit<T, R> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}
impl<T: Send + 'static, R: Send + 'static> GroupCommit<T, R> {
    /// Create a bounded commit queue and its single leader receiver.
    pub fn channel(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<Pending<T, R>>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        (Self { sender }, receiver)
    }
    /// Submit a request. Cancellation after submission does not cancel a durable write.
    pub async fn submit(&self, request: T) -> Result<R, ApiError> {
        let (reply, result) = tokio::sync::oneshot::channel();
        self.sender
            .send(Pending { request, reply })
            .await
            .map_err(|_| ApiError::new(503, "write coordinator unavailable"))?;
        result
            .await
            .map_err(|_| ApiError::new(503, "write coordinator unavailable"))
    }
}
