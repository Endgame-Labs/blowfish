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
// WAL slot records are permanent CAS fences. Document-bearing payloads are separate
// so a tombstone can physically erase them without making an old slot writable again.
const SLOT_VERSION: u8 = 2;
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitRecord {
    write_id: Uuid,
    committed_at: DateTime<Utc>,
    tombstone: bool,
}
impl CommitRecord {
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = vec![SLOT_VERSION];
        bytes.extend(serde_json::to_vec(self)?);
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.first() != Some(&SLOT_VERSION) {
            return Err(Error::Format);
        }
        Ok(serde_json::from_slice(&bytes[1..])?)
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
    #[error("WAL payload was reclaimed after namespace deletion")]
    Pruned,
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
    fn payload_key(&self, id: Uuid) -> String {
        format!("ns/{}/wal-data/{id}.wal", self.namespace)
    }
    /// Attempt a slot. The payload is durable before the slot's conditional commit.
    /// Callers must catch up and revalidate after `Conflict`.
    pub async fn append(&self, seq: u64, entry: &WalEntry) -> Result<CreateResult, Error> {
        let payload_key = self.payload_key(entry.write_id);
        if !entry.tombstone {
            self.store
                .create_with_id(&payload_key, entry.encode()?, entry.write_id)
                .await?;
        }
        let record = CommitRecord {
            write_id: entry.write_id,
            committed_at: entry.committed_at,
            tombstone: entry.tombstone,
        };
        let result = self
            .store
            .create_with_id(&self.key(seq), record.encode()?, entry.write_id)
            .await?;
        if result == CreateResult::Conflict && !entry.tombstone {
            // This UUID belongs only to this attempt; a losing slot cannot reference it.
            self.store.delete(&payload_key).await?;
        }
        Ok(result)
    }
    async fn record(&self, seq: u64) -> Result<Option<CommitRecord>, Error> {
        match self.store.get(&self.key(seq)).await {
            Ok(bytes) => Ok(Some(CommitRecord::decode(&bytes)?)),
            Err(bf_storage::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    async fn materialize(&self, record: &CommitRecord) -> Result<WalEntry, Error> {
        if record.tombstone {
            return Ok(WalEntry {
                write_id: record.write_id,
                committed_at: record.committed_at,
                batches: vec![],
                tombstone: true,
            });
        }
        let bytes = match self.store.get(&self.payload_key(record.write_id)).await {
            Ok(bytes) => bytes,
            Err(bf_storage::Error::NotFound) => return Err(Error::Pruned),
            Err(e) => return Err(e.into()),
        };
        let entry = WalEntry::decode(&bytes)?;
        if entry.write_id != record.write_id || entry.tombstone {
            return Err(Error::Format);
        }
        Ok(entry)
    }
    /// Read a slot. A reclaimed pre-tombstone payload returns `Error::Pruned`.
    pub async fn read(&self, seq: u64) -> Result<Option<WalEntry>, Error> {
        match self.record(seq).await? {
            Some(record) => Ok(Some(self.materialize(&record).await?)),
            None => Ok(None),
        }
    }
    /// Replay a contiguous tail after `after`, stopping at the first missing slot.
    /// Tombstones let cold and stale nodes skip safely reclaimed document payloads.
    pub async fn tail(&self, after: u64) -> Result<Vec<(u64, WalEntry)>, Error> {
        let mut previous_head = None;
        loop {
            let mut records = vec![];
            let mut seq = after + 1;
            while let Some(record) = self.record(seq).await? {
                records.push((seq, record));
                seq += 1;
            }
            let start = records.iter().rposition(|(_, r)| r.tombstone).unwrap_or(0);
            let mut entries = vec![];
            let mut pruned = false;
            for (seq, record) in records.into_iter().skip(start) {
                match self.materialize(&record).await {
                    Ok(entry) => entries.push((seq, entry)),
                    Err(Error::Pruned) => {
                        pruned = true;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
            if !pruned {
                return Ok(entries);
            }
            // A deletion may have committed after the record scan. Rescan to see
            // its tombstone; an unchanged head with a missing body is corruption.
            if previous_head == Some(seq - 1) {
                return Err(Error::Format);
            }
            previous_head = Some(seq - 1);
        }
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
    /// Erase document-bearing payloads and old manifests through a committed tombstone.
    /// Tiny slot records remain occupied, fencing stale writers without retaining documents.
    pub async fn cleanup_deleted(&self, tombstone_seq: u64) -> Result<(), Error> {
        if !self
            .record(tombstone_seq)
            .await?
            .is_some_and(|r| r.tombstone)
        {
            return Err(Error::Format);
        }
        for seq in 1..tombstone_seq {
            if let Some(record) = self.record(seq).await?
                && !record.tombstone
            {
                self.store
                    .delete(&self.payload_key(record.write_id))
                    .await?;
            }
        }
        for object in self
            .store
            .list(&format!("ns/{}/manifest/", self.namespace))
            .await?
        {
            let seq = object
                .key
                .rsplit('/')
                .next()
                .and_then(|s| s.strip_suffix(".json"))
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or(Error::Format)?;
            if seq <= tombstone_seq {
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
