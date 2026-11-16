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
        encode_payload(&serde_json::to_vec(self)?)
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
fn encode_payload(json: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = vec![FORMAT_VERSION];
    out.extend(zstd::stream::encode_all(json, 3)?);
    Ok(out)
}
// Slot occupancy and retry identities are permanent. Small payloads transition only
// from inline to redacted; atomic replacement never makes an old slot writable.
const SLOT_VERSION: u8 = 3;
/// Maximum uncompressed entry size stored directly in a commit slot (64 KiB).
pub const INLINE_PAYLOAD_BYTES: usize = 64 * 1024;
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitRecord {
    write_id: Uuid,
    // New payload keys identify their attempted slot, making abandoned payloads
    // reclaimable once that slot is permanently occupied and checkpointed.
    #[serde(default)]
    payload_seq: Option<u64>,
    committed_at: DateTime<Utc>,
    tombstone: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inline: Option<WalEntry>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    redacted: bool,
}
impl CommitRecord {
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = vec![SLOT_VERSION];
        bytes.extend(serde_json::to_vec(self)?);
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if !matches!(bytes.first(), Some(2) | Some(&SLOT_VERSION)) {
            return Err(Error::Format);
        }
        let record: Self = serde_json::from_slice(&bytes[1..])?;
        if (record.inline.is_some() && (record.redacted || record.tombstone))
            || (record.redacted && record.tombstone)
        {
            return Err(Error::Format);
        }
        Ok(record)
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
    /// A tombstone checkpoint retains ordering without retaining namespace contents.
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub segments: Vec<Segment>,
    /// Namespace totals as of `indexed_through`. Per-term text statistics live in
    /// `text_stats` when present, so queries that do not rank by text never download them.
    #[serde(default)]
    pub stats: NamespaceStats,
    /// Separate object holding `stats.text` (a map of field to `TextStats`), when non-empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_stats: Option<SegmentFile>,
    /// Namespace ANN directories, one per vector attribute (`ann:<attribute>` to object). A
    /// directory is valid only while its segment list equals `segments`; readers fall back to
    /// opening each segment otherwise.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub ann_dirs: std::collections::BTreeMap<String, SegmentFile>,
    /// Last indexed write timestamp; absent on legacy schema-only manifests.
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    /// Objects retired by a publication, with the start of their retention window.
    #[serde(default)]
    pub retired: Vec<RetiredObject>,
    /// Publication-time grace periods for reclaiming checkpointed WAL payloads.
    #[serde(default)]
    pub checkpoints: Vec<(u64, DateTime<Utc>)>,
}
/// Immutable file reference. Lengths exclude the storage retry-identity header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentFile {
    pub key: String,
    pub size: u64,
    pub metadata_len: u64,
}
/// One immutable segment and its current live-row generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub id: String,
    pub wal_start: u64,
    pub wal_end: u64,
    pub row_count: u32,
    /// Build-time schema allows queries to detect indexes needing a schema rebuild.
    #[serde(default)]
    pub schema: Schema,
    pub files: std::collections::BTreeMap<String, SegmentFile>,
    pub live: SegmentFile,
    pub live_generation: u64,
}
/// Field-level corpus statistics, excluding missing/null fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TextStats {
    pub documents: u64,
    pub total_length: u64,
    pub document_frequencies: std::collections::BTreeMap<String, u64>,
}
/// Exact live namespace totals at the indexing checkpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamespaceStats {
    pub rows: u64,
    pub logical_bytes: u64,
    pub text: std::collections::BTreeMap<String, TextStats>,
}
/// A file may be reclaimed only after its retirement grace period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetiredObject {
    pub key: String,
    pub retired_at: DateTime<Utc>,
}
/// Ownership is advisory; the manifest generation and epoch are the actual fence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerLease {
    pub owner: Uuid,
    pub expires_at: DateTime<Utc>,
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
    #[error("WAL payload was reclaimed after checkpointing or namespace deletion")]
    Pruned,
    #[error("indexer was fenced by a newer manifest")]
    Fenced,
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
    fn staged_payload_key(&self, seq: u64, id: Uuid) -> String {
        format!("ns/{}/wal-data/{seq:020}-{id}.wal", self.namespace)
    }
    fn record_payload_key(&self, record: &CommitRecord) -> String {
        record
            .payload_seq
            .map(|seq| self.staged_payload_key(seq, record.write_id))
            .unwrap_or_else(|| self.payload_key(record.write_id))
    }
    /// Attempt a slot. Small entries use one create-only PUT; larger payloads are
    /// durable before the slot's conditional commit.
    /// Callers must catch up and revalidate after `Conflict`.
    pub async fn append(&self, seq: u64, entry: &WalEntry) -> Result<CreateResult, Error> {
        let started = std::time::Instant::now();
        let result = self.append_inner(seq, entry).await;
        metrics::histogram!("blowfish_wal_commit_duration_seconds", "outcome" => match &result { Ok(CreateResult::Conflict) => "conflict", Ok(_) => "success", Err(_) => "error" }).record(started.elapsed().as_secs_f64());
        result
    }
    async fn append_inner(&self, seq: u64, entry: &WalEntry) -> Result<CreateResult, Error> {
        let payload_key = self.staged_payload_key(seq, entry.write_id);
        let json = if entry.tombstone {
            vec![]
        } else {
            serde_json::to_vec(entry)?
        };
        let inline = !entry.tombstone && json.len() <= INLINE_PAYLOAD_BYTES;
        if !entry.tombstone && !inline {
            let payload = encode_payload(&json)?;
            let len = payload.len();
            let result = self
                .store
                .create_unique_with_id(&payload_key, payload, entry.write_id)
                .await?;
            if result == CreateResult::Created {
                metrics::counter!("blowfish_wal_bytes_written_total").increment(len as u64);
            }
        }
        let record = CommitRecord {
            write_id: entry.write_id,
            payload_seq: Some(seq),
            committed_at: entry.committed_at,
            tombstone: entry.tombstone,
            inline: inline.then(|| entry.clone()),
            redacted: false,
        };
        let record = record.encode()?;
        let record_len = record.len();
        let result = self
            .store
            .create_with_id(&self.key(seq), record, entry.write_id)
            .await?;
        if result == CreateResult::Created {
            metrics::counter!("blowfish_wal_bytes_written_total").increment(record_len as u64);
        }
        if result == CreateResult::Conflict {
            metrics::counter!("blowfish_wal_cas_conflicts_total").increment(1);
        }
        if result == CreateResult::Created {
            metrics::histogram!("blowfish_wal_batch_requests").record(entry.batches.len() as f64);
        }
        if result == CreateResult::Conflict && !entry.tombstone && !inline {
            // This UUID belongs only to this attempt; a losing slot cannot reference it.
            self.store.delete(&payload_key).await?;
        }
        Ok(result)
    }
    async fn record(&self, seq: u64) -> Result<Option<CommitRecord>, Error> {
        match self.store.get(&self.key(seq)).await {
            Ok(bytes) => {
                let record = CommitRecord::decode(&bytes)?;
                if record.payload_seq.is_some_and(|attempt| attempt != seq) {
                    return Err(Error::Format);
                }
                Ok(Some(record))
            }
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
        if record.redacted {
            return Err(Error::Pruned);
        }
        if let Some(entry) = &record.inline {
            if entry.write_id != record.write_id
                || entry.tombstone
                || entry.committed_at != record.committed_at
            {
                return Err(Error::Format);
            }
            return Ok(entry.clone());
        }
        let bytes = match self.store.get(&self.record_payload_key(record)).await {
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
        self.tail_bounded(after, u64::MAX).await
    }
    /// Replay at most `max_bytes` of decoded logical WAL entries. Atomic entries
    /// are never split; an entry that exceeds the remaining budget stays in the tail.
    pub async fn tail_bounded(
        &self,
        after: u64,
        max_bytes: u64,
    ) -> Result<Vec<(u64, WalEntry)>, Error> {
        let mut previous_head = None;
        loop {
            let mut records = vec![];
            let mut seq = after + 1;
            // Probe growing windows of consecutive slots concurrently: a cold reader with
            // a tail of n entries needs about log2(n) round trips instead of n + 1.
            let mut window = 2u64;
            'scan: loop {
                let found =
                    futures::future::try_join_all((seq..seq + window).map(|s| self.record(s)))
                        .await?;
                for record in found {
                    match record {
                        Some(record) => {
                            records.push((seq, record));
                            seq += 1;
                        }
                        None => break 'scan,
                    }
                }
                window = (window * 2).min(32);
            }
            let start = records.iter().rposition(|(_, r)| r.tombstone).unwrap_or(0);
            let mut entries = vec![];
            let mut pruned = false;
            let mut included_bytes = 0u64;
            for (seq, record) in records.into_iter().skip(start) {
                match self.materialize(&record).await {
                    Ok(entry) => {
                        let size = serde_json::to_vec(&entry)?.len() as u64;
                        if included_bytes.saturating_add(size) > max_bytes {
                            break;
                        }
                        included_bytes += size;
                        entries.push((seq, entry));
                    }
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
        // The slot holds the whole manifest (compressed when large), so a cold reader needs
        // one GET after the listing. Older slots point at a separate payload object.
        let identity = Uuid::new_v4();
        self.store
            .create_with_id(
                &format!("ns/{}/manifest/{seq:020}.json", self.namespace),
                encode_manifest(manifest)?,
                identity,
            )
            .await
            .map_err(Into::into)
    }
    /// Publish against exactly one predecessor. No retry may reuse stale indexer work.
    ///
    /// The check and create are safe together: a competing claim must occupy the
    /// same next slot before it can advance the epoch. A loser is always fenced.
    pub async fn publish_fenced(
        &self,
        predecessor: u64,
        epoch: u64,
        manifest: &Manifest,
    ) -> Result<u64, Error> {
        let latest = self.latest_manifest().await?;
        let (sequence, current_epoch) = latest
            .as_ref()
            .map(|(s, m)| (*s, m.indexer_epoch))
            .unwrap_or((0, 0));
        if sequence != predecessor
            || current_epoch != epoch
            || !(manifest.indexer_epoch == epoch || manifest.indexer_epoch == epoch + 1)
            || latest
                .as_ref()
                .is_some_and(|(_, m)| manifest.indexed_through < m.indexed_through)
        {
            return Err(Error::Fenced);
        }
        let mut manifest = manifest.clone();
        if predecessor > 0 {
            let bytes = self
                .store
                .get(&format!(
                    "ns/{}/manifest/{predecessor:020}.json",
                    self.namespace
                ))
                .await?;
            if let Some(payload) = slot_payload_key(&bytes) {
                manifest.retired.push(RetiredObject {
                    key: payload,
                    retired_at: Utc::now(),
                });
            }
        }
        if latest.as_ref().map_or(0, |(_, m)| m.indexed_through) < manifest.indexed_through {
            manifest
                .checkpoints
                .push((manifest.indexed_through, Utc::now()));
        }
        match self.publish_manifest(predecessor + 1, &manifest).await? {
            CreateResult::Conflict => Err(Error::Fenced),
            _ => Ok(predecessor + 1),
        }
    }
    /// Read an advisory lease. Missing leases permit a claim, never an unfenced publish.
    pub async fn indexer_lease(&self, epoch: u64) -> Result<Option<IndexerLease>, Error> {
        match self
            .store
            .get(&format!("ns/{}/indexer/{epoch:020}.json", self.namespace))
            .await
        {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(bf_storage::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    /// Record a create-only lease after winning an epoch claim in the manifest log.
    pub async fn create_lease(
        &self,
        epoch: u64,
        lease: &IndexerLease,
    ) -> Result<CreateResult, Error> {
        Ok(self
            .store
            .create(
                &format!("ns/{}/indexer/{epoch:020}.json", self.namespace),
                serde_json::to_vec(lease)?,
            )
            .await?)
    }
    /// Reclaim committed payloads through a checkpoint while preserving every slot fence.
    pub async fn prune_payloads(&self, through: u64) -> Result<(), Error> {
        for seq in 1..=through {
            if let Some(mut record) = self.record(seq).await?
                && !record.tombstone
                && !record.redacted
            {
                if record.inline.take().is_some() {
                    record.redacted = true;
                    if !self
                        .store
                        .redact_with_id(&self.key(seq), record.encode()?, record.write_id)
                        .await?
                    {
                        return Err(Error::Fenced);
                    }
                } else {
                    self.store.delete(&self.record_payload_key(&record)).await?;
                }
            }
        }
        for object in self
            .store
            .list(&format!("ns/{}/wal-data/", self.namespace))
            .await?
        {
            let attempt = object
                .key
                .rsplit('/')
                .next()
                .and_then(|name| name.split_once('-'))
                .filter(|(seq, _)| seq.len() == 20)
                .and_then(|(seq, _)| seq.parse::<u64>().ok());
            if attempt.is_some_and(|seq| seq <= through) {
                self.store.delete(&object.key).await?;
            }
        }
        Ok(())
    }
    /// Read the most recent manifest if one exists.
    pub async fn latest_manifest(&self) -> Result<Option<(u64, Manifest)>, Error> {
        self.manifest_after(0).await
    }
    /// Discover a newer immutable manifest, skipping payload reads and decoding when
    /// the caller already holds the latest generation. Listing still observes remote publications.
    pub async fn manifest_after(&self, known: u64) -> Result<Option<(u64, Manifest)>, Error> {
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
        if seq <= known {
            return Ok(None);
        }
        let bytes = self.store.get(&last.key).await?;
        let manifest = if let Some(payload) = slot_payload_key(&bytes) {
            decode_manifest(&self.store.get(&payload).await?)?
        } else {
            decode_manifest(&bytes)?
        };
        Ok(Some((seq, manifest)))
    }
    /// Erase document-bearing payloads through a committed tombstone.
    /// The indexer retires segment files and manifest payloads separately.
    /// Tiny slot records remain occupied, fencing stale writers without retaining documents.
    pub async fn cleanup_deleted(&self, tombstone_seq: u64) -> Result<(), Error> {
        if !self
            .record(tombstone_seq)
            .await?
            .is_some_and(|r| r.tombstone)
        {
            return Err(Error::Format);
        }
        self.prune_payloads(tombstone_seq - 1).await?;
        // Manifest generations have their own sequence space. Keep their CAS
        // fences; readers replay the tombstone after any older checkpoint.
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

/// Manifests carry namespace-wide term statistics and can reach megabytes of JSON, which every
/// cold query downloads. Large ones are zstd-compressed; plain JSON (always starting with `{`)
/// stays readable, so older manifests and older readers' files keep working.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const COMPRESS_ABOVE: usize = 64 << 10;

fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>, Error> {
    let json = serde_json::to_vec(manifest)?;
    if json.len() <= COMPRESS_ABOVE {
        return Ok(json);
    }
    zstd::stream::encode_all(json.as_slice(), 3).map_err(|_| Error::Format)
}

/// The payload object a manifest slot points at, for slots written before manifests were
/// stored inline. Inline slots (JSON or zstd) have none.
pub fn slot_payload_key(slot: &[u8]) -> Option<String> {
    if slot.starts_with(&ZSTD_MAGIC) {
        return None;
    }
    serde_json::from_slice::<serde_json::Value>(slot)
        .ok()?
        .get("payload")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn decode_manifest(bytes: &[u8]) -> Result<Manifest, Error> {
    if bytes.starts_with(&ZSTD_MAGIC) {
        let json = zstd::stream::decode_all(bytes).map_err(|_| Error::Format)?;
        return Ok(serde_json::from_slice(&json)?);
    }
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod manifest_codec_tests {
    use super::*;

    fn manifest_with_terms(terms: usize) -> Manifest {
        let mut text = TextStats::default();
        for i in 0..terms {
            text.document_frequencies
                .insert(format!("term{i:08}"), i as u64);
        }
        let mut stats = NamespaceStats::default();
        stats.text.insert("body".into(), text);
        Manifest {
            format_version: 1,
            schema: Default::default(),
            distance_metric: DistanceMetric::CosineDistance,
            id_type: AttributeType::Scalar(bf_types::ScalarType::Uint),
            created_at: Utc::now(),
            indexed_through: 7,
            indexer_epoch: 1,
            deleted: false,
            segments: vec![],
            stats,
            text_stats: None,
            ann_dirs: Default::default(),
            updated_at: None,
            retired: vec![],
            checkpoints: vec![],
        }
    }

    #[test]
    fn large_manifests_are_compressed_and_round_trip() {
        let big = manifest_with_terms(20_000);
        let plain = serde_json::to_vec(&big).unwrap();
        let packed = encode_manifest(&big).unwrap();
        assert!(packed.len() * 2 < plain.len());
        assert_eq!(decode_manifest(&packed).unwrap().indexed_through, 7);
        // Small manifests and legacy plain JSON stay readable as is.
        let small = encode_manifest(&manifest_with_terms(3)).unwrap();
        assert_eq!(small[0], b'{');
        assert_eq!(
            decode_manifest(&plain).unwrap().stats.text["body"]
                .document_frequencies
                .len(),
            20_000
        );
    }
}
