//! Versioned, immutable, contiguous WAL slots and extensible namespace manifests.
use bf_storage::{CreateResult, Storage};
use bf_types::{ApiError, AttributeType, DistanceMetric, Document, Id, Schema, validate_namespace};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod packed;

/// WAL encoding 1: one version byte followed by zstd-compressed JSON.
pub const FORMAT_VERSION: u8 = 1;
/// WAL encoding 2: as encoding 1, with the vectors of f16 attributes packed as f16 base64
/// (half the bytes, losslessly). Written for large payloads whose every such vector is exactly
/// f16; readers accept both encodings.
pub const PACKED_FORMAT_VERSION: u8 = 2;
/// Current manifest generation. Formats 1 to 3 are accepted only for automatic upgrade (format 3
/// replaced format 2's single `ann_slabs` layout with `ann_tiers`; format 4 added `lineage`, and
/// segments a tier covers no longer need an ANN object of their own).
pub const MANIFEST_FORMAT_VERSION: u8 = 4;
/// Current format of a tier's slabs and directory. A tier written in an older one is rebuilt by
/// the automatic recluster.
pub const TIER_FORMAT_VERSION: u32 = 1;
fn tier_format_default() -> u32 {
    1
}

/// `ann_tiers` as written (attribute to tiers), or format 2's `ann_slabs` (attribute to one slab
/// set), read as a base tier: the next publication writes it back as `ann_tiers`.
fn tiers_or_slabs<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<std::collections::BTreeMap<String, Vec<SlabSet>>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Tiers {
        Many(Vec<SlabSet>),
        One(Box<SlabSet>),
    }
    let map = std::collections::BTreeMap::<String, Tiers>::deserialize(d)?;
    Ok(map
        .into_iter()
        .map(|(attr, tiers)| {
            let tiers = match tiers {
                Tiers::Many(list) => list,
                Tiers::One(set) => vec![*set],
            };
            (attr, tiers)
        })
        .filter(|(_, tiers)| !tiers.is_empty())
        .collect())
}

fn manifest_version<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u8, D::Error> {
    let version = u64::deserialize(d)?;
    if !(1..=u64::from(MANIFEST_FORMAT_VERSION)).contains(&version) {
        return Err(serde::de::Error::custom(format!(
            "unsupported manifest format version {version}; this binary supports 1 through {MANIFEST_FORMAT_VERSION}; upgrade blowfish"
        )));
    }
    Ok(version as u8)
}
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
/// Payloads of at least this size are uploaded as concurrent parts of `WAL_PART` bytes.
const WAL_MULTIPART_MIN: usize = 8 << 20;
const WAL_PART: usize = 5 << 20;
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
        let (json, version) = self.to_json()?;
        encode_payload(&json, version)
    }
    /// The entry's JSON and the encoding version that reads it back: f16 vectors are packed
    /// when the entry is too large to stay inline and every one of them packs exactly.
    fn to_json(&self) -> Result<(Vec<u8>, u8), Error> {
        if !self.tombstone
            && packed::candidate_bytes(self) > INLINE_PAYLOAD_BYTES
            && let Some(packed) = packed::pack_entry(self)
        {
            return Ok((serde_json::to_vec(&packed)?, PACKED_FORMAT_VERSION));
        }
        Ok((serde_json::to_vec(self)?, FORMAT_VERSION))
    }
    /// Read a supported WAL encoding; corrupt and future versions fail closed.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        use std::io::Read;
        let version = match bytes.first() {
            Some(v) if *v == FORMAT_VERSION || *v == PACKED_FORMAT_VERSION => *v,
            _ => return Err(Error::Format),
        };
        let mut out = Vec::new();
        zstd::stream::read::Decoder::new(&bytes[1..])?
            .take(1024 * 1024 * 1024 + 1)
            .read_to_end(&mut out)?;
        if out.len() > 1024 * 1024 * 1024 {
            return Err(Error::Format);
        }
        let mut entry: Self = serde_json::from_slice(&out)?;
        if version == PACKED_FORMAT_VERSION {
            packed::unpack_entry(&mut entry)?;
        }
        Ok(entry)
    }
}
fn encode_payload(json: &[u8], version: u8) -> Result<Vec<u8>, Error> {
    // One-shot compression reads the contiguous JSON in place; the streaming encoder copies it
    // through a small buffer, which costs about 20% more on payloads zstd can barely shrink.
    let compressed = zstd::bulk::compress(json, 3)?;
    let mut out = Vec::with_capacity(compressed.len() + 1);
    out.push(version);
    out.extend_from_slice(&compressed);
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
/// A create-only manifest snapshot. Extensions require a format generation bump.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    #[serde(deserialize_with = "manifest_version")]
    pub format_version: u8,
    pub schema: Schema,
    pub distance_metric: DistanceMetric,
    pub id_type: AttributeType,
    pub created_at: DateTime<Utc>,
    /// Format 2 deliberately omits the old required `indexed_through` wire field,
    /// fencing released binaries whose serde readers ignored unknown fields.
    #[serde(rename = "checkpoint_wal", alias = "indexed_through")]
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
    /// Namespace ANN directories, one per vector attribute (`annv2:<attribute>` to object). A
    /// directory is valid only while its segment list equals `segments`; readers fall back to
    /// scanning document columns otherwise.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub ann_dirs: std::collections::BTreeMap<String, SegmentFile>,
    /// Namespace-wide ANN layouts (the automatic recluster's tiers) per vector attribute: a base,
    /// and later at most one delta. Rows of segments no tier covers are searched through
    /// `ann_dirs`. Format 2 manifests' `ann_slabs` is read as a base tier.
    #[serde(
        default,
        alias = "ann_slabs",
        deserialize_with = "tiers_or_slabs",
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub ann_tiers: std::collections::BTreeMap<String, Vec<SlabSet>>,
    /// Where the rows of tier-covered segments that compaction merged went: one entry per covered
    /// segment that is gone, while some tier still covers it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lineage: Vec<Lineage>,
    /// Last indexed write timestamp; absent on legacy schema-only manifests.
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    /// When the indexer last flushed a bulk-load-sized tail. The automatic recluster holds its
    /// builds while a load runs, and this keeps that across restarts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bulk_load_at: Option<DateTime<Utc>>,
    /// Objects retired by a publication, with the start of their retention window.
    #[serde(default)]
    pub retired: Vec<RetiredObject>,
    /// Publication-time grace periods for reclaiming checkpointed WAL payloads.
    #[serde(default)]
    pub checkpoints: Vec<(u64, DateTime<Utc>)>,
    /// Every WAL payload up to this sequence has been reclaimed, so the next pass starts after it
    /// instead of re-reading the whole log. Absent in older manifests (zero: start from the top).
    #[serde(default)]
    pub pruned_through: u64,
    /// Set only on the reader copy at the fixed `manifest-latest` key when it was written without
    /// the segments' text headers (see [`Manifest::reader_copy`]). Never set on a manifest slot.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub root: bool,
}
impl Manifest {
    /// The compact copy a cold query that does not rank by text reads: the same manifest without
    /// the per-segment text headers (dictionary chunk indexes, about two thirds of the bytes at 100
    /// million rows), which only BM25 and full-text filters use and which are read from the
    /// segments themselves when absent.
    pub fn reader_copy(&self) -> Manifest {
        let mut root = self.clone();
        for segment in &mut root.segments {
            for file in segment.files.values_mut() {
                file.text_header = None;
            }
        }
        root.root = true;
        root
    }
    /// Whether any segment carries a text header.
    fn has_text_headers(&self) -> bool {
        self.segments
            .iter()
            .any(|s| s.files.values().any(|f| f.text_header.is_some()))
    }
}
/// A namespace-wide ANN layout: the rows of `covered` segments, repartitioned by position in
/// vector space into slabs, each a pseudo segment with its own index and block fragment. A
/// slab's row IDs are global row numbers (`SlabCover::base` plus the segment-local row).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlabSet {
    pub covered: Vec<SlabCover>,
    pub slabs: Vec<Segment>,
    /// The block-layout directory over `slabs`.
    pub dir: SegmentFile,
    /// The same directory as a paged object (a resident coarse header, then entry pages): a query
    /// reads the header and only the pages of the cells it probes, never the whole of `dir`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pages: Option<SegmentFile>,
    /// The format of the slabs and directory ([`TIER_FORMAT_VERSION`] when written). Absent in
    /// format 2 manifests, whose slab sets are format 1.
    #[serde(default = "tier_format_default")]
    pub format: u32,
    /// The coarse cells the slabs were partitioned by (`cells x dims` little-endian f32, in the
    /// metric's prepared space). A delta tier is built against its base's cells, and a merge
    /// reuses them. Absent on a delta tier, and on tiers built before format 3 kept them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coarse: Option<SegmentFile>,
    /// Rows the coarse cells were trained over (a merge retrains them once the base has grown four
    /// times past this).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub coarse_rows: u64,
}
fn is_zero(n: &u64) -> bool {
    *n == 0
}
impl SlabSet {
    /// Rows the slabs hold (covered rows that were live when they were built).
    pub fn rows(&self) -> u64 {
        self.slabs.iter().map(|s| u64::from(s.row_count)).sum()
    }
    /// Whether `segment` is one of the covered segments.
    pub fn covers(&self, segment: &str) -> bool {
        self.covered.iter().any(|c| c.segment == segment)
    }
}
/// Where a tier-covered segment's rows went when compaction merged it away. A tier keeps
/// addressing them by its own global row numbers; each part maps the source's surviving rows to
/// rows of one segment that holds them now. Rows in no part were deleted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lineage {
    /// The covered segment (no longer in `segments`).
    pub source: String,
    /// The segments holding its surviving rows, in ID order. Empty when none survive.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<LineagePart>,
}
/// One target of a [`Lineage`] entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineagePart {
    /// The segment that holds the rows now.
    pub target: String,
    /// Rows mapped (the map object's size).
    pub rows: u32,
    /// Two roaring bitmaps of equal length: the source rows placed in `target`, and the target
    /// rows they became. The k-th smallest of one is the k-th smallest of the other (both segments
    /// are in ID order), so `rank` and `select` map a row either way.
    pub map: SegmentFile,
}
/// One covered segment's span of global row numbers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlabCover {
    pub segment: String,
    pub base: u32,
    pub rows: u32,
    /// The segment's live-row generation when the slabs were built (rows it had deleted by then
    /// are not in the slabs).
    #[serde(default)]
    pub live_generation: u64,
}
/// Record `blowfish_wal_commit_duration_seconds` for one slot attempt, from the start of staging
/// to the end of the slot's conditional commit. Pipelined writers that call `stage` and `commit`
/// separately must call this themselves.
pub fn record_commit(result: &Result<CreateResult, Error>, started: std::time::Instant) {
    let outcome = match result {
        Ok(CreateResult::Conflict) => "conflict",
        Ok(_) => "success",
        Err(_) => "error",
    };
    metrics::histogram!("blowfish_wal_commit_duration_seconds", "outcome" => outcome)
        .record(started.elapsed().as_secs_f64());
}
/// Which form of the newest manifest a reader wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestView {
    /// The manifest slot itself, with every field.
    Full,
    /// The fixed-key copy, which may omit the segments' text headers.
    Reader,
}
/// Immutable file reference. Lengths exclude the storage retry-identity header.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentFile {
    pub key: String,
    pub size: u64,
    pub metadata_len: u64,
    /// For a chunked text statistics object: the per-field totals and the key range of each
    /// chunk, so a query reads only the chunks that hold its terms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats_index: Option<TextStatsIndex>,
    /// For a version 4 full-text segment: its footer (hex) and dictionary chunk index, so a query
    /// starts without reading the object's tail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_header: Option<TextHeaderRef>,
}
/// A full-text segment's footer and dictionary chunk index, carried in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextHeaderRef {
    /// The 72-byte footer, hex encoded.
    pub footer: String,
    /// `(first term, offset, length, crc32)` per dictionary chunk, in term order.
    pub chunks: Vec<(String, u64, u32, u32)>,
}
/// Chunk index of a text statistics object, carried in the manifest.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TextStatsIndex {
    pub fields: std::collections::BTreeMap<String, FieldStatsIndex>,
}
/// One field's totals and the chunks of its term frequencies, in term order.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FieldStatsIndex {
    pub documents: u64,
    pub total_length: u64,
    /// `(first term, offset, length, crc32)` per chunk.
    pub chunks: Vec<(String, u64, u32, u32)>,
}
/// One immutable segment and its current live-row generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    #[error("unsupported or corrupt manifest: {0}")]
    Manifest(String),
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
fn record_stage(stage: &'static str, since: std::time::Instant) {
    metrics::histogram!("blowfish_wal_stage_seconds", "stage" => stage)
        .record(since.elapsed().as_secs_f64());
}

/// A payload made durable by `Wal::stage`, awaiting its slot's commit.
#[derive(Debug, Clone, Copy)]
pub struct Staged {
    inline: bool,
    uploaded: bool,
}
impl Staged {
    /// A completed upload with an unknown outcome: `discard` attempts the delete regardless.
    /// The caller must wait for an abandoned upload to finish before discarding its payload.
    pub fn maybe_uploaded() -> Self {
        Self {
            inline: false,
            uploaded: true,
        }
    }
}
/// WAL operations scoped to one validated namespace.
#[derive(Clone)]
pub struct Wal {
    store: Storage,
    namespace: String,
}
impl Wal {
    /// The namespace this log belongs to.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
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
        record_commit(&result, started);
        result
    }
    async fn append_inner(&self, seq: u64, entry: &WalEntry) -> Result<CreateResult, Error> {
        let staged = self.stage(seq, entry).await?;
        self.commit(seq, entry, &staged).await
    }
    /// First half of an append: make the entry's payload durable under a key unique to this
    /// attempt (nothing references it yet, so readers cannot see it). Small entries stay inline
    /// in the slot and upload nothing. Staging several slots concurrently and committing them in
    /// order with `commit` keeps many uploads in flight at once.
    pub async fn stage(&self, seq: u64, entry: &WalEntry) -> Result<Staged, Error> {
        self.store.cached_bucket_format()?;
        let payload_key = self.staged_payload_key(seq, entry.write_id);
        let started = std::time::Instant::now();
        let (json, version) = if entry.tombstone {
            (vec![], FORMAT_VERSION)
        } else {
            entry.to_json()?
        };
        record_stage("serialize", started);
        metrics::histogram!("blowfish_wal_uncompressed_bytes").record(json.len() as f64);
        let inline = !entry.tombstone && json.len() <= INLINE_PAYLOAD_BYTES;
        let mut uploaded = false;
        if !entry.tombstone && !inline {
            let started = std::time::Instant::now();
            let payload = encode_payload(&json, version)?;
            record_stage("compress", started);
            let len = payload.len();
            // The key carries this attempt's UUID, so no other writer can use it: a large
            // payload goes up as concurrent parts (one PUT stream moves about 5 MB/s).
            let started = std::time::Instant::now();
            let result = if len >= WAL_MULTIPART_MIN && self.store.is_conditional() {
                self.store
                    .create_unique_sized(&payload_key, payload, WAL_MULTIPART_MIN, WAL_PART)
                    .await?
            } else {
                self.store
                    .create_with_id(&payload_key, payload, entry.write_id)
                    .await?
            };
            record_stage("payload_put", started);
            if result == CreateResult::Created {
                metrics::counter!("blowfish_wal_bytes_written_total").increment(len as u64);
            }
            uploaded = true;
        }
        Ok(Staged { inline, uploaded })
    }
    /// Delete a staged payload that will never be committed (a failed or abandoned pipeline slot).
    pub async fn discard(&self, seq: u64, entry: &WalEntry, staged: &Staged) -> Result<(), Error> {
        if staged.uploaded {
            self.store
                .delete(&self.staged_payload_key(seq, entry.write_id))
                .await?;
        }
        Ok(())
    }
    /// Reclaim expired, unreferenced payloads whose keys identify their attempted WAL slots.
    ///
    /// An empty slot is fenced with an empty entry before deletion: an arbitrarily paused writer
    /// must lose its conditional commit, rather than publish a reference to a deleted payload.
    /// Fill earlier gaps in order, preserving the contiguous log. Payloads referenced by occupied
    /// slots remain intact, regardless of age; checkpoint reclamation is a separate operation.
    /// Legacy UUID-only keys have no attempted slot to fence and are left untouched.
    pub async fn collect_orphan_payloads(&self, cutoff: DateTime<Utc>) -> Result<usize, Error> {
        self.store.check_bucket_format().await?;
        self.latest_manifest().await?;
        let mut attempts = std::collections::BTreeMap::<u64, Vec<String>>::new();
        for object in self
            .store
            .list(&format!("ns/{}/wal-data/", self.namespace))
            .await?
        {
            if object.last_modified > cutoff {
                continue;
            }
            let attempt = object
                .key
                .rsplit('/')
                .next()
                .and_then(|name| name.strip_suffix(".wal"))
                .and_then(|name| name.split_once('-'))
                .filter(|(seq, id)| seq.len() == 20 && Uuid::parse_str(id).is_ok())
                .and_then(|(seq, _)| seq.parse::<u64>().ok())
                .filter(|seq| *seq > 0);
            if let Some(seq) = attempt {
                attempts.entry(seq).or_default().push(object.key);
            }
        }
        let mut removed = 0;
        for (seq, keys) in attempts {
            let record = match self.record(seq).await? {
                Some(record) => record,
                None => {
                    let head = self.head().await?;
                    for fence in head.saturating_add(1)..=seq {
                        self.append(fence, &WalEntry::new(vec![])).await?;
                    }
                    // Another writer may have won the fence; protect its payload in that case.
                    self.record(seq).await?.ok_or(Error::Format)?
                }
            };
            let referenced = (!record.tombstone && !record.redacted && record.inline.is_none())
                .then(|| self.record_payload_key(&record));
            for key in keys {
                if referenced.as_ref() != Some(&key) {
                    self.store.delete(&key).await?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }
    /// Second half of an append: the create-only conditional PUT of the slot record. `Conflict`
    /// means another writer holds the slot; the staged payload is deleted and the caller must
    /// catch up and revalidate.
    pub async fn commit(
        &self,
        seq: u64,
        entry: &WalEntry,
        staged: &Staged,
    ) -> Result<CreateResult, Error> {
        self.store.cached_bucket_format()?;
        let inline = staged.inline;
        let record = CommitRecord {
            write_id: entry.write_id,
            payload_seq: Some(seq),
            committed_at: entry.committed_at,
            tombstone: entry.tombstone,
            inline: inline.then(|| entry.clone()),
            redacted: false,
        };
        let started = std::time::Instant::now();
        let record = record.encode()?;
        record_stage("slot_encode", started);
        let record_len = record.len();
        let started = std::time::Instant::now();
        let result = self
            .store
            .create_with_id(&self.key(seq), record, entry.write_id)
            .await?;
        record_stage("slot_put", started);
        if result == CreateResult::Created {
            metrics::counter!("blowfish_wal_bytes_written_total").increment(record_len as u64);
        }
        if result == CreateResult::Conflict {
            metrics::counter!("blowfish_wal_cas_conflicts_total").increment(1);
        }
        if result == CreateResult::Created {
            metrics::histogram!("blowfish_wal_batch_requests").record(entry.batches.len() as f64);
        }
        if result == CreateResult::Conflict {
            // This UUID belongs only to this attempt; a losing slot cannot reference it.
            self.discard(seq, entry, staged).await?;
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
        self.store.cached_bucket_format()?;
        // Even direct publishers must not overwrite a namespace they cannot understand.
        self.latest_manifest().await?;
        // The slot holds the whole manifest (compressed when large), so a cold reader needs
        // one GET after the listing. Older slots point at a separate payload object.
        if manifest.root {
            return Err(Error::Format);
        }
        let identity = Uuid::new_v4();
        let encoded = encode_manifest(manifest)?;
        let result = self
            .store
            .create_with_id(
                &format!("ns/{}/manifest/{seq:020}.json", self.namespace),
                encoded.clone(),
                identity,
            )
            .await?;
        if result == CreateResult::Conflict {
            self.store.check_bucket_format().await?;
        }
        if result == CreateResult::Created {
            // A copy at a fixed key, so a cold reader fetches the manifest in the same round trip
            // as the listing that confirms it is the newest. Best effort: readers verify it.
            // For a manifest of megabytes the copy omits the text headers, which a query that does
            // not rank by text never reads (`Manifest::reader_copy`).
            let above = compact_copy_above();
            let compact = if manifest.has_text_headers() && encoded.len() > above {
                encode_manifest(&manifest.reader_copy())
                    .ok()
                    // Worth the extra round trip a text query then pays only when much smaller
                    // (a threshold of zero, for tests, takes any).
                    .filter(|c| above == 0 || c.len() < encoded.len() / 2)
            } else {
                None
            };
            let mut pointer = seq.to_be_bytes().to_vec();
            pointer.extend_from_slice(compact.as_ref().unwrap_or(&encoded));
            let _ = self.store.put(&self.latest_key(), pointer).await;
        }
        Ok(result)
    }
    fn latest_key(&self) -> String {
        format!("ns/{}/manifest-latest", self.namespace)
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
        self.prune_payloads_from(0, through).await
    }
    /// Like `prune_payloads`, for sequences in `(after, through]` only (the caller knows everything
    /// up to `after` is already reclaimed). Slots are processed concurrently: a serial pass over a
    /// long log would stall indexing for minutes.
    pub async fn prune_payloads_from(&self, after: u64, through: u64) -> Result<(), Error> {
        self.store.check_bucket_format().await?;
        self.latest_manifest().await?;
        use futures::StreamExt;
        let mut work = futures::stream::iter(after.saturating_add(1)..=through)
            .map(|seq| async move {
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
                Ok(())
            })
            .buffer_unordered(32);
        while let Some(done) = work.next().await {
            done?;
        }
        drop(work);
        let orphans: Vec<String> = self
            .store
            .list(&format!("ns/{}/wal-data/", self.namespace))
            .await?
            .into_iter()
            .filter(|object| {
                object
                    .key
                    .rsplit('/')
                    .next()
                    .and_then(|name| name.split_once('-'))
                    .filter(|(seq, _)| seq.len() == 20)
                    .and_then(|(seq, _)| seq.parse::<u64>().ok())
                    .is_some_and(|seq| seq <= through)
            })
            .map(|object| object.key)
            .collect();
        let mut deletes = futures::stream::iter(orphans)
            .map(|key| async move { self.store.delete(&key).await })
            .buffer_unordered(32);
        while let Some(done) = deletes.next().await {
            done?;
        }
        Ok(())
    }
    /// Read the most recent manifest if one exists.
    pub async fn latest_manifest(&self) -> Result<Option<(u64, Manifest)>, Error> {
        self.store.cached_bucket_format()?;
        self.manifest_after(0).await
    }
    /// Discover a newer immutable manifest, skipping payload reads and decoding when
    /// the caller already holds the latest generation. Listing still observes remote publications.
    pub async fn manifest_after(&self, known: u64) -> Result<Option<(u64, Manifest)>, Error> {
        self.manifest_view_after(known, ManifestView::Full).await
    }
    /// Rewrite the fixed-key reader copy of the newest manifest as the compact copy, for a namespace
    /// published before compact copies existed. Returns the sequence and the new copy's encoded size,
    /// or `None` when the manifest is small or has no text headers. Readers verify the copy against
    /// the manifest log, so a copy that lags a concurrent publication is ignored, not trusted.
    pub async fn write_reader_copy(&self) -> Result<Option<(u64, usize)>, Error> {
        self.store.cached_bucket_format()?;
        let Some((seq, manifest)) = self.latest_manifest().await? else {
            return Ok(None);
        };
        if !manifest.has_text_headers() {
            return Ok(None);
        }
        let compact = encode_manifest(&manifest.reader_copy())?;
        let mut pointer = seq.to_be_bytes().to_vec();
        pointer.extend_from_slice(&compact);
        let size = pointer.len();
        self.store.put(&self.latest_key(), pointer).await?;
        Ok(Some((seq, size)))
    }
    /// [`Wal::manifest_after`] with a choice of view. `ManifestView::Reader` may return the compact
    /// copy (`Manifest::root` set), which lacks the segments' text headers.
    pub async fn manifest_view_after(
        &self,
        known: u64,
        view: ManifestView,
    ) -> Result<Option<(u64, Manifest)>, Error> {
        self.store.cached_bucket_format()?;
        let result = self.manifest_view_after_inner(known, view).await;
        if matches!(
            &result,
            Err(Error::Manifest(_) | Error::Format | Error::Io(_))
        ) {
            // A newer writer may have advanced the bucket while this process was running.
            // Refresh only on failure; successful reads and publications need no marker I/O.
            self.store.check_bucket_format().await?;
        }
        result
    }
    async fn manifest_view_after_inner(
        &self,
        known: u64,
        view: ManifestView,
    ) -> Result<Option<(u64, Manifest)>, Error> {
        // A cold reader asks for the fixed-key copy while it lists; when the copy is the newest
        // manifest, that is one round trip instead of two. A copy that lags the listing is ignored.
        let t0 = std::time::Instant::now();
        let prefix = format!("ns/{}/manifest/", self.namespace);
        // A namespace with thousands of publications lists over several sequential pages. With
        // the copy in hand, only the manifests after it need listing (usually none, one page): take
        // whichever of the two ways of finding the newest sequence finishes first.
        use futures::FutureExt as _;
        let copy_read = async {
            if known == 0 {
                self.store.get(&self.latest_key()).await.ok()
            } else {
                None
            }
        }
        .shared();
        let tail = {
            let (copy_read, prefix) = (copy_read.clone(), &prefix);
            async move {
                let copy = copy_read.await?;
                let seq = u64::from_be_bytes(copy.get(..8)?.try_into().ok()?);
                let after = format!("{prefix}{seq:020}.json");
                let newer = self.store.list_after(prefix, &after).await.ok()?;
                Some((seq, after, newer))
            }
        };
        let full = self.store.list(&prefix);
        let tail = tail.boxed();
        let (objects, copy) = match futures::future::select(full.boxed(), tail).await {
            futures::future::Either::Left((listed, _)) => (listed?, copy_read.await),
            futures::future::Either::Right((Some((_, after, newer)), _)) => {
                let objects = if newer.is_empty() {
                    vec![bf_storage::ObjectInfo {
                        key: after,
                        size: 0,
                        last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap_or_default(),
                    }]
                } else {
                    newer
                };
                (objects, copy_read.await)
            }
            futures::future::Either::Right((None, full)) => (full.await?, copy_read.await),
        };
        if std::env::var_os("BF_TRACE_STAGES").is_some() {
            eprintln!(
                "MANIFEST found newest after {} ms",
                t0.elapsed().as_millis()
            );
        }
        let Some(last) = objects.last() else {
            return Ok(None);
        };
        let seq: u64 = last
            .key
            .rsplit('/')
            .next()
            .and_then(|s| s.strip_suffix(".json"))
            .and_then(|s| s.parse().ok())
            .ok_or(Error::Format)?;
        if seq <= known {
            return Ok(None);
        }
        if let Some(copy) = copy
            && copy.len() > 8
            && copy[..8] == seq.to_be_bytes()
            && slot_payload_key(&copy[8..]).is_none()
        {
            let manifest = decode_manifest(&copy[8..])?;
            if view == ManifestView::Reader || !manifest.root {
                if std::env::var_os("BF_TRACE_STAGES").is_some() {
                    eprintln!("MANIFEST decoded after {} ms", t0.elapsed().as_millis());
                }
                return Ok(Some((seq, manifest)));
            }
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
/// Manifests whose encoding is larger than this get a compact reader copy (when it is much
/// smaller). `BF_READER_COPY_MIN_BYTES` overrides it.
const COMPACT_COPY_ABOVE: usize = 256 << 10;

fn compact_copy_above() -> usize {
    std::env::var("BF_READER_COPY_MIN_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(COMPACT_COPY_ABOVE)
}

fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>, Error> {
    if !(1..=MANIFEST_FORMAT_VERSION).contains(&manifest.format_version) {
        return Err(Error::Manifest(format!(
            "unsupported format version {}; this binary supports up to {MANIFEST_FORMAT_VERSION}",
            manifest.format_version
        )));
    }
    let mut upgraded;
    let manifest = if manifest.format_version < MANIFEST_FORMAT_VERSION {
        upgraded = manifest.clone();
        upgraded.format_version = MANIFEST_FORMAT_VERSION;
        &upgraded
    } else {
        manifest
    };
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
        return serde_json::from_slice(&json).map_err(|e| Error::Manifest(e.to_string()));
    }
    serde_json::from_slice(bytes).map_err(|e| Error::Manifest(e.to_string()))
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
            ann_tiers: Default::default(),
            lineage: Default::default(),
            updated_at: None,
            bulk_load_at: None,
            retired: vec![],
            checkpoints: vec![],
            pruned_through: 0,
            root: false,
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

    #[test]
    fn every_manifest_encoding_and_direct_deserializer_rejects_future_versions() {
        let mut value = serde_json::to_value(manifest_with_terms(3)).unwrap();
        for version in [0, u64::from(MANIFEST_FORMAT_VERSION) + 1, 256, u64::MAX] {
            value["format_version"] = serde_json::json!(version);
            let json = serde_json::to_vec(&value).unwrap();
            let compressed = zstd::stream::encode_all(json.as_slice(), 3).unwrap();
            for bytes in [&json, &compressed] {
                assert!(
                    decode_manifest(bytes)
                        .unwrap_err()
                        .to_string()
                        .contains(&format!("unsupported manifest format version {version}"))
                );
            }
            assert!(serde_json::from_slice::<Manifest>(&json).is_err());
        }
        let mut future = manifest_with_terms(3);
        future.format_version = MANIFEST_FORMAT_VERSION + 1;
        assert!(encode_manifest(&future).is_err());
        // Also fail closed if an extension accidentally forgets to raise its version.
        let mut value = serde_json::to_value(manifest_with_terms(3)).unwrap();
        value["future_objects"] = serde_json::json!(["segments/new-layout"]);
        assert!(decode_manifest(&serde_json::to_vec(&value).unwrap()).is_err());
        let file = serde_json::json!({
            "key": "segments/known", "size": 1, "metadata_len": 0,
            "future_object": "segments/new-layout"
        });
        assert!(serde_json::from_value::<SegmentFile>(file).is_err());
    }

    #[test]
    fn format_two_slab_sets_are_read_as_base_tiers_and_written_back_as_tiers() {
        let mut value = serde_json::to_value(manifest_with_terms(3)).unwrap();
        value["format_version"] = serde_json::json!(2);
        let file = serde_json::json!({"key": "ns/a/slabs/dir", "size": 1, "metadata_len": 0});
        value["ann_slabs"] = serde_json::json!({
            "vector": {
                "covered": [{"segment": "s1", "base": 0, "rows": 5, "live_generation": 0}],
                "slabs": [],
                "dir": file,
            }
        });
        let old = decode_manifest(&serde_json::to_vec(&value).unwrap()).unwrap();
        let tiers = &old.ann_tiers["vector"];
        assert_eq!(tiers.len(), 1);
        assert_eq!(tiers[0].covered[0].segment, "s1");
        assert_eq!(tiers[0].format, 1);
        let written: serde_json::Value =
            serde_json::from_slice(&encode_manifest(&old).unwrap()).unwrap();
        assert_eq!(written["format_version"], MANIFEST_FORMAT_VERSION);
        assert!(written.get("ann_slabs").is_none());
        assert_eq!(written["ann_tiers"]["vector"][0]["covered"][0]["rows"], 5);
        let again = decode_manifest(&serde_json::to_vec(&written).unwrap()).unwrap();
        assert_eq!(again.ann_tiers["vector"][0].format, 1);
    }
}
