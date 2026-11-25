//! Immutable column blocks, ID dictionaries, filter bitmaps, and live generations.
use crate::{filter, read_cache, storage_error};
use bf_storage::{CreateResult, Storage};
use bf_types::{ApiError, AttributeType, Document, Filter, Id, Schema};
use bf_wal::{Segment, SegmentFile};
use bytes::Bytes;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{collections::BTreeMap, io, ops::Range, sync::Arc};

#[path = "segment_upgrade.rs"]
pub(crate) mod upgrade;

const MAGIC: &[u8; 8] = b"BFSEG001";
const FOOTER: usize = 32;
const BLOCK_ROWS: usize = 256;

#[cfg(test)]
#[path = "segment_prefetch_tests.rs"]
mod prefetch_tests;

#[derive(Clone, Serialize, Deserialize)]
struct Block {
    offset: u64,
    length: u64,
    crc: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    uncompressed_length: Option<usize>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Column {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    compressed: bool,
    blocks: Vec<Block>,
    /// Vector blocks use native f16/f32 little-endian elements, separate from attributes.
    vector: Option<(usize, bool)>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    vector_i8: bool,
}
#[derive(Default, Serialize, Deserialize)]
struct Directory {
    #[serde(skip)]
    version: u32,
    /// Dense attributes live in ANN objects: dimensions, f16, and i8, respectively.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    vectors: BTreeMap<String, (usize, bool, bool)>,
    columns: BTreeMap<String, Column>,
}

/// A small adapter keeps storage/cache range semantics out of the on-disk formats.
#[derive(Clone)]
pub(crate) struct ObjectReader {
    pub store: Storage,
    pub file: SegmentFile,
    prefetched: Option<(u64, Bytes)>,
    /// One-pass scans (compaction, recluster) read through the caches without filling them.
    bypass: bool,
}
impl ObjectReader {
    pub fn new(store: &Storage, file: &SegmentFile) -> Self {
        Self {
            store: store.clone(),
            file: file.clone(),
            prefetched: None,
            bypass: false,
        }
    }
    /// Read through the block caches without admitting what this reader fetches.
    pub fn bypass_cache(mut self) -> Self {
        self.bypass = true;
        self
    }
    fn range_reader(&self) -> io::Result<bf_storage::RangeReader> {
        let reader = self
            .store
            .range_reader_with_length(&self.file.key, self.file.size, true)
            .map_err(io::Error::other)?;
        Ok(if self.bypass || !read_cache::admitting() {
            reader.bypass_cache()
        } else {
            reader
        })
    }
    pub async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        if let Some((start, bytes)) = &self.prefetched
            && range.start >= *start
            && range.end <= *start + bytes.len() as u64
            && range.start <= range.end
        {
            return Ok(bytes.slice((range.start - start) as usize..(range.end - start) as usize));
        }
        if !self.bypass
            && let Some(mut bytes) =
                crate::local_ann::read(&self.file.key, std::slice::from_ref(&range))
        {
            return Ok(bytes.remove(0));
        }
        self.range_reader()?
            .read_with_stats(range, &read_cache::stats())
            .await
            .map_err(io::Error::other)
    }
    pub async fn read_batch(&self, ranges: Vec<Range<u64>>) -> io::Result<Vec<Bytes>> {
        if !self.bypass
            && let Some(bytes) = crate::local_ann::read(&self.file.key, &ranges)
        {
            return Ok(bytes);
        }
        self.range_reader()?
            .read_batch_with_stats(&ranges, &read_cache::stats())
            .await
            .map_err(io::Error::other)
    }
    /// Read small ranges scattered over the object (rerank vectors) through small cache
    /// blocks, so a miss fetches `BF_SPARSE_BLOCK_BYTES` (default 16 KiB) instead of a full
    /// block, and the cache holds those vectors without the megabyte around each one.
    pub async fn read_batch_sparse(&self, ranges: Vec<Range<u64>>) -> io::Result<Vec<Bytes>> {
        if !self.bypass
            && let Some(bytes) = crate::local_ann::read(&self.file.key, &ranges)
        {
            return Ok(bytes);
        }
        static BLOCK: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        let block = *BLOCK.get_or_init(|| {
            std::env::var("BF_SPARSE_BLOCK_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(16 << 10)
        });
        self.range_reader()?
            .sparse(block)
            .read_batch_with_stats(&ranges, &read_cache::stats())
            .await
            .map_err(io::Error::other)
    }
}

pub(crate) async fn vector_index(
    store: &Storage,
    file: &SegmentFile,
) -> Result<Arc<bf_vector::Index<ObjectReader>>, ApiError> {
    if let Some(index) = read_cache::get(&file.key, file.metadata_len) {
        return Ok(index);
    }
    let index =
        bf_vector::Index::open_with_metadata_len(ObjectReader::new(store, file), file.metadata_len)
            .await
            .map_err(index_error)?;
    // Resident bytes of the decoded centroids, row sets, and checksums.
    let resident = index.resident_bytes();
    Ok(read_cache::insert(&file.key, resident, index))
}

/// The whole object, read through the block cache.
pub(crate) async fn read_whole(store: &Storage, file: &SegmentFile) -> Result<Bytes, ApiError> {
    ObjectReader::new(store, file)
        .read(0..file.size)
        .await
        .map_err(index_error)
}

/// A whole object read once through the block caches without filling them.
pub(crate) async fn read_whole_uncached(
    store: &Storage,
    file: &SegmentFile,
) -> Result<Bytes, ApiError> {
    ObjectReader::new(store, file)
        .bypass_cache()
        .read(0..file.size)
        .await
        .map_err(index_error)
}

/// Independent byte ranges of an object, in one batch through the block cache.
pub(crate) async fn read_ranges(
    store: &Storage,
    file: &SegmentFile,
    ranges: Vec<Range<u64>>,
) -> io::Result<Vec<Bytes>> {
    ObjectReader::new(store, file).read_batch(ranges).await
}

/// Read and cache a docs file's column directory, so hydration needs only the data blocks.
pub(crate) async fn warm_docs(store: &Storage, segment: &Segment) {
    if let Some(file) = segment.files.get("docs") {
        let _ = directory(&ObjectReader::new(store, file)).await;
    }
}

pub(crate) async fn text_index(
    store: &Storage,
    file: &SegmentFile,
) -> Result<Arc<bf_text::SegmentReader>, ApiError> {
    if let Some(index) = read_cache::get(&file.key, file.metadata_len) {
        return Ok(index);
    }
    let mut reader = ObjectReader::new(store, file);
    if file.metadata_len > file.size {
        return Err(corrupt());
    }
    if let Some(header) = &file.text_header {
        // The manifest carries the footer and dictionary index: no read until a query needs data.
        let header = text_header_of(header)?;
        let index = bf_text::SegmentReader::open_with_header(Arc::new(reader), &header)
            .map_err(index_error)?;
        let resident = index.resident_bytes() + 16384;
        let index = read_cache::insert(&file.key, resident, index);
        return Ok(charge_memo(&file.key, resident, index));
    }
    let start = file.size.saturating_sub(file.metadata_len.max(65536));
    let bytes = reader.read(start..file.size).await.map_err(index_error)?;
    reader.prefetched = Some((start, bytes));
    let index = bf_text::SegmentReader::open(Arc::new(reader))
        .await
        .map_err(index_error)?;
    // The retained encoded buffer (the dictionary borrows it) plus the decoded directory.
    let resident = file.metadata_len.max(65536) + index.resident_bytes();
    let index = read_cache::insert(&file.key, resident, index);
    Ok(charge_memo(&file.key, resident, index))
}

/// Make a cached reader's decoded-posting memo pay its actual size into the decoded cache. Each
/// pool may hold up to a thirty-second of the general budget (8 to 64 MiB), so a few readers
/// cannot crowd out the rest.
fn charge_memo(
    key: &str,
    base: u64,
    index: Arc<bf_text::SegmentReader>,
) -> Arc<bf_text::SegmentReader> {
    if let Some(cache) = read_cache::context().map(|c| c.cache) {
        let limit = (read_cache::capacity() / 32).clamp(8 << 20, 64 << 20) as usize;
        let (key, ptr) = (key.to_owned(), Arc::as_ptr(&index) as usize);
        index.set_memo_charge(
            limit,
            Arc::new(move |memo| cache.recharge::<bf_text::SegmentReader>(&key, ptr, base + memo)),
        );
    }
    index
}

fn text_header_of(header: &bf_wal::TextHeaderRef) -> Result<bf_text::TextHeader, ApiError> {
    if !header.footer.len().is_multiple_of(2) || !header.footer.is_ascii() {
        return Err(corrupt());
    }
    Ok(bf_text::TextHeader {
        footer: (0..header.footer.len() / 2)
            .map(|i| {
                u8::from_str_radix(&header.footer[2 * i..2 * i + 2], 16).map_err(|_| corrupt())
            })
            .collect::<Result<_, _>>()?,
        chunks: header
            .chunks
            .iter()
            .map(|(first, o, l, c)| (first.clone().into_bytes(), *o, *l, *c))
            .collect(),
    })
}

pub(crate) fn corrupt() -> ApiError {
    ApiError::new(500, "stored segment is corrupt or unsupported")
}
pub(crate) fn index_error(e: impl std::fmt::Display) -> ApiError {
    tracing::error!(error=%e, "segment operation failed");
    ApiError::new(503, "segment is unavailable or corrupt")
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ApiError> {
    serde_json::to_vec(value).map_err(index_error)
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(bytes).map_err(index_error)
}
fn block(body: &mut Vec<u8>, bytes: &[u8]) -> Block {
    let b = Block {
        offset: body.len() as u64,
        length: bytes.len() as u64,
        crc: crc32fast::hash(bytes),
        uncompressed_length: None,
    };
    body.extend_from_slice(bytes);
    b
}

/// Document-v2 keeps dense-vector shape metadata, but no vector payload. Every string column
/// is zstd level 3, independently per 256-row block; checksums cover the compressed bytes.
fn encode_documents(schema: &Schema, rows: &[&Document]) -> Result<(Vec<u8>, u64), ApiError> {
    let mut body = Vec::new();
    let mut dir = Directory::default();
    for (name, config) in schema {
        if let Some((dims, dtype)) = dense_type(config) {
            dir.vectors.insert(
                name.clone(),
                (
                    dims,
                    dtype == bf_vector::VectorType::F16,
                    dtype == bf_vector::VectorType::I8,
                ),
            );
            continue;
        }
        let compressed = matches!(
            config.data_type,
            Some(
                AttributeType::Scalar(bf_types::ScalarType::String)
                    | AttributeType::Array(bf_types::ScalarType::String)
            )
        );
        let mut blocks = Vec::new();
        for chunk in rows.chunks(BLOCK_ROWS) {
            let data = encode(&chunk.iter().map(|d| d.get(name)).collect::<Vec<_>>())?;
            let b = if compressed {
                let encoded = zstd::bulk::compress(&data, 3).map_err(index_error)?;
                let mut b = block(&mut body, &encoded);
                b.uncompressed_length = Some(data.len());
                b
            } else {
                block(&mut body, &data)
            };
            blocks.push(b);
        }
        dir.columns.insert(
            name.clone(),
            Column {
                blocks,
                vector: None,
                vector_i8: false,
                compressed,
            },
        );
    }
    finish_version(body, &dir, 2)
}

fn decode_column(data: &[u8], col: &Column, b: &Block) -> Result<Vec<Option<Value>>, ApiError> {
    if col.compressed {
        let length = b.uncompressed_length.ok_or_else(corrupt)?;
        let bytes = zstd::bulk::decompress(data, length).map_err(index_error)?;
        if bytes.len() != length {
            return Err(corrupt());
        }
        decode(&bytes)
    } else {
        decode(data)
    }
}
/// Maintenance scans need only a few rows of each compressed JSON block. Skip the other
/// values while parsing, rather than allocating hundreds of strings for each requested row.
fn decode_selected_column(
    data: &[u8],
    col: &Column,
    b: &Block,
    first: u32,
    count: usize,
    rows: &RoaringBitmap,
) -> Result<Vec<(u32, Value)>, ApiError> {
    struct Selected<'a> {
        first: u32,
        count: usize,
        rows: &'a RoaringBitmap,
    }
    impl<'de> serde::de::Visitor<'de> for Selected<'_> {
        type Value = Vec<(u32, Value)>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "a document column with {} values", self.count)
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            let mut found = Vec::new();
            for i in 0..self.count {
                if self.rows.contains(self.first + i as u32) {
                    let value = seq
                        .next_element::<Option<Value>>()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
                    if let Some(value) = value {
                        found.push((self.first + i as u32, value));
                    }
                } else if seq.next_element::<serde::de::IgnoredAny>()?.is_none() {
                    return Err(serde::de::Error::invalid_length(i, &self));
                }
            }
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::invalid_length(self.count + 1, &self));
            }
            Ok(found)
        }
    }
    let unpacked;
    let data = if col.compressed {
        let length = b.uncompressed_length.ok_or_else(corrupt)?;
        unpacked = zstd::bulk::decompress(data, length).map_err(index_error)?;
        if unpacked.len() != length {
            return Err(corrupt());
        }
        unpacked.as_slice()
    } else {
        data
    };
    let mut decoder = serde_json::Deserializer::from_slice(data);
    let found = serde::Deserializer::deserialize_seq(&mut decoder, Selected { first, count, rows })
        .map_err(index_error)?;
    decoder.end().map_err(index_error)?;
    Ok(found)
}
fn finish(body: Vec<u8>, directory: &Directory) -> Result<(Vec<u8>, u64), ApiError> {
    finish_version(body, directory, 1)
}
fn finish_version(
    mut body: Vec<u8>,
    directory: &Directory,
    version: u32,
) -> Result<(Vec<u8>, u64), ApiError> {
    let dir = encode(directory)?;
    let mut footer = Vec::with_capacity(FOOTER);
    footer.extend_from_slice(MAGIC);
    footer.extend_from_slice(&version.to_le_bytes());
    footer.extend_from_slice(&(body.len() as u64).to_le_bytes());
    footer.extend_from_slice(&(dir.len() as u32).to_le_bytes());
    footer.extend_from_slice(&crc32fast::hash(&dir).to_le_bytes());
    footer.extend_from_slice(&crc32fast::hash(&footer).to_le_bytes());
    let meta = (dir.len() + FOOTER) as u64;
    body.extend_from_slice(&dir);
    body.extend_from_slice(&footer);
    Ok((body, meta))
}
async fn directory(reader: &ObjectReader) -> Result<Arc<Directory>, ApiError> {
    if let Some(dir) = read_cache::get(&reader.file.key, reader.file.metadata_len) {
        return Ok(dir);
    }
    let size = reader.file.size;
    let start = size.saturating_sub(reader.file.metadata_len.max(65536));
    let bytes = reader.read(start..size).await.map_err(index_error)?;
    let dir = decode_directory(reader, &bytes, start)?;
    Ok(read_cache::insert(
        &reader.file.key,
        reader.file.metadata_len.max(65536).saturating_mul(16),
        dir,
    ))
}
fn decode_directory(
    reader: &ObjectReader,
    bytes: &[u8],
    start: u64,
) -> Result<Directory, ApiError> {
    let size = reader.file.size;
    if size < FOOTER as u64 || reader.file.metadata_len > size {
        return Err(corrupt());
    }
    let footer = &bytes[bytes.len() - FOOTER..];
    if &footer[..8] != MAGIC
        || !matches!(u32::from_le_bytes(footer[8..12].try_into().unwrap()), 1 | 2)
        || crc32fast::hash(&footer[..28]) != u32::from_le_bytes(footer[28..32].try_into().unwrap())
    {
        return Err(corrupt());
    }
    let offset = u64::from_le_bytes(footer[12..20].try_into().unwrap());
    let length = u32::from_le_bytes(footer[20..24].try_into().unwrap()) as u64;
    if offset.checked_add(length) != Some(size - FOOTER as u64) || offset < start {
        return Err(corrupt());
    }
    let data = &bytes[(offset - start) as usize..bytes.len() - FOOTER];
    if crc32fast::hash(data) != u32::from_le_bytes(footer[24..28].try_into().unwrap()) {
        return Err(corrupt());
    }
    let mut dir: Directory = decode(data)?;
    dir.version = u32::from_le_bytes(footer[8..12].try_into().unwrap());
    if dir.vectors.iter().any(|(name, (dims, f16, i8))| {
        *dims == 0 || *dims > 65536 || (*f16 && *i8) || dir.columns.contains_key(name)
    }) || (dir.version == 2 && dir.columns.values().any(|c| c.vector.is_some()))
    {
        return Err(corrupt());
    }
    for col in dir.columns.values() {
        for b in &col.blocks {
            if b.offset
                .checked_add(b.length)
                .is_none_or(|end| end > offset)
            {
                return Err(corrupt());
            }
        }
    }
    Ok(dir)
}
async fn read_block(reader: &ObjectReader, b: &Block) -> Result<Bytes, ApiError> {
    let data = reader
        .read(b.offset..b.offset + b.length)
        .await
        .map_err(index_error)?;
    if crc32fast::hash(&data) != b.crc {
        return Err(corrupt());
    }
    Ok(data)
}
pub(crate) async fn put_file(
    store: &Storage,
    key: String,
    bytes: Vec<u8>,
    metadata_len: u64,
) -> Result<SegmentFile, ApiError> {
    put_bytes(store, key, bytes.into(), metadata_len).await
}
/// [`put_file`] for bytes the caller keeps a handle on.
pub(crate) async fn put_bytes(
    store: &Storage,
    key: String,
    bytes: bytes::Bytes,
    metadata_len: u64,
) -> Result<SegmentFile, ApiError> {
    let file = SegmentFile {
        stats_index: None,
        text_header: None,
        key,
        size: bytes.len() as u64,
        metadata_len,
    };
    if store
        .create_unique(&file.key, bytes)
        .await
        .map_err(storage_error)?
        == CreateResult::Conflict
    {
        return Err(corrupt());
    }
    Ok(file)
}
pub(crate) async fn put_value<T: Serialize>(
    store: &Storage,
    key: String,
    value: &T,
) -> Result<SegmentFile, ApiError> {
    let mut body = vec![];
    let b = block(&mut body, &encode(value)?);
    let dir = Directory {
        columns: BTreeMap::from([(
            "value".into(),
            Column {
                compressed: false,
                blocks: vec![b],
                vector: None,
                vector_i8: false,
            },
        )]),
        ..Default::default()
    };
    let (bytes, meta) = finish(body, &dir)?;
    put_file(store, key, bytes, meta).await
}
pub(crate) async fn get_value<T: DeserializeOwned + Send + Sync + 'static>(
    store: &Storage,
    file: &SegmentFile,
) -> Result<Arc<T>, ApiError> {
    if let Some(value) = read_cache::get(&file.key, file.size) {
        return Ok(value);
    }
    let mut reader = ObjectReader::new(store, file);
    let bytes = reader.read(0..file.size).await.map_err(index_error)?;
    let dir = decode_directory(&reader, &bytes, 0)?;
    reader.prefetched = Some((0, bytes));
    let b = dir
        .columns
        .get("value")
        .and_then(|c| c.blocks.first())
        .ok_or_else(corrupt)?;
    let value = decode(&read_block(&reader, b).await?)?;
    Ok(read_cache::insert(
        &file.key,
        file.size.saturating_mul(16),
        value,
    ))
}

#[derive(Serialize, Deserialize)]
pub(crate) struct IdIndex {
    ids: Vec<Id>,
}
impl std::ops::Deref for IdIndex {
    type Target = Vec<Id>;
    fn deref(&self) -> &Self::Target {
        &self.ids
    }
}
/// A segment's ID filter: its smallest and largest ID plus a Bloom filter with a false-positive rate
/// low enough (about 1e-5) that a write batch of thousands of IDs rarely has to open any segment's
/// full ID list. It is a small object of its own (3 bytes per row, against about 35 for `ids.bfi`),
/// so a write or publication checks every segment without loading a single ID list.
///
/// Layout: `BFIF0001`, the Bloom's probe count (u8), the length of the JSON `[min, max]` (u32 LE),
/// that JSON, then the bit array.
pub(crate) struct IdFilter {
    min: Id,
    max: Id,
    probes: u8,
    bits: Vec<u8>,
}
const ID_FILTER_MAGIC: &[u8; 8] = b"BFIF0001";
/// 24 bits per ID with 16 probes: a false-positive rate of about 1e-5.
const ID_FILTER_BITS_PER_ID: usize = 24;
const ID_FILTER_PROBES: u8 = 16;
/// Two independent 64-bit hashes of an ID (FNV-1a with a splitmix finalizer, then a second round).
pub(crate) fn id_hash(id: &Id) -> (u64, u64) {
    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    let h = match id {
        Id::Uint(n) => mix(*n ^ 0x9e3779b97f4a7c15),
        Id::String(s) => mix(s.bytes().fold(0xcbf29ce484222325u64, |h, b| {
            (h ^ b as u64).wrapping_mul(0x100000001b3)
        }) ^ 0x517cc1b727220a95),
    };
    (h, mix(h ^ 0xd6e8feb86659fd93) | 1)
}
impl IdFilter {
    /// `ids` must be sorted and non-empty.
    pub(crate) fn build(ids: &[Id]) -> Self {
        let nbits = (ids.len() * ID_FILTER_BITS_PER_ID).max(64);
        let mut bits = vec![0u8; nbits.div_ceil(8)];
        let modulus = bits.len() as u64 * 8;
        for id in ids {
            let (h1, h2) = id_hash(id);
            for i in 0..ID_FILTER_PROBES as u64 {
                let bit = h1.wrapping_add(i.wrapping_mul(h2)) % modulus;
                bits[(bit / 8) as usize] |= 1 << (bit % 8);
            }
        }
        Self {
            min: ids.first().cloned().unwrap_or(Id::Uint(0)),
            max: ids.last().cloned().unwrap_or(Id::Uint(0)),
            probes: ID_FILTER_PROBES,
            bits,
        }
    }
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let range = serde_json::to_vec(&(&self.min, &self.max)).expect("IDs serialize");
        let mut out = Vec::with_capacity(13 + range.len() + self.bits.len());
        out.extend_from_slice(ID_FILTER_MAGIC);
        out.push(self.probes);
        out.extend_from_slice(&(range.len() as u32).to_le_bytes());
        out.extend_from_slice(&range);
        out.extend_from_slice(&self.bits);
        out
    }
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 13 || &bytes[..8] != ID_FILTER_MAGIC {
            return None;
        }
        let probes = bytes[8];
        let len = u32::from_le_bytes(bytes[9..13].try_into().ok()?) as usize;
        let (min, max): (Id, Id) = serde_json::from_slice(bytes.get(13..13 + len)?).ok()?;
        let bits = bytes.get(13 + len..)?.to_vec();
        (probes > 0 && !bits.is_empty()).then_some(Self {
            min,
            max,
            probes,
            bits,
        })
    }
    fn resident_bytes(&self) -> u64 {
        (self.bits.capacity() + 128) as u64
    }
    /// The wanted IDs this segment might hold: those inside its ID range that pass the Bloom filter.
    fn candidates(&self, wanted: &std::collections::BTreeSet<Id>) -> Vec<Id> {
        let modulus = self.bits.len() as u64 * 8;
        wanted
            .range(&self.min..=&self.max)
            .filter(|id| {
                let (h1, h2) = id_hash(id);
                (0..self.probes as u64).all(|i| {
                    let bit = h1.wrapping_add(i.wrapping_mul(h2)) % modulus;
                    self.bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0
                })
            })
            .cloned()
            .collect()
    }
}
/// The segment's ID filter, or `None` for a segment built before filters existed.
async fn id_filter(store: &Storage, segment: &Segment) -> Result<Option<Arc<IdFilter>>, ApiError> {
    let Some(file) = segment.files.get("idfilter") else {
        return Ok(None);
    };
    let key = format!("idfilter:{}", file.key);
    if let Some(filter) = read_cache::get(&key, file.size) {
        return Ok(Some(filter));
    }
    let bytes = ObjectReader::new(store, file)
        .read(0..file.size)
        .await
        .map_err(index_error)?;
    let filter = IdFilter::from_bytes(&bytes).ok_or_else(corrupt)?;
    let size = filter.resident_bytes();
    Ok(Some(read_cache::insert(&key, size, filter)))
}
/// Build documents and filter dictionaries in sorted global-ID order.
/// Tokenize in parallel (bounded chunks keep memory flat), then feed the builder in row order.
fn build_text(
    values: Vec<Option<Value>>,
    params: bf_text::FtsParams,
) -> Result<(Vec<u8>, u64, Option<bf_wal::TextHeaderRef>), ApiError> {
    use rayon::prelude::*;
    let mut builder = bf_text::SegmentBuilder::with_tokenizer(true, params.tokenizer);
    for chunk in values.chunks(1024) {
        let tokens: Vec<Option<Vec<bf_text::Token>>> = chunk
            .par_iter()
            .map(|value| value.as_ref().map(|v| crate::text::tokens(v, &params)))
            .collect();
        for tokens in tokens {
            match tokens {
                Some(tokens) => {
                    builder.add_document(&tokens).map_err(index_error)?;
                }
                None => {
                    builder.add_missing_document().map_err(index_error)?;
                }
            }
        }
    }
    text_file_parts(builder.finish().map_err(index_error)?)
}

fn text_file_parts(
    bytes: Vec<u8>,
) -> Result<(Vec<u8>, u64, Option<bf_wal::TextHeaderRef>), ApiError> {
    // bf-text v1 stores the document-length region offset at footer byte 32.
    let footer = &bytes[bytes.len() - 72..];
    let lengths_offset = u64::from_le_bytes(footer[32..40].try_into().expect("v1 footer"));
    let metadata_len = bytes.len() as u64 - lengths_offset;
    let header = bf_text::text_header(&bytes)
        .map_err(index_error)?
        .map(|h| {
            Ok::<_, ApiError>(bf_wal::TextHeaderRef {
                footer: h.footer.iter().map(|b| format!("{b:02x}")).collect(),
                chunks: h
                    .chunks
                    .into_iter()
                    .map(|(first, o, l, c)| {
                        Ok((String::from_utf8(first).map_err(index_error)?, o, l, c))
                    })
                    .collect::<Result<_, ApiError>>()?,
            })
        })
        .transpose()?;
    Ok((bytes, metadata_len, header))
}

/// Whether a text object needs the current layout or a manifest header.
pub(crate) fn text_needs_upgrade(file: &SegmentFile) -> bool {
    file.text_header
        .as_ref()
        .is_none_or(|h| text_header_of(h).is_ok_and(|h| !h.is_current()))
}

pub(crate) async fn upgrade_text(
    store: &Storage,
    ns: &str,
    seg: &mut Segment,
) -> Result<Option<String>, ApiError> {
    let Some((key, old)) = seg
        .files
        .iter()
        .find(|(key, file)| key.starts_with("fts:") && text_needs_upgrade(file))
        .map(|(k, f)| (k.clone(), f.clone()))
    else {
        return Ok(None);
    };
    let source = ObjectReader::new(store, &old)
        .bypass_cache()
        .read(0..old.size)
        .await
        .map_err(index_error)?
        .to_vec();
    let converted = tokio::task::spawn_blocking(move || bf_text::upgrade_segment(source))
        .await
        .map_err(index_error)?
        .map_err(index_error)?;
    let (bytes, metadata, header) = if let Some(bytes) = converted {
        text_file_parts(bytes)?
    } else {
        // Formats 1–3 lack colocated records. Only these layouts need document tokenization.
        let attribute = key.strip_prefix("fts:").expect("text file");
        let params = seg
            .schema
            .get(attribute)
            .and_then(|s| s.text_config())
            .ok_or_else(corrupt)?;
        let rows = (0..seg.row_count).collect();
        let columns = std::collections::BTreeSet::from([attribute.to_string()]);
        let mut documents = projected_rows_with(store, seg, &rows, Some(&columns), true).await?;
        let values = (0..seg.row_count)
            .map(|row| {
                documents
                    .get_mut(&row)
                    .and_then(|d| d.remove(attribute))
                    .filter(|v| !v.is_null())
            })
            .collect();
        drop(documents);
        tokio::task::spawn_blocking(move || build_text(values, params))
            .await
            .map_err(index_error)??
    };
    let mut file = put_file(
        store,
        format!(
            "ns/{ns}/segments/{}/fts.{}.bft",
            seg.id,
            uuid::Uuid::new_v4()
        ),
        bytes,
        metadata,
    )
    .await?;
    file.text_header = header;
    seg.files.insert(key, file);
    Ok(Some(old.key))
}

/// One vector attribute's values at the stored precision, for the rows that have one: row
/// positions (ascending, within the segment being built) and their packed little-endian bytes.
pub(crate) struct PackedColumn {
    pub dims: usize,
    pub dtype: bf_vector::VectorType,
    pub rows: Vec<u32>,
    pub data: Vec<u8>,
}
impl PackedColumn {
    pub fn new(dims: usize, dtype: bf_vector::VectorType) -> Self {
        Self {
            dims,
            dtype,
            rows: Vec::new(),
            data: Vec::new(),
        }
    }
    /// Append a vector for row `row` (rows must be pushed in ascending order).
    pub fn push(&mut self, row: u32, v: &[f32]) -> Result<(), ApiError> {
        self.dtype
            .pack(v, self.dims, &mut self.data)
            .map_err(index_error)?;
        self.rows.push(row);
        Ok(())
    }
}
/// The dimensions and stored precision of a dense vector attribute (None for any other type).
pub(crate) fn dense_type(
    config: &bf_types::AttributeSchema,
) -> Option<(usize, bf_vector::VectorType)> {
    match config.data_type {
        Some(AttributeType::Vector { dimensions, f16 }) => Some((
            dimensions,
            if f16 {
                bf_vector::VectorType::F16
            } else {
                bf_vector::VectorType::F32
            },
        )),
        Some(AttributeType::VectorI8 { dimensions }) => {
            Some((dimensions, bf_vector::VectorType::I8))
        }
        _ => None,
    }
}

/// Build a segment from `docs` (ID order). A vector attribute in `vectors` is taken from there
/// (and `docs` need not carry it); any other is packed from the documents. Vectors stay at their
/// stored precision from here on: the ANN build, which runs on a blocking worker, owns the packed
/// bytes (4 KB per row less than a decoded f32 copy), with no duplicate vector column in the document object.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_with<D: std::borrow::Borrow<Document>>(
    store: &Storage,
    ns: &str,
    schema: &Schema,
    metric: bf_types::DistanceMetric,
    docs: &BTreeMap<Id, D>,
    mut vectors: BTreeMap<String, PackedColumn>,
    start: u64,
    end: u64,
) -> Result<Segment, ApiError> {
    let row_count =
        u32::try_from(docs.len()).map_err(|_| ApiError::bad("segment exceeds u32 rows"))?;
    let id = segment_id();
    let prefix = format!("ns/{ns}/segments/{id}");
    let mut files = BTreeMap::new();
    let rows: Vec<&Document> = docs.values().map(|d| d.borrow()).collect();
    for (name, config) in schema {
        if let Some((dims, dtype)) = dense_type(config)
            && !vectors.contains_key(name)
        {
            let mut column = PackedColumn::new(dims, dtype);
            for (i, row) in rows.iter().enumerate() {
                if let Some(v) = row.get(name).filter(|v| !v.is_null()) {
                    column.push(i as u32, &bf_types::decode_vector(v)?)?;
                }
            }
            vectors.insert(name.clone(), column);
        }
    }
    // Index builds are CPU-bound: start them on blocking workers as soon as a column's blocks
    // are written, so they overlap with document encoding and uploads below.
    let mut ann_jobs = Vec::new();
    for (column, (name, config)) in schema.iter().enumerate() {
        let Some((dimensions, dtype)) = dense_type(config) else {
            continue;
        };
        let packed = vectors.remove(name).ok_or_else(corrupt)?;
        if packed.dims != dimensions || packed.dtype != dtype {
            return Err(corrupt());
        }
        let metric = vector_metric(metric);
        // Leaf clusters: sqrt(rows).
        let job = tokio::task::spawn_blocking(move || {
            let params = bf_vector::BuildParams {
                clusters: Some(((packed.rows.len() as f64).sqrt().ceil() as usize).max(1)),
                ..Default::default()
            };
            bf_vector::IndexBuilder::new(dimensions, metric)?
                .params(params)
                .vector_type(dtype)
                .build_packed(packed.rows, packed.data)
        });
        ann_jobs.push((name.clone(), column, job));
    }
    let mut fts_jobs = Vec::new();
    for (column, (name, config)) in schema.iter().enumerate() {
        if let Some(params) = config.text_config() {
            let values: Vec<Option<Value>> = rows
                .iter()
                .map(|row| row.get(name).filter(|v| !v.is_null()).cloned())
                .collect();
            let job = tokio::task::spawn_blocking(move || build_text(values, params));
            fts_jobs.push((name.clone(), column, job));
        }
    }
    let (bytes, meta) = encode_documents(schema, &rows)?;
    files.insert(
        "docs".into(),
        put_file(store, format!("{prefix}/docs.bfc"), bytes, meta).await?,
    );
    let ids: Vec<_> = docs.keys().cloned().collect();
    let filter = IdFilter::build(&ids);
    files.insert(
        "idfilter".into(),
        put_file(
            store,
            format!("{prefix}/idfilter.bff"),
            filter.to_bytes(),
            0,
        )
        .await?,
    );
    if !fts_jobs.is_empty() {
        files.insert(
            crate::text_keys::ROLE.into(),
            crate::text_keys::put(store, &prefix, &ids).await?,
        );
    }
    files.insert(
        "ids".into(),
        put_value(store, format!("{prefix}/ids.bfi"), &IdIndex { ids }).await?,
    );
    let mut body = vec![];
    let mut dir = Directory::default();
    for (name, cfg) in schema {
        if name != "id" && cfg.filterable != Some(true) {
            continue;
        }
        let mut values: BTreeMap<String, (Value, RoaringBitmap)> = BTreeMap::new();
        for (row, doc) in rows.iter().enumerate() {
            let value = doc.get(name).unwrap_or(&Value::Null);
            values
                .entry(value.to_string())
                .or_insert_with(|| (value.clone(), RoaringBitmap::new()))
                .1
                .insert(row as u32);
        }
        let mut values: Vec<_> = values.into_values().collect();
        values.sort_by(|a, b| filter::compare(&a.0, &b.0));
        let b = block(&mut body, &encode(&values)?);
        dir.columns.insert(
            name.clone(),
            Column {
                compressed: false,
                blocks: vec![b],
                vector: None,
                vector_i8: false,
            },
        );
    }
    let (bytes, meta) = finish(body, &dir)?;
    files.insert(
        "filters".into(),
        put_file(store, format!("{prefix}/filter.bfx"), bytes, meta).await?,
    );
    for (column, (name, config)) in schema.iter().enumerate() {
        if config.fuzzy == Some(true) || config.regex == Some(true) || config.glob == Some(true) {
            files.insert(
                format!("trigram:{name}"),
                put_value(
                    store,
                    format!("{prefix}/trigram.{column}.bfx"),
                    &crate::pattern::Trigrams::build(docs, name),
                )
                .await?,
            );
        }
    }
    crate::sparse::build(store, &prefix, schema, docs, &mut files).await?;
    // Await each index and upload it as soon as it is ready, all of them at once.
    let mut uploads = Vec::new();
    for (name, column, job) in ann_jobs {
        let (store, prefix) = (store.clone(), prefix.clone());
        uploads.push(Box::pin(async move {
            let bytes = job.await.map_err(index_error)?.map_err(index_error)?;
            let (bytes, meta) = bf_vector::Fragment::build_ann(&bytes).map_err(index_error)?;
            let file = put_file(&store, format!("{prefix}/ann.{column}.bfa"), bytes, meta).await?;
            Ok::<_, ApiError>(vec![(format!("annv2:{name}"), file)])
        })
            as std::pin::Pin<
                Box<dyn Future<Output = Result<Vec<(String, SegmentFile)>, ApiError>> + Send>,
            >);
    }
    for (name, column, job) in fts_jobs {
        let (store, prefix) = (store.clone(), prefix.clone());
        uploads.push(Box::pin(async move {
            let (bytes, metadata_len, header) = job.await.map_err(index_error)??;
            let mut file = put_file(
                &store,
                format!("{prefix}/fts.{column}.bft"),
                bytes,
                metadata_len,
            )
            .await?;
            file.text_header = header;
            Ok::<_, ApiError>(vec![(format!("fts:{name}"), file)])
        }));
    }
    files.extend(
        futures::future::try_join_all(uploads)
            .await?
            .into_iter()
            .flatten(),
    );
    let live = put_value(
        store,
        format!("{prefix}/live.00000000000000000000.bfl"),
        &(0..row_count).collect::<RoaringBitmap>(),
    )
    .await?;
    Ok(Segment {
        id,
        wal_start: start,
        wal_end: end,
        row_count,
        schema: schema.clone(),
        files,
        live,
        live_generation: 0,
    })
}
pub(crate) async fn live(store: &Storage, segment: &Segment) -> Result<RoaringBitmap, ApiError> {
    let live: Arc<RoaringBitmap> = get_value(store, &segment.live).await?;
    if live.max().is_some_and(|row| row >= segment.row_count) {
        return Err(corrupt());
    }
    Ok((*live).clone())
}
pub(crate) async fn set_live(
    store: &Storage,
    ns: &str,
    segment: &mut Segment,
    rows: &RoaringBitmap,
) -> Result<(), ApiError> {
    segment.live_generation += 1;
    let prefix = format!("ns/{ns}/segments/{}", segment.id);
    // UUID distinguishes concurrent builders before either one wins publication.
    segment.live = put_value(
        store,
        format!(
            "{prefix}/live.{}-{}.bfl",
            segment.live_generation,
            uuid::Uuid::new_v4()
        ),
        rows,
    )
    .await?;
    Ok(())
}
pub(crate) async fn ids(store: &Storage, segment: &Segment) -> Result<Arc<IdIndex>, ApiError> {
    let file = segment.files.get("ids").ok_or_else(corrupt)?;
    let key = format!("validated:{}", file.key);
    if let Some(index) = read_cache::get(&key, file.size) {
        return Ok(index);
    }
    let mut reader = ObjectReader::new(store, file);
    let bytes = reader.read(0..file.size).await.map_err(index_error)?;
    let dir = decode_directory(&reader, &bytes, 0)?;
    reader.prefetched = Some((0, bytes));
    let b = dir
        .columns
        .get("value")
        .and_then(|c| c.blocks.first())
        .ok_or_else(corrupt)?;
    let index: IdIndex = decode(&read_block(&reader, b).await?)?;
    if index.ids.len() != segment.row_count as usize || index.ids.windows(2).any(|w| w[0] >= w[1]) {
        return Err(corrupt());
    }
    Ok(read_cache::insert(
        &key,
        (index.ids.capacity() * std::mem::size_of::<Id>()
            + index
                .ids
                .iter()
                .map(|id| match id {
                    Id::String(value) => value.capacity(),
                    _ => 0,
                })
                .sum::<usize>()) as u64,
        index,
    ))
}
pub(crate) async fn lookup_many(
    store: &Storage,
    segment: &Segment,
    wanted: &std::collections::BTreeSet<Id>,
) -> Result<RoaringBitmap, ApiError> {
    if wanted.is_empty() {
        return Ok(RoaringBitmap::new());
    }
    // The small filter first: most segments hold none of the wanted IDs, and then their (large)
    // ID lists are never read.
    let narrowed;
    let wanted = match id_filter(store, segment).await? {
        Some(filter) => {
            narrowed = filter.candidates(wanted).into_iter().collect();
            if std::collections::BTreeSet::is_empty(&narrowed) {
                return Ok(RoaringBitmap::new());
            }
            &narrowed
        }
        None => wanted,
    };
    let index = ids(store, segment).await?;
    let mut rows = RoaringBitmap::new();
    for id in wanted {
        if let Ok(row) = index.ids.binary_search(id) {
            rows.insert(row as u32);
        }
    }
    Ok(rows)
}
pub(crate) async fn documents(
    store: &Storage,
    segment: &Segment,
    rows: &RoaringBitmap,
) -> Result<BTreeMap<Id, Document>, ApiError> {
    projected_documents(store, segment, rows, None).await
}
pub(crate) async fn projected_documents(
    store: &Storage,
    segment: &Segment,
    rows: &RoaringBitmap,
    columns: Option<&std::collections::BTreeSet<String>>,
) -> Result<BTreeMap<Id, Document>, ApiError> {
    if rows.is_empty() {
        return Ok(BTreeMap::new());
    }
    if columns.is_some_and(|c| c.is_empty()) {
        let ids = ids(store, segment).await?;
        return rows
            .iter()
            .map(|row| {
                let id = ids.get(row as usize).ok_or_else(corrupt)?.clone();
                let doc = Document::from_iter([(
                    "id".into(),
                    serde_json::to_value(&id).map_err(index_error)?,
                )]);
                Ok((id, doc))
            })
            .collect();
    }
    let rows_docs = projected_rows(store, segment, rows, columns).await?;
    rows_docs
        .into_values()
        .map(|doc| {
            let id = serde_json::from_value(doc.get("id").ok_or_else(corrupt)?.clone())
                .map_err(index_error)?;
            Ok((id, doc))
        })
        .collect()
}
/// Write an ID index object, as a segment build does.
#[cfg(test)]
pub(crate) async fn put_ids(
    store: &Storage,
    key: String,
    ids: Vec<Id>,
) -> Result<SegmentFile, ApiError> {
    put_value(store, key, &IdIndex { ids }).await
}
/// A segment's IDs in row order as packed little-endian `u64`s, built from the bytes of its ID
/// index for a local stage. `None` when any ID is not an unsigned integer.
pub(crate) fn packed_numeric_ids(
    store: &Storage,
    file: &SegmentFile,
    rows: u32,
    bytes: Bytes,
) -> Result<Option<Vec<u8>>, ApiError> {
    let reader = ObjectReader::new(store, file);
    let dir = decode_directory(&reader, &bytes, 0)?;
    let b = dir
        .columns
        .get("value")
        .and_then(|c| c.blocks.first())
        .ok_or_else(corrupt)?;
    let data = bytes
        .get(b.offset as usize..b.offset.checked_add(b.length).ok_or_else(corrupt)? as usize)
        .ok_or_else(corrupt)?;
    if crc32fast::hash(data) != b.crc {
        return Err(corrupt());
    }
    let index: IdIndex = decode(data)?;
    if index.ids.len() != rows as usize || index.ids.windows(2).any(|w| w[0] >= w[1]) {
        return Err(corrupt());
    }
    let mut packed = Vec::with_capacity(index.ids.len() * 8);
    for id in &index.ids {
        match id {
            Id::Uint(n) => packed.extend_from_slice(&n.to_le_bytes()),
            Id::String(_) => return Ok(None),
        }
    }
    Ok(Some(packed))
}
/// Rows for hydrating search winners. An ID-only projection reads a few bytes of the segment's
/// staged row-to-ID map when a local stage holds one, so the response costs no storage request;
/// otherwise this is [`projected_rows`].
pub(crate) async fn hydrated_rows(
    store: &Storage,
    segment: &Segment,
    rows: &RoaringBitmap,
    columns: &std::collections::BTreeSet<String>,
) -> Result<BTreeMap<u32, Document>, ApiError> {
    if columns.is_empty()
        && let Some(file) = segment.files.get("ids")
        && let Some(ids) = crate::local_ann::staged_row_ids(&file.key, rows)
    {
        return rows
            .iter()
            .zip(ids)
            .map(|(row, id)| Ok((row, Document::from_iter([("id".into(), Value::from(id))]))))
            .collect();
    }
    projected_rows(store, segment, rows, Some(columns)).await
}
pub(crate) async fn projected_rows(
    store: &Storage,
    segment: &Segment,
    rows: &RoaringBitmap,
    columns: Option<&std::collections::BTreeSet<String>>,
) -> Result<BTreeMap<u32, Document>, ApiError> {
    projected_rows_with(store, segment, rows, columns, false).await
}
/// [`projected_rows`], optionally reading through the block caches without filling them.
pub(crate) async fn projected_rows_with(
    store: &Storage,
    segment: &Segment,
    rows: &RoaringBitmap,
    columns: Option<&std::collections::BTreeSet<String>>,
    bypass: bool,
) -> Result<BTreeMap<u32, Document>, ApiError> {
    if rows.is_empty() {
        return Ok(BTreeMap::new());
    }
    let docs_file = segment.files.get("docs").ok_or_else(corrupt)?;
    let docs_key = &docs_file.key;
    let mut reader = ObjectReader::new(store, docs_file);
    if bypass {
        reader = reader.bypass_cache();
    }
    let trace_start = crate::ann::trace_stages().then(std::time::Instant::now);
    let dir = directory(&reader).await?;
    trace_hydration(trace_start, docs_key, "directory", rows.len());
    let trace_start = crate::ann::trace_stages().then(std::time::Instant::now);
    let mut docs: BTreeMap<u32, Document> = rows.iter().map(|row| (row, Document::new())).collect();
    let blocks: std::collections::BTreeSet<_> =
        rows.iter().map(|row| row as usize / BLOCK_ROWS).collect();
    let indices: Vec<_> = blocks.iter().copied().collect();
    // Columns are independent: read them all concurrently.
    let reader = &reader;
    let indices = &indices;
    let fetched = futures::future::try_join_all(
        dir.columns
            .iter()
            .filter(|(name, _)| *name == "id" || columns.is_none_or(|c| c.contains(*name)))
            .map(|(name, col)| async move {
                let mut found: Vec<(u32, Value)> = Vec::new();
                // Decoded column blocks are kept in the decoded cache: a warm top-10 query
                // hydrates about ten blocks of 256 rows, and parsing them again from JSON cost
                // about a fifth of its CPU.
                let block_key = |i: usize| format!("{docs_key}#{name}#{i}");
                let context = read_cache::context().filter(|_| !bypass);
                let mut decoded: Vec<Option<Arc<Vec<Option<Value>>>>> = indices
                    .iter()
                    .map(|&i| {
                        let b = col.blocks.get(i)?;
                        let hit = context.as_ref()?.cache.column(&block_key(i))?;
                        context.as_ref()?.stats.record_memory_hit(b.length);
                        Some(hit)
                    })
                    .collect();
                let missing: Vec<usize> = (0..indices.len())
                    .filter(|n| decoded[*n].is_none())
                    .collect();
                // Two bounded maintenance scans retain at most the previous 64-block batch
                // in aggregate. Query hydration retains its whole-block decoded cache.
                for batch in missing.chunks(if bypass { 32 } else { 64 }) {
                    let descriptors = batch
                        .iter()
                        .map(|n| col.blocks.get(indices[*n]).ok_or_else(corrupt))
                        .collect::<Result<Vec<_>, _>>()?;
                    let data = reader
                        .read_batch(
                            descriptors
                                .iter()
                                .map(|b| b.offset..b.offset + b.length)
                                .collect(),
                        )
                        .await
                        .map_err(index_error)?;
                    for ((&n, b), data) in batch.iter().zip(descriptors).zip(data) {
                        let i = indices[n];
                        if crc32fast::hash(&data) != b.crc {
                            return Err(corrupt());
                        }
                        let first = (i * BLOCK_ROWS) as u32;
                        let last = first
                            .saturating_add(BLOCK_ROWS as u32)
                            .min(segment.row_count);
                        if bypass && col.vector.is_none() {
                            found.extend(decode_selected_column(
                                &data,
                                col,
                                b,
                                first,
                                (last - first) as usize,
                                rows,
                            )?);
                            continue;
                        }
                        let values: Vec<Option<Value>> = if let Some((dims, f16)) = col.vector {
                            upgrade::decode_vectors(
                                &data,
                                (last - first) as usize,
                                dims,
                                f16,
                                col.vector_i8,
                            )?
                        } else {
                            decode_column(&data, col, b)?
                        };
                        if values.len() != (last - first) as usize {
                            return Err(corrupt());
                        }
                        let values = Arc::new(values);
                        if let Some(c) = &context {
                            c.cache.insert_column(&block_key(i), values.clone());
                        }
                        decoded[n] = Some(values);
                    }
                }
                if bypass && col.vector.is_none() {
                    return Ok((name, found));
                }
                for (&i, values) in indices.iter().zip(&decoded) {
                    let values = values.as_ref().ok_or_else(corrupt)?;
                    let first = (i * BLOCK_ROWS) as u32;
                    let last = first
                        .saturating_add(BLOCK_ROWS as u32)
                        .min(segment.row_count);
                    if values.len() != (last - first) as usize {
                        return Err(corrupt());
                    }
                    for row in rows.range(first..last) {
                        if let Some(value) = &values[(row - first) as usize] {
                            found.push((row, value.clone()));
                        }
                    }
                }
                Ok::<_, ApiError>((name, found))
            }),
    )
    .await?;
    for (name, found) in fetched {
        for (row, value) in found {
            docs.get_mut(&row).unwrap().insert(name.clone(), value);
        }
    }
    for (name, &(dims, f16, i8)) in &dir.vectors {
        if columns.is_some_and(|c| !c.contains(name)) {
            continue;
        }
        let (present, packed) = read_ann_packed(store, segment, rows, name, bypass).await?;
        let dtype = if i8 {
            bf_vector::VectorType::I8
        } else if f16 {
            bf_vector::VectorType::F16
        } else {
            bf_vector::VectorType::F32
        };
        let width = dims * dtype.width();
        for (row, bytes) in present.into_iter().zip(packed.chunks_exact(width)) {
            let value = if i8 {
                serde_json::json!(bytes.iter().map(|x| *x as i8).collect::<Vec<_>>())
            } else {
                let vector: Vec<f32> = if f16 {
                    bytes
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .map(|x| half::f16::from_le_bytes(*x).to_f32())
                        .collect()
                } else {
                    bytes
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .map(|x| f32::from_le_bytes(*x))
                        .collect()
                };
                Value::String(bf_types::encode_vector(&vector, false))
            };
            docs.get_mut(&row)
                .ok_or_else(corrupt)?
                .insert(name.clone(), value);
        }
    }
    trace_hydration(trace_start, docs_key, "data", rows.len());
    Ok(docs)
}
/// Diagnostic boundaries distinguish dependent directory and data waves, including cache hits.
fn trace_hydration(start: Option<std::time::Instant>, key: &str, phase: &str, rows: u64) {
    if let Some(start) = start {
        eprintln!(
            "HYDRATION {key} {phase} {rows} {} {}",
            start.elapsed().as_micros(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_micros())
        );
    }
}
/// A segment's rows for compaction: documents without the `strip` columns (vector attributes,
/// read separately) keyed by ID, plus each stripped column's present values as packed bytes
/// (the ID of each present row, in row order, and their stored-precision bytes). Reads bypass the
/// block caches. A vector costs its stored width here (2 KB at 1,024 dimensions in f16) rather
/// than a 5.5 KB base64 string plus JSON value overhead.
pub(crate) type SegmentVectors = BTreeMap<String, PackedPart>;
/// The IDs of a segment's present vectors, in row order, and their packed bytes.
pub(crate) type PackedPart = (Vec<Id>, Vec<u8>);
pub(crate) async fn compaction_rows(
    store: &Storage,
    segment: &Segment,
    rows: &RoaringBitmap,
    strip: &std::collections::BTreeSet<String>,
) -> Result<(BTreeMap<Id, Document>, SegmentVectors), ApiError> {
    if rows.is_empty() {
        return Ok((BTreeMap::new(), BTreeMap::new()));
    }
    let columns: std::collections::BTreeSet<String> = segment
        .schema
        .keys()
        .filter(|name| !strip.contains(*name))
        .cloned()
        .collect();
    let docs = projected_rows_with(store, segment, rows, Some(&columns), true).await?;
    let docs: BTreeMap<Id, Document> = docs
        .into_values()
        .map(|doc| {
            let id = serde_json::from_value(doc.get("id").ok_or_else(corrupt)?.clone())
                .map_err(index_error)?;
            Ok((id, doc))
        })
        .collect::<Result<_, ApiError>>()?;
    let mut vectors = SegmentVectors::new();
    let ids = ids(store, segment).await?;
    for name in strip {
        if !segment.schema.contains_key(name) {
            continue;
        }
        let (present, data) = read_packed(store, segment, rows, name).await?;
        let ids: Vec<Id> = present
            .iter()
            .map(|row| ids.get(*row as usize).cloned().ok_or_else(corrupt))
            .collect::<Result<_, _>>()?;
        vectors.insert(name.clone(), (ids, data));
    }
    Ok((docs, vectors))
}
/// The stored-precision bytes of one vector column for the requested rows: which of them have a
/// vector (ascending), and those vectors back to back. Reads block by block, bypassing the caches.
pub(crate) async fn read_packed(
    store: &Storage,
    segment: &Segment,
    rows: &RoaringBitmap,
    attribute: &str,
) -> Result<(Vec<u32>, Vec<u8>), ApiError> {
    read_ann_packed(store, segment, rows, attribute, true).await
}

/// Resident ANN metadata for projection and vector-width accounting. No vector payload is read.
pub(crate) async fn projection_directory(
    store: &Storage,
    segment: &Segment,
    file: &SegmentFile,
    bypass: bool,
) -> Result<Arc<bf_vector::Directory>, ApiError> {
    let reader = ObjectReader::new(store, file);
    let reader = if bypass {
        reader.bypass_cache()
    } else {
        reader
    };
    let key = format!("projection-directory:{}", file.key);
    let dir = if let Some(dir) = read_cache::get::<bf_vector::Directory>(&key, file.metadata_len) {
        dir
    } else {
        let metadata = reader
            .read(0..file.metadata_len)
            .await
            .map_err(index_error)?;
        let fragment = bf_vector::Fragment::decode(&metadata).map_err(index_error)?;
        let bytes = bf_vector::Directory::build(&[(
            segment.id.clone(),
            file.key.clone(),
            file.key.clone(),
            fragment,
        )])
        .map_err(index_error)?
        .ok_or_else(corrupt)?;
        let dir = bf_vector::Directory::decode(&bytes).map_err(index_error)?;
        read_cache::insert(&key, dir.resident_bytes(), dir)
    };
    Ok(dir)
}

async fn read_ann_packed(
    store: &Storage,
    segment: &Segment,
    rows: &RoaringBitmap,
    attribute: &str,
    bypass: bool,
) -> Result<(Vec<u32>, Vec<u8>), ApiError> {
    let Some(file) = segment.files.get(&format!("annv2:{attribute}")) else {
        if let Some(file) = segment.files.get(&format!("ann:{attribute}")) {
            let reader = ObjectReader::new(store, file);
            let reader = if bypass {
                reader.bypass_cache()
            } else {
                reader
            };
            let index = bf_vector::Index::open_with_metadata_len(reader, file.metadata_len)
                .await
                .map_err(index_error)?;
            return index.read_packed_rows(rows).await.map_err(index_error);
        }
        // While D1 is pending, the document-v1 column remains a safe source at every
        // checkpoint. It disappears only after the current ANN object is published.
        return upgrade::read_packed(store, segment, rows, attribute).await;
    };
    let mut reader = ObjectReader::new(store, file);
    if bypass {
        reader = reader.bypass_cache();
    }
    let dir = projection_directory(store, segment, file, bypass).await?;
    let (dims, dtype) = segment
        .schema
        .get(attribute)
        .and_then(dense_type)
        .ok_or_else(corrupt)?;
    let mut partial = dir.partial_index(0, reader.clone()).map_err(index_error)?;
    if dir.dims() != dims || partial.index().vector_type() != dtype {
        return Err(corrupt());
    }
    let membership =
        crate::ann_dir::memberships(store, std::slice::from_ref(segment), &dir, attribute)
            .await?
            .pop()
            .flatten();
    let counts = membership
        .as_ref()
        .map(|m| m.counts(rows, dir.segments()[0].clusters));
    // Sparse vector columns and flat objects do not have a row-to-cluster map. Their leaf
    // row sets supply membership instead, including the distinction between null and present.
    let entries: Vec<_> = dir
        .entries()
        .iter()
        .filter(|e| counts.as_ref().is_none_or(|c| c[e.cluster] > 0))
        .collect();
    let clusters = read_cache::context()
        .filter(|_| !bypass)
        .map(|c| c.cache.clusters(&file.key));
    for batch in entries.chunks(16) {
        let decoded: Vec<_> = batch
            .iter()
            .map(|entry| {
                clusters
                    .as_ref()
                    .and_then(|(cache, key)| cache.get(&(key.clone(), entry.cluster as u32)))
            })
            .collect();
        let blobs = reader
            .read_batch(
                batch
                    .iter()
                    .zip(&decoded)
                    .filter(|(_, d)| d.is_none())
                    .map(|(e, _)| e.blob_range())
                    .collect(),
            )
            .await
            .map_err(index_error)?;
        let mut blobs = blobs.iter();
        for (entry, known) in batch.iter().zip(decoded) {
            let decoded = match known {
                Some(decoded) => decoded,
                None => {
                    let decoded = partial
                        .decode(blobs.next().ok_or_else(corrupt)?)
                        .map_err(index_error)?;
                    if let Some((cache, key)) = &clusters {
                        cache.insert((key.clone(), entry.cluster as u32), decoded.clone());
                    }
                    decoded
                }
            };
            partial
                .install_decoded(entry.cluster, &decoded)
                .map_err(index_error)?;
        }
    }
    partial
        .index()
        .read_packed_rows(rows)
        .await
        .map_err(index_error)
}
/// Join the per-segment packed vectors of a compaction into one column per attribute in the
/// order of `docs` (the merged documents, by ID).
pub(crate) fn merge_packed<D>(
    schema: &Schema,
    docs: &BTreeMap<Id, D>,
    parts: BTreeMap<String, Vec<PackedPart>>,
) -> Result<BTreeMap<String, PackedColumn>, ApiError> {
    let mut out = BTreeMap::new();
    for (name, parts) in parts {
        let (dims, dtype) = schema.get(&name).and_then(dense_type).ok_or_else(corrupt)?;
        let width = dims * dtype.width();
        let mut at: BTreeMap<&Id, (usize, usize)> = BTreeMap::new();
        for (p, (ids, data)) in parts.iter().enumerate() {
            if data.len() != ids.len() * width {
                return Err(corrupt());
            }
            for (k, id) in ids.iter().enumerate() {
                at.insert(id, (p, k));
            }
        }
        let mut column = PackedColumn::new(dims, dtype);
        column.data.reserve(at.len() * width);
        for (row, id) in docs.keys().enumerate() {
            if let Some(&(p, k)) = at.get(id) {
                column.rows.push(row as u32);
                column
                    .data
                    .extend_from_slice(&parts[p].1[k * width..(k + 1) * width]);
            }
        }
        out.insert(name, column);
    }
    Ok(out)
}
/// Cache a predicate over one immutable live generation, before applying WAL shadows. Snapshot
/// prefetch and query planning share this result, rather than decoding and scanning a large
/// value dictionary twice on every query. Schema and file keys also fence schema-only changes.
pub(crate) async fn filter_live(
    store: &Storage,
    segment: &Segment,
    f: &Filter,
    schema: &Schema,
) -> Result<Arc<RoaringBitmap>, ApiError> {
    // Object keys name immutable bytes, so the file keys, sizes, and live generation identify the
    // segment. The bulky dictionary chunk indexes they carry derive from those objects and are
    // left out, since serializing them on every query cost about a fifth of a selective filter's CPU.
    let files: Vec<_> = segment
        .files
        .iter()
        .map(|(name, file)| (name, &file.key, file.size, file.metadata_len))
        .collect();
    let identity = (
        &segment.id,
        segment.wal_start,
        segment.wal_end,
        segment.row_count,
        &segment.schema,
        files,
        (
            &segment.live.key,
            segment.live.size,
            segment.live.metadata_len,
        ),
        segment.live_generation,
    );
    let key =
        serde_json::to_string(&("filter-live-v2", identity, f, schema)).map_err(index_error)?;
    if let Some(rows) = read_cache::predicate(&key) {
        return Ok(rows);
    }
    let live = live(store, segment).await?;
    let rows = filter_rows(store, segment, f, schema, &live).await?;
    Ok(read_cache::insert_predicate(&key, rows))
}

/// Evaluate scalar, array, range, and boolean filters over sorted value dictionaries.
pub(crate) fn filter_rows<'a>(
    store: &'a Storage,
    segment: &'a Segment,
    f: &'a Filter,
    schema: &'a Schema,
    live: &'a RoaringBitmap,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RoaringBitmap, ApiError>> + Send + 'a>>
{
    Box::pin(async move {
        match f {
            Filter::And(fs) => {
                let mut out = live.clone();
                for f in fs {
                    out &= filter_rows(store, segment, f, schema, &out).await?;
                }
                Ok(out)
            }
            Filter::Or(fs) => {
                let mut out = RoaringBitmap::new();
                for f in fs {
                    out |= filter_rows(store, segment, f, schema, live).await?;
                }
                Ok(out)
            }
            Filter::Not(f) => Ok(live - filter_rows(store, segment, f, schema, live).await?),
            Filter::Compare {
                attribute,
                op,
                value,
                params,
            } => {
                if matches!(
                    op,
                    bf_types::FilterOp::ContainsAllTokens
                        | bf_types::FilterOp::ContainsAnyToken
                        | bf_types::FilterOp::ContainsTokenSequence
                ) && seg_text_matches(segment, schema, attribute)
                    && let Some(file) = segment.files.get(&format!("fts:{attribute}"))
                {
                    let cfg = schema[attribute].text_config().expect("validated FTS");
                    let query = bf_text::Query::new(
                        crate::text::tokens(value, &cfg),
                        crate::text::prefix(params.as_ref()),
                    );
                    let reader = text_index(store, file).await?;
                    let rows = match op {
                        bf_types::FilterOp::ContainsAllTokens => {
                            reader.contains_all_tokens(&query).await
                        }
                        bf_types::FilterOp::ContainsAnyToken => {
                            reader.contains_any_token(&query).await
                        }
                        _ => reader.contains_token_sequence(&query).await,
                    }
                    .map_err(index_error)?;
                    return Ok(rows & live);
                }
                if matches!(
                    op,
                    bf_types::FilterOp::Fuzzy
                        | bf_types::FilterOp::Regex
                        | bf_types::FilterOp::Glob
                        | bf_types::FilterOp::NotGlob
                        | bf_types::FilterOp::IGlob
                        | bf_types::FilterOp::NotIGlob
                ) && let Some(file) = segment.files.get(&format!("trigram:{attribute}"))
                {
                    let index: std::sync::Arc<crate::pattern::Trigrams> =
                        get_value(store, file).await?;
                    let candidates = index.candidates(f, live)?;
                    let columns = std::collections::BTreeSet::from([attribute.clone()]);
                    let docs =
                        projected_documents(store, segment, &candidates, Some(&columns)).await?;
                    let ids = ids(store, segment).await?;
                    let mut out = RoaringBitmap::new();
                    for row in &candidates {
                        if filter::matches(f, docs.get(&ids[row as usize]), None, schema)? {
                            out.insert(row);
                        }
                    }
                    return Ok(
                        if matches!(
                            op,
                            bf_types::FilterOp::NotGlob | bf_types::FilterOp::NotIGlob
                        ) {
                            live - (candidates - out)
                        } else {
                            out
                        },
                    );
                }
                let reader =
                    ObjectReader::new(store, segment.files.get("filters").ok_or_else(corrupt)?);
                let dir = directory(&reader).await?;
                let Some(col) = dir.columns.get(attribute) else {
                    let columns = std::collections::BTreeSet::from([attribute.clone()]);
                    let docs = projected_documents(store, segment, live, Some(&columns)).await?;
                    let ids = ids(store, segment).await?;
                    let mut out = RoaringBitmap::new();
                    for row in live {
                        if filter::matches(f, docs.get(&ids[row as usize]), None, schema)? {
                            out.insert(row);
                        }
                    }
                    return Ok(out);
                };
                let key = format!("{}:{attribute}", reader.file.key);
                let values: Arc<Vec<(Value, RoaringBitmap)>> =
                    if let Some(values) = read_cache::get(&key, col.blocks[0].length) {
                        values
                    } else {
                        let value = decode(&read_block(&reader, &col.blocks[0]).await?)?;
                        read_cache::insert(&key, col.blocks[0].length.saturating_mul(32), value)
                    };
                let mut out = RoaringBitmap::new();
                // Sorted dictionary runs permit binary seeks for scalar ranges.
                // Canonicalization preserves datetime, UUID, and exact integer ordering.
                use bf_types::FilterOp::*;
                if matches!(op, Eq | NotEq | Lt | Lte | Gt | Gte) {
                    let operand =
                        if let Some(t) = schema.get(attribute).and_then(|s| s.data_type.as_ref()) {
                            crate::write::normalize_value(value, t)?
                        } else {
                            value.clone()
                        };
                    let low = values.partition_point(|(v, _)| filter::compare(v, &operand).is_lt());
                    let high =
                        values.partition_point(|(v, _)| !filter::compare(v, &operand).is_gt());
                    let range = match op {
                        Eq | NotEq => low..high,
                        Lt => 0..low,
                        Lte => 0..high,
                        Gt => high..values.len(),
                        Gte => low..values.len(),
                        _ => unreachable!(),
                    };
                    for (_, rows) in &values[range] {
                        out |= rows;
                    }
                    return Ok(if *op == NotEq { live - out } else { out & live });
                }
                for (value, rows) in values.iter() {
                    let doc = Document::from_iter([(attribute.clone(), value.clone())]);
                    if filter::matches(f, Some(&doc), None, schema)? {
                        out |= rows;
                    }
                }
                Ok(out & live)
            }
        }
    })
}

fn segment_id() -> String {
    const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let random = uuid::Uuid::new_v4().as_u128() & ((1u128 << 80) - 1);
    let mut bits = ((chrono::Utc::now().timestamp_millis() as u128) << 80) | random;
    let mut out = [b'0'; 26];
    for ch in out.iter_mut().rev() {
        *ch = ALPHABET[(bits & 31) as usize];
        bits >>= 5;
    }
    String::from_utf8(out.to_vec()).expect("ASCII ULID")
}

pub(crate) fn vector_metric(metric: bf_types::DistanceMetric) -> bf_vector::Metric {
    match metric {
        bf_types::DistanceMetric::CosineDistance => bf_vector::Metric::CosineDistance,
        bf_types::DistanceMetric::EuclideanSquared => bf_vector::Metric::EuclideanSquared,
    }
}
impl bf_vector::RangeRead for ObjectReader {
    async fn read_batch(&self, ranges: Vec<Range<u64>>) -> io::Result<Vec<Bytes>> {
        ObjectReader::read_batch(self, ranges).await
    }
    async fn read_batch_sparse(&self, ranges: Vec<Range<u64>>) -> io::Result<Vec<Bytes>> {
        ObjectReader::read_batch_sparse(self, ranges).await
    }
    fn len(&self) -> u64 {
        self.file.size
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        ObjectReader::read(self, range).await
    }
    fn caches_blocks(&self) -> bool {
        true
    }
}

#[async_trait::async_trait]
impl bf_text::RangeRead for ObjectReader {
    async fn read_batch(&self, ranges: Vec<Range<u64>>) -> io::Result<Vec<Bytes>> {
        ObjectReader::read_batch(self, ranges).await
    }
    fn len(&self) -> u64 {
        self.file.size
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        ObjectReader::read(self, range).await
    }
}
pub(crate) fn seg_text_matches(segment: &Segment, schema: &Schema, attribute: &str) -> bool {
    segment.schema.get(attribute).and_then(|s| s.text_config())
        == schema.get(attribute).and_then(|s| s.text_config())
}

#[cfg(test)]
mod id_filter_tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn filter_round_trips_and_has_a_tiny_false_positive_rate() {
        let ids: Vec<Id> = (0..50_000u64).map(|n| Id::Uint(n * 3 + 1_000)).collect();
        let filter = IdFilter::build(&ids);
        let again = IdFilter::from_bytes(&filter.to_bytes()).expect("decodes");
        assert_eq!((&again.min, &again.max), (&ids[0], ids.last().unwrap()));
        let present: BTreeSet<Id> = ids.iter().step_by(7).cloned().collect();
        assert_eq!(
            again.candidates(&present).len(),
            present.len(),
            "no false negatives"
        );
        // IDs outside the range never reach the Bloom filter; inside it, almost none pass.
        let outside: BTreeSet<Id> = (0..1_000u64).map(Id::Uint).collect();
        assert!(again.candidates(&outside).is_empty());
        let absent: BTreeSet<Id> = (0..200_000u64).map(|n| Id::Uint(n * 3 + 1_001)).collect();
        let false_positives = again.candidates(&absent).len();
        assert!(
            false_positives < 20,
            "{false_positives} false positives in 200,000"
        );
        assert!(IdFilter::from_bytes(b"BFIF0001").is_none());
    }

    #[test]
    fn string_ids_and_corrupt_bytes() {
        let mut ids: Vec<Id> = (0..1_000)
            .map(|n| Id::String(format!("doc-{n:05}")))
            .collect();
        ids.sort();
        let filter = IdFilter::from_bytes(&IdFilter::build(&ids).to_bytes()).unwrap();
        let wanted: BTreeSet<Id> = ids.iter().cloned().collect();
        assert_eq!(filter.candidates(&wanted).len(), 1_000);
        assert!(IdFilter::from_bytes(&[0u8; 40]).is_none());
    }
}

#[cfg(test)]
#[path = "document_tests.rs"]
mod document_tests;
