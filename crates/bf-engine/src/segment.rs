//! Immutable column blocks, ID dictionaries, filter bitmaps, and live generations.
use crate::{filter, storage_error};
use bf_storage::{CreateResult, Storage};
use bf_types::{ApiError, AttributeType, Document, Filter, Id, Schema};
use bf_wal::{Segment, SegmentFile};
use bytes::Bytes;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{collections::BTreeMap, io, ops::Range};

const MAGIC: &[u8; 8] = b"BFSEG001";
const FOOTER: usize = 32;
const BLOCK_ROWS: usize = 256;

#[derive(Clone, Serialize, Deserialize)]
struct Block {
    offset: u64,
    length: u64,
    crc: u32,
}
#[derive(Clone, Serialize, Deserialize)]
struct Column {
    blocks: Vec<Block>,
    /// Vector blocks use native f16/f32 little-endian elements, separate from attributes.
    vector: Option<(usize, bool)>,
}
#[derive(Default, Serialize, Deserialize)]
struct Directory {
    columns: BTreeMap<String, Column>,
}

/// A small adapter keeps storage/cache range semantics out of the on-disk formats.
#[derive(Clone)]
pub(crate) struct ObjectReader {
    pub store: Storage,
    pub file: SegmentFile,
}
impl ObjectReader {
    pub async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        self.store
            .get_range(&self.file.key, range)
            .await
            .map_err(io::Error::other)
    }
}

fn corrupt() -> ApiError {
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
    };
    body.extend_from_slice(bytes);
    b
}
fn finish(mut body: Vec<u8>, directory: &Directory) -> Result<(Vec<u8>, u64), ApiError> {
    let dir = encode(directory)?;
    let mut footer = Vec::with_capacity(FOOTER);
    footer.extend_from_slice(MAGIC);
    footer.extend_from_slice(&1u32.to_le_bytes());
    footer.extend_from_slice(&(body.len() as u64).to_le_bytes());
    footer.extend_from_slice(&(dir.len() as u32).to_le_bytes());
    footer.extend_from_slice(&crc32fast::hash(&dir).to_le_bytes());
    footer.extend_from_slice(&crc32fast::hash(&footer).to_le_bytes());
    let meta = (dir.len() + FOOTER) as u64;
    body.extend_from_slice(&dir);
    body.extend_from_slice(&footer);
    Ok((body, meta))
}
async fn directory(reader: &ObjectReader) -> Result<Directory, ApiError> {
    let size = reader.file.size;
    if size < FOOTER as u64 || reader.file.metadata_len > size {
        return Err(corrupt());
    }
    let start = size.saturating_sub(reader.file.metadata_len.max(65536));
    let bytes = reader.read(start..size).await.map_err(index_error)?;
    let footer = &bytes[bytes.len() - FOOTER..];
    if &footer[..8] != MAGIC
        || u32::from_le_bytes(footer[8..12].try_into().unwrap()) != 1
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
    let dir: Directory = decode(data)?;
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
    let file = SegmentFile {
        key,
        size: bytes.len() as u64,
        metadata_len,
    };
    if store
        .create(&file.key, bytes)
        .await
        .map_err(storage_error)?
        == CreateResult::Conflict
    {
        return Err(corrupt());
    }
    Ok(file)
}
async fn put_value<T: Serialize>(
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
                blocks: vec![b],
                vector: None,
            },
        )]),
    };
    let (bytes, meta) = finish(body, &dir)?;
    put_file(store, key, bytes, meta).await
}
async fn get_value<T: DeserializeOwned>(
    store: &Storage,
    file: &SegmentFile,
) -> Result<T, ApiError> {
    let reader = ObjectReader {
        store: store.clone(),
        file: file.clone(),
    };
    let dir = directory(&reader).await?;
    let b = dir
        .columns
        .get("value")
        .and_then(|c| c.blocks.first())
        .ok_or_else(corrupt)?;
    decode(&read_block(&reader, b).await?)
}

#[derive(Serialize, Deserialize)]
struct IdIndex {
    ids: Vec<Id>,
    bloom: Vec<u8>,
}
fn hashes(id: &Id, bits: usize) -> [usize; 3] {
    let data = serde_json::to_vec(id).expect("ID serializes");
    let h = data.iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ *b as u64).wrapping_mul(0x100000001b3)
    });
    [
        h as usize % bits,
        h.rotate_left(21) as usize % bits,
        h.rotate_left(42) as usize % bits,
    ]
}
/// Build documents and filter dictionaries in sorted global-ID order.
pub(crate) async fn build(
    store: &Storage,
    ns: &str,
    schema: &Schema,
    metric: bf_types::DistanceMetric,
    docs: &BTreeMap<Id, Document>,
    start: u64,
    end: u64,
) -> Result<Segment, ApiError> {
    let row_count =
        u32::try_from(docs.len()).map_err(|_| ApiError::bad("segment exceeds u32 rows"))?;
    let id = segment_id();
    let prefix = format!("ns/{ns}/segments/{id}");
    let mut files = BTreeMap::new();
    let mut body = vec![];
    let mut dir = Directory::default();
    let rows: Vec<_> = docs.values().collect();
    for (name, config) in schema {
        let vector = match config.data_type {
            Some(AttributeType::Vector { dimensions, f16 }) => Some((dimensions, f16)),
            _ => None,
        };
        let mut blocks = vec![];
        for chunk in rows.chunks(BLOCK_ROWS) {
            let data = if let Some((dims, f16)) = vector {
                let mut data = Vec::new();
                for row in chunk {
                    let v = row.get(name).filter(|v| !v.is_null());
                    data.push(u8::from(v.is_some()));
                    if let Some(v) = v {
                        let v = bf_types::decode_vector(v)?;
                        if v.len() != dims {
                            return Err(corrupt());
                        }
                        for x in v {
                            if f16 {
                                data.extend_from_slice(&half::f16::from_f32(x).to_le_bytes());
                            } else {
                                data.extend_from_slice(&x.to_le_bytes());
                            }
                        }
                    }
                }
                data
            } else {
                encode(&chunk.iter().map(|d| d.get(name)).collect::<Vec<_>>())?
            };
            blocks.push(block(&mut body, &data));
        }
        dir.columns.insert(name.clone(), Column { blocks, vector });
    }
    let (bytes, meta) = finish(body, &dir)?;
    files.insert(
        "docs".into(),
        put_file(store, format!("{prefix}/docs.bfc"), bytes, meta).await?,
    );
    let ids: Vec<_> = docs.keys().cloned().collect();
    let mut bloom = vec![0u8; (ids.len() * 10).div_ceil(8).max(1)];
    for id in &ids {
        for bit in hashes(id, bloom.len() * 8) {
            bloom[bit / 8] |= 1 << (bit % 8);
        }
    }
    files.insert(
        "ids".into(),
        put_value(store, format!("{prefix}/ids.bfi"), &IdIndex { ids, bloom }).await?,
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
                blocks: vec![b],
                vector: None,
            },
        );
    }
    let (bytes, meta) = finish(body, &dir)?;
    files.insert(
        "filters".into(),
        put_file(store, format!("{prefix}/filter.bfx"), bytes, meta).await?,
    );
    for (column, (name, config)) in schema.iter().enumerate() {
        if let Some(AttributeType::Vector { dimensions, f16 }) = config.data_type {
            let vectors = rows
                .iter()
                .enumerate()
                .filter_map(|(i, row)| row.get(name).map(|v| (i as u32, v)))
                .map(|(i, v)| Ok((i, bf_types::decode_vector(v)?)))
                .collect::<Result<Vec<_>, ApiError>>()?;
            let bytes = tokio::task::spawn_blocking(move || {
                bf_vector::IndexBuilder::new(dimensions, vector_metric(metric))?
                    .vector_type(if f16 {
                        bf_vector::VectorType::F16
                    } else {
                        bf_vector::VectorType::F32
                    })
                    .build(&vectors)
            })
            .await
            .map_err(index_error)?
            .map_err(index_error)?;
            let meta = bf_vector::metadata_len(&bytes).map_err(index_error)?;
            files.insert(
                format!("ann:{name}"),
                put_file(store, format!("{prefix}/ann.{column}.bfv"), bytes, meta).await?,
            );
        }
    }
    for (column, (name, config)) in schema.iter().enumerate() {
        if let Some(params) = config.text_config() {
            let mut builder = bf_text::SegmentBuilder::with_tokenizer(true, params.tokenizer);
            for row in &rows {
                if let Some(value) = row.get(name).filter(|v| !v.is_null()) {
                    builder
                        .add_document(&crate::text::tokens(value, &params))
                        .map_err(index_error)?;
                } else {
                    builder.add_missing_document().map_err(index_error)?;
                }
            }
            let bytes = builder.finish().map_err(index_error)?;
            let metadata_len = 72;
            files.insert(
                format!("fts:{name}"),
                put_file(
                    store,
                    format!("{prefix}/fts.{column}.bft"),
                    bytes,
                    metadata_len,
                )
                .await?,
            );
        }
    }
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
    let live: RoaringBitmap = get_value(store, &segment.live).await?;
    if live.max().is_some_and(|row| row >= segment.row_count) {
        return Err(corrupt());
    }
    Ok(live)
}
pub(crate) async fn set_live(
    store: &Storage,
    segment: &mut Segment,
    rows: &RoaringBitmap,
) -> Result<(), ApiError> {
    segment.live_generation += 1;
    let prefix = segment.live.key.rsplit_once('/').ok_or_else(corrupt)?.0;
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
pub(crate) async fn ids(store: &Storage, segment: &Segment) -> Result<Vec<Id>, ApiError> {
    let index: IdIndex = get_value(store, segment.files.get("ids").ok_or_else(corrupt)?).await?;
    if index.ids.len() != segment.row_count as usize || index.ids.windows(2).any(|w| w[0] >= w[1]) {
        return Err(corrupt());
    }
    Ok(index.ids)
}
pub(crate) async fn lookup_many(
    store: &Storage,
    segment: &Segment,
    wanted: &std::collections::BTreeSet<Id>,
) -> Result<RoaringBitmap, ApiError> {
    let index: IdIndex = get_value(store, segment.files.get("ids").ok_or_else(corrupt)?).await?;
    if index.bloom.is_empty() || index.ids.len() != segment.row_count as usize {
        return Err(corrupt());
    }
    let mut rows = RoaringBitmap::new();
    for id in wanted {
        if hashes(id, index.bloom.len() * 8)
            .iter()
            .any(|b| index.bloom[b / 8] & (1 << (b % 8)) == 0)
        {
            continue;
        }
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
    let reader = ObjectReader {
        store: store.clone(),
        file: segment.files.get("docs").ok_or_else(corrupt)?.clone(),
    };
    let dir = directory(&reader).await?;
    let mut docs: BTreeMap<u32, Document> = rows.iter().map(|row| (row, Document::new())).collect();
    for (name, col) in dir.columns {
        if name != "id" && columns.is_some_and(|c| !c.contains(&name)) {
            continue;
        }
        for (i, b) in col.blocks.iter().enumerate() {
            let first = (i * BLOCK_ROWS) as u32;
            let last = first
                .saturating_add(BLOCK_ROWS as u32)
                .min(segment.row_count);
            if rows.range(first..last).next().is_none() {
                continue;
            }
            let data = read_block(&reader, b).await?;
            let values: Vec<Option<Value>> = if let Some((dims, f16)) = col.vector {
                let mut pos = 0;
                let mut values = vec![];
                for _ in first..last {
                    let present = *data.get(pos).ok_or_else(corrupt)?;
                    pos += 1;
                    if present == 0 {
                        values.push(None);
                        continue;
                    }
                    if present != 1 {
                        return Err(corrupt());
                    }
                    let mut v = vec![];
                    for _ in 0..dims {
                        let width = if f16 { 2 } else { 4 };
                        let bytes = data.get(pos..pos + width).ok_or_else(corrupt)?;
                        let x = if f16 {
                            half::f16::from_le_bytes(bytes.try_into().unwrap()).to_f32()
                        } else {
                            f32::from_le_bytes(bytes.try_into().unwrap())
                        };
                        v.push(x);
                        pos += width;
                    }
                    values.push(Some(serde_json::json!(v)));
                }
                if pos != data.len() {
                    return Err(corrupt());
                }
                values
            } else {
                decode(&data)?
            };
            if values.len() != (last - first) as usize {
                return Err(corrupt());
            }
            for row in rows.range(first..last) {
                if let Some(value) = &values[(row - first) as usize] {
                    docs.get_mut(&row)
                        .unwrap()
                        .insert(name.clone(), value.clone());
                }
            }
        }
    }
    docs.into_values()
        .map(|doc| {
            let id = serde_json::from_value(doc.get("id").ok_or_else(corrupt)?.clone())
                .map_err(index_error)?;
            Ok((id, doc))
        })
        .collect()
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
                    let reader = bf_text::SegmentReader::open(std::sync::Arc::new(ObjectReader {
                        store: store.clone(),
                        file: file.clone(),
                    }))
                    .await
                    .map_err(index_error)?;
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
                let reader = ObjectReader {
                    store: store.clone(),
                    file: segment.files.get("filters").ok_or_else(corrupt)?.clone(),
                };
                let dir = directory(&reader).await?;
                let Some(col) = dir.columns.get(attribute) else {
                    let docs = documents(store, segment, live).await?;
                    let ids = ids(store, segment).await?;
                    let mut out = RoaringBitmap::new();
                    for row in live {
                        if filter::matches(f, docs.get(&ids[row as usize]), None, schema)? {
                            out.insert(row);
                        }
                    }
                    return Ok(out);
                };
                let values: Vec<(Value, RoaringBitmap)> =
                    decode(&read_block(&reader, &col.blocks[0]).await?)?;
                let mut out = RoaringBitmap::new();
                for (value, rows) in values {
                    let doc = Document::from_iter([(attribute.clone(), value)]);
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
    fn len(&self) -> u64 {
        self.file.size
    }
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        ObjectReader::read(self, range).await
    }
}

#[async_trait::async_trait]
impl bf_text::RangeRead for ObjectReader {
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
