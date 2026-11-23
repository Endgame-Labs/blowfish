#[path = "bundle.rs"]
mod bundle;
use crate::scoring::{Saturation, TermWeight};
use crate::{CorpusStats, FtsParams, Query, Token, Tokenizer};
use async_trait::async_trait;
use bitpacking::{BitPacker, BitPacker4x};
use bundle::VERSION_BUNDLED;
use bytes::Bytes;
use fst::{IntoStreamer, Streamer};
use futures::{StreamExt, TryStreamExt, future::try_join_all};
use roaring::RoaringBitmap;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::io::{self, ErrorKind};
use std::ops::Range;
use std::sync::Arc;

const BLOCK: usize = 128;
const FOOTER: usize = 72;
const MAGIC: &[u8; 8] = b"BFTEXT01";
/// Eager term metadata: postings, lengths, term metadata, dictionary.
const VERSION: u32 = 1;
/// Lazy term metadata: postings, term metadata, lengths, dictionary. The dictionary maps each
/// term to `(record offset << DF_BITS) | document frequency`, and every term's block directory is
/// a separately checksummed record that is read only when a query needs the term.
const VERSION_LAZY: u32 = 2;
/// Chunked dictionary and packed lengths: postings, term records, dictionary chunks, lengths, chunk index. Each
/// chunk is a small FST over `CHUNK_TERMS` consecutive terms with its own checksum, and the
/// index (first term, offset, length, checksum per chunk) sits next to the lengths in the
/// metadata tail, so a query reads only the chunks that hold its terms.
const VERSION_CHUNKED: u32 = 3;
/// Version 3 with each term's record stored immediately before its posting blocks, and the phrase
/// positions in one region after all postings. `metadata_offset` is where that positions region begins,
/// so records live in `[0, metadata_offset)` and a record's handle offset is absolute.
const VERSION_COLOCATED: u32 = 4;
/// Terms of at most this many blocks are read together with their record (version 4).
const SMALL_BLOCKS: usize = 32;
/// Upper bound of one compressed posting block.
const MAX_BLOCK_BYTES: usize = 1026;
/// Terms per dictionary chunk (version 3).
const CHUNK_TERMS: usize = 2048;
/// Decoded dictionary chunks kept per reader before the memo is reset.
const MEMO_CHUNKS: usize = 512;
/// Decoded posting payloads per immutable reader, evicted in insertion order.
const MEMO_POSTING_BYTES: usize = 8 << 20;
type Postings = Arc<[(u32, u32)]>;
#[derive(Default)]
struct DecodedPostings {
    blocks: std::collections::HashMap<u64, Postings>,
    order: std::collections::VecDeque<u64>,
    bytes: usize,
}
const DF_BITS: u32 = 28;
const DF_MASK: u64 = (1 << DF_BITS) - 1;
const RECORD_HEADER: usize = 8;
const BLOCK_META: usize = 52;
/// Decoded term records kept per reader before the memo is reset.
const MEMO_TERMS: usize = 16384;
const TAIL: u64 = 64 * 1024;
/// Windows whose posting blocks a prefetching search reads in its first batch.
const FIRST_WINDOWS: usize = 24;

fn invalid(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}
fn input(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message)
}
fn put32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn put64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn checked_slice(data: &[u8], range: Range<usize>) -> io::Result<&[u8]> {
    data.get(range)
        .ok_or_else(|| invalid("truncated text segment"))
}
struct Cursor<'a> {
    data: &'a [u8],
    at: usize,
}
impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, at: 0 }
    }
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .ok_or_else(|| invalid("overflowing length"))?;
        let data = checked_slice(self.data, self.at..end)?;
        self.at = end;
        Ok(data)
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}
fn verify(data: &[u8], checksum: u32) -> io::Result<()> {
    if crc32fast::hash(data) != checksum {
        Err(invalid("text segment checksum mismatch"))
    } else {
        Ok(())
    }
}

/// A cache-friendly immutable object abstraction. Implementations must return
/// exactly the requested bytes, and the object's length and contents must be stable.
#[async_trait]
pub trait RangeRead: Send + Sync {
    /// Read a half-open byte range from the immutable object.
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes>;
    /// Read independent ranges in request order. Cached backends should override
    /// this to coalesce aligned blocks before issuing bounded parallel requests.
    async fn read_batch(&self, ranges: Vec<Range<u64>>) -> io::Result<Vec<Bytes>> {
        futures::stream::iter(ranges)
            .map(|range| self.read(range))
            .buffered(32)
            .try_collect()
            .await
    }
    /// Object length in bytes.
    fn len(&self) -> u64;
    /// Whether the object contains no bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// An in-memory object, useful for tests, newly built segments, and small deployments.
#[derive(Clone, Debug)]
pub struct MemoryRangeRead(pub Bytes);
impl From<Vec<u8>> for MemoryRangeRead {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into())
    }
}
#[async_trait]
impl RangeRead for MemoryRangeRead {
    async fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        if range.start > range.end || range.end > self.len() {
            return Err(input("range outside object"));
        }
        Ok(self.0.slice(range.start as usize..range.end as usize))
    }
    fn len(&self) -> u64 {
        self.0.len() as u64
    }
}

#[derive(Clone, Debug)]
struct BlockMeta {
    first: u32,
    last: u32,
    count: u32,
    max_tf: u32,
    min_length: u32,
    offset: u64,
    size: u32,
    crc: u32,
    positions: u64,
    positions_size: u32,
    positions_crc: u32,
}
impl BlockMeta {
    fn encode(&self, out: &mut Vec<u8>) {
        for v in [
            self.first,
            self.last,
            self.count,
            self.max_tf,
            self.min_length,
        ] {
            put32(out, v);
        }
        put64(out, self.offset);
        put32(out, self.size);
        put32(out, self.crc);
        put64(out, self.positions);
        put32(out, self.positions_size);
        put32(out, self.positions_crc);
    }
    fn decode(c: &mut Cursor<'_>) -> io::Result<Self> {
        Ok(Self {
            first: c.u32()?,
            last: c.u32()?,
            count: c.u32()?,
            max_tf: c.u32()?,
            min_length: c.u32()?,
            offset: c.u64()?,
            size: c.u32()?,
            crc: c.u32()?,
            positions: c.u64()?,
            positions_size: c.u32()?,
            positions_crc: c.u32()?,
        })
    }
    fn upper_bound(&self, weight: &TermWeight, saturation: &Saturation) -> f32 {
        // Independent extrema form an upper bound under arbitrary corpus statistics.
        // The upward margin protects against f32 rounding near the heap threshold.
        weight.score_cached(self.max_tf, self.min_length, saturation) * (1.0 + 8.0 * f32::EPSILON)
    }
}
#[derive(Clone, Debug)]
struct TermMeta {
    df: u32,
    blocks: Vec<BlockMeta>,
}
/// One term's postings, stored flat: a posting is one `rows` entry and one `ends` entry (the
/// running count of positions through that posting), with every position in `positions`. A
/// `Vec<Posting>` with a position vector per posting cost about 48 bytes plus a heap allocation
/// per posting; this costs 8.
#[derive(Debug, Default)]
struct TermPostings {
    rows: Vec<u32>,
    ends: Vec<u32>,
    positions: Vec<(u32, u32)>,
}
impl TermPostings {
    /// The positions of the `i`th posting.
    fn of(&self, i: usize) -> &[(u32, u32)] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.positions[start..self.ends[i] as usize]
    }
}

/// Incremental segment writer. Rows are assigned consecutive zero-based ordinals;
/// use `add_missing_document` for a missing/null field to keep row alignment with other indexes.
#[derive(Default)]
pub struct SegmentBuilder {
    lengths: Vec<u32>,
    postings: BTreeMap<String, TermPostings>,
    positions: bool,
    tokenizer: Tokenizer,
}
impl SegmentBuilder {
    /// Start an index, optionally including the independent phrase-position stream.
    pub fn new(positions: bool) -> Self {
        Self {
            positions,
            ..Self::default()
        }
    }
    /// Select the field tokenizer for boolean subword semantics. It must match
    /// the tokenizer used to produce documents and queries.
    pub fn with_tokenizer(positions: bool, tokenizer: Tokenizer) -> Self {
        Self {
            positions,
            tokenizer,
            ..Self::default()
        }
    }
    /// Add one analyzed document and return its segment-local row ID.
    pub fn add_document(&mut self, tokens: &[Token]) -> io::Result<u32> {
        let row = u32::try_from(self.lengths.len()).map_err(|_| input("too many segment rows"))?;
        let length =
            u32::try_from(tokens.len().max(1)).map_err(|_| input("too many tokens in document"))?;
        let mut terms: BTreeMap<&str, Vec<(u32, u32)>> = BTreeMap::new();
        for token in tokens {
            terms
                .entry(&token.text)
                .or_default()
                .push((token.element, token.position));
        }
        self.lengths.push(length);
        for (term, mut positions) in terms {
            positions.sort_unstable();
            let entry = match self.postings.get_mut(term) {
                Some(entry) => entry,
                None => self.postings.entry(term.into()).or_default(),
            };
            entry.positions.extend_from_slice(&positions);
            u32::try_from(entry.positions.len()).map_err(|_| input("too many positions"))?;
            entry.rows.push(row);
            entry.ends.push(entry.positions.len() as u32);
        }
        Ok(row)
    }
    /// Add a missing/null field while preserving row alignment. Missing fields
    /// are excluded from BM25 corpus counts; present but empty fields count as
    /// one normalization token, matching the oracle.
    pub fn add_missing_document(&mut self) -> io::Result<u32> {
        let row = u32::try_from(self.lengths.len()).map_err(|_| input("too many segment rows"))?;
        self.lengths.push(0);
        Ok(row)
    }
    /// Produce immutable dictionary-bundled bytes. Older segments remain readable
    /// during automatic upgrades. No other crate's serialization is embedded.
    pub fn finish(self) -> io::Result<Vec<u8>> {
        bundle::bundle(self.finish_colocated()?)
    }

    fn finish_colocated(self) -> io::Result<Vec<u8>> {
        let packer = BitPacker4x::new();
        // Phase 1: compress every term's blocks. `post` holds the blocks of all terms in order and
        // `pos` the phrase positions; block offsets are relative until phase 2 places them.
        let mut post: Vec<u8> = Vec::new();
        let mut pos: Vec<u8> = Vec::new();
        let mut terms: Vec<(&str, Vec<BlockMeta>)> = Vec::with_capacity(self.postings.len());
        for (term, postings) in self.postings.iter() {
            let mut blocks = Vec::new();
            for first in (0..postings.rows.len()).step_by(BLOCK) {
                let last = (first + BLOCK).min(postings.rows.len());
                let rows = &postings.rows[first..last];
                let mut deltas = [0; BLOCK];
                let mut frequencies = [0; BLOCK];
                let mut previous = rows[0];
                for (i, row) in rows.iter().enumerate() {
                    deltas[i] = row - previous;
                    previous = *row;
                    frequencies[i] = postings.of(first + i).len() as u32;
                }
                let doc_bits = packer.num_bits(&deltas);
                let tf_bits = packer.num_bits(&frequencies);
                let mut compressed = vec![0; 2 + BLOCK * 8];
                compressed[0] = doc_bits;
                compressed[1] = tf_bits;
                let n = packer.compress(&deltas, &mut compressed[2..], doc_bits);
                let m = packer.compress(&frequencies, &mut compressed[2 + n..], tf_bits);
                compressed.truncate(2 + n + m);
                let offset = post.len() as u64;
                post.extend_from_slice(&compressed);
                let positions_offset = pos.len() as u64;
                if self.positions {
                    for i in first..last {
                        for &(element, position) in postings.of(i) {
                            put32(&mut pos, element);
                            put32(&mut pos, position);
                        }
                    }
                }
                let positions_size = pos.len() as u64 - positions_offset;
                let positions_crc = crc32fast::hash(&pos[positions_offset as usize..]);
                blocks.push(BlockMeta {
                    first: rows[0],
                    last: *rows.last().unwrap(),
                    count: rows.len() as u32,
                    max_tf: *frequencies.iter().max().unwrap(),
                    min_length: rows
                        .iter()
                        .map(|row| self.lengths[*row as usize])
                        .min()
                        .unwrap(),
                    offset,
                    size: compressed.len() as u32,
                    crc: crc32fast::hash(&compressed),
                    positions: positions_offset,
                    positions_size: u32::try_from(positions_size)
                        .map_err(|_| input("positions block too large"))?,
                    positions_crc,
                });
            }
            terms.push((term, blocks));
        }
        // Phase 2: each term's record sits immediately before its blocks, so one read returns a
        // short term's record and postings together. Phrase positions follow all postings.
        let record_size = |blocks: usize| RECORD_HEADER + BLOCK_META * blocks + 4;
        let postings_len = post.len() as u64
            + terms
                .iter()
                .map(|(_, b)| record_size(b.len()) as u64)
                .sum::<u64>();
        let mut bytes = Vec::with_capacity(postings_len as usize + pos.len());
        let mut chunk_terms: Vec<(&str, u64)> = Vec::new();
        let mut chunk_data: Vec<(String, Vec<u8>)> = Vec::new();
        let mut cursor = 0usize;
        for (term, mut blocks) in terms {
            let record_offset = bytes.len() as u64;
            let df: u64 = blocks.iter().map(|b| b.count as u64).sum();
            if df > DF_MASK || record_offset >> (64 - DF_BITS) != 0 {
                return Err(input("text segment too large"));
            }
            let blocks_start = record_offset + record_size(blocks.len()) as u64;
            let first = blocks[0].offset;
            let mut record = Vec::with_capacity(record_size(blocks.len()));
            put32(&mut record, df as u32);
            put32(&mut record, blocks.len() as u32);
            for block in &mut blocks {
                block.offset = blocks_start + (block.offset - first);
                block.positions += postings_len;
                block.encode(&mut record);
            }
            let crc = crc32fast::hash(&record);
            put32(&mut record, crc);
            bytes.extend_from_slice(&record);
            let last = blocks.last().unwrap();
            let length = (last.offset + last.size as u64 - blocks_start) as usize;
            bytes.extend_from_slice(&post[cursor..cursor + length]);
            cursor += length;
            chunk_terms.push((term, (record_offset << DF_BITS) | df));
            if chunk_terms.len() == CHUNK_TERMS {
                chunk_data.push(build_chunk(&chunk_terms)?);
                chunk_terms.clear();
            }
        }
        if !chunk_terms.is_empty() {
            chunk_data.push(build_chunk(&chunk_terms)?);
        }
        drop(post);
        debug_assert_eq!(bytes.len() as u64, postings_len);
        let metadata_offset = bytes.len() as u64;
        bytes.extend_from_slice(&pos);
        drop(pos);
        let mut index = Vec::new();
        put32(&mut index, chunk_data.len() as u32);
        for (first, blob) in &chunk_data {
            put32(&mut index, first.len() as u32);
            index.extend_from_slice(first.as_bytes());
            put64(&mut index, bytes.len() as u64);
            put32(&mut index, blob.len() as u32);
            put32(&mut index, crc32fast::hash(blob));
            bytes.extend_from_slice(blob);
        }
        let lengths_offset = bytes.len() as u64;
        encode_lengths(&self.lengths, &mut bytes);
        let dictionary_offset = bytes.len() as u64;
        bytes.extend_from_slice(&index);
        let lengths_crc =
            crc32fast::hash(&bytes[lengths_offset as usize..dictionary_offset as usize]);
        let dictionary_crc = crc32fast::hash(&bytes[dictionary_offset as usize..]);
        let mut footer = Vec::with_capacity(FOOTER);
        footer.extend_from_slice(MAGIC);
        put32(&mut footer, VERSION_COLOCATED);
        put32(&mut footer, u32::from(self.positions));
        put32(&mut footer, self.lengths.len() as u32);
        put32(&mut footer, self.tokenizer as u32);
        put64(&mut footer, self.lengths.iter().map(|&n| n as u64).sum());
        // Offsets keep the version-one field order: lengths first, then term metadata.
        // Term records are checksummed individually, so the metadata checksum is zero.
        for n in [lengths_offset, metadata_offset, dictionary_offset] {
            put64(&mut footer, n);
        }
        for crc in [lengths_crc, 0, dictionary_crc] {
            put32(&mut footer, crc);
        }
        let crc = crc32fast::hash(&footer);
        put32(&mut footer, crc);
        debug_assert_eq!(footer.len(), FOOTER);
        bytes.extend(footer);
        Ok(bytes)
    }
}

/// Document lengths, packed per 128-row block as `minimum (u32) | bits (u8) | packed offsets`.
/// Typical passages need about one byte per row, against four unpacked.
fn encode_lengths(lengths: &[u32], out: &mut Vec<u8>) {
    let packer = BitPacker4x::new();
    for block in lengths.chunks(BLOCK) {
        let min = *block.iter().min().unwrap();
        let mut offsets = [min; BLOCK];
        offsets[..block.len()].copy_from_slice(block);
        for v in &mut offsets {
            *v -= min;
        }
        let bits = packer.num_bits(&offsets);
        put32(out, min);
        out.push(bits);
        let start = out.len();
        out.resize(start + 16 * bits as usize, 0);
        packer.compress(&offsets, &mut out[start..], bits);
    }
}
fn decode_lengths(data: &[u8], docs: usize) -> io::Result<Vec<u32>> {
    let packer = BitPacker4x::new();
    let mut out = Vec::with_capacity(docs.div_ceil(BLOCK) * BLOCK);
    let mut at = 0;
    while out.len() < docs {
        let min = u32::from_le_bytes(checked_slice(data, at..at + 4)?.try_into().unwrap());
        let bits = *data
            .get(at + 4)
            .ok_or_else(|| invalid("truncated lengths"))?;
        let size = 16 * bits as usize;
        if bits > 32 {
            return Err(invalid("invalid length block"));
        }
        let packed = checked_slice(data, at + 5..at + 5 + size)?;
        let mut offsets = [0u32; BLOCK];
        packer.decompress(packed, &mut offsets, bits);
        for v in offsets {
            out.push(
                min.checked_add(v)
                    .ok_or_else(|| invalid("invalid document length"))?,
            );
        }
        at += 5 + size;
    }
    if at != data.len() {
        return Err(invalid("trailing document lengths"));
    }
    out.truncate(docs);
    Ok(out)
}

fn build_chunk(terms: &[(&str, u64)]) -> io::Result<(String, Vec<u8>)> {
    let mut builder = fst::MapBuilder::memory();
    for (term, handle) in terms {
        builder
            .insert(term, *handle)
            .map_err(|e| invalid(&e.to_string()))?;
    }
    let blob = builder.into_inner().map_err(|e| invalid(&e.to_string()))?;
    Ok((terms[0].0.to_owned(), blob))
}

/// One positive BM25 result. Scores descend, with the smaller row ID winning ties.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SearchHit {
    /// Segment-local document ordinal.
    pub row: u32,
    /// Positive BM25 score.
    pub score: f32,
}
impl Eq for SearchHit {}
impl Ord for SearchHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.row.cmp(&self.row))
    }
}
impl PartialOrd for SearchHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Diagnostics for checking pruning effectiveness and I/O budgets.
#[derive(Clone, Debug, Default)]
pub struct SearchMetrics {
    /// Posting blocks whose compressed payloads were read.
    pub blocks_read: usize,
    /// Row windows skipped using conservative block-max bounds.
    pub windows_pruned: usize,
    /// Rows scored after allowed-row filtering.
    pub rows_scored: usize,
}

/// Where a reader finds each term's block directory.
enum Terms {
    /// Version 1: every term decoded at open, addressed by dictionary ordinal.
    Eager(Vec<Arc<TermMeta>>),
    /// Version 2: records are read on demand, addressed by the dictionary value.
    Lazy {
        start: u64,
        end: u64,
        memo: std::sync::Mutex<std::collections::HashMap<u64, Arc<TermMeta>>>,
    },
}

struct DictionaryChunk {
    map: fst::Map<Bytes>,
    bytes: Bytes,
    offset: u64,
}

struct ChunkEntry {
    first: Vec<u8>,
    offset: u64,
    len: u32,
    crc: u32,
}
/// The term dictionary: one FST read whole (versions 1 and 2), or an index of small FST chunks
/// read on demand (version 3).
enum Dictionary {
    Whole(fst::Map<Bytes>),
    Chunked {
        entries: Vec<ChunkEntry>,
        memo: std::sync::Mutex<std::collections::HashMap<usize, DictionaryChunk>>,
    },
}

/// What a reader needs to start without reading the object: its footer and the dictionary's chunk
/// index `(first term, offset, length, crc32)`. The engine keeps it in the manifest (version 4 segments).
#[derive(Clone, Debug, PartialEq)]
pub struct TextHeader {
    /// The 72-byte footer.
    pub footer: Vec<u8>,
    /// Dictionary chunks in term order.
    pub chunks: Vec<(Vec<u8>, u64, u32, u32)>,
}
impl TextHeader {
    /// Whether this header describes the current dictionary-bundled layout.
    /// Older layouts remain readable while the indexer rebuilds them.
    pub fn is_current(&self) -> bool {
        self.footer.get(8..12) == Some(VERSION_BUNDLED.to_le_bytes().as_slice())
    }
}
/// The header of a finished version 4 or 5 segment (`None` for older versions).
pub fn text_header(bytes: &[u8]) -> io::Result<Option<TextHeader>> {
    if bytes.len() < FOOTER {
        return Err(invalid("text segment has no footer"));
    }
    let footer = &bytes[bytes.len() - FOOTER..];
    let mut c = Cursor::new(footer);
    if c.take(8)? != MAGIC {
        return Err(invalid("unknown text segment format"));
    }
    if !matches!(c.u32()?, VERSION_COLOCATED | VERSION_BUNDLED) {
        return Ok(None);
    }
    let mut c = Cursor::new(&footer[32..]);
    let (lengths_offset, metadata_offset, dictionary_offset) = (c.u64()?, c.u64()?, c.u64()?);
    let index = checked_slice(bytes, dictionary_offset as usize..bytes.len() - FOOTER)?;
    let entries = parse_chunk_index(index, metadata_offset, lengths_offset)?;
    Ok(Some(TextHeader {
        footer: footer.to_vec(),
        chunks: entries
            .into_iter()
            .map(|e| (e.first, e.offset, e.len, e.crc))
            .collect(),
    }))
}

/// An immutable segment. Opening reads the last 64 KiB, then any missing index
/// metadata in one additional read (version 2: just the lengths and dictionary, about
/// 20 bytes per distinct term, with each term's block directory read when a query needs it).
/// Posting payloads and phrase positions stay lazy.
pub struct SegmentReader {
    source: Arc<dyn RangeRead>,
    dictionary: Dictionary,
    terms: Terms,
    /// Per-row lengths: set at open, or read on demand (`ensure_lengths`) for a reader opened from a header.
    lengths: std::sync::OnceLock<Vec<u32>>,
    docs: u32,
    /// Where the packed lengths are, for a reader opened from a header: offset, end, and checksum.
    lazy_lengths: Option<(u64, u64, u32)>,
    total_tokens: u64,
    positions: bool,
    tokenizer: Tokenizer,
    /// End of the posting and position payloads.
    postings_end: u64,
    /// Version 4: each term's record is followed by its blocks (`SMALL_BLOCKS` and fewer are read with it).
    colocated: bool,
    bundled: bool,
    decoded: std::sync::Mutex<DecodedPostings>,
    saturation: std::sync::Mutex<Option<Arc<Saturation>>>,
}

fn check_block(
    b: &BlockMeta,
    docs: u32,
    previous: Option<u32>,
    postings_end: u64,
    positions: bool,
) -> io::Result<()> {
    if b.count == 0
        || b.count > BLOCK as u32
        || b.first > b.last
        || b.last >= docs
        || previous.is_some_and(|last| last >= b.first)
        || b.max_tf == 0
        || b.min_length == 0
        || b.size < 2
        || b.size > 1026
        || b.offset
            .checked_add(b.size as u64)
            .is_none_or(|end| end > postings_end)
        || b.positions
            .checked_add(b.positions_size as u64)
            .is_none_or(|end| end > postings_end)
        || (!positions && b.positions_size != 0)
    {
        return Err(invalid("invalid posting block metadata"));
    }
    Ok(())
}

impl SegmentReader {
    fn saturation(&self, stats: &CorpusStats, params: &FtsParams) -> Arc<Saturation> {
        let mut memo = self.saturation.lock().unwrap();
        if let Some(table) = memo.as_ref().filter(|table| table.matches(stats, params)) {
            return table.clone();
        }
        let table = Arc::new(Saturation::new(stats, params));
        *memo = Some(table.clone());
        table
    }

    /// Slice already fetched dictionary bundles before consulting the backend. This
    /// also works for uncached range readers, and keeps short postings query-local.
    async fn read_ranges(&self, ranges: Vec<Range<u64>>) -> io::Result<Vec<Bytes>> {
        let mut out = vec![Bytes::new(); ranges.len()];
        let mut missing = Vec::new();
        let mut slots = Vec::new();
        {
            let memo = match &self.dictionary {
                Dictionary::Chunked { memo, .. } if self.bundled => Some(memo.lock().unwrap()),
                _ => None,
            };
            for (i, range) in ranges.into_iter().enumerate() {
                let found = memo.as_ref().and_then(|memo| {
                    let Dictionary::Chunked { entries, .. } = &self.dictionary else {
                        return None;
                    };
                    let chunk = entries
                        .partition_point(|e| e.offset <= range.start)
                        .checked_sub(1)
                        .and_then(|i| memo.get(&i));
                    if let Some(chunk) =
                        chunk.filter(|chunk| range.end <= chunk.offset + chunk.bytes.len() as u64)
                    {
                        return Some(chunk.bytes.slice(
                            (range.start - chunk.offset) as usize
                                ..(range.end - chunk.offset) as usize,
                        ));
                    }
                    None
                });
                if let Some(bytes) = found {
                    out[i] = bytes;
                } else {
                    slots.push(i);
                    missing.push(range);
                }
            }
        }
        if !missing.is_empty() {
            let values = self.source.read_batch(missing).await?;
            if values.len() != slots.len() {
                return Err(invalid("invalid range batch"));
            }
            for (slot, value) in slots.into_iter().zip(values) {
                out[slot] = value;
            }
        }
        Ok(out)
    }

    fn decode_bundle(bytes: &Bytes) -> io::Result<fst::Map<Bytes>> {
        let mut c = Cursor::new(bytes);
        let len = c.u32()? as usize;
        let reserve = c.u32()? as usize;
        if len > reserve {
            return Err(invalid("invalid dictionary reservation"));
        }
        c.take(reserve)?;
        fst::Map::new(bytes.slice(8..8 + len)).map_err(|e| invalid(&e.to_string()))
    }

    /// Open and validate the footer, dictionary, and document lengths (and, for version-1
    /// segments, the whole block directory).
    pub async fn open(source: Arc<dyn RangeRead>) -> io::Result<Self> {
        let len = source.len();
        if len < FOOTER as u64 {
            return Err(invalid("text segment has no footer"));
        }
        let tail_start = len.saturating_sub(TAIL);
        let tail = read_exact(&*source, tail_start..len).await?;
        let raw_footer = &tail[tail.len() - FOOTER..];
        let mut c = Cursor::new(raw_footer);
        if c.take(8)? != MAGIC {
            return Err(invalid("unknown text segment format"));
        }
        let version = c.u32()?;
        if version != VERSION
            && version != VERSION_LAZY
            && version != VERSION_CHUNKED
            && version != VERSION_COLOCATED
            && version != VERSION_BUNDLED
        {
            return Err(invalid("unknown text segment format"));
        }
        let colocated = version >= VERSION_COLOCATED;
        let bundled = version == VERSION_BUNDLED;
        let chunked = version == VERSION_CHUNKED || colocated;
        let lazy = version == VERSION_LAZY || chunked;
        let flags = c.u32()?;
        if flags > 1 {
            return Err(invalid("unknown text segment flags"));
        }
        let docs = c.u32()?;
        let tokenizer = match c.u32()? {
            0 => Tokenizer::WordV4,
            1 => Tokenizer::WordV3,
            2 => Tokenizer::WordV2,
            3 => Tokenizer::WordV1,
            4 => Tokenizer::WordV0,
            5 => Tokenizer::Whitespace,
            6 => Tokenizer::Smartcn,
            7 => Tokenizer::Kuromoji,
            8 => Tokenizer::Nori,
            9 => Tokenizer::PreTokenizedArray,
            _ => return Err(invalid("unknown field tokenizer")),
        };
        let total_tokens = c.u64()?;
        let lengths_offset = c.u64()?;
        let metadata_offset = c.u64()?;
        let dictionary_offset = c.u64()?;
        let lengths_crc = c.u32()?;
        let metadata_crc = c.u32()?;
        let dictionary_crc = c.u32()?;
        verify(&raw_footer[..FOOTER - 4], c.u32()?)?;
        let footer_start = len - FOOTER as u64;
        let ordered = if lazy {
            metadata_offset <= lengths_offset
                && lengths_offset <= dictionary_offset
                && dictionary_offset < footer_start
                && (chunked || dictionary_offset - lengths_offset == docs as u64 * 4)
        } else {
            lengths_offset <= metadata_offset
                && metadata_offset <= dictionary_offset
                && dictionary_offset < footer_start
                && metadata_offset - lengths_offset == docs as u64 * 4
        };
        if !ordered {
            return Err(invalid("invalid text segment offsets"));
        }
        let index = if lengths_offset >= tail_start {
            tail.slice((lengths_offset - tail_start) as usize..tail.len() - FOOTER)
        } else {
            read_exact(&*source, lengths_offset..footer_start).await?
        };
        // Version 1 keeps `lengths | metadata | dictionary` contiguous. Version 2 keeps
        // `lengths | dictionary` here, with the term records earlier in the file.
        let (lengths_end, dict_start) = if lazy {
            let end = (dictionary_offset - lengths_offset) as usize;
            (end, end)
        } else {
            (
                (metadata_offset - lengths_offset) as usize,
                (dictionary_offset - lengths_offset) as usize,
            )
        };
        verify(&index[..lengths_end], lengths_crc)?;
        if !lazy {
            verify(&index[lengths_end..dict_start], metadata_crc)?;
        }
        verify(&index[dict_start..], dictionary_crc)?;
        let lengths: Vec<u32> = if chunked {
            decode_lengths(&index[..lengths_end], docs as usize)?
        } else {
            index[..lengths_end]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|n| u32::from_le_bytes(*n))
                .collect()
        };
        if lengths.iter().map(|&n| n as u64).sum::<u64>() != total_tokens {
            return Err(invalid("invalid document length sum"));
        }
        if chunked {
            let entries = parse_chunk_index(&index[dict_start..], metadata_offset, lengths_offset)?;
            return Ok(Self {
                source,
                dictionary: Dictionary::Chunked {
                    entries,
                    memo: Default::default(),
                },
                terms: Terms::Lazy {
                    start: if colocated { 0 } else { metadata_offset },
                    end: if colocated && !bundled {
                        metadata_offset
                    } else {
                        lengths_offset
                    },
                    memo: Default::default(),
                },
                lengths: lengths_cell(lengths),
                docs,
                lazy_lengths: None,
                total_tokens,
                positions: flags != 0,
                tokenizer,
                postings_end: if colocated {
                    lengths_offset
                } else {
                    metadata_offset
                },
                colocated,
                bundled,
                decoded: Default::default(),
                saturation: Default::default(),
            });
        }
        let dictionary =
            fst::Map::new(index.slice(dict_start..)).map_err(|e| invalid(&e.to_string()))?;
        if lazy {
            return Ok(Self {
                source,
                dictionary: Dictionary::Whole(dictionary),
                terms: Terms::Lazy {
                    start: metadata_offset,
                    end: lengths_offset,
                    memo: Default::default(),
                },
                lengths: lengths_cell(lengths),
                docs,
                lazy_lengths: None,
                total_tokens,
                positions: flags != 0,
                tokenizer,
                postings_end: metadata_offset,
                colocated: false,
                bundled: false,
                decoded: Default::default(),
                saturation: Default::default(),
            });
        }
        let mut c = Cursor::new(&index[lengths_end..dict_start]);
        let count = c.u32()? as usize;
        if count != dictionary.len() || count > c.data.len() / 8 {
            return Err(invalid("invalid term count"));
        }
        let mut terms = Vec::with_capacity(count);
        for _ in 0..count {
            let df = c.u32()?;
            let block_count = c.u32()? as usize;
            if df == 0
                || df > docs
                || block_count != (df as usize).div_ceil(BLOCK)
                || block_count > c.data.len() / 52
            {
                return Err(invalid("invalid posting count"));
            }
            let mut blocks = Vec::with_capacity(block_count);
            let mut previous = None;
            let mut posting_count = 0u64;
            for _ in 0..block_count {
                let b = BlockMeta::decode(&mut c)?;
                check_block(&b, docs, previous, lengths_offset, flags != 0)?;
                previous = Some(b.last);
                posting_count += b.count as u64;
                blocks.push(b);
            }
            if posting_count != df as u64 {
                return Err(invalid("inconsistent document frequency"));
            }
            terms.push(Arc::new(TermMeta { df, blocks }));
        }
        if c.at != c.data.len() {
            return Err(invalid("trailing term metadata"));
        }
        let mut stream = dictionary.stream();
        let mut ordinal = 0;
        while let Some((term, id)) = stream.next() {
            if id != ordinal || std::str::from_utf8(term).is_err() {
                return Err(invalid("invalid term dictionary"));
            }
            ordinal += 1;
        }
        Ok(Self {
            source,
            dictionary: Dictionary::Whole(dictionary),
            terms: Terms::Eager(terms),
            lengths: lengths_cell(lengths),
            docs,
            lazy_lengths: None,
            total_tokens,
            positions: flags != 0,
            tokenizer,
            postings_end: lengths_offset,
            colocated: false,
            bundled: false,
            decoded: Default::default(),
            saturation: Default::default(),
        })
    }
    /// Document frequency recorded in a dictionary handle.
    fn handle_df(&self, handle: u64) -> u32 {
        match &self.terms {
            Terms::Eager(terms) => terms[handle as usize].df,
            Terms::Lazy { .. } => (handle & DF_MASK) as u32,
        }
    }
    /// Block directories for dictionary handles. Version 2 reads only the missing records,
    /// in one batched request, and memoizes them for later queries.
    async fn load(
        &self,
        handles: impl IntoIterator<Item = u64>,
    ) -> io::Result<BTreeMap<u64, Arc<TermMeta>>> {
        let mut out = BTreeMap::new();
        match &self.terms {
            Terms::Eager(terms) => {
                for h in handles {
                    out.insert(h, terms[h as usize].clone());
                }
            }
            Terms::Lazy { start, end, memo } => {
                let mut missing = Vec::new();
                {
                    let memo = memo.lock().unwrap();
                    for h in handles {
                        match memo.get(&h) {
                            Some(meta) => {
                                out.insert(h, meta.clone());
                            }
                            None => missing.push(h),
                        }
                    }
                }
                missing.sort_unstable();
                missing.dedup();
                if missing.is_empty() {
                    return Ok(out);
                }
                let docs = self.num_docs();
                let mut ranges = Vec::with_capacity(missing.len());
                for &h in &missing {
                    let df = (h & DF_MASK) as usize;
                    let begin = start
                        .checked_add(h >> DF_BITS)
                        .ok_or_else(|| invalid("invalid term record"))?;
                    let size = RECORD_HEADER + BLOCK_META * df.div_ceil(BLOCK) + 4;
                    if df == 0 || begin + size as u64 > *end {
                        return Err(invalid("invalid term record"));
                    }
                    // A short term's blocks follow its record: read them in the same request, so
                    // the block cache holds the postings by the time a query asks for them.
                    let blocks = df.div_ceil(BLOCK);
                    let extra = if self.colocated && !self.bundled && blocks <= SMALL_BLOCKS {
                        blocks * MAX_BLOCK_BYTES
                    } else {
                        0
                    };
                    ranges.push(begin..(begin + (size + extra) as u64).min(*end));
                }
                let values = self.read_ranges(ranges.clone()).await?;
                if values.len() != ranges.len() {
                    return Err(invalid("invalid term record batch"));
                }
                let mut decoded = Vec::with_capacity(missing.len());
                for ((&h, range), bytes) in missing.iter().zip(&ranges).zip(values) {
                    if bytes.len() as u64 != range.end - range.start {
                        return Err(invalid("short term record"));
                    }
                    let size =
                        RECORD_HEADER + BLOCK_META * ((h & DF_MASK) as usize).div_ceil(BLOCK) + 4;
                    let bytes = &bytes[..size];
                    let body = &bytes[..size - 4];
                    verify(
                        body,
                        u32::from_le_bytes(bytes[size - 4..].try_into().unwrap()),
                    )?;
                    let mut c = Cursor::new(body);
                    let df = c.u32()?;
                    let count = c.u32()? as usize;
                    if df as u64 != h & DF_MASK || count != (df as usize).div_ceil(BLOCK) {
                        return Err(invalid("invalid posting count"));
                    }
                    let mut blocks = Vec::with_capacity(count);
                    let (mut previous, mut total) = (None, 0u64);
                    for _ in 0..count {
                        let b = BlockMeta::decode(&mut c)?;
                        check_block(&b, docs, previous, self.postings_end, self.positions)?;
                        previous = Some(b.last);
                        total += b.count as u64;
                        blocks.push(b);
                    }
                    if total != df as u64 || c.at != c.data.len() {
                        return Err(invalid("inconsistent document frequency"));
                    }
                    decoded.push((h, Arc::new(TermMeta { df, blocks })));
                }
                let mut memo = memo.lock().unwrap();
                if memo.len() + decoded.len() > MEMO_TERMS {
                    memo.clear();
                }
                for (h, meta) in decoded {
                    memo.insert(h, meta.clone());
                    out.insert(h, meta);
                }
            }
        }
        Ok(out)
    }
    /// Conservative cache charge for lengths, term metadata, dictionary bundles,
    /// and the bounded decoded-posting memo. A whole legacy FST's backing bytes
    /// are charged separately by the caller. Lazy lengths are charged before loading.
    pub fn resident_bytes(&self) -> u64 {
        const MALLOC_OVERHEAD: usize = 16;
        let terms = match &self.terms {
            Terms::Eager(terms) => {
                let blocks: usize = terms
                    .iter()
                    .map(|t| {
                        t.blocks.capacity() * std::mem::size_of::<BlockMeta>() + MALLOC_OVERHEAD
                    })
                    .sum();
                terms.capacity() * std::mem::size_of::<Arc<TermMeta>>() + blocks
            }
            // Memoized records are bounded by `MEMO_TERMS`; count a typical small term each.
            Terms::Lazy { .. } => MEMO_TERMS * 128,
        };
        let dictionary = match &self.dictionary {
            Dictionary::Whole(_) => 0,
            // Charge the largest possible resident bundles and their alias tables.
            Dictionary::Chunked { entries, .. } => {
                let mut sizes: Vec<_> = entries.iter().map(|e| e.len as usize).collect();
                sizes.sort_unstable_by(|a, b| b.cmp(a));
                entries.iter().map(|e| e.first.len() + 48).sum::<usize>()
                    + sizes.into_iter().take(MEMO_CHUNKS).sum::<usize>()
            }
        };
        (terms + dictionary + self.docs as usize * 4 + MEMO_POSTING_BYTES * 2 + Saturation::BYTES)
            as u64
    }
    /// The decoded lengths. Callers that read postings or score call `ensure_lengths` first.
    fn lens(&self) -> &[u32] {
        self.lengths.get().map_or(&[], Vec::as_slice)
    }
    /// Read the per-row lengths of a reader opened from a header (a no-op after the first call).
    pub async fn ensure_lengths(&self) -> io::Result<()> {
        if self.lengths.get().is_some() {
            return Ok(());
        }
        let Some((offset, end, crc)) = self.lazy_lengths else {
            return Err(invalid("missing document lengths"));
        };
        let bytes = read_exact(&*self.source, offset..end).await?;
        verify(&bytes, crc)?;
        let lengths = decode_lengths(&bytes, self.docs as usize)?;
        let sum: u64 = lengths.iter().map(|&n| n as u64).sum();
        if sum != self.total_tokens {
            return Err(invalid("invalid document length sum"));
        }
        let _ = self.lengths.set(lengths);
        Ok(())
    }
    /// Open from a header kept elsewhere (the manifest), without reading the object: the footer
    /// and the dictionary's chunk index. Document lengths load on first use. Versions 4
    /// and 5 have headers.
    pub fn open_with_header(source: Arc<dyn RangeRead>, header: &TextHeader) -> io::Result<Self> {
        let len = source.len();
        if header.footer.len() != FOOTER || len < FOOTER as u64 {
            return Err(invalid("invalid text header"));
        }
        let raw = &header.footer[..];
        let mut c = Cursor::new(raw);
        if c.take(8)? != MAGIC {
            return Err(invalid("unknown text segment format"));
        }
        let version = c.u32()?;
        if !matches!(version, VERSION_COLOCATED | VERSION_BUNDLED) {
            return Err(invalid("unknown text segment format"));
        }
        let flags = c.u32()?;
        let docs = c.u32()?;
        let tokenizer = tokenizer_of(c.u32()?)?;
        let total_tokens = c.u64()?;
        let lengths_offset = c.u64()?;
        let metadata_offset = c.u64()?;
        let dictionary_offset = c.u64()?;
        let lengths_crc = c.u32()?;
        let _metadata_crc = c.u32()?;
        let _dictionary_crc = c.u32()?;
        verify(&raw[..FOOTER - 4], c.u32()?)?;
        if flags > 1
            || !(metadata_offset <= lengths_offset
                && lengths_offset <= dictionary_offset
                && dictionary_offset < len - FOOTER as u64)
        {
            return Err(invalid("invalid text segment offsets"));
        }
        let mut entries: Vec<ChunkEntry> = Vec::with_capacity(header.chunks.len());
        for (first, offset, length, crc) in &header.chunks {
            if *offset < metadata_offset
                || offset
                    .checked_add(*length as u64)
                    .is_none_or(|end| end > lengths_offset)
                || entries.last().is_some_and(|p| p.first >= *first)
            {
                return Err(invalid("invalid dictionary chunk"));
            }
            entries.push(ChunkEntry {
                first: first.clone(),
                offset: *offset,
                len: *length,
                crc: *crc,
            });
        }
        Ok(Self {
            source,
            dictionary: Dictionary::Chunked {
                entries,
                memo: Default::default(),
            },
            terms: Terms::Lazy {
                start: 0,
                end: if version == VERSION_BUNDLED {
                    lengths_offset
                } else {
                    metadata_offset
                },
                memo: Default::default(),
            },
            lengths: std::sync::OnceLock::new(),
            docs,
            lazy_lengths: Some((lengths_offset, dictionary_offset, lengths_crc)),
            total_tokens,
            positions: flags != 0,
            tokenizer,
            postings_end: lengths_offset,
            colocated: true,
            bundled: version == VERSION_BUNDLED,
            decoded: Default::default(),
            saturation: Default::default(),
        })
    }
    /// Number of segment rows, including empty or missing fields.
    pub fn num_docs(&self) -> u32 {
        self.docs
    }
    /// Per-row normalization lengths. Present fields have a minimum of one;
    /// missing fields have length zero.
    pub fn doc_lengths(&self) -> &[u32] {
        self.lens()
    }
    /// Per-segment statistics for the engine's namespace-wide aggregation. Reads the whole
    /// dictionary (every chunk, for version 3 segments).
    pub async fn stats(&self) -> io::Result<CorpusStats> {
        self.ensure_lengths().await?;
        let present = self.lens().iter().filter(|&&n| n > 0).count() as u64;
        let mut stats = CorpusStats {
            total_docs: present,
            average_doc_length: if present == 0 {
                0.0
            } else {
                self.total_tokens as f64 / present as f64
            },
            ..CorpusStats::default()
        };
        for (term, id) in self.all_terms().await? {
            stats.document_frequencies.insert(
                String::from_utf8(term).map_err(|e| invalid(&e.to_string()))?,
                self.handle_df(id) as u64,
            );
        }
        Ok(stats)
    }
    /// Decoded chunks, fetched in one batch for those the memo lacks.
    async fn chunks(&self, wanted: &[usize]) -> io::Result<Vec<fst::Map<Bytes>>> {
        let Dictionary::Chunked { entries, memo } = &self.dictionary else {
            return Ok(Vec::new());
        };
        let mut have: BTreeMap<usize, fst::Map<Bytes>> = BTreeMap::new();
        let mut missing = Vec::new();
        {
            let memo = memo.lock().unwrap();
            for &c in wanted {
                match memo.get(&c) {
                    Some(map) => {
                        have.insert(c, map.map.clone());
                    }
                    None => missing.push(c),
                }
            }
        }
        missing.sort_unstable();
        missing.dedup();
        if !missing.is_empty() {
            let ranges = missing
                .iter()
                .map(|&c| entries[c].offset..entries[c].offset + entries[c].len as u64)
                .collect::<Vec<_>>();
            let values = self.source.read_batch(ranges).await?;
            if values.len() != missing.len() {
                return Err(invalid("invalid dictionary batch"));
            }
            let mut decoded = Vec::new();
            for (&c, bytes) in missing.iter().zip(values) {
                if bytes.len() != entries[c].len as usize {
                    return Err(invalid("short dictionary chunk"));
                }
                verify(&bytes, entries[c].crc)?;
                let map = if self.bundled {
                    Self::decode_bundle(&bytes)?
                } else {
                    fst::Map::new(bytes.clone()).map_err(|e| invalid(&e.to_string()))?
                };
                decoded.push((
                    c,
                    DictionaryChunk {
                        map,
                        bytes,
                        offset: entries[c].offset,
                    },
                ));
            }
            let mut memo = memo.lock().unwrap();
            if memo.len() + decoded.len() > MEMO_CHUNKS {
                memo.clear();
            }
            for (c, map) in decoded {
                have.insert(c, map.map.clone());
                memo.insert(c, map);
            }
            while memo.len() > MEMO_CHUNKS {
                let key = *memo.keys().next().expect("nonempty chunk memo");
                memo.remove(&key);
            }
        }
        Ok(wanted.iter().map(|c| have[c].clone()).collect())
    }
    /// Dictionary handles of exact terms, reading only the chunks that hold them.
    async fn lookup(&self, texts: &[&str]) -> io::Result<Vec<Option<u64>>> {
        match &self.dictionary {
            Dictionary::Whole(map) => Ok(texts.iter().map(|t| map.get(t)).collect()),
            Dictionary::Chunked { entries, .. } => {
                let at: Vec<Option<usize>> = texts
                    .iter()
                    .map(|t| {
                        entries
                            .partition_point(|e| e.first.as_slice() <= t.as_bytes())
                            .checked_sub(1)
                    })
                    .collect();
                let wanted: Vec<usize> = at.iter().flatten().copied().collect();
                let maps = self.chunks(&wanted).await?;
                let by_chunk: BTreeMap<usize, &fst::Map<Bytes>> =
                    wanted.iter().copied().zip(maps.iter()).collect();
                Ok(texts
                    .iter()
                    .zip(at)
                    .map(|(t, c)| c.and_then(|c| by_chunk[&c].get(t)))
                    .collect())
            }
        }
    }
    async fn term(&self, text: &str) -> io::Result<Option<u64>> {
        Ok(self.lookup(&[text]).await?[0])
    }
    async fn prefix_terms(&self, prefix: &str) -> io::Result<Vec<u64>> {
        use fst::Automaton;
        let scan = |map: &fst::Map<Bytes>, out: &mut Vec<u64>| {
            let mut stream = map
                .search(fst::automaton::Str::new(prefix).starts_with())
                .into_stream();
            while let Some((_, id)) = stream.next() {
                out.push(id);
            }
        };
        let mut terms = Vec::new();
        match &self.dictionary {
            Dictionary::Whole(map) => scan(map, &mut terms),
            Dictionary::Chunked { entries, .. } if entries.is_empty() => {}
            Dictionary::Chunked { entries, .. } => {
                let first = entries
                    .partition_point(|e| e.first.as_slice() <= prefix.as_bytes())
                    .saturating_sub(1);
                let mut wanted = vec![first];
                wanted.extend(
                    (first + 1..entries.len())
                        .take_while(|&c| entries[c].first.starts_with(prefix.as_bytes())),
                );
                for map in self.chunks(&wanted).await? {
                    scan(&map, &mut terms);
                }
            }
        }
        Ok(terms)
    }
    /// Every term and handle (a scan: fetches every chunk of a version 3 dictionary).
    async fn all_terms(&self) -> io::Result<Vec<(Vec<u8>, u64)>> {
        let mut out = Vec::new();
        let mut collect = |map: &fst::Map<Bytes>| {
            let mut stream = map.stream();
            while let Some((term, id)) = stream.next() {
                out.push((term.to_vec(), id));
            }
        };
        match &self.dictionary {
            Dictionary::Whole(map) => collect(map),
            Dictionary::Chunked { entries, .. } => {
                let all: Vec<usize> = (0..entries.len()).collect();
                for map in self.chunks(&all).await? {
                    collect(&map);
                }
            }
        }
        Ok(out)
    }
    fn sparse_term(&self, df: u32) -> bool {
        df as usize
            <= (self.docs as usize)
                .div_ceil(BLOCK)
                .saturating_mul(4)
                .max(SMALL_BLOCKS * BLOCK)
    }

    async fn postings(&self, b: &BlockMeta, df: u32) -> io::Result<Postings> {
        if let Some(postings) = self.decoded.lock().unwrap().blocks.get(&b.offset).cloned() {
            return Ok(postings);
        }
        self.ensure_lengths().await?;
        let bytes = self
            .read_ranges(std::iter::once(b.offset..b.offset + b.size as u64).collect())
            .await?
            .remove(0);
        self.decode_postings(b, &bytes, df)
    }
    fn decode_postings(&self, b: &BlockMeta, bytes: &[u8], df: u32) -> io::Result<Postings> {
        if bytes.len() != b.size as usize {
            return Err(invalid("short posting block"));
        }
        verify(bytes, b.crc)?;
        let bits = bytes[0];
        let tf_bits = bytes[1];
        if bits > 32 || tf_bits > 32 || bytes.len() != 2 + 16 * (bits as usize + tf_bits as usize) {
            return Err(invalid("invalid packed posting size"));
        }
        let packer = BitPacker4x::new();
        let mut deltas = [0; BLOCK];
        let mut tfs = [0; BLOCK];
        let n = packer.decompress(&bytes[2..], &mut deltas, bits);
        packer.decompress(&bytes[2 + n..], &mut tfs, tf_bits);
        let mut row = b.first;
        let mut out = Vec::with_capacity(b.count as usize);
        for i in 0..b.count as usize {
            if (i == 0 && deltas[i] != 0)
                || (i > 0 && deltas[i] == 0)
                || tfs[i] == 0
                || tfs[i] > b.max_tf
            {
                return Err(invalid("invalid posting payload"));
            }
            row = row
                .checked_add(deltas[i])
                .ok_or_else(|| invalid("posting row overflow"))?;
            if row > b.last
                || tfs[i] > self.lens()[row as usize]
                || self.lens()[row as usize] < b.min_length
            {
                return Err(invalid("posting row out of bounds"));
            }
            out.push((row, tfs[i]));
        }
        if row != b.last {
            return Err(invalid("invalid last posting row"));
        }
        let out: Postings = out.into();
        // Sparse lists can replace loose block spans with exact row-window bounds.
        // Dense stopword lists would evict those useful lists before the next query.
        if !self.sparse_term(df) {
            return Ok(out);
        }
        let mut memo = self.decoded.lock().unwrap();
        if let Some(existing) = memo.blocks.get(&b.offset) {
            return Ok(existing.clone());
        }
        memo.bytes += out.len() * 8;
        memo.order.push_back(b.offset);
        memo.blocks.insert(b.offset, out.clone());
        while memo.bytes > MEMO_POSTING_BYTES || memo.blocks.len() > MEMO_POSTING_BYTES / 1024 {
            let offset = memo.order.pop_front().expect("posting memo is nonempty");
            if let Some(old) = memo.blocks.remove(&offset) {
                memo.bytes -= old.len() * 8;
            }
        }
        Ok(out)
    }
    /// Posting payload ranges for exact terms and the final prefix expansion.
    /// Sorted, unique ranges exclude positions and blocks with no allowed rows.
    /// Planning reads the queried terms' block directories (version 2 segments) in one batch.
    pub async fn prefetch_plan(
        &self,
        query: &Query,
        allowed: Option<&RoaringBitmap>,
    ) -> io::Result<Vec<Range<u64>>> {
        let (_, ids) = futures::try_join!(self.ensure_lengths(), self.query_handles(query))?;
        let metas = self.load(ids.iter().copied()).await?;
        Ok(Self::plan_ranges(&ids, &metas, allowed))
    }
    async fn query_handles(&self, query: &Query) -> io::Result<std::collections::BTreeSet<u64>> {
        let terms: Vec<&str> = query.exact_terms().keys().copied().collect();
        let mut ids: std::collections::BTreeSet<u64> =
            self.lookup(&terms).await?.into_iter().flatten().collect();
        if let Some(prefix) = query.prefix() {
            ids.extend(self.prefix_terms(prefix).await?);
        }
        Ok(ids)
    }
    fn plan_ranges(
        ids: &std::collections::BTreeSet<u64>,
        metas: &BTreeMap<u64, Arc<TermMeta>>,
        allowed: Option<&RoaringBitmap>,
    ) -> Vec<Range<u64>> {
        let mut ranges: Vec<_> = ids
            .iter()
            .flat_map(|id| &metas[id].blocks)
            .filter(|b| allowed.is_none_or(|rows| rows.range(b.first..=b.last).next().is_some()))
            .map(|b| b.offset..b.offset + b.size as u64)
            .collect();
        ranges.sort_unstable_by_key(|range| range.start);
        ranges.dedup();
        ranges
    }
    /// Batch all posting ranges before scoring. This trades block-max I/O pruning
    /// for one parallel posting stage, which is useful on remote storage.
    /// The fetched payloads remain query-local even when the backend cache is disabled.
    pub async fn search_prefetched(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        k: usize,
        allowed: Option<&RoaringBitmap>,
    ) -> io::Result<Vec<SearchHit>> {
        self.search_internal(query, stats, params, k, allowed, true)
            .await
            .map(|(hits, _)| hits)
    }
    /// Batched, exact search with posting-read, scored-row, and pruning counters.
    pub async fn search_prefetched_with_metrics(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        k: usize,
        allowed: Option<&RoaringBitmap>,
    ) -> io::Result<(Vec<SearchHit>, SearchMetrics)> {
        self.search_internal(query, stats, params, k, allowed, true)
            .await
    }
    /// Top-k BM25 with namespace-wide statistics, optional live/allowed rows, and
    /// conservative block-max pruning. An empty query or `k == 0` returns no hits.
    pub async fn search(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        k: usize,
        allowed: Option<&RoaringBitmap>,
    ) -> io::Result<Vec<SearchHit>> {
        self.search_with_metrics(query, stats, params, k, allowed)
            .await
            .map(|(hits, _)| hits)
    }
    /// Search with pruning and I/O diagnostics.
    pub async fn search_with_metrics(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        k: usize,
        allowed: Option<&RoaringBitmap>,
    ) -> io::Result<(Vec<SearchHit>, SearchMetrics)> {
        self.search_internal(query, stats, params, k, allowed, false)
            .await
    }
    async fn search_internal(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        k: usize,
        allowed: Option<&RoaringBitmap>,
        prefetch: bool,
    ) -> io::Result<(Vec<SearchHit>, SearchMetrics)> {
        params.validate().map_err(|e| input(&e))?;
        self.ensure_lengths().await?;
        let mut metrics = SearchMetrics::default();
        if k == 0 || query.tokens.is_empty() {
            return Ok((Vec::new(), metrics));
        }
        // A fully live, unfiltered segment needs no per-posting bitmap probe.
        // Check the actual range, rather than cardinality, because callers may
        // supply a bitmap containing ordinals outside this segment.
        let allowed = allowed.filter(|rows| !rows.contains_range(0..self.docs));
        let exact = query.exact_terms();
        let mut selections: BTreeMap<u64, (u32, bool, u64)> = BTreeMap::new();
        let exact_terms: Vec<&str> = exact.keys().copied().collect();
        let found = self.lookup(&exact_terms).await?;
        for ((term, qf), id) in exact.iter().zip(found) {
            if let Some(id) = id {
                selections.insert(
                    id,
                    (
                        *qf,
                        false,
                        *stats.document_frequencies.get(*term).unwrap_or(&0),
                    ),
                );
            }
        }
        if let Some(prefix) = query.prefix() {
            for id in self.prefix_terms(prefix).await? {
                selections.entry(id).or_insert((0, false, 0)).1 = true;
            }
        }
        let weights: BTreeMap<_, _> = selections
            .iter()
            .map(|(&id, &(qf, _, df))| (id, TermWeight::new(qf, df, stats, params)))
            .collect();
        let saturation = self.saturation(stats, params);
        let metas = self.load(selections.keys().copied()).await?;
        // Global 128-row windows share a safe bound formed from intersecting
        // posting blocks. Descending bounds fill the heap early (block-max MAXSCORE).
        let windows = (self.num_docs() as usize).div_ceil(BLOCK);
        let mut bounds = vec![0.0f64; windows];
        let mut prefix_windows = vec![false; windows];
        let mut heap: BinaryHeap<Reverse<SearchHit>> = BinaryHeap::new();
        // Block ordinals are dense within each selected term. Direct slots avoid
        // a tree search for every cache probe in every intersecting row window.
        let mut cache: BTreeMap<u64, Vec<Option<Postings>>> = {
            let memo = self.decoded.lock().unwrap();
            metas
                .iter()
                .map(|(&id, meta)| {
                    (
                        id,
                        meta.blocks
                            .iter()
                            .map(|b| memo.blocks.get(&b.offset).cloned())
                            .collect(),
                    )
                })
                .collect()
        };
        let roundoff = (selections.len() as f64 + 8.0) * f64::from(f32::EPSILON);
        let margin = if roundoff < 1.0 {
            1.0 / (1.0 - roundoff)
        } else {
            f64::INFINITY
        };
        let mut row_bounds = Vec::new();
        let mut dense_bounds = Vec::new();
        if prefetch {
            // Short exact terms are read whole (their record and blocks usually arrived together
            // while the dictionary was consulted), so their windows are known exactly: a window's
            // bound for such a term is the best score any of its postings gives, and a window with
            // no posting gets nothing. Long terms keep per-block bounds, at most one per window.
            let mut small = Vec::new();
            for (&id, &(_, prefix, _)) in &selections {
                if !prefix
                    && (metas[&id].blocks.len() <= SMALL_BLOCKS
                        || (self.sparse_term(metas[&id].df)
                            && cache[&id].iter().all(Option::is_some)))
                {
                    small.push(id);
                }
            }
            // Sum known sparse contributions at the same row before taking a
            // window maximum. Summing independent term maxima falsely assumes
            // different matching rows co-occur, and makes stopword blocks competitive.
            if !small.is_empty() {
                row_bounds = vec![0.0f64; self.docs as usize];
            }
            let mut ranges: Vec<_> = small
                .iter()
                .flat_map(|id| {
                    metas[id]
                        .blocks
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| cache[id][*i].is_none())
                        .map(|(_, b)| b)
                })
                .map(|b| b.offset..b.offset + b.size as u64)
                .collect();
            ranges.sort_unstable_by_key(|r| r.start);
            if !ranges.is_empty() {
                let values = self.read_ranges(ranges.clone()).await?;
                if values.len() != ranges.len() {
                    return Err(invalid("invalid posting batch length"));
                }
                let values: BTreeMap<_, _> = ranges
                    .into_iter()
                    .zip(values)
                    .map(|(r, v)| (r.start, v))
                    .collect();
                for &id in &small {
                    for (block, b) in metas[&id].blocks.iter().enumerate() {
                        if cache[&id][block].is_none() {
                            cache.get_mut(&id).unwrap()[block] =
                                Some(self.decode_postings(b, &values[&b.offset], metas[&id].df)?);
                            metrics.blocks_read += 1;
                        }
                    }
                }
            }
            for (&id, &(_, prefix, _)) in &selections {
                let mut best = vec![0.0f64; windows];
                if small.contains(&id) {
                    let weight = &weights[&id];
                    for postings in cache[&id].iter().flatten() {
                        for &(row, tf) in postings.iter() {
                            let score =
                                weight.score_cached(tf, self.lens()[row as usize], &saturation);
                            row_bounds[row as usize] += f64::from(score);
                        }
                    }
                } else {
                    for b in &metas[&id].blocks {
                        let bound = b.upper_bound(&weights[&id], &saturation) as f64;
                        for w in b.first as usize / BLOCK..=b.last as usize / BLOCK {
                            best[w] = best[w].max(bound);
                            prefix_windows[w] |= prefix;
                        }
                    }
                }
                for (w, v) in best.into_iter().enumerate() {
                    bounds[w] += v;
                }
            }
            dense_bounds = bounds.clone();
            for (w, scores) in row_bounds.chunks(BLOCK).enumerate() {
                bounds[w] += scores.iter().copied().fold(0.0, f64::max);
            }
        } else {
            for (&id, &(_, prefix, _)) in &selections {
                for b in &metas[&id].blocks {
                    let bound = b.upper_bound(&weights[&id], &saturation) as f64;
                    for w in b.first as usize / BLOCK..=b.last as usize / BLOCK {
                        bounds[w] += bound;
                        prefix_windows[w] |= prefix;
                    }
                }
            }
        }
        let mut order: Vec<_> = bounds
            .into_iter()
            .enumerate()
            .map(|(w, b)| (w, (b + f64::from(prefix_windows[w])) * margin))
            .filter(|(_, b)| *b > 0.0)
            .collect();
        order.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        // In prefetch mode, posting blocks arrive in at most a few parallel batches, chosen by
        // window bound: the most promising windows first, which fills the heap and sets a threshold;
        // then every window whose bound can still reach the threshold. Exactness is unchanged: a
        // window is skipped only when its bound is below the heap's k-th score.
        let mut next = 0;
        let mut batch_size = if prefetch { FIRST_WINDOWS } else { usize::MAX };
        while next < order.len() {
            let mut batch = Vec::new();
            while next < order.len() && batch.len() < batch_size {
                let (window, bound) = order[next];
                next += 1;
                if heap.len() == k && bound < heap.peek().unwrap().0.score as f64 {
                    metrics.windows_pruned += 1;
                    continue;
                }
                let start = (window * BLOCK) as u32;
                let end = ((window + 1) * BLOCK).min(self.num_docs() as usize) as u32;
                if allowed.is_some_and(|rows| rows.range(start..end).next().is_none()) {
                    continue;
                }
                batch.push((window, bound));
            }
            if batch.is_empty() {
                break;
            }
            if std::env::var_os("BF_TRACE_STAGES").is_some() {
                eprintln!(
                    "FTS batch windows={} of {} first_bound={:.2} last_bound={:.2} heap_min={:?}",
                    batch.len(),
                    order.len(),
                    batch[0].1,
                    batch.last().unwrap().1,
                    heap.peek().map(|h| h.0.score)
                );
            }
            batch_size = usize::MAX;
            if prefetch {
                let mut wanted = std::collections::BTreeSet::new();
                for &(window, _) in &batch {
                    let start = (window * BLOCK) as u32;
                    let end = ((window + 1) * BLOCK).min(self.num_docs() as usize) as u32;
                    for &id in selections.keys() {
                        let blocks = &metas[&id].blocks;
                        let first = blocks.partition_point(|b| b.last < start);
                        for (block, b) in blocks
                            .iter()
                            .enumerate()
                            .skip(first)
                            .take_while(|(_, b)| b.first < end)
                        {
                            if cache[&id][block].is_none()
                                && allowed.is_none_or(|rows| {
                                    rows.range(b.first..=b.last).next().is_some()
                                })
                            {
                                wanted.insert((id, block));
                            }
                        }
                    }
                }
                let wanted: Vec<_> = wanted.into_iter().collect();
                let mut ranges: Vec<_> = wanted
                    .iter()
                    .map(|&(id, block)| {
                        let b = &metas[&id].blocks[block];
                        b.offset..b.offset + b.size as u64
                    })
                    .collect();
                ranges.sort_unstable_by_key(|r| r.start);
                let values = self.read_ranges(ranges.clone()).await?;
                if values.len() != ranges.len() {
                    return Err(invalid("invalid posting batch length"));
                }
                let values: BTreeMap<_, _> = ranges
                    .into_iter()
                    .zip(values)
                    .map(|(r, v)| (r.start, v))
                    .collect();
                for (id, block) in wanted {
                    let b = &metas[&id].blocks[block];
                    cache.get_mut(&id).unwrap()[block] =
                        Some(self.decode_postings(b, &values[&b.offset], metas[&id].df)?);
                    metrics.blocks_read += 1;
                }
            }
            for (window, bound) in batch {
                if heap.len() == k && bound < heap.peek().unwrap().0.score as f64 {
                    metrics.windows_pruned += 1;
                    continue;
                }
                let start = (window * BLOCK) as u32;
                let end = ((window + 1) * BLOCK).min(self.num_docs() as usize) as u32;
                if allowed.is_some_and(|rows| rows.range(start..end).next().is_none()) {
                    continue;
                }
                let mut needed = Vec::new();
                for &id in selections.keys() {
                    let blocks = &metas[&id].blocks;
                    let first = blocks.partition_point(|b| b.last < start);
                    for (block, b) in blocks
                        .iter()
                        .enumerate()
                        .skip(first)
                        .take_while(|(_, b)| b.first < end)
                    {
                        if cache[&id][block].is_none()
                            && allowed
                                .is_none_or(|rows| rows.range(b.first..=b.last).next().is_some())
                        {
                            needed.push((id, block, b));
                        }
                    }
                }
                let results = try_join_all(
                    needed
                        .iter()
                        .map(|(id, _, b)| self.postings(b, metas[id].df)),
                )
                .await?;
                for ((id, block, _), postings) in needed.into_iter().zip(results) {
                    metrics.blocks_read += 1;
                    cache.get_mut(&id).unwrap()[block] = Some(postings);
                }
                // Sparse row contributions are exact. Once the heap is full,
                // dense terms cannot rescue a row whose remaining score bound is
                // below the threshold, even if another row made its window competitive.
                let floor = (heap.len() == k).then(|| heap.peek().unwrap().0.score as f64);
                let competitive: [bool; BLOCK] = std::array::from_fn(|i| {
                    let row = start as usize + i;
                    row < end as usize
                        && floor.is_none_or(|floor| {
                            row_bounds.is_empty()
                                || (row_bounds[row]
                                    + dense_bounds[window]
                                    + f64::from(prefix_windows[window]))
                                    * margin
                                    >= floor
                        })
                });
                let mut scores = [0.0f32; BLOCK];
                let mut prefix_matches = [false; BLOCK];
                for (&id, &(_, prefix, _)) in &selections {
                    let blocks = &metas[&id].blocks;
                    let first = blocks.partition_point(|b| b.last < start);
                    for (block, _) in blocks
                        .iter()
                        .enumerate()
                        .skip(first)
                        .take_while(|(_, b)| b.first < end)
                    {
                        let Some(postings) = cache[&id][block].as_ref() else {
                            continue;
                        };
                        let lo = postings.partition_point(|(row, _)| *row < start);
                        let hi = postings.partition_point(|(row, _)| *row < end);
                        let weight = &weights[&id];
                        for &(row, tf) in &postings[lo..hi] {
                            let i = (row - start) as usize;
                            if competitive[i] && allowed.is_none_or(|rows| rows.contains(row)) {
                                scores[i] +=
                                    weight.score_cached(tf, self.lens()[row as usize], &saturation);
                                prefix_matches[i] |= prefix;
                            }
                        }
                    }
                }
                for row in start..end {
                    if allowed.is_some_and(|rows| !rows.contains(row)) {
                        continue;
                    }
                    let i = (row - start) as usize;
                    if !competitive[i] {
                        continue;
                    }
                    let score = scores[i] + f32::from(prefix_matches[i]);
                    if score <= 0.0 {
                        continue;
                    }
                    metrics.rows_scored += 1;
                    let hit = SearchHit { row, score };
                    if heap.len() < k {
                        heap.push(Reverse(hit));
                    } else if hit > heap.peek().unwrap().0 {
                        *heap.peek_mut().unwrap() = Reverse(hit);
                    }
                }
            }
        }
        let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
        hits.sort_unstable_by(|a, b| b.cmp(a));
        Ok((hits, metrics))
    }
    async fn bitmap(&self, ids: &[u64]) -> io::Result<RoaringBitmap> {
        let metas = self.load(ids.iter().copied()).await?;
        let blocks: Vec<_> = ids
            .iter()
            .flat_map(|id| metas[id].blocks.iter().map(|b| (b, metas[id].df)))
            .collect();
        let postings = try_join_all(blocks.iter().map(|(b, df)| self.postings(b, *df))).await?;
        Ok(postings
            .iter()
            .flat_map(|p| p.iter().map(|(row, _)| *row))
            .collect())
    }
    async fn boolean_terms(&self, text: &str, prefix: bool) -> io::Result<Vec<u64>> {
        if !matches!(self.tokenizer, Tokenizer::WordV3 | Tokenizer::WordV4) {
            return Ok(if prefix {
                self.prefix_terms(text).await?
            } else {
                self.term(text).await?.into_iter().collect()
            });
        }
        let mut ids = Vec::new();
        for (term, id) in self.all_terms().await? {
            if crate::analysis::boolean_term_matches(
                std::str::from_utf8(&term).unwrap(),
                text,
                prefix,
                self.tokenizer,
            ) {
                ids.push(id);
            }
        }
        Ok(ids)
    }
    /// Rows containing every query token. Empty queries match every row, including
    /// null fields. Boolean UAX #29 matching also recognizes punctuation-delimited
    /// subwords; it does not affect BM25 statistics or phrase positions.
    pub async fn contains_all_tokens(&self, query: &Query) -> io::Result<RoaringBitmap> {
        let mut result: RoaringBitmap = (0..self.num_docs()).collect();
        for (i, token) in query.tokens.iter().enumerate() {
            let prefix = query.last_as_prefix && i + 1 == query.tokens.len();
            result &= self
                .bitmap(&self.boolean_terms(&token.text, prefix).await?)
                .await?;
            if result.is_empty() {
                break;
            }
        }
        Ok(result)
    }
    /// Rows containing at least one query token; empty queries match no rows.
    pub async fn contains_any_token(&self, query: &Query) -> io::Result<RoaringBitmap> {
        let mut ids = Vec::new();
        for (i, token) in query.tokens.iter().enumerate() {
            ids.extend(
                self.boolean_terms(
                    &token.text,
                    query.last_as_prefix && i + 1 == query.tokens.len(),
                )
                .await?,
            );
        }
        ids.sort_unstable();
        ids.dedup();
        self.bitmap(&ids).await
    }
    /// Rows containing query tokens at their relative positions within one array
    /// element. Returns `Unsupported` if positions were omitted at build time.
    /// Prefix matching is not valid for this operator.
    pub async fn contains_token_sequence(&self, query: &Query) -> io::Result<RoaringBitmap> {
        if query.last_as_prefix {
            return Err(input("phrase queries do not support last_as_prefix"));
        }
        if !self.positions {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "segment has no positions",
            ));
        }
        let mut candidates: RoaringBitmap = (0..self.num_docs()).collect();
        for token in &query.tokens {
            let Some(id) = self.term(&token.text).await? else {
                return Ok(RoaringBitmap::new());
            };
            candidates &= self.bitmap(&[id]).await?;
        }
        if query.tokens.is_empty() || candidates.is_empty() {
            return Ok(candidates);
        }
        let mut terms: BTreeMap<String, BTreeMap<u32, Vec<(u32, u32)>>> = BTreeMap::new();
        for term in query.exact_terms().keys() {
            let id = self.term(term).await?.unwrap();
            let meta = self.load([id]).await?.remove(&id).unwrap();
            let mut docs = BTreeMap::new();
            let blocks: Vec<_> = meta
                .blocks
                .iter()
                .filter(|b| candidates.range(b.first..=b.last).next().is_some())
                .collect();
            let results = try_join_all(blocks.iter().map(|b| async {
                let postings = self.postings(b, meta.df).await?;
                let raw = read_exact(
                    &*self.source,
                    b.positions..b.positions + b.positions_size as u64,
                )
                .await?;
                verify(&raw, b.positions_crc)?;
                Ok::<_, io::Error>((postings, raw))
            }))
            .await?;
            for (postings, raw) in results {
                let mut c = Cursor::new(&raw);
                for (row, tf) in postings.iter().copied() {
                    // Check the payload size before allocating for untrusted frequencies.
                    if tf as u64 * 8 > (c.data.len() - c.at) as u64 {
                        return Err(invalid("truncated positions"));
                    }
                    let mut positions = Vec::with_capacity(tf as usize);
                    for _ in 0..tf {
                        positions.push((c.u32()?, c.u32()?));
                    }
                    if !positions.windows(2).all(|p| p[0] <= p[1]) {
                        return Err(invalid("unsorted positions"));
                    }
                    if candidates.contains(row) {
                        docs.insert(row, positions);
                    }
                }
                if c.at != c.data.len() {
                    return Err(invalid("trailing positions"));
                }
            }
            terms.insert((*term).to_owned(), docs);
        }
        let first = &query.tokens[0];
        Ok(candidates
            .into_iter()
            .filter(|row| {
                terms[&first.text][row].iter().any(|&(element, position)| {
                    query.tokens.iter().all(|q| {
                        let target = position as i64 + q.position as i64 - first.position as i64;
                        target >= 0
                            && target <= u32::MAX as i64
                            && terms[&q.text][row]
                                .binary_search(&(element, target as u32))
                                .is_ok()
                    })
                })
            })
            .collect())
    }
}

fn parse_chunk_index(data: &[u8], low: u64, high: u64) -> io::Result<Vec<ChunkEntry>> {
    let mut c = Cursor::new(data);
    let count = c.u32()? as usize;
    if count > data.len() / 20 {
        return Err(invalid("invalid dictionary chunk count"));
    }
    let mut entries: Vec<ChunkEntry> = Vec::with_capacity(count);
    for _ in 0..count {
        let first_len = c.u32()? as usize;
        let first = c.take(first_len)?.to_vec();
        let offset = c.u64()?;
        let len = c.u32()?;
        let crc = c.u32()?;
        if offset < low
            || offset.checked_add(len as u64).is_none_or(|end| end > high)
            || entries.last().is_some_and(|p| p.first >= first)
        {
            return Err(invalid("invalid dictionary chunk"));
        }
        entries.push(ChunkEntry {
            first,
            offset,
            len,
            crc,
        });
    }
    if c.at != data.len() {
        return Err(invalid("trailing dictionary index"));
    }
    Ok(entries)
}

fn tokenizer_of(code: u32) -> io::Result<Tokenizer> {
    Ok(match code {
        0 => Tokenizer::WordV4,
        1 => Tokenizer::WordV3,
        2 => Tokenizer::WordV2,
        3 => Tokenizer::WordV1,
        4 => Tokenizer::WordV0,
        5 => Tokenizer::Whitespace,
        6 => Tokenizer::Smartcn,
        7 => Tokenizer::Kuromoji,
        8 => Tokenizer::Nori,
        9 => Tokenizer::PreTokenizedArray,
        _ => return Err(invalid("unknown field tokenizer")),
    })
}

fn lengths_cell(lengths: Vec<u32>) -> std::sync::OnceLock<Vec<u32>> {
    let cell = std::sync::OnceLock::new();
    let _ = cell.set(lengths);
    cell
}

async fn read_exact(source: &dyn RangeRead, range: Range<u64>) -> io::Result<Bytes> {
    if range.start > range.end || range.end > source.len() {
        return Err(invalid("invalid object range"));
    }
    let length = range.end - range.start;
    let data = source.read(range).await?;
    if data.len() as u64 != length {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "short object range read",
        ));
    }
    Ok(data)
}
