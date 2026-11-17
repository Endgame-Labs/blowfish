use crate::scoring::term_score;
use crate::{CorpusStats, FtsParams, Query, Token, Tokenizer};
use async_trait::async_trait;
use bitpacking::{BitPacker, BitPacker4x};
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
/// Terms per dictionary chunk (version 3).
const CHUNK_TERMS: usize = 2048;
/// Decoded dictionary chunks kept per reader before the memo is reset.
const MEMO_CHUNKS: usize = 64;
const DF_BITS: u32 = 28;
const DF_MASK: u64 = (1 << DF_BITS) - 1;
const RECORD_HEADER: usize = 8;
const BLOCK_META: usize = 52;
/// Decoded term records kept per reader before the memo is reset.
const MEMO_TERMS: usize = 16384;
const TAIL: u64 = 64 * 1024;

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
    fn upper_bound(&self, qf: u32, df: u64, stats: &CorpusStats, params: &FtsParams) -> f32 {
        // Independent extrema form an upper bound under arbitrary corpus statistics.
        // The upward margin protects against f32 rounding near the heap threshold.
        term_score(self.max_tf, self.min_length, qf, df, stats, params) * (1.0 + 8.0 * f32::EPSILON)
    }
}
#[derive(Clone, Debug)]
struct TermMeta {
    df: u32,
    blocks: Vec<BlockMeta>,
}
#[derive(Debug)]
struct Posting {
    row: u32,
    positions: Vec<(u32, u32)>,
}

/// Incremental segment writer. Rows are assigned consecutive zero-based ordinals;
/// use `add_missing_document` for a missing/null field to keep row alignment with other indexes.
#[derive(Default)]
pub struct SegmentBuilder {
    lengths: Vec<u32>,
    postings: BTreeMap<String, Vec<Posting>>,
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
            self.postings
                .entry(term.into())
                .or_default()
                .push(Posting { row, positions });
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
    /// Produce immutable version-2 bytes (version-1 segments remain readable). No other
    /// crate's serialization is embedded.
    pub fn finish(self) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut records = Vec::new();
        let mut chunk_terms: Vec<(&str, u64)> = Vec::new();
        let mut chunk_data: Vec<(String, Vec<u8>)> = Vec::new();
        let packer = BitPacker4x::new();
        for (term, postings) in self.postings.iter() {
            let mut blocks = Vec::new();
            for entries in postings.chunks(BLOCK) {
                let mut deltas = [0; BLOCK];
                let mut frequencies = [0; BLOCK];
                let mut previous = entries[0].row;
                for (i, posting) in entries.iter().enumerate() {
                    deltas[i] = posting.row - previous;
                    previous = posting.row;
                    frequencies[i] = posting.positions.len() as u32;
                }
                let doc_bits = packer.num_bits(&deltas);
                let tf_bits = packer.num_bits(&frequencies);
                let mut compressed = vec![0; 2 + BLOCK * 8];
                compressed[0] = doc_bits;
                compressed[1] = tf_bits;
                let n = packer.compress(&deltas, &mut compressed[2..], doc_bits);
                let m = packer.compress(&frequencies, &mut compressed[2 + n..], tf_bits);
                compressed.truncate(2 + n + m);
                let offset = bytes.len() as u64;
                bytes.extend_from_slice(&compressed);
                let mut positions = Vec::new();
                if self.positions {
                    for entry in entries {
                        for &(element, position) in &entry.positions {
                            put32(&mut positions, element);
                            put32(&mut positions, position);
                        }
                    }
                }
                let positions_offset = bytes.len() as u64;
                bytes.extend_from_slice(&positions);
                blocks.push(BlockMeta {
                    first: entries[0].row,
                    last: entries.last().unwrap().row,
                    count: entries.len() as u32,
                    max_tf: *frequencies.iter().max().unwrap(),
                    min_length: entries
                        .iter()
                        .map(|e| self.lengths[e.row as usize])
                        .min()
                        .unwrap(),
                    offset,
                    size: compressed.len() as u32,
                    crc: crc32fast::hash(&compressed),
                    positions: positions_offset,
                    positions_size: u32::try_from(positions.len())
                        .map_err(|_| input("positions block too large"))?,
                    positions_crc: crc32fast::hash(&positions),
                });
            }
            let df = postings.len() as u64;
            let record_offset = records.len() as u64;
            if df > DF_MASK || record_offset >> (64 - DF_BITS) != 0 {
                return Err(input("text segment too large"));
            }
            let start = records.len();
            put32(&mut records, df as u32);
            put32(&mut records, blocks.len() as u32);
            for block in &blocks {
                block.encode(&mut records);
            }
            let crc = crc32fast::hash(&records[start..]);
            put32(&mut records, crc);
            chunk_terms.push((term, (record_offset << DF_BITS) | df));
            if chunk_terms.len() == CHUNK_TERMS {
                chunk_data.push(build_chunk(&chunk_terms)?);
                chunk_terms.clear();
            }
        }
        if !chunk_terms.is_empty() {
            chunk_data.push(build_chunk(&chunk_terms)?);
        }
        let metadata_offset = bytes.len() as u64;
        bytes.extend_from_slice(&records);
        drop(records);
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
        put32(&mut footer, VERSION_CHUNKED);
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
        memo: std::sync::Mutex<std::collections::HashMap<usize, fst::Map<Bytes>>>,
    },
}

/// An immutable segment. Opening reads the last 64 KiB, then any missing index
/// metadata in one additional read (version 2: just the lengths and dictionary, about
/// 20 bytes per distinct term, with each term's block directory read when a query needs it).
/// Posting payloads and phrase positions stay lazy.
pub struct SegmentReader {
    source: Arc<dyn RangeRead>,
    dictionary: Dictionary,
    terms: Terms,
    lengths: Vec<u32>,
    total_tokens: u64,
    positions: bool,
    tokenizer: Tokenizer,
    /// End of the posting and position payloads.
    postings_end: u64,
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
        if version != VERSION && version != VERSION_LAZY && version != VERSION_CHUNKED {
            return Err(invalid("unknown text segment format"));
        }
        let chunked = version == VERSION_CHUNKED;
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
                    start: metadata_offset,
                    end: lengths_offset,
                    memo: Default::default(),
                },
                lengths,
                total_tokens,
                positions: flags != 0,
                tokenizer,
                postings_end: metadata_offset,
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
                lengths,
                total_tokens,
                positions: flags != 0,
                tokenizer,
                postings_end: metadata_offset,
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
            lengths,
            total_tokens,
            positions: flags != 0,
            tokenizer,
            postings_end: lengths_offset,
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
                    ranges.push(begin..begin + size as u64);
                }
                let values = self.source.read_batch(ranges.clone()).await?;
                if values.len() != ranges.len() {
                    return Err(invalid("invalid term record batch"));
                }
                let mut decoded = Vec::with_capacity(missing.len());
                for ((&h, range), bytes) in missing.iter().zip(&ranges).zip(values) {
                    if bytes.len() as u64 != range.end - range.start {
                        return Err(invalid("short term record"));
                    }
                    let body = &bytes[..bytes.len() - 4];
                    verify(
                        body,
                        u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap()),
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
    /// Approximate heap bytes owned by the decoded directory (term table, block metadata,
    /// and document lengths). The encoded buffer that the dictionary borrows is not counted.
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
            // The chunk index, plus the bounded memo of decoded chunks (about 16 KiB each).
            Dictionary::Chunked { entries, .. } => {
                entries.iter().map(|e| e.first.len() + 48).sum::<usize>() + MEMO_CHUNKS * 16384
            }
        };
        (terms + dictionary + self.lengths.capacity() * 4) as u64
    }
    /// Number of segment rows, including empty or missing fields.
    pub fn num_docs(&self) -> u32 {
        self.lengths.len() as u32
    }
    /// Per-row normalization lengths. Present fields have a minimum of one;
    /// missing fields have length zero.
    pub fn doc_lengths(&self) -> &[u32] {
        &self.lengths
    }
    /// Per-segment statistics for the engine's namespace-wide aggregation. Reads the whole
    /// dictionary (every chunk, for version 3 segments).
    pub async fn stats(&self) -> io::Result<CorpusStats> {
        let present = self.lengths.iter().filter(|&&n| n > 0).count() as u64;
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
                        have.insert(c, map.clone());
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
                decoded.push((
                    c,
                    fst::Map::new(bytes).map_err(|e| invalid(&e.to_string()))?,
                ));
            }
            let mut memo = memo.lock().unwrap();
            if memo.len() + decoded.len() > MEMO_CHUNKS {
                memo.clear();
            }
            for (c, map) in decoded {
                memo.insert(c, map.clone());
                have.insert(c, map);
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
    async fn postings(&self, b: &BlockMeta) -> io::Result<Vec<(u32, u32)>> {
        let bytes = read_exact(&*self.source, b.offset..b.offset + b.size as u64).await?;
        self.decode_postings(b, &bytes)
    }
    fn decode_postings(&self, b: &BlockMeta, bytes: &[u8]) -> io::Result<Vec<(u32, u32)>> {
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
                || tfs[i] > self.lengths[row as usize]
                || self.lengths[row as usize] < b.min_length
            {
                return Err(invalid("posting row out of bounds"));
            }
            out.push((row, tfs[i]));
        }
        if row != b.last {
            return Err(invalid("invalid last posting row"));
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
        let ids = self.query_handles(query).await?;
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
        let mut metrics = SearchMetrics::default();
        if k == 0 || query.tokens.is_empty() {
            return Ok((Vec::new(), metrics));
        }
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
        let metas = self.load(selections.keys().copied()).await?;
        // Global 128-row windows share a safe bound formed from intersecting
        // posting blocks. Descending bounds fill the heap early (block-max MAXSCORE).
        let windows = (self.num_docs() as usize).div_ceil(BLOCK);
        let mut bounds = vec![0.0f64; windows];
        let mut prefix_windows = vec![false; windows];
        for (&id, &(qf, prefix, df)) in &selections {
            for b in &metas[&id].blocks {
                let bound = b.upper_bound(qf, df, stats, params) as f64;
                for w in b.first as usize / BLOCK..=b.last as usize / BLOCK {
                    bounds[w] += bound;
                    prefix_windows[w] |= prefix;
                }
            }
        }
        let mut order: Vec<_> = bounds
            .into_iter()
            .enumerate()
            .map(|(w, b)| (w, b + f64::from(prefix_windows[w])))
            .filter(|(_, b)| *b > 0.0)
            .collect();
        order.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        let mut heap: BinaryHeap<Reverse<SearchHit>> = BinaryHeap::new();
        let mut cache: BTreeMap<(u64, usize), Vec<(u32, u32)>> = BTreeMap::new();
        if prefetch {
            let ids: std::collections::BTreeSet<u64> = selections.keys().copied().collect();
            let ranges = Self::plan_ranges(&ids, &metas, allowed);
            let values = self.source.read_batch(ranges.clone()).await?;
            if values.len() != ranges.len() {
                return Err(invalid("invalid posting batch length"));
            }
            let values: BTreeMap<_, _> = ranges
                .into_iter()
                .zip(values)
                .map(|(r, v)| (r.start, v))
                .collect();
            for &id in selections.keys() {
                for (block, b) in metas[&id].blocks.iter().enumerate() {
                    if let Some(bytes) = values.get(&b.offset) {
                        cache.insert((id, block), self.decode_postings(b, bytes)?);
                        metrics.blocks_read += 1;
                    }
                }
            }
        }
        for (window, bound) in order {
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
                    if !cache.contains_key(&(id, block))
                        && allowed.is_none_or(|rows| rows.range(b.first..=b.last).next().is_some())
                    {
                        needed.push((id, block, b));
                    }
                }
            }
            let results = try_join_all(needed.iter().map(|(_, _, b)| self.postings(b))).await?;
            for ((id, block, _), postings) in needed.into_iter().zip(results) {
                metrics.blocks_read += 1;
                cache.insert((id, block), postings);
            }
            let mut scores = [0.0f32; BLOCK];
            let mut prefix_matches = [false; BLOCK];
            for (&id, &(qf, prefix, df)) in &selections {
                let blocks = &metas[&id].blocks;
                let first = blocks.partition_point(|b| b.last < start);
                for (block, _) in blocks
                    .iter()
                    .enumerate()
                    .skip(first)
                    .take_while(|(_, b)| b.first < end)
                {
                    let Some(postings) = cache.get(&(id, block)) else {
                        continue;
                    };
                    for &(row, tf) in postings {
                        if row >= start
                            && row < end
                            && allowed.is_none_or(|rows| rows.contains(row))
                        {
                            let i = (row - start) as usize;
                            scores[i] +=
                                term_score(tf, self.lengths[row as usize], qf, df, stats, params);
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
        let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
        hits.sort_unstable_by(|a, b| b.cmp(a));
        Ok((hits, metrics))
    }
    async fn bitmap(&self, ids: &[u64]) -> io::Result<RoaringBitmap> {
        let metas = self.load(ids.iter().copied()).await?;
        let blocks: Vec<_> = ids.iter().flat_map(|id| &metas[id].blocks).collect();
        let postings = try_join_all(blocks.iter().map(|b| self.postings(b))).await?;
        Ok(postings.into_iter().flatten().map(|(row, _)| row).collect())
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
                let postings = self.postings(b).await?;
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
                for (row, tf) in postings {
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
