#[path = "bundle.rs"]
mod bundle;
#[path = "compact.rs"]
mod compact;
#[path = "positions.rs"]
mod positions;
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

/// Bundled dictionaries with lossless per-document position and element deltas.
const VERSION_COMPRESSED: u32 = 6;
/// Actual-length posting tails and variable-width block directories.
const VERSION_COMPACT: u32 = 7;

const BLOCK: usize = 128;
/// Whether to score a mask's rows directly instead of walking windows. The window walk pays per
/// window it touches and the direct path per allowed row, so the direct path wins when the mask is
/// sparse and scattered: at most a quarter of the segment's rows, and (sampling eight spots of
/// 64 consecutive allowed rows) fewer than `DIRECT_ROWS_PER_WINDOW` allowed rows per touched window.
/// A contiguous range keeps the window walk, whose bounds prune it. Only speed depends on this.
fn direct_mask(allowed: &RoaringBitmap, docs: u32) -> bool {
    const SPOTS: u64 = 8;
    const RUN: usize = 64;
    const DIRECT_ROWS_PER_WINDOW: u64 = 24;
    let len = allowed.len();
    if len == 0 || len.saturating_mul(4) > u64::from(docs) {
        return false;
    }
    let (mut rows, mut windows) = (0u64, 0u64);
    for spot in 0..SPOTS.min(len) {
        let Some(first) = allowed.select((spot * len / SPOTS) as u32) else {
            return false;
        };
        let mut last = usize::MAX;
        for row in allowed.range(first..).take(RUN) {
            rows += 1;
            if row as usize / BLOCK != last {
                windows += 1;
                last = row as usize / BLOCK;
            }
        }
    }
    rows < windows * DIRECT_ROWS_PER_WINDOW
}
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
/// Separate decoded posting budgets for sparse and dense lists.
const MEMO_POSTING_BYTES: usize = 8 << 20;
/// One decoded posting block: `(row, tf)` entries in row order, with a small table that maps a
/// scoring window to its slice of entries without a binary search. The table is built once when
/// the block is decoded and shared by every query that scores the block.
///
/// Everything lives in one allocation so a query touches one pointer: pair 0 is a header
/// (`first window`, `shift | table pairs << 8`), the next pairs hold the table's bytes, and the
/// entries follow. Each table bucket spans `1 << shift` windows, so the table never exceeds 129
/// bytes, and `offsets[k]` counts the entries before bucket `k`'s first row. The last item equals
/// the entry count.
#[derive(Clone, Debug)]
struct Postings(Arc<[(u32, u32)]>);
impl Postings {
    fn header(&self) -> (usize, u32, usize) {
        let (first_window, packed) = self.0[0];
        (
            first_window as usize,
            packed & 0xff,
            1 + (packed >> 8) as usize,
        )
    }
    fn offset(&self, k: usize) -> usize {
        let pair = self.0[1 + k / 8];
        let word = if k % 8 < 4 { pair.0 } else { pair.1 };
        (word >> (8 * (k % 4))) as usize & 0xff
    }
    /// The entries whose rows lie in `window`, exactly as two partition points would find them.
    /// The caller must pass a window the block touches.
    fn window(&self, window: usize) -> &[(u32, u32)] {
        let (first_window, shift, entries) = self.header();
        let k = (window - first_window) >> shift;
        let bucket = &self.0[entries..][self.offset(k)..self.offset(k + 1)];
        if shift == 0 {
            return bucket;
        }
        let (start, end) = (window * BLOCK, (window + 1) * BLOCK);
        let a = bucket.partition_point(|&(row, _)| (row as usize) < start);
        let b = a + bucket[a..].partition_point(|&(row, _)| (row as usize) < end);
        &bucket[a..b]
    }
}
impl std::ops::Deref for Postings {
    type Target = [(u32, u32)];
    fn deref(&self) -> &[(u32, u32)] {
        &self.0[self.header().2..]
    }
}
impl PartialEq for Postings {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl From<Vec<(u32, u32)>> for Postings {
    fn from(entries: Vec<(u32, u32)>) -> Self {
        assert!(
            entries.len() <= BLOCK,
            "a posting block holds at most BLOCK entries"
        );
        let (first_window, last_window) = match (entries.first(), entries.last()) {
            (Some(&(first, _)), Some(&(last, _))) => {
                (first as usize / BLOCK, last as usize / BLOCK)
            }
            _ => (0, 0),
        };
        let windows = last_window - first_window + 1;
        let mut shift = 0;
        while windows.div_ceil(1 << shift) > BLOCK {
            shift += 1;
        }
        let buckets = windows.div_ceil(1 << shift);
        let mut offsets = Vec::with_capacity(buckets + 8);
        let mut at = 0;
        for k in 0..buckets {
            let boundary = (first_window + (k << shift)) * BLOCK;
            while at < entries.len() && (entries[at].0 as usize) < boundary {
                at += 1;
            }
            offsets.push(at as u8);
        }
        offsets.push(entries.len() as u8);
        offsets.resize(offsets.len().next_multiple_of(8), 0);
        let table_pairs = offsets.len() / 8;
        let mut data = Vec::with_capacity(1 + table_pairs + entries.len());
        data.push((first_window as u32, shift | (table_pairs as u32) << 8));
        data.extend(offsets.as_chunks::<8>().0.iter().map(|b| {
            (
                u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
                u32::from_le_bytes([b[4], b[5], b[6], b[7]]),
            )
        }));
        data.extend(entries);
        Self(data.into())
    }
}
/// Raw, statistics-independent frequencies for one selected dense term.
struct DenseFrequencies {
    values: Box<[u8]>,
    overflow: Box<[(u32, u32)]>,
    /// Per window, the Pareto frontier of its rows' (tf, length) pairs: no other row has both a
    /// frequency at least as high and a length at least as short. BM25's saturation factor is
    /// monotone in both, so a window's exact maximum is the maximum over its frontier. This keeps
    /// the bounds exact while making a recompute (when corpus statistics change, as they do on
    /// every write) cost a few points per window instead of every row. Window `w` owns
    /// `frontier[frontier_start[w]..frontier_start[w + 1]]`.
    frontier_start: Box<[u32]>,
    frontier: Box<[(u32, u32)]>,
    bounds: std::sync::Mutex<Option<DenseBounds>>,
}
struct DenseBounds {
    saturation: std::sync::Weak<Saturation>,
    values: Arc<[f32]>,
}
/// A term earns its array on this use (see `SegmentReader::earn_dense`).
const DENSE_USES: u32 = 2;
/// Most arrays one reader keeps.
const MAX_DENSE_TERMS: usize = 32;
#[derive(Default)]
struct DenseSelection {
    uses: std::collections::HashMap<u64, u32>,
    /// Arrays built or being built, and the bytes they hold or will hold.
    terms: usize,
    bytes: usize,
}
impl DenseFrequencies {
    /// Bytes an array for `docs` rows takes before any overflow entries.
    fn reserve(docs: usize) -> usize {
        // Bounds plus an allowance of four frontier points per window.
        docs + std::mem::size_of::<Self>() + 128 + docs.div_ceil(BLOCK) * (4 + 4 + 4 * 8)
    }
    fn get(&self, row: u32) -> u32 {
        let tf = self.values[row as usize];
        if tf < u8::MAX {
            u32::from(tf)
        } else {
            let i = self
                .overflow
                .binary_search_by_key(&row, |&(r, _)| r)
                .unwrap();
            self.overflow[i].1
        }
    }
    fn bytes(&self) -> usize {
        self.values.len()
            + self.overflow.len() * 8
            + std::mem::size_of::<Self>()
            + 128
            + self.values.len().div_ceil(BLOCK) * 4
            + self.frontier_start.len() * 4
            + self.frontier.len() * 8
    }
    /// Build the per-window frontiers of already-filled values and overflow.
    fn frontiers(&mut self, lengths: &[u32]) {
        let mut start = Vec::with_capacity(self.values.len().div_ceil(BLOCK) + 1);
        let mut points = Vec::new();
        let mut window = Vec::with_capacity(BLOCK);
        for (w, rows) in self.values.chunks(BLOCK).enumerate() {
            start.push(points.len() as u32);
            window.clear();
            for (i, &tf) in rows.iter().enumerate() {
                if tf != 0 {
                    let row = w * BLOCK + i;
                    window.push((self.get(row as u32), lengths[row]));
                }
            }
            window.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            let mut shortest = u32::MAX;
            for &(tf, length) in &window {
                if length < shortest {
                    shortest = length;
                    points.push((tf, length));
                }
            }
        }
        start.push(points.len() as u32);
        self.frontier_start = start.into();
        self.frontier = points.into();
    }
    fn bounds(&self, saturation: &Arc<Saturation>) -> Arc<[f32]> {
        let mut memo = self.bounds.lock().unwrap();
        if let Some(bounds) = &*memo
            && bounds.saturation.ptr_eq(&Arc::downgrade(saturation))
        {
            return bounds.values.clone();
        }
        let mut values = vec![-1.0f32; self.values.len().div_ceil(BLOCK)];
        for (w, value) in values.iter_mut().enumerate() {
            let points = &self.frontier
                [self.frontier_start[w] as usize..self.frontier_start[w + 1] as usize];
            for &(tf, length) in points {
                // Round upward when narrowing, preserving the exact upper bound.
                let bound = (saturation.factor(tf, length) as f32).next_up();
                *value = value.max(bound);
            }
        }
        let values: Arc<[f32]> = values.into();
        *memo = Some(DenseBounds {
            saturation: Arc::downgrade(saturation),
            values: values.clone(),
        });
        values
    }
}

struct CachedPostings {
    postings: Postings,
    dense: bool,
    recent: bool,
}
struct DecodedPostings {
    /// Byte budget of each of the sparse and dense pools.
    limit: usize,
    /// The charge last reported to the owner's cache.
    reported: u64,
    blocks: crate::scoring::OffsetMap<CachedPostings>,
    order: std::collections::VecDeque<u64>,
    bytes: usize,
    dense_order: std::collections::VecDeque<u64>,
    dense_bytes: usize,
    /// Bytes of the reader's frequency arrays, charged with the memo.
    arrays: usize,
}
impl Default for DecodedPostings {
    fn default() -> Self {
        Self {
            limit: MEMO_POSTING_BYTES,
            reported: 0,
            blocks: Default::default(),
            order: Default::default(),
            bytes: 0,
            dense_order: Default::default(),
            dense_bytes: 0,
            arrays: 0,
        }
    }
}
impl DecodedPostings {
    /// Bytes the memo holds now: decoded entries plus per-block bookkeeping (map slot, queue slot,
    /// allocation header, and window table).
    fn charge(&self) -> u64 {
        const PER_BLOCK: usize = 200;
        (self.bytes + self.dense_bytes + self.arrays + self.blocks.len() * PER_BLOCK) as u64
    }
    /// The charge to report to the cache if it moved enough since the last report.
    fn report(&mut self) -> Option<u64> {
        let now = self.charge();
        if now.abs_diff(self.reported) < (1 << 20).max(self.reported / 8) {
            return None;
        }
        self.reported = now;
        Some(now)
    }
    fn get(&mut self, offset: u64) -> Option<Postings> {
        let cached = self.blocks.get_mut(&offset)?;
        if cached.dense {
            cached.recent = true;
        }
        Some(cached.postings.clone())
    }

    fn insert(&mut self, offset: u64, postings: Postings, dense: bool) {
        let bytes = postings.len() * 8;
        self.blocks.insert(
            offset,
            CachedPostings {
                postings,
                dense,
                recent: false,
            },
        );
        if dense {
            self.dense_bytes += bytes;
            self.dense_order.push_back(offset);
            // CLOCK protects frequently queried common words. Sparse lists retain
            // their own budget, so long lists cannot evict exact sparse bounds.
            while self.dense_bytes > self.limit || self.dense_order.len() > self.limit / 1024 {
                let offset = self
                    .dense_order
                    .pop_front()
                    .expect("dense posting memo is nonempty");
                match self.blocks.get_mut(&offset) {
                    Some(cached) if cached.recent => {
                        cached.recent = false;
                        self.dense_order.push_back(offset);
                    }
                    Some(_) => {
                        let cached = self.blocks.remove(&offset).unwrap();
                        self.dense_bytes -= cached.postings.len() * 8;
                    }
                    None => {}
                }
            }
        } else {
            self.bytes += bytes;
            self.order.push_back(offset);
            while self.bytes > self.limit || self.order.len() > self.limit / 1024 {
                let offset = self
                    .order
                    .pop_front()
                    .expect("sparse posting memo is nonempty");
                if let Some(cached) = self.blocks.remove(&offset) {
                    self.bytes -= cached.postings.len() * 8;
                }
            }
        }
    }
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
        bundle::bundle(self.finish_colocated_impl(true)?, true)
    }

    /// Produce format-4 bytes solely for migration tests and measurements.
    #[cfg(feature = "legacy-fixtures")]
    pub fn finish_colocated_fixture(self) -> io::Result<Vec<u8>> {
        self.finish_colocated()
    }

    #[cfg(any(test, feature = "legacy-fixtures"))]
    fn finish_colocated(self) -> io::Result<Vec<u8>> {
        self.finish_colocated_impl(false)
    }

    /// Produce format-5 bytes solely for migration tests and measurements.
    #[cfg(feature = "legacy-fixtures")]
    pub fn finish_bundled_fixture(self) -> io::Result<Vec<u8>> {
        bundle::legacy_bundle(self.finish_colocated()?)
    }

    /// Produce format-6 bytes solely for migration tests and measurements.
    #[cfg(feature = "legacy-fixtures")]
    pub fn finish_compressed_fixture(self) -> io::Result<Vec<u8>> {
        bundle::format_six(self.finish_colocated_impl(true)?)
    }

    fn finish_colocated_impl(self, compressed_positions: bool) -> io::Result<Vec<u8>> {
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
                    if compressed_positions {
                        positions::encode((first..last).map(|i| postings.of(i)), &mut pos);
                    } else {
                        for i in first..last {
                            for &(element, position) in postings.of(i) {
                                put32(&mut pos, element);
                                put32(&mut pos, position);
                            }
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

/// A monotone score floor for one top-k query over disjoint live segments.
///
/// Readers publish a floor only after finding `k` actual hits. Other segments
/// can then omit strictly lower scores, while retaining ties. Create a fresh
/// threshold for each query; never share it across queries or rank expressions.
pub struct SearchThreshold {
    k: usize,
    score: std::sync::atomic::AtomicU32,
}
impl SearchThreshold {
    /// Start a query whose merged result needs at most `k` hits.
    pub fn new(k: usize) -> Self {
        Self {
            k,
            score: std::sync::atomic::AtomicU32::new(0),
        }
    }

    fn publish(&self, heap: &BinaryHeap<Reverse<SearchHit>>) {
        if self.k > 0 && heap.len() == self.k {
            // Nonnegative finite scores and positive infinity have ordered bits.
            let score = heap.peek().unwrap().0.score.to_bits();
            if score > self.score.load(std::sync::atomic::Ordering::Relaxed) {
                self.score
                    .fetch_max(score, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

enum SearchMode<'a> {
    Demand,
    Prefetched,
    Shared(&'a SearchThreshold),
}

fn search_floor(
    heap: &BinaryHeap<Reverse<SearchHit>>,
    k: usize,
    shared: Option<&SearchThreshold>,
) -> Option<f64> {
    let local = if heap.len() == k {
        heap.peek().map_or(0.0, |hit| hit.0.score)
    } else {
        0.0
    };
    let shared = shared.map_or(0.0, |threshold| {
        f32::from_bits(threshold.score.load(std::sync::atomic::Ordering::Relaxed))
    });
    let floor = local.max(shared);
    (floor > 0.0).then_some(f64::from(floor))
}

/// Query-local terms stay in dictionary order, preserving reference f32 addition.
struct SearchTerm {
    meta: Arc<TermMeta>,
    weight: TermWeight,
    prefix: bool,
    postings: Vec<Option<Postings>>,
    spans: Vec<(u32, u32)>,
    bounds: crate::scratch::Windows,
    small: bool,
    dense: Option<Arc<DenseFrequencies>>,
}

impl SearchTerm {
    fn new(meta: Arc<TermMeta>, weight: TermWeight, prefix: bool) -> Self {
        Self {
            meta,
            weight,
            prefix,
            postings: Vec::new(),
            spans: Vec::new(),
            bounds: Default::default(),
            small: false,
            dense: None,
        }
    }

    fn cache_windows(&mut self, windows: usize) {
        if self.dense.is_some() {
            return;
        }
        // Sweep the ordered blocks once instead of binary-searching each term's
        // directory again during planning, fetching, candidate selection, and scoring.
        let (mut first, mut last) = (0, 0);
        let meta = &self.meta;
        self.spans = (0..windows)
            .map(|w| {
                while first < meta.blocks.len() && (meta.blocks[first].last as usize) < w * BLOCK {
                    first += 1;
                }
                last = last.max(first);
                while last < meta.blocks.len()
                    && (meta.blocks[last].first as usize) < (w + 1) * BLOCK
                {
                    last += 1;
                }
                (first as u32, last as u32)
            })
            .collect();
    }

    fn span(&self, window: usize) -> Range<usize> {
        if let Some(&(first, last)) = self.spans.get(window) {
            return first as usize..last as usize;
        }
        let blocks = &self.meta.blocks;
        blocks.partition_point(|b| (b.last as usize) < window * BLOCK)
            ..blocks.partition_point(|b| (b.first as usize) < (window + 1) * BLOCK)
    }

    fn bound(&self, window: usize, saturation: &Saturation) -> f64 {
        self.bounds.get(window).copied().unwrap_or_else(|| {
            self.meta.blocks[self.span(window)]
                .iter()
                .map(|b| f64::from(b.upper_bound(&self.weight, saturation)))
                .fold(0.0, f64::max)
        }) + f64::from(self.prefix)
    }
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

#[derive(Clone)]
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
    /// Older layouts remain readable while the indexer upgrades them.
    pub fn is_current(&self) -> bool {
        self.footer.get(8..12) == Some(VERSION_COMPACT.to_le_bytes().as_slice())
    }
}

/// Upgrade format-4, format-5, or format-6 postings to format 7 without tokenizing documents.
/// Current bytes are returned unchanged. Formats 1–3 return `None`, so callers can rebuild
/// from their immutable document columns. The footer, lengths, and dictionary index are checked;
/// conversion also checks posting and phrase payloads. Unknown formats are errors.
pub fn upgrade_segment(bytes: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
    let footer = checked_slice(&bytes, bytes.len().saturating_sub(FOOTER)..bytes.len())?;
    if footer.len() != FOOTER || &footer[..8] != MAGIC {
        return Err(invalid("text segment has no valid footer"));
    }
    verify(
        &footer[..FOOTER - 4],
        u32::from_le_bytes(footer[68..72].try_into().unwrap()),
    )?;
    let version = u32::from_le_bytes(footer[8..12].try_into().unwrap());
    match version {
        VERSION | VERSION_LAZY | VERSION_CHUNKED => Ok(None),
        VERSION_COLOCATED | VERSION_BUNDLED | VERSION_COMPRESSED | VERSION_COMPACT => {
            let _header = text_header(&bytes)?.ok_or_else(|| invalid("missing text header"))?;
            let lengths = u64::from_le_bytes(footer[32..40].try_into().unwrap()) as usize;
            let positions = u64::from_le_bytes(footer[40..48].try_into().unwrap()) as usize;
            let dictionary = u64::from_le_bytes(footer[48..56].try_into().unwrap()) as usize;
            if u32::from_le_bytes(footer[12..16].try_into().unwrap()) > 1
                || !(positions <= lengths
                    && lengths <= dictionary
                    && dictionary < bytes.len() - FOOTER)
            {
                return Err(invalid("invalid text segment offsets or flags"));
            }
            tokenizer_of(u32::from_le_bytes(footer[20..24].try_into().unwrap()))?;
            verify(
                checked_slice(&bytes, lengths..dictionary)?,
                u32::from_le_bytes(footer[56..60].try_into().unwrap()),
            )?;
            verify(
                checked_slice(&bytes, dictionary..bytes.len() - FOOTER)?,
                u32::from_le_bytes(footer[64..68].try_into().unwrap()),
            )?;
            if version == VERSION_COMPACT {
                Ok(Some(bytes))
            } else {
                bundle::bundle(bytes, version >= VERSION_COMPRESSED).map(Some)
            }
        }
        _ => Err(invalid("unknown text segment format")),
    }
}
/// The header of a finished version 4, 5, 6, or 7 segment (`None` for older versions).
pub fn text_header(bytes: &[u8]) -> io::Result<Option<TextHeader>> {
    if bytes.len() < FOOTER {
        return Err(invalid("text segment has no footer"));
    }
    let footer = &bytes[bytes.len() - FOOTER..];
    let mut c = Cursor::new(footer);
    if c.take(8)? != MAGIC {
        return Err(invalid("unknown text segment format"));
    }
    if !matches!(
        c.u32()?,
        VERSION_COLOCATED | VERSION_BUNDLED | VERSION_COMPRESSED | VERSION_COMPACT
    ) {
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
    compressed_positions: bool,
    compact: bool,
    decoded: std::sync::Mutex<DecodedPostings>,
    /// Told the memo's charge as it grows and shrinks (set once by the owning cache).
    memo_charge: std::sync::OnceLock<Arc<dyn Fn(u64) + Send + Sync>>,
    saturation: std::sync::Mutex<Option<Arc<Saturation>>>,
    /// Directly addressable frequencies of the dense terms this reader has seen often (memory-only).
    dense: std::sync::RwLock<BTreeMap<u64, Arc<DenseFrequencies>>>,
    dense_select: std::sync::Mutex<DenseSelection>,
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
        || b.size == 0
        || b.size > 1275
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
            && version != VERSION_COMPRESSED
            && version != VERSION_COMPACT
        {
            return Err(invalid("unknown text segment format"));
        }
        let colocated = version >= VERSION_COLOCATED;
        let bundled = version >= VERSION_BUNDLED;
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
                compressed_positions: version >= VERSION_COMPRESSED,
                compact: version >= VERSION_COMPACT,
                decoded: Default::default(),
                memo_charge: Default::default(),
                dense: Default::default(),
                dense_select: Default::default(),
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
                compressed_positions: false,
                compact: false,
                decoded: Default::default(),
                memo_charge: Default::default(),
                dense: Default::default(),
                dense_select: Default::default(),
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
            compressed_positions: false,
            compact: false,
            decoded: Default::default(),
            memo_charge: Default::default(),
            dense: Default::default(),
            dense_select: Default::default(),
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
                if self.compact {
                    let decoded = self.compact_records(&missing).await?;
                    let mut memo = memo.lock().unwrap();
                    if memo.len() + decoded.len() > MEMO_TERMS {
                        memo.clear();
                    }
                    for (h, meta) in missing.into_iter().zip(decoded) {
                        memo.insert(h, meta.clone());
                        out.insert(h, meta);
                    }
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
    /// (the decoded-posting memo is charged as it grows, through `set_memo_charge`). A whole legacy FST's backing bytes
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
        (terms + dictionary + self.docs as usize * 4 + Saturation::BYTES) as u64
    }
    /// Dense terms with a resident frequency array, and the bytes those arrays hold.
    pub fn dense_arrays(&self) -> (usize, usize) {
        let dense = self.dense.read().unwrap();
        (dense.len(), dense.values().map(|v| v.bytes()).sum())
    }
    /// Count this query's use of each dense term, and build the arrays that have earned one.
    ///
    /// Rule (no training workload, no setting): a term whose document frequency is above the
    /// sparse threshold is "dense". Serving it from the posting memo costs a decode of the frames
    /// each query touches, and building its array costs one decode of the whole list, so the array
    /// is built when the term is seen for the `DENSE_USES`th time (rent twice, then buy). At most
    /// `MAX_DENSE_TERMS` arrays per reader, and their bytes plus the posting memo's charge fit in
    /// the reader's pool budget; a term that does not fit stays on the posting path. The
    /// arrays are charged to the owning cache through the memo's charge hook, and are dropped with
    /// the reader. Terms used only as prefix expansions are never counted.
    async fn earn_dense(
        &self,
        selections: &BTreeMap<u64, (u32, bool, u64)>,
        metas: &BTreeMap<u64, Arc<TermMeta>>,
    ) -> io::Result<()> {
        let mut earned = Vec::new();
        {
            let mut select = self.dense_select.lock().unwrap();
            let budget = self.decoded.lock().unwrap().limit;
            for (&id, &(qf, _, _)) in selections {
                let meta = &metas[&id];
                if qf == 0 || self.sparse_term(meta.df) {
                    continue;
                }
                let uses = select.uses.entry(id).or_default();
                *uses = uses.saturating_add(1);
                if *uses != DENSE_USES {
                    continue;
                }
                let cost = DenseFrequencies::reserve(self.docs as usize);
                if select.terms < MAX_DENSE_TERMS && select.bytes + cost <= budget {
                    select.terms += 1;
                    select.bytes += cost;
                    earned.push(id);
                }
            }
        }
        for id in earned {
            let built = self.build_dense(&metas[&id]).await;
            let mut select = self.dense_select.lock().unwrap();
            match built {
                Ok(dense) => {
                    let cost = DenseFrequencies::reserve(self.docs as usize);
                    select.bytes = select.bytes - cost + dense.bytes();
                    self.dense.write().unwrap().insert(id, Arc::new(dense));
                }
                Err(e) => {
                    select.terms -= 1;
                    select.bytes -= DenseFrequencies::reserve(self.docs as usize);
                    return Err(e);
                }
            }
            let arrays = select.bytes;
            drop(select);
            let mut memo = self.decoded.lock().unwrap();
            memo.arrays = arrays;
            let charge = memo.charge();
            memo.reported = charge;
            drop(memo);
            if let Some(hook) = self.memo_charge.get() {
                hook(charge);
            }
        }
        Ok(())
    }
    /// Decode a term's whole list (all blocks in one batched read) into a raw frequency array.
    /// The decode bypasses the posting memo: the array replaces those frames.
    async fn build_dense(&self, meta: &TermMeta) -> io::Result<DenseFrequencies> {
        self.ensure_lengths().await?;
        let mut values = vec![0u8; self.docs as usize];
        let mut overflow = Vec::new();
        let mut fill = |postings: &mut dyn Iterator<Item = (u32, u32)>| {
            for (row, tf) in postings {
                values[row as usize] = tf.min(u32::from(u8::MAX)) as u8;
                if tf >= u32::from(u8::MAX) {
                    overflow.push((row, tf));
                }
            }
        };
        // Frames the memo already decoded need no read; the rest come in one batched read.
        let cached: Vec<_> = {
            let mut memo = self.decoded.lock().unwrap();
            meta.blocks.iter().map(|b| memo.get(b.offset)).collect()
        };
        let mut missing = Vec::new();
        for (block, hit) in meta.blocks.iter().zip(&cached) {
            match hit {
                Some(postings) => fill(&mut postings.iter().copied()),
                None => missing.push(block),
            }
        }
        let ranges = missing
            .iter()
            .map(|b| b.offset..b.offset + u64::from(b.size))
            .collect();
        let raws = self.read_ranges(ranges).await?;
        for (block, raw) in missing.into_iter().zip(raws) {
            fill(&mut self.decode_posting_entries(block, &raw)?.into_iter());
        }
        let mut dense = DenseFrequencies {
            values: values.into(),
            overflow: overflow.into(),
            frontier_start: Default::default(),
            frontier: Default::default(),
            bounds: Default::default(),
        };
        dense.frontiers(self.lens());
        Ok(dense)
    }

    /// Bound each decoded-posting pool (sparse and dense) to `limit` bytes, and report the memo's
    /// actual charge to `charge` as it grows and shrinks, so the owning cache pays for what is
    /// resident rather than for a flat worst case. Takes effect once; later calls do nothing.
    pub fn set_memo_charge(&self, limit: usize, charge: Arc<dyn Fn(u64) + Send + Sync>) {
        if self.memo_charge.set(charge).is_ok() {
            self.decoded.lock().unwrap().limit = limit;
        }
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
    /// and the dictionary's chunk index. Document lengths load on first use. Versions 4,
    /// 5, 6, and 7 have headers.
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
        if !matches!(
            version,
            VERSION_COLOCATED | VERSION_BUNDLED | VERSION_COMPRESSED | VERSION_COMPACT
        ) {
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
                end: if version >= VERSION_BUNDLED {
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
            bundled: version >= VERSION_BUNDLED,
            compressed_positions: version >= VERSION_COMPRESSED,
            compact: version >= VERSION_COMPACT,
            decoded: Default::default(),
            memo_charge: Default::default(),
            dense: Default::default(),
            dense_select: Default::default(),
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
    /// Count rows containing each exact term within a segment-local row mask.
    /// Counts follow the input order, including repeated and absent terms. No
    /// prefix expansion or boolean subword matching is applied.
    ///
    /// Only blocks intersecting the mask are read, without positions. Within
    /// each block, intersection walks the smaller of the mask and the postings.
    /// A row outside this segment returns an error.
    pub async fn document_frequencies(
        &self,
        terms: &[&str],
        rows: &RoaringBitmap,
    ) -> io::Result<Vec<u64>> {
        if rows.max().is_some_and(|row| row >= self.docs) {
            return Err(input("statistics row outside segment"));
        }
        if rows.is_empty() || terms.is_empty() {
            return Ok(vec![0; terms.len()]);
        }
        let handles = self.lookup(terms).await?;
        // A complete mask needs only the frequencies already in the dictionary.
        if rows.len() == u64::from(self.docs) {
            return Ok(handles
                .iter()
                .map(|h| h.map_or(0, |h| u64::from(self.handle_df(h))))
                .collect());
        }
        let metas = self.load(handles.iter().flatten().copied()).await?;
        let mut counts = BTreeMap::<u64, u64>::new();
        let mut missing = Vec::new();
        {
            let mut decoded = self.decoded.lock().unwrap();
            for (&handle, meta) in &metas {
                let count = counts.entry(handle).or_default();
                for block in &meta.blocks {
                    let selected = rows.range_cardinality(block.first..=block.last);
                    if selected == 0 {
                        continue;
                    }
                    let span = u64::from(block.last) - u64::from(block.first) + 1;
                    if selected == span {
                        *count += u64::from(block.count);
                    } else if u64::from(block.count) == span {
                        // A gap-free block contains every selected row, even
                        // for a ubiquitous term and a very sparse mask.
                        *count += selected;
                    } else if let Some(postings) = decoded.get(block.offset) {
                        *count += masked_count(&postings, block, rows, selected);
                    } else {
                        missing.push((handle, meta.df, block, selected));
                    }
                }
            }
        }
        if !missing.is_empty() {
            self.ensure_lengths().await?;
            let ranges = missing
                .iter()
                .map(|(_, _, block, _)| block.offset..block.offset + u64::from(block.size))
                .collect();
            let values = self.read_ranges(ranges).await?;
            if values.len() != missing.len() {
                return Err(invalid("invalid statistics posting batch"));
            }
            for ((handle, df, block, selected), bytes) in missing.into_iter().zip(values) {
                let postings = self.decode_postings(block, &bytes, df)?;
                *counts.entry(handle).or_default() +=
                    masked_count(&postings, block, rows, selected);
            }
        }
        Ok(handles
            .iter()
            .map(|h| h.map_or(0, |h| counts[&h]))
            .collect())
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
    async fn chunks(&self, wanted: &[usize]) -> io::Result<Vec<DictionaryChunk>> {
        let Dictionary::Chunked { entries, memo } = &self.dictionary else {
            return Ok(Vec::new());
        };
        let mut have: BTreeMap<usize, DictionaryChunk> = BTreeMap::new();
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
                have.insert(c, map.clone());
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
                let by_chunk: BTreeMap<usize, &fst::Map<Bytes>> = wanted
                    .iter()
                    .copied()
                    .zip(maps.iter().map(|chunk| &chunk.map))
                    .collect();
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
                    scan(&map.map, &mut terms);
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
                    collect(&map.map);
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
        if let Some(postings) = self.decoded.lock().unwrap().get(b.offset) {
            return Ok(postings);
        }
        self.ensure_lengths().await?;
        let bytes = self
            .read_ranges(std::iter::once(b.offset..b.offset + b.size as u64).collect())
            .await?
            .remove(0);
        self.decode_postings(b, &bytes, df)
    }
    fn decode_posting_entries(&self, b: &BlockMeta, bytes: &[u8]) -> io::Result<Vec<(u32, u32)>> {
        if bytes.len() != b.size as usize {
            return Err(invalid("short posting block"));
        }
        verify(bytes, b.crc)?;
        let out = if self.compact && b.count < BLOCK as u32 {
            let out = compact::decode_tail(bytes, b)?;
            let lengths = self.lens();
            for &(row, tf) in &out {
                if tf > lengths[row as usize] || lengths[row as usize] < b.min_length {
                    return Err(invalid("posting row out of bounds"));
                }
            }
            out
        } else {
            if bytes.len() < 2 {
                return Err(invalid("short posting block"));
            }
            let bits = bytes[0];
            let tf_bits = bytes[1];
            if bits > 32
                || tf_bits > 32
                || bytes.len() != 2 + 16 * (bits as usize + tf_bits as usize)
            {
                return Err(invalid("invalid packed posting size"));
            }
            let packer = BitPacker4x::new();
            let mut deltas = [0; BLOCK];
            let mut tfs = [0; BLOCK];
            let n = packer.decompress(&bytes[2..], &mut deltas, bits);
            packer.decompress(&bytes[2 + n..], &mut tfs, tf_bits);
            let mut row = b.first;
            let mut out = Vec::with_capacity(b.count as usize);
            let lengths = self.lens();
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
                    || tfs[i] > lengths[row as usize]
                    || lengths[row as usize] < b.min_length
                {
                    return Err(invalid("posting row out of bounds"));
                }
                out.push((row, tfs[i]));
            }
            if row != b.last {
                return Err(invalid("invalid last posting row"));
            }
            out
        };
        Ok(out)
    }
    fn decode_postings(&self, b: &BlockMeta, bytes: &[u8], df: u32) -> io::Result<Postings> {
        let out: Postings = self.decode_posting_entries(b, bytes)?.into();
        let mut memo = self.decoded.lock().unwrap();
        if let Some(existing) = memo.get(b.offset) {
            return Ok(existing);
        }
        memo.insert(b.offset, out.clone(), !self.sparse_term(df));
        let report = memo.report();
        drop(memo);
        if let (Some(charge), Some(hook)) = (report, self.memo_charge.get()) {
            hook(charge);
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
    /// Exact BM25 with block-max MaxScore and parallel posting reads. Promising
    /// windows establish a top-k threshold before the remaining competitive
    /// frames are fetched. Batch payloads stay available throughout scoring,
    /// including when the backend has no cache.
    pub async fn search_prefetched(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        k: usize,
        allowed: Option<&RoaringBitmap>,
    ) -> io::Result<Vec<SearchHit>> {
        self.search_internal(query, stats, params, k, allowed, SearchMode::Prefetched)
            .await
            .map(|(hits, _)| hits)
    }
    /// Contribute to a shared top-k query over disjoint live segments.
    ///
    /// This may return fewer than `k` hits when another segment has established
    /// a higher floor. Merge all readers' results before truncating to `k`.
    pub async fn search_prefetched_shared(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        allowed: Option<&RoaringBitmap>,
        threshold: &SearchThreshold,
    ) -> io::Result<Vec<SearchHit>> {
        self.search_internal(
            query,
            stats,
            params,
            threshold.k,
            allowed,
            SearchMode::Shared(threshold),
        )
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
        self.search_internal(query, stats, params, k, allowed, SearchMode::Prefetched)
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
        self.search_internal(query, stats, params, k, allowed, SearchMode::Demand)
            .await
    }
    async fn search_internal(
        &self,
        query: &Query,
        stats: &CorpusStats,
        params: &FtsParams,
        k: usize,
        allowed: Option<&RoaringBitmap>,
        mode: SearchMode<'_>,
    ) -> io::Result<(Vec<SearchHit>, SearchMetrics)> {
        let prefetch = !matches!(mode, SearchMode::Demand);
        let threshold = if let SearchMode::Shared(threshold) = mode {
            Some(threshold)
        } else {
            None
        };
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
        let saturation = self.saturation(stats, params);
        let lengths = self.lens();
        let metas = self.load(selections.keys().copied()).await?;
        let windows = (self.num_docs() as usize).div_ceil(BLOCK);
        // Bound auxiliary window metadata, including broad prefix expansions.
        let cached_windows = if windows.saturating_mul(selections.len()) <= 65_536 {
            windows
        } else {
            0
        };
        self.earn_dense(&selections, &metas).await?;
        let mut terms: Vec<_> = {
            let dense = self.dense.read().unwrap();
            selections
                .iter()
                .map(|(&id, &(qf, prefix, df))| {
                    let mut term = SearchTerm::new(
                        metas[&id].clone(),
                        TermWeight::new(qf, df, stats, params),
                        prefix,
                    );
                    term.dense = dense.get(&id).cloned();
                    term
                })
                .collect()
        };
        if terms.is_empty() {
            return Ok((Vec::new(), metrics));
        }
        {
            let mut memo = self.decoded.lock().unwrap();
            for term in &mut terms {
                // Dense frames are consulted only after window pruning. Probing
                // them here hashes and retains every common-word frame, including
                // those that cannot affect the top k.
                term.postings = if term.dense.is_some() {
                    Vec::new()
                } else if self.sparse_term(term.meta.df) {
                    term.meta
                        .blocks
                        .iter()
                        .map(|b| memo.get(b.offset))
                        .collect()
                } else {
                    vec![None; term.meta.blocks.len()]
                };
                term.small = prefetch
                    && term.dense.is_none()
                    && !term.prefix
                    && (term.meta.blocks.len() <= SMALL_BLOCKS
                        || (self.sparse_term(term.meta.df)
                            && term.postings.iter().all(Option::is_some)));
            }
        }
        // Sparse partials and final scores each accumulate at most n positive
        // terms. Two n-unit-roundoff allowances fit within n * EPSILON; the
        // additional margin covers bounds and the prefix contribution.
        let roundoff = (terms.len() as f64 + 8.0) * f64::from(f32::EPSILON);
        let margin = if roundoff < 1.0 {
            1.0 / (1.0 - roundoff)
        } else {
            f64::INFINITY
        };
        let mut bounds = vec![0.0f64; windows];
        let mut prefix_windows = vec![false; windows];
        let mut heap: BinaryHeap<Reverse<SearchHit>> = BinaryHeap::new();
        if prefetch {
            let wanted = terms
                .iter()
                .enumerate()
                .filter(|(_, term)| term.small)
                .flat_map(|(t, term)| {
                    term.postings
                        .iter()
                        .enumerate()
                        .filter(|(_, p)| p.is_none())
                        .map(move |(b, _)| (t, b))
                })
                .collect();
            self.read_search_blocks(&mut terms, wanted, &saturation, &mut metrics)
                .await?;
        }
        if terms.iter().all(|term| term.small) {
            // Every posting is already available. Accumulate once in reference
            // order instead of building window bounds and scoring those rows again.
            let mut scores = crate::scratch::Rows::take(self.docs as usize);
            for term in &terms {
                for postings in term.postings.iter().flatten() {
                    for &(row, tf) in postings.iter() {
                        if allowed.is_some_and(|rows| !rows.contains(row)) {
                            continue;
                        }
                        let score =
                            term.weight
                                .score_cached(tf, lengths[row as usize], &saturation);
                        if score <= 0.0 {
                            continue;
                        }
                        scores.add(row, score);
                    }
                }
            }
            metrics.rows_scored = scores.touched().len();
            let floor = search_floor(&heap, k, threshold);
            for &row in scores.touched() {
                if floor.is_some_and(|floor| f64::from(scores[row as usize]) < floor) {
                    continue;
                }
                let hit = SearchHit {
                    row,
                    score: scores[row as usize],
                };
                if heap.len() < k {
                    heap.push(Reverse(hit));
                } else if hit > heap.peek().unwrap().0 {
                    *heap.peek_mut().unwrap() = Reverse(hit);
                }
            }
            if let Some(threshold) = threshold {
                threshold.publish(&heap);
            }
            let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
            hits.sort_unstable_by(|a, b| b.cmp(a));
            return Ok((hits, metrics));
        }
        if let Some(rows) = allowed
            && query.prefix().is_none()
            && direct_mask(rows, self.docs)
        {
            return self
                .search_allowed_rows(&mut terms, rows, k, &saturation, threshold, &mut metrics)
                .await
                .map(|hits| (hits, metrics));
        }
        let mut row_bounds = crate::scratch::Rows::take(if terms.iter().any(|term| term.small) {
            self.docs as usize
        } else {
            0
        });
        for term in &mut terms {
            term.cache_windows(cached_windows);
        }
        for term in &mut terms {
            let mut best = crate::scratch::Windows::take(windows);
            if let Some(dense) = &term.dense {
                let normalized = dense.bounds(&saturation);
                for (w, &maximum) in normalized.iter().enumerate() {
                    if maximum >= 0.0 {
                        best[w] = f64::from(
                            term.weight.score_saturation(f64::from(maximum))
                                * (1.0 + 8.0 * f32::EPSILON),
                        );
                        prefix_windows[w] |= term.prefix;
                    }
                }
                for (bound, &value) in bounds.iter_mut().zip(best.iter()) {
                    *bound += value;
                }
            } else if term.small {
                for postings in term.postings.iter().flatten() {
                    for &(row, tf) in postings.iter() {
                        let score =
                            term.weight
                                .score_cached(tf, lengths[row as usize], &saturation);
                        row_bounds.add(row, score);
                        best[row as usize / BLOCK] =
                            best[row as usize / BLOCK].max(f64::from(score));
                    }
                }
            } else {
                let key = term.meta.blocks.first().map(|b| b.offset);
                let mut apply = |normalized: &[f64]| {
                    for (w, &maximum) in normalized.iter().enumerate() {
                        if maximum >= 0.0 {
                            best[w] = f64::from(
                                term.weight.score_saturation(maximum) * (1.0 + 8.0 * f32::EPSILON),
                            );
                            prefix_windows[w] |= term.prefix;
                        }
                    }
                };
                // Clone immutable cached bounds under a read lock. Sparse
                // scoring, normalization, and query weights run without it.
                let (known_frames, cached, frames) = {
                    let maxima = saturation.maxima.read().unwrap();
                    let cached = key.and_then(|key| maxima.term(key));
                    let frames: Vec<_> = if cached.is_none() {
                        term.meta
                            .blocks
                            .iter()
                            .map(|b| maxima.frames.get(&b.offset).cloned())
                            .collect()
                    } else {
                        Vec::new()
                    };
                    (maxima.frames.len(), cached, frames)
                };
                if let Some(cached) = cached {
                    // Reuse query-independent window maxima while the set of
                    // decoded frame bounds is unchanged. New frames invalidate
                    // this summary, allowing their tighter bounds to take effect.
                    apply(&cached);
                } else {
                    let mut normalized = vec![-1.0f64; windows];
                    for (b, frame) in term.meta.blocks.iter().zip(frames) {
                        if let Some(windows) = frame {
                            for &(w, maximum) in windows.iter() {
                                normalized[w] = normalized[w].max(maximum);
                            }
                        } else {
                            let maximum = saturation.factor(b.max_tf, b.min_length).max(0.0);
                            for value in
                                &mut normalized[b.first as usize / BLOCK..=b.last as usize / BLOCK]
                            {
                                *value = value.max(maximum);
                            }
                        }
                    }
                    apply(&normalized);
                    if let Some(key) = key {
                        let normalized = normalized.into();
                        // Publish the generation actually observed, even if
                        // another query admitted tighter frames in the meantime.
                        saturation.maxima.write().unwrap().remember_term(
                            key,
                            normalized,
                            known_frames,
                        );
                    }
                }
                for (bound, &value) in bounds.iter_mut().zip(best.iter()) {
                    *bound += value;
                }
            }
            if cached_windows > 0 {
                term.bounds = best;
            }
        }
        let dense_bounds = bounds.clone();
        // Known sparse terms must co-occur at a row, rather than at unrelated
        // maxima in the same window. This tightens the bound before any dense read.
        for (w, scores) in row_bounds.chunks(BLOCK).enumerate() {
            bounds[w] += f64::from(scores.iter().copied().fold(0.0, f32::max));
        }
        let mut order: Vec<_> = bounds
            .into_iter()
            .enumerate()
            .map(|(w, b)| (w, (b + f64::from(prefix_windows[w])) * margin))
            .filter(|(_, b)| *b > 0.0)
            .collect();
        order.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        let mut next = 0;
        let mut batch_size = if prefetch { FIRST_WINDOWS } else { usize::MAX };
        let mut by_bound = Vec::with_capacity(terms.len());
        let mut essential = vec![true; terms.len()];
        let mut dense_candidates = Vec::with_capacity(32);
        while next < order.len() {
            let mut batch = Vec::new();
            while next < order.len() && batch.len() < batch_size {
                let (window, bound) = order[next];
                if search_floor(&heap, k, threshold).is_some_and(|floor| bound < floor) {
                    // Bounds descend, and the heap threshold only rises.
                    metrics.windows_pruned += order.len() - next;
                    next = order.len();
                    break;
                }
                next += 1;
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
                let mut wanted = Vec::new();
                // A frame can intersect several windows. Sweep their union in
                // row order, requesting each frame once, without sorting a much
                // larger vector of duplicate term/frame pairs. Scoring retains
                // the original descending-bound window order.
                let mut windows: Vec<_> = batch.iter().map(|&(w, _)| w).collect();
                windows.sort_unstable();
                for (t, term) in terms.iter().enumerate() {
                    if term.dense.is_some() {
                        continue;
                    }
                    let mut after = 0;
                    for &window in &windows {
                        let span = term.span(window);
                        for b in span.start.max(after)..span.end {
                            let meta = &term.meta.blocks[b];
                            if term.postings[b].is_none()
                                && allowed.is_none_or(|rows| {
                                    rows.range(meta.first..=meta.last).next().is_some()
                                })
                            {
                                wanted.push((t, b));
                            }
                        }
                        after = after.max(span.end);
                    }
                }
                self.read_search_blocks(&mut terms, wanted, &saturation, &mut metrics)
                    .await?;
            }
            for (window, bound) in batch {
                if search_floor(&heap, k, threshold).is_some_and(|floor| bound < floor) {
                    metrics.windows_pruned += 1;
                    continue;
                }
                let start = (window * BLOCK) as u32;
                let end = ((window + 1) * BLOCK).min(self.num_docs() as usize) as u32;
                if !prefetch {
                    let mut wanted = Vec::new();
                    for (t, term) in terms.iter().enumerate() {
                        if term.dense.is_some() {
                            continue;
                        }
                        for b in term.span(window) {
                            let meta = &term.meta.blocks[b];
                            if term.postings[b].is_none()
                                && allowed.is_none_or(|rows| {
                                    rows.range(meta.first..=meta.last).next().is_some()
                                })
                            {
                                wanted.push((t, b));
                            }
                        }
                    }
                    self.read_search_blocks(&mut terms, wanted, &saturation, &mut metrics)
                        .await?;
                }
                let floor = search_floor(&heap, k, threshold);
                let mut candidates = [true; BLOCK];
                dense_candidates.clear();
                if let Some(floor) = floor {
                    // Block-max MaxScore: low-impact terms cannot create a hit on
                    // their own. Generate candidates only from the essential terms.
                    // Strict inequality retains every score tie. Prefix expansions
                    // each get a conservative flat contribution, counted only once
                    // when computing the final score.
                    by_bound.clear();
                    by_bound.extend(
                        terms
                            .iter()
                            .enumerate()
                            .map(|(t, term)| (t, term.bound(window, &saturation))),
                    );
                    by_bound.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
                    essential.fill(true);
                    let mut nonessential = 0.0;
                    let mut skipped = 0;
                    for &(t, bound) in &by_bound {
                        if (nonessential + bound) * margin >= floor {
                            break;
                        }
                        nonessential += bound;
                        essential[t] = false;
                        skipped += 1;
                    }
                    if skipped > 0 {
                        candidates.fill(false);
                        for (t, term) in terms.iter().enumerate().filter(|(t, _)| essential[*t]) {
                            if term.dense.is_some() {
                                dense_candidates.push(t);
                                continue;
                            }
                            for b in term.span(window) {
                                let Some(postings) = &term.postings[b] else {
                                    continue;
                                };
                                for &(row, _) in postings.window(window) {
                                    candidates[(row - start) as usize] = true;
                                }
                            }
                        }
                    }
                }
                // Test the sparse bound and allowed mask once per candidate, not
                // again for every term that hits it.
                let competitive: Vec<_> = (start..end)
                    .filter(|&row| {
                        (candidates[(row - start) as usize]
                            || dense_candidates.iter().any(|&t| {
                                terms[t].dense.as_ref().unwrap().values[row as usize] != 0
                            }))
                            && allowed.is_none_or(|rows| rows.contains(row))
                            && floor.is_none_or(|floor| {
                                row_bounds.is_empty()
                                    || (f64::from(row_bounds[row as usize])
                                        + dense_bounds[window]
                                        + f64::from(prefix_windows[window]))
                                        * margin
                                        >= floor
                            })
                    })
                    .collect();
                if competitive.is_empty() {
                    continue;
                }
                candidates.fill(false);
                for &row in &competitive {
                    candidates[(row - start) as usize] = true;
                }
                let mut scores = [0.0f32; BLOCK];
                let mut prefix_matches = [false; BLOCK];
                for term in &terms {
                    if let Some(dense) = &term.dense {
                        for &row in &competitive {
                            let tf = dense.get(row);
                            if tf > 0 {
                                let i = (row - start) as usize;
                                scores[i] += term.weight.score_cached(
                                    tf,
                                    lengths[row as usize],
                                    &saturation,
                                );
                                prefix_matches[i] |= term.prefix;
                            }
                        }
                        continue;
                    }
                    for b in term.span(window) {
                        let Some(postings) = &term.postings[b] else {
                            continue;
                        };
                        let postings = postings.window(window);
                        let mut add = |row: u32, tf| {
                            let i = (row - start) as usize;
                            scores[i] +=
                                term.weight
                                    .score_cached(tf, lengths[row as usize], &saturation);
                            prefix_matches[i] |= term.prefix;
                        };
                        if competitive.len() * 8 < postings.len() {
                            // A common-word frame can have 128 entries but only one
                            // rare-term candidate. Seek those rows instead of scanning
                            // the entire frame and testing the same mask repeatedly.
                            for &row in &competitive {
                                if let Ok(i) = postings.binary_search_by_key(&row, |&(r, _)| r) {
                                    add(row, postings[i].1);
                                }
                            }
                        } else {
                            for &(row, tf) in postings {
                                if candidates[(row - start) as usize] {
                                    add(row, tf);
                                }
                            }
                        }
                    }
                }
                for row in competitive {
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
                if let Some(threshold) = threshold {
                    threshold.publish(&heap);
                }
            }
        }
        let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
        hits.sort_unstable_by(|a, b| b.cmp(a));
        Ok((hits, metrics))
    }

    /// Score a highly selective mask directly: look up each query term's frequency at the allowed
    /// rows (the dense array where present, the intersecting decoded posting blocks otherwise)
    /// instead of walking windows. Terms add in dictionary order into one f32 accumulator per row,
    /// exactly as the window path does, so scores and ordering are bit-identical.
    async fn search_allowed_rows(
        &self,
        terms: &mut [SearchTerm],
        allowed: &RoaringBitmap,
        k: usize,
        saturation: &Arc<Saturation>,
        threshold: Option<&SearchThreshold>,
        metrics: &mut SearchMetrics,
    ) -> io::Result<Vec<SearchHit>> {
        let rows: Vec<u32> = allowed.range(0..self.docs).collect();
        let mut wanted = Vec::new();
        for (t, term) in terms.iter().enumerate() {
            if term.dense.is_some() {
                continue;
            }
            for (b, meta) in term.meta.blocks.iter().enumerate() {
                if term.postings[b].is_none()
                    && allowed.range(meta.first..=meta.last).next().is_some()
                {
                    wanted.push((t, b));
                }
            }
        }
        self.read_search_blocks(terms, wanted, saturation, metrics)
            .await?;
        let lengths = self.lens();
        let mut scores = crate::scratch::Rows::take(self.docs as usize);
        for term in terms.iter() {
            if let Some(dense) = &term.dense {
                for &row in &rows {
                    let tf = dense.get(row);
                    if tf > 0 {
                        let score = term
                            .weight
                            .score_cached(tf, lengths[row as usize], saturation);
                        if score > 0.0 {
                            scores.add(row, score);
                        }
                    }
                }
                continue;
            }
            for (meta, postings) in term.meta.blocks.iter().zip(&term.postings) {
                let Some(postings) = postings else {
                    continue;
                };
                let lo = rows.partition_point(|&r| r < meta.first);
                let hi = lo + rows[lo..].partition_point(|&r| r <= meta.last);
                let wanted = &rows[lo..hi];
                let mut add = |row: u32, tf: u32| {
                    let score = term
                        .weight
                        .score_cached(tf, lengths[row as usize], saturation);
                    if score > 0.0 {
                        scores.add(row, score);
                    }
                };
                if wanted.len() * 8 < postings.len() {
                    for &row in wanted {
                        if let Ok(i) = postings.binary_search_by_key(&row, |&(r, _)| r) {
                            add(row, postings[i].1);
                        }
                    }
                } else {
                    let mut at = 0;
                    for &(row, tf) in postings.iter() {
                        while at < wanted.len() && wanted[at] < row {
                            at += 1;
                        }
                        if at == wanted.len() {
                            break;
                        }
                        if wanted[at] == row {
                            add(row, tf);
                        }
                    }
                }
            }
        }
        metrics.rows_scored = scores.touched().len();
        let mut heap: BinaryHeap<Reverse<SearchHit>> = BinaryHeap::new();
        let floor = search_floor(&heap, k, threshold);
        for &row in scores.touched() {
            if floor.is_some_and(|floor| f64::from(scores[row as usize]) < floor) {
                continue;
            }
            let hit = SearchHit {
                row,
                score: scores[row as usize],
            };
            if heap.len() < k {
                heap.push(Reverse(hit));
            } else if hit > heap.peek().unwrap().0 {
                *heap.peek_mut().unwrap() = Reverse(hit);
            }
        }
        if let Some(threshold) = threshold {
            threshold.publish(&heap);
        }
        let mut hits: Vec<_> = heap.into_iter().map(|r| r.0).collect();
        hits.sort_unstable_by(|a, b| b.cmp(a));
        Ok(hits)
    }
    /// Read missing frames in address order, keeping direct term/block ordinals.
    async fn read_search_blocks(
        &self,
        terms: &mut [SearchTerm],
        mut wanted: Vec<(usize, usize)>,
        saturation: &Saturation,
        metrics: &mut SearchMetrics,
    ) -> io::Result<()> {
        if wanted.is_empty() {
            return Ok(());
        }
        {
            let mut memo = self.decoded.lock().unwrap();
            wanted.retain(|&(t, b)| {
                let term = &mut terms[t];
                if term.postings[b].is_none() {
                    term.postings[b] = memo.get(term.meta.blocks[b].offset);
                }
                term.postings[b].is_none()
            });
        }
        if wanted.is_empty() {
            return Ok(());
        }
        wanted.sort_unstable_by_key(|&(t, b)| terms[t].meta.blocks[b].offset);
        let ranges = wanted
            .iter()
            .map(|&(t, b)| {
                let meta = &terms[t].meta.blocks[b];
                meta.offset..meta.offset + u64::from(meta.size)
            })
            .collect();
        let values = self.read_ranges(ranges).await?;
        if values.len() != wanted.len() {
            return Err(invalid("invalid posting batch length"));
        }
        for ((t, b), bytes) in wanted.into_iter().zip(values) {
            let term = &mut terms[t];
            term.postings[b] =
                Some(self.decode_postings(&term.meta.blocks[b], &bytes, term.meta.df)?);
            if !self.sparse_term(term.meta.df) {
                saturation.remember(
                    term.meta.blocks[b].offset,
                    term.postings[b].as_ref().unwrap(),
                    self.lens(),
                    BLOCK,
                );
            }
            metrics.blocks_read += 1;
        }
        Ok(())
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
                let mut decoder = positions::Decoder::new(&raw, self.compressed_positions)?;
                for (row, tf) in postings.iter().copied() {
                    let positions = decoder.document(tf)?;
                    if candidates.contains(row) {
                        docs.insert(row, positions);
                    }
                }
                decoder.finish()?;
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

/// Each posting block has at most 128 rows. Sparse masks probe those sorted
/// postings, while dense masks visit each posting once.
fn masked_count(
    postings: &[(u32, u32)],
    block: &BlockMeta,
    rows: &RoaringBitmap,
    selected: u64,
) -> u64 {
    if selected < u64::from(block.count) {
        rows.range(block.first..=block.last)
            .filter(|row| postings.binary_search_by_key(row, |&(row, _)| row).is_ok())
            .count() as u64
    } else {
        postings
            .iter()
            .filter(|(row, _)| rows.contains(*row))
            .count() as u64
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

#[cfg(test)]
mod posting_cache_tests {
    use super::*;

    #[test]
    fn memo_reports_its_actual_charge_as_it_grows_and_shrinks() {
        let mut memo = DecodedPostings {
            limit: 4 << 20,
            ..Default::default()
        };
        assert_eq!(memo.charge(), 0);
        assert_eq!(memo.report(), None, "a small change is not reported");
        let block = || -> Postings { (0..128u32).map(|row| (row, 1)).collect::<Vec<_>>().into() };
        let mut reports = Vec::new();
        for offset in 0..20_000u64 {
            memo.insert(offset, block(), true);
            reports.extend(memo.report());
        }
        // Dense blocks beyond the limit evict older ones, so the charge stays bounded.
        assert!(memo.dense_bytes <= memo.limit);
        assert!(reports.len() > 1, "growth is reported in steps");
        let last = *reports.last().unwrap();
        assert!(last >= memo.dense_bytes as u64);
        assert!(last <= memo.limit as u64 + (memo.blocks.len() * 200) as u64 + (1 << 20));
        assert_eq!(memo.reported, last);
    }

    #[test]
    fn pruned_dense_frames_do_not_receive_cache_second_chances() {
        futures::executor::block_on(async {
            let params = FtsParams::default();
            let docs: Vec<_> = (0..32_768)
                .map(|row| {
                    crate::tokenize(if row < 10 { "common jackpot" } else { "common" }, &params)
                })
                .collect();
            let mut builder = SegmentBuilder::new(false);
            for doc in &docs {
                builder.add_document(doc).unwrap();
            }
            let reader =
                SegmentReader::open(Arc::new(MemoryRangeRead::from(builder.finish().unwrap())))
                    .await
                    .unwrap();
            // These tests exercise the posting memo: keep the frequency arrays off.
            reader.dense_select.lock().unwrap().terms = MAX_DENSE_TERMS;
            let stats = CorpusStats::from_documents(&docs);
            reader
                .search_prefetched(
                    &Query::new(crate::tokenize("common", &params), false),
                    &stats,
                    &params,
                    docs.len(),
                    None,
                )
                .await
                .unwrap();
            {
                let mut memo = reader.decoded.lock().unwrap();
                for cached in memo.blocks.values_mut() {
                    cached.recent = false;
                }
            }
            let query = Query::new(crate::tokenize("common jackpot", &params), false);
            let (hits, metrics) = reader
                .search_prefetched_with_metrics(&query, &stats, &params, 10, None)
                .await
                .unwrap();
            let expected: Vec<_> = (0..10)
                .map(|row| SearchHit {
                    row,
                    score: crate::score_document(&docs[row as usize], &query, &stats, &params),
                })
                .collect();
            assert_eq!(hits, expected);
            assert!(metrics.windows_pruned > 200, "{metrics:?}");
            let memo = reader.decoded.lock().unwrap();
            let dense = memo.blocks.values().filter(|cached| cached.dense).count();
            let probed = memo
                .blocks
                .values()
                .filter(|cached| cached.dense && cached.recent)
                .count();
            assert!(probed > 0 && probed < dense / 4, "{probed}/{dense}");
        });
    }

    #[test]
    fn later_queries_rebuild_tighter_bounds_from_decoded_frames() {
        futures::executor::block_on(async {
            let params = FtsParams::default();
            let docs: Vec<_> = (0..16_384)
                .map(|row| {
                    if row % 128 == 0 {
                        let mut tokens = vec![crate::Token::new("common", 0); 16];
                        tokens.extend(vec![crate::Token::new("filler", 0); 128]);
                        tokens
                    } else {
                        vec![crate::Token::new("common", 0)]
                    }
                })
                .collect();
            let mut builder = SegmentBuilder::new(false);
            for doc in &docs {
                builder.add_document(doc).unwrap();
            }
            let reader =
                SegmentReader::open(Arc::new(MemoryRangeRead::from(builder.finish().unwrap())))
                    .await
                    .unwrap();
            // These tests exercise the posting memo: keep the frequency arrays off.
            reader.dense_select.lock().unwrap().terms = MAX_DENSE_TERMS;
            let stats = CorpusStats::from_documents(&docs);
            let saturation = reader.saturation(&stats, &params);
            let id = reader.term("common").await.unwrap().unwrap();
            let meta = reader.load([id]).await.unwrap().remove(&id).unwrap();
            let key = meta.blocks[0].offset;
            let coarse = meta
                .blocks
                .iter()
                .map(|b| saturation.factor(b.max_tf, b.min_length))
                .fold(0.0, f64::max);
            let query = Query::new(crate::tokenize("common", &params), false);
            reader
                .search_prefetched(&query, &stats, &params, docs.len(), None)
                .await
                .unwrap();
            assert!(
                saturation.maxima.read().unwrap().term(key).is_none(),
                "new frame maxima invalidate the original directory-only summary"
            );
            let hits = reader
                .search_prefetched(&query, &stats, &params, 10, None)
                .await
                .unwrap();
            let tightened = saturation.maxima.read().unwrap().term(key).unwrap();
            assert!(tightened.iter().copied().fold(0.0, f64::max) < coarse);
            let mut expected: Vec<_> = docs
                .iter()
                .enumerate()
                .map(|(row, doc)| SearchHit {
                    row: row as u32,
                    score: crate::score_document(doc, &query, &stats, &params),
                })
                .collect();
            expected.sort_unstable_by(|a, b| b.cmp(a));
            expected.truncate(10);
            assert_eq!(hits, expected);
        });
    }

    #[test]
    fn posting_window_slices_match_partition_points() {
        // Dense blocks (one window per bucket), wide sparse blocks (several windows per
        // bucket), single-window blocks, and blocks that start or end mid-window.
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for case in 0..400u64 {
            let n = 1 + (next() % BLOCK as u64) as usize;
            let spread = [1u64, 3, 40, 900, 100_000][(case % 5) as usize];
            let mut row = (next() % 5000) as u32;
            let entries: Vec<(u32, u32)> = (0..n)
                .map(|i| {
                    if i > 0 {
                        row += 1 + (next() % spread) as u32;
                    }
                    (row, 1 + (next() % 4) as u32)
                })
                .collect();
            let postings: Postings = entries.clone().into();
            let (first, last) = (
                entries[0].0 as usize / BLOCK,
                entries[n - 1].0 as usize / BLOCK,
            );
            for window in first..=last {
                let (start, end) = ((window * BLOCK) as u32, ((window + 1) * BLOCK) as u32);
                let lo = entries.partition_point(|&(r, _)| r < start);
                let hi = entries.partition_point(|&(r, _)| r < end);
                assert_eq!(postings.window(window), &entries[lo..hi], "case {case}");
            }
        }
    }

    #[test]
    fn dense_clock_is_bounded_and_cannot_evict_sparse_frames() {
        let mut memo = DecodedPostings::default();
        let postings: Postings = vec![(0, 1); BLOCK].into();
        memo.insert(0, postings.clone(), false);
        let capacity = MEMO_POSTING_BYTES / (BLOCK * 8);
        for offset in 1..=capacity as u64 {
            memo.insert(offset, postings.clone(), true);
        }
        assert!(memo.get(1).is_some());
        memo.insert(capacity as u64 + 1, postings.clone(), true);
        assert!(memo.get(1).is_some(), "a read earns a second chance");
        assert!(memo.get(2).is_none());
        for offset in capacity as u64 + 2..=capacity as u64 * 2 {
            memo.insert(offset, postings.clone(), true);
        }
        assert!(memo.get(0).is_some());
        assert!(memo.get(capacity as u64 * 2).is_some());
        assert!(memo.bytes <= MEMO_POSTING_BYTES);
        assert!(memo.dense_bytes <= MEMO_POSTING_BYTES);
        assert!(memo.dense_order.len() <= capacity);
        assert_eq!(memo.order.len(), 1);
    }
}

#[cfg(test)]
#[path = "experiments.rs"]
mod experiments;
