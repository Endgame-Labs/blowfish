use crate::scoring::term_score;
use crate::{CorpusStats, FtsParams, Query, Token, Tokenizer};
use async_trait::async_trait;
use bitpacking::{BitPacker, BitPacker4x};
use bytes::Bytes;
use fst::{IntoStreamer, Streamer};
use futures::future::try_join_all;
use roaring::RoaringBitmap;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::io::{self, ErrorKind};
use std::ops::Range;
use std::sync::Arc;

const BLOCK: usize = 128;
const FOOTER: usize = 72;
const MAGIC: &[u8; 8] = b"BFTEXT01";
const VERSION: u32 = 1;
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
    /// Produce immutable version-1 bytes. No other crate's serialization is embedded.
    pub fn finish(self) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut metas = Vec::new();
        let mut dictionary = fst::MapBuilder::memory();
        let packer = BitPacker4x::new();
        for (ordinal, (term, postings)) in self.postings.iter().enumerate() {
            dictionary
                .insert(term, ordinal as u64)
                .map_err(|e| invalid(&e.to_string()))?;
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
            metas.push(TermMeta {
                df: postings.len() as u32,
                blocks,
            });
        }
        let lengths_offset = bytes.len() as u64;
        for length in &self.lengths {
            put32(&mut bytes, *length);
        }
        let metadata_offset = bytes.len() as u64;
        put32(&mut bytes, metas.len() as u32);
        for term in &metas {
            put32(&mut bytes, term.df);
            put32(&mut bytes, term.blocks.len() as u32);
            for block in &term.blocks {
                block.encode(&mut bytes);
            }
        }
        let dictionary_offset = bytes.len() as u64;
        bytes.extend_from_slice(
            &dictionary
                .into_inner()
                .map_err(|e| invalid(&e.to_string()))?,
        );
        let lengths_crc =
            crc32fast::hash(&bytes[lengths_offset as usize..metadata_offset as usize]);
        let metadata_crc =
            crc32fast::hash(&bytes[metadata_offset as usize..dictionary_offset as usize]);
        let dictionary_crc = crc32fast::hash(&bytes[dictionary_offset as usize..]);
        let mut footer = Vec::with_capacity(FOOTER);
        footer.extend_from_slice(MAGIC);
        put32(&mut footer, VERSION);
        put32(&mut footer, u32::from(self.positions));
        put32(&mut footer, self.lengths.len() as u32);
        put32(&mut footer, self.tokenizer as u32);
        put64(&mut footer, self.lengths.iter().map(|&n| n as u64).sum());
        for n in [lengths_offset, metadata_offset, dictionary_offset] {
            put64(&mut footer, n);
        }
        for crc in [lengths_crc, metadata_crc, dictionary_crc] {
            put32(&mut footer, crc);
        }
        let crc = crc32fast::hash(&footer);
        put32(&mut footer, crc);
        debug_assert_eq!(footer.len(), FOOTER);
        bytes.extend(footer);
        Ok(bytes)
    }
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

/// An immutable segment. Opening reads the last 64 KiB, then any missing index
/// metadata in one additional read. Posting payloads and phrase positions stay lazy.
pub struct SegmentReader {
    source: Arc<dyn RangeRead>,
    dictionary: fst::Map<Bytes>,
    terms: Vec<TermMeta>,
    lengths: Vec<u32>,
    total_tokens: u64,
    positions: bool,
    tokenizer: Tokenizer,
}
impl SegmentReader {
    /// Open and validate the footer, dictionary, document lengths, and block directory.
    pub async fn open(source: Arc<dyn RangeRead>) -> io::Result<Self> {
        let len = source.len();
        if len < FOOTER as u64 {
            return Err(invalid("text segment has no footer"));
        }
        let tail_start = len.saturating_sub(TAIL);
        let tail = read_exact(&*source, tail_start..len).await?;
        let raw_footer = &tail[tail.len() - FOOTER..];
        let mut c = Cursor::new(raw_footer);
        if c.take(8)? != MAGIC || c.u32()? != VERSION {
            return Err(invalid("unknown text segment format"));
        }
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
        if !(lengths_offset <= metadata_offset
            && metadata_offset <= dictionary_offset
            && dictionary_offset < footer_start)
            || metadata_offset - lengths_offset != docs as u64 * 4
        {
            return Err(invalid("invalid text segment offsets"));
        }
        let index = if lengths_offset >= tail_start {
            tail.slice((lengths_offset - tail_start) as usize..tail.len() - FOOTER)
        } else {
            read_exact(&*source, lengths_offset..footer_start).await?
        };
        let meta_start = (metadata_offset - lengths_offset) as usize;
        let dict_start = (dictionary_offset - lengths_offset) as usize;
        verify(&index[..meta_start], lengths_crc)?;
        verify(&index[meta_start..dict_start], metadata_crc)?;
        verify(&index[dict_start..], dictionary_crc)?;
        let lengths: Vec<u32> = index[..meta_start]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|n| u32::from_le_bytes(*n))
            .collect();
        if lengths.iter().map(|&n| n as u64).sum::<u64>() != total_tokens {
            return Err(invalid("invalid document length sum"));
        }
        let dictionary =
            fst::Map::new(index.slice(dict_start..)).map_err(|e| invalid(&e.to_string()))?;
        let mut c = Cursor::new(&index[meta_start..dict_start]);
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
                        .is_none_or(|end| end > lengths_offset)
                    || b.positions
                        .checked_add(b.positions_size as u64)
                        .is_none_or(|end| end > lengths_offset)
                    || (flags == 0 && b.positions_size != 0)
                {
                    return Err(invalid("invalid posting block metadata"));
                }
                previous = Some(b.last);
                posting_count += b.count as u64;
                blocks.push(b);
            }
            if posting_count != df as u64 {
                return Err(invalid("inconsistent document frequency"));
            }
            terms.push(TermMeta { df, blocks });
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
            dictionary,
            terms,
            lengths,
            total_tokens,
            positions: flags != 0,
            tokenizer,
        })
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
    /// Per-segment statistics for the engine's namespace-wide aggregation.
    pub fn stats(&self) -> CorpusStats {
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
        let mut stream = self.dictionary.stream();
        while let Some((term, id)) = stream.next() {
            stats.document_frequencies.insert(
                String::from_utf8(term.to_vec()).unwrap(),
                self.terms[id as usize].df as u64,
            );
        }
        stats
    }
    fn term(&self, text: &str) -> Option<usize> {
        self.dictionary.get(text).map(|n| n as usize)
    }
    fn prefix_terms(&self, prefix: &str) -> Vec<usize> {
        let automaton = fst::automaton::Str::new(prefix);
        use fst::Automaton;
        let mut stream = self
            .dictionary
            .search(automaton.starts_with())
            .into_stream();
        let mut terms = Vec::new();
        while let Some((_, id)) = stream.next() {
            terms.push(id as usize);
        }
        terms
    }
    async fn postings(&self, b: &BlockMeta) -> io::Result<Vec<(u32, u32)>> {
        let bytes = read_exact(&*self.source, b.offset..b.offset + b.size as u64).await?;
        verify(&bytes, b.crc)?;
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
        params.validate().map_err(|e| input(&e))?;
        let mut metrics = SearchMetrics::default();
        if k == 0 || query.tokens.is_empty() {
            return Ok((Vec::new(), metrics));
        }
        let exact = query.exact_terms();
        let mut selections: BTreeMap<usize, (u32, bool, u64)> = BTreeMap::new();
        for (term, qf) in &exact {
            if let Some(id) = self.term(term) {
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
            for id in self.prefix_terms(prefix) {
                selections.entry(id).or_insert((0, false, 0)).1 = true;
            }
        }
        // Global 128-row windows share a safe bound formed from intersecting
        // posting blocks. Descending bounds fill the heap early (block-max MAXSCORE).
        let windows = (self.num_docs() as usize).div_ceil(BLOCK);
        let mut bounds = vec![0.0f64; windows];
        let mut prefix_windows = vec![false; windows];
        for (&id, &(qf, prefix, df)) in &selections {
            for b in &self.terms[id].blocks {
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
        let mut cache: BTreeMap<(usize, usize), Vec<(u32, u32)>> = BTreeMap::new();
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
                let blocks = &self.terms[id].blocks;
                let first = blocks.partition_point(|b| b.last < start);
                for (block, b) in blocks
                    .iter()
                    .enumerate()
                    .skip(first)
                    .take_while(|(_, b)| b.first < end)
                {
                    if !cache.contains_key(&(id, block)) {
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
                let blocks = &self.terms[id].blocks;
                let first = blocks.partition_point(|b| b.last < start);
                for (block, _) in blocks
                    .iter()
                    .enumerate()
                    .skip(first)
                    .take_while(|(_, b)| b.first < end)
                {
                    for &(row, tf) in &cache[&(id, block)] {
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
    async fn bitmap(&self, ids: &[usize]) -> io::Result<RoaringBitmap> {
        let blocks: Vec<_> = ids.iter().flat_map(|&id| &self.terms[id].blocks).collect();
        let postings = try_join_all(blocks.iter().map(|b| self.postings(b))).await?;
        Ok(postings.into_iter().flatten().map(|(row, _)| row).collect())
    }
    fn boolean_terms(&self, text: &str, prefix: bool) -> Vec<usize> {
        if !matches!(self.tokenizer, Tokenizer::WordV3 | Tokenizer::WordV4) {
            return if prefix {
                self.prefix_terms(text)
            } else {
                self.term(text).into_iter().collect()
            };
        }
        let mut stream = self.dictionary.stream();
        let mut ids = Vec::new();
        while let Some((term, id)) = stream.next() {
            if crate::analysis::boolean_term_matches(
                std::str::from_utf8(term).unwrap(),
                text,
                prefix,
                self.tokenizer,
            ) {
                ids.push(id as usize);
            }
        }
        ids
    }
    /// Rows containing every query token. Empty queries match every row, including
    /// null fields. Boolean UAX #29 matching also recognizes punctuation-delimited
    /// subwords; it does not affect BM25 statistics or phrase positions.
    pub async fn contains_all_tokens(&self, query: &Query) -> io::Result<RoaringBitmap> {
        let mut result: RoaringBitmap = (0..self.num_docs()).collect();
        for (i, token) in query.tokens.iter().enumerate() {
            let prefix = query.last_as_prefix && i + 1 == query.tokens.len();
            result &= self
                .bitmap(&self.boolean_terms(&token.text, prefix))
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
            ids.extend(self.boolean_terms(
                &token.text,
                query.last_as_prefix && i + 1 == query.tokens.len(),
            ));
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
            let Some(id) = self.term(&token.text) else {
                return Ok(RoaringBitmap::new());
            };
            candidates &= self.bitmap(&[id]).await?;
        }
        if query.tokens.is_empty() || candidates.is_empty() {
            return Ok(candidates);
        }
        let mut terms: BTreeMap<String, BTreeMap<u32, Vec<(u32, u32)>>> = BTreeMap::new();
        for term in query.exact_terms().keys() {
            let id = self.term(term).unwrap();
            let mut docs = BTreeMap::new();
            let blocks: Vec<_> = self.terms[id]
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
