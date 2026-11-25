use crate::{FtsParams, Token};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

/// Namespace-wide statistics, including all segments and the unindexed WAL tail.
///
/// Present but empty fields contribute a normalization length of one. Missing
/// fields are excluded from these statistics. Deleted or superseded rows
/// must be removed by the engine before combining statistics.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CorpusStats {
    /// Number of documents with this field, including present but empty text.
    pub total_docs: u64,
    /// Average normalization length (retained token count, with a minimum of one).
    pub average_doc_length: f64,
    /// Number of documents containing each term (not its total occurrence count).
    pub document_frequencies: BTreeMap<String, u64>,
}

impl CorpusStats {
    /// Compute statistics for tokenized documents, suitable for a WAL tail.
    pub fn from_documents(documents: &[Vec<Token>]) -> Self {
        let mut stats = Self {
            total_docs: documents.len() as u64,
            ..Self::default()
        };
        let mut length = 0u64;
        for doc in documents {
            length += doc.len().max(1) as u64;
            for term in doc.iter().map(|t| &t.text).collect::<BTreeSet<_>>() {
                *stats.document_frequencies.entry(term.clone()).or_default() += 1;
            }
        }
        stats.average_doc_length = if stats.total_docs == 0 {
            0.0
        } else {
            length as f64 / stats.total_docs as f64
        };
        stats
    }

    /// Combine disjoint collections, weighting their average lengths by document count.
    pub fn merge(&mut self, other: &Self) {
        let length = self.average_doc_length * self.total_docs as f64
            + other.average_doc_length * other.total_docs as f64;
        self.total_docs += other.total_docs;
        self.average_doc_length = if self.total_docs == 0 {
            0.0
        } else {
            length / self.total_docs as f64
        };
        for (term, count) in &other.document_frequencies {
            *self.document_frequencies.entry(term.clone()).or_default() += count;
        }
    }
}

/// A normalized query. Repeated terms retain their query frequency.
#[derive(Clone, Debug, Default)]
pub struct Query {
    /// Tokens in query order (positions are used by phrase filters).
    pub tokens: Vec<Token>,
    /// Treat the last retained token as a byte prefix, contributing a flat 1.0.
    pub last_as_prefix: bool,
}

impl Query {
    /// Construct a query from tokens, preserving repetitions and phrase position gaps.
    pub fn new(tokens: Vec<Token>, last_as_prefix: bool) -> Self {
        Self {
            tokens,
            last_as_prefix,
        }
    }

    pub(crate) fn exact_terms(&self) -> BTreeMap<&str, u32> {
        let end = self
            .tokens
            .len()
            .saturating_sub(usize::from(self.last_as_prefix));
        let mut terms = BTreeMap::new();
        for token in &self.tokens[..end] {
            *terms.entry(token.text.as_str()).or_default() += 1;
        }
        terms
    }

    pub(crate) fn prefix(&self) -> Option<&str> {
        self.last_as_prefix
            .then(|| self.tokens.last().map(|t| t.text.as_str()))
            .flatten()
    }
}

pub(crate) fn term_score(
    tf: u32,
    length: u32,
    qf: u32,
    df: u64,
    stats: &CorpusStats,
    p: &FtsParams,
) -> f32 {
    if tf == 0 || qf == 0 || df == 0 || stats.total_docs == 0 || stats.average_doc_length <= 0.0 {
        return 0.0;
    }
    let n = stats.total_docs as f64;
    let df = df.min(stats.total_docs) as f64;
    let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
    let k1 = p.k1 as f64;
    let k3 = p.k3 as f64;
    let norm = k1 * (1.0 - p.b as f64 + p.b as f64 * length as f64 / stats.average_doc_length);
    (idf * (tf as f64 * (k1 + 1.0) / (tf as f64 + norm))
        * (qf as f64 * (k3 + 1.0) / (qf as f64 + k3))) as f32
}

/// The common saturation factor is independent of the term's IDF and query frequency.
/// Cache its exact f64 values for ordinary passage lengths and term frequencies.
/// Unusual values use the reference arithmetic directly.
pub(crate) struct Saturation {
    key: (u32, u32, u64),
    values: Vec<f64>,
    /// Exact row-window maxima, independent of IDF and query frequency. They belong
    /// to this normalization table, so changed statistics cannot reuse them.
    pub(crate) maxima: RwLock<BoundCache>,
}

/// A multiplicative hasher for maps keyed by one object offset or term hash. These keys are
/// never attacker-chosen hash-flooding inputs, and `SipHash` showed up in warm-query profiles.
#[derive(Default, Clone, Copy)]
pub(crate) struct OffsetHasher(u64);
impl std::hash::Hasher for OffsetHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = (self.0.rotate_left(5) ^ u64::from(byte)).wrapping_mul(0x517c_c1b7_2722_0a95);
        }
    }
    fn write_u64(&mut self, value: u64) {
        // The high bits carry the entropy hashbrown uses for its control bytes.
        self.0 = (self.0 ^ value).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        self.0 ^= self.0 >> 29;
    }
}
pub(crate) type OffsetMap<V> =
    std::collections::HashMap<u64, V, std::hash::BuildHasherDefault<OffsetHasher>>;

#[derive(Default)]
pub(crate) struct BoundCache {
    pub(crate) frames: OffsetMap<Arc<[(usize, f64)]>>,
    // Term maxima use -1 for windows without any posting. This keeps
    // prefix presence separate from a zero normalization value.
    terms: OffsetMap<TermBounds>,
    bytes: usize,
}
struct TermBounds {
    known_frames: usize,
    maxima: Arc<[f64]>,
}
impl BoundCache {
    pub(crate) fn term(&self, offset: u64) -> Option<Arc<[f64]>> {
        self.terms
            .get(&offset)
            .filter(|term| term.known_frames == self.frames.len())
            .map(|term| term.maxima.clone())
    }

    pub(crate) fn remember_term(&mut self, offset: u64, windows: Arc<[f64]>, known_frames: usize) {
        if self
            .terms
            .get(&offset)
            .is_some_and(|term| term.known_frames > known_frames)
        {
            return;
        }
        let charge = std::mem::size_of_val(windows.as_ref()) + 64;
        let old_charge = self
            .terms
            .get(&offset)
            .map_or(0, |term| std::mem::size_of_val(term.maxima.as_ref()) + 64);
        if self.bytes - old_charge + charge <= Saturation::MAX_BOUND_BYTES {
            self.bytes = self.bytes - old_charge + charge;
            self.terms.insert(
                offset,
                TermBounds {
                    known_frames,
                    maxima: windows,
                },
            );
        }
    }
}
impl Saturation {
    const LENGTHS: usize = 1025;
    const FREQUENCIES: usize = 17;
    const MAX_BOUND_BYTES: usize = 8 << 20;
    pub(crate) const BYTES: usize =
        Self::LENGTHS * Self::FREQUENCIES * 8 + Self::MAX_BOUND_BYTES * 2 + 64;
    pub(crate) fn new(stats: &CorpusStats, p: &FtsParams) -> Self {
        let k1 = p.k1 as f64;
        let b = p.b as f64;
        let mut values = Vec::with_capacity(Self::LENGTHS * Self::FREQUENCIES);
        for length in 0..Self::LENGTHS {
            let norm = k1 * (1.0 - b + b * length as f64 / stats.average_doc_length);
            for tf in 0..Self::FREQUENCIES {
                values.push(tf as f64 * (k1 + 1.0) / (tf as f64 + norm));
            }
        }
        Self {
            key: (
                p.k1.to_bits(),
                p.b.to_bits(),
                stats.average_doc_length.to_bits(),
            ),
            values,
            maxima: Default::default(),
        }
    }
    pub(crate) fn matches(&self, stats: &CorpusStats, p: &FtsParams) -> bool {
        self.key
            == (
                p.k1.to_bits(),
                p.b.to_bits(),
                stats.average_doc_length.to_bits(),
            )
    }
    fn get(&self, tf: u32, length: u32) -> Option<f64> {
        ((tf as usize) < Self::FREQUENCIES && (length as usize) < Self::LENGTHS)
            .then(|| self.values[length as usize * Self::FREQUENCIES + tf as usize])
    }
    pub(crate) fn factor(&self, tf: u32, length: u32) -> f64 {
        self.get(tf, length).unwrap_or_else(|| {
            let k1 = f32::from_bits(self.key.0) as f64;
            let b = f32::from_bits(self.key.1) as f64;
            let average = f64::from_bits(self.key.2);
            let norm = k1 * (1.0 - b + b * length as f64 / average);
            tf as f64 * (k1 + 1.0) / (tf as f64 + norm)
        })
    }
    pub(crate) fn remember(
        &self,
        offset: u64,
        postings: &[(u32, u32)],
        lengths: &[u32],
        window_rows: usize,
    ) {
        {
            let maxima = self.maxima.read().unwrap();
            if maxima.bytes >= Self::MAX_BOUND_BYTES || maxima.frames.contains_key(&offset) {
                return;
            }
        }
        let mut windows: Vec<(usize, f64)> = Vec::new();
        for &(row, tf) in postings {
            let length = lengths[row as usize];
            let saturation = self.factor(tf, length).max(0.0);
            let window = row as usize / window_rows;
            if let Some((last, maximum)) = windows.last_mut()
                && *last == window
            {
                *maximum = maximum.max(saturation);
            } else {
                windows.push((window, saturation));
            }
        }
        // Include vector capacity, map capacity, and allocation overhead in admission.
        let charge = windows.capacity() * std::mem::size_of::<(usize, f64)>() + 64;
        let windows = windows.into();
        let mut maxima = self.maxima.write().unwrap();
        if maxima.bytes + charge <= Self::MAX_BOUND_BYTES && !maxima.frames.contains_key(&offset) {
            maxima.bytes += charge;
            maxima.frames.insert(offset, windows);
        }
    }
}

/// Query-invariant work for one term. Keep the reference scorer's operation order so
/// precomputation cannot move a rounded score across a top-k tie or pruning bound.
pub(crate) struct TermWeight {
    idf: f64,
    query: f64,
    k1: f64,
    b: f64,
    average: f64,
}

impl TermWeight {
    pub(crate) fn new(qf: u32, df: u64, stats: &CorpusStats, p: &FtsParams) -> Self {
        let n = stats.total_docs as f64;
        let df = df.min(stats.total_docs) as f64;
        let k3 = p.k3 as f64;
        Self {
            idf: if qf == 0 || df == 0.0 || n == 0.0 || stats.average_doc_length <= 0.0 {
                0.0
            } else {
                (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
            },
            query: qf as f64 * (k3 + 1.0) / (qf as f64 + k3),
            k1: p.k1 as f64,
            b: p.b as f64,
            average: stats.average_doc_length,
        }
    }

    #[inline]
    pub(crate) fn score_cached(&self, tf: u32, length: u32, table: &Saturation) -> f32 {
        if tf == 0 || self.idf == 0.0 {
            return 0.0;
        }
        match table.get(tf, length) {
            Some(saturation) => self.score_saturation(saturation),
            None => self.score(tf, length),
        }
    }

    #[inline]
    pub(crate) fn score_saturation(&self, saturation: f64) -> f32 {
        (self.idf * saturation * self.query) as f32
    }

    pub(crate) fn score(&self, tf: u32, length: u32) -> f32 {
        if tf == 0 || self.idf == 0.0 {
            return 0.0;
        }
        let norm = self.k1 * (1.0 - self.b + self.b * length as f64 / self.average);
        (self.idf * (tf as f64 * (self.k1 + 1.0) / (tf as f64 + norm)) * self.query) as f32
    }
}

/// Score a tokenized document using externally supplied corpus statistics.
///
/// This uses Lucene's positive IDF, exact retained-token lengths, standard BM25
/// document saturation, and `(k3+1)*qtf/(k3+qtf)` query saturation. Prefix matches
/// contribute once per document regardless of the number of expanded terms.
/// It is the reference scorer for the engine's brute-force WAL tail.
pub fn score_document(
    document: &[Token],
    query: &Query,
    stats: &CorpusStats,
    params: &FtsParams,
) -> f32 {
    let mut frequencies = BTreeMap::new();
    for token in document {
        *frequencies.entry(token.text.as_str()).or_insert(0u32) += 1;
    }
    let mut score = 0.0;
    for (term, qf) in query.exact_terms() {
        score += term_score(
            *frequencies.get(term).unwrap_or(&0),
            document.len() as u32,
            qf,
            *stats.document_frequencies.get(term).unwrap_or(&0),
            stats,
            params,
        );
    }
    if let Some(prefix) = query.prefix()
        && frequencies.keys().any(|term| term.starts_with(prefix))
    {
        score += 1.0;
    }
    score
}

/// Position-free term counts for an immutable document. Reuse these across
/// queries when only BM25 scores and corpus deltas are needed.
#[derive(Clone, Debug, Default)]
pub struct DocumentTerms {
    length: u32,
    frequencies: BTreeMap<String, u32>,
}
impl DocumentTerms {
    /// Analyze already-tokenized input once, retaining exact term frequencies.
    pub fn new(tokens: &[Token]) -> Self {
        let mut frequencies = BTreeMap::new();
        for token in tokens {
            *frequencies.entry(token.text.clone()).or_default() += 1;
        }
        Self {
            length: tokens.len() as u32,
            frequencies,
        }
    }
    /// Retained token count, before the corpus's minimum-one normalization.
    pub fn len(&self) -> u32 {
        self.length
    }
    /// Whether the document has no retained tokens.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    /// Number of occurrences of an exact term.
    pub fn frequency(&self, term: &str) -> u32 {
        self.frequencies.get(term).copied().unwrap_or(0)
    }
    /// Conservative allocation charge, including sparsely populated tree nodes.
    pub fn resident_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>()
            + (self.frequencies.len() / 5 + 1) * 512
            + self.frequencies.keys().map(String::capacity).sum::<usize>()) as u64
    }
}

/// Query weights prepared once for scoring many immutable document summaries.
/// Contributions accumulate in the reference scorer's lexical f32 order.
pub struct PreparedQuery<'a> {
    terms: Vec<(&'a str, TermWeight)>,
    prefix: Option<&'a str>,
}
impl<'a> PreparedQuery<'a> {
    /// Prepare exact weights for this query, corpus snapshot, and configuration.
    pub fn new(query: &'a Query, stats: &CorpusStats, params: &FtsParams) -> Self {
        Self {
            terms: query
                .exact_terms()
                .into_iter()
                .map(|(term, qf)| {
                    (
                        term,
                        TermWeight::new(
                            qf,
                            *stats.document_frequencies.get(term).unwrap_or(&0),
                            stats,
                            params,
                        ),
                    )
                })
                .collect(),
            prefix: query.prefix(),
        }
    }
    /// Score cached counts with bit-identical arithmetic to [`score_document`].
    pub fn score(&self, document: &DocumentTerms) -> f32 {
        let mut score = 0.0;
        for (term, weight) in &self.terms {
            score += weight.score(document.frequency(term), document.length);
        }
        if let Some(prefix) = self.prefix
            && document
                .frequencies
                .range(prefix.to_owned()..)
                .next()
                .is_some_and(|(term, _)| term.starts_with(prefix))
        {
            score += 1.0;
        }
        score
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_bounds_refresh_after_new_frames_and_share_the_admission_budget() {
        let params = FtsParams::default();
        let stats = CorpusStats {
            total_docs: 256,
            average_doc_length: 1.0,
            ..Default::default()
        };
        let table = Saturation::new(&stats, &params);
        table.remember(10, &[(0, 1)], &[1; 256], 128);
        {
            let mut maxima = table.maxima.write().unwrap();
            maxima.remember_term(10, vec![1.0, -1.0].into(), 1);
            assert_eq!(maxima.term(10).as_deref(), Some([1.0, -1.0].as_slice()));
        }
        table.remember(20, &[(128, 1)], &[1; 256], 128);
        let mut maxima = table.maxima.write().unwrap();
        assert!(
            maxima.term(10).is_none(),
            "a new frame tightens the summary"
        );
        let bytes = maxima.bytes;
        maxima.remember_term(10, vec![1.0, 1.0].into(), 2);
        assert_eq!(maxima.bytes, bytes, "replacement charges only one vector");
        assert_eq!(maxima.term(10).as_deref(), Some([1.0, 1.0].as_slice()));
        for offset in 100..300 {
            maxima.remember_term(offset, vec![1.0; 8192].into(), 2);
        }
        assert!(maxima.bytes <= Saturation::MAX_BOUND_BYTES);
        assert!(maxima.terms.len() < 200);
        assert_eq!(maxima.term(10).as_deref(), Some([1.0, 1.0].as_slice()));
    }

    #[test]
    fn concurrent_bound_publication_preserves_the_observed_generation() {
        let table = Saturation::new(
            &CorpusStats {
                total_docs: 256,
                average_doc_length: 1.0,
                ..Default::default()
            },
            &FtsParams::default(),
        );
        table.remember(10, &[(0, 1)], &[1; 256], 128);
        let old_generation = table.maxima.read().unwrap().frames.len();
        let old: Arc<[f64]> = vec![2.0, 2.0].into();
        table.remember(20, &[(128, 1)], &[1; 256], 128);
        {
            let mut cache = table.maxima.write().unwrap();
            cache.remember_term(10, old.clone(), old_generation);
            assert!(cache.term(10).is_none(), "late publication must stay stale");
            cache.remember_term(10, vec![1.0, 1.0].into(), 2);
        }
        let current = table.maxima.read().unwrap().term(10).unwrap();
        // An owned snapshot holds no read guard. A delayed older computation
        // cannot replace this newer summary or prevent another frame admission.
        let mut cache = table.maxima.try_write().unwrap();
        cache.remember_term(10, old.clone(), old_generation);
        assert!(Arc::ptr_eq(&current, &cache.term(10).unwrap()));
        assert_eq!(current.as_ref(), [1.0, 1.0]);
        assert_eq!(old.as_ref(), [2.0, 2.0]);
    }

    #[test]
    fn prepared_weights_preserve_reference_score_bits() {
        for k1 in [0.0, 0.5, 1.2, 3.0] {
            for b in [0.0, 0.75, 1.0] {
                for k3 in [0.0, 8.0, 100.0] {
                    let p = FtsParams {
                        k1,
                        b,
                        k3,
                        ..Default::default()
                    };
                    for n in [0, 1, 128, 1_000_000] {
                        for average in [0.0, 1.0, 73.8123] {
                            let stats = CorpusStats {
                                total_docs: n,
                                average_doc_length: average,
                                ..Default::default()
                            };
                            let table = Saturation::new(&stats, &p);
                            for df in [0, 1, n / 2, n, n + 1] {
                                for qf in [0, 1, 3, 10] {
                                    let weight = TermWeight::new(qf, df, &stats, &p);
                                    for tf in [0, 1, 4, 16, 17, 128] {
                                        for length in [1, 32, 128, 1023, 1024, 1025, 4096] {
                                            assert_eq!(
                                                weight.score_cached(tf, length, &table).to_bits(),
                                                term_score(tf, length, qf, df, &stats, &p)
                                                    .to_bits()
                                            );
                                            assert_eq!(
                                                weight.score(tf, length).to_bits(),
                                                term_score(tf, length, qf, df, &stats, &p)
                                                    .to_bits()
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod prepared_tests {
    use super::*;
    use crate::{tokenize, tokenize_array};
    #[test]
    fn prepared_counts_preserve_reference_bits_for_queries_and_parameters() {
        for k1 in [0.1, 1.2, 3.0] {
            for b in [0.0, 0.75, 1.0] {
                for k3 in [0.1, 8.0, 100.0] {
                    let params = FtsParams {
                        k1,
                        b,
                        k3,
                        ..Default::default()
                    };
                    let documents: Vec<_> = [
                        "",
                        "a",
                        "a a z",
                        "red foxes fox",
                        "prefix prefixed prelude",
                        "é 東京",
                    ]
                    .map(|s| tokenize(s, &params))
                    .into_iter()
                    .chain([tokenize_array(&["a red", "a fox", ""], &params)])
                    .collect();
                    let stats = CorpusStats::from_documents(&documents);
                    for raw in [
                        "",
                        "a",
                        "a a z",
                        "red fox missing",
                        "pre",
                        "prefix pre",
                        "é 東京",
                    ] {
                        for prefix in [false, true] {
                            let query = Query::new(tokenize(raw, &params), prefix);
                            let scorer = PreparedQuery::new(&query, &stats, &params);
                            for tokens in &documents {
                                let terms = DocumentTerms::new(tokens);
                                assert_eq!(terms.len(), tokens.len() as u32);
                                assert_eq!(terms.is_empty(), tokens.is_empty());
                                assert_eq!(
                                    scorer.score(&terms).to_bits(),
                                    score_document(tokens, &query, &stats, &params).to_bits(),
                                    "{raw} prefix={prefix}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
