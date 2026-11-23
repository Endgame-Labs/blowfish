use crate::{FtsParams, Token};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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
    pub(crate) maxima: std::sync::Mutex<BoundCache>,
}

#[derive(Default)]
pub(crate) struct BoundCache {
    pub(crate) frames: std::collections::HashMap<u64, Vec<(usize, f64)>>,
    bytes: usize,
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
    pub(crate) fn remember(
        &self,
        offset: u64,
        postings: &[(u32, u32)],
        lengths: &[u32],
        window_rows: usize,
    ) {
        {
            let maxima = self.maxima.lock().unwrap();
            if maxima.bytes >= Self::MAX_BOUND_BYTES || maxima.frames.contains_key(&offset) {
                return;
            }
        }
        let k1 = f32::from_bits(self.key.0) as f64;
        let b = f32::from_bits(self.key.1) as f64;
        let average = f64::from_bits(self.key.2);
        let mut windows: Vec<(usize, f64)> = Vec::new();
        for &(row, tf) in postings {
            let length = lengths[row as usize];
            let saturation = self
                .get(tf, length)
                .unwrap_or_else(|| {
                    let norm = k1 * (1.0 - b + b * length as f64 / average);
                    tf as f64 * (k1 + 1.0) / (tf as f64 + norm)
                })
                .max(0.0);
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
        let mut maxima = self.maxima.lock().unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;

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
