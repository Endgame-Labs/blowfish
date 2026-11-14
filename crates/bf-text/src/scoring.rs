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
