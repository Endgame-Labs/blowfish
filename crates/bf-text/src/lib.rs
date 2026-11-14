//! Text analysis, namespace-wide BM25 scoring, and immutable range-readable text segments.
//!
//! Row IDs are segment-local `u32` ordinals. The engine owns document IDs, live-row
//! masks, schema validation, and aggregation of corpus statistics across segments and the WAL.
#![warn(missing_docs)]

mod analysis;
mod scoring;

pub use analysis::{
    FtsParams, Language, Token, Tokenizer, matches_all_tokens, matches_any_token,
    matches_token_sequence, tokenize, tokenize_array,
};
pub use scoring::{CorpusStats, Query, score_document};
mod segment;
pub use segment::{
    MemoryRangeRead, RangeRead, SearchHit, SearchMetrics, SegmentBuilder, SegmentReader,
};
