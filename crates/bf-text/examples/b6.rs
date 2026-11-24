//! Disposable B6 exact-BM25 kill tests; no production API or format changes.
pub use analysis::*;
pub use scoring::{CorpusStats, Query};
#[allow(dead_code)]
#[path = "../src/analysis.rs"]
mod analysis;
#[allow(dead_code)]
#[path = "../src/scoring.rs"]
mod scoring;
#[allow(dead_code)]
mod reader {
    include!("../src/segment.rs");
    include!("b6/bands_search.rs");
    pub mod experiment {
        include!("b6/prototype.rs");
    }
}
fn main() {
    reader::experiment::run();
}
#[cfg(test)]
pub use scoring::score_document;
