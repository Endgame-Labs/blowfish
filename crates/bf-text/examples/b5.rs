//! Offline Round B5 kill tests. No experimental library or production format path.
//! See `b5/README.md` for fixture preparation and measurement instructions.
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
    pub mod experiment {
        include!("b5/prototype.rs");
    }
}
fn main() {
    reader::experiment::run();
}
#[cfg(test)]
pub use scoring::score_document;
