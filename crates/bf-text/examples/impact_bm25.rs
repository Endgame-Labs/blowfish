//! Offline B4 kill tests over T1's five 200K-row segments and 1,000 dev queries.
//!
//! Prepare fixtures with `experiments::prepare_msmarco`, then run this example
//! with the fixture directory and `baseline`, `impact`, or `dense` as arguments.
//! It embeds the unchanged reader to inspect private posting metadata without
//! expanding the library API or enabling an experimental production code path.
pub use analysis::*;
pub use scoring::{CorpusStats, Query};
#[allow(dead_code)]
#[path = "../src/scoring.rs"]
mod scoring;
#[allow(dead_code)]
mod reader {
    include!("../src/segment.rs");
    pub mod experiment {
        include!("impact_bm25/prototype.rs");
    }
}
fn main() {
    reader::experiment::run();
}
#[allow(dead_code)]
#[path = "../src/analysis.rs"]
mod analysis;
#[cfg(test)]
pub use scoring::score_document;
