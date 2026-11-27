//! Monotone expressions over BM25 (constants, nesting, row-wise terms such as a recency decay or
//! a saturated attribute, and three predicates) decide the top `k + offset` from short per-clause
//! lists when a strict bound proves them. These tests hold every shape to the full-scoring
//! answer (`query_exhaustive`, scores included) across segments, a WAL tail with overwrites and
//! deletes, a reindex, filters, and offsets, on a tie-heavy corpus and a varied one.
#[path = "common/shapes.rs"]
mod common;
use common::*;

#[tokio::test]
async fn expression_shapes_match_full_scoring_with_ties() {
    let e = node().await;
    corpus(&e, text).await;
    run(&e, TIES, text).await;
}
#[tokio::test]
async fn expression_shapes_match_full_scoring_with_varied_scores() {
    let e = node().await;
    corpus(&e, varied).await;
    run(&e, VARIED, varied).await;
}
