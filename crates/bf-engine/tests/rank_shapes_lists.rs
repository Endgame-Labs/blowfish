//! Shapes with a bounded boost or a constant are answered from lists, not by the full scorer. The outcome
//! counters are per process, so this file holds only the one test that reads them.
use serde_json::json;
#[path = "common/shapes.rs"]
mod common;
use common::*;

/// Shapes with a bounded boost or a constant are answered from lists, not by the full scorer.
#[tokio::test]
async fn bounded_shapes_are_accepted_from_lists() {
    let e = node().await;
    corpus(&e, varied).await;
    for (name, rank) in shapes("w3 w9", "w1 w40") {
        let accepted = [
            "sum+const",
            "const+sum",
            "max+const",
            "product of sum",
            "saturate of sum",
            "sum of max",
            "max of sum",
            "recency",
            "popularity",
            "recency+popularity",
            "recency+bonus",
            "bonus+const",
            "three predicates",
            "decay of text",
            "decay small midpoint",
            "decay tiny midpoint",
            "decay of sum",
            "popularity dominates",
            "dist of text",
            "dist small",
            "dist huge",
            "bare attribute",
            "weighted bare attribute",
            "bare float guarded by max",
        ];
        if !accepted.contains(&name) {
            continue;
        }
        let before = bf_engine::sum_outcomes();
        identical(&e, json!({"rank_by": rank, "top_k": 10}), name).await;
        let after = bf_engine::sum_outcomes();
        let ok = after.first + after.grown - before.first - before.grown;
        let fallback = after.fallback - before.fallback;
        assert!(
            ok > 0 && fallback == 0,
            "{name}: {ok} accepted, {fallback} fallback"
        );
    }
}
