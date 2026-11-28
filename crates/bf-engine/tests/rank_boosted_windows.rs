//! On a segment of many windows, a boosted `Sum` opens only the windows that can reach the `k`th
//! total, and the answer is still the full scorer's. Its own test binary, so no other test moves the
//! process-wide window counters.
#[path = "common/shapes.rs"]
mod common;
use common::*;
use serde_json::json;

#[tokio::test]
async fn windows_are_skipped_and_the_answer_is_exact() {
    let e = node().await;
    write(
        &e,
        json!({"schema":{"text":{"type":"string","full_text_search":true},
            "m":{"type":"uint","filterable":false}},
            "upsert_rows": (0..40_000u64).map(|id| {
                let mut r = row(id, varied);
                r["m"] = json!((id * 7919) % 101);
                r
            }).collect::<Vec<_>>()}),
    )
    .await;
    e.index_namespace("t", true).await.unwrap();
    let mut skipped = None;
    for (a, weight) in [("w3 w9", 0.05), ("w1 w40", 0.1), ("w600", 0.02)] {
        let rank = json!(["Sum", [clause(a), ["Product", weight, ["Attribute", "m"]]]]);
        let before = bf_engine::boost_outcomes();
        for k in [1, 10] {
            identical(
                &e,
                json!({"rank_by": rank, "top_k": k, "include_attributes": ["m"]}),
                a,
            )
            .await;
        }
        // A weak boost over a rare word leaves most windows unable to reach the kth total (the
        // other cases hold ties among high boosts, which every such window must open for).
        let after = bf_engine::boost_outcomes();
        assert!(after.segments > before.segments, "{after:?}");
        skipped.get_or_insert((
            after.opened - before.opened,
            after.candidates - before.candidates,
        ));
    }
    let (opened, candidates) = skipped.unwrap();
    assert!(
        opened * 2 < candidates,
        "opened {opened} of {candidates} windows"
    );
}
