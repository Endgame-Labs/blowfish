//! `Sum(BM25, boost of one non-filterable attribute)` opens only the text windows that can reach the
//! `k`th total. These tests hold the shapes to full scoring (`query_exhaustive`, scores included) on
//! attributes with no filter dictionary (`m` is a uint and `q` a float, so no stream can help), across
//! segments, a WAL tail with overwrites and deletes, and a reindex, with filters and offsets, on a
//! tie-heavy corpus and a varied one, and check that the boosted path answered and answered.
#[path = "common/shapes.rs"]
mod common;
use common::*;
use serde_json::{Value, json};

fn shapes(a: &str, b: &str) -> Vec<(&'static str, Value)> {
    let (ca, cb) = (clause(a), clause(b));
    let float = json!(["Max", [["Attribute", "q"], 0]]);
    vec![
        ("bare", json!(["Sum", [ca, ["Attribute", "m"]]])),
        ("bare first", json!(["Sum", [["Attribute", "m"], ca]])),
        (
            "small weight",
            json!(["Sum", [ca, ["Product", 0.05, ["Attribute", "m"]]]]),
        ),
        (
            "weight",
            json!(["Sum", [ca, ["Product", 0.5, ["Attribute", "m"]]]]),
        ),
        (
            "big weight",
            json!(["Sum", [ca, ["Product", 40.0, ["Attribute", "m"]]]]),
        ),
        (
            "zero weight",
            json!(["Sum", [ca, ["Product", 0.0, ["Attribute", "m"]]]]),
        ),
        (
            "float",
            json!(["Sum", [ca, ["Product", 0.1, float.clone()]]]),
        ),
        (
            "float two clauses",
            json!(["Sum", [["Sum", [ca, cb]], ["Product", 0.1, float.clone()]]]),
        ),
        (
            "saturated",
            json!(["Sum", [ca, ["Product", 8.0, ["Saturate", ["Attribute", "m"], {"midpoint": 30}]]]]),
        ),
        (
            "max of attributes",
            json!(["Sum", [ca, ["Max", [["Attribute", "m"], 50]]]]),
        ),
        (
            "product of sum",
            json!(["Product", 2.0, ["Sum", [ca, ["Attribute", "m"]]]]),
        ),
    ]
}

async fn compare(e: &bf_engine::Engine, tag: &str, sets: &[(&str, &str)]) {
    for (a, b) in sets {
        for (name, rank) in shapes(a, b) {
            for (k, offset) in [(1, 0), (10, 3), (100, 0), (10, 70)] {
                let mut q = json!({"rank_by": rank, "top_k": k, "offset": offset,
                    "include_attributes": ["m", "q"]});
                if k == 10 {
                    q["filters"] = json!(["n", "NotEq", 0]);
                }
                identical(e, q, &format!("{tag} {name} {a}|{b} k={k} off={offset}")).await;
            }
        }
    }
}

async fn run(e: &bf_engine::Engine, sets: &[(&str, &str)], f: fn(u64) -> String) {
    compare(e, "segments", sets).await;
    write(
        e,
        json!({
            "patch_rows": [{"id": 0, "text": "plain"}, {"id": 5, "text": "red fox red fox wolf w3 w3 w3"},
                {"id": 77, "text": "blue bird w9 w9", "q": 99.5}, {"id": 100, "text": "nothing"}],
            "deletes": [1, 2, 9, 13, 64, 501, 600, 1200, 2100],
            "upsert_rows": batch_with(3000..3060, f),
        }),
    )
    .await;
    compare(e, "tail", sets).await;
    e.index_namespace("t", true).await.unwrap();
    compare(e, "reindexed", sets).await;
}

/// Enough rows for dozens of windows per segment.
async fn big_corpus(e: &bf_engine::Engine, f: fn(u64) -> String) {
    write(
        e,
        json!({"schema":{"text":{"type":"string","full_text_search":true},
            "q":{"type":"float","filterable":false},"m":{"type":"uint","filterable":false},
            "n":{"type":"uint","filterable":true}},
            "upsert_rows": batch_with(0..2000, f)}),
    )
    .await;
    e.index_namespace("t", true).await.unwrap();
    write(e, json!({"upsert_rows": batch_with(2000..3000, f)})).await;
    e.index_namespace("t", true).await.unwrap();
}

#[tokio::test]
async fn boosted_shapes_match_full_scoring_with_ties() {
    let e = node().await;
    big_corpus(&e, text).await;
    let before = bf_engine::boost_outcomes();
    run(&e, TIES, text).await;
    let after = bf_engine::boost_outcomes();
    assert!(after.segments > before.segments, "{before:?} {after:?}");
}
#[tokio::test]
async fn boosted_shapes_match_full_scoring_with_varied_scores() {
    let e = node().await;
    big_corpus(&e, varied).await;
    let before = bf_engine::boost_outcomes();
    run(&e, VARIED, varied).await;
    let after = bf_engine::boost_outcomes();
    assert!(after.segments > before.segments, "{before:?} {after:?}");
}

/// A row with no value for the attribute keeps the ordinal scorer (and the same answer).
#[tokio::test]
async fn a_missing_value_keeps_the_ordinal_scorer() {
    let e = node().await;
    write(
        &e,
        json!({"schema":{"text":{"type":"string","full_text_search":true},
            "m":{"type":"uint","filterable":false}},
            "upsert_rows": (0..600u64).map(|id| if id % 7 == 3 {
                json!({"id": id, "text": text(id)})
            } else {
                json!({"id": id, "text": text(id), "m": id % 13})
            }).collect::<Vec<_>>()}),
    )
    .await;
    e.index_namespace("t", true).await.unwrap();
    let before = bf_engine::boost_outcomes();
    for k in [1, 10, 50] {
        let rank = json!([
            "Sum",
            [clause("red fox"), ["Product", 0.5, ["Attribute", "m"]]]
        ]);
        identical(
            &e,
            json!({"rank_by": rank, "top_k": k}),
            &format!("missing k={k}"),
        )
        .await;
    }
    assert_eq!(bf_engine::boost_outcomes().segments, before.segments);
}
