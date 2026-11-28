//! A BM25 query with token-set filters (`ContainsAllTokens`, `ContainsAnyToken`, and
//! `ContainsTokenSequence`, alone, anded, with prefixes, and negated) is held to full scoring
//! (`query_exhaustive`, which matches the filter on documents) across segments, a WAL tail with
//! overwrites and deletes, and a reindex. Token sets start from the first token's rows rather than
//! from every row of the segment, so empty, absent, and one-token queries are the edges.
#[path = "common/shapes.rs"]
mod common;
use common::*;
use serde_json::{Value, json};

fn gate_text(id: u64) -> String {
    let mut words = vec![format!("w{}", (id * 7) % 40)];
    if id.is_multiple_of(3) {
        words.push("common".into());
    }
    if id.is_multiple_of(2) {
        words.push("usual".into());
    }
    if id.is_multiple_of(29) {
        words.push("mid".into());
    }
    if id.is_multiple_of(501) {
        words.push("rare".into());
    }
    // "alpha beta" adjacent, reversed, and apart; `alpha` and `beta` are each common.
    match id % 8 {
        0 => words.push("alpha beta".into()),
        1 => words.push("beta alpha".into()),
        2 => words.push("alpha filler beta".into()),
        3 => words.push("alpha".into()),
        4 => words.push("beta".into()),
        5 => words.push("alpha beta alpha".into()),
        _ => {}
    }
    words.push(format!("u{}", id % 97));
    words.join(" ")
}
fn f(op: &str, text: &str) -> Value {
    json!(["text", op, text])
}
fn filters() -> Vec<(&'static str, Value)> {
    vec![
        ("all large+large", f("ContainsAllTokens", "common usual")),
        ("all large+rare", f("ContainsAllTokens", "common rare")),
        ("all rare", f("ContainsAllTokens", "rare")),
        ("all absent", f("ContainsAllTokens", "common nothere")),
        (
            "all prefix",
            json!(["text", "ContainsAllTokens", "comm", {"last_as_prefix": true}]),
        ),
        (
            "all two prefixes",
            json!(["text", "ContainsAllTokens", "usual comm", {"last_as_prefix": true}]),
        ),
        ("all empty", f("ContainsAllTokens", "")),
        ("any large", f("ContainsAnyToken", "common usual")),
        ("any large+mid", f("ContainsAnyToken", "common mid")),
        ("any mid+rare", f("ContainsAnyToken", "mid rare")),
        ("any absent", f("ContainsAnyToken", "nothere")),
        (
            "any prefix",
            json!(["text", "ContainsAnyToken", "us", {"last_as_prefix": true}]),
        ),
        ("seq large", f("ContainsTokenSequence", "alpha beta")),
        ("seq reversed", f("ContainsTokenSequence", "beta alpha")),
        ("seq large+mid", f("ContainsTokenSequence", "usual mid")),
        ("seq absent", f("ContainsTokenSequence", "alpha nothere")),
        (
            "and non-token",
            json!([
                "And",
                [f("ContainsAllTokens", "common usual"), ["n", "Eq", 2]]
            ]),
        ),
        (
            "and two tokens",
            json!([
                "And",
                [
                    f("ContainsAnyToken", "common mid"),
                    f("ContainsTokenSequence", "alpha beta")
                ]
            ]),
        ),
        (
            "nested and",
            json!([
                "And",
                [
                    ["And", [f("ContainsAllTokens", "common"), ["n", "Lt", 3]]],
                    f("ContainsAnyToken", "usual rare")
                ]
            ]),
        ),
        // Not and Or keep every row's token test, so they take the whole allowed set.
        (
            "not",
            json!(["Not", f("ContainsAllTokens", "common usual")]),
        ),
        (
            "or",
            json!([
                "Or",
                [
                    f("ContainsAllTokens", "common usual"),
                    f("ContainsAnyToken", "rare")
                ]
            ]),
        ),
    ]
}
const QUERIES: &[&str] = &["common", "w3 w9 usual", "rare mid", "alpha w5"];

async fn compare_all(e: &bf_engine::Engine, tag: &str) {
    for (name, filter) in filters() {
        for (i, text) in QUERIES.iter().enumerate() {
            // A few shapes per filter keep the debug build quick: vary k and offset by query.
            let (top_k, offset) = [(10, 0), (3, 2), (25, 0), (10, 7)][i];
            let q = json!({"rank_by": clause(text), "filters": filter,
                "top_k": top_k, "offset": offset, "include_attributes": ["n"]});
            identical(e, q, &format!("{tag} {name} / {text}")).await;
        }
    }
}
#[tokio::test]
async fn token_filters_with_bm25_match_full_scoring() {
    let e = node().await;
    let schema = json!({"text":{"type":"string","full_text_search":true},
        "n":{"type":"uint","filterable":true}});
    let batch = |ids: std::ops::Range<u64>| -> Vec<Value> {
        ids.rev()
            .map(|id| json!({"id": id, "text": gate_text(id), "n": id % 5}))
            .collect()
    };
    write(&e, json!({"schema": schema, "upsert_rows": batch(0..1500)})).await;
    e.index_namespace("t", true).await.unwrap();
    write(&e, json!({"upsert_rows": batch(1500..2400)})).await;
    e.index_namespace("t", true).await.unwrap();
    compare_all(&e, "segments").await;
    write(
        &e,
        json!({
            "patch_rows": [{"id": 0, "text": "plain"}, {"id": 6, "text": "common usual alpha beta"},
                {"id": 3, "text": "rare common"}],
            "deletes": [1, 2, 9, 12, 501, 1002, 2004],
            "upsert_rows": batch(2400..2500),
        }),
    )
    .await;
    compare_all(&e, "tail").await;
    e.index_namespace("t", true).await.unwrap();
    compare_all(&e, "reindexed").await;
}
