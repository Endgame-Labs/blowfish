//! `Sum` of plain BM25 clauses decides the top `k + offset` from short per-clause lists when a
//! strict bound proves them, and otherwise scores every row. These tests hold it to the
//! full-scoring answer (`query_exhaustive`, scores included) on a tie-heavy corpus (the bound
//! often fails, so the fallback runs) and on a varied one (the bound often holds), across
//! segments, a WAL tail that overwrites and deletes rows, filters, offsets, and three clauses.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use serde_json::{Value, json};
use std::time::Duration;

async fn node() -> Engine {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    Engine::with_config(
        store,
        EngineConfig {
            indexing: false,
            retention: Duration::ZERO,
            compaction_segments: 64,
            ..Default::default()
        },
    )
}
async fn write(e: &Engine, body: Value) {
    e.write("t", serde_json::from_value::<WriteRequest>(body).unwrap())
        .await
        .unwrap();
}
/// Varied text: 3 to 8 words from a skewed vocabulary, so scores rarely tie.
fn varied(id: u64) -> String {
    let mut x = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let len = 3 + next() % 6;
    (0..len)
        .map(|_| {
            let r = next() % 1000;
            format!("w{}", (r * r) / 5000)
        })
        .collect::<Vec<_>>()
        .join(" ")
}
/// A row's text: a few topic words, so many rows tie exactly (same words, same length).
fn text(id: u64) -> String {
    let a = ["red", "blue", "green", "gold"][(id % 4) as usize];
    let b = ["fox", "bird", "cat"][(id / 4 % 3) as usize];
    let c = ["wolf", "owl"][(id / 12 % 2) as usize];
    format!("{a} {b} {c}")
}
/// IDs descend within each batch, so row order opposes ID order for tied scores.
fn batch_with(ids: impl Iterator<Item = u64>, text: fn(u64) -> String) -> Vec<Value> {
    let mut ids: Vec<u64> = ids.collect();
    ids.reverse();
    ids.into_iter()
        .map(|id| json!({"id":id,"text":text(id),"n":id%5}))
        .collect()
}
/// Same rows and score bits (JSON equality compares floats by value, so compare the text).
async fn identical(e: &Engine, q: Value, label: &str) {
    let q: QueryRequest = serde_json::from_value(q).unwrap();
    let fast = serde_json::to_string(&e.query("t", q.clone()).await.unwrap().result).unwrap();
    let full = serde_json::to_string(&e.query_exhaustive("t", q).await.unwrap().result).unwrap();
    assert_eq!(fast, full, "{label}");
}
fn clause(s: &str) -> Value {
    json!(["text", "BM25", s])
}
async fn compare(e: &Engine, tag: &str, sets: &[&[&str]]) {
    for clauses in sets {
        for k in [1, 10, 100] {
            for offset in [0, 3, 70] {
                for filter in [
                    None,
                    Some(json!(["n", "Eq", 2])),
                    Some(json!(["n", "NotEq", 0])),
                ] {
                    let mut q = json!({
                        "rank_by": ["Sum", clauses.iter().map(|c| clause(c)).collect::<Vec<_>>()],
                        "top_k": k, "offset": offset, "include_attributes": ["n"]
                    });
                    if let Some(f) = filter.clone() {
                        q["filters"] = f;
                    }
                    identical(
                        e,
                        q,
                        &format!("{tag} {clauses:?} k={k} off={offset} {filter:?}"),
                    )
                    .await;
                }
            }
        }
    }
}
const TIES: &[&[&str]] = &[
    &["red fox", "blue bird"],
    &["wolf", "green cat owl"],
    &["red fox", "blue bird", "gold wolf"],
    &["fox", "fox bird"],
];
const VARIED: &[&[&str]] = &[
    &["w3 w9", "w1 w40"],
    &["w0", "w2 w5 w300"],
    &["w7", "w12 w13", "w2 w3 w600"],
    &["w100 w200", "w150", "w1"],
    &["w4", "w4"],
];

async fn corpus(e: &Engine, f: fn(u64) -> String) {
    write(
        e,
        json!({"schema":{"text":{"type":"string","full_text_search":true}},
            "upsert_rows": batch_with(0..500, f)}),
    )
    .await;
    e.index_namespace("t", true).await.unwrap();
    write(e, json!({"upsert_rows": batch_with(500..900, f)})).await;
    e.index_namespace("t", true).await.unwrap();
}
async fn tail_and_reindex(e: &Engine, sets: &[&[&str]], f: fn(u64) -> String) {
    compare(e, "segments", sets).await;
    write(
        e,
        json!({
            "patch_rows": [{"id": 0, "text": "plain"}, {"id": 5, "text": "red fox red fox wolf w3 w3 w3"},
                {"id": 77, "text": "blue bird w9 w9"}, {"id": 100, "text": "nothing"}],
            "deletes": [1, 2, 9, 13, 64, 501, 600],
            "upsert_rows": batch_with(900..960, f),
        }),
    )
    .await;
    compare(e, "tail", sets).await;
    e.index_namespace("t", true).await.unwrap();
    compare(e, "reindexed", sets).await;
}

#[tokio::test]
async fn sum_of_bm25_matches_full_scoring_with_ties_where_the_bound_fails() {
    let e = node().await;
    corpus(&e, text).await;
    tail_and_reindex(&e, TIES, text).await;
}

#[tokio::test]
async fn sum_of_bm25_matches_full_scoring_with_varied_scores_where_the_bound_holds() {
    let e = node().await;
    corpus(&e, varied).await;
    tail_and_reindex(&e, VARIED, varied).await;
}

#[tokio::test]
async fn sum_with_a_huge_page_and_in_the_tail_alone_matches_full_scoring() {
    let e = node().await;
    corpus(&e, varied).await;
    for top_k in [json!(1200), json!(4096), json!(4097), json!(9000)] {
        let q = json!({"rank_by": ["Sum", [clause("w1 w2"), clause("w3")]], "top_k": top_k});
        identical(&e, q, "page").await;
    }
    // A segment-less namespace (everything in the WAL tail) takes the same path.
    let t = node().await;
    write(
        &t,
        json!({"schema":{"text":{"type":"string","full_text_search":true}},
            "upsert_rows": batch_with(0..300, varied)}),
    )
    .await;
    compare(&t, "tail only", VARIED).await;
}

/// Past the longest list (4,096 rows) a tie-heavy corpus never proves the bound, so the lists
/// grow to their cap and the full scorer answers: still exactly.
#[tokio::test]
async fn sum_falls_back_to_full_scoring_when_no_list_proves_the_answer() {
    let e = node().await;
    write(
        &e,
        json!({"schema":{"text":{"type":"string","full_text_search":true}},
            "upsert_rows": batch_with(0..5200, text)}),
    )
    .await;
    e.index_namespace("t", true).await.unwrap();
    write(
        &e,
        json!({"patch_rows": [{"id": 3, "text": "red fox red fox red fox wolf"}],
            "deletes": [4, 5, 6], "upsert_rows": batch_with(5200..5230, text)}),
    )
    .await;
    for (k, offset) in [(1, 0), (10, 0), (100, 7)] {
        for clauses in [&["red fox", "blue bird"][..], &["fox", "fox bird", "wolf"]] {
            let q = json!({"rank_by": ["Sum", clauses.iter().map(|c| clause(c)).collect::<Vec<_>>()],
                "top_k": k, "offset": offset, "include_attributes": ["n"]});
            identical(&e, q, &format!("{clauses:?} k={k} offset={offset}")).await;
        }
    }
}
