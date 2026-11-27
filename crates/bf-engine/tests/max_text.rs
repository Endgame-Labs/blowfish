//! `Max` of plain BM25 clauses takes each clause's top `k + offset` rows per segment. These
//! tests hold it to the full-scoring answer (`query_exhaustive`): exact ties at the cutoff
//! whose row order opposes ID order, several segments, a WAL tail that overwrites and deletes
//! rows from a clause's top list, filters, offsets, and three clauses.
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
/// A row's text: a few topic words, so many rows tie exactly (same words, same length).
fn text(id: u64) -> String {
    let a = ["red", "blue", "green", "gold"][(id % 4) as usize];
    let b = ["fox", "bird", "cat"][(id / 4 % 3) as usize];
    let c = ["wolf", "owl"][(id / 12 % 2) as usize];
    format!("{a} {b} {c}")
}
/// IDs descend within each batch, so row order opposes ID order for tied scores.
fn batch(ids: impl Iterator<Item = u64>) -> Vec<Value> {
    let mut ids: Vec<u64> = ids.collect();
    ids.reverse();
    ids.into_iter()
        .map(|id| json!({"id":id,"text":text(id),"n":id%5}))
        .collect()
}
async fn same(e: &Engine, q: Value, label: &str) {
    let q: QueryRequest = serde_json::from_value(q.clone()).unwrap();
    let fast = serde_json::to_value(e.query("t", q.clone()).await.unwrap().result).unwrap();
    let full = serde_json::to_value(e.query_exhaustive("t", q).await.unwrap().result).unwrap();
    assert_eq!(fast, full, "{label}");
}
fn clause(s: &str) -> Value {
    json!(["text", "BM25", s])
}
async fn compare_all(e: &Engine, tag: &str) {
    let sets: [&[&str]; 4] = [
        &["red fox", "blue bird"],
        &["wolf", "green cat owl"],
        &["red fox", "blue bird", "gold wolf"],
        &["fox", "fox bird"],
    ];
    for clauses in sets {
        for k in [1, 10, 100] {
            for offset in [0, 3, 25] {
                for filter in [
                    None,
                    Some(json!(["n", "Eq", 2])),
                    Some(json!(["n", "NotEq", 0])),
                ] {
                    let mut q = json!({
                        "rank_by": ["Max", clauses.iter().map(|c| clause(c)).collect::<Vec<_>>()],
                        "top_k": k, "offset": offset, "include_attributes": ["n"]
                    });
                    if let Some(f) = filter.clone() {
                        q["filters"] = f;
                    }
                    same(
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

#[tokio::test]
async fn max_of_bm25_matches_full_scoring_across_segments_ties_and_the_wal_tail() {
    let e = node().await;
    write(
        &e,
        json!({"schema":{"text":{"type":"string","full_text_search":true}},
            "upsert_rows": batch(0..60)}),
    )
    .await;
    e.index_namespace("t", true).await.unwrap();
    write(&e, json!({"upsert_rows": batch(60..130)})).await;
    e.index_namespace("t", true).await.unwrap();
    compare_all(&e, "segments").await;

    // A tail that rewrites rows of a clause's top list (to text that no longer matches, and to
    // a stronger match), deletes others, and adds rows that tie with indexed ones.
    write(
        &e,
        json!({
            "patch_rows": [{"id": 0, "text": "plain"}, {"id": 5, "text": "red fox red fox wolf"},
                {"id": 77, "text": "blue bird"}, {"id": 100, "text": "nothing"}],
            "deletes": [1, 2, 9, 13, 64],
            "upsert_rows": batch(130..150),
        }),
    )
    .await;
    compare_all(&e, "tail").await;

    // And again once the tail is indexed (shadowed rows now live in a segment's tombstones).
    e.index_namespace("t", true).await.unwrap();
    compare_all(&e, "reindexed").await;
}

#[tokio::test]
async fn sum_and_nested_max_keep_full_scoring() {
    let e = node().await;
    write(
        &e,
        json!({"schema":{"text":{"type":"string","full_text_search":true}},
            "upsert_rows": batch(0..80)}),
    )
    .await;
    e.index_namespace("t", true).await.unwrap();
    for rank in [
        json!(["Sum", [clause("red fox"), clause("blue bird")]]),
        json!([
            "Max",
            [["Sum", [clause("red"), clause("fox")]], clause("blue")]
        ]),
        json!(["Max", [clause("red fox"), ["n", "Eq", 1]]]),
        json!([
            "Max",
            [["Product", 2, clause("red fox")], clause("blue bird")]
        ]),
    ] {
        same(&e, json!({"rank_by": rank, "top_k": 10}), "non-plain").await;
    }
}
