//! Helpers shared by the rank-shape tests: a node, a corpus with scalar columns, the shape list, and the
//! comparison with the full scorer.
#![allow(dead_code)]
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use bf_types::{QueryRequest, WriteRequest};
use serde_json::{Value, json};
use std::time::Duration;

pub async fn node() -> Engine {
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
pub async fn write(e: &Engine, body: Value) {
    e.write("t", serde_json::from_value::<WriteRequest>(body).unwrap())
        .await
        .unwrap();
}
pub fn varied(id: u64) -> String {
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
pub fn text(id: u64) -> String {
    let a = ["red", "blue", "green", "gold"][(id % 4) as usize];
    let b = ["fox", "bird", "cat"][(id / 4 % 3) as usize];
    let c = ["wolf", "owl"][(id / 12 % 2) as usize];
    format!("{a} {b} {c}")
}
/// A row: text, a small integer, a float in `[0, 100)` with many repeats, and a date.
pub fn row(id: u64, text: fn(u64) -> String) -> Value {
    let pop = ((id * 37) % 101) as f64 * 0.99;
    let day = 1 + (id * 7) % 28;
    let month = 1 + (id / 3) % 12;
    json!({"id": id, "text": text(id), "n": id % 5, "pop": pop,
        "ts": format!("2026-{month:02}-{day:02}T00:00:00Z")})
}
/// IDs descend within each batch, so row order opposes ID order for tied scores.
pub fn batch_with(ids: impl Iterator<Item = u64>, text: fn(u64) -> String) -> Vec<Value> {
    let mut ids: Vec<u64> = ids.collect();
    ids.reverse();
    ids.into_iter().map(|id| row(id, text)).collect()
}
pub async fn identical(e: &Engine, q: Value, label: &str) {
    let q: QueryRequest = serde_json::from_value(q).unwrap();
    let fast = serde_json::to_string(&e.query("t", q.clone()).await.unwrap().result).unwrap();
    let full = serde_json::to_string(&e.query_exhaustive("t", q).await.unwrap().result).unwrap();
    assert_eq!(fast, full, "{label}");
}
pub fn clause(s: &str) -> Value {
    json!(["text", "BM25", s])
}
pub const NOW: &str = "2026-06-15T00:00:00Z";
pub fn recency(w: f64) -> Value {
    json!(["Product", w, ["Decay", ["Dist", ["Attribute", "ts"], NOW], {"midpoint": "60d"}]])
}
pub fn popularity(w: f64) -> Value {
    json!(["Product", w, ["Saturate", ["Attribute", "pop"], {"midpoint": 20}]])
}
/// Every shape built from one or two query strings.
pub fn shapes(a: &str, b: &str) -> Vec<(&'static str, Value)> {
    let (ca, cb) = (clause(a), clause(b));
    vec![
        // A bare clause keeps its own path.
        ("plain", ca.clone()),
        ("sum+const", json!(["Sum", [ca, 0.1]])),
        ("const+sum", json!(["Sum", [0.25, ca, cb]])),
        ("sum+zero", json!(["Sum", [ca, 0]])),
        ("max+const", json!(["Max", [ca, 0.1]])),
        ("max+big const", json!(["Max", [ca, cb, 3.0]])),
        ("product of sum", json!(["Product", 2.0, ["Sum", [ca, cb]]])),
        (
            "saturate of sum",
            json!(["Saturate", ["Sum", [ca, cb]], {"midpoint": 8}]),
        ),
        (
            "sum of max",
            json!(["Sum", [["Max", [ca, cb]], clause("w1 w2")]]),
        ),
        ("max of sum", json!(["Max", [["Sum", [ca, cb]], ca]])),
        (
            "sum of saturates in max",
            json!(["Max", [["Sum", [ca, cb]], ["Saturate", cb, {"midpoint": 5}]]]),
        ),
        ("recency", json!(["Sum", [ca, recency(2.0)]])),
        ("recency 2 clauses", json!(["Sum", [ca, recency(4.0), cb]])),
        ("popularity", json!(["Sum", [ca, popularity(3.0)]])),
        (
            "popularity dominates",
            json!(["Sum", [ca, popularity(500.0)]]),
        ),
        (
            "recency+popularity",
            json!(["Sum", [ca, recency(1.0), popularity(1.5)]]),
        ),
        (
            "zero weight",
            json!(["Sum", [ca, ["Product", 0.0, popularity(1.0)]]]),
        ),
        (
            "recency+bonus",
            json!(["Sum", [ca, recency(1.0), ["Product", 2.0, ["n", "Eq", 2]]]]),
        ),
        (
            "bonus+const",
            json!(["Sum", [ca, 0.5, ["Product", 2.0, ["n", "Eq", 2]]]]),
        ),
        ("max of doc", json!(["Max", [ca, popularity(2.0)]])),
        (
            "three predicates",
            json!([
                "Sum",
                [
                    ca,
                    ["Product", 2.0, ["n", "Eq", 1]],
                    ["Product", 3.0, ["n", "Eq", 2]],
                    ["Product", 1.0, ["n", "In", [3, 4]]]
                ]
            ]),
        ),
        (
            "attribute alone is unbounded",
            json!(["Sum", [ca, ["Max", [["Attribute", "n"], 0]]]]),
        ),
        ("decay of text", json!(["Decay", ca, {"midpoint": 10}])),
        (
            "decay small midpoint",
            json!(["Decay", ca, {"midpoint": 2, "exponent": 2}]),
        ),
        (
            "decay tiny midpoint",
            json!(["Decay", ca, {"midpoint": 1e9}]),
        ),
        (
            "decay of sum",
            json!(["Decay", ["Sum", [ca, cb]], {"midpoint": 6}]),
        ),
        ("dist of text", json!(["Dist", ca, 5.0])),
    ]
}
pub async fn compare(e: &Engine, tag: &str, sets: &[(&str, &str)]) {
    for (a, b) in sets {
        for (name, rank) in shapes(a, b) {
            for (k, offset) in [(1, 0), (10, 3), (100, 0), (10, 70)] {
                let mut q = json!({"rank_by": rank, "top_k": k, "offset": offset,
                    "include_attributes": ["n"]});
                if k == 10 {
                    q["filters"] = json!(["n", "NotEq", 0]);
                }
                identical(e, q, &format!("{tag} {name} {a}|{b} k={k} off={offset}")).await;
            }
        }
    }
}
pub const TIES: &[(&str, &str)] = &[("red fox", "blue bird"), ("wolf", "green cat owl")];
pub const VARIED: &[(&str, &str)] = &[("w3 w9", "w1 w40"), ("w0", "w2 w5 w300"), ("w600", "w4")];

pub async fn corpus(e: &Engine, f: fn(u64) -> String) {
    write(
        e,
        json!({"schema":{"text":{"type":"string","full_text_search":true},
            "pop":{"type":"float"},"ts":{"type":"datetime"}},
            "upsert_rows": batch_with(0..500, f)}),
    )
    .await;
    e.index_namespace("t", true).await.unwrap();
    write(e, json!({"upsert_rows": batch_with(500..900, f)})).await;
    e.index_namespace("t", true).await.unwrap();
}
pub async fn run(e: &Engine, sets: &[(&str, &str)], f: fn(u64) -> String) {
    compare(e, "segments", sets).await;
    write(
        e,
        json!({
            "patch_rows": [{"id": 0, "text": "plain"}, {"id": 5, "text": "red fox red fox wolf w3 w3 w3"},
                {"id": 77, "text": "blue bird w9 w9", "pop": 99.5}, {"id": 100, "text": "nothing"}],
            "deletes": [1, 2, 9, 13, 64, 501, 600],
            "upsert_rows": batch_with(900..960, f),
        }),
    )
    .await;
    compare(e, "tail", sets).await;
    e.index_namespace("t", true).await.unwrap();
    compare(e, "reindexed", sets).await;
}
