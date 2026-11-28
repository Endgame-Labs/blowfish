//! `limit.per` over filterable scalar attributes groups rows by a tuple of integer codes (the filter
//! dictionaries, inverted once per segment); every other attribute reads the column. Both must match
//! the full scorer: string and integer groups, rows with a null or missing value (one group),
//! arrays and non-filterable attributes (the column path), two attributes (tuples of codes, a direct table and a map), and every page shape,
//! across segments, a WAL tail, and a reindex.
#[path = "common/shapes.rs"]
mod common;
use common::*;
use serde_json::{Value, json};

fn rows(ids: std::ops::Range<u64>) -> Vec<Value> {
    ids.rev()
        .map(|id| {
            let mut row = json!({"id": id, "text": text(id), "n": id % 5, "wide": id % 300, "wider": id % 281,
                "plain": format!("p{}", id % 3), "tags": [format!("t{}", id % 2), format!("t{}", id % 7)]});
            // A string group with missing and null values.
            match id % 6 {
                0 | 1 => row["cat"] = json!(["red", "green", "blue"][(id % 3) as usize]),
                2 => row["cat"] = Value::Null,
                _ => {}
            }
            row
        })
        .collect()
}
async fn compare_all(e: &bf_engine::Engine, tag: &str) {
    for rank in [
        clause("red fox"),
        clause("wolf owl"),
        clause("blue bird green cat"),
    ] {
        for attributes in [
            vec!["cat"],
            vec!["n"],
            vec!["plain"],
            vec!["tags"],
            vec!["cat", "n"],
            vec!["n", "cat"],
            vec!["cat", "n", "cat"],
            // A plain attribute has no codes: the column path.
            vec!["cat", "plain"],
            // Two columns of 300 and 281 values: 84,300 tuples, over the direct table's size.
            vec!["wide", "wider"],
        ] {
            for (total, per, offset) in [(10, 1, 0), (10, 2, 3), (30, 3, 0), (5, 1, 4)] {
                let q = json!({"rank_by": rank, "limit": {"total": total,
                    "per": {"attributes": attributes, "limit": per}}, "offset": offset,
                    "include_attributes": ["cat", "n"]});
                identical(
                    e,
                    q,
                    &format!("{tag} {rank} per {attributes:?} {total}/{per}+{offset}"),
                )
                .await;
            }
        }
    }
}
#[tokio::test]
async fn limit_per_by_group_code_matches_full_scoring() {
    let e = node().await;
    write(
        &e,
        json!({"schema": {"text": {"type": "string", "full_text_search": true},
            "n": {"type": "uint", "filterable": true}, "wide": {"type": "uint", "filterable": true},
            "wider": {"type": "uint", "filterable": true}, "cat": {"type": "string", "filterable": true},
            "plain": {"type": "string", "filterable": false},
            "tags": {"type": "[]string", "filterable": true}},
            "upsert_rows": rows(0..400)}),
    )
    .await;
    e.index_namespace("t", true).await.unwrap();
    write(&e, json!({"upsert_rows": rows(400..700)})).await;
    e.index_namespace("t", true).await.unwrap();
    compare_all(&e, "segments").await;
    write(
        &e,
        json!({"patch_rows": [{"id": 0, "cat": "gold"}, {"id": 5, "cat": null}, {"id": 77, "text": "red fox wolf"}],
            "deletes": [1, 2, 9, 13, 64, 501],
            "upsert_rows": rows(700..760)}),
    )
    .await;
    compare_all(&e, "tail").await;
    e.index_namespace("t", true).await.unwrap();
    compare_all(&e, "reindexed").await;
}
