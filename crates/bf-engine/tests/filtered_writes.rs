//! Filtered deletes and patches resolve their rows without loading the namespace. Every case runs
//! twice: against a namespace with indexed segments plus a WAL tail (the fast path), and against
//! the same data held only in the tail (every row evaluated document by document, which is what
//! the old full scan did). Responses and final contents must match exactly.
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::{Value, json};

async fn engine() -> Engine {
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
            recall_sample_rate: 0.0,
            ..Default::default()
        },
    )
}

async fn write(e: &Engine, request: Value) -> Result<Value, String> {
    let mut request = request;
    request["return_affected_ids"] = json!(true);
    e.write("t", serde_json::from_value(request).unwrap())
        .await
        .map(|r| {
            let mut v = serde_json::to_value(r).unwrap();
            v.as_object_mut().unwrap().remove("performance");
            v.as_object_mut().unwrap().remove("billing");
            v
        })
        .map_err(|e| format!("{e:?}"))
}

async fn dump(e: &Engine) -> Vec<Value> {
    e.query(
        "t",
        serde_json::from_value(
            json!({"rank_by":["id","asc"],"top_k":10000,"include_attributes":true}),
        )
        .unwrap(),
    )
    .await
    .unwrap()
    .result
    .rows
    .unwrap()
    .into_iter()
    .map(|r| serde_json::to_value(r).unwrap())
    .collect()
}

fn base_rows() -> Vec<Value> {
    (0..3000u64)
        .map(|id| {
            let s = ["a", "b", "c"][(id % 3) as usize];
            json!({"id": id, "vector": [id as f32, 1.0], "k": (id % 10) as i64,
                "s": s, "tags": [format!("t{}", id % 4)]})
        })
        .collect()
}

/// Tail edits over the base rows: moves rows between filters, deletes, re-upserts, new rows.
fn tail_edits() -> Vec<Value> {
    vec![
        // Rows 3, 13 leave k=3; row 4 enters it.
        json!({"upsert_rows": [
            {"id": 3, "vector": [3.0, 1.0], "k": 30, "s": "a", "tags": ["t3"]},
            {"id": 13, "vector": [13.0, 1.0], "k": 30, "s": "b", "tags": ["t1"]},
            {"id": 4, "vector": [4.0, 1.0], "k": 3, "s": "c", "tags": ["t0"]},
            {"id": 5000, "vector": [0.0, 1.0], "k": 3, "s": "a", "tags": ["t0"]},
            {"id": 5001, "vector": [0.0, 1.0], "k": 5, "s": "b", "tags": ["t9"]}]}),
        // Deleted, then re-upserted with a different value.
        json!({"deletes": [23, 33]}),
        json!({"upsert_rows": [{"id": 23, "vector": [23.0, 1.0], "k": 3, "s": "b", "tags": ["t3"]}]}),
        json!({"patch_rows": [{"id": 43, "k": 3}, {"id": 53, "s": "zzz"}]}),
    ]
}

fn cases() -> Vec<(&'static str, Vec<Value>)> {
    vec![
        (
            "delete eq",
            vec![json!({"delete_by_filter": ["k", "Eq", 3]})],
        ),
        (
            "delete none",
            vec![json!({"delete_by_filter": ["k", "Eq", 77]})],
        ),
        (
            "delete in",
            vec![json!({"delete_by_filter": ["k", "In", [3, 5, 30]]})],
        ),
        (
            "delete and/or/not",
            vec![json!({"delete_by_filter":
                ["And", [["Or", [["k", "Eq", 3], ["s", "Eq", "zzz"]]], ["Not", ["tags", "Contains", "t9"]]]]})],
        ),
        (
            "delete by id",
            vec![json!({"delete_by_filter": ["id", "In", [3, 4, 23, 33, 5000, 2999, 123456]]})],
        ),
        (
            "delete range",
            vec![json!({"delete_by_filter": ["id", "Lt", 40]})],
        ),
        (
            "delete array",
            vec![json!({"delete_by_filter": ["tags", "Contains", "t0"]})],
        ),
        (
            "patch eq",
            vec![
                json!({"patch_by_filter": {"filters": ["k", "Eq", 3], "patch": {"s": "patched"}}}),
            ],
        ),
        (
            "patch changes the filtered attribute",
            vec![json!({"patch_by_filter": {"filters": ["k", "Eq", 3], "patch": {"k": 4}}})],
        ),
        (
            "patch then delete on the patched attribute, two requests",
            vec![
                json!({"patch_by_filter": {"filters": ["k", "Eq", 5], "patch": {"k": 6}}}),
                json!({"delete_by_filter": ["k", "Eq", 6]}),
            ],
        ),
        (
            "delete then patch on the survivors",
            vec![
                json!({"delete_by_filter": ["s", "Eq", "a"]}),
                json!({"patch_by_filter": {"filters": ["k", "Gte", 0], "patch": {"k": 1000}}}),
            ],
        ),
        (
            "one request: filtered delete plus upsert of a matching id",
            vec![json!({"delete_by_filter": ["k", "Eq", 3],
                "upsert_rows": [{"id": 13, "vector": [1.0, 1.0], "k": 3, "s": "x", "tags": ["t0"]},
                                {"id": 9000, "vector": [1.0, 1.0], "k": 3, "s": "x", "tags": ["t0"]}]})],
        ),
        (
            "one request: filtered patch plus patch of the same id and a delete",
            vec![
                json!({"patch_by_filter": {"filters": ["k", "Eq", 3], "patch": {"s": "q"}},
                "patch_rows": [{"id": 4, "s": "r"}], "deletes": [23]}),
            ],
        ),
        (
            "upsert makes a row match a later filter",
            vec![
                json!({"upsert_rows": [{"id": 700, "vector": [1.0, 1.0], "k": 3, "s": "n", "tags": []}]}),
                json!({"delete_by_filter": ["k", "Eq", 3]}),
            ],
        ),
        (
            "delete a row, upsert it back, filter again",
            vec![
                json!({"deletes": [4]}),
                json!({"upsert_rows": [{"id": 4, "vector": [1.0, 1.0], "k": 3, "s": "n", "tags": []}]}),
                json!({"patch_by_filter": {"filters": ["k", "Eq", 3], "patch": {"s": "again"}}}),
            ],
        ),
        (
            "new attribute introduced by a patch, then filtered on",
            vec![
                json!({"patch_rows": [{"id": 8, "fresh": "yes"}]}),
                json!({"delete_by_filter": ["fresh", "Eq", "yes"]}),
            ],
        ),
        (
            "pure delete with a partial-delete flag",
            vec![
                json!({"delete_by_filter": ["k", "Eq", 3], "delete_by_filter_allow_partial": true}),
            ],
        ),
        (
            "pure delete matching tail-only, deleted, and re-upserted rows",
            vec![json!({"delete_by_filter": ["id", "In", [5000, 5001, 23, 33, 43, 53]]})],
        ),
        (
            "one request: filtered delete, filtered patch, and an upsert",
            vec![json!({"delete_by_filter": ["k", "Eq", 3],
                "patch_by_filter": {"filters": ["k", "Eq", 5], "patch": {"s": "p"}},
                "upsert_rows": [{"id": 5, "vector": [1.0, 1.0], "k": 3, "s": "x", "tags": []}]})],
        ),
        (
            "one request: filtered delete plus an ID delete of a matching row",
            vec![json!({"delete_by_filter": ["k", "Eq", 3], "deletes": [13, 3, 4]})],
        ),
        (
            "two pure deletes in a row, the second matching nothing",
            vec![
                json!({"delete_by_filter": ["s", "Eq", "b"]}),
                json!({"delete_by_filter": ["s", "Eq", "b"]}),
            ],
        ),
        (
            "unknown attribute",
            vec![json!({"delete_by_filter": ["nope", "Eq", 1]})],
        ),
        (
            "patch every row",
            vec![json!({"patch_by_filter": {"filters": ["id", "Gte", 0], "patch": {"s": "all"}}})],
        ),
    ]
}

async fn run(indexed: bool, case: &[Value]) -> (Vec<Result<Value, String>>, Vec<Value>) {
    let e = engine().await;
    e.write(
        "t",
        serde_json::from_value(json!({"distance_metric": "euclidean_squared",
            "schema": {"vector": {"type": "[2]f32"}}, "upsert_rows": base_rows()}))
        .unwrap(),
    )
    .await
    .unwrap();
    if indexed {
        e.index_namespace("t", true).await.unwrap();
    }
    for edit in tail_edits() {
        write(&e, edit).await.unwrap();
    }
    let mut out = vec![];
    for request in case {
        out.push(write(&e, request.clone()).await);
    }
    let contents = dump(&e).await;
    assert!(e.shutdown(std::time::Duration::from_secs(5)).await);
    (out, contents)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_writes_match_the_tail_only_reference() {
    for (name, case) in cases() {
        let fast = run(true, &case).await;
        let reference = run(false, &case).await;
        assert_eq!(fast.0, reference.0, "responses differ: {name}");
        assert_eq!(fast.1, reference.1, "contents differ: {name}");
        // The cases must actually do something, or they prove nothing.
        if !matches!(name, "delete none" | "unknown attribute") {
            assert!(
                fast.0.iter().any(|r| r
                    .as_ref()
                    .is_ok_and(|v| v["rows_affected"].as_u64() > Some(0))),
                "case affected no rows: {name}: {:?}",
                fast.0
            );
        }
    }
}

/// Several requests submitted together share a group; the result must equal running them one
/// after the other against the reference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_writes_in_one_group_match_sequential_reference() {
    let requests = vec![
        json!({"upsert_rows": [{"id": 800, "vector": [1.0, 1.0], "k": 3, "s": "g", "tags": []}]}),
        json!({"patch_by_filter": {"filters": ["k", "Eq", 3], "patch": {"k": 4}}}),
        json!({"delete_by_filter": ["k", "Eq", 4]}),
        json!({"upsert_rows": [{"id": 801, "vector": [1.0, 1.0], "k": 4, "s": "g", "tags": []}]}),
        json!({"delete_by_filter": ["s", "Eq", "g"]}),
    ];
    let subject = engine().await;
    let reference = engine().await;
    for (e, indexed) in [(&subject, true), (&reference, false)] {
        e.write(
            "t",
            serde_json::from_value(json!({"distance_metric": "euclidean_squared",
                "schema": {"vector": {"type": "[2]f32"}}, "upsert_rows": base_rows()}))
            .unwrap(),
        )
        .await
        .unwrap();
        if indexed {
            e.index_namespace("t", true).await.unwrap();
        }
        for edit in tail_edits() {
            write(e, edit).await.unwrap();
        }
    }
    let grouped = futures_join(&subject, &requests).await;
    let mut sequential = vec![];
    for r in &requests {
        sequential.push(write(&reference, r.clone()).await);
    }
    assert_eq!(grouped, sequential);
    assert_eq!(dump(&subject).await, dump(&reference).await);
}

async fn futures_join(e: &Engine, requests: &[Value]) -> Vec<Result<Value, String>> {
    // join_all polls in order, so the requests reach the namespace's queue in order.
    futures::future::join_all(requests.iter().map(|r| write(e, r.clone()))).await
}

/// Another writer's commit lands between this engine's snapshot and its write; the filter is
/// resolved against the new state, so the other writer's matching row is deleted too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_delete_sees_a_commit_from_another_writer() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let config = || EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        ..Default::default()
    };
    let a = Engine::with_config(store.clone(), config());
    let b = Engine::with_config(store, config());
    a.write(
        "t",
        serde_json::from_value(json!({"distance_metric": "euclidean_squared",
            "schema": {"vector": {"type": "[2]f32"}}, "upsert_rows": base_rows()}))
        .unwrap(),
    )
    .await
    .unwrap();
    a.index_namespace("t", true).await.unwrap();
    // `a` has a fresh view; `b` appends a matching row, and re-keys another so one stops matching.
    a.write(
        "t",
        serde_json::from_value(
            json!({"upsert_rows": [{"id": 1, "vector": [1.0, 1.0], "k": 1, "s": "a", "tags": []}]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.write(
        "t",
        serde_json::from_value(json!({"upsert_rows": [
        {"id": 6000, "vector": [1.0, 1.0], "k": 3, "s": "a", "tags": []},
        {"id": 3, "vector": [3.0, 1.0], "k": 31, "s": "a", "tags": []}]}))
        .unwrap(),
    )
    .await
    .unwrap();
    let response = write(&a, json!({"delete_by_filter": ["k", "Eq", 3]}))
        .await
        .unwrap();
    let deleted: Vec<u64> = response["deleted_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert!(
        deleted.contains(&6000),
        "other writer's row missed: {deleted:?}"
    );
    assert!(
        !deleted.contains(&3),
        "row that no longer matches was deleted"
    );
    let rows = dump(&a).await;
    assert!(rows.iter().all(|r| r["k"] != json!(3)));
    assert!(rows.iter().any(|r| r["id"] == json!(3)));
}

/// A pure filtered delete commits its validated ID set, and a lost WAL slot makes it resolve the
/// filter again: a row the other writer deleted is not reported, rows it added are, and rows it
/// moved out of the filter survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pure_filtered_delete_resolves_again_after_a_lost_slot() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let config = || EngineConfig {
        indexing: false,
        recall_sample_rate: 0.0,
        ..Default::default()
    };
    let a = Engine::with_config(store.clone(), config());
    let b = Engine::with_config(store, config());
    a.write(
        "t",
        serde_json::from_value(json!({"distance_metric": "euclidean_squared",
            "schema": {"vector": {"type": "[2]f32"}}, "upsert_rows": base_rows()}))
        .unwrap(),
    )
    .await
    .unwrap();
    a.index_namespace("t", true).await.unwrap();
    // `a` trusts its fresh view, so `b`'s commit is discovered only when `a`'s append loses.
    a.write(
        "t",
        serde_json::from_value(
            json!({"upsert_rows": [{"id": 1, "vector": [1.0, 1.0], "k": 1, "s": "a", "tags": []}]}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.write(
        "t",
        serde_json::from_value(json!({
            "upsert_rows": [
                {"id": 6000, "vector": [1.0, 1.0], "k": 3, "s": "a", "tags": []},
                {"id": 3, "vector": [3.0, 1.0], "k": 31, "s": "a", "tags": []}],
            "deletes": [13, 23]}))
        .unwrap(),
    )
    .await
    .unwrap();
    let response = write(&a, json!({"delete_by_filter": ["k", "Eq", 3]}))
        .await
        .unwrap();
    let deleted: std::collections::BTreeSet<u64> = response["deleted_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    let expected: std::collections::BTreeSet<u64> = (0..3000u64)
        .filter(|id| id % 10 == 3 && *id != 3 && *id != 13 && *id != 23)
        .chain([6000])
        .collect();
    assert_eq!(deleted, expected);
    assert_eq!(response["rows_deleted"], json!(expected.len()));
    let rows = dump(&a).await;
    assert!(rows.iter().all(|r| r["k"] != json!(3)));
    assert!(rows.iter().any(|r| r["id"] == json!(3)));
    assert!(rows.iter().all(|r| r["id"] != json!(13)));
    assert_eq!(rows.len(), 2999 - expected.len());
}

/// A patch over enough rows to hydrate in parallel chunks gives the tail-only reference's result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_filtered_patch_hydrates_in_chunks_exactly() {
    let rows: Vec<Value> = (0..20_000u64)
        .map(|id| json!({"id": id, "vector": [id as f32, 1.0], "k": (id % 2) as i64, "s": "a"}))
        .collect();
    let mut results = vec![];
    for indexed in [true, false] {
        let e = engine().await;
        for chunk in rows.chunks(5_000) {
            e.write(
                "t",
                serde_json::from_value(json!({"distance_metric": "euclidean_squared",
                    "schema": {"vector": {"type": "[2]f32"}}, "upsert_rows": chunk}))
                .unwrap(),
            )
            .await
            .unwrap();
        }
        if indexed {
            e.index_namespace("t", true).await.unwrap();
        }
        write(
            &e,
            json!({"upsert_rows": [{"id": 4, "vector": [4.0, 1.0], "k": 1, "s": "t"}]}),
        )
        .await
        .unwrap();
        let response = write(
            &e,
            json!({"patch_by_filter": {"filters": ["k", "Eq", 0], "patch": {"s": "patched"}}}),
        )
        .await
        .unwrap();
        results.push((response, dump(&e).await));
        assert!(e.shutdown(std::time::Duration::from_secs(5)).await);
    }
    assert_eq!(results[0].0["rows_patched"], json!(9_999));
    assert_eq!(results[0], results[1]);
}
