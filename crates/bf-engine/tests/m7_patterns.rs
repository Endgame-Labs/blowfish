use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::{Value, json};

async fn node() -> Engine {
    Engine::with_config(
        Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap(),
        EngineConfig {
            indexing: false,
            ..Default::default()
        },
    )
}
async fn write(e: &Engine, value: Value) {
    e.write("patterns", serde_json::from_value(value).unwrap())
        .await
        .unwrap();
}
async fn rows(e: &Engine, filter: Value) -> Value {
    serde_json::to_value(
        e.query(
            "patterns",
            serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":100,"filters":filter}))
                .unwrap(),
        )
        .await
        .unwrap(),
    )
    .unwrap()["rows"]
        .clone()
}
#[tokio::test]
async fn fuzzy_substrings_thresholds_case_arrays_and_index_overlay() {
    let e = node().await;
    write(&e, json!({"schema":{"s":{"type":"string","fuzzy":true},"a":{"type":"[]string","fuzzy":true}},
        "upsert_rows":[{"id":1,"s":"prefix turbopuffer suffix","a":["turbo","puffer"]},{"id":2,"s":"Turbopufer","a":["a turbopuffer"]},{"id":3,"s":"túrbopuffer"},{"id":4}]})).await;
    let params = json!({"max_edit_distance":[{"min_query_chars":3,"distance":0},{"min_query_chars":6,"distance":1}],"case_sensitive":false});
    let tests = [
        (
            json!(["s", "Fuzzy", "turbopufer", params]),
            json!([{"id":1},{"id":2}]),
        ),
        (
            json!(["s", "Fuzzy", "turbopuffer", params]),
            json!([{"id":1},{"id":2},{"id":3}]),
        ),
        (json!(["s", "Fuzzy", "tu", params]), json!([])),
        (
            json!(["a", "Fuzzy", "turbopuffer", params]),
            json!([{"id":2}]),
        ),
        (
            json!(["Not", ["s", "Fuzzy", "turbopuffer", params]]),
            json!([{"id":4}]),
        ),
    ];
    for (filter, expected) in &tests {
        assert_eq!(rows(&e, filter.clone()).await, *expected);
    }
    e.index_namespace("patterns", true).await.unwrap();
    assert!(
        e.storage()
            .list("ns/patterns/segments/")
            .await
            .unwrap()
            .iter()
            .any(|f| f.key.contains("trigram."))
    );
    for (filter, expected) in &tests {
        assert_eq!(rows(&e, filter.clone()).await, *expected);
    }
    write(
        &e,
        json!({"patch_rows":[{"id":1,"s":"unrelated"}],"deletes":[2]}),
    )
    .await;
    assert_eq!(
        rows(&e, json!(["s", "Fuzzy", "turbopuffer", params])).await,
        json!([{"id":3}])
    );
    e.index_namespace("patterns", true).await.unwrap();
    assert_eq!(
        rows(&e, json!(["s", "Fuzzy", "turbopuffer", params])).await,
        json!([{"id":3}])
    );
}
#[tokio::test]
async fn online_fuzzy_schema_rebuild_and_validation() {
    let e = node().await;
    write(&e, json!({"upsert_rows":[{"id":1,"s":"turbopuffer"}]})).await;
    e.index_namespace("patterns", true).await.unwrap();
    write(&e, json!({"schema":{"s":{"fuzzy":true}}})).await;
    let q = json!({"rank_by":["id","asc"],"top_k":10,"filters":["s","Fuzzy","turbopufer",{"max_edit_distance":[{"min_query_chars":6,"distance":1}]}]});
    assert_eq!(
        e.query("patterns", serde_json::from_value(q.clone()).unwrap())
            .await
            .unwrap_err()
            .status,
        409
    );
    e.index_namespace("patterns", true).await.unwrap();
    assert_eq!(
        e.query("patterns", serde_json::from_value(q).unwrap())
            .await
            .unwrap()
            .result
            .rows
            .unwrap()
            .len(),
        1
    );
    for params in [
        json!({}),
        json!({"max_edit_distance":[]}),
        json!({"max_edit_distance":[{"min_query_chars":3,"distance":1}]}),
        json!({"max_edit_distance":[{"min_query_chars":12,"distance":3}]}),
    ] {
        assert_eq!(e.query("patterns",serde_json::from_value(json!({"rank_by":["id","asc"],"top_k":10,"filters":["s","Fuzzy","turbopuffer",params]})).unwrap()).await.unwrap_err().status,400);
    }
}

#[tokio::test]
async fn regex_and_glob_trigrams_preserve_boolean_and_unicode_semantics() {
    let e = node().await;
    write(&e,json!({"schema":{"s":{"type":"string","regex":true,"glob":true},"scan":{"type":"string","filterable":true}},"upsert_rows":[
        {"id":1,"s":"turbopuffer.txt","scan":"turbopuffer.txt"}, {"id":2,"s":"TURBOPUFFER.rs"}, {"id":3,"s":"turbofish.txt"}, {"id":4,"s":"ſtring.TXT"}, {"id":5}]})).await;
    let filters = [
        json!(["s", "Regex", "turbo(puffer|fish)\\.txt$"]),
        json!(["s", "Regex", "(turbo)?fish|PUFFER"]),
        json!(["s", "Regex", "(?i)turbopuffer"]),
        json!(["s", "Regex", "^.*$"]),
        json!(["s", "Glob", "*puffer.*"]),
        json!(["s", "NotGlob", "*puffer.*"]),
        json!(["s", "IGlob", "*PUFFER.*"]),
        json!(["s", "NotIGlob", "*PUFFER.*"]),
        json!(["s", "IGlob", "STRING.*"]),
        json!(["scan", "Glob", "*puffer.*"]),
        json!(["Not", ["s", "Regex", "fish"]]),
        json!(["Or", [["s", "Glob", "*fish*"], ["s", "Regex", "txt$"]]]),
    ];
    let mut expected = vec![];
    for f in &filters {
        expected.push(rows(&e, f.clone()).await);
    }
    e.index_namespace("patterns", true).await.unwrap();
    for (f, want) in filters.iter().zip(expected) {
        assert_eq!(rows(&e, f.clone()).await, want, "{f}");
    }
}
#[tokio::test]
async fn online_regex_and_glob_builds_return_retryable_conflicts() {
    for (setting, operator) in [("regex", "Regex"), ("glob", "Glob")] {
        let e = node().await;
        write(&e, json!({"upsert_rows":[{"id":1,"s":"turbopuffer"}]})).await;
        e.index_namespace("patterns", true).await.unwrap();
        write(&e, json!({"schema":{"s":{setting:true}}})).await;
        let q = json!({"rank_by":["id","asc"],"top_k":10,"filters":["s",operator,"turbopuffer"]});
        assert_eq!(
            e.query("patterns", serde_json::from_value(q.clone()).unwrap())
                .await
                .unwrap_err()
                .status,
            409
        );
        e.index_namespace("patterns", true).await.unwrap();
        assert_eq!(
            e.query("patterns", serde_json::from_value(q).unwrap())
                .await
                .unwrap()
                .result
                .rows
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn pattern_scores_use_trigram_indexes_and_fail_closed_on_missing_files() {
    let e = node().await;
    write(&e,json!({"schema":{"s":{"type":"string","fuzzy":true,"glob":true}},"upsert_rows":[{"id":1,"s":"turbopuffer"},{"id":2,"s":"other"}]})).await;
    let params = json!({"max_edit_distance":[{"min_query_chars":6,"distance":1}]});
    let rank = json!([
        "Sum",
        [
            ["s", "Fuzzy", "turbopufer", params],
            ["s", "Glob", "*puffer*"]
        ]
    ]);
    let q = json!({"rank_by":rank,"top_k":10});
    let expected = e
        .query("patterns", serde_json::from_value(q.clone()).unwrap())
        .await
        .unwrap()
        .result
        .rows
        .unwrap();
    assert_eq!(
        expected,
        json!([{"id":1,"$dist":2.0}])
            .as_array()
            .unwrap()
            .iter()
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .collect::<Vec<bf_types::Document>>()
    );
    e.index_namespace("patterns", true).await.unwrap();
    assert_eq!(
        e.query("patterns", serde_json::from_value(q.clone()).unwrap())
            .await
            .unwrap()
            .result
            .rows
            .unwrap(),
        expected
    );
    let file = e
        .storage()
        .list("ns/patterns/segments/")
        .await
        .unwrap()
        .into_iter()
        .find(|f| f.key.contains("trigram."))
        .unwrap();
    e.storage().delete(&file.key).await.unwrap();
    assert_eq!(
        e.query("patterns", serde_json::from_value(q).unwrap())
            .await
            .unwrap_err()
            .status,
        503
    );
}
