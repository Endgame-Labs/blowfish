use bf_engine::Engine;
use bf_storage::{Storage, StorageConfig};
use bf_types::*;
use serde_json::{Value, json};

async fn engine() -> Engine {
    Engine::new(
        Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap(),
    )
}
async fn write(e: &Engine, ns: &str, v: Value) -> Result<WriteResponse, ApiError> {
    e.write(ns, serde_json::from_value(v).unwrap()).await
}
async fn query(e: &Engine, ns: &str, v: Value) -> Value {
    serde_json::to_value(
        e.query(ns, serde_json::from_value(v).unwrap())
            .await
            .unwrap(),
    )
    .unwrap()
}
async fn seed(e: &Engine) {
    write(e,"test",json!({"distance_metric":"cosine_distance","schema":{"text":{"type":"string","full_text_search":true},"tags":"[]string"},"upsert_rows":[{"id":1,"vector":[0.1,0.2,0.3],"n":5,"tags":["a","b"],"text":"the quick brown fox","color":"red"},{"id":2,"vector":[0.3,0.1,0.0],"n":7,"tags":["b"],"text":"lazy dogs sleep all day","color":"blue"},{"id":3,"vector":[0.0,0.0,1.0],"n":1,"tags":[],"text":"a fox and a dog","color":"red"}]})).await.unwrap();
}
#[tokio::test]
async fn oracle_vector_bm25_metadata_and_base64() {
    let e = engine().await;
    seed(&e).await;
    let result = query(
        &e,
        "test",
        json!({"rank_by":["vector","ANN",[0.1,0.2,0.4]],"top_k":3,"include_attributes":true}),
    )
    .await;
    assert_eq!(result["rows"][0]["id"], 1);
    assert!(result["rows"][0]["$dist"].as_f64().unwrap() < 0.01);
    assert_eq!(result["performance"]["exhaustive_search_count"], 3);
    let result = query(
        &e,
        "test",
        json!({"rank_by":["text","BM25","fox"],"top_k":10,"include_attributes":["text"]}),
    )
    .await;
    assert_eq!(result["rows"][0]["id"], 1);
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
    assert!((result["rows"][0]["$dist"].as_f64().unwrap() - 0.49917623).abs() < 0.00001);
    assert!((result["rows"][1]["$dist"].as_f64().unwrap() - 0.45665967).abs() < 0.00001);
    let metadata = e.metadata("test").await.unwrap();
    assert_eq!(metadata["schema"]["text"]["filterable"], false);
    assert_eq!(
        metadata["schema"]["text"]["full_text_search"]["tokenizer"],
        "word_v4"
    );
    assert_eq!(
        metadata["schema"]["vector"]["ann"]["distance_metric"],
        "cosine_distance"
    );
    assert_eq!(metadata["schema"]["id"], json!({"type":"uint"}));
    let schema = e.schema("test").await.unwrap();
    assert_eq!(schema["vector"]["ann"], true);
    assert!(schema["id"]["filterable"].is_null());
    let result=query(&e,"test",json!({"rank_by":["id","asc"],"limit":1,"vector_encoding":"base64","include_attributes":["vector"]})).await;
    assert_eq!(
        decode_vector(&result["rows"][0]["vector"]).unwrap(),
        vec![0.1, 0.2, 0.3]
    );
}
#[tokio::test]
async fn atomic_validation_conditions_and_write_phases() {
    let e = engine().await;
    seed(&e).await;
    assert_eq!(
        write(
            &e,
            "test",
            json!({"upsert_rows":[{"id":4,"vector":[1,0,0]},{"id":"mixed","vector":[0,1,0]}]})
        )
        .await
        .unwrap_err()
        .status,
        400
    );
    assert_eq!(
        query(&e, "test", json!({"aggregate_by":{"c":["Count"]}})).await["aggregations"]["c"],
        3
    );
    let r=write(&e,"test",json!({"upsert_rows":[{"id":1,"vector":[1,0,0],"n":4}],"upsert_condition":["n","Lt",{"$ref_new":"n"}]})).await.unwrap();
    assert_eq!(r.rows_affected, 0);
    let r=write(&e,"test",json!({"patch_rows":[{"id":1,"n":50},{"id":999,"n":4}],"deletes":[2],"return_affected_ids":true})).await.unwrap();
    assert_eq!(r.rows_affected, 2);
    assert_eq!(r.patched_ids, vec![Id::Uint(1)]);
    let r=write(&e,"test",json!({"delete_by_filter":["id","Eq",1],"patch_by_filter":{"filters":["n","Gte",0],"patch":{"n":10}},"upsert_rows":[{"id":4,"vector":[1,0,0],"n":3}]})).await.unwrap();
    assert_eq!(r.rows_deleted, Some(1));
    assert_eq!(r.rows_patched, Some(1));
    assert_eq!(r.rows_remaining, Some(false));
    assert_eq!(
        query(
            &e,
            "test",
            json!({"rank_by":["id","asc"],"limit":10,"include_attributes":["n"]})
        )
        .await["rows"],
        json!([{"id":3,"n":10},{"id":4,"n":3}])
    );
    let r = write(&e, "test", json!({"patch_rows":[{"id":3,"n":null}]}))
        .await
        .unwrap();
    assert_eq!(r.rows_patched, Some(1));
    assert_eq!(
        query(&e, "test", json!({"rank_by":["n","asc"],"limit":10})).await["rows"][0]["id"],
        3
    );
    assert!(
        write(
            &e,
            "test",
            json!({"upsert_rows":[{"id":5,"vector":[1,0,0]}],"deletes":[5]})
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn null_array_glob_regex_and_token_filters() {
    let e = engine().await;
    write(&e,"f",json!({"schema":{"id":"uint","n":"int","tags":"[]int","s":{"type":"string","glob":true,"regex":true,"filterable":true},"text":{"type":"[]string","full_text_search":true}},"upsert_rows":[{"id":1,"n":2,"tags":[2,4],"s":"Fox.txt","text":["quick brown","fox dog"]},{"id":2,"n":null,"tags":[],"s":"dog.rs","text":["quick brown fox"]},{"id":3}]})).await.unwrap();
    for (f, ids) in [
        (json!(["n", "Eq", null]), vec![2, 3]),
        (json!(["n", "NotEq", null]), vec![1]),
        (json!(["n", "Lt", 3]), vec![1, 2, 3]),
        (json!(["n", "Lt", null]), vec![]),
        (json!(["tags", "AnyGt", 3]), vec![1]),
        (json!(["tags", "ContainsAny", [4, 7]]), vec![1]),
        (json!(["tags", "NotContains", 4]), vec![2, 3]),
        (json!(["s", "IGlob", "*.TXT"]), vec![1]),
        (json!(["s", "Regex", "^dog\\."]), vec![2]),
        (
            json!(["text", "ContainsAllTokens", "brown fox"]),
            vec![1, 2],
        ),
        (
            json!(["text", "ContainsTokenSequence", "brown fox"]),
            vec![2],
        ),
        (
            json!(["text","ContainsAnyToken","fo",{"last_as_prefix":true}]),
            vec![1, 2],
        ),
        (
            json!(["And", [["n", "In", [2]], ["Not", ["s", "Glob", "dog*"]]]]),
            vec![1],
        ),
        (json!(["Or", []]), vec![]),
    ] {
        let result = query(
            &e,
            "f",
            json!({"rank_by":["id","asc"],"top_k":10,"filters":f}),
        )
        .await;
        let got: Vec<_> = result["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_u64().unwrap())
            .collect();
        assert_eq!(got, ids, "filter {f}");
    }
    let bad = e
        .query(
            "f",
            serde_json::from_value(
                json!({"rank_by":["id","asc"],"top_k":10,"filters":["s","Fuzzy","fox"]}),
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(bad.status, 400);
}
#[tokio::test]
async fn score_expressions_aggregations_groups_and_diversification() {
    let e = engine().await;
    seed(&e).await;
    let result=query(&e,"test",json!({"rank_by":["Sum",[["Product",2,["color","Eq","red"]],["Saturate",["Attribute","n"],{"midpoint":5}]]],"limit":{"total":3,"per":{"attributes":["color"],"limit":1}},"include_attributes":["n"]})).await;
    assert_eq!(result["rows"][0]["id"], 1);
    assert_eq!(result["rows"][1]["id"], 2);
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
    assert_eq!(
        query(
            &e,
            "test",
            json!({"rank_by":["color","Eq","blue"],"limit":10})
        )
        .await["rows"],
        json!([{"id":2,"$dist":1.0}])
    );
    assert_eq!(query(&e,"test",json!({"aggregate_by":{"c":["Count"],"sum":["Sum","n"],"min":["Min","color"],"max":["Max","n"]}})).await["aggregations"],json!({"c":3,"sum":13,"min":"blue","max":7}));
    assert_eq!(
        query(
            &e,
            "test",
            json!({"aggregate_by":{"c":["Count"]},"group_by":[{"tag":["ForEachUnique","tags"]}]})
        )
        .await["aggregation_groups"],
        json!([{"tag":"a","c":1},{"tag":"b","c":2}])
    );
    let result = query(
        &e,
        "test",
        json!({"rank_by":[["color","asc"],["n","desc"]],"limit":2,"offset":1}),
    )
    .await;
    assert_eq!(result["rows"], json!([{"id":1},{"id":3}]));
    assert!(
        e.query(
            "test",
            serde_json::from_value(json!({"rank_by":["Attribute","n"],"top_k":3})).unwrap()
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn snapshot_multi_query_rrf_copy_delete_and_recreate() {
    let e = engine().await;
    seed(&e).await;
    let result=query(&e,"test",json!({"queries":[{"rank_by":["n","desc"],"top_k":3},{"rank_by":["n","asc"],"top_k":3}],"rerank_by":["RRF",{"rank_constant":1,"weights":[2,1]}],"limit":2})).await;
    assert_eq!(result["results"].as_array().unwrap().len(), 1);
    assert_eq!(result["results"][0]["rows"][0]["id"], 2);
    write(
        &e,
        "copy",
        json!({"copy_from_namespace":{"source_namespace":"test"}}),
    )
    .await
    .unwrap();
    e.delete_namespace("test").await.unwrap();
    assert_eq!(e.schema("test").await.unwrap_err().status, 404);
    assert_eq!(
        query(&e, "copy", json!({"aggregate_by":{"c":["Count"]}})).await["aggregations"]["c"],
        3
    );
    write(&e, "test", json!({"upsert_rows":[{"id":"new","n":1}]}))
        .await
        .unwrap();
    assert_eq!(
        query(&e, "test", json!({"rank_by":["id","asc"],"limit":10})).await["rows"],
        json!([{"id":"new"}])
    );
    let first = e.list_namespaces("", None, 1).await.unwrap();
    assert_eq!(first["namespaces"][0]["id"], "copy");
    let next = e
        .list_namespaces("", first["next_cursor"].as_str(), 1)
        .await
        .unwrap();
    assert_eq!(next["namespaces"][0]["id"], "test");
    assert!(next["next_cursor"].is_null());
}
#[tokio::test]
async fn uuid_datetime_f16_and_flat_column_vectors() {
    let e = engine().await;
    write(&e,"typed",json!({"distance_metric":"cosine_distance","schema":{"id":"uuid","d":"datetime","v":{"type":"[2]f16","ann":true},"u":"uint","a":"[]uuid"},"upsert_columns":{"id":["00112233-4455-6677-8899-aabbccddeeff"],"d":["1970-01-01"],"v":[0.5,1.0],"u":[18446744073709551615_u64],"a":[[]]}})).await.unwrap();
    let result=query(&e,"typed",json!({"rank_by":["v","ANN","AAAAPwAAgD8="],"top_k":1,"include_attributes":true,"vector_encoding":"base64","filters":["d","Eq","1970-01-01T00:00:00Z"]})).await;
    assert_eq!(result["rows"][0]["v"], "ADgAPA==");
    assert_eq!(result["rows"][0]["u"], json!(u64::MAX));
    let result=query(&e,"typed",json!({"rank_by":["Decay",["Dist",["Attribute","d"],"1970-01-01T06:00:00Z"],{"midpoint":"6h"}],"limit":1})).await;
    assert_eq!(result["rows"][0]["$dist"], 0.5);
}

#[tokio::test]
async fn conditional_deletes_ref_new_patches_schema_and_validation_edges() {
    let e = engine().await;
    write(&e,"edge",json!({"schema":{"id":"uint","a":"[]int"},"upsert_rows":[{"id":1,"n":1,"a":[]},{"id":2,"n":2,"a":[2]}]})).await.unwrap();
    let r=write(&e,"edge",json!({"patch_columns":{"id":[1,3],"n":[3,9]},"patch_condition":["n","Lt",{"$ref_new":"n"}]})).await.unwrap();
    assert_eq!(r.rows_patched, Some(1));
    let r = write(
        &e,
        "edge",
        json!({"deletes":[1,2],"delete_condition":["n","Eq",2],"return_affected_ids":true}),
    )
    .await
    .unwrap();
    assert_eq!(r.deleted_ids, vec![Id::Uint(2)]);
    e.update_schema(
        "edge",
        serde_json::from_value(json!({"n":{"type":"int","filterable":false}})).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        e.query(
            "edge",
            serde_json::from_value(
                json!({"rank_by":["id","asc"],"limit":10,"filters":["n","Eq",3]})
            )
            .unwrap()
        )
        .await
        .is_err()
    );
    e.update_schema(
        "edge",
        serde_json::from_value(json!({"a":{"drop":true}})).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        !e.metadata("edge").await.unwrap()["schema"]
            .as_object()
            .unwrap()
            .contains_key("a")
    );
    assert!(
        !query(
            &e,
            "edge",
            json!({"rank_by":["id","asc"],"limit":1,"include_attributes":true})
        )
        .await["rows"][0]
            .as_object()
            .unwrap()
            .contains_key("a")
    );
    assert!(
        write(&e, "edge", json!({"schema":{"n":"string"}}))
            .await
            .is_err()
    );
    assert!(
        write(
            &e,
            "vectors",
            json!({"upsert_rows":[{"id":1,"vector":[1,0]}]})
        )
        .await
        .is_err()
    );
    write(
        &e,
        "vectors",
        json!({"distance_metric":"euclidean_squared","upsert_rows":[{"id":1,"vector":[1,0]}]}),
    )
    .await
    .unwrap();
    assert!(
        write(
            &e,
            "vectors",
            json!({"patch_rows":[{"id":1,"vector":[0,1]}]})
        )
        .await
        .is_err()
    );
    assert!(
        write(
            &e,
            "vectors",
            json!({"patch_by_filter":{"filters":["id","Eq",1],"patch":{"vector":null}}})
        )
        .await
        .is_err()
    );
    let result = query(
        &e,
        "vectors",
        json!({"rank_by":["vector","kNN",[0,1]],"filters":["id","Eq",1],"limit":1}),
    )
    .await;
    assert_eq!(result["rows"][0]["$dist"], 2.0);
    write(&e,"large",json!({"schema":{"u":"uint"},"upsert_rows":[{"id":18446744073709551615_u64,"u":18446744073709551615_u64},{"id":18446744073709551614_u64,"u":0}]})).await.unwrap();
    let rows = query(
        &e,
        "large",
        json!({"rank_by":["id","asc"],"limit":2,"filters":["u","Gte",0]}),
    )
    .await;
    assert_eq!(rows["rows"][0]["id"], json!(u64::MAX - 1));
    assert_eq!(
        query(&e, "large", json!({"aggregate_by":{"sum":["Sum","u"]}})).await["aggregations"]["sum"],
        json!(u64::MAX)
    );
}

#[tokio::test]
async fn patch_by_filter_cap_is_atomic_and_partial_reports_remaining() {
    let e = engine().await;
    let docs: Vec<_> = (0..50_001)
        .map(|id| json!({"id":id,"done":false}))
        .collect();
    write(&e, "cap", json!({"upsert_rows":docs})).await.unwrap();
    let patch = json!({"patch_by_filter":{"filters":["done","Eq",false],"patch":{"done":true}}});
    assert!(write(&e, "cap", patch.clone()).await.is_err());
    assert_eq!(
        query(
            &e,
            "cap",
            json!({"filters":["done","Eq",false],"aggregate_by":{"c":["Count"]}})
        )
        .await["aggregations"]["c"],
        50_001
    );
    let mut partial = patch;
    partial["patch_by_filter_allow_partial"] = json!(true);
    let r = write(&e, "cap", partial.clone()).await.unwrap();
    assert_eq!(r.rows_patched, Some(50_000));
    assert_eq!(r.rows_remaining, Some(true));
    let r = write(&e, "cap", partial).await.unwrap();
    assert_eq!(r.rows_patched, Some(1));
    assert_eq!(r.rows_remaining, Some(false));
}

#[tokio::test]
async fn attribute_scores_accept_a_literal_zero_floor() {
    let e = engine().await;
    write(
        &e,
        "scores",
        json!({"upsert_rows":[{"id":1,"n":-2},{"id":2,"n":3},{"id":3,"n":1}]}),
    )
    .await
    .unwrap();
    let result = query(
        &e,
        "scores",
        json!({"rank_by":["Max",[0,["Attribute","n"]]],"top_k":10}),
    )
    .await;
    assert_eq!(
        result["rows"],
        json!([{"id":2,"$dist":3.0},{"id":3,"$dist":1.0}])
    );
}

#[tokio::test]
async fn decay_accepts_signed_numeric_attribute_scores() {
    let e = engine().await;
    write(&e, "decay", json!({"schema":{"f":"float"},"upsert_rows":[{"id":1,"n":1,"f":1.0},{"id":2,"n":2,"f":2.0}]})).await.unwrap();
    for attribute in ["n", "f"] {
        let result = query(
            &e,
            "decay",
            json!({"rank_by":["Decay",["Attribute",attribute],{"midpoint":2}],"top_k":10}),
        )
        .await;
        let rows = result["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
        assert!((rows[0]["$dist"].as_f64().unwrap() - 2.0 / 3.0).abs() < 1e-6);
        assert_eq!(rows[1]["$dist"], 0.5);
    }
}
