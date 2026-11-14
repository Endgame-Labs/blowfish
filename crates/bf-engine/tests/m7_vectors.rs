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
async fn write(e: &Engine, body: Value) {
    e.write("vectors", serde_json::from_value(body).unwrap())
        .await
        .unwrap();
}
async fn rows(e: &Engine, rank: Value) -> Value {
    serde_json::to_value(
        e.query(
            "vectors",
            serde_json::from_value(json!({"rank_by":rank,"top_k":10,"include_attributes":true}))
                .unwrap(),
        )
        .await
        .unwrap(),
    )
    .unwrap()["rows"]
        .clone()
}
#[tokio::test]
async fn sparse_f16_dot_products_composition_and_indexed_tail() {
    let e = node().await;
    write(&e,json!({"schema":{"s":{"type":"{}f16","sparse_knn":{"distance_metric":"dot_product"}}},"upsert_rows":[
        {"id":1,"s":{"a":0.3333,"b":2}}, {"id":2,"s":{"b":1}}, {"id":3,"s":{}}, {"id":4}]})).await;
    let ranks = [
        json!(["s","SparseKNN",{"a":0.3333,"b":1}]),
        json!(["Sum",[["s","SparseKNN",{"a":0.3333}],["s","SparseKNN",{"b":1}]]]),
        json!(["Saturate",["s","SparseKNN",{"b":1}],{"midpoint":2}]),
        json!(["Max",[["s","SparseKNN",{"b":1}],0]]),
    ];
    let mut expected = vec![];
    for rank in &ranks {
        expected.push(rows(&e, rank.clone()).await);
    }
    assert_eq!(expected[0][0]["id"], 1);
    assert!((expected[0][0]["$dist"].as_f64().unwrap() - 2.1110568).abs() < 1e-6);
    assert_eq!(expected[0][0]["s"]["a"], json!(0.33325195));
    e.index_namespace("vectors", true).await.unwrap();
    assert!(
        e.storage()
            .list("ns/vectors/segments/")
            .await
            .unwrap()
            .iter()
            .any(|f| f.key.contains("sparse."))
    );
    for (rank, want) in ranks.iter().zip(expected) {
        assert_eq!(rows(&e, rank.clone()).await, want);
    }
    write(
        &e,
        json!({"patch_rows":[{"id":2,"s":{"b":10}}],"deletes":[1]}),
    )
    .await;
    let want = rows(&e, ranks[0].clone()).await;
    assert_eq!(want[0]["id"], 2);
    assert_eq!(want[0]["$dist"], 10.0);
    e.index_namespace("vectors", true).await.unwrap();
    assert_eq!(rows(&e, ranks[0].clone()).await, want);
    assert_eq!(rows(&e, json!(["s", "SparseKNN", {}])).await, json!([]));
    for value in [
        json!({"a":0}),
        json!({"a":-1}),
        json!({"a":1e-20}),
        json!({"a":1e10}),
    ] {
        assert_eq!(
            e.write(
                "vectors",
                serde_json::from_value(json!({"upsert_rows":[{"id":5,"s":value}]})).unwrap()
            )
            .await
            .unwrap_err()
            .status,
            400
        );
        assert!(bf_types::decode_sparse(&value).is_err());
    }
}

#[tokio::test]
async fn i8_wire_storage_ann_base64_and_validation() {
    let e = node().await;
    write(&e,json!({"distance_metric":"euclidean_squared","schema":{"v":{"type":"[2]i8","ann":true}},"upsert_rows":[
        {"id":1,"v":[-128,127]},{"id":2,"v":bf_types::encode_vector(&[1.0,2.0],false)},{"id":3,"v":[3,4]}]})).await;
    let rank = json!(["v", "ANN", [1, 2]]);
    let expected = rows(&e, rank.clone()).await;
    assert_eq!(expected[0]["id"], 2);
    assert_eq!(expected[0]["$dist"], 0.0);
    e.index_namespace("vectors", true).await.unwrap();
    assert_eq!(rows(&e, rank).await, expected);
    let r=e.query("vectors",serde_json::from_value(json!({"rank_by":["v","kNN",[1,2]],"filters":["id","Gt",0],"top_k":10,"include_attributes":true,"vector_encoding":"base64"})).unwrap()).await.unwrap();
    use base64::Engine as _;
    assert_eq!(
        base64::prelude::BASE64_STANDARD
            .decode(r.result.rows.unwrap()[0]["v"].as_str().unwrap())
            .unwrap(),
        [1, 2]
    );
    for invalid in [json!([128, 0]), json!([-129, 0]), json!([0.5, 0])] {
        assert_eq!(
            e.write(
                "vectors",
                serde_json::from_value(json!({"upsert_rows":[{"id":4,"v":invalid}]})).unwrap()
            )
            .await
            .unwrap_err()
            .status,
            400
        );
        assert_eq!(
            e.query(
                "vectors",
                serde_json::from_value(json!({"rank_by":["v","ANN",invalid],"top_k":1})).unwrap()
            )
            .await
            .unwrap_err()
            .status,
            400
        );
    }
}
