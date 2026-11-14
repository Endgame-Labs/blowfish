use bf_types::*;
use serde_json::{Value, json};

#[test]
fn filter_contract_roundtrips_every_operator() {
    for op in [
        "Eq",
        "NotEq",
        "Lt",
        "Lte",
        "Gt",
        "Gte",
        "AnyLt",
        "AnyLte",
        "AnyGt",
        "AnyGte",
        "Contains",
        "NotContains",
        "Glob",
        "NotGlob",
        "IGlob",
        "NotIGlob",
        "Regex",
        "Fuzzy",
        "ContainsAllTokens",
        "ContainsAnyToken",
        "ContainsTokenSequence",
    ] {
        let wire = json!(["attr", op, "term"]);
        let filter: Filter = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(filter).unwrap(), wire);
    }
    for op in ["In", "NotIn", "ContainsAny", "NotContainsAny"] {
        let wire = json!(["attr", op, [1, 2, null]]);
        assert_eq!(
            serde_json::to_value(serde_json::from_value::<Filter>(wire.clone()).unwrap()).unwrap(),
            wire
        );
    }
    for wire in [
        json!(["And", [["a", "Eq", null], ["b", "Gt", 3]]]),
        json!(["Or", []]),
        json!(["Not",["a","Eq",{"$ref_new":"a"}]]),
        json!(["t","ContainsAnyToken","fox",{"last_as_prefix":true}]),
    ] {
        assert_eq!(
            serde_json::to_value(serde_json::from_value::<Filter>(wire.clone()).unwrap()).unwrap(),
            wire
        );
    }
    assert_eq!(
        Filter::parse(json!(["x", "Unknown", 1]))
            .unwrap_err()
            .status,
        422
    );
    assert!(serde_json::from_value::<Filter>(json!(["x", "In", 3])).is_err());
}
#[test]
fn ranking_contract_roundtrips() {
    let examples = vec![
        json!(["vector", "ANN", [0.1, 0.2]]),
        json!(["vector", "kNN", "AACAPwAAAAA="]),
        json!(["content", "BM25", "quick fox"]),
        json!(["content", "BM25", ["tok1", "tok2"]]),
        json!(["content","BM25","lazy wal",{"last_as_prefix":true}]),
        json!(["Sum", [["t", "BM25", "fox"], ["n", "Eq", 1]]]),
        json!(["Max", [["Attribute", "clicks"], ["x", "Eq", 1]]]),
        json!(["Product", 2.0, ["x", "Eq", 1]]),
        json!(["Product", ["x", "Eq", 1], 2.0]),
        json!(["Attribute", "clicks"]),
        json!(["Max", [0, ["Attribute", "clicks"]]]),
        json!(["Sum", [1.5, ["Attribute", "clicks"]]]),
        json!(["Saturate",["Attribute","clicks"],{"midpoint":100,"exponent":1}]),
        json!(["Decay",["Dist",["Attribute","published_at"],"2026-02-03T12:13:14"],{"midpoint":"6h"}]),
        json!(["timestamp", "desc"]),
        json!([["category", "asc"], ["score", "desc"]]),
    ];
    for wire in examples {
        let expr: RankBy = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(expr).unwrap(), wire);
    }
    assert!(serde_json::from_value::<RankBy>(json!(["Product", -1, ["n", "Eq", 1]])).is_err());
}
#[test]
fn schema_types_and_sdk_write() {
    for t in [
        "string",
        "int",
        "uint",
        "float",
        "bool",
        "uuid",
        "datetime",
        "bytes",
        "[]string",
        "[]int",
        "[]uint",
        "[]float",
        "[]bool",
        "[]uuid",
        "[]datetime",
        "[768]f32",
        "[2]f16",
    ] {
        let parsed: AttributeType = t.parse().unwrap();
        assert_eq!(parsed.to_string(), t);
    }
    let req:WriteRequest=serde_json::from_value(json!({"upsert_rows":[{"id":1,"vector":"AACAPwAAAAA="}],"schema":{"text":{"type":"string","full_text_search":true},"id":"uint"},"upsert_condition":["version","Lt",{"$ref_new":"version"}]})).unwrap();
    assert_eq!(req.upsert_rows.unwrap()[0]["id"], 1);
    let copy: WriteRequest =
        serde_json::from_value(json!({"copy_from_namespace":{"source_namespace":"source"}}))
            .unwrap();
    assert!(copy.copy_from_namespace.is_some());
    let v = decode_vector(&json!("AACAPwAAAAA=")).unwrap();
    assert_eq!(v, vec![1.0, 0.0]);
    assert_eq!(encode_vector(&v, false), "AACAPwAAAAA=");
    assert!(decode_vector(&json!("AA==")).is_err());
    assert_eq!(datetime_millis("1970-01-01").unwrap(), 0);
    assert_eq!(datetime_millis("1970-01-01T01:00:00+01:00").unwrap(), 0);
}
#[test]
fn aggregate_group_and_query_wire() {
    for wire in [
        json!(["Count"]),
        json!(["Count", "x"]),
        json!(["Sum", "x"]),
        json!(["Min", "x"]),
        json!(["Max", "x"]),
    ] {
        assert_eq!(
            serde_json::to_value(serde_json::from_value::<Aggregate>(wire.clone()).unwrap())
                .unwrap(),
            wire
        );
    }
    for wire in [json!("color"), json!({"tag":["ForEachUnique","tags"]})] {
        assert_eq!(
            serde_json::to_value(serde_json::from_value::<GroupBy>(wire.clone()).unwrap()).unwrap(),
            wire
        );
    }
    let q:QueryRequest=serde_json::from_value(json!({"queries":[{"rank_by":["id","asc"],"limit":{"total":10,"per":{"attributes":["color"],"limit":2}}},{"aggregate_by":{"c":["Count"]}}],"consistency":{"level":"eventual","max_staleness_seconds":1},"vector_encoding":"base64"})).unwrap();
    assert_eq!(q.queries.unwrap().len(), 2);
    let err = ApiError::malformed("bad");
    assert_eq!(err.body(), json!({"status":"error","error":"bad"}));
    assert!(validate_namespace("../x").is_err());
    let _: Value = serde_json::to_value(WriteResponse::default()).unwrap();
}

#[test]
fn inferred_dense_and_sparse_vectors_obey_dimension_limits() {
    for size in [10752, 10753] {
        let value = serde_json::json!(vec![1.0; size]);
        let inferred = AttributeType::infer("vector", &value).unwrap();
        let mut schema = AttributeSchema::new(inferred);
        assert_eq!(
            schema
                .normalize("vector", DistanceMetric::CosineDistance)
                .is_ok(),
            size == 10752
        );
    }
    for size in [4096, 4097] {
        let value = serde_json::Value::Object(
            (0..size)
                .map(|i| (i.to_string(), serde_json::json!(1.0)))
                .collect(),
        );
        assert_eq!(decode_sparse(&value).is_ok(), size == 4096);
        assert_eq!(
            RankBy::parse(serde_json::json!(["s", "SparseKNN", value])).is_ok(),
            size == 4096
        );
    }
}
