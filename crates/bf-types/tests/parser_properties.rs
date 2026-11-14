//! Bounded adversarial JSON generation for every public request parser.
use bf_types::*;
use proptest::prelude::*;
use serde_json::{Value, json};

fn values() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        ".{0,64}".prop_map(Value::String)
    ]
    .prop_recursive(8, 256, 16, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..16).prop_map(Value::Array),
            prop::collection::btree_map(".{0,16}", inner, 0..16)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]
    #[test]
    fn arbitrary_json_never_panics(value in values(), bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        let _ = Filter::parse(value.clone());
        let _ = RankBy::parse(value.clone());
        let _ = serde_json::from_value::<WriteRequest>(value.clone());
        let _ = serde_json::from_value::<QueryRequest>(value.clone());
        let _ = serde_json::from_value::<SchemaUpdate>(value.clone());
        let _ = serde_json::from_value::<QueryRequest>(json!({"filters":value.clone(), "rank_by":value.clone()}));
        let _ = serde_json::from_value::<WriteRequest>(json!({"delete_by_filter":value.clone()}));
        let _ = serde_json::from_slice::<QueryRequest>(&bytes);
        let _ = serde_json::from_slice::<WriteRequest>(&bytes);
    }
}
#[test]
fn deeply_nested_and_wide_expressions_fail_before_recursive_parsing() {
    let mut filter = json!(["id", "Eq", 1]);
    let mut rank = json!(1);
    for _ in 0..80 {
        filter = json!(["Not", filter]);
        rank = json!(["Product", 1, rank]);
    }
    assert_eq!(Filter::parse(filter).unwrap_err().status, 422);
    assert_eq!(RankBy::parse(rank).unwrap_err().status, 422);
    assert!(Filter::parse(json!(["And", vec![json!(["id", "Eq", 1]); 17000]])).is_err());
    let deep = format!("{}0{}", "[".repeat(256), "]".repeat(256));
    assert!(serde_json::from_str::<QueryRequest>(&format!("{{\"filters\":{deep}}}")).is_err());
}
