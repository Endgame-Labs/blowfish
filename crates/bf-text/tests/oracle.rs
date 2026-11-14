use bf_text::{
    CorpusStats, FtsParams, Query, Token, matches_all_tokens, matches_token_sequence,
    score_document, tokenize, tokenize_array,
};
use serde_json::{Value, json};

fn analyze(value: &Value, params: &FtsParams) -> Vec<Token> {
    if let Some(s) = value.as_str() {
        tokenize(s, params)
    } else {
        tokenize_array(
            &value
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap())
                .collect::<Vec<_>>(),
            params,
        )
    }
}

#[test]
fn expanded_defaults_match_oracle() {
    let expected = json!({"k1":1.2,"b":0.75,"k3":8.0,"ascii_folding":false,"case_sensitive":false,"language":"english","stemming":false,"remove_stopwords":false,"tokenizer":"word_v4","max_token_length":39});
    let params: FtsParams = serde_json::from_value(true.into()).unwrap();
    let wire: Value = serde_json::from_str(&serde_json::to_string(&params).unwrap()).unwrap();
    assert_eq!(wire, expected);
    assert_eq!(params, FtsParams::default());
    for invalid in [
        json!(false),
        json!({"k1":0}),
        json!({"b":2}),
        json!({"max_token_length":0}),
        json!({"max_token_length":255}),
        json!({"case_sensitive":true,"stemming":true}),
        json!({"language":"arabic","remove_stopwords":true}),
    ] {
        assert!(serde_json::from_value::<FtsParams>(invalid).is_err());
    }
}

#[test]
fn bm25_scores_match_recorded_service() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/oracle.json")).unwrap();
    for case in fixture["scores"].as_array().unwrap() {
        let params: FtsParams =
            serde_json::from_value(case.get("params").cloned().unwrap_or_else(|| json!({})))
                .unwrap();
        let docs: Vec<_> = case["documents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| analyze(s, &params))
            .collect();
        let stats = CorpusStats::from_documents(&docs);
        let q = Query::new(
            analyze(&case["query"], &params),
            case["last_as_prefix"].as_bool().unwrap(),
        );
        let mut actual: Vec<_> = docs
            .iter()
            .enumerate()
            .map(|(i, d)| (i + 1, score_document(d, &q, &stats, &params)))
            .filter(|(_, s)| *s > 0.0)
            .collect();
        actual.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        if let Some(gap) = case.get("known_gap") {
            assert_eq!(
                actual.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                gap["local_ids"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as usize)
                    .collect::<Vec<_>>()
            );
            continue;
        }
        for (actual, expected) in actual.iter().zip(case["rows"].as_array().unwrap()) {
            assert_eq!(actual.0 as u64, expected["id"].as_u64().unwrap());
            let target = expected["$dist"].as_f64().unwrap() as f32;
            assert!(
                (actual.1 - target).abs() < target * 0.01,
                "{case}: {actual:?}"
            );
        }
        assert_eq!(actual.len(), case["rows"].as_array().unwrap().len());
    }
}

#[test]
fn tokenizer_filter_parity() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/oracle.json")).unwrap();
    let mut mismatches = Vec::new();
    for case in fixture["filters"].as_array().unwrap() {
        let params: FtsParams = serde_json::from_value(case["params"].clone()).unwrap();
        let query = analyze(&case["query"], &params);
        let docs = case["documents"].as_array().unwrap();
        // Oracle fixture has three rows, with missing values for short input lists.
        let actual: Vec<_> = (0..3)
            .filter(|&i| {
                let doc = docs.get(i).map(|s| analyze(s, &params)).unwrap_or_default();
                let q = Query::new(query.clone(), false);
                if case["operator"] == "ContainsTokenSequence" {
                    matches_token_sequence(&doc, &q)
                } else {
                    matches_all_tokens(&doc, &q, params.tokenizer)
                }
            })
            .map(|i| i + 1)
            .collect();
        let expected: Vec<_> = case["ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as usize)
            .collect();
        if let Some(gap) = case.get("known_gap") {
            assert_eq!(
                actual,
                gap["local_ids"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as usize)
                    .collect::<Vec<_>>()
            );
        } else if actual != expected {
            mismatches.push(format!(
                "params={}, query={}, expected={expected:?}, actual={actual:?}",
                case["params"], case["query"]
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn cjk_and_pretokenized_metadata_roundtrip() {
    for (tokenizer, case_sensitive, max_length) in [
        ("smartcn", false, json!(39)),
        ("kuromoji", false, json!(39)),
        ("nori", false, json!(39)),
        ("pre_tokenized_array", true, Value::Null),
    ] {
        let expected = json!({"k1":1.2,"b":0.75,"k3":8.0,"ascii_folding":false,"case_sensitive":case_sensitive,"language":null,"stemming":false,"remove_stopwords":false,"tokenizer":tokenizer,"max_token_length":max_length});
        let params: FtsParams = serde_json::from_value(json!({"tokenizer":tokenizer})).unwrap();
        let wire: Value = serde_json::from_str(&serde_json::to_string(&params).unwrap()).unwrap();
        assert_eq!(wire, expected);
        assert_eq!(serde_json::from_value::<FtsParams>(wire).unwrap(), params);
    }
    let p: FtsParams = serde_json::from_value(json!({"tokenizer":"pre_tokenized_array"})).unwrap();
    let token = "é".repeat(300);
    assert_eq!(
        tokenize_array(&[token.as_str(), "Foo Bar"], &p)
            .iter()
            .map(|t| t.text.as_str())
            .collect::<Vec<_>>(),
        vec![token.as_str(), "Foo Bar"]
    );
}

#[test]
fn token_length_is_checked_before_normalization_expands_it() {
    let params = FtsParams {
        max_token_length: 1,
        ascii_folding: true,
        ..FtsParams::default()
    };
    // Unicode lowercase and ASCII folding can expand a single original character.
    assert_eq!(
        tokenize("Æ İ", &params)
            .iter()
            .map(|t| t.text.as_str())
            .collect::<Vec<_>>(),
        vec!["ae", "i̇"]
    );
    let params = FtsParams {
        max_token_length: 2,
        ..FtsParams::default()
    };
    assert_eq!(
        tokenize("éé ééé ok", &params)
            .iter()
            .map(|t| (t.text.as_str(), t.position))
            .collect::<Vec<_>>(),
        vec![("éé", 0), ("ok", 2)]
    );
}
