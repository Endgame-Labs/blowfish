use super::*;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Default)]
struct Fake {
    calls: Arc<AtomicUsize>,
    inputs: Arc<Mutex<Vec<Vec<String>>>>,
    fail_first: usize,
    status: u16,
}

async fn embeddings(State(f): State<Fake>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let n = f.calls.fetch_add(1, Ordering::SeqCst);
    if f.status != 0 && n < f.fail_first {
        return (
            StatusCode::from_u16(f.status).unwrap(),
            Json(json!({"error":"secret detail"})),
        );
    }
    let inputs: Vec<String> = b["input"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    f.inputs.lock().unwrap().push(inputs.clone());
    // Native 4 dimensions; the vector depends on the text length so truncation is visible.
    let data: Vec<_> = inputs
        .iter()
        .enumerate()
        .rev()
        .map(|(i, t)| json!({"index": i, "embedding": [t.len() as f64, 1.0, 2.0, 2.0]}))
        .collect();
    (
        StatusCode::OK,
        Json(json!({"data": data, "usage": {"prompt_tokens": inputs.len() * 3}})),
    )
}
async fn rerank(Json(b): Json<Value>) -> Json<Value> {
    let q = b["query"].as_str().unwrap();
    let results: Vec<_> = b["documents"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, d)| json!({"index": i, "relevance_score": if d.as_str().unwrap().contains(q) {0.9} else {0.1}}))
        .collect();
    Json(json!({"results": results}))
}

async fn serve(f: Fake) -> String {
    let app = Router::new()
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/rerank", post(rerank))
        .with_state(f);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}
fn models(url: &str, extra: Value) -> Arc<Models> {
    let mut e = json!({"provider":"openai_embeddings","base_url":url,"dims":4,"allowed_dims":[2],
        "retries":2,"batch_size":2,"query_prefix":"Q:","document_prefix":"D:","max_input_tokens":2});
    e.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let config: ModelsConfig = serde_json::from_value(json!({
        "emb": e,
        "rr": {"provider":"rerank","base_url":url,"query_prefix":"","batch_size":2},
    }))
    .unwrap();
    Models::new(config).unwrap()
}

#[tokio::test]
async fn embeds_in_batches_with_prefixes_truncation_and_order() {
    let f = Fake::default();
    let m = models(&serve(f.clone()).await, json!({}));
    let texts: Vec<String> = vec!["a".into(), "bb".into(), "x".repeat(100)];
    let out = m
        .embed("emb", &texts, InputKind::Document, None)
        .await
        .unwrap();
    assert_eq!(out.vectors.len(), 3);
    assert_eq!(out.tokens, 2 * 3 + 3);
    // The provider answers in reverse order; indexes restore it. Length reveals prefix and truncation.
    assert_eq!(out.vectors[0][0], 3.0); // "D:a"
    assert_eq!(out.vectors[1][0], 4.0);
    assert_eq!(out.vectors[2][0], 2.0 + 6.0); // "D:" plus 2 tokens * 3 characters
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn matryoshka_truncates_and_renormalizes() {
    let m = models(&serve(Fake::default()).await, json!({}));
    let out = m
        .embed("emb", &["a".into()], InputKind::Document, Some(2))
        .await
        .unwrap();
    let v = &out.vectors[0];
    assert_eq!(v.len(), 2);
    assert!((v.iter().map(|x| x * x).sum::<f32>() - 1.0).abs() < 1e-5);
    let err = m
        .embed("emb", &["a".into()], InputKind::Document, Some(3))
        .await
        .unwrap_err();
    assert_eq!(err.status, 400);
    assert!(err.message.contains("2, 4"), "{}", err.message);
}

#[tokio::test]
async fn query_embeddings_are_cached_and_documents_are_not() {
    let f = Fake::default();
    let m = models(&serve(f.clone()).await, json!({}));
    let q = ["hello".to_string()];
    let first = m.embed("emb", &q, InputKind::Query, None).await.unwrap();
    let second = m.embed("emb", &q, InputKind::Query, None).await.unwrap();
    assert_eq!(first.vectors, second.vectors);
    assert_eq!((first.tokens, second.tokens), (3, 0));
    m.embed("emb", &q, InputKind::Document, None).await.unwrap();
    m.embed("emb", &q, InputKind::Document, None).await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 3);
    assert_eq!(f.inputs.lock().unwrap()[0], vec!["Q:hello"]);
}

#[tokio::test]
async fn retries_then_maps_errors() {
    let f = Fake {
        fail_first: 2,
        status: 429,
        ..Default::default()
    };
    let m = models(&serve(f.clone()).await, json!({}));
    m.embed("emb", &["a".into()], InputKind::Document, None)
        .await
        .unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 3);

    let f = Fake {
        fail_first: 99,
        status: 429,
        ..Default::default()
    };
    let m = models(&serve(f.clone()).await, json!({}));
    let e = m
        .embed("emb", &["a".into()], InputKind::Document, None)
        .await
        .unwrap_err();
    assert_eq!(e.status, 429);
    assert_eq!(f.calls.load(Ordering::SeqCst), 3);

    let f = Fake {
        fail_first: 99,
        status: 500,
        ..Default::default()
    };
    let m = models(&serve(f).await, json!({}));
    let e = m
        .embed("emb", &["a".into()], InputKind::Document, None)
        .await
        .unwrap_err();
    assert_eq!(e.status, 502);
    assert!(!e.message.contains("secret"));

    let f = Fake {
        fail_first: 99,
        status: 400,
        ..Default::default()
    };
    let m = models(&serve(f.clone()).await, json!({}));
    let e = m
        .embed("emb", &["a".into()], InputKind::Document, None)
        .await
        .unwrap_err();
    assert_eq!(e.status, 502);
    assert_eq!(
        f.calls.load(Ordering::SeqCst),
        1,
        "client errors are not retried"
    );

    let m = models("http://127.0.0.1:1", json!({"retries":0}));
    let e = m
        .embed("emb", &["a".into()], InputKind::Document, None)
        .await
        .unwrap_err();
    assert_eq!(e.status, 503);
}

#[tokio::test]
async fn unknown_and_mismatched_models_are_400() {
    let m = models("http://127.0.0.1:1", json!({}));
    assert_eq!(
        m.embed("nope", &[], InputKind::Query, None)
            .await
            .unwrap_err()
            .status,
        400
    );
    assert_eq!(
        m.embed("rr", &[], InputKind::Query, None)
            .await
            .unwrap_err()
            .status,
        400
    );
    assert_eq!(
        m.rerank("emb", "q", &["d".into()])
            .await
            .unwrap_err()
            .status,
        400
    );
}

#[tokio::test]
async fn reranks_across_batches() {
    let m = models(&serve(Fake::default()).await, json!({}));
    let docs: Vec<String> = ["no", "has fox", "none", "fox", "x"]
        .map(String::from)
        .to_vec();
    let r = m.rerank("rr", "fox", &docs).await.unwrap();
    assert_eq!(r.scores, vec![0.1, 0.9, 0.1, 0.9, 0.1]);
}

#[test]
fn configuration_validation_and_env_merge() {
    let bad: Result<ModelsConfig, _> = serde_json::from_value(
        json!({"m":{"provider":"openai_embeddings","base_url":"x","bogus":1}}),
    );
    assert!(bad.is_err());
    let no_dims: ModelsConfig =
        serde_json::from_value(json!({"m":{"provider":"openai_embeddings","base_url":"http://x"}}))
            .unwrap();
    assert!(Models::new(no_dims).unwrap_err().contains("dims"));
    let mut base = ModelsConfig::new();
    apply_env(
        &mut base,
        r#"{"m":{"provider":"rerank","base_url":"http://x"}}"#,
    )
    .unwrap();
    assert!(base.contains_key("m"));
    assert!(apply_env(&mut base, "[").is_err());
    let missing: ModelsConfig = serde_json::from_value(json!({"m":{"provider":"rerank","base_url":"http://x","api_key_env":"BF_MODELS_TEST_UNSET_KEY"}})).unwrap();
    assert!(Models::new(missing).unwrap_err().contains("not set"));
}
