//! Native embedding and model reranking against an in-process fake provider.
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use bf_engine::{Engine, EngineConfig};
use bf_storage::{Storage, StorageConfig};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Fake {
    embedded: Arc<Mutex<Vec<String>>>,
    reranked: Arc<Mutex<Vec<Vec<String>>>>,
}
const DIMS: usize = 8;
/// A bag-of-words embedding: each word adds one to a dimension chosen by its bytes.
fn embed_text(text: &str) -> Vec<f64> {
    let mut v = vec![0.0; DIMS];
    let text = text
        .trim_start_matches("doc: ")
        .trim_start_matches("query: ");
    for w in text.to_lowercase().split(|c: char| !c.is_alphanumeric()) {
        if !w.is_empty() {
            v[w.bytes().map(usize::from).sum::<usize>() % DIMS] += 1.0;
        }
    }
    v
}
async fn embeddings(State(f): State<Fake>, Json(b): Json<Value>) -> (StatusCode, Json<Value>) {
    let inputs: Vec<String> = b["input"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    if inputs.iter().any(|i| i.contains("RATELIMIT")) {
        return (StatusCode::TOO_MANY_REQUESTS, Json(json!({})));
    }
    if inputs.iter().any(|i| i.contains("BOOM")) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({})));
    }
    f.embedded.lock().unwrap().extend(inputs.clone());
    let data: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(i, t)| json!({"index": i, "embedding": embed_text(t)}))
        .collect();
    (
        StatusCode::OK,
        Json(json!({"data": data, "usage": {"prompt_tokens": 5 * inputs.len()}})),
    )
}
async fn rerank(State(f): State<Fake>, Json(b): Json<Value>) -> Json<Value> {
    let words: Vec<String> = b["query"]
        .as_str()
        .unwrap()
        .split_whitespace()
        .map(str::to_lowercase)
        .collect();
    let docs: Vec<String> = b["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d.as_str().unwrap().to_string())
        .collect();
    f.reranked.lock().unwrap().push(docs.clone());
    let results: Vec<_> = docs
        .iter()
        .enumerate()
        .map(|(i, d)| {
            let d = d.to_lowercase();
            let hits = words.iter().filter(|w| d.contains(w.as_str())).count();
            json!({"index": i, "relevance_score": hits as f64 / words.len() as f64})
        })
        .collect();
    Json(json!({"results": results}))
}
async fn engine(indexing: bool) -> (Engine, Fake) {
    let fake = Fake::default();
    let app = Router::new()
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/rerank", post(rerank))
        .with_state(fake.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let models = bf_models::Models::new(
        serde_json::from_value(json!({
            "fake-embed": {"provider":"openai_embeddings","base_url":url,"dims":DIMS,"allowed_dims":[4],"retries":0,
                "query_prefix":"query: ","document_prefix":"doc: "},
            "fake-rerank": {"provider":"rerank","base_url":url,"retries":0},
        }))
        .unwrap(),
    )
    .unwrap();
    let e = Engine::with_config(
        Storage::open(&StorageConfig {
            url: "memory://".into(),
            ..Default::default()
        })
        .await
        .unwrap(),
        EngineConfig {
            indexing,
            models: Some(models),
            ..Default::default()
        },
    );
    (e, fake)
}
async fn write(e: &Engine, body: Value) -> Result<Value, bf_types::ApiError> {
    Ok(serde_json::to_value(e.write("n", serde_json::from_value(body).unwrap()).await?).unwrap())
}
async fn query(e: &Engine, body: Value) -> Result<Value, bf_types::ApiError> {
    Ok(serde_json::to_value(e.query("n", serde_json::from_value(body).unwrap()).await?).unwrap())
}
fn ids(v: &Value) -> Vec<i64> {
    v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect()
}
async fn seed(e: &Engine) {
    write(e, json!({"schema":{"text":{"type":"string","embed":"fake-embed","full_text_search":true}},"upsert_rows":[
        {"id":1,"text":"brown fox"},{"id":2,"text":"lazy dog sleeps"},{"id":3,"text":"fox fox jumps"},{"id":4,"text":"cat"},
    ]})).await.unwrap();
}

#[tokio::test]
async fn write_time_embedding_and_embed_queries() {
    let (e, fake) = engine(false).await;
    let w = write(&e, json!({"schema":{"text":{"type":"string","embed":"fake-embed","full_text_search":true}},"upsert_rows":[
        {"id":1,"text":"brown fox"},{"id":2,"text":"lazy dog sleeps"},{"id":3,"text":"fox fox jumps"},{"id":4,"text":"cat"},
    ]})).await.unwrap();
    assert_eq!(w["rows_upserted"], 4);
    assert_eq!(w["performance"]["embedding_tokens"], 20);
    assert!(w["performance"]["embedding_ms"].is_u64());
    assert!(
        fake.embedded
            .lock()
            .unwrap()
            .iter()
            .all(|t| t.starts_with("doc: "))
    );
    // The generated column is named embed_<attribute>, and the schema echoes the setting.
    let meta = e.metadata("n").await.unwrap();
    assert_eq!(meta["schema"]["embed_text"]["type"], "[8]f32");
    assert_eq!(meta["schema"]["text"]["embed"], "fake-embed");
    let r = query(
        &e,
        json!({"rank_by":["text","ANN",["Embed","fox"]],"top_k":2}),
    )
    .await
    .unwrap();
    assert_eq!(ids(&r), vec![3, 1]);
    assert!(r["performance"]["embedding_tokens"].as_u64().unwrap() > 0);
    assert!(r["performance"]["embedding_ms"].is_u64());
    assert_eq!(fake.embedded.lock().unwrap().last().unwrap(), "query: fox");
    // Explicit model on the vector attribute, and kNN.
    let r = query(&e, json!({"rank_by":["embed_text","kNN",["Embed","fox",{"model":"fake-embed"}]],"top_k":2,"filters":["id","Gt",0]})).await.unwrap();
    assert_eq!(ids(&r), vec![3, 1]);
    // Embedded vectors are returned like any attribute.
    let r = query(&e, json!({"rank_by":["text","ANN",["Embed","dog"]],"top_k":1,"include_attributes":["embed_text"]})).await.unwrap();
    assert_eq!(r["rows"][0]["embed_text"].as_array().unwrap().len(), 8);
    // Embed composes inside a score expression.
    let r = query(&e, json!({"rank_by":["Sum",[["text","ANN",["Embed","fox"]],["text","ANN",["Embed","fox"]]]],"top_k":1})).await;
    assert!(r.is_err() || ids(&r.unwrap()).len() == 1);
    // A plain-vector-less, model-less Embed on a vector attribute is a 400... unless a string
    // attribute's embed setting targets it, which it does here.
    let r = query(
        &e,
        json!({"rank_by":["embed_text","ANN",["Embed","fox"]],"top_k":1}),
    )
    .await
    .unwrap();
    assert_eq!(ids(&r), vec![3]);
}

#[tokio::test]
async fn embed_queries_work_over_an_indexed_namespace() {
    let (e, _) = engine(true).await;
    seed(&e).await;
    e.index_namespace("n", true).await.unwrap();
    let r = query(
        &e,
        json!({"rank_by":["text","ANN",["Embed","lazy dog"]],"top_k":1}),
    )
    .await
    .unwrap();
    assert_eq!(ids(&r), vec![2]);
}

#[tokio::test]
async fn embed_into_an_existing_vector_attribute_with_dims() {
    let (e, _) = engine(false).await;
    write(
        &e,
        json!({"distance_metric":"cosine_distance","schema":{
        "text":{"type":"string","embed":{"model":"fake-embed","attribute":"vector","dims":4}},
        "vector":"[4]f32"},"upsert_rows":[{"id":1,"text":"dog"},{"id":2,"text":"fox"}]}),
    )
    .await
    .unwrap();
    let r = query(
        &e,
        json!({"rank_by":["text","ANN",["Embed","dog"]],"top_k":1,"include_attributes":["vector"]}),
    )
    .await
    .unwrap();
    assert_eq!(ids(&r), vec![1]);
    let v = r["rows"][0]["vector"].as_array().unwrap();
    assert_eq!(v.len(), 4);
    let norm: f64 = v
        .iter()
        .map(|x| x.as_f64().unwrap().powi(2))
        .sum::<f64>()
        .sqrt();
    assert!(
        (norm - 1.0).abs() < 1e-5,
        "truncated vectors are renormalized"
    );
    // Dimensions the model does not offer, or that do not match the column, are rejected.
    let bad = write(&e, json!({"schema":{"t2":{"type":"string","embed":{"model":"fake-embed","dims":3,"attribute":"vector"}}}})).await;
    assert_eq!(bad.unwrap_err().status, 400);
}

#[tokio::test]
async fn supplied_vectors_skip_the_provider_and_are_validated() {
    let (e, fake) = engine(false).await;
    seed(&e).await;
    let before = fake.embedded.lock().unwrap().len();
    write(&e, json!({"upsert_rows":[
        {"id":5,"text":"supplied","embed_text":[1,0,0,0,0,0,0,0]},{"id":6,"text":"computed words"}]})).await.unwrap();
    let after = fake.embedded.lock().unwrap().clone();
    assert_eq!(after.len(), before + 1);
    assert_eq!(after.last().unwrap(), "doc: computed words");
    let bad = write(
        &e,
        json!({"upsert_rows":[{"id":7,"text":"x","embed_text":[1,0]}]}),
    )
    .await;
    assert_eq!(bad.unwrap_err().status, 400);
    // Rows without the source attribute need their own vector.
    let bad = write(&e, json!({"upsert_rows":[{"id":8}]})).await;
    assert_eq!(bad.unwrap_err().status, 400);
    write(
        &e,
        json!({"upsert_rows":[{"id":8,"embed_text":[0,1,0,0,0,0,0,0]}]}),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn columnar_upserts_are_embedded() {
    let (e, _) = engine(false).await;
    seed(&e).await;
    write(
        &e,
        json!({"upsert_columns":{"id":[10,11],"text":["red fox","blue whale"]}}),
    )
    .await
    .unwrap();
    let r = query(
        &e,
        json!({"rank_by":["text","ANN",["Embed","whale"]],"top_k":1}),
    )
    .await
    .unwrap();
    assert_eq!(ids(&r), vec![11]);
}

#[tokio::test]
async fn upserts_over_256_rows_are_rejected_and_256_pass() {
    let (e, _) = engine(false).await;
    seed(&e).await;
    let rows = |n: usize, base: usize| {
        (0..n)
            .map(|i| json!({"id": base + i, "text": format!("row {i}")}))
            .collect::<Vec<_>>()
    };
    let err = write(&e, json!({"upsert_rows": rows(257, 100)}))
        .await
        .unwrap_err();
    assert_eq!(err.status, 400);
    assert!(err.message.contains("256"));
    write(&e, json!({"upsert_rows": rows(256, 1000)}))
        .await
        .unwrap();
}

#[tokio::test]
async fn rows_are_embedded_before_upsert_condition() {
    let (e, fake) = engine(false).await;
    write(&e, json!({"schema":{"text":{"type":"string","embed":"fake-embed"},"v":"int"},"upsert_rows":[{"id":1,"text":"old","v":5}]})).await.unwrap();
    let n = fake.embedded.lock().unwrap().len();
    let w = write(&e, json!({"upsert_condition":["v","Lt",{"$ref_new":"v"}],"upsert_rows":[{"id":1,"text":"new text","v":1}]})).await.unwrap();
    assert!(w.get("rows_upserted").is_none_or(|v| v == 0), "{w}");
    assert_eq!(
        fake.embedded.lock().unwrap().len(),
        n + 1,
        "embedded although the condition failed"
    );
    assert!(w["performance"]["embedding_tokens"].as_u64().unwrap() > 0);
    let w = write(&e, json!({"upsert_condition":["v","Lt",{"$ref_new":"v"}],"upsert_rows":[{"id":1,"text":"newer","v":9}]})).await.unwrap();
    assert_eq!(w["rows_upserted"], 1);
}

#[tokio::test]
async fn embed_can_be_added_and_removed_but_not_modified() {
    let (e, fake) = engine(false).await;
    write(
        &e,
        json!({"distance_metric":"cosine_distance","schema":{"vec":"[8]f32"},"upsert_rows":[
        {"id":1,"text":"plain old","vec":[1,0,0,0,0,0,0,0]}]}),
    )
    .await
    .unwrap();
    assert!(fake.embedded.lock().unwrap().is_empty());
    // Adding embed does not re-embed existing rows; later writes are embedded.
    write(
        &e,
        json!({"schema":{"text":{"type":"string","embed":{"model":"fake-embed","attribute":"vec"}}},
        "upsert_rows":[{"id":2,"text":"fox"}]}),
    )
    .await
    .unwrap();
    assert_eq!(fake.embedded.lock().unwrap().len(), 1);
    // Re-sending the same setting is fine; a different one is not.
    write(&e, json!({"schema":{"text":{"type":"string","embed":{"attribute":"vec","model":"fake-embed"}}}})).await.unwrap();
    let err = write(&e, json!({"schema":{"text":{"type":"string","embed":{"model":"fake-embed","attribute":"vec","dims":8}}}})).await.unwrap_err();
    assert_eq!(err.status, 400);
    assert!(err.message.contains("modified"));
    // Removing stops embedding, and the stored setting disappears.
    write(
        &e,
        json!({"schema":{"text":{"type":"string","embed":null}}}),
    )
    .await
    .unwrap();
    assert!(
        e.metadata("n").await.unwrap()["schema"]["text"]
            .get("embed")
            .is_none()
    );
    let err = write(&e, json!({"upsert_rows":[{"id":3,"text":"no vector"}]}))
        .await
        .unwrap_err();
    assert_eq!(err.status, 400);
    assert_eq!(fake.embedded.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn embed_limits_and_unknown_models() {
    let (e, _) = engine(false).await;
    let err = write(&e, json!({"schema":{"text":{"type":"string","embed":"nope/unknown"}},"upsert_rows":[{"id":1,"text":"x"}]})).await.unwrap_err();
    assert_eq!(err.status, 400);
    assert!(err.message.contains("nope/unknown"));
    let err = write(&e, json!({"schema":{"text":{"type":"string","embed":"fake-rerank"}},"upsert_rows":[{"id":1,"text":"x"}]})).await.unwrap_err();
    assert_eq!(err.status, 400);
    let schema: serde_json::Map<String, Value> = (0..5)
        .map(|i| {
            (
                format!("t{i}"),
                json!({"type":"string","embed":"fake-embed"}),
            )
        })
        .collect();
    let err = write(&e, json!({"schema":schema,"upsert_rows":[{"id":1}]}))
        .await
        .unwrap_err();
    assert_eq!(err.status, 400);
    assert!(
        err.message.contains("four") || err.message.contains('4'),
        "{}",
        err.message
    );
    // Embed on an attribute with no embed setting and no model.
    seed(&e).await;
    let err = query(&e, json!({"rank_by":["id","ANN",["Embed","x"]],"top_k":1}))
        .await
        .unwrap_err();
    assert_eq!(err.status, 400);
    let err = query(
        &e,
        json!({"rank_by":["text","ANN",["Embed","x",{"model":"fake-rerank"}]],"top_k":1}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, 400);
}

#[tokio::test]
async fn provider_errors_map_to_http_statuses() {
    let (e, _) = engine(false).await;
    seed(&e).await;
    let err = write(&e, json!({"upsert_rows":[{"id":9,"text":"RATELIMIT"}]}))
        .await
        .unwrap_err();
    assert_eq!(err.status, 429);
    let err = write(&e, json!({"upsert_rows":[{"id":9,"text":"BOOM"}]}))
        .await
        .unwrap_err();
    assert_eq!(err.status, 502);
    let err = query(
        &e,
        json!({"rank_by":["text","ANN",["Embed","BOOM"]],"top_k":1}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, 502);
    // Nothing was written by the failed requests.
    let r = query(&e, json!({"rank_by":["id","asc"],"top_k":100}))
        .await
        .unwrap();
    assert_eq!(ids(&r).len(), 4);
}

#[tokio::test]
async fn single_query_rerank_orders_by_score() {
    let (e, fake) = engine(false).await;
    seed(&e).await;
    write(&e, json!({"upsert_rows":[{"id":5,"text":"jumps high fox"}, {"id":6,"embed_text":[0,0,0,0,0,0,0,1]}]})).await.unwrap();
    let r = query(
        &e,
        json!({"rank_by":["text","BM25","fox jumps"],"limit":10,
        "rerank_by":{"model":"fake-rerank","attribute":"text","query":"fox jumps high"}}),
    )
    .await
    .unwrap();
    // 5 matches all three words, 3 two of them, and 1 one.
    assert_eq!(ids(&r), vec![5, 3, 1]);
    let dist: Vec<f64> = r["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["$dist"].as_f64().unwrap())
        .collect();
    assert!(dist.windows(2).all(|w| w[0] >= w[1]));
    assert!((dist[0] - 1.0).abs() < 1e-6);
    // The attribute was not requested, so it is not returned.
    assert!(r["rows"][0].get("text").is_none());
    assert!(r["performance"]["rerank_ms"].is_u64());
    assert_eq!(fake.reranked.lock().unwrap().last().unwrap().len(), 3);
    // Requested attributes are returned; rows without the attribute score zero and are not sent.
    let r = query(
        &e,
        json!({"rank_by":["id","asc"],"limit":10,"include_attributes":["text"],
        "rerank_by":{"model":"fake-rerank","attribute":"text","query":"dog"}}),
    )
    .await
    .unwrap();
    assert_eq!(ids(&r)[0], 2);
    assert_eq!(*ids(&r).last().unwrap(), 6);
    assert_eq!(r["rows"].as_array().unwrap().last().unwrap()["$dist"], 0.0);
    assert_eq!(fake.reranked.lock().unwrap().last().unwrap().len(), 5);
}

#[tokio::test]
async fn multi_query_rerank_dedupes_and_applies_limit_and_offset() {
    let (e, fake) = engine(false).await;
    seed(&e).await;
    let body = |limit: Value, offset: u64| {
        json!({"queries":[
            {"rank_by":["text","BM25","fox"],"limit":10,"include_attributes":["text"]},
            {"rank_by":["text","ANN",["Embed","fox"]],"limit":3,"include_attributes":["text"]},
        ],"rerank_by":{"model":"fake-rerank","attribute":"text","query":"fox"},"limit":limit,"offset":offset})
    };
    let r = query(&e, body(json!(10), 0)).await.unwrap();
    let rows = r["results"][0].clone();
    assert_eq!(r["results"].as_array().unwrap().len(), 1);
    let found = ids(&rows);
    let mut unique = found.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(found.len(), unique.len(), "deduplicated by id");
    let sent = fake.reranked.lock().unwrap().last().unwrap().len();
    assert_eq!(sent, found.len(), "each document is scored once");
    assert_eq!(
        &found[..2]
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        &[1, 3].into_iter().collect()
    );
    assert!(rows["rows"][0].get("text").is_some());
    let page = query(&e, body(json!(1), 1)).await.unwrap();
    assert_eq!(ids(&page["results"][0]), vec![found[1]]);
    let err = query(
        &e,
        json!({"queries":[{"rank_by":["text","BM25","fox"],"limit":3}],"limit":1,
        "rerank_by":{"model":"nope","attribute":"text","query":"fox"}}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, 400);
}

#[tokio::test]
async fn rerank_errors_and_restrictions() {
    let (e, _) = engine(false).await;
    seed(&e).await;
    let mk = |extra: Value| {
        let mut q = json!({"rank_by":["text","BM25","fox"],"limit":5,
            "rerank_by":{"model":"fake-rerank","attribute":"text","query":"fox"}});
        q.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        q
    };
    for bad in [
        mk(json!({"rerank_by":{"model":"unknown","attribute":"text","query":"x"}})),
        mk(json!({"rerank_by":{"model":"fake-embed","attribute":"text","query":"x"}})),
        mk(json!({"rerank_by":{"model":"fake-rerank","attribute":"text"}})),
        mk(json!({"aggregate_by":{"n":["Count"]}})),
    ] {
        assert_eq!(query(&e, bad).await.unwrap_err().status, 400);
    }
}
