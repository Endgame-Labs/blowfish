use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use bf_auth::KeyStore;
use bf_engine::Engine;
use bf_storage::{Storage, StorageConfig};
use blowfish::{ServerOptions, router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    time::Duration,
};
use tower::ServiceExt;

async fn app(key: Option<&str>) -> Router {
    let storage = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let auth = KeyStore::from_storage(storage.clone(), key, Duration::from_secs(5))
        .await
        .unwrap();
    router(
        Engine::new(storage),
        auth,
        ServerOptions {
            auth_enabled: key.is_some(),
            storage_url: "memory://".into(),
        },
    )
}
async fn call(app: &Router, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
async fn endpoints_sdk_overloads_and_json_errors() {
    let app = app(None).await;
    assert_eq!(
        call(&app, "GET", "/", Value::Null).await,
        (StatusCode::OK, json!({"status":"🐡"}))
    );
    assert_eq!(
        call(&app, "GET", "/v1/namespaces/missing/schema", Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status,write)=call(&app,"POST","/v2/namespaces/test?stainless_overload=write",json!({"schema":{"text":{"type":"string","full_text_search":true}},"distance_metric":"cosine_distance","upsert_rows":[{"id":1,"vector":"AACAPwAAAAA=","text":"hello fish"},{"id":2,"vector":[0,1],"text":"hello ocean"}]})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(write["rows_affected"], 2);
    let (status, q) = call(
        &app,
        "POST",
        "/v2/namespaces/test/query",
        json!({"rank_by":["vector","ANN","AACAPwAAAAA="],"top_k":2,"include_attributes":true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(q["rows"][0]["id"], 1);
    assert!(q["billing"].is_object());
    assert!(q["performance"]["query_execution_ms"].is_number());
    let (_, q) = call(
        &app,
        "POST",
        "/v2/namespaces/test/query?stainless_overload=multiQuery",
        json!({"queries":[{"rank_by":["id","asc"],"top_k":1},{"aggregate_by":{"c":["Count"]}}]}),
    )
    .await;
    assert_eq!(q["results"].as_array().unwrap().len(), 2);
    for path in [
        "/v1/namespaces/test/metadata",
        "/v2/namespaces/test/metadata",
    ] {
        let (status, metadata) = call(&app, "GET", path, Value::Null).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(metadata["approx_row_count"], 2);
        assert_eq!(metadata["encryption"]["mode"], "default");
    }
    let (status, schema) = call(
        &app,
        "POST",
        "/v1/namespaces/test/schema",
        json!({"n":"int"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(schema["n"]["type"], "int");
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/namespaces/test/hint_cache_warm",
            Value::Null
        )
        .await
        .0,
        StatusCode::ACCEPTED
    );
    let (_, recall) = call(
        &app,
        "POST",
        "/v1/namespaces/test/_debug/recall",
        json!({"num":1,"top_k":1,"include_ground_truth":true}),
    )
    .await;
    assert_eq!(recall["avg_recall"], 1.0);
    assert_eq!(recall["ground_truth"].as_array().unwrap().len(), 1);
    let (_, explain) = call(
        &app,
        "POST",
        "/v2/namespaces/test/explain_query",
        json!({"rank_by":["id","asc"],"limit":1}),
    )
    .await;
    assert!(
        explain["plan_text"]
            .as_str()
            .unwrap()
            .contains("exhaustive")
    );
    let (_, copy) = call(
        &app,
        "POST",
        "/v2/namespaces/copy?stainless_overload=copyFrom",
        json!({"copy_from_namespace":{"source_namespace":"test"}}),
    )
    .await;
    assert_eq!(copy["status"], "OK");
    let (_, list) = call(
        &app,
        "GET",
        "/v1/namespaces?prefix=te&page_size=1&unknown=true",
        Value::Null,
    )
    .await;
    assert_eq!(list["namespaces"], json!([{"id":"test"}]));
    assert_eq!(
        call(
            &app,
            "POST",
            "/v2/namespaces/test/query",
            json!({"rank_by":["id","asc"],"top_k":2,"filters":["text","Bogus",1]})
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v2/namespaces/test/query",
            json!({"rank_by":["vector","ANN",[1]],"top_k":2})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, error) = call(&app, "GET", "/unknown", Value::Null).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error["status"], "error");
    assert_eq!(
        call(&app, "PATCH", "/", json!({})).await.0,
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        call(&app, "DELETE", "/v2/namespaces/test", Value::Null).await,
        (StatusCode::OK, json!({"status":"OK"}))
    );
    assert_eq!(
        call(&app, "GET", "/v1/namespaces/test/schema", Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}
#[tokio::test]
async fn auth_and_invalid_json() {
    let app = app(Some("test-admin")).await;
    let (status, err) = call(&app, "GET", "/", Value::Null).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(err["status"], "error");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .header("authorization", "Bearer test-admin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/namespaces/a")
                .header("authorization", "Bearer test-admin")
                .header("content-type", "application/json")
                .body(Body::from("{broken"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
}
#[tokio::test]
async fn gzip_requests_and_responses() {
    let app = app(None).await;
    let text = "hello fish ".repeat(1000);
    let body = json!({"upsert_rows":[{"id":1,"text":text}]}).to_string();
    let mut gz = flate2::write::GzEncoder::new(vec![], flate2::Compression::default());
    gz.write_all(body.as_bytes()).unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/namespaces/gzip")
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .body(Body::from(gz.finish().unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/namespaces/gzip/query")
                .header("content-type", "application/json")
                .header("accept-encoding", "gzip")
                .body(Body::from(
                    json!({"rank_by":["id","asc"],"limit":1,"include_attributes":true}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-encoding"], "gzip");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let mut decoded = String::new();
    flate2::read::GzDecoder::new(bytes.as_ref())
        .read_to_string(&mut decoded)
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&decoded).unwrap()["rows"][0]["text"],
        text
    );
}
#[tokio::test]
async fn permanent_internal_errors_disable_retries() {
    let store = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    store
        .put("ns/bad/wal/00000000000000000001.wal", "corrupt")
        .await
        .unwrap();
    let keys = KeyStore::from_storage(store.clone(), None, Duration::from_secs(5))
        .await
        .unwrap();
    let app = router(Engine::new(store), keys, ServerOptions::default());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/bad/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(response.headers()["x-should-retry"], "false");
}
