use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use bf_auth::KeyStore;
use bf_engine::Engine;
use bf_storage::{Storage, StorageConfig};
use blowfish::{ServerOptions, router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;

async fn store() -> Storage {
    Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap()
}
async fn app(store: Storage) -> Router {
    let auth = KeyStore::from_storage(store.clone(), None, Duration::from_secs(1))
        .await
        .unwrap();
    router(Engine::new(store), auth, ServerOptions::default())
}
async fn call(
    app: &Router,
    method: &str,
    path: &str,
    body: Value,
    prefer: bool,
) -> (StatusCode, HeaderMap, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if prefer {
        request = request.header("prefer", "respond-async");
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        headers,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}
async fn seed(app: &Router, ns: &str) {
    assert_eq!(call(app,"POST",&format!("/v2/namespaces/{ns}"),json!({"upsert_rows":[{"id":1,"vector":[1.,0.],"label":"a"},{"id":2,"vector":[0.,1.]}],"distance_metric":"cosine_distance"}),true).await.0,StatusCode::OK);
}
async fn finish(app: &Router, location: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (status, _, value) = call(app, "GET", location, Value::Null, false).await;
            assert_eq!(status, StatusCode::OK);
            assert!(value["start_time"].is_string());
            if value["status"] == "finished" {
                assert!(value["finish_time"].is_string());
                return value;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn async_copy_is_shared_and_errors_are_pollable() {
    let storage = store().await;
    let a = app(storage.clone()).await;
    let b = app(storage.clone()).await;
    seed(&a, "source").await;
    for (ns, endpoint, prefer) in [("explicit", "/async", false), ("preferred", "", true)] {
        let (status, headers, body) = call(
            &a,
            "POST",
            &format!("/v2/namespaces/{ns}{endpoint}"),
            json!({"copy_from_namespace":{"source_namespace":"source"}}),
            prefer,
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(headers.contains_key("preference-applied"), prefer);
        let location = headers["location"].to_str().unwrap();
        assert!(location.starts_with(&format!("/v1/namespaces/{ns}/operations/")));
        assert!(body["token"].is_string());
        let result = finish(&b, location).await;
        assert_eq!(result["result"]["success"]["status"], "OK");
        let wrong_namespace = location.replace(&format!("/{ns}/"), "/other/");
        assert_eq!(
            call(&b, "GET", &wrong_namespace, Value::Null, false)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    let (_, headers, _) = call(
        &a,
        "POST",
        "/v2/namespaces/explicit/async",
        json!({"copy_from_namespace":"source"}),
        false,
    )
    .await;
    let failed = finish(&b, headers["location"].to_str().unwrap()).await;
    assert_eq!(failed["result"]["error"]["status_code"], 400);
    assert_eq!(failed["result"]["error"]["detail"]["status"], "error");
    let token = uuid::Uuid::new_v4();
    storage.create(&format!("_system/operations/{token}/finished.json"),serde_json::to_vec(&json!({"namespace":"expired","state":{"status":"finished","start_time":"2020-01-01T00:00:00Z","finish_time":"2020-01-01T00:00:00Z","result":{"success":{}}}})).unwrap()).await.unwrap();
    assert_eq!(
        call(
            &b,
            "GET",
            &format!("/v1/namespaces/expired/operations/{token}"),
            Value::Null,
            false
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &a,
            "POST",
            "/v2/namespaces/no-copy/async",
            json!({"deletes":[1]}),
            false
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
}
#[tokio::test]
async fn metadata_persists_enforces_writes_and_resets_on_recreation() {
    let storage = store().await;
    let a = app(storage.clone()).await;
    let b = app(storage.clone()).await;
    seed(&a, "settings").await;
    let patch = "/v1/namespaces/settings/metadata";
    let (status, _, value) = call(
        &a,
        "PATCH",
        patch,
        json!({"read_only":true,"pinning":true}),
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["read_only"], true);
    assert_eq!(value["pinning"], json!({"replicas":1}));
    for (path, body) in [
        ("/v2/namespaces/settings", json!({"deletes":[1]})),
        (
            "/v1/namespaces/settings/schema",
            json!({"label":{"type":"string"}}),
        ),
        (
            "/v1/namespaces/settings",
            json!({"ids":[1],"vectors":[null]}),
        ),
    ] {
        let (status, _, value) = call(&b, "POST", path, body, false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            value,
            json!({"status":"error","error":"💔 Writes not permitted. This namespace is read-only."})
        );
    }
    assert_eq!(
        call(
            &b,
            "GET",
            "/v2/namespaces/settings/metadata",
            Value::Null,
            false
        )
        .await
        .2["read_only"],
        true
    );
    assert_eq!(
        call(
            &b,
            "POST",
            "/v2/namespaces/copied",
            json!({"copy_from_namespace":"settings"}),
            false
        )
        .await
        .0,
        StatusCode::OK
    );
    let copied = call(
        &a,
        "GET",
        "/v2/namespaces/copied/metadata",
        Value::Null,
        false,
    )
    .await
    .2;
    assert_eq!(copied["read_only"], true);
    assert!(copied.get("pinning").is_none());
    let (_, _, value) = call(&b, "PATCH", patch, json!({"pinning":{"replicas":2}}), false).await;
    assert_eq!(value["pinning"]["replicas"], 2);
    assert_eq!(value["read_only"], true);
    let (_, _, value) = call(
        &a,
        "PATCH",
        patch,
        json!({"read_only":false,"pinning":null}),
        false,
    )
    .await;
    assert!(value.get("read_only").is_none());
    assert!(value.get("pinning").is_none());
    assert_eq!(
        call(&a, "PATCH", patch, json!({"pinning":{"replicas":0}}), false)
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    call(&a, "PATCH", patch, json!({"read_only":true}), false).await;
    assert_eq!(
        call(&b, "DELETE", "/v2/namespaces/settings", Value::Null, false)
            .await
            .0,
        StatusCode::OK
    );
    seed(&a, "settings").await;
    assert!(
        call(&b, "GET", patch, Value::Null, false)
            .await
            .2
            .get("read_only")
            .is_none()
    );
    let (left, right) = tokio::join!(
        call(&a, "PATCH", patch, json!({"pinning":{"replicas":3}}), false),
        call(&b, "PATCH", patch, json!({"read_only":true}), false)
    );
    assert_eq!(left.0, StatusCode::OK);
    assert_eq!(right.0, StatusCode::OK);
    let value = call(&a, "GET", patch, Value::Null, false).await.2;
    assert_eq!(value["read_only"], true);
    assert_eq!(value["pinning"]["replicas"], 3);
}
#[tokio::test]
async fn legacy_translates_columns_rows_filters_encoding_and_export_pages() {
    let a = app(store().await).await;
    let path = "/v1/namespaces/legacy";
    let (status,_,_) = call(&a,"POST",path,json!({"ids":[1,2],"vectors":["AACAPwAAAAA=",[0.,1.]],"attributes":{"label":["a",null]},"distance_metric":"cosine_distance"}),false).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = call(
        &a,
        "POST",
        path,
        json!({"upserts":[{"id":3,"vector":[0.5,0.5],"attributes":{"label":"c"}}]}),
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status,_,rows) = call(&a,"POST",&format!("{path}/query"),json!({"vector":[1.,0.],"distance_metric":null,"include_vectors":true,"include_attributes":true,"filters":{"label":[["Eq","a"]]},"vector_encoding_format":"base64","rank_by":null,"top_k":null,"consistency":null}),false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        rows,
        json!([{"id":1,"dist":0.,"vector":"AACAPwAAAAA=","attributes":{"label":"a"}}])
    );
    let (_, _, rows) = call(
        &a,
        "POST",
        &format!("{path}/query"),
        json!({"rank_by":["id","asc"],"include_attributes":true}),
        false,
    )
    .await;
    assert_eq!(rows[1], json!({"id":2,"attributes":{}}));
    assert!(rows[0].get("vector").is_none());
    let (status, headers, _) = call(&a, "HEAD", path, Value::Null, false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-turbopuffer-dimensions"], "2");
    assert_eq!(headers["x-turbopuffer-approx-num-vectors"], "3");
    let (_, _, page) = call(
        &a,
        "GET",
        &format!("{path}?page_size=2"),
        Value::Null,
        false,
    )
    .await;
    assert_eq!(page["ids"], json!([1, 2]));
    assert_eq!(page["attributes"]["label"], json!(["a", null]));
    let cursor = page["next_cursor"].as_str().unwrap();
    let (_, _, last) = call(
        &a,
        "GET",
        &format!("{path}?cursor={cursor}&page_size=2"),
        Value::Null,
        false,
    )
    .await;
    assert_eq!(last["ids"], json!([3]));
    assert!(last["next_cursor"].is_null());
    assert_eq!(
        call(
            &a,
            "GET",
            &format!("{path}?cursor=invalid"),
            Value::Null,
            false
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&a, "POST", path, json!({"ids":[2],"vectors":[null]}), false)
            .await
            .2["rows_deleted"],
        1
    );
    assert_eq!(
        call(
            &a,
            "POST",
            path,
            json!({"delete_by_filter":{"label":[["Eq","c"]]}}),
            false
        )
        .await
        .2["rows_deleted"],
        1
    );
    assert_eq!(
        call(
            &a,
            "POST",
            path,
            json!({"ids":[1,2],"vectors":[[1.,0.]]}),
            false
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &a,
            "POST",
            "/v1/namespaces/clone",
            json!({"copy_from_namespace":"legacy"}),
            false
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&a, "DELETE", path, Value::Null, false).await.2,
        json!({"status":"ok"})
    );
}
