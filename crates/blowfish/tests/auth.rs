use axum::{Router, body::Body, http::Request};
use bf_auth::KeyStore;
use bf_engine::Engine;
use bf_storage::{Storage, StorageConfig};
use blowfish::{ServerOptions, metrics_router, router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;

const SUPER: &str = "test-super-key";
async fn app() -> Router {
    let storage = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let keys = KeyStore::from_storage(storage.clone(), Some(SUPER), Duration::from_secs(5))
        .await
        .unwrap();
    router(
        Engine::new(storage),
        keys,
        ServerOptions {
            auth_enabled: true,
            storage_url: "s3://user:password@bucket/prefix?token=private#secret".into(),
        },
    )
}
async fn raw(
    app: &Router,
    token: Option<&str>,
    method: &str,
    path: &str,
    value: Value,
) -> (u16, String, String) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(value.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let content_type = response.headers()["content-type"]
        .to_str()
        .unwrap()
        .to_owned();
    let body = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    (status, content_type, body)
}
async fn call(
    app: &Router,
    token: Option<&str>,
    method: &str,
    path: &str,
    value: Value,
) -> (u16, Value) {
    let (status, content_type, text) = raw(app, token, method, path, value).await;
    assert!(content_type.starts_with("application/json"), "{text}");
    (status, serde_json::from_str(&text).unwrap())
}
async fn create(app: &Router, roles: &[&str], prefixes: Value) -> Value {
    let (status, value) = call(
        app,
        Some(SUPER),
        "POST",
        "/_blowfish/v1/keys",
        json!({"name":roles.join("-"),"roles":roles,"namespace_prefixes":prefixes}),
    )
    .await;
    assert_eq!(status, 201, "{value}");
    value
}
async fn seed(app: &Router, ns: &str) {
    assert_eq!(call(app, Some(SUPER), "POST", &format!("/v2/namespaces/{ns}"), json!({"upsert_rows":[{"id":1,"value":1,"vector":[1.0,0.0]}],"distance_metric":"cosine_distance"})).await.0, 200);
}
#[tokio::test]
async fn every_role_over_real_routes() {
    let app = app().await;
    for role in ["admin", "write", "read", "list", "metrics", "keys"] {
        seed(&app, "allowed-test").await;
        let key = create(&app, &[role], json!(["allowed-"])).await;
        let token = key["token"].as_str();
        let routes = [
            ("GET", "/", Value::Null, true),
            (
                "POST",
                "/v2/namespaces/allowed-test",
                json!({"upsert_rows":[{"id":2,"value":2,"vector":[0.0,1.0]}]}),
                matches!(role, "admin" | "write"),
            ),
            (
                "POST",
                "/v2/namespaces/allowed-test/query",
                json!({"rank_by":["id","asc"],"top_k":10}),
                matches!(role, "admin" | "write" | "read"),
            ),
            (
                "GET",
                "/v1/namespaces/allowed-test/metadata",
                Value::Null,
                matches!(role, "admin" | "write" | "read"),
            ),
            (
                "GET",
                "/v2/namespaces/allowed-test/metadata",
                Value::Null,
                matches!(role, "admin" | "write" | "read"),
            ),
            (
                "GET",
                "/v1/namespaces/allowed-test/schema",
                Value::Null,
                matches!(role, "admin" | "write" | "read"),
            ),
            (
                "POST",
                "/v1/namespaces/allowed-test/schema",
                json!({"value":{"type":"int"}}),
                matches!(role, "admin" | "write"),
            ),
            (
                "GET",
                "/v1/namespaces/allowed-test/hint_cache_warm",
                Value::Null,
                matches!(role, "admin" | "write" | "read"),
            ),
            (
                "POST",
                "/v1/namespaces/allowed-test/_debug/recall",
                json!({"rank_by":["vector","ANN",[1.0,0.0]],"top_k":1}),
                matches!(role, "admin" | "write" | "read"),
            ),
            (
                "POST",
                "/v2/namespaces/allowed-test/explain_query",
                json!({"rank_by":["id","asc"],"top_k":1}),
                matches!(role, "admin" | "write" | "read"),
            ),
            (
                "GET",
                "/v1/namespaces",
                Value::Null,
                matches!(role, "admin" | "list"),
            ),
            ("GET", "/metrics", Value::Null, role == "metrics"),
            (
                "GET",
                "/_blowfish/v1/status",
                Value::Null,
                matches!(role, "metrics" | "keys"),
            ),
            ("GET", "/_blowfish/v1/keys", Value::Null, role == "keys"),
            (
                "DELETE",
                "/v2/namespaces/allowed-test",
                Value::Null,
                role == "admin",
            ),
        ];
        for (method, path, body, allowed) in routes {
            let (status, _, text) = raw(&app, token, method, path, body).await;
            if allowed {
                assert!(
                    (200..300).contains(&status),
                    "{role} {method} {path}: {status} {text}"
                );
            } else {
                assert_eq!(status, 403, "{role} {method} {path}: {text}");
                assert_eq!(
                    serde_json::from_str::<Value>(&text).unwrap()["status"],
                    "error"
                );
            }
        }
        assert_eq!(
            call(
                &app,
                token,
                "GET",
                "/v2/namespaces/outside/metadata",
                Value::Null
            )
            .await
            .0,
            403
        );
        assert_eq!(
            call(
                &app,
                token,
                "GET",
                "/v2/namespaces/%6futside/metadata",
                Value::Null
            )
            .await
            .0,
            403
        );
    }
}
#[tokio::test]
async fn scoped_listing_paginates_without_hidden_names() {
    let app = app().await;
    for ns in ["a-secret", "visible-a", "visible-b", "x-secret"] {
        seed(&app, ns).await;
    }
    let key = create(&app, &["list"], json!(["visible-"])).await;
    let token = key["token"].as_str();
    let (status, page) = call(
        &app,
        token,
        "GET",
        "/v1/namespaces?page_size=1",
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(page["namespaces"], json!([{"id":"visible-a"}]));
    let cursor = page["next_cursor"].as_str().unwrap();
    let (_, page) = call(
        &app,
        token,
        "GET",
        &format!("/v1/namespaces?page_size=1&cursor={cursor}"),
        Value::Null,
    )
    .await;
    assert_eq!(page["namespaces"], json!([{"id":"visible-b"}]));
    assert!(page["next_cursor"].is_null());
    let none = create(&app, &["list"], json!([])).await;
    assert_eq!(
        call(
            &app,
            none["token"].as_str(),
            "GET",
            "/v1/namespaces",
            Value::Null
        )
        .await
        .1["namespaces"],
        json!([])
    );
    assert_eq!(
        call(
            &app,
            token,
            "GET",
            "/v1/namespaces?page_size=0",
            Value::Null
        )
        .await
        .0,
        400
    );
}
#[tokio::test]
async fn administration_revocation_expiry_and_status() {
    let app = app().await;
    for path in [
        "/",
        "/metrics",
        "/_blowfish/v1/status",
        "/v1/namespaces",
        "/_blowfish/v1/keys",
    ] {
        for token in [None, Some("bad")] {
            assert_eq!(call(&app, token, "GET", path, Value::Null).await.0, 401);
        }
    }
    let key = create(&app, &["read"], Value::Null).await;
    let id = key["key"]["id"].as_str().unwrap();
    let path = format!("/_blowfish/v1/keys/{id}");
    let (status, metadata) = call(&app, Some(SUPER), "GET", &path, Value::Null).await;
    assert_eq!(status, 200);
    assert_eq!(metadata["id"], id);
    assert!(metadata.get("token").is_none());
    assert!(metadata.get("secret_hash").is_none());
    let manager = create(&app, &["keys", "read"], json!(["allowed-"])).await;
    for spec in [
        json!({"name":"escalate","roles":["admin"],"namespace_prefixes":["allowed-"]}),
        json!({"name":"escape","roles":["read"]}),
    ] {
        assert_eq!(
            call(
                &app,
                manager["token"].as_str(),
                "POST",
                "/_blowfish/v1/keys",
                spec
            )
            .await
            .0,
            403
        );
    }
    assert_eq!(
        call(
            &app,
            Some(SUPER),
            "POST",
            "/_blowfish/v1/keys",
            json!({"name":"expired","roles":["read"],"expires_at":"2000-01-01T00:00:00Z"})
        )
        .await
        .0,
        400
    );
    seed(&app, "cached").await;
    let (_, status) = call(
        &app,
        Some(SUPER),
        "GET",
        "/_blowfish/v1/status",
        Value::Null,
    )
    .await;
    assert_eq!(status["storage_url"], "s3://bucket/prefix");
    assert_eq!(status["storage_mode"], "multi_writer");
    assert_eq!(status["namespaces_cached"], json!(["cached"]));
    assert!(status["uptime_seconds"].is_number());
    assert_eq!(
        call(&app, Some(SUPER), "DELETE", &path, Value::Null)
            .await
            .0,
        200
    );
    assert_eq!(
        call(
            &app,
            key["token"].as_str(),
            "GET",
            "/v1/namespaces/cached/schema",
            Value::Null
        )
        .await
        .0,
        401
    );
    assert_eq!(
        call(&app, Some(SUPER), "GET", &path, Value::Null).await.0,
        404
    );
    assert_eq!(
        call(&app, Some(SUPER), "DELETE", &path, Value::Null)
            .await
            .0,
        404
    );
}
#[tokio::test]
async fn copy_requires_source_scope_and_metrics_expose_facade() {
    let app = app().await;
    seed(&app, "private").await;
    let key = create(&app, &["write"], json!(["public-"])).await;
    for source in [json!("private"), json!({"source_namespace":"private"})] {
        assert_eq!(
            call(
                &app,
                key["token"].as_str(),
                "POST",
                "/v2/namespaces/public-copy",
                json!({"copy_from_namespace":source})
            )
            .await
            .0,
            403
        );
    }
    let (_, before) = call(
        &app,
        Some(SUPER),
        "POST",
        "/v2/namespaces/private/query",
        json!({"rank_by":["vector","ANN",[1.0,0.0]],"top_k":1}),
    )
    .await;
    assert_eq!(before["rows"][0]["id"], 1);
    let metrics_key = create(&app, &["metrics"], Value::Null).await;
    let (status, content_type, metrics) = raw(
        &app,
        metrics_key["token"].as_str(),
        "GET",
        "/metrics",
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert!(content_type.starts_with("text/plain"));
    for name in [
        "blowfish_http_requests_total",
        "blowfish_http_request_duration_seconds",
        "blowfish_http_requests_in_flight",
        "blowfish_write_rows_total",
        "blowfish_query_requests_total",
        "blowfish_query_rows_scanned_total",
        "blowfish_wal_commit_duration_seconds",
        "blowfish_wal_batch_requests",
        "blowfish_wal_bytes_written_total",
    ] {
        assert!(metrics.contains(name), "missing {name}");
    }
    assert!(metrics.contains("route=\"/v2/namespaces/{ns}/query\""));
    assert!(metrics.contains("key_name=\"super\""));
    assert!(metrics.contains("kind=\"ann\""));
    assert!(!metrics.contains(SUPER));
    assert_eq!(
        raw(&metrics_router(), None, "GET", "/metrics", Value::Null)
            .await
            .0,
        200
    );
}
#[test]
fn require_auth_fails_before_opening_storage() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_blowfish"))
        .args(["serve", "--require-auth", "--storage-url", "memory://"])
        .env_remove("BLOWFISH_ADMIN_KEY")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--require-auth needs"));
}

#[test]
fn key_cli_help_validates_global_arguments() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_blowfish"))
        .args(["keys", "--help"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("--json"));
}

#[tokio::test]
async fn extras_routes_enforce_roles_and_copy_source_scope() {
    let app = app().await;
    seed(&app, "allowed-extras").await;
    seed(&app, "hidden-source").await;
    let read = create(&app, &["read"], json!(["allowed-"])).await;
    let writer = create(&app, &["write"], json!(["allowed-"])).await;
    for (method, path, value) in [
        (
            "PATCH",
            "/v1/namespaces/allowed-extras/metadata",
            json!({"read_only":true}),
        ),
        (
            "POST",
            "/v1/namespaces/allowed-extras",
            json!({"ids":[1],"vectors":[null]}),
        ),
        (
            "POST",
            "/v2/namespaces/allowed-copy/async",
            json!({"copy_from_namespace":"allowed-extras"}),
        ),
    ] {
        assert_eq!(
            call(&app, read["token"].as_str(), method, path, value)
                .await
                .0,
            403
        );
    }
    for path in [
        "/v2/namespaces/allowed-copy/async",
        "/v2/namespaces/allowed-copy",
        "/v1/namespaces/allowed-copy",
    ] {
        assert_eq!(
            call(
                &app,
                writer["token"].as_str(),
                "POST",
                path,
                json!({"copy_from_namespace":"hidden-source"})
            )
            .await
            .0,
            403
        );
    }
    assert_eq!(
        call(
            &app,
            read["token"].as_str(),
            "GET",
            "/v1/namespaces/allowed-extras",
            Value::Null
        )
        .await
        .0,
        200
    );
    assert_eq!(
        call(
            &app,
            read["token"].as_str(),
            "POST",
            "/v1/namespaces/allowed-extras/query",
            json!({"rank_by":["id","asc"]})
        )
        .await
        .0,
        200
    );
    assert_eq!(
        call(
            &app,
            read["token"].as_str(),
            "GET",
            "/v1/namespaces/hidden-source/operations/invalid",
            Value::Null
        )
        .await
        .0,
        403
    );
}
#[tokio::test]
async fn branch_requires_source_scope() {
    let app = app().await;
    seed(&app, "private").await;
    let scoped = create(&app, &["write"], json!(["public-"])).await;
    for source in [json!("private"), json!({"source_namespace":"private"})] {
        assert_eq!(
            call(
                &app,
                scoped["token"].as_str(),
                "POST",
                "/v2/namespaces/public-branch",
                json!({"branch_from_namespace":source})
            )
            .await
            .0,
            403
        );
    }
    // Nothing was created by the rejected branches.
    let (status, _) = call(
        &app,
        Some(SUPER),
        "GET",
        "/v1/namespaces/public-branch/schema",
        Value::Null,
    )
    .await;
    assert_eq!(status, 404);
    // A key that can read the source may branch it.
    let broad = create(&app, &["write", "read"], Value::Null).await;
    let (status, body) = call(
        &app,
        broad["token"].as_str(),
        "POST",
        "/v2/namespaces/public-branch",
        json!({"branch_from_namespace":"private"}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (_, rows) = call(
        &app,
        Some(SUPER),
        "POST",
        "/v2/namespaces/public-branch/query",
        json!({"rank_by":["vector","ANN",[1.0,0.0]],"top_k":1}),
    )
    .await;
    assert_eq!(rows["rows"][0]["id"], 1);
}
