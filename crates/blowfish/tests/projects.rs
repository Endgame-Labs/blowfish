//! Org, project, and key isolation matrix over the real HTTP routes.
use axum::{Router, body::Body, http::Request};
use bf_auth::KeyStore;
use bf_engine::Engine;
use bf_storage::{Storage, StorageConfig};
use blowfish::{ServerOptions, router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;

const BREAK_GLASS: &str = "test-break-glass";

struct Keys {
    admin: String,
    rw: String,
    read: String,
}
struct World {
    app: Router,
    /// A minted org superadmin key.
    org: String,
    a: Keys,
    b: Keys,
}
async fn call(
    app: &Router,
    token: Option<&str>,
    project: Option<&str>,
    method: &str,
    path: &str,
    value: Value,
) -> (u16, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    if let Some(project) = project {
        request = request.header("x-blowfish-project", project);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(value.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn mint(app: &Router, token: &str, path: &str, kind: &str) -> (String, String) {
    let (status, value) = call(
        app,
        Some(token),
        None,
        "POST",
        path,
        json!({"name":kind,"kind":kind}),
    )
    .await;
    assert_eq!(status, 201, "{value}");
    (
        value["token"].as_str().unwrap().to_owned(),
        value["key"]["id"].as_str().unwrap().to_owned(),
    )
}
async fn world() -> World {
    let storage = Storage::open(&StorageConfig {
        url: "memory://".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let keys = KeyStore::from_storage(storage.clone(), Some(BREAK_GLASS), Duration::from_secs(5))
        .await
        .unwrap();
    let app = router(
        Engine::new(storage),
        keys,
        ServerOptions {
            auth_enabled: true,
            storage_url: "memory://".into(),
        },
    );
    let (status, org) = call(
        &app,
        Some(BREAK_GLASS),
        None,
        "POST",
        "/_blowfish/v1/org/keys",
        json!({"name":"root"}),
    )
    .await;
    assert_eq!(status, 201, "{org}");
    let org = org["token"].as_str().unwrap().to_owned();
    let mut projects = Vec::new();
    for id in ["a", "b"] {
        let (status, project) = call(
            &app,
            Some(&org),
            None,
            "POST",
            "/_blowfish/v1/projects",
            json!({"id":id,"name":format!("Project {id}")}),
        )
        .await;
        assert_eq!(status, 201, "{project}");
        let path = format!("/_blowfish/v1/projects/{id}/keys");
        projects.push(Keys {
            admin: mint(&app, &org, &path, "admin").await.0,
            rw: mint(&app, &org, &path, "read_write").await.0,
            read: mint(&app, &org, &path, "read").await.0,
        });
    }
    let b = projects.pop().unwrap();
    let a = projects.pop().unwrap();
    World { app, org, a, b }
}
fn rows(value: &Value) -> Vec<i64> {
    value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect()
}
async fn write_row(app: &Router, token: &str, ns: &str, id: i64) -> u16 {
    call(
        app,
        Some(token),
        None,
        "POST",
        &format!("/v2/namespaces/{ns}"),
        json!({"upsert_rows":[{"id":id,"v":id}]}),
    )
    .await
    .0
}
async fn query(app: &Router, token: &str, project: Option<&str>, ns: &str) -> (u16, Value) {
    call(
        app,
        Some(token),
        project,
        "POST",
        &format!("/v2/namespaces/{ns}/query"),
        json!({"rank_by":["id","asc"],"top_k":10}),
    )
    .await
}

#[tokio::test]
async fn namespace_routes_by_kind_own_and_other_project() {
    let w = world().await;
    let app = &w.app;
    assert_eq!(write_row(app, &w.a.rw, "docs", 1).await, 200);
    assert_eq!(write_row(app, &w.a.admin, "docs", 2).await, 200);
    // Roles inside the own project.
    assert_eq!(write_row(app, &w.a.read, "docs", 3).await, 403);
    assert_eq!(query(app, &w.a.read, None, "docs").await.0, 200);
    assert_eq!(query(app, &w.a.rw, None, "docs").await.0, 200);
    for (method, path) in [
        ("GET", "/v1/namespaces/docs/metadata"),
        ("GET", "/v1/namespaces/docs/schema"),
    ] {
        for token in [&w.a.read, &w.a.rw, &w.a.admin] {
            assert_eq!(
                call(app, Some(token), None, method, path, Value::Null)
                    .await
                    .0,
                200
            );
        }
    }
    let delete = |token: String| async move {
        call(
            app,
            Some(&token),
            None,
            "DELETE",
            "/v2/namespaces/docs",
            Value::Null,
        )
        .await
        .0
    };
    assert_eq!(delete(w.a.read.clone()).await, 403);
    assert_eq!(delete(w.a.rw.clone()).await, 403);
    // Listing is admin-only, and filtered to the key's own project.
    assert_eq!(write_row(app, &w.b.rw, "only-b", 1).await, 200);
    for token in [&w.a.read, &w.a.rw] {
        assert_eq!(
            call(app, Some(token), None, "GET", "/v1/namespaces", Value::Null)
                .await
                .0,
            403
        );
    }
    let (status, list) = call(
        app,
        Some(&w.a.admin),
        None,
        "GET",
        "/v1/namespaces",
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(list["namespaces"], json!([{"id":"docs"}]));
    let (_, list) = call(
        app,
        Some(&w.b.admin),
        None,
        "GET",
        "/v1/namespaces",
        Value::Null,
    )
    .await;
    assert_eq!(list["namespaces"], json!([{"id":"only-b"}]));
    // Another project's namespace, or its project header, is a 404 for every kind.
    for token in [&w.b.read, &w.b.rw, &w.b.admin] {
        assert_eq!(query(app, token, Some("a"), "docs").await.0, 404);
        assert_eq!(
            call(
                app,
                Some(token),
                Some("a"),
                "GET",
                "/v1/namespaces",
                Value::Null
            )
            .await
            .0,
            404
        );
        // Without the header, the key sees only its own project, where `docs` is absent.
        assert_eq!(query(app, token, None, "docs").await.0, 404);
        // Naming its own project in the header is fine.
        assert_eq!(query(app, token, Some("b"), "only-b").await.0, 200);
    }
    assert_eq!(write_row(app, &w.b.rw, "docs", 9).await, 200);
    assert_eq!(
        call(
            app,
            Some(&w.b.rw),
            Some("a"),
            "POST",
            "/v2/namespaces/docs",
            json!({"upsert_rows":[{"id":5}]})
        )
        .await
        .0,
        404
    );
    // Same-named namespaces are unrelated.
    assert_eq!(rows(&query(app, &w.a.rw, None, "docs").await.1), vec![1, 2]);
    assert_eq!(rows(&query(app, &w.b.rw, None, "docs").await.1), vec![9]);
    // Unauthenticated, and nonsense tokens.
    assert_eq!(
        call(app, None, None, "GET", "/v1/namespaces", Value::Null)
            .await
            .0,
        401
    );
    assert_eq!(
        call(
            app,
            Some("bf_nope"),
            None,
            "GET",
            "/_blowfish/v1/whoami",
            Value::Null
        )
        .await
        .0,
        401
    );
    // The org key picks a project per request; no header means `default`.
    assert_eq!(
        rows(&query(app, &w.org, Some("a"), "docs").await.1),
        vec![1, 2]
    );
    assert_eq!(
        rows(&query(app, &w.org, Some("b"), "docs").await.1),
        vec![9]
    );
    assert_eq!(query(app, &w.org, None, "docs").await.0, 404);
    assert_eq!(query(app, &w.org, Some("nope"), "docs").await.0, 404);
    assert_eq!(query(app, BREAK_GLASS, Some("a"), "docs").await.0, 200);
    // Admin can delete in its own project, and only there.
    assert_eq!(delete(w.a.admin.clone()).await, 200);
    assert_eq!(query(app, &w.org, Some("a"), "docs").await.0, 404);
    assert_eq!(rows(&query(app, &w.b.rw, None, "docs").await.1), vec![9]);
}

#[tokio::test]
async fn default_project_keeps_legacy_keys_and_data() {
    let w = world().await;
    let app = &w.app;
    // A legacy role-and-prefix key, as before.
    let (status, legacy) = call(
        app,
        Some(BREAK_GLASS),
        None,
        "POST",
        "/_blowfish/v1/keys",
        json!({"name":"legacy","roles":["write","read"],"namespace_prefixes":["old-"]}),
    )
    .await;
    assert_eq!(status, 201, "{legacy}");
    let legacy = legacy["token"].as_str().unwrap();
    assert_eq!(write_row(app, legacy, "old-data", 1).await, 200);
    assert_eq!(write_row(app, legacy, "elsewhere", 1).await, 403);
    // The break-glass key with no header acts on `default`, where the legacy data lives.
    assert_eq!(
        rows(&query(app, BREAK_GLASS, None, "old-data").await.1),
        vec![1]
    );
    assert_eq!(
        rows(&query(app, &w.org, Some("default"), "old-data").await.1),
        vec![1]
    );
    // No project sees it.
    for (token, project) in [(&w.a.admin, None), (&w.org, Some("a")), (&w.b.rw, None)] {
        assert_eq!(query(app, token, project, "old-data").await.0, 404);
    }
    // A legacy key is pinned to `default`.
    assert_eq!(query(app, legacy, Some("a"), "old-data").await.0, 404);
    assert_eq!(query(app, legacy, Some("default"), "old-data").await.0, 200);
    let (_, me) = call(
        app,
        Some(legacy),
        None,
        "GET",
        "/_blowfish/v1/whoami",
        Value::Null,
    )
    .await;
    assert_eq!(
        (me["kind"].as_str(), me["project"].as_str()),
        (Some("legacy"), Some("default"))
    );
    // Legacy key routes still work, and show only legacy keys.
    let (status, list) = call(
        app,
        Some(BREAK_GLASS),
        None,
        "GET",
        "/_blowfish/v1/keys",
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(list["keys"].as_array().unwrap().len(), 1);
    // Project keys in `default` work alongside them.
    let (token, _) = mint(
        app,
        &w.org,
        "/_blowfish/v1/projects/default/keys",
        "read_write",
    )
    .await;
    assert_eq!(rows(&query(app, &token, None, "old-data").await.1), vec![1]);
    // The legacy keys role can't use the new admin routes.
    let (status, keys_role) = call(
        app,
        Some(BREAK_GLASS),
        None,
        "POST",
        "/_blowfish/v1/keys",
        json!({"name":"k","roles":["keys"]}),
    )
    .await;
    assert_eq!(status, 201);
    let keys_role = keys_role["token"].as_str().unwrap();
    for (method, path) in [
        ("GET", "/_blowfish/v1/projects"),
        ("GET", "/_blowfish/v1/projects/default/keys"),
        ("GET", "/_blowfish/v1/org/keys"),
    ] {
        assert_eq!(
            call(app, Some(keys_role), None, method, path, Value::Null)
                .await
                .0,
            403
        );
    }
}

#[tokio::test]
async fn admin_routes_by_kind() {
    let w = world().await;
    let app = &w.app;
    let routes = |project: &str| {
        vec![
            ("GET", "/_blowfish/v1/projects".to_owned()),
            ("GET", format!("/_blowfish/v1/projects/{project}")),
            ("GET", format!("/_blowfish/v1/projects/{project}/keys")),
            ("GET", "/_blowfish/v1/org/keys".to_owned()),
        ]
    };
    // Expected status per route for each kind of key acting on project `a`.
    let cases: Vec<(&str, &str, [u16; 4])> = vec![
        ("org", &w.org, [200, 200, 200, 200]),
        ("a admin", &w.a.admin, [403, 403, 200, 403]),
        ("a rw", &w.a.rw, [403, 403, 403, 403]),
        ("a read", &w.a.read, [403, 403, 403, 403]),
        // Another project's keys can't tell whether `a` exists.
        ("b admin", &w.b.admin, [403, 403, 404, 403]),
        ("b rw", &w.b.rw, [403, 403, 404, 403]),
        ("b read", &w.b.read, [403, 403, 404, 403]),
    ];
    for (who, token, expected) in cases {
        for ((method, path), want) in routes("a").into_iter().zip(expected) {
            let got = call(app, Some(token), None, method, &path, Value::Null)
                .await
                .0;
            assert_eq!(got, want, "{who} {method} {path}");
        }
        // Everyone authenticated can ask who they are.
        assert_eq!(
            call(
                app,
                Some(token),
                None,
                "GET",
                "/_blowfish/v1/whoami",
                Value::Null
            )
            .await
            .0,
            200
        );
    }
    for (method, path) in routes("a") {
        assert_eq!(
            call(app, None, None, method, &path, Value::Null).await.0,
            401,
            "{path}"
        );
    }
    // Non-superadmins can't create or change projects or org keys.
    for token in [&w.a.admin, &w.a.rw, &w.a.read] {
        for (method, path, body) in [
            (
                "POST",
                "/_blowfish/v1/projects",
                json!({"id":"c","name":"C"}),
            ),
            ("PATCH", "/_blowfish/v1/projects/a", json!({"name":"x"})),
            ("DELETE", "/_blowfish/v1/projects/a", Value::Null),
            ("POST", "/_blowfish/v1/org/keys", json!({"name":"x"})),
        ] {
            assert_eq!(
                call(app, Some(token), None, method, path, body).await.0,
                403,
                "{path}"
            );
        }
    }
    // whoami.
    let (_, me) = call(
        app,
        Some(&w.a.rw),
        None,
        "GET",
        "/_blowfish/v1/whoami",
        Value::Null,
    )
    .await;
    assert_eq!(
        (me["kind"].as_str(), me["project"].as_str()),
        (Some("read_write"), Some("a"))
    );
    assert!(me["key_id"].as_str().unwrap().len() == 32);
    let (_, me) = call(
        app,
        Some(&w.org),
        None,
        "GET",
        "/_blowfish/v1/whoami",
        Value::Null,
    )
    .await;
    assert_eq!(me["kind"], "superadmin");
    assert!(me["project"].is_null());
    let (_, me) = call(
        app,
        Some(BREAK_GLASS),
        None,
        "GET",
        "/_blowfish/v1/whoami",
        Value::Null,
    )
    .await;
    assert_eq!(me["key_id"], "super");
}

#[tokio::test]
async fn project_admin_cannot_escalate_and_revocation_is_immediate() {
    let w = world().await;
    let app = &w.app;
    let keys = "/_blowfish/v1/projects/a/keys";
    // Can mint rw and read, not admin or superadmin.
    let (rw_token, rw_id) = mint(app, &w.a.admin, keys, "read_write").await;
    let (read_token, _) = mint(app, &w.a.admin, keys, "read").await;
    for kind in ["admin", "superadmin"] {
        let (status, _) = call(
            app,
            Some(&w.a.admin),
            None,
            "POST",
            keys,
            json!({"name":"x","kind":kind}),
        )
        .await;
        assert_eq!(status, if kind == "admin" { 403 } else { 400 }, "{kind}");
    }
    assert_eq!(
        call(
            app,
            Some(&w.a.admin),
            None,
            "POST",
            keys,
            json!({"name":"x"})
        )
        .await
        .0,
        400
    );
    // It can't create keys in another project.
    assert_eq!(
        call(
            app,
            Some(&w.a.admin),
            None,
            "POST",
            "/_blowfish/v1/projects/b/keys",
            json!({"name":"x","kind":"read"})
        )
        .await
        .0,
        404
    );
    // Admin keys are invisible to it, and can't be revoked by it.
    let (_, listed) = call(app, Some(&w.org), None, "GET", keys, Value::Null).await;
    let admin_id = listed["keys"]
        .as_array()
        .unwrap()
        .iter()
        .find(|k| k["kind"] == "admin")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (_, seen) = call(app, Some(&w.a.admin), None, "GET", keys, Value::Null).await;
    let seen = seen["keys"].as_array().unwrap();
    assert!(seen.iter().all(|k| k["kind"] != "admin"));
    assert_eq!(seen.len(), 4);
    assert!(
        seen.iter()
            .all(|k| k.get("secret_hash").is_none() && k.get("token").is_none())
    );
    let admin_path = format!("{keys}/{admin_id}");
    assert_eq!(
        call(app, Some(&w.a.admin), None, "GET", &admin_path, Value::Null)
            .await
            .0,
        404
    );
    assert_eq!(
        call(
            app,
            Some(&w.a.admin),
            None,
            "DELETE",
            &admin_path,
            Value::Null
        )
        .await
        .0,
        404
    );
    assert_eq!(
        query(app, &w.a.admin, None, "x").await.0,
        404,
        "admin key still works"
    );
    // Another project's admin can't touch this project's keys.
    let rw_path = format!("{keys}/{rw_id}");
    assert_eq!(
        call(app, Some(&w.b.admin), None, "DELETE", &rw_path, Value::Null)
            .await
            .0,
        404
    );
    // Revocation applies at once.
    assert_eq!(write_row(app, &rw_token, "docs", 1).await, 200);
    assert_eq!(
        call(app, Some(&w.a.admin), None, "DELETE", &rw_path, Value::Null)
            .await
            .0,
        200
    );
    assert_eq!(write_row(app, &rw_token, "docs", 2).await, 401);
    assert_eq!(
        call(app, Some(&w.a.admin), None, "DELETE", &rw_path, Value::Null)
            .await
            .0,
        404
    );
    assert_eq!(query(app, &read_token, None, "docs").await.0, 200);
    // The org key can revoke an admin key.
    assert_eq!(
        call(app, Some(&w.org), None, "DELETE", &admin_path, Value::Null)
            .await
            .0,
        200
    );
    assert_eq!(query(app, &w.a.admin, None, "docs").await.0, 401);
}

#[tokio::test]
async fn org_keys_projects_and_deletion() {
    let w = world().await;
    let app = &w.app;
    // Org keys mint and revoke org keys, including each other; the break-glass key survives.
    let (second, second_id) = mint(app, &w.org, "/_blowfish/v1/org/keys", "superadmin").await;
    let (status, listed) = call(
        app,
        Some(&second),
        None,
        "GET",
        "/_blowfish/v1/org/keys",
        Value::Null,
    )
    .await;
    assert_eq!((status, listed["keys"].as_array().unwrap().len()), (200, 2));
    let org_id = listed["keys"][0]["id"].as_str().unwrap().to_owned();
    let org_path = |id: &str| format!("/_blowfish/v1/org/keys/{id}");
    assert_eq!(
        call(
            app,
            Some(&second),
            None,
            "DELETE",
            &org_path(&org_id),
            Value::Null
        )
        .await
        .0,
        200
    );
    assert_eq!(
        call(
            app,
            Some(&w.org),
            None,
            "GET",
            "/_blowfish/v1/whoami",
            Value::Null
        )
        .await
        .0,
        401
    );
    assert_eq!(
        call(
            app,
            Some(BREAK_GLASS),
            None,
            "DELETE",
            &org_path(&second_id),
            Value::Null
        )
        .await
        .0,
        200
    );
    assert_eq!(
        call(
            app,
            Some(&second),
            None,
            "GET",
            "/_blowfish/v1/whoami",
            Value::Null
        )
        .await
        .0,
        401
    );
    // Project validation, conflicts, rename.
    let app_org = BREAK_GLASS;
    for id in ["", "Upper", "has space", "default", "a", &"x".repeat(41)] {
        let (status, _) = call(
            app,
            Some(app_org),
            None,
            "POST",
            "/_blowfish/v1/projects",
            json!({"id":id,"name":"n"}),
        )
        .await;
        assert_eq!(
            status,
            if matches!(id, "default" | "a") {
                409
            } else {
                400
            },
            "{id:?}"
        );
    }
    let (status, renamed) = call(
        app,
        Some(app_org),
        None,
        "PATCH",
        "/_blowfish/v1/projects/a",
        json!({"name":"Alpha"}),
    )
    .await;
    assert_eq!((status, renamed["name"].as_str()), (200, Some("Alpha")));
    let (_, all) = call(
        app,
        Some(app_org),
        None,
        "GET",
        "/_blowfish/v1/projects",
        Value::Null,
    )
    .await;
    let ids: Vec<_> = all["projects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["default", "a", "b"]);
    assert_eq!(
        call(
            app,
            Some(app_org),
            None,
            "PATCH",
            "/_blowfish/v1/projects/default",
            json!({"name":"x"})
        )
        .await
        .0,
        400
    );
    assert_eq!(
        call(
            app,
            Some(app_org),
            None,
            "DELETE",
            "/_blowfish/v1/projects/default",
            Value::Null
        )
        .await
        .0,
        400
    );
    assert_eq!(
        call(
            app,
            Some(app_org),
            None,
            "GET",
            "/_blowfish/v1/projects/zzz",
            Value::Null
        )
        .await
        .0,
        404
    );
    // Deleting a project with namespaces needs force, then revokes its keys.
    assert_eq!(write_row(app, &w.a.rw, "docs", 1).await, 200);
    let (status, refusal) = call(
        app,
        Some(app_org),
        None,
        "DELETE",
        "/_blowfish/v1/projects/a",
        Value::Null,
    )
    .await;
    assert_eq!(status, 409, "{refusal}");
    assert_eq!(query(app, &w.a.rw, None, "docs").await.0, 200);
    let (status, _) = call(
        app,
        Some(app_org),
        None,
        "DELETE",
        "/_blowfish/v1/projects/a?force=true",
        Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    for token in [&w.a.admin, &w.a.rw, &w.a.read] {
        assert_eq!(
            call(
                app,
                Some(token),
                None,
                "GET",
                "/_blowfish/v1/whoami",
                Value::Null
            )
            .await
            .0,
            401
        );
    }
    assert_eq!(query(app, app_org, Some("a"), "docs").await.0, 404);
    // The id is reusable, with nothing carried over.
    let (status, _) = call(
        app,
        Some(app_org),
        None,
        "POST",
        "/_blowfish/v1/projects",
        json!({"id":"a","name":"A again"}),
    )
    .await;
    assert_eq!(status, 201);
    assert_eq!(query(app, app_org, Some("a"), "docs").await.0, 404);
    // Project b is untouched.
    assert_eq!(
        call(
            app,
            Some(&w.b.read),
            None,
            "GET",
            "/_blowfish/v1/whoami",
            Value::Null
        )
        .await
        .0,
        200
    );
    // Expiry: a past date is refused, and a child can't outlive its parent.
    let (status, _) = call(
        app,
        Some(app_org),
        None,
        "POST",
        "/_blowfish/v1/projects/b/keys",
        json!({"name":"x","kind":"read","expires_at":"2001-01-01T00:00:00Z"}),
    )
    .await;
    assert_eq!(status, 400);
    let soon = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    let (status, short) = call(
        app,
        Some(app_org),
        None,
        "POST",
        "/_blowfish/v1/projects/b/keys",
        json!({"name":"x","kind":"admin","expires_at":soon}),
    )
    .await;
    assert_eq!(status, 201);
    let short = short["token"].as_str().unwrap();
    let (status, _) = call(
        app,
        Some(short),
        None,
        "POST",
        "/_blowfish/v1/projects/b/keys",
        json!({"name":"x","kind":"read"}),
    )
    .await;
    assert_eq!(status, 403);
    // Attribution labels are honored from superadmins only.
    let (_, labeled) = call(
        app,
        Some(app_org),
        None,
        "POST",
        "/_blowfish/v1/projects/b/keys",
        json!({"name":"x","kind":"read","created_by":"webapp:alice"}),
    )
    .await;
    assert_eq!(labeled["key"]["created_by"], "webapp:alice");
    let (_, forged) = call(
        app,
        Some(&w.b.admin),
        None,
        "POST",
        "/_blowfish/v1/projects/b/keys",
        json!({"name":"x","kind":"read","created_by":"webapp:alice"}),
    )
    .await;
    assert_ne!(forged["key"]["created_by"], "webapp:alice");
}
