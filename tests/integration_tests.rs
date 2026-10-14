use axum::http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use n8n_rust_v5::{build_app, db::StorageEngine};
use serde_json::json;
use std::path::Path;
use tower::ServiceExt;
use uuid::Uuid;

fn setup_test_engine() -> (StorageEngine, std::path::PathBuf) {
    let test_db_path = std::env::temp_dir().join(format!("n8n_test_{}.sqlite", Uuid::new_v4()));
    let engine = StorageEngine::init(&test_db_path).expect("Failed to init test sqlite db");
    (engine, test_db_path)
}

#[tokio::test]
async fn test_health_settings_and_auto_auth() {
    let (db, db_path) = setup_test_engine();
    let app = build_app(Path::new("ui-dist"), db);

    // Test GET /healthz
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["status"], "ok");

    // Test GET /rest/healthz
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/healthz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["status"], "ok");

    // Test GET /rest/settings - 100% Enterprise Unlocked
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/settings")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(body["data"].is_object());
    assert_eq!(body["data"]["settingsMode"], "authenticated");
    assert_eq!(body["data"]["enterprise"]["variables"], true);
    assert_eq!(body["data"]["enterprise"]["sharing"], true);
    assert_eq!(body["data"]["enterprise"]["workflowDiffs"], true);
    assert_eq!(body["data"]["folders"]["enabled"], true);
    assert_eq!(body["data"]["aiAssistant"]["enabled"], true);

    // Test GET /rest/login - Auto-authenticated Owner
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/login")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["data"]["role"], "global:owner");
    assert_eq!(body["data"]["email"], "admin@n8n-rust.local");

    // Test GET /rest/me
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/me")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);

    // Clean up test sqlite
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_spa_index_html_clean_placeholders() {
    let (db, db_path) = setup_test_engine();
    let app = build_app(Path::new("ui-dist"), db);

    // Test GET /
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(bytes.to_vec()).unwrap();

    // Verify zero raw template placeholders
    assert!(
        !html.contains("{{BASE_PATH}}"),
        "Must not contain {{{{BASE_PATH}}}}"
    );
    assert!(
        !html.contains("%CONFIG_TAGS%"),
        "Must not contain %CONFIG_TAGS%"
    );
    assert!(
        !html.contains("{{CSP_NONCE}}"),
        "Must not contain {{{{CSP_NONCE}}}}"
    );
    assert!(
        !html.contains("{{REST_ENDPOINT}}"),
        "Must not contain {{{{REST_ENDPOINT}}}}"
    );

    // Verify correct replacements
    assert!(
        html.contains("href=\"/favicon.ico\""),
        "Favicon path should be /favicon.ico"
    );
    assert!(
        html.contains("n8n:config:rest-endpoint"),
        "Must contain rest-endpoint config meta tag"
    );
    assert!(html.contains("id=\"app\""), "Must contain Vue root #app");

    // Clean up test sqlite
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_workflow_full_crud_lifecycle() {
    let (db, db_path) = setup_test_engine();
    let app = build_app(Path::new("ui-dist"), db);

    // 1. Create Workflow
    let create_payload = json!({
        "name": "Integration Test Pipeline",
        "nodes": [
            {
                "id": "node-1",
                "name": "Manual Trigger",
                "type": "n8n-nodes-base.manualTrigger",
                "typeVersion": 1,
                "position": [250, 300]
            },
            {
                "id": "node-2",
                "name": "HTTP Request",
                "type": "n8n-nodes-base.httpRequest",
                "typeVersion": 4.2,
                "position": [450, 300]
            }
        ],
        "connections": {
            "Manual Trigger": {
                "main": [
                    [
                        {
                            "node": "HTTP Request",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            }
        },
        "settings": {
            "executionOrder": "v1"
        }
    });

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/workflows")
                .method("POST")
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(create_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let created_wf: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let wf_id = created_wf["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(created_wf["data"]["name"], "Integration Test Pipeline");
    assert_eq!(created_wf["data"]["nodes"].as_array().unwrap().len(), 2);

    // 2. Read Workflow by ID
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/rest/workflows/{wf_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let fetched_wf: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(fetched_wf["data"]["id"], wf_id);

    // 3. Update Workflow (PATCH)
    let patch_payload = json!({
        "name": "Updated Test Pipeline",
        "active": true
    });

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/rest/workflows/{wf_id}"))
                .method("PATCH")
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(patch_payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let updated_wf: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(updated_wf["data"]["name"], "Updated Test Pipeline");
    assert_eq!(updated_wf["data"]["active"], true);

    // 4. List Workflows
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/workflows")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let list: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(list["data"].as_array().unwrap().len(), 1);

    // 5. Delete Workflow
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/rest/workflows/{wf_id}"))
                .method("DELETE")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);

    // Verify deletion
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/rest/workflows/{wf_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // Clean up test sqlite
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_credentials_tags_and_zstd_executions() {
    let (db, db_path) = setup_test_engine();
    let app = build_app(Path::new("ui-dist"), db.clone());

    // 1. Create Tag
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/tags")
                .method("POST")
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "production" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 2. Create Credential
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/rest/credentials")
                .method("POST")
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(
                    json!({
                        "name": "Stripe Live API",
                        "type": "stripeApi",
                        "data": { "apiKey": "sk_live_12345" }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let cred: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(cred["data"]["name"], "Stripe Live API");

    // 3. Test Executions Storage & Zstd Decompression
    let execution_id = "exec-test-999";
    let execution_payload = json!({
        "resultData": {
            "runData": {
                "Manual Trigger": [
                    {
                        "data": {
                            "main": [
                                [{ "json": { "status": "executed", "items": [1, 2, 3] } }]
                            ]
                        }
                    }
                ]
            }
        }
    });

    // Save directly with zstd compression via StorageEngine
    db.save_execution(
        execution_id,
        Some("wf-test-1"),
        "success",
        "manual",
        "2026-10-14T20:00:00.000Z",
        Some("2026-10-14T20:00:01.000Z"),
        &execution_payload,
    )
    .expect("Failed to save zstd execution");

    // Retrieve via REST endpoint which automatically decompresses zstd
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/rest/executions/{execution_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let exec_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(exec_res["data"]["id"], execution_id);
    assert_eq!(exec_res["data"]["status"], "success");
    assert_eq!(
        exec_res["data"]["data"]["resultData"]["runData"]["Manual Trigger"][0]["data"]["main"][0]
            [0]["json"]["status"],
        "executed"
    );

    // Clean up test sqlite
    let _ = std::fs::remove_file(db_path);
}
