use axum::{
    http::{header, Request, StatusCode},
    routing::get,
    Router,
};
use chrono::Utc;
use http_body_util::BodyExt;
use n8n_rust_v5::{
    build_app,
    db::{ExecutionWriteMessage, StorageEngine},
    engine::{ArenaNode, CompiledGraph, HyperScaleEngine, NodeKind},
};
use serde_json::json;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tower::ServiceExt;
use uuid::Uuid;

fn setup_test_engine() -> (StorageEngine, std::path::PathBuf) {
    let test_db_path = std::env::temp_dir().join(format!("n8n_test_{}.sqlite", Uuid::new_v4()));
    let engine = StorageEngine::init(&test_db_path).expect("Failed to init test sqlite db");
    (engine, test_db_path)
}

// ---------------------------------------------------------------------------
// 1. SPA & Template Placeholders Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_spa_index_html_clean_placeholders() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db);

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

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 1 timed out!");
}

// ---------------------------------------------------------------------------
// 2. 100% Unlocked Enterprise & AI Settings Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_settings_100_percent_enterprise_unlocked() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db);

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
        let data = &body["data"];

        // Base settings
        assert_eq!(data["settingsMode"], "authenticated");
        assert_eq!(data["userManagement"]["showSetupOnFirstLoad"], false);
        assert_eq!(data["folders"]["enabled"], true);
        assert_eq!(data["variables"]["limit"], -1);
        assert_eq!(data["aiAssistant"]["enabled"], true);
        assert_eq!(data["aiBuilder"]["enabled"], true);
        assert_eq!(data["askAi"]["enabled"], true);
        assert_eq!(data["evaluation"]["quota"], -1);

        // 100% Enterprise Flags Assertion (All true except showNonProdBanner)
        let ent = &data["enterprise"];
        assert_eq!(ent["sharing"], true);
        assert_eq!(ent["ldap"], true);
        assert_eq!(ent["saml"], true);
        assert_eq!(ent["oidc"], true);
        assert_eq!(ent["mfaEnforcement"], true);
        assert_eq!(ent["logStreaming"], true);
        assert_eq!(ent["advancedExecutionFilters"], true);
        assert_eq!(ent["variables"], true);
        assert_eq!(ent["sourceControl"], true);
        assert_eq!(ent["auditLogs"], true);
        assert_eq!(ent["externalSecrets"], true);
        assert_eq!(ent["showNonProdBanner"], false);
        assert_eq!(ent["debugInEditor"], true);
        assert_eq!(ent["binaryDataS3"], true);
        assert_eq!(ent["workerView"], true);
        assert_eq!(ent["advancedPermissions"], true);
        assert_eq!(ent["workflowDiffs"], true);
        assert_eq!(ent["namedVersions"], true);
        assert_eq!(ent["provisioning"], true);
        assert_eq!(ent["customRoles"], true);
        assert_eq!(ent["personalSpacePolicy"], true);
        assert_eq!(ent["dataRedaction"], true);
        assert_eq!(ent["otelCustomSpanAttributes"], true);
        assert_eq!(ent["workflowReviews"], true);
        assert_eq!(ent["projects"]["team"]["limit"], -1);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 2 timed out!");
}

// ---------------------------------------------------------------------------
// 3. Auto-Auth Login, Me, and License Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_auto_auth_login_me_and_license() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db);

        // Test GET /rest/login
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
        assert_eq!(body["data"]["isOwner"], true);
        assert_eq!(body["data"]["isPending"], false);
        assert_eq!(body["data"]["email"], "admin@n8n-rust.local");
        assert!(body["data"]["globalScopes"].as_array().unwrap().len() >= 20);

        // Test POST /rest/login
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/login")
                    .method("POST")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

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

        // Test GET /rest/license
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/license")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let lic: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(lic["data"]["planName"], "Enterprise");
        assert_eq!(lic["data"]["isPlanActive"], true);

        // Test GET /rest/projects
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/projects")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let proj: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(proj["data"].as_array().unwrap().len(), 1);
        assert_eq!(proj["data"][0]["type"], "personal");

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 3 timed out!");
}

// ---------------------------------------------------------------------------
// 4. Node Types & Credential Types Contracts Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_node_types_and_credential_types_contracts() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db);

        // Test GET /rest/node-types
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/node-types")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let nodes = body["data"].as_array().expect("Node types must be array");
        assert!(nodes.len() >= 9, "Must contain all core node definitions");

        let mut names = Vec::new();
        for n in nodes {
            assert!(n["name"].is_string());
            assert!(n["displayName"].is_string());
            assert!(n["properties"].is_array());
            names.push(n["name"].as_str().unwrap().to_string());
        }
        assert!(names.contains(&"n8n-nodes-base.manualTrigger".to_string()));
        assert!(names.contains(&"n8n-nodes-base.httpRequest".to_string()));
        assert!(names.contains(&"n8n-nodes-base.webhook".to_string()));
        assert!(names.contains(&"n8n-nodes-base.code".to_string()));

        // Test GET /rest/credential-types
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/credential-types")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let creds = body["data"]
            .as_array()
            .expect("Credential types must be array");
        assert!(creds.len() >= 3);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 4 timed out!");
}

// ---------------------------------------------------------------------------
// 5. Workflow Full CRUD & Archive Lifecycle Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_workflow_full_crud_and_archive_lifecycle() {
    tokio::time::timeout(Duration::from_secs(30), async {
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

        // 4. Archive Workflow
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{wf_id}/archive"))
                    .method("POST")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let archived_wf: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(archived_wf["data"]["isArchived"], true);

        // 5. Unarchive Workflow
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{wf_id}/unarchive"))
                    .method("POST")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let unarchived_wf: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(unarchived_wf["data"]["isArchived"], false);

        // 6. Delete Workflow
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

        // Verify deletion returns 404
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

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 5 timed out!");
}

// ---------------------------------------------------------------------------
// 6. Folders & Variables Full CRUD Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_folders_and_variables_full_crud() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db);

        // --- FOLDERS CRUD ---
        // 1. Create Folder
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/folders")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({ "name": "Production Pipelines" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let folder_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let folder_id = folder_res["data"]["id"].as_str().unwrap().to_string();
        assert_eq!(folder_res["data"]["name"], "Production Pipelines");

        // 2. List Folders
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/folders")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let f_list: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(f_list["data"].as_array().unwrap().len(), 1);

        // 3. Update Folder
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/folders/{folder_id}"))
                    .method("PATCH")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({ "name": "Core Automations" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 4. Delete Folder
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/folders/{folder_id}"))
                    .method("DELETE")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // --- VARIABLES CRUD ---
        // 1. Create Variable
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/variables")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({ "key": "API_BASE_URL", "value": "https://api.gateway.internal", "type": "string" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let var_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let var_id = var_res["data"]["id"].as_str().unwrap().to_string();
        assert_eq!(var_res["data"]["key"], "API_BASE_URL");

        // 2. List Variables
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/variables")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let v_list: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v_list["data"].as_array().unwrap().len(), 1);

        // 3. Update Variable
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/variables/{var_id}"))
                    .method("PATCH")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({ "value": "https://api.gateway.v2.internal" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 4. Delete Variable
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/variables/{var_id}"))
                    .method("DELETE")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 6 timed out!");
}

// ---------------------------------------------------------------------------
// 7. Credentials & Tags Full CRUD Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_credentials_and_tags_full_crud() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db);

        // --- TAGS CRUD ---
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
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let tag_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let tag_id = tag_res["data"]["id"].as_str().unwrap().to_string();

        // 2. List Tags
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/tags")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 3. Update Tag
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/tags/{tag_id}"))
                    .method("PATCH")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({ "name": "production-core" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 4. Delete Tag
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/tags/{tag_id}"))
                    .method("DELETE")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // --- CREDENTIALS CRUD ---
        // 1. Create Credential
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
        let cred_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let cred_id = cred_res["data"]["id"].as_str().unwrap().to_string();
        assert_eq!(cred_res["data"]["name"], "Stripe Live API");

        // 2. Get Credential by ID
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/credentials/{cred_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 3. Update Credential
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/credentials/{cred_id}"))
                    .method("PATCH")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({ "name": "Stripe Live Primary" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // 4. Delete Credential
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/credentials/{cred_id}"))
                    .method("DELETE")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 7 timed out!");
}

// ---------------------------------------------------------------------------
// 8. True Batch Write-Behind Zstd Executions & Explicit Flush Test (100 parallel items)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_write_behind_batch_zstd_executions() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        let total_items = 100;

        // Send 100 executions concurrently to the write-behind buffer
        for i in 0..total_items {
            let msg = ExecutionWriteMessage {
                id: format!("exec-batch-{i:03}"),
                workflow_id: Some("wf-bench-pipeline".into()),
                status: if i % 2 == 0 {
                    "success".into()
                } else {
                    "error".into()
                },
                mode: "integrated".into(),
                started_at: "2026-10-14T22:00:00.000Z".into(),
                stopped_at: Some("2026-10-14T22:00:01.000Z".into()),
                data_json: json!({
                    "batchIndex": i,
                    "resultData": {
                        "runData": {
                            "Manual Trigger": [
                                {
                                    "data": {
                                        "main": [
                                            [{ "json": { "item": i, "checksum": format!("crc32-{}", i * 13) } }]
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }),
            };

            db.record_execution_async(msg)
                .expect("Failed to enqueue execution message");
        }

        // Explicitly flush all queued executions to disk (0ms race condition proof)
        db.flush_executions()
            .await
            .expect("Explicit flush executions failed");

        // 1. Verify via GET /rest/executions that 100 executions are returned
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/executions")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let exec_list: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let records = exec_list["data"].as_array().unwrap();
        assert_eq!(
            records.len(),
            total_items,
            "All 100 executions must be committed in SQLite"
        );

        // 2. Verify individual execution decompression with item 042
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/executions/exec-batch-042")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let detail_42: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(detail_42["data"]["id"], "exec-batch-042");
        assert_eq!(detail_42["data"]["status"], "success");
        assert_eq!(detail_42["data"]["data"]["batchIndex"], 42);
        assert_eq!(
            detail_42["data"]["data"]["resultData"]["runData"]["Manual Trigger"][0]["data"]["main"][0]
                [0]["json"]["item"],
            42
        );

        // 3. Verify individual execution decompression with item 099
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/executions/exec-batch-099")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let detail_99: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(detail_99["data"]["id"], "exec-batch-099");
        assert_eq!(detail_99["data"]["status"], "error");
        assert_eq!(detail_99["data"]["data"]["batchIndex"], 99);
        assert_eq!(
            detail_99["data"]["data"]["resultData"]["runData"]["Manual Trigger"][0]["data"]["main"][0]
                [0]["json"]["checksum"],
            "crc32-1287"
        );

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 8 timed out!");
}

// ---------------------------------------------------------------------------
// 9. Engine DAG Execution & REST Run Test (Multi-branch Fan-out & Fan-in)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_engine_dag_execution_and_rest_run() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // 1. Create a branching DAG workflow with parallel fan-in:
        // Trigger -> Branch1 (Set { branch: 1 }) & Branch2 (Set { branch: 2 }) -> MergeNodes
        let workflow_payload = json!({
            "name": "Parallel Branching Fan-In Test",
            "nodes": [
                {
                    "id": "node-trigger",
                    "name": "Trigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "typeVersion": 1,
                    "position": [100, 200]
                },
                {
                    "id": "node-branch-1",
                    "name": "Branch1",
                    "type": "n8n-nodes-base.set",
                    "typeVersion": 3.4,
                    "position": [300, 100],
                    "parameters": { "branch": 1 }
                },
                {
                    "id": "node-branch-2",
                    "name": "Branch2",
                    "type": "n8n-nodes-base.set",
                    "typeVersion": 3.4,
                    "position": [300, 300],
                    "parameters": { "branch": 2 }
                },
                {
                    "id": "node-merge",
                    "name": "MergeNodes",
                    "type": "n8n-nodes-base.merge",
                    "typeVersion": 3,
                    "position": [500, 200]
                },
                {
                    "id": "node-post-merge",
                    "name": "PostMergeSet",
                    "type": "n8n-nodes-base.set",
                    "typeVersion": 1,
                    "position": [700, 200],
                    "parameters": {
                        "multiplied": "={{ $json.branch * 10 }}"
                    }
                }
            ],
            "connections": {
                "Trigger": {
                    "main": [
                        [
                            { "node": "Branch1", "type": "main", "index": 0 },
                            { "node": "Branch2", "type": "main", "index": 0 }
                        ]
                    ]
                },
                "Branch1": {
                    "main": [
                        [
                            { "node": "MergeNodes", "type": "main", "index": 0 }
                        ]
                    ]
                },
                "Branch2": {
                    "main": [
                        [
                            { "node": "MergeNodes", "type": "main", "index": 1 }
                        ]
                    ]
                },
                "MergeNodes": {
                    "main": [
                        [
                            { "node": "PostMergeSet", "type": "main", "index": 0 }
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
                    .body(axum::body::Body::from(workflow_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let created: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let wf_id = created["data"]["id"].as_str().unwrap().to_string();

        // 2. Execute workflow via POST /rest/workflows/:id/run with Vue UI payload format (workflowData wrap)
        let run_body = json!({
            "workflowData": {
                "nodes": workflow_payload["nodes"],
                "connections": workflow_payload["connections"]
            }
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{wf_id}/run"))
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(run_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let run_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let exec_id = run_res["data"]["executionId"].as_str().unwrap().to_string();
        assert_eq!(run_res["data"]["status"], "success");
        assert_eq!(run_res["data"]["executedNodes"], 5);

        // Flush write-behind executions buffer
        db.flush_executions().await.expect("Flush failed");

        // 3. Verify execution was recorded to SQLite and multi-input fan-in merged correctly
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/executions/{exec_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let db_exec: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(db_exec["data"]["id"], exec_id);
        assert_eq!(db_exec["data"]["status"], "success");

        // Verify Vue Canvas full ITaskData contract across all 5 executed nodes
        let run_data = &db_exec["data"]["data"]["resultData"]["runData"];
        for node_name in ["Trigger", "Branch1", "Branch2", "MergeNodes", "PostMergeSet"] {
            let task_entry = &run_data[node_name][0];
            assert_eq!(task_entry["executionStatus"], "success");
            assert!(
                task_entry["startTime"].as_i64().unwrap_or(0) > 0,
                "Node {node_name} must have a valid positive startTime timestamp"
            );
            assert!(
                task_entry["executionTime"].as_u64().unwrap_or(0) >= 1,
                "Node {node_name} must have an executionTime of at least 1ms"
            );
            assert!(task_entry["source"].is_array());
        }

        let merge_items = run_data["MergeNodes"][0]["data"]["main"][0]
            .as_array()
            .expect("MergeNodes main[0] must be an array");

        // Strictly verify that BOTH branch outputs are preserved with top-level branch properties!
        assert_eq!(
            merge_items.len(),
            2,
            "MergeNodes must contain items from both Branch1 and Branch2"
        );
        let b1_found = merge_items.iter().any(|item| item["json"]["branch"] == 1);
        let b2_found = merge_items.iter().any(|item| item["json"]["branch"] == 2);
        assert!(b1_found, "Branch 1 output must be preserved in MergeNodes");
        assert!(b2_found, "Branch 2 output must be preserved in MergeNodes");

        // Strictly verify per-element array expression evaluation on PostMergeSet
        let post_merge_items = run_data["PostMergeSet"][0]["data"]["main"][0]
            .as_array()
            .expect("PostMergeSet main[0] must be an array");
        assert_eq!(post_merge_items.len(), 2);
        let mult1_found = post_merge_items
            .iter()
            .any(|item| item["json"]["branch"] == 1 && item["json"]["multiplied"] == 10);
        let mult2_found = post_merge_items
            .iter()
            .any(|item| item["json"]["branch"] == 2 && item["json"]["multiplied"] == 20);
        assert!(mult1_found, "Per-element expr evaluation: branch 1 multiplied to 10");
        assert!(mult2_found, "Per-element expr evaluation: branch 2 multiplied to 20");

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 9 timed out!");
}

// ---------------------------------------------------------------------------
// 10. Real-Time Push Events & Direct Workflow Run Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_websocket_push_realtime_events() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // 1. Verify /rest/push endpoint connects cleanly as SSE
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/push?pushRef=test-client-1")
                    .header(header::ACCEPT, "text/event-stream")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let ct = res
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            ct.contains("text/event-stream"),
            "Expected text/event-stream, got {}",
            ct
        );

        // 2. Direct run workflow via POST /rest/workflows/run
        let run_direct_payload = json!({
            "nodes": [
                {
                    "id": "direct-node-1",
                    "name": "Direct Trigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "typeVersion": 1,
                    "position": [100, 200]
                },
                {
                    "id": "direct-node-2",
                    "name": "Direct Transform",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [300, 200]
                }
            ],
            "connections": {
                "Direct Trigger": {
                    "main": [
                        [
                            { "node": "Direct Transform", "type": "main", "index": 0 }
                        ]
                    ]
                }
            }
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(run_direct_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let run_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(run_res["data"]["status"], "success");
        assert_eq!(run_res["data"]["executedNodes"], 2);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 10 timed out!");
}

// ---------------------------------------------------------------------------
// 11. Hyper-Scale 1,000,000 (One Million) Node Zero-Copy Arena Stress Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_hyperscale_one_million_nodes_arena_execution() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let engine = HyperScaleEngine::new(None).expect("Failed to initialize engine");

        let total_nodes = 1_000_000;
        let chains_count = 1_000;
        let nodes_per_chain = total_nodes / chains_count; // 1,000 nodes per chain

        println!("\n========================================================");
        println!(">>> BUILDING 1,000,000 NODE FLAT ARENA DAG GRAPH <<<");
        let build_start = Instant::now();

        let mut nodes = Vec::with_capacity(total_nodes);
        let mut edges = vec![Vec::new(); total_nodes];

        let static_set_params = json!({ "step": "processed", "verified": true });
        let static_if_params = json!({ "value1": 10, "value2": 5, "operation": "largerEqual" });
        let static_filter_params = json!({ "value1": 10, "value2": 5, "operation": "largerEqual" });

        // Build 1,000 parallel independent real heterogeneous n8n pipelines
        for c in 0..chains_count {
            let base = c * nodes_per_chain;
            for s in 0..nodes_per_chain {
                let idx = base + s;
                let (node_type, params) = if s == 0 {
                    ("n8n-nodes-base.manualTrigger", json!({}))
                } else {
                    match idx % 5 {
                        1 => ("n8n-nodes-base.set", static_set_params.clone()),
                        2 => ("n8n-nodes-base.if", static_if_params.clone()),
                        3 => ("n8n-nodes-base.filter", static_filter_params.clone()),
                        4 => ("n8n-nodes-base.code", json!({})),
                        _ => ("n8n-nodes-base.set", static_set_params.clone()),
                    }
                };

                nodes.push(ArenaNode {
                    id: format!("n_{idx}"),
                    name: format!("Node_{idx}"),
                    node_type: node_type.to_string(),
                    parameters: params,
                    has_expressions: false,
                    kind: NodeKind::from_type_str(node_type),
                });

                if s + 1 < nodes_per_chain {
                    edges[idx].push((idx + 1) as u32);
                }
            }
        }

        let graph = CompiledGraph::programmatic(nodes, edges);
        let build_duration = build_start.elapsed();
        println!(
            ">>> Graph Construction Completed in: {:?} <<<",
            build_duration
        );
        println!(
            ">>> Total Nodes: {} | Total Roots: {} <<<",
            graph.nodes.len(),
            graph.roots.len()
        );
        assert_eq!(graph.nodes.len(), 1_000_000);
        assert_eq!(graph.roots.len(), 1_000);

        // Initial zero-copy seed payload
        let initial_payload = Arc::new(json!({
            "benchmark": "1_000_000_real_n8n_nodes_execution",
            "zeroCopy": true,
            "mode": "hyperScale"
        }));

        println!(">>> STARTING ZERO-COPY MULTI-RUNNER EXECUTION <<<");
        let exec_start = Instant::now();
        let exec_id = format!("bench-1m-{}", Uuid::new_v4());

        let result = engine
            .execute(
                &exec_id,
                Some("wf-benchmark-1m"),
                &graph,
                initial_payload,
                None,
                None,
            )
            .await
            .expect("HyperScale execution failed");

        let exec_duration = exec_start.elapsed();
        let exec_ms = exec_duration.as_millis();
        let throughput = (total_nodes as f64) / exec_duration.as_secs_f64();

        println!("========================================================");
        println!(">>> [HYPERSCALE VERIFICATION RESULT] <<<");
        println!(">>> Executed Nodes Count: {}", result.executed_nodes);
        println!(">>> Status: {}", result.status);
        println!(">>> Duration: {} ms ({:?})", exec_ms, exec_duration);
        println!(
            ">>> Throughput: {:.2} nodes/sec ({:.2} million nodes/sec)",
            throughput,
            throughput / 1_000_000.0
        );
        println!("========================================================\n");

        // Strict asserts: 1M nodes and true transformations
        assert_eq!(
            result.executed_nodes, 1_000_000,
            "Must execute exactly 1,000,000 nodes"
        );
        assert_eq!(result.status, "success", "Execution status must be success");
        assert!(
            exec_ms < 5000,
            "Execution of 1,000,000 nodes took {} ms (must be under 5,000 ms)",
            exec_ms
        );
        assert_eq!(
            result.final_output["step"], "processed",
            "Final output must contain step == processed"
        );
        assert_eq!(
            result.final_output["verified"], true,
            "Final output must contain verified == true"
        );
        assert_eq!(
            result.final_output["codeExecuted"], true,
            "Final output must contain codeExecuted == true"
        );

        // Save to SQLite via write-behind buffer and flush
        let write_msg = ExecutionWriteMessage {
            id: exec_id.clone(),
            workflow_id: Some("wf-benchmark-1m".to_string()),
            status: result.status.clone(),
            mode: "hyperScaleCompact".to_string(),
            started_at: Utc::now().to_rfc3339(),
            stopped_at: Some(Utc::now().to_rfc3339()),
            data_json: result.payload.clone(),
        };
        db.record_execution_async(write_msg)
            .expect("Failed to enqueue 1M execution record");
        db.flush_executions()
            .await
            .expect("Failed to flush 1M execution to disk");

        // Verify persisted record in SQLite
        let db_record = db
            .get_execution(&exec_id)
            .expect("Failed to query db")
            .expect("1M execution record not found in SQLite");
        assert_eq!(db_record.id, exec_id);
        assert_eq!(db_record.status, "success");
        assert_eq!(db_record.data["executedNodes"], 1_000_000);
        assert_eq!(db_record.data["mode"], "hyperScaleCompact");

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 11 timed out!");
}

// ---------------------------------------------------------------------------
// 12. Engine Cancellation & Anti-Stuck Watchdog Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_engine_cancellation_and_watchdog_timeout() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let engine = HyperScaleEngine::new(Some(4)).expect("Engine init failed");

        let nodes = vec![
            ArenaNode {
                id: "c1".to_string(),
                name: "Trigger".to_string(),
                node_type: "n8n-nodes-base.manualTrigger".to_string(),
                parameters: json!({}),
                has_expressions: false,
                kind: NodeKind::ManualTrigger,
            },
            ArenaNode {
                id: "c2".to_string(),
                name: "Step".to_string(),
                node_type: "n8n-nodes-base.set".to_string(),
                parameters: json!({ "v": 1 }),
                has_expressions: false,
                kind: NodeKind::Set,
            },
        ];
        let edges = vec![vec![1], vec![]];
        let graph = CompiledGraph::programmatic(nodes, edges);

        let cancel_token = Arc::new(AtomicBool::new(true)); // Pre-cancelled
        let exec_id = format!("exec-cancel-{}", Uuid::new_v4());

        let res = engine
            .execute(
                &exec_id,
                Some("wf-cancel"),
                &graph,
                Arc::new(json!({})),
                None,
                Some(cancel_token),
            )
            .await
            .expect("Execute call failed");

        assert_eq!(
            res.status, "canceled",
            "Pre-cancelled execution must return canceled status"
        );

        // Test API stop execution endpoint
        let app = build_app(Path::new("ui-dist"), db);
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/executions/{exec_id}/stop"))
                    .method("POST")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // 404 since it's not active anymore, proving clean state handling
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 12 timed out!");
}

// ---------------------------------------------------------------------------
// 13. Expression Engine Evaluation ($json, $node, Math, String Methods) Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_expression_engine_json_node_and_math_evaluation() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // Workflow: Trigger -> Node1 (Set { price: 100, qty: 3, name: "rust" })
        //                   -> Node2 (Set with expressions for total, label, fromNode)
        let workflow_payload = json!({
            "name": "Expression Engine Test Pipeline",
            "nodes": [
                {
                    "id": "node-trigger",
                    "name": "Trigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "typeVersion": 1,
                    "position": [100, 200]
                },
                {
                    "id": "node-1",
                    "name": "Node1",
                    "type": "n8n-nodes-base.set",
                    "typeVersion": 1,
                    "position": [300, 200],
                    "parameters": {
                        "values": {
                            "number": [
                                { "name": "price", "value": 100 },
                                { "name": "qty", "value": 3 }
                            ],
                            "string": [
                                { "name": "name", "value": "rust" }
                            ]
                        }
                    }
                },
                {
                    "id": "node-2",
                    "name": "Node2",
                    "type": "n8n-nodes-base.set",
                    "typeVersion": 1,
                    "position": [500, 200],
                    "parameters": {
                        "total": "={{ $json.price * $json.qty }}",
                        "label": "={{ $json.name.toUpperCase() }}",
                        "fromNode": "={{ $node[\"Node1\"].json.price }}",
                        "isMatch": "={{ $json.price > 50 && $json.qty == 3 }}",
                        "isEither": "={{ $json.price < 50 || $json.qty == 3 }}"
                    }
                }
            ],
            "connections": {
                "Trigger": {
                    "main": [
                        [ { "node": "Node1", "type": "main", "index": 0 } ]
                    ]
                },
                "Node1": {
                    "main": [
                        [ { "node": "Node2", "type": "main", "index": 0 } ]
                    ]
                }
            }
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({ "workflowData": workflow_payload }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let run_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(run_res["data"]["status"], "success");

        let node2_output =
            &run_res["data"]["resultData"]["runData"]["Node2"][0]["data"]["main"][0][0]["json"];
        assert_eq!(
            node2_output["total"], 300,
            "Math expression must evaluate to integer 300"
        );
        assert_eq!(
            node2_output["label"], "RUST",
            "String method toUpperCase must evaluate to 'RUST'"
        );
        assert_eq!(
            node2_output["fromNode"], 100,
            "Upstream $node access must resolve price from Node1"
        );
        assert_eq!(
            node2_output["isMatch"], true,
            "Boolean && operator expression must evaluate to true"
        );
        assert_eq!(
            node2_output["isEither"], true,
            "Boolean || operator expression must evaluate to true"
        );

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 13 timed out!");
}

// ---------------------------------------------------------------------------
// 14. Conditional Branch Routing (IF Node True/False Port Routing & Pruning) Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_conditional_if_node_true_false_branch_routing() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // Helper to run IF workflow with a given condition
        let run_if_wf = |is_condition_true: bool| {
            json!({
                "name": "Conditional IF Routing Test Pipeline",
                "nodes": [
                    {
                        "id": "node-trigger",
                        "name": "Trigger",
                        "type": "n8n-nodes-base.manualTrigger",
                        "typeVersion": 1,
                        "position": [100, 200]
                    },
                    {
                        "id": "node-if",
                        "name": "IfNode",
                        "type": "n8n-nodes-base.if",
                        "typeVersion": 1,
                        "position": [300, 200],
                        "parameters": {
                            "value1": if is_condition_true { 10 } else { 2 },
                            "value2": 10,
                            "operation": "largerEqual"
                        }
                    },
                    {
                        "id": "node-true",
                        "name": "TrueBranch",
                        "type": "n8n-nodes-base.set",
                        "typeVersion": 1,
                        "position": [500, 100],
                        "parameters": { "result": "passed" }
                    },
                    {
                        "id": "node-false",
                        "name": "FalseBranch",
                        "type": "n8n-nodes-base.set",
                        "typeVersion": 1,
                        "position": [500, 300],
                        "parameters": { "result": "failed" }
                    }
                ],
                "connections": {
                    "Trigger": {
                        "main": [
                            [ { "node": "IfNode", "type": "main", "index": 0 } ]
                        ]
                    },
                    "IfNode": {
                        "main": [
                            [ { "node": "TrueBranch", "type": "main", "index": 0 } ],
                            [ { "node": "FalseBranch", "type": "main", "index": 0 } ]
                        ]
                    }
                }
            })
        };

        // 1. Run with TRUE condition: TrueBranch must execute, FalseBranch must NOT execute
        let res_true = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({ "workflowData": run_if_wf(true) }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_true.status(), StatusCode::OK);
        let bytes_true = res_true.into_body().collect().await.unwrap().to_bytes();
        let body_true: serde_json::Value = serde_json::from_slice(&bytes_true).unwrap();
        let run_data_true = &body_true["data"]["resultData"]["runData"];

        assert!(
            run_data_true.get("TrueBranch").is_some(),
            "TrueBranch must be executed when condition is true"
        );
        assert!(
            run_data_true.get("FalseBranch").is_none(),
            "FalseBranch must be pruned when condition is true"
        );
        assert_eq!(
            run_data_true["TrueBranch"][0]["data"]["main"][0][0]["json"]["result"],
            "passed"
        );

        // 2. Run with FALSE condition: FalseBranch must execute, TrueBranch must NOT execute
        let res_false = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(
                        json!({ "workflowData": run_if_wf(false) }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_false.status(), StatusCode::OK);
        let bytes_false = res_false.into_body().collect().await.unwrap().to_bytes();
        let body_false: serde_json::Value = serde_json::from_slice(&bytes_false).unwrap();
        let run_data_false = &body_false["data"]["resultData"]["runData"];

        assert!(
            run_data_false.get("FalseBranch").is_some(),
            "FalseBranch must be executed when condition is false"
        );
        assert!(
            run_data_false.get("TrueBranch").is_none(),
            "TrueBranch must be pruned when condition is false"
        );
        assert_eq!(
            run_data_false["FalseBranch"][0]["data"]["main"][0][0]["json"]["result"],
            "failed"
        );

        // 3. Test Sort node directly via HyperScaleEngine with numeric values and descending order
        let sort_engine = HyperScaleEngine::new(Some(2)).expect("Sort engine init failed");
        let sort_nodes = vec![
            ArenaNode {
                id: "s1".to_string(),
                name: "SortTrigger".to_string(),
                node_type: "n8n-nodes-base.manualTrigger".to_string(),
                parameters: json!({}),
                has_expressions: false,
                kind: NodeKind::ManualTrigger,
            },
            ArenaNode {
                id: "s2".to_string(),
                name: "SortNode".to_string(),
                node_type: "n8n-nodes-base.sort".to_string(),
                parameters: json!({
                    "fieldName": "score",
                    "descending": true
                }),
                has_expressions: false,
                kind: NodeKind::Sort,
            },
        ];
        let sort_edges = vec![vec![1], vec![]];
        let sort_graph = CompiledGraph::programmatic(sort_nodes, sort_edges);
        let sort_input = Arc::new(json!([
            { "score": 10, "name": "ten" },
            { "score": 100, "name": "hundred" },
            { "score": 25, "name": "twenty-five" }
        ]));
        let sort_res = sort_engine
            .execute(
                &format!("exec-sort-{}", Uuid::new_v4()),
                Some("wf-sort-test"),
                &sort_graph,
                sort_input,
                None,
                None,
            )
            .await
            .expect("Sort engine execution failed");

        assert_eq!(sort_res.status, "success");
        let sorted_arr = sort_res.final_output.as_array().expect("Output must be array");
        assert_eq!(sorted_arr[0]["score"], 100);
        assert_eq!(sorted_arr[1]["score"], 25);
        assert_eq!(sorted_arr[2]["score"], 10);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 14 timed out!");
}

// ---------------------------------------------------------------------------
// 15. Live Webhook Trigger Execution End-to-End Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_live_webhook_trigger_execution_end_to_end() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // 1. Create a workflow with Webhook trigger node via POST /rest/workflows
        let wf_payload = json!({
            "name": "Order Processing Webhook Workflow",
            "nodes": [
                {
                    "id": "hook-1",
                    "name": "OrderWebhook",
                    "type": "n8n-nodes-base.webhook",
                    "typeVersion": 1,
                    "position": [100, 200],
                    "parameters": {
                        "path": "order-hook",
                        "httpMethod": "POST"
                    }
                },
                {
                    "id": "step-2",
                    "name": "ProcessOrder",
                    "type": "n8n-nodes-base.set",
                    "typeVersion": 1,
                    "position": [300, 200],
                    "parameters": {
                        "orderId": "={{ $json.body.orderId }}",
                        "status": "confirmed"
                    }
                }
            ],
            "connections": {
                "OrderWebhook": {
                    "main": [
                        [ { "node": "ProcessOrder", "type": "main", "index": 0 } ]
                    ]
                }
            },
            "settings": { "executionOrder": "v1" }
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(wf_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let created_wf: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let wf_id = created_wf["data"]["id"].as_str().unwrap().to_string();

        // 2. Invoke POST /webhook/order-hook with order payload
        let webhook_payload = json!({
            "orderId": "ORD-999",
            "amount": 500
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/webhook/order-hook")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(webhook_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let hook_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        // Assert transformed data from workflow
        assert_eq!(hook_res["orderId"], "ORD-999");
        assert_eq!(hook_res["status"], "confirmed");

        // Flush write-behind executions and assert recorded mode == "webhook"
        db.flush_executions()
            .await
            .expect("Flush executions failed");
        let exec_list = db.list_executions().expect("List executions failed");
        let hook_exec = exec_list
            .iter()
            .find(|e| e.mode == "webhook" && e.workflow_id.as_deref() == Some(&wf_id))
            .expect("Webhook execution must be persisted in SQLite with mode == 'webhook'");

        assert_eq!(hook_exec.status, "success");

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 15 timed out!");
}

// ---------------------------------------------------------------------------
// 16. Spatial LOD Viewport & Clustering Engine Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_spatial_lod_viewport_clustering() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db);

        // 1. Create a workflow with 10 nodes:
        // 2 nodes inside viewport [0..500, 0..500], 8 nodes outside in clusters
        let mut nodes = Vec::new();
        // 2 inside
        nodes.push(json!({
            "id": "node-in-1",
            "name": "Node Inside 1",
            "type": "n8n-nodes-base.manualTrigger",
            "position": [100.0, 100.0]
        }));
        nodes.push(json!({
            "id": "node-in-2",
            "name": "Node Inside 2",
            "type": "n8n-nodes-base.set",
            "position": [200.0, 200.0]
        }));

        // 4 nodes in cluster grid (4, 4) -> position [2100, 2100]
        for i in 1..=4 {
            nodes.push(json!({
                "id": format!("node-out-a-{}", i),
                "name": format!("Node Out A {}", i),
                "type": "n8n-nodes-base.set",
                "position": [2050.0 + (i as f64) * 10.0, 2050.0 + (i as f64) * 10.0]
            }));
        }

        // 4 nodes in cluster grid (6, 6) -> position [3100, 3100]
        for i in 1..=4 {
            nodes.push(json!({
                "id": format!("node-out-b-{}", i),
                "name": format!("Node Out B {}", i),
                "type": "n8n-nodes-base.set",
                "position": [3050.0 + (i as f64) * 10.0, 3050.0 + (i as f64) * 10.0]
            }));
        }

        let wf_payload = json!({
            "name": "Spatial LOD Test Workflow",
            "nodes": nodes,
            "connections": {}
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(wf_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let created_wf: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let wf_id = created_wf["data"]["id"].as_str().unwrap().to_string();

        // 2. Query spatial map with viewport [0..500, 0..500] and gridSize 500
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{wf_id}/spatial-map?minX=0&maxX=500&minY=0&maxY=500&gridSize=500"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let spatial_map: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let data = &spatial_map["data"];

        assert_eq!(data["totalOriginalNodes"], 10);
        assert!(data["renderedNodes"].as_u64().unwrap() < 10);
        assert_eq!(data["isClustered"], true);

        // Verify presence of cluster super-nodes
        let rendered_nodes = data["nodes"].as_array().unwrap();
        let has_cluster = rendered_nodes.iter().any(|n| {
            n.get("type").and_then(|t| t.as_str()) == Some("n8n-nodes-base.clusterSuperNode")
        });
        assert!(has_cluster, "Spatial map must include clusterSuperNode");

        // 3. Query get_workflow with viewport=true
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{wf_id}?viewport=true&minX=0&maxX=500&minY=0&maxY=500&gridSize=500"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let wf_resp: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(wf_resp["data"]["meta"]["lodClustered"], true);
        assert_eq!(wf_resp["data"]["meta"]["totalOriginalNodes"], 10);

        // 4. Anti-regression check: Query with gridSize=0 and inverted coordinates must NOT panic or produce NaN
        let res_zero = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{wf_id}/spatial-map?gridSize=0&minX=500&maxX=0&minY=500&maxY=0"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_zero.status(), StatusCode::OK);
        let bytes_zero = res_zero.into_body().collect().await.unwrap().to_bytes();
        let map_zero: serde_json::Value = serde_json::from_slice(&bytes_zero).unwrap();
        assert_eq!(map_zero["data"]["totalOriginalNodes"], 10);
        assert_eq!(map_zero["data"]["isClustered"], true);

        // 5. Anti-regression check: Extreme coordinates [1e100, -1e100] with gridSize=0
        let extreme_nodes_wf = json!({
            "name": "Extreme Coordinates Spatial Workflow",
            "nodes": [
                {
                    "id": "extreme-1",
                    "name": "Extreme Node 1",
                    "type": "n8n-nodes-base.set",
                    "position": [1e100, -1e100]
                },
                {
                    "id": "extreme-2",
                    "name": "Extreme Node 2",
                    "type": "n8n-nodes-base.set",
                    "position": [-1e100, 1e100]
                }
            ],
            "connections": {}
        });

        let res_ext = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(extreme_nodes_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_ext.status(), StatusCode::OK);
        let ext_bytes = res_ext.into_body().collect().await.unwrap().to_bytes();
        let ext_wf: serde_json::Value = serde_json::from_slice(&ext_bytes).unwrap();
        let ext_id = ext_wf["data"]["id"].as_str().unwrap();

        let res_ext_map = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{ext_id}/spatial-map?gridSize=0"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_ext_map.status(), StatusCode::OK);
        let ext_map_bytes = res_ext_map.into_body().collect().await.unwrap().to_bytes();
        let ext_map: serde_json::Value = serde_json::from_slice(&ext_map_bytes).unwrap();
        let rendered = ext_map["data"]["nodes"].as_array().unwrap();
        assert!(!rendered.is_empty());
        for n in rendered {
            if let Some(pos) = n.get("position").and_then(|p| p.as_array()) {
                let px = pos[0].as_f64().unwrap();
                let py = pos[1].as_f64().unwrap();
                assert!(px.is_finite(), "X coordinate must be finite, got {}", px);
                assert!(py.is_finite(), "Y coordinate must be finite, got {}", py);
            }
        }

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 16 timed out!");
}

// ---------------------------------------------------------------------------
// 17. Community Packages Registry & Dynamic Node Types Merge Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_community_packages_registry_and_node_types_merge() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db);

        // 1. Install custom community package
        let pkg_payload = json!({
            "name": "n8n-nodes-custom-crm",
            "version": "1.2.0",
            "author": "Community Developer",
            "description": "Custom CRM integration nodes",
            "manifest": {
                "nodeTypes": [
                    {
                        "name": "n8n-nodes-custom-crm.leadCreator",
                        "displayName": "CRM Lead Creator",
                        "group": ["action"],
                        "version": 1,
                        "description": "Creates a lead in custom CRM"
                    }
                ]
            }
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/community-packages")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(pkg_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let installed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(installed["data"]["name"], "n8n-nodes-custom-crm");

        // 2. List community packages
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/community-packages")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let list: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let pkgs = list["data"].as_array().unwrap();
        assert!(pkgs.iter().any(|p| p["name"] == "n8n-nodes-custom-crm"));

        // 3. Verify node-types catalog dynamically includes custom CRM node
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/node-types")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let catalog: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let nodes = catalog["data"].as_array().unwrap();
        let has_custom_node = nodes.iter().any(|n| {
            n.get("name").and_then(|name| name.as_str()) == Some("n8n-nodes-custom-crm.leadCreator")
        });
        assert!(has_custom_node, "Catalog must dynamically merge community nodes");

        // 4. Delete community package
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/community-packages/n8n-nodes-custom-crm")
                    .method("DELETE")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);

        // 5. Anti-regression check: Install package with empty/whitespace name must return 400 Bad Request
        let bad_pkg = json!({
            "name": "   ",
            "version": "1.0.0"
        });
        let res_bad = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/community-packages")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(bad_pkg.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_bad.status(), StatusCode::BAD_REQUEST);

        // 6. Anti-regression check: Install package with malformed manifest nodeTypes must return 400 Bad Request
        let bad_manifest_pkg = json!({
            "name": "bad-pkg",
            "manifest": {
                "nodeTypes": [
                    { "noName": 123 }
                ]
            }
        });
        let res_bad_m = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/community-packages")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(bad_manifest_pkg.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_bad_m.status(), StatusCode::BAD_REQUEST);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 17 timed out!");
}

// ---------------------------------------------------------------------------
// 18. Real HTTP Client & Active Workflow ScheduleTrigger Daemon Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_real_http_client_and_schedule_daemon() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // Spawn genuinely active local mock HTTP server
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let listener = rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr).unwrap();
            let mock_app = Router::new()
                .route("/healthz", get(|| async { axum::Json(json!({ "status": "ok" })) }))
                .route("/plain-text", get(|| async { "pong-plain-text-response" }));
            rt.block_on(async move {
                let _ = axum::serve(listener, mock_app).await;
            });
        });

        let local_addr = rx.recv().unwrap();

        // 1. Direct execution of HttpRequest node (genuine 200 OK without continueOnFail)
        let http_wf_payload = json!({
            "nodes": [
                {
                    "id": "http-1",
                    "name": "HTTP Request Node",
                    "type": "n8n-nodes-base.httpRequest",
                    "parameters": {
                        "url": format!("http://{local_addr}/healthz"),
                        "method": "GET"
                    }
                }
            ],
            "connections": {}
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(http_wf_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let run_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(run_res["data"]["status"], "success");
        let http1_out = &run_res["data"]["resultData"]["runData"]["HTTP Request Node"][0]["data"]["main"][0][0]["json"];
        assert_eq!(http1_out["statusCode"], 200);
        assert_eq!(http1_out["data"]["status"], "ok");

        // 2. Test active workflow with scheduleTrigger (intervalSecs: 1)
        let schedule_wf = json!({
            "name": "Scheduled Cron Workflow",
            "active": true,
            "nodes": [
                {
                    "id": "sched-1",
                    "name": "Schedule Trigger",
                    "type": "n8n-nodes-base.scheduleTrigger",
                    "parameters": {
                        "intervalSecs": 1
                    }
                },
                {
                    "id": "set-1",
                    "name": "Set Processed",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "ran": true
                    }
                }
            ],
            "connections": {
                "Schedule Trigger": {
                    "main": [
                        [
                            {
                                "node": "Set Processed",
                                "type": "main",
                                "index": 0
                            }
                        ]
                    ]
                }
            }
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(schedule_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);

        // Allow scheduler daemon to tick and trigger execution
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let _ = db.flush_executions().await;
        let execs = db.list_executions().unwrap_or_default();
        assert!(
            !execs.is_empty(),
            "ScheduleTrigger daemon must have triggered at least one execution"
        );

        // 3. Anti-regression check: Plain-text endpoint response & Connection Pool reuse
        let target_url = format!("http://{local_addr}/plain-text");
        for _ in 0..3 {
            let resp_val = n8n_rust_v5::engine::execute_real_http_request(&target_url, "GET", None, None);
            assert_eq!(resp_val["statusCode"], 200, "resp_val was: {}", resp_val);
            assert_eq!(
                resp_val["data"], "pong-plain-text-response",
                "Plain-text response must be preserved as Value::String and not dropped to Null"
            );
        }

        // 4. Anti-regression check: Dead local port with continueOnFail must return statusCode 502 and continueOnFail: true
        let dead_http_wf = json!({
            "nodes": [
                {
                    "id": "http-dead",
                    "name": "Dead HTTP Request",
                    "type": "n8n-nodes-base.httpRequest",
                    "parameters": {
                        "url": "http://127.0.0.1:1",
                        "method": "GET",
                        "continueOnFail": true
                    }
                }
            ],
            "connections": {}
        });

        let res_dead = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(dead_http_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_dead.status(), StatusCode::OK);
        let bytes_dead = res_dead.into_body().collect().await.unwrap().to_bytes();
        let dead_res: serde_json::Value = serde_json::from_slice(&bytes_dead).unwrap();
        assert_eq!(dead_res["data"]["status"], "success");
        let res_data = &dead_res["data"]["resultData"]["runData"]["Dead HTTP Request"][0]["data"]["main"][0][0]["json"];
        assert_eq!(res_data["statusCode"], 502, "Expected 502 Bad Gateway for dead port");
        assert_eq!(res_data["continueOnFail"], true, "Expected continueOnFail to be true");

        // 5. Anti-regression check: Dead local port without continueOnFail (continueOnFail: false) must propagate global error
        let dead_http_fail_wf = json!({
            "nodes": [
                {
                    "id": "http-dead-fail",
                    "name": "Dead HTTP Fail",
                    "type": "n8n-nodes-base.httpRequest",
                    "parameters": {
                        "url": "http://127.0.0.1:1",
                        "method": "GET",
                        "continueOnFail": false
                    }
                }
            ],
            "connections": {}
        });

        let res_dead_fail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(dead_http_fail_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_dead_fail.status(), StatusCode::OK);
        let bytes_dead_fail = res_dead_fail.into_body().collect().await.unwrap().to_bytes();
        let dead_res_fail: serde_json::Value = serde_json::from_slice(&bytes_dead_fail).unwrap();
        assert_eq!(dead_res_fail["data"]["status"], "error");
        let fail_run_entry = &dead_res_fail["data"]["resultData"]["runData"]["Dead HTTP Fail"][0];
        assert_eq!(fail_run_entry["executionStatus"], "error");
        assert!(
            fail_run_entry["error"]["message"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
            "Expected non-empty error message on failed node"
        );

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 18 timed out!");
}

// ---------------------------------------------------------------------------
// 19. Sub-Workflow Execution Engine & Recursion Depth Guard Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_sub_workflow_execution_and_recursion_guard() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // A. Successful Parent -> Sub-Workflow Execution (with inline workflow definition & parameter mapping)
        let sub_wf_definition = json!({
            "nodes": [
                {
                    "id": "sub-set-1",
                    "name": "Sub Set Node",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "sub_executed": true,
                        "multiplier": 42
                    }
                }
            ],
            "connections": {}
        });

        let parent_wf_payload = json!({
            "nodes": [
                {
                    "id": "parent-manual",
                    "name": "Parent Trigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "sub-exec-1",
                    "name": "Execute SubWorkflow",
                    "type": "n8n-nodes-base.executeWorkflow",
                    "parameters": {
                        "workflow": sub_wf_definition
                    }
                }
            ],
            "connections": {
                "Parent Trigger": {
                    "main": [
                        [
                            {
                                "node": "Execute SubWorkflow",
                                "type": "main",
                                "index": 0
                            }
                        ]
                    ]
                }
            }
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(parent_wf_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let run_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(run_res["data"]["status"], "success");
        let sub_run_data = &run_res["data"]["resultData"]["runData"]["Execute SubWorkflow"][0]["data"]["main"][0][0]["json"];
        assert_eq!(sub_run_data["sub_executed"], true);
        assert_eq!(sub_run_data["multiplier"], 42);

        // B. Real End-to-End Test: Create sub-workflow via REST POST /rest/workflows, then execute it by workflowId
        let db_subwf_payload = json!({
            "name": "Database Sub-Workflow",
            "active": true,
            "nodes": [
                {
                    "id": "db-sub-1",
                    "name": "DB Sub Node",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "from_db_subwf": true,
                        "value": 999
                    }
                }
            ],
            "connections": {}
        });

        let res_create_sub = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(db_subwf_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_create_sub.status(), StatusCode::OK);
        let bytes_create = res_create_sub.into_body().collect().await.unwrap().to_bytes();
        let create_res: serde_json::Value = serde_json::from_slice(&bytes_create).unwrap();
        let created_sub_id = create_res["data"]["id"].as_str().unwrap().to_string();

        let parent_db_payload = json!({
            "nodes": [
                {
                    "id": "p-trig",
                    "name": "Parent Trig",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "p-sub-exec",
                    "name": "Execute DB SubWF",
                    "type": "n8n-nodes-base.executeWorkflow",
                    "parameters": {
                        "workflowId": created_sub_id
                    }
                }
            ],
            "connections": {
                "Parent Trig": {
                    "main": [
                        [
                            {
                                "node": "Execute DB SubWF",
                                "type": "main",
                                "index": 0
                            }
                        ]
                    ]
                }
            }
        });

        let res_run_db_sub = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(parent_db_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run_db_sub.status(), StatusCode::OK);
        let bytes_db_sub = res_run_db_sub.into_body().collect().await.unwrap().to_bytes();
        let run_db_res: serde_json::Value = serde_json::from_slice(&bytes_db_sub).unwrap();
        assert_eq!(run_db_res["data"]["status"], "success");
        let db_sub_data = &run_db_res["data"]["resultData"]["runData"]["Execute DB SubWF"][0]["data"]["main"][0][0]["json"];
        assert_eq!(db_sub_data["from_db_subwf"], true);
        assert_eq!(db_sub_data["value"], 999);

        // C. Recursion Depth Guard: Circular Sub-Workflow Calling Itself (> 10 levels)
        let circular_sub_id = "circular-loop-subwf";
        let circular_wf = json!({
            "nodes": [
                {
                    "id": "recurse-node",
                    "name": "Recurse Node",
                    "type": "n8n-nodes-base.executeWorkflow",
                    "parameters": {
                        "workflowId": circular_sub_id
                    }
                }
            ],
            "connections": {}
        });

        n8n_rust_v5::engine::register_subworkflow(circular_sub_id, circular_wf);

        let parent_loop_payload = json!({
            "nodes": [
                {
                    "id": "trigger",
                    "name": "Loop Trigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "loop-exec",
                    "name": "Loop SubWorkflow",
                    "type": "n8n-nodes-base.executeWorkflow",
                    "parameters": {
                        "workflowId": circular_sub_id
                    }
                }
            ],
            "connections": {
                "Loop Trigger": {
                    "main": [
                        [
                            {
                                "node": "Loop SubWorkflow",
                                "type": "main",
                                "index": 0
                            }
                        ]
                    ]
                }
            }
        });

        let res_loop = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(parent_loop_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_loop.status(), StatusCode::OK);
        let bytes_loop = res_loop.into_body().collect().await.unwrap().to_bytes();
        let loop_res: serde_json::Value = serde_json::from_slice(&bytes_loop).unwrap();
        assert_eq!(loop_res["data"]["status"], "error");
        let loop_node_entry = &loop_res["data"]["resultData"]["runData"]["Loop SubWorkflow"][0];
        assert_eq!(loop_node_entry["executionStatus"], "error");
        let error_msg = loop_node_entry["error"]["message"].as_str().unwrap_or("");
        assert!(
            error_msg.contains("Exceeded maximum sub-workflow recursion depth (10)"),
            "Expected recursion guard error message, got: {}",
            error_msg
        );

        // D. Test continueOnFail: true on sub-workflow recursion failure
        let parent_loop_cof_payload = json!({
            "nodes": [
                {
                    "id": "trigger-cof",
                    "name": "Loop Trigger COF",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "loop-exec-cof",
                    "name": "Loop SubWorkflow COF",
                    "type": "n8n-nodes-base.executeWorkflow",
                    "parameters": {
                        "workflowId": circular_sub_id,
                        "continueOnFail": true
                    }
                }
            ],
            "connections": {
                "Loop Trigger COF": {
                    "main": [
                        [
                            {
                                "node": "Loop SubWorkflow COF",
                                "type": "main",
                                "index": 0
                            }
                        ]
                    ]
                }
            }
        });

        let res_loop_cof = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(parent_loop_cof_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_loop_cof.status(), StatusCode::OK);
        let bytes_loop_cof = res_loop_cof.into_body().collect().await.unwrap().to_bytes();
        let loop_cof_res: serde_json::Value = serde_json::from_slice(&bytes_loop_cof).unwrap();
        assert_eq!(loop_cof_res["data"]["status"], "success");
        let cof_entry = &loop_cof_res["data"]["resultData"]["runData"]["Loop SubWorkflow COF"][0]["data"]["main"][0][0]["json"];
        assert_eq!(cof_entry["continueOnFail"], true);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 19 timed out!");
}

// ---------------------------------------------------------------------------
// 20. HTTP Request Retry Policy (maxTries & waitBetweenTries) Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_request_retry_policy() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // Setup mock server with atomic hit counter
        let hit_counter = Arc::new(AtomicU32::new(0));
        let counter_clone = Arc::clone(&hit_counter);

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let listener = rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr).unwrap();
            let retry_app = Router::new().route(
                "/retry-endpoint",
                get(move || {
                    let c = Arc::clone(&counter_clone);
                    async move {
                        let current_hits = c.fetch_add(1, Ordering::SeqCst) + 1;
                        if current_hits < 3 {
                            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(json!({ "error": "Temporary Failure" })))
                        } else {
                            (StatusCode::OK, axum::Json(json!({ "recovered": true, "hits": current_hits })))
                        }
                    }
                }),
            );
            rt.block_on(async move {
                let _ = axum::serve(listener, retry_app).await;
            });
        });

        let local_addr = rx.recv().unwrap();

        // A. Successful recovery after 3 attempts
        let retry_wf = json!({
            "nodes": [
                {
                    "id": "http-retry",
                    "name": "Retry HTTP Request",
                    "type": "n8n-nodes-base.httpRequest",
                    "parameters": {
                        "url": format!("http://{local_addr}/retry-endpoint"),
                        "method": "GET",
                        "maxTries": 3,
                        "waitBetweenTries": 10
                    }
                }
            ],
            "connections": {}
        });

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(retry_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let run_res: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(run_res["data"]["status"], "success");
        let node_data = &run_res["data"]["resultData"]["runData"]["Retry HTTP Request"][0]["data"]["main"][0][0]["json"];
        assert_eq!(node_data["statusCode"], 200);
        assert_eq!(node_data["attempts"], 3, "Expected exactly 3 retry attempts");
        assert_eq!(node_data["data"]["recovered"], true);

        // B. Permanent failure on dead port verifying attempts count
        let dead_retry_wf = json!({
            "nodes": [
                {
                    "id": "dead-retry",
                    "name": "Dead Retry Node",
                    "type": "n8n-nodes-base.httpRequest",
                    "parameters": {
                        "url": "http://127.0.0.1:1",
                        "method": "GET",
                        "maxTries": 3,
                        "waitBetweenTries": 10,
                        "continueOnFail": true
                    }
                }
            ],
            "connections": {}
        });

        let res_dead = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(dead_retry_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_dead.status(), StatusCode::OK);
        let bytes_dead = res_dead.into_body().collect().await.unwrap().to_bytes();
        let dead_res: serde_json::Value = serde_json::from_slice(&bytes_dead).unwrap();
        assert_eq!(dead_res["data"]["status"], "success");
        let dead_node_data = &dead_res["data"]["resultData"]["runData"]["Dead Retry Node"][0]["data"]["main"][0][0]["json"];
        assert_eq!(dead_node_data["statusCode"], 502);
        assert_eq!(dead_node_data["attempts"], 3, "Expected 3 attempts on failing endpoint");

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 20 timed out!");
}

