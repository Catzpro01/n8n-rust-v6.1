use axum::http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use n8n_rust_v5::{
    build_app,
    db::{ExecutionWriteMessage, StorageEngine},
};
use serde_json::json;
use std::path::Path;
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
}

// ---------------------------------------------------------------------------
// 2. 100% Unlocked Enterprise & AI Settings Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_settings_100_percent_enterprise_unlocked() {
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
}

// ---------------------------------------------------------------------------
// 3. Auto-Auth Login, Me, and License Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_auto_auth_login_me_and_license() {
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
}

// ---------------------------------------------------------------------------
// 4. Node Types & Credential Types Contracts Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_node_types_and_credential_types_contracts() {
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
}

// ---------------------------------------------------------------------------
// 5. Workflow Full CRUD & Archive Lifecycle Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_workflow_full_crud_and_archive_lifecycle() {
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
}

// ---------------------------------------------------------------------------
// 6. Folders & Variables Full CRUD Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_folders_and_variables_full_crud() {
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
}

// ---------------------------------------------------------------------------
// 7. Credentials & Tags Full CRUD Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_credentials_and_tags_full_crud() {
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
}

// ---------------------------------------------------------------------------
// 8. True Batch Write-Behind Zstd Executions & Explicit Flush Test (100 parallel items)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_write_behind_batch_zstd_executions() {
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
}
