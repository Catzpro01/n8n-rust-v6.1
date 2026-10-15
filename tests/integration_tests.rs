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

        // Contract verification: functional nodes must have non-empty properties array
        let functional_nodes = [
            "n8n-nodes-base.switch",
            "n8n-nodes-base.filter",
            "n8n-nodes-base.sort",
            "n8n-nodes-base.limit",
            "n8n-nodes-base.executeWorkflow",
            "n8n-nodes-base.respondToWebhook",
            "n8n-nodes-base.splitOut",
            "n8n-nodes-base.aggregate",
            "n8n-nodes-base.removeDuplicates",
            "n8n-nodes-base.splitInBatches",
            "n8n-nodes-base.wait",
            "n8n-nodes-base.stopAndError",
        ];

        for target in functional_nodes {
            let node_def = nodes
                .iter()
                .find(|n| n["name"].as_str() == Some(target))
                .unwrap_or_else(|| panic!("Functional node {} must exist in catalog", target));
            let props = node_def["properties"]
                .as_array()
                .expect("Field 'properties' must be an array");
            assert!(
                !props.is_empty(),
                "Functional node {} field 'properties' must NOT be empty",
                target
            );
        }

        // Contract verification: every property with type "options" must have a non-empty options array with valid items
        for n in nodes {
            if let Some(props) = n.get("properties").and_then(|p| p.as_array()) {
                for prop in props {
                    if prop.get("type").and_then(|t| t.as_str()) == Some("options") {
                        let opt_arr = prop
                            .get("options")
                            .and_then(|o| o.as_array())
                            .unwrap_or_else(|| {
                                panic!(
                                    "Property '{}' on node '{}' has type 'options' but missing 'options' array",
                                    prop["name"], n["name"]
                                )
                            });
                        assert!(
                            !opt_arr.is_empty(),
                            "Property '{}' on node '{}' must have non-empty 'options' array",
                            prop["name"], n["name"]
                        );
                        for opt in opt_arr {
                            assert!(
                                opt.get("name").is_some(),
                                "Option item on property '{}' of node '{}' must contain 'name'",
                                prop["name"], n["name"]
                            );
                            assert!(
                                opt.get("value").is_some(),
                                "Option item on property '{}' of node '{}' must contain 'value'",
                                prop["name"], n["name"]
                            );
                        }
                    }
                }
            }
        }

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
        let static_filter_params = json!({ "field": "verified", "operator": "equals", "value": true });
        let static_switch_params = json!({ "rules": [{ "field": "step", "operator": "equals", "value": "processed", "outputPort": 0 }] });
        let static_sort_params = json!({ "sortField": "step", "order": "asc" });
        let static_limit_params = json!({ "maxItems": 10, "keep": "firstItems" });
        let static_aggregate_params = json!({ "destinationFieldName": "items" });
        let static_split_out_params = json!({ "fieldToSplitOut": "items" });
        let static_remove_dup_params = json!({ "compareField": "step" });
        let static_split_batch_params = json!({ "batchSize": 10, "batchIndex": 0 });
        let static_code_params = json!({});

        let mut type_counts: std::collections::BTreeMap<&'static str, usize> = std::collections::BTreeMap::new();

        // Build 1,000 parallel independent real heterogeneous n8n pipelines
        for c in 0..chains_count {
            let base = c * nodes_per_chain;
            for s in 0..nodes_per_chain {
                let idx = base + s;
                let (node_type, params) = if s == 0 {
                    ("n8n-nodes-base.manualTrigger", json!({}))
                } else {
                    match (s - 1) % 12 {
                        1 => ("n8n-nodes-base.set", static_set_params.clone()),
                        2 => ("n8n-nodes-base.if", static_if_params.clone()),
                        3 => ("n8n-nodes-base.filter", static_filter_params.clone()),
                        4 => ("n8n-nodes-base.switch", static_switch_params.clone()),
                        5 => ("n8n-nodes-base.sort", static_sort_params.clone()),
                        6 => ("n8n-nodes-base.limit", static_limit_params.clone()),
                        7 => ("n8n-nodes-base.code", static_code_params.clone()),
                        8 => ("n8n-nodes-base.aggregate", static_aggregate_params.clone()),
                        9 => ("n8n-nodes-base.splitOut", static_split_out_params.clone()),
                        10 => ("n8n-nodes-base.removeDuplicates", static_remove_dup_params.clone()),
                        11 => ("n8n-nodes-base.splitInBatches", static_split_batch_params.clone()),
                        _ => ("n8n-nodes-base.set", static_set_params.clone()),
                    }
                };

                *type_counts.entry(node_type).or_insert(0) += 1;

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
        println!(">>> Node Type Distribution:");
        for (ntype, count) in &type_counts {
            println!("    - {}: {}", ntype, count);
        }
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

        assert_eq!(result.executed_nodes, 1_000_000, "Must execute exactly 1,000,000 nodes");
        assert_eq!(result.status, "success", "Execution status must be success");
        assert!(
            exec_ms < 5000,
            "Execution of 1,000,000 nodes took {} ms (must be under 5,000 ms)",
            exec_ms
        );
        let final_item = result
            .final_output
            .as_array()
            .and_then(|a| a.first())
            .unwrap_or(&result.final_output);
        assert_eq!(final_item["step"], "processed", "Final output must contain step == processed");
        assert_eq!(final_item["verified"], true, "Final output must contain verified == true");
        assert_eq!(final_item["codeExecuted"], true, "Final output must contain codeExecuted == true");

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

// ---------------------------------------------------------------------------
// 21. Credential Vault At-Rest Encryption & HTTP Auth Injection Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_credential_vault_encryption_and_http_auth_injection() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        let secret_token = "super-secret-vault-token-xyz-12345";
        let cred_payload = json!({
            "name": "Production Vault Secret",
            "type": "customAuth",
            "data": {
                "token": secret_token,
                "custom_field": "confidential_value"
            }
        });

        // 1. Create first credential via REST API
        let res_create = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/credentials")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(cred_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_create.status(), StatusCode::OK);
        let bytes_create = res_create.into_body().collect().await.unwrap().to_bytes();
        let created_json: serde_json::Value = serde_json::from_slice(&bytes_create).unwrap();
        let cred_id = created_json["data"]["id"].as_str().unwrap().to_string();
        assert!(!cred_id.is_empty());

        // Create second credential with 100% identical data to verify per-record random nonce
        let cred_payload_2 = json!({
            "name": "Production Vault Secret Identical Copy",
            "type": "customAuth",
            "data": {
                "token": secret_token,
                "custom_field": "confidential_value"
            }
        });

        let res_create_2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/credentials")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(cred_payload_2.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_create_2.status(), StatusCode::OK);
        let bytes_create_2 = res_create_2.into_body().collect().await.unwrap().to_bytes();
        let created_json_2: serde_json::Value = serde_json::from_slice(&bytes_create_2).unwrap();
        let cred_id_2 = created_json_2["data"]["id"].as_str().unwrap().to_string();
        assert!(!cred_id_2.is_empty());

        // 2. Direct SQLite Inspection: Verify at-rest encryption format, random nonce, and zero plaintext leak
        {
            let raw_conn = rusqlite::Connection::open(&db_path).unwrap();
            let mut stmt = raw_conn
                .prepare("SELECT data FROM credentials WHERE id = ?1")
                .unwrap();
            let raw_encrypted_1: String = stmt
                .query_row(rusqlite::params![cred_id], |row| row.get(0))
                .unwrap();
            let raw_encrypted_2: String = stmt
                .query_row(rusqlite::params![cred_id_2], |row| row.get(0))
                .unwrap();

            // Verifikasi keduanya berawalan "enc:v1:"
            assert!(
                raw_encrypted_1.starts_with("enc:v1:"),
                "Credential 1 in SQLite must be encrypted at-rest and start with 'enc:v1:'. Found: {}",
                raw_encrypted_1
            );
            assert!(
                raw_encrypted_2.starts_with("enc:v1:"),
                "Credential 2 in SQLite must be encrypted at-rest and start with 'enc:v1:'. Found: {}",
                raw_encrypted_2
            );

            // Verifikasi format memiliki 2 titik dua (misal enc:v1:{nonce}:{ciphertext})
            assert!(
                raw_encrypted_1.matches(':').count() >= 2,
                "Format of credential 1 must contain colons separating prefix, nonce, and ciphertext: {}",
                raw_encrypted_1
            );
            assert!(
                raw_encrypted_2.matches(':').count() >= 2,
                "Format of credential 2 must contain colons separating prefix, nonce, and ciphertext: {}",
                raw_encrypted_2
            );

            // Verifikasi raw_encrypted_1 != raw_encrypted_2 (karena per-record nonce acak, ciphertext dua record identik WAJIB berbeda!)
            assert_ne!(
                raw_encrypted_1, raw_encrypted_2,
                "Per-record random nonce must ensure identical plaintext data produces completely distinct ciphertexts!"
            );

            // Verifikasi keduanya TIDAK mengandung secret_token dalam bentuk plaintext
            assert!(
                !raw_encrypted_1.contains(secret_token),
                "Encrypted credential 1 must NEVER leak plaintext secret tokens in storage! Found: {}",
                raw_encrypted_1
            );
            assert!(
                !raw_encrypted_2.contains(secret_token),
                "Encrypted credential 2 must NEVER leak plaintext secret tokens in storage! Found: {}",
                raw_encrypted_2
            );
        }

        // 3. Verify Transparent Decryption via REST API
        let res_get = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/credentials/{cred_id}"))
                    .method("GET")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_get.status(), StatusCode::OK);
        let bytes_get = res_get.into_body().collect().await.unwrap().to_bytes();
        let get_json: serde_json::Value = serde_json::from_slice(&bytes_get).unwrap();
        assert_eq!(get_json["data"]["data"]["token"], secret_token);
        assert_eq!(get_json["data"]["data"]["custom_field"], "confidential_value");

        // 4. Create third credential with "type": "httpBasicAuth"
        let cred_payload_3 = json!({
            "name": "Production Basic Auth Credential",
            "type": "httpBasicAuth",
            "data": {
                "user": "n8n_admin",
                "password": "super_secret_password_777"
            }
        });

        let res_create_3 = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/credentials")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(cred_payload_3.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_create_3.status(), StatusCode::OK);
        let bytes_create_3 = res_create_3.into_body().collect().await.unwrap().to_bytes();
        let created_json_3: serde_json::Value = serde_json::from_slice(&bytes_create_3).unwrap();
        let cred_id_3 = created_json_3["data"]["id"].as_str().unwrap().to_string();
        assert!(!cred_id_3.is_empty());

        // 5. Setup mock HTTP server to verify automatic Bearer token and Basic Auth injection
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let listener = rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr).unwrap();
            let auth_mock_app = Router::new()
                .route(
                    "/auth-test",
                    get(|headers: axum::http::HeaderMap| async move {
                        if let Some(auth_val) = headers.get(axum::http::header::AUTHORIZATION) {
                            if auth_val == "Bearer super-secret-vault-token-xyz-12345" {
                                (StatusCode::OK, axum::Json(json!({ "authenticated": true })))
                            } else {
                                (StatusCode::UNAUTHORIZED, axum::Json(json!({ "authenticated": false, "reason": "invalid_token" })))
                            }
                        } else {
                            (StatusCode::UNAUTHORIZED, axum::Json(json!({ "authenticated": false, "reason": "missing_header" })))
                        }
                    }),
                )
                .route(
                    "/auth-basic-test",
                    get(|headers: axum::http::HeaderMap| async move {
                        if let Some(auth_val) = headers.get(axum::http::header::AUTHORIZATION) {
                            if auth_val == "Basic bjhuX2FkbWluOnN1cGVyX3NlY3JldF9wYXNzd29yZF83Nzc=" {
                                (StatusCode::OK, axum::Json(json!({ "basic_authenticated": true })))
                            } else {
                                (StatusCode::UNAUTHORIZED, axum::Json(json!({ "basic_authenticated": false, "reason": "invalid_auth" })))
                            }
                        } else {
                            (StatusCode::UNAUTHORIZED, axum::Json(json!({ "basic_authenticated": false, "reason": "missing_header" })))
                        }
                    }),
                );
            rt.block_on(async move {
                let _ = axum::serve(listener, auth_mock_app).await;
            });
        });

        let local_addr = rx.recv().unwrap();

        // 6. Run workflow using HttpRequest node with Bearer token credentialId
        let http_cred_wf = json!({
            "nodes": [
                {
                    "id": "http-vault-node",
                    "name": "Vault HTTP Node",
                    "type": "n8n-nodes-base.httpRequest",
                    "parameters": {
                        "url": format!("http://{local_addr}/auth-test"),
                        "method": "GET",
                        "credentialId": cred_id
                    }
                }
            ],
            "connections": {}
        });

        let res_run = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(http_cred_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run.status(), StatusCode::OK);
        let bytes_run = res_run.into_body().collect().await.unwrap().to_bytes();
        let run_json: serde_json::Value = serde_json::from_slice(&bytes_run).unwrap();
        assert_eq!(run_json["data"]["status"], "success");
        let node_out = &run_json["data"]["resultData"]["runData"]["Vault HTTP Node"][0]["data"]["main"][0][0]["json"];
        assert_eq!(node_out["statusCode"], 200);
        assert_eq!(node_out["data"]["authenticated"], true, "Expected node to be authenticated via injected Bearer header");

        // 7. Run workflow using HttpRequest node with Basic Auth credentialId (cred_id_3)
        let http_basic_wf = json!({
            "nodes": [
                {
                    "id": "http-basic-node",
                    "name": "Basic Auth HTTP Node",
                    "type": "n8n-nodes-base.httpRequest",
                    "parameters": {
                        "url": format!("http://{local_addr}/auth-basic-test"),
                        "method": "GET",
                        "credentialId": cred_id_3
                    }
                }
            ],
            "connections": {}
        });

        let res_run_basic = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(http_basic_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run_basic.status(), StatusCode::OK);
        let bytes_run_basic = res_run_basic.into_body().collect().await.unwrap().to_bytes();
        let run_json_basic: serde_json::Value = serde_json::from_slice(&bytes_run_basic).unwrap();
        assert_eq!(run_json_basic["data"]["status"], "success");
        let basic_out = &run_json_basic["data"]["resultData"]["runData"]["Basic Auth HTTP Node"][0]["data"]["main"][0][0]["json"];
        assert_eq!(basic_out["statusCode"], 200);
        assert_eq!(basic_out["data"]["basic_authenticated"], true, "Expected node to be authenticated via injected Basic Auth header");

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 21 timed out!");
}

// ---------------------------------------------------------------------------
// 22. RespondToWebhook & Collection Transform Nodes (SplitOut, Aggregate, RemoveDuplicates) Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_respond_to_webhook_and_collection_transform_nodes() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // Create workflow DAG: Webhook -> SplitOut -> RemoveDuplicates -> Aggregate -> RespondToWebhook
        let full_transform_wf = json!({
            "name": "Collection Transform & Webhook Respond Workflow",
            "nodes": [
                {
                    "id": "wh-trigger",
                    "name": "Webhook Trigger",
                    "type": "n8n-nodes-base.webhook",
                    "parameters": {
                        "path": "collection-pipeline"
                    }
                },
                {
                    "id": "split-node",
                    "name": "Split Out Records",
                    "type": "n8n-nodes-base.splitOut",
                    "parameters": {
                        "fieldToSplitOut": "records"
                    }
                },
                {
                    "id": "dedup-node",
                    "name": "Remove Duplicates",
                    "type": "n8n-nodes-base.removeDuplicates",
                    "parameters": {
                        "compareField": "id"
                    }
                },
                {
                    "id": "agg-node",
                    "name": "Aggregate Items",
                    "type": "n8n-nodes-base.aggregate",
                    "parameters": {
                        "destinationFieldName": "deduplicatedRecords"
                    }
                },
                {
                    "id": "resp-node",
                    "name": "Respond to Webhook",
                    "type": "n8n-nodes-base.respondToWebhook",
                    "parameters": {
                        "responseCode": 201,
                        "responseHeaders": {
                            "X-Engine": "n8n-rust-v5.7",
                            "X-Transform-Status": "deduplicated-ok"
                        },
                        "responseBody": {
                            "success": true,
                            "processedCount": 3,
                            "summary": "Records processed and deduplicated"
                        }
                    }
                }
            ],
            "connections": {
                "Webhook Trigger": {
                    "main": [
                        [
                            { "node": "Split Out Records", "type": "main", "index": 0 }
                        ]
                    ]
                },
                "Split Out Records": {
                    "main": [
                        [
                            { "node": "Remove Duplicates", "type": "main", "index": 0 }
                        ]
                    ]
                },
                "Remove Duplicates": {
                    "main": [
                        [
                            { "node": "Aggregate Items", "type": "main", "index": 0 }
                        ]
                    ]
                },
                "Aggregate Items": {
                    "main": [
                        [
                            { "node": "Respond to Webhook", "type": "main", "index": 0 }
                        ]
                    ]
                }
            }
        });

        // Save workflow to DB so Webhook handler can find it by path
        let wf_record = n8n_rust_v5::db::WorkflowRecord {
            id: "wf-collection-pipeline".to_string(),
            name: "Collection Transform & Webhook Respond Workflow".to_string(),
            active: true,
            is_archived: false,
            nodes: full_transform_wf["nodes"].clone(),
            connections: full_transform_wf["connections"].clone(),
            settings: json!({}),
            static_data: json!({}),
            pin_data: json!({}),
            meta: json!({}),
            version_id: "v1".to_string(),
            trigger_count: 1,
            tags: json!([]),
            parent_folder_id: None,
            created_at: "2026-10-15T00:00:00Z".to_string(),
            updated_at: "2026-10-15T00:00:00Z".to_string(),
        };
        db.insert_or_update_workflow(&wf_record).unwrap();

        // 1. Test SplitOut -> RemoveDuplicates -> Aggregate pipeline via REST API
        let transform_direct_wf = json!({
            "nodes": [
                {
                    "id": "t-manual",
                    "name": "Manual Input",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "batchId": "b-1",
                        "records": [
                            { "id": 1, "val": "A" },
                            { "id": 2, "val": "B" },
                            { "id": 1, "val": "A-duplicate" },
                            { "id": "10", "label": "string_ten" },
                            { "id": 10, "label": "number_ten" },
                            { "label": "no_id_field" }
                        ]
                    }
                },
                {
                    "id": "t-split",
                    "name": "Split Direct",
                    "type": "n8n-nodes-base.splitOut",
                    "parameters": {
                        "fieldToSplitOut": "records"
                    }
                },
                {
                    "id": "t-dedup",
                    "name": "Dedup Direct",
                    "type": "n8n-nodes-base.removeDuplicates",
                    "parameters": {
                        "compareField": "id"
                    }
                },
                {
                    "id": "t-agg",
                    "name": "Agg Direct",
                    "type": "n8n-nodes-base.aggregate",
                    "parameters": {
                        "destinationFieldName": "grouped"
                    }
                }
            ],
            "connections": {
                "Manual Input": {
                    "main": [[{ "node": "Split Direct", "type": "main", "index": 0 }]]
                },
                "Split Direct": {
                    "main": [[{ "node": "Dedup Direct", "type": "main", "index": 0 }]]
                },
                "Dedup Direct": {
                    "main": [[{ "node": "Agg Direct", "type": "main", "index": 0 }]]
                }
            }
        });

        let res_pipe = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(transform_direct_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_pipe.status(), StatusCode::OK);
        let bytes_pipe = res_pipe.into_body().collect().await.unwrap().to_bytes();
        let pipe_json: serde_json::Value = serde_json::from_slice(&bytes_pipe).unwrap();
        assert_eq!(pipe_json["data"]["status"], "success");
        let agg_output = &pipe_json["data"]["resultData"]["runData"]["Agg Direct"][0]["data"]["main"][0][0]["json"];
        let grouped_arr = agg_output["grouped"].as_array().expect("Agg Direct must produce grouped array");

        // Verifikasi:
        // 1. Item duplikat nyata: {"id": 1, "val": "A"} dan {"id": 1, "val": "A-duplicate"} -> hanya 1 yang lolos.
        // 2. Item dengan tipe berbeda pada field yang sama: {"id": "10", "label": "string_ten"} vs {"id": 10, "label": "number_ten"} -> KEDUANYA harus lolos deduplikasi (tidak saling menghapus!).
        // 3. Item tanpa field id: {"label": "no_id_field"} -> TIDAK BOLEH dihapus!
        assert_eq!(grouped_arr.len(), 5, "Expected 5 deduplicated records preserving distinct types and missing fields");
        assert_eq!(grouped_arr[0]["id"], 1);
        assert_eq!(grouped_arr[0]["val"], "A");
        assert_eq!(grouped_arr[1]["id"], 2);
        assert_eq!(grouped_arr[1]["val"], "B");
        assert_eq!(grouped_arr[2]["id"], "10");
        assert_eq!(grouped_arr[2]["label"], "string_ten");
        assert_eq!(grouped_arr[3]["id"], 10);
        assert_eq!(grouped_arr[3]["label"], "number_ten");
        assert_eq!(grouped_arr[4]["label"], "no_id_field");
        assert!(grouped_arr[4].get("id").is_none());
        assert!(
            !grouped_arr.iter().any(|item| item.get("val").and_then(|v| v.as_str()) == Some("A-duplicate")),
            "Duplicate item with id 1 must be removed"
        );

        // 2. Trigger Webhook with request payload and verify RespondToWebhook overrides HTTP response
        let webhook_input_payload = json!({
            "records": [
                { "id": "u-10", "name": "User 10" },
                { "id": "u-20", "name": "User 20" },
                { "id": "u-10", "name": "User 10 Dup" },
                { "id": "u-30", "name": "User 30" }
            ]
        });

        let res_wh = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/webhook/collection-pipeline")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(webhook_input_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Verify status code 201 Created from RespondToWebhook
        assert_eq!(res_wh.status(), StatusCode::CREATED, "RespondToWebhook must set status 201 Created");

        // Verify custom response headers
        let headers = res_wh.headers();
        assert_eq!(
            headers.get("X-Engine").and_then(|v| v.to_str().ok()),
            Some("n8n-rust-v5.7"),
            "Custom X-Engine header must match RespondToWebhook config"
        );
        assert_eq!(
            headers.get("X-Transform-Status").and_then(|v| v.to_str().ok()),
            Some("deduplicated-ok")
        );

        // Verifikasi header content-type ada dan bernilai application/json
        assert_eq!(
            headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
            Some("application/json"),
            "Content-Type header must exist and equal application/json"
        );

        // Verify custom response body
        let bytes_wh = res_wh.into_body().collect().await.unwrap().to_bytes();
        let wh_json: serde_json::Value = serde_json::from_slice(&bytes_wh).unwrap();
        assert_eq!(wh_json["success"], true);
        assert_eq!(wh_json["processedCount"], 3);
        assert_eq!(wh_json["summary"], "Records processed and deduplicated");

        // Verifikasi bahwa JSON response body yang diterima client TIDAK mengandung key "__webhook_response"
        assert!(
            wh_json.get("__webhook_response").is_none(),
            "Client response body must NOT contain internal '__webhook_response' key: {:?}",
            wh_json
        );

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 22 timed out!");
}

// ---------------------------------------------------------------------------
// 23. Variables Expression Resolution & Flow Control Nodes Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_vars_expression_and_flow_control_nodes() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // 1. Create global variable via REST API POST /rest/variables
        let var_payload = json!({
            "key": "API_HOST",
            "value": "api.production.internal",
            "type": "string"
        });

        let res_var = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/variables")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(var_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_var.status(), StatusCode::OK);
        let bytes_var = res_var.into_body().collect().await.unwrap().to_bytes();
        let var_json: serde_json::Value = serde_json::from_slice(&bytes_var).unwrap();
        assert_eq!(var_json["data"]["key"], "API_HOST");
        assert_eq!(var_json["data"]["value"], "api.production.internal");
        let var_id = var_json["data"]["id"].as_str().unwrap().to_string();

        // 2. Test $vars.API_HOST expression resolution in workflow Set node
        let vars_wf = json!({
            "nodes": [
                {
                    "id": "set-target",
                    "name": "Set Target URL",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "target_url": "https://{{$vars.API_HOST}}/v1/users"
                    }
                }
            ],
            "connections": {}
        });

        let res_run = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(vars_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run.status(), StatusCode::OK);
        let bytes_run = res_run.into_body().collect().await.unwrap().to_bytes();
        let run_json: serde_json::Value = serde_json::from_slice(&bytes_run).unwrap();
        assert_eq!(run_json["data"]["status"], "success");
        let set_output = &run_json["data"]["resultData"]["runData"]["Set Target URL"][0]["data"]["main"][0][0]["json"];
        assert_eq!(set_output["target_url"], "https://api.production.internal/v1/users");

        // 2a. Update variable via PUT /rest/variables/{var_id} and test bracket expression {{$vars['API_HOST']}}
        let update_payload = json!({
            "value": "api.staging.internal"
        });

        let res_put = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/variables/{}", var_id))
                    .method("PUT")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(update_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_put.status(), StatusCode::OK);

        let vars_wf_bracket = json!({
            "nodes": [
                {
                    "id": "set-target",
                    "name": "Set Target URL",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "target_url": "https://{{$vars['API_HOST']}}/v2/users"
                    }
                }
            ],
            "connections": {}
        });

        let res_run_bracket = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(vars_wf_bracket.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run_bracket.status(), StatusCode::OK);
        let bytes_run_bracket = res_run_bracket.into_body().collect().await.unwrap().to_bytes();
        let run_json_bracket: serde_json::Value = serde_json::from_slice(&bytes_run_bracket).unwrap();
        assert_eq!(run_json_bracket["data"]["status"], "success");
        let set_output_bracket = &run_json_bracket["data"]["resultData"]["runData"]["Set Target URL"][0]["data"]["main"][0][0]["json"];
        assert_eq!(set_output_bracket["target_url"], "https://api.staging.internal/v2/users");

        // 2b. Delete variable via DELETE /rest/variables/{var_id} and verify resolution fallback
        let res_del = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/variables/{}", var_id))
                    .method("DELETE")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_del.status(), StatusCode::OK);

        let res_run_after_delete = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(vars_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run_after_delete.status(), StatusCode::OK);
        let bytes_run_del = res_run_after_delete.into_body().collect().await.unwrap().to_bytes();
        let run_json_del: serde_json::Value = serde_json::from_slice(&bytes_run_del).unwrap();
        assert_eq!(run_json_del["data"]["status"], "success");
        let set_output_del = &run_json_del["data"]["resultData"]["runData"]["Set Target URL"][0]["data"]["main"][0][0]["json"];
        assert_eq!(set_output_del["target_url"], "https:///v1/users");

        // 3. Test SplitInBatches node with [1, 2, 3, 4, 5]
        let engine = HyperScaleEngine::new(Some(2)).expect("HyperScaleEngine init failed");

        // batchSize: 2, batchIndex: 0 -> port 0, [1, 2]
        let split_node_0 = json!({
            "nodes": [
                {
                    "id": "split-0",
                    "name": "Split Batch 0",
                    "type": "n8n-nodes-base.splitInBatches",
                    "parameters": {
                        "batchSize": 2,
                        "batchIndex": 0
                    }
                }
            ],
            "connections": {}
        });
        let graph_0 = CompiledGraph::compile(&split_node_0["nodes"], &split_node_0["connections"]).unwrap();
        let res_0 = engine.execute(
            "exec-split-0",
            None,
            &graph_0,
            Arc::new(json!([1, 2, 3, 4, 5])),
            None,
            None,
        ).await.unwrap();
        assert_eq!(res_0.status, "success");
        assert_eq!(res_0.final_output, json!([1, 2]));

        // batchSize: 2, batchIndex: 2 -> port 0, [5]
        let split_node_2 = json!({
            "nodes": [
                {
                    "id": "split-2",
                    "name": "Split Batch 2",
                    "type": "n8n-nodes-base.splitInBatches",
                    "parameters": {
                        "batchSize": 2,
                        "batchIndex": 2
                    }
                }
            ],
            "connections": {}
        });
        let graph_2 = CompiledGraph::compile(&split_node_2["nodes"], &split_node_2["connections"]).unwrap();
        let res_2 = engine.execute(
            "exec-split-2",
            None,
            &graph_2,
            Arc::new(json!([1, 2, 3, 4, 5])),
            None,
            None,
        ).await.unwrap();
        assert_eq!(res_2.status, "success");
        assert_eq!(res_2.final_output, json!([5]));

        // batchSize: 2, batchIndex: 3 -> port 1 (done), []
        let split_node_3 = json!({
            "nodes": [
                {
                    "id": "split-3",
                    "name": "Split Batch 3",
                    "type": "n8n-nodes-base.splitInBatches",
                    "parameters": {
                        "batchSize": 2,
                        "batchIndex": 3
                    }
                }
            ],
            "connections": {}
        });
        let graph_3 = CompiledGraph::compile(&split_node_3["nodes"], &split_node_3["connections"]).unwrap();
        let res_3 = engine.execute(
            "exec-split-3",
            None,
            &graph_3,
            Arc::new(json!([1, 2, 3, 4, 5])),
            None,
            None,
        ).await.unwrap();
        assert_eq!(res_3.status, "success");
        assert_eq!(res_3.final_output, json!([]));

        // 4. Test Wait node
        let wait_wf = json!({
            "nodes": [
                {
                    "id": "wait-node",
                    "name": "Wait Node",
                    "type": "n8n-nodes-base.wait",
                    "parameters": {
                        "amount": 50
                    }
                }
            ],
            "connections": {}
        });
        let wait_start = Instant::now();
        let res_wait = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(wait_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let elapsed_wait = wait_start.elapsed();
        assert_eq!(res_wait.status(), StatusCode::OK);
        assert!(
            elapsed_wait >= Duration::from_millis(40),
            "Wait node with amount 50 must sleep >= 40ms, got {:?}",
            elapsed_wait
        );

        // 5. Test StopAndError node
        // Case A: Without continueOnFail -> workflow status "error", node produces __node_error: true
        let stop_err_wf = json!({
            "nodes": [
                {
                    "id": "stop-err-node",
                    "name": "Stop and Error Node",
                    "type": "n8n-nodes-base.stopAndError",
                    "parameters": {
                        "errorMessage": "Emergency Stop Triggered"
                    }
                }
            ],
            "connections": {}
        });
        let res_stop = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(stop_err_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_stop.status(), StatusCode::OK);
        let bytes_stop = res_stop.into_body().collect().await.unwrap().to_bytes();
        let stop_json: serde_json::Value = serde_json::from_slice(&bytes_stop).unwrap();
        assert_eq!(stop_json["data"]["status"], "error");
        let stop_out = &stop_json["data"]["resultData"]["runData"]["Stop and Error Node"][0]["data"]["main"][0][0]["json"];
        assert_eq!(stop_out["__node_error"], true);
        assert_eq!(stop_out["error"]["message"], "Emergency Stop Triggered");

        // Case B: With continueOnFail: true -> workflow status "success", output node has continueOnFail: true
        let stop_cof_wf = json!({
            "nodes": [
                {
                    "id": "stop-cof-node",
                    "name": "Stop and Error Continue",
                    "type": "n8n-nodes-base.stopAndError",
                    "continueOnFail": true,
                    "parameters": {
                        "errorMessage": "Emergency Stop Triggered",
                        "continueOnFail": true
                    }
                }
            ],
            "connections": {}
        });
        let res_cof = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(stop_cof_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_cof.status(), StatusCode::OK);
        let bytes_cof = res_cof.into_body().collect().await.unwrap().to_bytes();
        let cof_json: serde_json::Value = serde_json::from_slice(&bytes_cof).unwrap();
        assert_eq!(cof_json["data"]["status"], "success");
        let cof_out = &cof_json["data"]["resultData"]["runData"]["Stop and Error Continue"][0]["data"]["main"][0][0]["json"];
        assert_eq!(cof_out["continueOnFail"], true);
        assert_eq!(cof_out["error"]["message"], "Emergency Stop Triggered");

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 23 timed out!");
}

// ---------------------------------------------------------------------------
// 24. Error Trigger & Error Workflow Handling Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_error_trigger_and_error_workflow_handling() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // 1. Create error handler workflow:
        //    Node 1: Error Trigger (n8n-nodes-base.errorTrigger)
        //    Node 2: Set (n8n-nodes-base.set) that captures $json.error.message and $json.execution.id
        let error_handler_wf = json!({
            "name": "Global Error Handler Workflow",
            "nodes": [
                {
                    "id": "err-trigger",
                    "name": "Error Trigger",
                    "type": "n8n-nodes-base.errorTrigger",
                    "parameters": {}
                },
                {
                    "id": "err-set",
                    "name": "Capture Error Info",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "error_message": "={{$json.error.message}}",
                        "failed_execution_id": "={{$json.execution.id}}"
                    }
                }
            ],
            "connections": {
                "Error Trigger": {
                    "main": [
                        [
                            { "node": "Capture Error Info", "type": "main", "index": 0 }
                        ]
                    ]
                }
            }
        });

        // Save to DB with ID "err-handler-wf-001" and activate
        let err_wf_record = n8n_rust_v5::db::WorkflowRecord {
            id: "err-handler-wf-001".to_string(),
            name: "Global Error Handler Workflow".to_string(),
            active: true,
            is_archived: false,
            nodes: error_handler_wf["nodes"].clone(),
            connections: error_handler_wf["connections"].clone(),
            settings: json!({}),
            static_data: json!({}),
            pin_data: json!({}),
            meta: json!({}),
            version_id: "v1".to_string(),
            trigger_count: 1,
            tags: json!([]),
            parent_folder_id: None,
            created_at: "2026-10-15T00:00:00Z".to_string(),
            updated_at: "2026-10-15T00:00:00Z".to_string(),
        };
        db.insert_or_update_workflow(&err_wf_record).unwrap();
        n8n_rust_v5::engine::register_subworkflow(
            "err-handler-wf-001",
            json!({
                "nodes": err_wf_record.nodes,
                "connections": err_wf_record.connections
            }),
        );

        // 2. Create main workflow:
        //    Node: StopAndError (errorMessage: "Critical DB Connection Failure", continueOnFail: false)
        //    Settings: { "errorWorkflow": "err-handler-wf-001" }
        let main_wf = json!({
            "nodes": [
                {
                    "id": "main-stop-err",
                    "name": "Stop on Critical DB Error",
                    "type": "n8n-nodes-base.stopAndError",
                    "parameters": {
                        "errorMessage": "Critical DB Connection Failure",
                        "continueOnFail": false
                    }
                }
            ],
            "connections": {},
            "settings": {
                "errorWorkflow": "err-handler-wf-001"
            }
        });

        // 3. Execute main workflow via POST /rest/workflows/run
        let res_main = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(main_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_main.status(), StatusCode::OK);
        let bytes_main = res_main.into_body().collect().await.unwrap().to_bytes();
        let main_json: serde_json::Value = serde_json::from_slice(&bytes_main).unwrap();

        // 4. Verifications:
        let main_exec_id = main_json["data"]["executionId"].as_str().unwrap();

        //    - Status eksekusi main workflow adalah "error"
        assert_eq!(
            main_json["data"]["status"], "error",
            "Main workflow execution status must be 'error'"
        );

        //    - Respon API mengandung "errorWorkflowExecution" dengan status: "success" dan workflowId: "err-handler-wf-001"
        let error_wf_execution = &main_json["data"]["errorWorkflowExecution"];
        assert!(
            !error_wf_execution.is_null(),
            "Response data must contain 'errorWorkflowExecution': {:?}",
            main_json
        );
        assert_eq!(
            error_wf_execution["status"], "success",
            "Error workflow execution status must be 'success'"
        );
        assert_eq!(
            error_wf_execution["workflowId"], "err-handler-wf-001",
            "Error workflow ID must match configured 'err-handler-wf-001'"
        );
        assert_eq!(
            error_wf_execution["finalOutput"]["error_message"],
            "Critical DB Connection Failure",
            "Captured error_message in error workflow finalOutput must match"
        );
        assert_eq!(
            error_wf_execution["finalOutput"]["failed_execution_id"],
            main_exec_id,
            "Captured failed_execution_id in error workflow finalOutput must match main execution ID"
        );
        let captured_node_out = &error_wf_execution["resultData"]["runData"]["Capture Error Info"][0]["data"]["main"][0][0]["json"];
        assert_eq!(captured_node_out["error_message"], "Critical DB Connection Failure");
        assert_eq!(captured_node_out["failed_execution_id"], main_exec_id);

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 24 timed out!");
}

// ---------------------------------------------------------------------------
// 25. Switch, Sort, Limit, and Filter Nodes End-to-End Pipeline & Pruned Merge Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_switch_sort_limit_and_filter_nodes() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db.clone());

        // 1. Pipeline Filter -> Sort -> Limit via /rest/workflows/run
        let pipeline_items = json!([
            { "id": 1, "name": "Task 1", "active": true, "score": 40 },
            { "id": 2, "name": "Task 2", "active": false, "score": 90 },
            { "id": 3, "name": "Task 3", "active": true, "score": 85 },
            { "id": 4, "name": "Task 4", "active": true, "score": 95 },
            { "id": 5, "name": "Task 5", "active": true, "score": 60 }
        ]);

        let pipeline_wf = json!({
            "name": "Filter Sort Limit Test Pipeline",
            "nodes": [
                {
                    "id": "node-trigger-1",
                    "name": "Trigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "node-filter-1",
                    "name": "FilterActive",
                    "type": "n8n-nodes-base.filter",
                    "parameters": {
                        "field": "active",
                        "operator": "equals",
                        "value": true
                    }
                },
                {
                    "id": "node-sort-1",
                    "name": "SortScore",
                    "type": "n8n-nodes-base.sort",
                    "parameters": {
                        "sortField": "score",
                        "order": "descending"
                    }
                },
                {
                    "id": "node-limit-1",
                    "name": "LimitTop3",
                    "type": "n8n-nodes-base.limit",
                    "parameters": {
                        "maxItems": 3,
                        "keep": "firstItems"
                    }
                }
            ],
            "connections": {
                "Trigger": {
                    "main": [
                        [ { "node": "FilterActive", "type": "main", "index": 0 } ]
                    ]
                },
                "FilterActive": {
                    "main": [
                        [ { "node": "SortScore", "type": "main", "index": 0 } ]
                    ]
                },
                "SortScore": {
                    "main": [
                        [ { "node": "LimitTop3", "type": "main", "index": 0 } ]
                    ]
                }
            },
            "pinData": {
                "Trigger": pipeline_items
            }
        });

        let res_pipeline = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(pipeline_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_pipeline.status(), StatusCode::OK);
        let bytes_p = res_pipeline.into_body().collect().await.unwrap().to_bytes();
        let res_p_json: serde_json::Value = serde_json::from_slice(&bytes_p).unwrap();
        assert_eq!(
            res_p_json["data"]["status"], "success",
            "Pipeline Filter->Sort->Limit execution status must be success"
        );

        let limit_out = &res_p_json["data"]["resultData"]["runData"]["LimitTop3"][0]["data"]["main"][0];
        let limit_arr = limit_out.as_array().expect("LimitTop3 output must be an array");
        assert_eq!(limit_arr.len(), 3, "LimitTop3 must return exactly 3 items");

        assert_eq!(limit_arr[0]["json"]["id"], 4);
        assert_eq!(limit_arr[0]["json"]["name"], "Task 4");
        assert_eq!(limit_arr[0]["json"]["score"], 95);

        assert_eq!(limit_arr[1]["json"]["id"], 3);
        assert_eq!(limit_arr[1]["json"]["name"], "Task 3");
        assert_eq!(limit_arr[1]["json"]["score"], 85);

        assert_eq!(limit_arr[2]["json"]["id"], 5);
        assert_eq!(limit_arr[2]["json"]["name"], "Task 5");
        assert_eq!(limit_arr[2]["json"]["score"], 60);

        // 1b. Test Limit dengan keep: "lastItems" dari input 5 item
        let five_items = json!([
            { "id": 101, "name": "Item 1" },
            { "id": 102, "name": "Item 2" },
            { "id": 103, "name": "Item 3" },
            { "id": 104, "name": "Item 4" },
            { "id": 105, "name": "Item 5" }
        ]);

        let limit_last_wf = json!({
            "name": "Limit Keep LastItems Test Pipeline",
            "nodes": [
                {
                    "id": "node-trigger-limit-last",
                    "name": "TriggerLimitLast",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "node-limit-last",
                    "name": "LimitLast2",
                    "type": "n8n-nodes-base.limit",
                    "parameters": {
                        "maxItems": 2,
                        "keep": "lastItems"
                    }
                }
            ],
            "connections": {
                "TriggerLimitLast": {
                    "main": [
                        [ { "node": "LimitLast2", "type": "main", "index": 0 } ]
                    ]
                }
            },
            "pinData": {
                "TriggerLimitLast": five_items
            }
        });

        let res_limit_last = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(limit_last_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_limit_last.status(), StatusCode::OK);
        let bytes_ll = res_limit_last.into_body().collect().await.unwrap().to_bytes();
        let res_ll_json: serde_json::Value = serde_json::from_slice(&bytes_ll).unwrap();
        assert_eq!(
            res_ll_json["data"]["status"], "success",
            "Limit lastItems pipeline execution status must be success"
        );

        let limit_last_out = &res_ll_json["data"]["resultData"]["runData"]["LimitLast2"][0]["data"]["main"][0];
        let limit_last_arr = limit_last_out.as_array().expect("LimitLast2 output must be an array");
        assert_eq!(limit_last_arr.len(), 2, "LimitLast2 must return exactly 2 items");
        assert_eq!(limit_last_arr[0]["json"]["id"], 104, "First of last 2 items must be id 104");
        assert_eq!(limit_last_arr[1]["json"]["id"], 105, "Second of last 2 items must be id 105");

        // 1c. Test Filter dengan operator: "exists" untuk field "score"
        let filter_exists_items = json!([
            { "id": 1, "name": "Alpha", "score": 100 },
            { "id": 2, "name": "Beta" },
            { "id": 3, "name": "Gamma", "score": 250 },
            { "id": 4, "name": "Delta", "score": null },
            { "id": 5, "name": "Epsilon", "score": 300 }
        ]);

        let filter_exists_wf = json!({
            "name": "Filter Exists Score Test Pipeline",
            "nodes": [
                {
                    "id": "node-trigger-filter-exists",
                    "name": "TriggerFilterExists",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "node-filter-exists",
                    "name": "FilterScoreExists",
                    "type": "n8n-nodes-base.filter",
                    "parameters": {
                        "field": "score",
                        "operator": "exists"
                    }
                }
            ],
            "connections": {
                "TriggerFilterExists": {
                    "main": [
                        [ { "node": "FilterScoreExists", "type": "main", "index": 0 } ]
                    ]
                }
            },
            "pinData": {
                "TriggerFilterExists": filter_exists_items
            }
        });

        let res_filter_exists = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(filter_exists_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_filter_exists.status(), StatusCode::OK);
        let bytes_fe = res_filter_exists.into_body().collect().await.unwrap().to_bytes();
        let res_fe_json: serde_json::Value = serde_json::from_slice(&bytes_fe).unwrap();
        assert_eq!(
            res_fe_json["data"]["status"], "success",
            "Filter exists pipeline execution status must be success"
        );

        let filter_fe_out = &res_fe_json["data"]["resultData"]["runData"]["FilterScoreExists"][0]["data"]["main"][0];
        let filter_fe_arr = filter_fe_out.as_array().expect("FilterScoreExists output must be an array");
        assert_eq!(filter_fe_arr.len(), 3, "FilterScoreExists must return exactly 3 items with existing score");
        assert_eq!(filter_fe_arr[0]["json"]["id"], 1);
        assert_eq!(filter_fe_arr[0]["json"]["score"], 100);
        assert_eq!(filter_fe_arr[1]["json"]["id"], 3);
        assert_eq!(filter_fe_arr[1]["json"]["score"], 250);
        assert_eq!(filter_fe_arr[2]["json"]["id"], 5);
        assert_eq!(filter_fe_arr[2]["json"]["score"], 300);

        // 2. Multi-branch Switch Routing + Downstream Merge Node
        let switch_input_data = json!({ "tier": "silver", "amount": 250 });

        let switch_wf = json!({
            "name": "Switch Routing and Merge Test Pipeline",
            "nodes": [
                {
                    "id": "node-trigger-2",
                    "name": "Trigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "node-switch-1",
                    "name": "SwitchTier",
                    "type": "n8n-nodes-base.switch",
                    "parameters": {
                        "fallbackOutput": 2,
                        "rules": [
                            { "field": "tier", "operator": "equals", "value": "bronze", "outputPort": 0 },
                            { "field": "tier", "operator": "equals", "value": "silver", "outputPort": 1 },
                            { "field": "tier", "operator": "equals", "value": "gold", "outputPort": 2 }
                        ]
                    }
                },
                {
                    "id": "node-branch-bronze",
                    "name": "BranchBronze",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "reward": "bronze_medal"
                    }
                },
                {
                    "id": "node-branch-silver",
                    "name": "BranchSilver",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "reward": "silver_medal"
                    }
                },
                {
                    "id": "node-branch-gold",
                    "name": "BranchGold",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "reward": "gold_medal"
                    }
                },
                {
                    "id": "node-merge-1",
                    "name": "MergeResults",
                    "type": "n8n-nodes-base.merge",
                    "parameters": {}
                }
            ],
            "connections": {
                "Trigger": {
                    "main": [
                        [ { "node": "SwitchTier", "type": "main", "index": 0 } ]
                    ]
                },
                "SwitchTier": {
                    "main": [
                        [ { "node": "BranchBronze", "type": "main", "index": 0 } ],
                        [ { "node": "BranchSilver", "type": "main", "index": 0 } ],
                        [ { "node": "BranchGold", "type": "main", "index": 0 } ]
                    ]
                },
                "BranchBronze": {
                    "main": [
                        [ { "node": "MergeResults", "type": "main", "index": 0 } ]
                    ]
                },
                "BranchSilver": {
                    "main": [
                        [ { "node": "MergeResults", "type": "main", "index": 0 } ]
                    ]
                },
                "BranchGold": {
                    "main": [
                        [ { "node": "MergeResults", "type": "main", "index": 0 } ]
                    ]
                }
            },
            "pinData": {
                "Trigger": [ { "json": switch_input_data } ]
            }
        });

        let res_switch = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(switch_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_switch.status(), StatusCode::OK);
        let bytes_s = res_switch.into_body().collect().await.unwrap().to_bytes();
        let res_s_json: serde_json::Value = serde_json::from_slice(&bytes_s).unwrap();
        assert_eq!(
            res_s_json["data"]["status"], "success",
            "Switch and Merge workflow execution status must be success"
        );

        let run_data_s = &res_s_json["data"]["resultData"]["runData"];

        // Verifikasi port 1 (Silver) dieksekusi, cabang bronze dan gold dipangkas (pruned)
        assert!(
            run_data_s.get("BranchSilver").is_some(),
            "BranchSilver must be executed for silver tier"
        );
        assert!(
            run_data_s.get("BranchBronze").is_none(),
            "BranchBronze must be pruned for silver tier"
        );
        assert!(
            run_data_s.get("BranchGold").is_none(),
            "BranchGold must be pruned for silver tier"
        );

        // Verifikasi output MergeResults memuat reward "silver_medal" tanpa terjadi deadlock pada Merge node!
        assert!(
            run_data_s.get("MergeResults").is_some(),
            "MergeResults must be executed without deadlock"
        );
        let merge_out = &run_data_s["MergeResults"][0]["data"]["main"][0];
        let merge_arr = merge_out.as_array().expect("MergeResults output must be an array");
        assert!(!merge_arr.is_empty(), "MergeResults must contain merged output");
        let silver_found = merge_arr.iter().any(|item| {
            item["json"]["reward"] == "silver_medal"
        });
        assert!(
            silver_found,
            "MergeResults output must contain reward 'silver_medal', got: {:?}",
            merge_arr
        );

        // 3. Switch fallbackOutput Routing Test (Platinum -> fallbackOutput: 2 -> BranchGold)
        let switch_fallback_input = json!({ "tier": "platinum", "amount": 999 });

        let switch_fallback_wf = json!({
            "name": "Switch Fallback Output Test Pipeline",
            "nodes": [
                {
                    "id": "node-trigger-fb",
                    "name": "Trigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "node-switch-fb",
                    "name": "SwitchTier",
                    "type": "n8n-nodes-base.switch",
                    "parameters": {
                        "fallbackOutput": 2,
                        "rules": [
                            { "field": "tier", "operator": "equals", "value": "bronze", "outputPort": 0 },
                            { "field": "tier", "operator": "equals", "value": "silver", "outputPort": 1 },
                            { "field": "tier", "operator": "equals", "value": "gold", "outputPort": 2 }
                        ]
                    }
                },
                {
                    "id": "node-branch-bronze-fb",
                    "name": "BranchBronze",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "reward": "bronze_medal"
                    }
                },
                {
                    "id": "node-branch-silver-fb",
                    "name": "BranchSilver",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "reward": "silver_medal"
                    }
                },
                {
                    "id": "node-branch-gold-fb",
                    "name": "BranchGold",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "reward": "gold_medal"
                    }
                },
                {
                    "id": "node-merge-fb",
                    "name": "MergeResults",
                    "type": "n8n-nodes-base.merge",
                    "parameters": {}
                }
            ],
            "connections": {
                "Trigger": {
                    "main": [
                        [ { "node": "SwitchTier", "type": "main", "index": 0 } ]
                    ]
                },
                "SwitchTier": {
                    "main": [
                        [ { "node": "BranchBronze", "type": "main", "index": 0 } ],
                        [ { "node": "BranchSilver", "type": "main", "index": 0 } ],
                        [ { "node": "BranchGold", "type": "main", "index": 0 } ]
                    ]
                },
                "BranchBronze": {
                    "main": [
                        [ { "node": "MergeResults", "type": "main", "index": 0 } ]
                    ]
                },
                "BranchSilver": {
                    "main": [
                        [ { "node": "MergeResults", "type": "main", "index": 0 } ]
                    ]
                },
                "BranchGold": {
                    "main": [
                        [ { "node": "MergeResults", "type": "main", "index": 0 } ]
                    ]
                }
            },
            "pinData": {
                "Trigger": [ { "json": switch_fallback_input } ]
            }
        });

        let res_fallback = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(switch_fallback_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_fallback.status(), StatusCode::OK);
        let bytes_fb = res_fallback.into_body().collect().await.unwrap().to_bytes();
        let res_fb_json: serde_json::Value = serde_json::from_slice(&bytes_fb).unwrap();
        assert_eq!(
            res_fb_json["data"]["status"], "success",
            "Switch fallback workflow execution status must be success"
        );

        let run_data_fb = &res_fb_json["data"]["resultData"]["runData"];

        // Verifikasi bahwa fallbackOutput: 2 mengalihkan routing ke BranchGold (port 2)
        assert!(
            run_data_fb.get("BranchGold").is_some(),
            "BranchGold must be executed when tier is platinum via fallbackOutput: 2"
        );
        assert!(
            run_data_fb.get("BranchBronze").is_none(),
            "BranchBronze must be pruned for platinum tier fallback"
        );
        assert!(
            run_data_fb.get("BranchSilver").is_none(),
            "BranchSilver must be pruned for platinum tier fallback"
        );

        // Verifikasi output MergeResults memuat reward "gold_medal" dari BranchGold
        assert!(
            run_data_fb.get("MergeResults").is_some(),
            "MergeResults must be executed following fallbackOutput to BranchGold"
        );
        let merge_fb_out = &run_data_fb["MergeResults"][0]["data"]["main"][0];
        let merge_fb_arr = merge_fb_out.as_array().expect("MergeResults output must be an array");
        assert!(!merge_fb_arr.is_empty(), "MergeResults must contain merged output for fallback run");
        let gold_found = merge_fb_arr.iter().any(|item| {
            item["json"]["reward"] == "gold_medal"
        });
        assert!(
            gold_found,
            "MergeResults output must contain reward 'gold_medal' via fallbackOutput, got: {:?}",
            merge_fb_arr
        );

        // 4. Test PinData Override & run_workflow_by_id via DB persistence
        let override_wf_def = json!({
            "name": "PinData Override Persistence Test",
            "nodes": [
                {
                    "id": "node-override-trig",
                    "name": "TriggerOverride",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {
                        "data": [{ "id": 999 }]
                    }
                },
                {
                    "id": "node-override-set",
                    "name": "SetCapture",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "captured_id": "={{ $json.id }}"
                    }
                }
            ],
            "connections": {
                "TriggerOverride": {
                    "main": [
                        [ { "node": "SetCapture", "type": "main", "index": 0 } ]
                    ]
                }
            }
        });

        let res_create_override = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(override_wf_def.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_create_override.status(), StatusCode::OK);
        let bytes_co = res_create_override.into_body().collect().await.unwrap().to_bytes();
        let res_co_json: serde_json::Value = serde_json::from_slice(&bytes_co).unwrap();
        let wf_override_id = res_co_json["data"]["id"]
            .as_str()
            .expect("Created workflow must have an id");

        // Run workflow via POST /rest/workflows/{id}/run with new pinData overriding old 999
        let run_override_payload = json!({
            "pinData": {
                "TriggerOverride": [
                    { "id": 888, "override": true }
                ]
            }
        });

        let res_run_override = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{}/run", wf_override_id))
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(run_override_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run_override.status(), StatusCode::OK);
        let bytes_ro = res_run_override.into_body().collect().await.unwrap().to_bytes();
        let res_ro_json: serde_json::Value = serde_json::from_slice(&bytes_ro).unwrap();
        assert_eq!(
            res_ro_json["data"]["status"], "success",
            "Workflow execution with overridden pinData must succeed"
        );

        let override_out = &res_ro_json["data"]["resultData"]["runData"]["SetCapture"][0]["data"]["main"][0];
        let override_arr = override_out.as_array().expect("SetCapture output must be an array");
        assert_eq!(override_arr.len(), 1, "Must contain exactly 1 output item");
        assert_eq!(override_arr[0]["json"]["id"], 888, "pinData must override 999 with 888");
        assert_eq!(override_arr[0]["json"]["override"], true, "pinData fields must be present");
        assert_eq!(override_arr[0]["json"]["captured_id"], 888, "Expression captured_id must evaluate to 888");

        // 5. Structure "conditions" on Filter and Switch + Per-Item Expression on Set
        // 5a. Filter using "conditions": { "number": [{ "value1": "={{ $json.score }}", "operation": "largerEqual", "value2": 80 }] }
        let filter_cond_items = json!([
            { "id": 10, "score": 75 },
            { "id": 20, "score": 85 },
            { "id": 30, "score": 60 },
            { "id": 40, "score": 92 }
        ]);

        let filter_cond_wf = json!({
            "name": "Filter Conditions Number Test Pipeline",
            "nodes": [
                {
                    "id": "node-trigger-fc",
                    "name": "TriggerFilterCond",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "node-filter-cond",
                    "name": "FilterScoreLargerEqual80",
                    "type": "n8n-nodes-base.filter",
                    "parameters": {
                        "conditions": {
                            "number": [
                                {
                                    "value1": "={{ $json.score }}",
                                    "operation": "largerEqual",
                                    "value2": 80
                                }
                            ]
                        }
                    }
                }
            ],
            "connections": {
                "TriggerFilterCond": {
                    "main": [
                        [ { "node": "FilterScoreLargerEqual80", "type": "main", "index": 0 } ]
                    ]
                }
            },
            "pinData": {
                "TriggerFilterCond": filter_cond_items
            }
        });

        let res_fc = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(filter_cond_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_fc.status(), StatusCode::OK);
        let bytes_fc = res_fc.into_body().collect().await.unwrap().to_bytes();
        let res_fc_json: serde_json::Value = serde_json::from_slice(&bytes_fc).unwrap();
        assert_eq!(
            res_fc_json["data"]["status"], "success",
            "Filter conditions pipeline execution status must be success"
        );

        let fc_out = &res_fc_json["data"]["resultData"]["runData"]["FilterScoreLargerEqual80"][0]["data"]["main"][0];
        let fc_arr = fc_out.as_array().expect("FilterScoreLargerEqual80 output must be an array");
        assert_eq!(fc_arr.len(), 2, "Filter must return exactly 2 items with score >= 80");
        assert_eq!(fc_arr[0]["json"]["id"], 20);
        assert_eq!(fc_arr[0]["json"]["score"], 85);
        assert_eq!(fc_arr[1]["json"]["id"], 40);
        assert_eq!(fc_arr[1]["json"]["score"], 92);

        // 5b. Switch using "rules": [{ "conditions": { "string": [{ "value1": "={{ $json.tier }}", "operation": "equals", "value2": "vip" }] }, "outputPort": 0 }]
        let switch_cond_input = json!([
            { "tier": "vip", "user": "alice" }
        ]);

        let switch_cond_wf = json!({
            "name": "Switch Conditions Test Pipeline",
            "nodes": [
                {
                    "id": "node-trigger-sc",
                    "name": "TriggerSwitchCond",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "node-switch-cond",
                    "name": "SwitchVip",
                    "type": "n8n-nodes-base.switch",
                    "parameters": {
                        "fallbackOutput": 1,
                        "rules": [
                            {
                                "conditions": {
                                    "string": [
                                        {
                                            "value1": "={{ $json.tier }}",
                                            "operation": "equals",
                                            "value2": "vip"
                                        }
                                    ]
                                },
                                "outputPort": 0
                            }
                        ]
                    }
                },
                {
                    "id": "node-branch-vip",
                    "name": "BranchVip",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "status": "vip_access"
                    }
                },
                {
                    "id": "node-branch-regular",
                    "name": "BranchRegular",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "status": "standard_access"
                    }
                }
            ],
            "connections": {
                "TriggerSwitchCond": {
                    "main": [
                        [ { "node": "SwitchVip", "type": "main", "index": 0 } ]
                    ]
                },
                "SwitchVip": {
                    "main": [
                        [ { "node": "BranchVip", "type": "main", "index": 0 } ],
                        [ { "node": "BranchRegular", "type": "main", "index": 0 } ]
                    ]
                }
            },
            "pinData": {
                "TriggerSwitchCond": switch_cond_input
            }
        });

        let res_sc = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(switch_cond_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_sc.status(), StatusCode::OK);
        let bytes_sc = res_sc.into_body().collect().await.unwrap().to_bytes();
        let res_sc_json: serde_json::Value = serde_json::from_slice(&bytes_sc).unwrap();
        assert_eq!(
            res_sc_json["data"]["status"], "success",
            "Switch conditions workflow execution status must be success"
        );

        let run_data_sc = &res_sc_json["data"]["resultData"]["runData"];
        assert!(
            run_data_sc.get("BranchVip").is_some(),
            "BranchVip must be executed when tier == vip via rule conditions"
        );
        assert!(
            run_data_sc.get("BranchRegular").is_none(),
            "BranchRegular must be pruned when rule 0 matches"
        );
        let vip_out = &run_data_sc["BranchVip"][0]["data"]["main"][0];
        let vip_arr = vip_out.as_array().expect("BranchVip output must be array");
        assert_eq!(vip_arr[0]["json"]["status"], "vip_access");

        // 5c. Set node receiving Value::Array (>1 element) and evaluating per-item expression "computed_label": "Item-{{ $json.id }}"
        let set_array_items = json!([
            { "id": 1 },
            { "id": 2 }
        ]);

        let set_array_wf = json!({
            "name": "Set Per-Item Expression Array Test Pipeline",
            "nodes": [
                {
                    "id": "node-trigger-set-arr",
                    "name": "TriggerSetArr",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {}
                },
                {
                    "id": "node-set-per-item",
                    "name": "SetPerItemLabel",
                    "type": "n8n-nodes-base.set",
                    "parameters": {
                        "computed_label": "Item-{{ $json.id }}"
                    }
                }
            ],
            "connections": {
                "TriggerSetArr": {
                    "main": [
                        [ { "node": "SetPerItemLabel", "type": "main", "index": 0 } ]
                    ]
                }
            },
            "pinData": {
                "TriggerSetArr": set_array_items
            }
        });

        let res_sa = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows/run")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(set_array_wf.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_sa.status(), StatusCode::OK);
        let bytes_sa = res_sa.into_body().collect().await.unwrap().to_bytes();
        let res_sa_json: serde_json::Value = serde_json::from_slice(&bytes_sa).unwrap();
        assert_eq!(
            res_sa_json["data"]["status"], "success",
            "Set per-item expression array pipeline execution status must be success"
        );

        let sa_out = &res_sa_json["data"]["resultData"]["runData"]["SetPerItemLabel"][0]["data"]["main"][0];
        let sa_arr = sa_out.as_array().expect("SetPerItemLabel output must be an array");
        assert_eq!(sa_arr.len(), 2, "SetPerItemLabel must preserve both array elements");
        assert_eq!(
            sa_arr[0]["json"]["computed_label"], "Item-1",
            "Element 0 must evaluate expression to 'Item-1'"
        );
        assert_eq!(
            sa_arr[1]["json"]["computed_label"], "Item-2",
            "Element 1 must evaluate expression to 'Item-2'"
        );

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 25 timed out!");
}

// ---------------------------------------------------------------------------
// 26. Polyglot Code Node, Download/Delete Runtime & Dynamic Manager Test
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_polyglot_code_node_and_dynamic_runtime_manager() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (db, db_path) = setup_test_engine();
        let app = build_app(Path::new("ui-dist"), db);

        // ===================================================================
        // Skenario 1: List Runtimes Contract
        // ===================================================================
        let res_list = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/runtimes")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_list.status(), StatusCode::OK, "GET /rest/runtimes must return 200 OK");
        let bytes_list = res_list.into_body().collect().await.unwrap().to_bytes();
        let body_list: serde_json::Value = serde_json::from_slice(&bytes_list).unwrap();
        let runtimes = body_list["data"].as_array().expect("runtimes 'data' must be an array");

        // Verify default runtimes
        let js_rt = runtimes
            .iter()
            .find(|r| r["language"] == "javascript" && r["version"] == "es2022-quickjs")
            .expect("Default javascript runtime 'es2022-quickjs' must be present");
        assert_eq!(js_rt["installed"], true, "javascript:es2022-quickjs must be installed");

        let py_rt = runtimes
            .iter()
            .find(|r| r["language"] == "python" && r["version"] == "3.11-sandbox")
            .expect("Default python runtime '3.11-sandbox' must be present");
        assert_eq!(py_rt["installed"], true, "python:3.11-sandbox must be installed");

        let wasm_rt = runtimes
            .iter()
            .find(|r| r["language"] == "wasm" && r["version"] == "wasm32-wasmer-4.0")
            .expect("Default wasm runtime 'wasm32-wasmer-4.0' must be present");
        assert_eq!(wasm_rt["installed"], true, "wasm:wasm32-wasmer-4.0 must be installed");

        // ===================================================================
        // Skenario 2: Download Versi Baru & Delete Versi Lama
        // ===================================================================
        // 1. Download python:3.13-jit
        let dl_payload = json!({
            "language": "python",
            "version": "3.13-jit"
        });

        let res_dl = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/runtimes/download")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(dl_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_dl.status(), StatusCode::OK, "POST /rest/runtimes/download must return 200 OK");
        let bytes_dl = res_dl.into_body().collect().await.unwrap().to_bytes();
        let body_dl: serde_json::Value = serde_json::from_slice(&bytes_dl).unwrap();
        assert_eq!(
            body_dl["data"]["installed"], true,
            "Downloaded runtime data must have installed == true"
        );

        // 2. Verify GET /rest/runtimes: 3.13-jit is registered with installed == true
        let res_list_after_dl = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/runtimes")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_list_after_dl.status(), StatusCode::OK);
        let bytes_lad = res_list_after_dl.into_body().collect().await.unwrap().to_bytes();
        let body_lad: serde_json::Value = serde_json::from_slice(&bytes_lad).unwrap();
        let runtimes_lad = body_lad["data"].as_array().expect("runtimes must be array");
        let py_13 = runtimes_lad
            .iter()
            .find(|r| r["language"] == "python" && r["version"] == "3.13-jit")
            .expect("python:3.13-jit must be registered after download");
        assert_eq!(py_13["installed"], true, "python:3.13-jit must have installed == true");

        // 3. Delete python:3.11-sandbox
        let res_del = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/runtimes/python/3.11-sandbox")
                    .method("DELETE")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_del.status(), StatusCode::OK, "DELETE /rest/runtimes/python/3.11-sandbox must return 200 OK");

        // 4. Verify GET /rest/runtimes: 3.11-sandbox is installed == false or not present
        let res_list_after_del = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/runtimes")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_list_after_del.status(), StatusCode::OK);
        let bytes_ld = res_list_after_del.into_body().collect().await.unwrap().to_bytes();
        let body_ld: serde_json::Value = serde_json::from_slice(&bytes_ld).unwrap();
        let runtimes_ld = body_ld["data"].as_array().expect("runtimes must be array");
        let py_11_opt = runtimes_ld
            .iter()
            .find(|r| r["language"] == "python" && r["version"] == "3.11-sandbox");
        if let Some(py_11) = py_11_opt {
            assert_eq!(
                py_11["installed"], false,
                "python:3.11-sandbox must be uninstalled (installed == false)"
            );
        }

        // ===================================================================
        // Skenario 3: Eksekusi Nyata Code Node Multi-Bahasa
        // ===================================================================

        // -------------------------------------------------------------------
        // 3a. JavaScript Code Node
        // -------------------------------------------------------------------
        let js_wf_payload = json!({
            "name": "Polyglot JavaScript Code Pipeline",
            "nodes": [
                {
                    "id": "node-js-trig",
                    "name": "manualTrigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {
                        "data": [{ "json": { "price": 10, "qty": 5 } }]
                    }
                },
                {
                    "id": "node-js-code",
                    "name": "Code",
                    "type": "n8n-nodes-base.code",
                    "parameters": {
                        "language": "javascript",
                        "runtimeVersion": "es2022-quickjs",
                        "jsCode": "item.total = item.price * item.qty;\nreturn item;"
                    }
                }
            ],
            "connections": {
                "manualTrigger": {
                    "main": [
                        [ { "node": "Code", "type": "main", "index": 0 } ]
                    ]
                }
            }
        });

        let res_create_js = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(js_wf_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_create_js.status(), StatusCode::OK);
        let bytes_cjs = res_create_js.into_body().collect().await.unwrap().to_bytes();
        let body_cjs: serde_json::Value = serde_json::from_slice(&bytes_cjs).unwrap();
        let js_wf_id = body_cjs["data"]["id"]
            .as_str()
            .expect("Created JS workflow must have an id");

        let run_js_payload = json!({
            "pinData": {
                "manualTrigger": [
                    { "json": { "price": 10, "qty": 5 } }
                ]
            }
        });

        let res_run_js = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{js_wf_id}/run"))
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(run_js_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run_js.status(), StatusCode::OK);
        let bytes_rjs = res_run_js.into_body().collect().await.unwrap().to_bytes();
        let body_rjs: serde_json::Value = serde_json::from_slice(&bytes_rjs).unwrap();
        assert_eq!(
            body_rjs["data"]["status"], "success",
            "JavaScript Code workflow execution must succeed"
        );

        let js_out = &body_rjs["data"]["resultData"]["runData"]["Code"][0]["data"]["main"][0];
        let js_arr = js_out.as_array().expect("Code output must be an array");
        assert!(!js_arr.is_empty(), "Code output must not be empty");
        let js_item = &js_arr[0];

        let total_val = js_item
            .get("total")
            .or_else(|| js_item.get("json").and_then(|j| j.get("total")));
        assert_eq!(total_val, Some(&json!(50)), "item total must equal 50");

        let js_code_exec = js_item
            .get("codeExecuted")
            .or_else(|| js_item.get("json").and_then(|j| j.get("codeExecuted")));
        assert_eq!(js_code_exec, Some(&json!(true)), "codeExecuted must be true");

        let js_runtime_used = js_item
            .get("runtimeUsed")
            .or_else(|| js_item.get("json").and_then(|j| j.get("runtimeUsed")));
        assert_eq!(
            js_runtime_used,
            Some(&json!("javascript:es2022-quickjs")),
            "runtimeUsed must be 'javascript:es2022-quickjs'"
        );

        // -------------------------------------------------------------------
        // 3b. Python Code Node
        // -------------------------------------------------------------------
        let py_wf_payload = json!({
            "name": "Polyglot Python Code Pipeline",
            "nodes": [
                {
                    "id": "node-py-trig",
                    "name": "manualTrigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {
                        "data": [{ "json": { "price": 100 } }]
                    }
                },
                {
                    "id": "node-py-code",
                    "name": "Code",
                    "type": "n8n-nodes-base.code",
                    "parameters": {
                        "language": "python",
                        "runtimeVersion": "3.13-jit",
                        "pythonCode": "item['tax'] = item['price'] * 0.1\nreturn item"
                    }
                }
            ],
            "connections": {
                "manualTrigger": {
                    "main": [
                        [ { "node": "Code", "type": "main", "index": 0 } ]
                    ]
                }
            }
        });

        let res_create_py = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(py_wf_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_create_py.status(), StatusCode::OK);
        let bytes_cpy = res_create_py.into_body().collect().await.unwrap().to_bytes();
        let body_cpy: serde_json::Value = serde_json::from_slice(&bytes_cpy).unwrap();
        let py_wf_id = body_cpy["data"]["id"]
            .as_str()
            .expect("Created Python workflow must have an id");

        let run_py_payload = json!({
            "pinData": {
                "manualTrigger": [
                    { "json": { "price": 100 } }
                ]
            }
        });

        let res_run_py = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{py_wf_id}/run"))
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(run_py_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run_py.status(), StatusCode::OK);
        let bytes_rpy = res_run_py.into_body().collect().await.unwrap().to_bytes();
        let body_rpy: serde_json::Value = serde_json::from_slice(&bytes_rpy).unwrap();
        assert_eq!(
            body_rpy["data"]["status"], "success",
            "Python Code workflow execution must succeed"
        );

        let py_out = &body_rpy["data"]["resultData"]["runData"]["Code"][0]["data"]["main"][0];
        let py_arr = py_out.as_array().expect("Code output must be an array");
        assert!(!py_arr.is_empty(), "Python Code output must not be empty");
        let py_item = &py_arr[0];

        let tax_val = py_item
            .get("tax")
            .or_else(|| py_item.get("json").and_then(|j| j.get("tax")));
        let is_tax_10 = tax_val == Some(&json!(10.0))
            || tax_val == Some(&json!(10))
            || tax_val == Some(&json!(10.0f64));
        assert!(
            is_tax_10,
            "tax must be 10.0 or 10, got: {:?}",
            tax_val
        );

        let py_code_exec = py_item
            .get("codeExecuted")
            .or_else(|| py_item.get("json").and_then(|j| j.get("codeExecuted")));
        assert_eq!(py_code_exec, Some(&json!(true)), "Python codeExecuted must be true");

        let py_runtime_used = py_item
            .get("runtimeUsed")
            .or_else(|| py_item.get("json").and_then(|j| j.get("runtimeUsed")));
        assert_eq!(
            py_runtime_used,
            Some(&json!("python:3.13-jit")),
            "Python runtimeUsed must be 'python:3.13-jit'"
        );

        // -------------------------------------------------------------------
        // 3c. WASM Code Node
        // -------------------------------------------------------------------
        let wasm_wf_payload = json!({
            "name": "Polyglot WASM Code Pipeline",
            "nodes": [
                {
                    "id": "node-wasm-trig",
                    "name": "manualTrigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "parameters": {
                        "data": [{ "json": { "id": 42 } }]
                    }
                },
                {
                    "id": "node-wasm-code",
                    "name": "Code",
                    "type": "n8n-nodes-base.code",
                    "parameters": {
                        "language": "wasm",
                        "runtimeVersion": "wasm32-wasmer-4.0",
                        "wasmCode": "transform"
                    }
                }
            ],
            "connections": {
                "manualTrigger": {
                    "main": [
                        [ { "node": "Code", "type": "main", "index": 0 } ]
                    ]
                }
            }
        });

        let res_create_wasm = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/rest/workflows")
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(wasm_wf_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_create_wasm.status(), StatusCode::OK);
        let bytes_cwasm = res_create_wasm.into_body().collect().await.unwrap().to_bytes();
        let body_cwasm: serde_json::Value = serde_json::from_slice(&bytes_cwasm).unwrap();
        let wasm_wf_id = body_cwasm["data"]["id"]
            .as_str()
            .expect("Created WASM workflow must have an id");

        let run_wasm_payload = json!({
            "pinData": {
                "manualTrigger": [
                    { "json": { "id": 42 } }
                ]
            }
        });

        let res_run_wasm = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/rest/workflows/{wasm_wf_id}/run"))
                    .method("POST")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(run_wasm_payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_run_wasm.status(), StatusCode::OK);
        let bytes_rwasm = res_run_wasm.into_body().collect().await.unwrap().to_bytes();
        let body_rwasm: serde_json::Value = serde_json::from_slice(&bytes_rwasm).unwrap();
        assert_eq!(
            body_rwasm["data"]["status"], "success",
            "WASM Code workflow execution must succeed"
        );

        let wasm_out = &body_rwasm["data"]["resultData"]["runData"]["Code"][0]["data"]["main"][0];
        let wasm_arr = wasm_out.as_array().expect("WASM Code output must be an array");
        assert!(!wasm_arr.is_empty(), "WASM Code output must not be empty");
        let wasm_item = &wasm_arr[0];

        let wasm_code_exec = wasm_item
            .get("codeExecuted")
            .or_else(|| wasm_item.get("json").and_then(|j| j.get("codeExecuted")));
        assert_eq!(wasm_code_exec, Some(&json!(true)), "WASM codeExecuted must be true");

        let wasm_runtime_used = wasm_item
            .get("runtimeUsed")
            .or_else(|| wasm_item.get("json").and_then(|j| j.get("runtimeUsed")));
        assert_eq!(
            wasm_runtime_used,
            Some(&json!("wasm:wasm32-wasmer-4.0")),
            "WASM runtimeUsed must be 'wasm:wasm32-wasmer-4.0'"
        );

        let _ = std::fs::remove_file(db_path);
    })
    .await
    .expect("Test 26 timed out!");
}
