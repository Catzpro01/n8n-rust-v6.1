use crate::db::{
    new_id, CommunityPackageRecord, CredentialRecord, ExecutionWriteMessage, FolderRecord,
    StorageEngine, TagRecord, VariableRecord, WorkflowRecord,
};
use crate::engine::{CompiledGraph, ExecutionNotifier, HyperScaleEngine};
use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{header, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{delete, get, patch, post},
    Json, Router,
};
use chrono::Utc;
use dashmap::{DashMap, DashSet};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Real-Time Push Hub & Execution Notifier
// ---------------------------------------------------------------------------

pub struct PushHub {
    sender: tokio::sync::broadcast::Sender<String>,
}

impl Default for PushHub {
    fn default() -> Self {
        Self::new()
    }
}

impl PushHub {
    pub fn new() -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(1024);
        Self { sender }
    }

    pub fn broadcast(&self, event: &Value) {
        if let Ok(serialized) = serde_json::to_string(event) {
            let _ = self.sender.send(serialized);
        }
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.sender.subscribe()
    }
}

pub struct PushHubNotifier {
    hub: Arc<PushHub>,
}

impl PushHubNotifier {
    pub fn new(hub: Arc<PushHub>) -> Self {
        Self { hub }
    }
}

impl ExecutionNotifier for PushHubNotifier {
    fn notify_started(&self, execution_id: &str, workflow_id: Option<&str>) {
        self.hub.broadcast(&json!({
            "type": "executionStarted",
            "data": {
                "executionId": execution_id,
                "workflowId": workflow_id
            }
        }));
    }

    fn notify_node_before(&self, execution_id: &str, node_name: &str) {
        self.hub.broadcast(&json!({
            "type": "nodeExecuteBefore",
            "data": {
                "executionId": execution_id,
                "nodeName": node_name
            }
        }));
    }

    fn notify_node_after(&self, execution_id: &str, node_name: &str, output: &Value) {
        self.hub.broadcast(&json!({
            "type": "nodeExecuteAfter",
            "data": {
                "executionId": execution_id,
                "nodeName": node_name,
                "data": output
            }
        }));
    }

    fn notify_finished(&self, execution_id: &str, status: &str, duration_ms: u64) {
        self.hub.broadcast(&json!({
            "type": "executionFinished",
            "data": {
                "executionId": execution_id,
                "status": status,
                "durationMs": duration_ms
            }
        }));
    }
}

#[derive(Clone)]
pub struct AppState {
    pub db: StorageEngine,
    pub engine: HyperScaleEngine,
    pub push_hub: Arc<PushHub>,
    pub active_executions: Arc<DashMap<String, Arc<AtomicBool>>>,
}

// ---------------------------------------------------------------------------
// Health & Settings Endpoints
// ---------------------------------------------------------------------------

pub async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// 100% Unlocked Enterprise & AI Settings payload for Vue editor-ui
pub const SETTINGS_ENTERPRISE_PAYLOAD: &str = r#"{
  "data": {
    "settingsMode": "authenticated",
    "inE2ETests": false,
    "isDocker": false,
    "databaseType": "sqlite",
    "previewMode": false,
    "endpointForm": "form",
    "endpointFormTest": "form-test",
    "endpointFormWaiting": "form-waiting",
    "endpointMcp": "mcp",
    "endpointMcpTest": "mcp-test",
    "endpointWebhook": "webhook",
    "endpointWebhookTest": "webhook-test",
    "endpointWebhookWaiting": "webhook-waiting",
    "endpointHealth": "healthz",
    "saveDataErrorExecution": "all",
    "saveDataSuccessExecution": "all",
    "saveManualExecutions": true,
    "saveExecutionProgress": true,
    "executionTimeout": -1,
    "maxExecutionTimeout": 3600,
    "workflowCallerPolicyDefaultOption": "workflowsFromSameOwner",
    "excludeNodes": [],
    "timezone": "UTC",
    "urlBaseWebhook": "http://localhost:5678/",
    "urlBaseEditor": "http://localhost:5678/",
    "urlBaseWebhookTest": "http://localhost:5678/",
    "binaryDataMode": "default",
    "nodeJsVersion": "20.18.0",
    "nodeEnv": "production",
    "versionCli": "2.43.5",
    "concurrency": -1,
    "evaluationConcurrencyLimit": 10,
    "authCookie": {
      "secure": false
    },
    "releaseChannel": "stable",
    "oauthCallbackUrls": {
      "oauth1": "http://localhost:5678/rest/oauth1-credential/callback",
      "oauth2": "http://localhost:5678/rest/oauth2-credential/callback"
    },
    "jwksUri": "http://localhost:5678/rest/.well-known/jwks.json",
    "versionNotifications": {
      "enabled": false,
      "endpoint": "",
      "whatsNewEnabled": false,
      "whatsNewEndpoint": "",
      "infoUrl": ""
    },
    "dynamicBanners": {
      "endpoint": "",
      "enabled": false,
      "filters": {
        "publishedWorkflowCount": 0
      }
    },
    "instanceId": "n8n-rust-v5-enterprise",
    "telemetry": {
      "enabled": false
    },
    "posthog": {
      "enabled": false,
      "apiHost": "",
      "apiKey": "",
      "autocapture": false,
      "disableSessionRecording": true,
      "proxy": "http://localhost:5678/rest/ph",
      "debug": false
    },
    "personalizationSurveyEnabled": false,
    "defaultLocale": "en",
    "userManagement": {
      "quota": -1,
      "showSetupOnFirstLoad": false,
      "smtpSetup": true,
      "authenticationMethod": "email",
      "passwordMinLength": 8
    },
    "sso": {
      "managedByEnv": false,
      "saml": {
        "loginEnabled": false,
        "loginLabel": ""
      },
      "ldap": {
        "loginEnabled": false,
        "loginLabel": ""
      },
      "oidc": {
        "loginEnabled": false,
        "loginUrl": "http://localhost:5678/rest/sso/oidc/login",
        "callbackUrl": "http://localhost:5678/rest/sso/oidc/callback"
      }
    },
    "logStreaming": {
      "managedByEnv": false
    },
    "dataTables": {
      "maxSize": 10000000
    },
    "publicApi": {
      "enabled": true,
      "latestVersion": 1,
      "path": "api/v1",
      "swaggerUi": {
        "enabled": true
      }
    },
    "workflowTagsDisabled": false,
    "workflowsAutosaveDisabled": false,
    "workflowsGroupsWithTriggersEnabled": true,
    "workflowsGroupsWithManyBoundariesEnabled": true,
    "useWorkflowPublicationService": false,
    "granularCredentialSharing": true,
    "logLevel": "info",
    "hiringBannerEnabled": false,
    "aiAssistant": {
      "enabled": true,
      "setup": true,
      "cloudUbbEnabled": true
    },
    "templates": {
      "enabled": true,
      "host": "https://api.n8n.io/api/"
    },
    "isMultiMain": false,
    "pushBackend": "websocket",
    "communityNodesEnabled": true,
    "unverifiedCommunityNodesEnabled": false,
    "communityNodesManagedByEnv": false,
    "deployment": {
      "type": "self-hosted"
    },
    "allowedModules": {
      "builtIn": [],
      "external": []
    },
    "enterprise": {
      "sharing": true,
      "ldap": true,
      "saml": true,
      "oidc": true,
      "mfaEnforcement": true,
      "logStreaming": true,
      "advancedExecutionFilters": true,
      "variables": true,
      "sourceControl": true,
      "auditLogs": true,
      "externalSecrets": true,
      "showNonProdBanner": false,
      "debugInEditor": true,
      "binaryDataS3": true,
      "workerView": true,
      "advancedPermissions": true,
      "workflowDiffs": true,
      "namedVersions": true,
      "provisioning": true,
      "projects": {
        "team": {
          "limit": -1
        }
      },
      "customRoles": true,
      "personalSpacePolicy": true,
      "dataRedaction": true,
      "otelCustomSpanAttributes": true,
      "workflowReviews": true
    },
    "mfa": {
      "enabled": false,
      "enforced": false
    },
    "hideUsagePage": false,
    "license": {
      "consumerId": "n8n-rust-v5-enterprise",
      "environment": "production"
    },
    "variables": {
      "limit": -1
    },
    "banners": {
      "dismissed": []
    },
    "askAi": {
      "enabled": true
    },
    "aiBuilder": {
      "enabled": true,
      "setup": true
    },
    "aiCredits": {
      "enabled": true,
      "credits": 999999,
      "setup": true
    },
    "ai": {
      "allowSendingParameterValues": true
    },
    "workflowHistory": {
      "pruneTime": -1,
      "licensePruneTime": -1
    },
    "pruning": {
      "isEnabled": false,
      "maxAge": 336,
      "maxCount": 10000
    },
    "security": {
      "blockFileAccessToN8nFiles": true,
      "postMessageAllowedOrigins": []
    },
    "chatTrigger": {
      "disablePublicChat": false
    },
    "easyAIWorkflowOnboarded": true,
    "folders": {
      "enabled": true
    },
    "workerPools": {
      "enabled": false
    },
    "evaluation": {
      "quota": -1,
      "collectionsEnabled": true,
      "configEvalsEnabled": true,
      "agentEvalsEnabled": true
    },
    "activeModules": [],
    "canvasOnly": false,
    "collaboration": {
      "crdt": false
    },
    "envFeatureFlags": {},
    "expressionEngine": "isolated-vm"
  }
}"#;

pub async fn settings() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        SETTINGS_ENTERPRISE_PAYLOAD,
    )
}

// ---------------------------------------------------------------------------
// Auto-Authenticated Owner & Session Endpoints
// ---------------------------------------------------------------------------

fn owner_user_payload() -> Value {
    json!({
        "data": {
            "id": "owner-admin-id",
            "email": "admin@n8n-rust.local",
            "firstName": "Admin",
            "lastName": "Owner",
            "isOwner": true,
            "isPending": false,
            "role": "global:owner",
            "globalScopes": [
                "workflow:create", "workflow:read", "workflow:update", "workflow:delete", "workflow:execute", "workflow:list", "workflow:publish",
                "credential:create", "credential:read", "credential:update", "credential:delete", "credential:list",
                "user:create", "user:read", "user:update", "user:delete", "user:list",
                "rbac:read", "rbac:update", "communityPackage:manage",
                "auditLog:read", "cloud:manage", "instanceSettings:manage", "orchestration:read"
            ],
            "personalProject": {
                "id": "project-personal-owner",
                "name": "Personal",
                "type": "personal",
                "createdAt": "2026-10-14T00:00:00.000Z",
                "updatedAt": "2026-10-14T00:00:00.000Z"
            },
            "settings": {
                "userActivated": true
            }
        }
    })
}

pub async fn auth_user_info() -> impl IntoResponse {
    Json(owner_user_payload())
}

pub async fn auth_logout() -> impl IntoResponse {
    Json(json!({ "data": true }))
}

pub async fn license_status() -> impl IntoResponse {
    Json(json!({
        "data": {
            "planName": "Enterprise",
            "isPlanActive": true,
            "features": {
                "sharing": true,
                "variables": true,
                "externalSecrets": true,
                "customRoles": true,
                "logStreaming": true,
                "workflowReviews": true
            },
            "quotas": {
                "users": -1,
                "workflows": -1
            }
        }
    }))
}

pub async fn list_projects() -> impl IntoResponse {
    Json(json!({
        "data": [
            {
                "id": "project-personal-owner",
                "name": "Personal",
                "type": "personal",
                "createdAt": "2026-10-14T00:00:00.000Z",
                "updatedAt": "2026-10-14T00:00:00.000Z",
                "role": "project:personalOwner",
                "icon": null
            }
        ]
    }))
}

pub async fn get_personal_project() -> impl IntoResponse {
    Json(json!({
        "data": {
            "id": "project-personal-owner",
            "name": "Personal",
            "type": "personal",
            "createdAt": "2026-10-14T00:00:00.000Z",
            "updatedAt": "2026-10-14T00:00:00.000Z",
            "role": "project:personalOwner",
            "icon": null
        }
    }))
}

pub async fn list_roles() -> impl IntoResponse {
    Json(json!({
        "data": [
            { "id": "global:owner", "name": "Owner", "scope": "global" },
            { "id": "global:admin", "name": "Admin", "scope": "global" },
            { "id": "global:member", "name": "Member", "scope": "global" }
        ]
    }))
}

pub fn get_core_node_types_array() -> Value {
    json!([
        {
            "name": "n8n-nodes-base.manualTrigger",
            "displayName": "When clicking 'Test workflow'",
            "icon": "fa:play",
            "group": ["trigger"],
            "version": 1,
            "description": "Starts workflow manually",
            "defaults": { "name": "When clicking ‘Test workflow’" },
            "inputs": [],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.scheduleTrigger",
            "displayName": "Schedule Trigger",
            "icon": "fa:clock",
            "group": ["trigger"],
            "version": 1.2,
            "description": "Runs workflow on a recurring schedule",
            "defaults": { "name": "Schedule Trigger" },
            "inputs": [],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.webhook",
            "displayName": "Webhook",
            "icon": "fa:plug",
            "group": ["trigger"],
            "version": 2,
            "description": "Receives HTTP requests to start workflow",
            "defaults": { "name": "Webhook" },
            "inputs": [],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.httpRequest",
            "displayName": "HTTP Request",
            "icon": "fa:globe",
            "group": ["action"],
            "version": 4.2,
            "description": "Makes HTTP requests and returns data",
            "defaults": { "name": "HTTP Request" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.code",
            "displayName": "Code",
            "icon": "fa:code",
            "group": ["transform"],
            "version": 2,
            "description": "Runs custom JavaScript or Python code",
            "defaults": { "name": "Code" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.set",
            "displayName": "Edit Fields (Set)",
            "icon": "fa:pen",
            "group": ["transform"],
            "version": 3.4,
            "description": "Sets or modifies item values",
            "defaults": { "name": "Edit Fields" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.if",
            "displayName": "If",
            "icon": "fa:code-branch",
            "group": ["transform"],
            "version": 2.2,
            "description": "Routes items to true or false paths",
            "defaults": { "name": "If" },
            "inputs": ["main"],
            "outputs": ["main", "main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.switch",
            "displayName": "Switch",
            "icon": "fa:random",
            "group": ["transform"],
            "version": 1,
            "description": "Route items to different output branches based on conditions",
            "defaults": { "name": "Switch" },
            "inputs": ["main"],
            "outputs": ["main", "main", "main", "main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.merge",
            "displayName": "Merge",
            "icon": "fa:compress-arrows-alt",
            "group": ["transform"],
            "version": 3,
            "description": "Merges data from multiple streams",
            "defaults": { "name": "Merge" },
            "inputs": ["main", "main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.executeWorkflow",
            "displayName": "Execute Workflow",
            "icon": "fa:sitemap",
            "group": ["transform"],
            "version": 1,
            "description": "Executes another workflow and returns its output",
            "defaults": { "name": "Execute Workflow" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.respondToWebhook",
            "displayName": "Respond to Webhook",
            "icon": "fa:reply",
            "group": ["action"],
            "version": 1.1,
            "description": "Responds directly to the HTTP request that triggered the webhook",
            "defaults": { "name": "Respond to Webhook" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.splitOut",
            "displayName": "Split Out",
            "icon": "fa:expand-arrows-alt",
            "group": ["transform"],
            "version": 1,
            "description": "Splits a list of items into separate individual items",
            "defaults": { "name": "Split Out" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.aggregate",
            "displayName": "Aggregate",
            "icon": "fa:object-group",
            "group": ["transform"],
            "version": 1,
            "description": "Aggregates multiple items into a single item containing a list",
            "defaults": { "name": "Aggregate" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.removeDuplicates",
            "displayName": "Remove Duplicates",
            "icon": "fa:filter",
            "group": ["transform"],
            "version": 1.2,
            "description": "Removes duplicate items while preserving original item ordering",
            "defaults": { "name": "Remove Duplicates" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.splitInBatches",
            "displayName": "Split In Batches",
            "icon": "fa:th-large",
            "group": ["transform"],
            "version": 1,
            "description": "Splits data into batches and iterates over them",
            "defaults": { "name": "Split In Batches" },
            "inputs": ["main"],
            "outputs": ["main", "main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.wait",
            "displayName": "Wait",
            "icon": "fa:clock",
            "group": ["flow"],
            "version": 1,
            "description": "Waits for a specified amount of time or resume event",
            "defaults": { "name": "Wait" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.stopAndError",
            "displayName": "Stop And Error",
            "icon": "fa:stop-circle",
            "group": ["flow"],
            "version": 1,
            "description": "Stops workflow execution and throws an error",
            "defaults": { "name": "Stop And Error" },
            "inputs": ["main"],
            "outputs": [],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.errorTrigger",
            "displayName": "Error Trigger",
            "icon": "fa:exclamation-triangle",
            "group": ["trigger"],
            "version": 1,
            "description": "Triggers workflow when another workflow encounters an error",
            "defaults": { "name": "Error Trigger" },
            "inputs": [],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.sort",
            "displayName": "Sort",
            "icon": "fa:sort-amount-down",
            "group": ["transform"],
            "version": 1,
            "description": "Sort items by fields",
            "defaults": { "name": "Sort" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.limit",
            "displayName": "Limit",
            "icon": "fa:compress-arrows-alt",
            "group": ["transform"],
            "version": 1,
            "description": "Limit the number of items passed through",
            "defaults": { "name": "Limit" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        },
        {
            "name": "n8n-nodes-base.filter",
            "displayName": "Filter",
            "icon": "fa:filter",
            "group": ["transform"],
            "version": 1,
            "description": "Filter items based on conditions",
            "defaults": { "name": "Filter" },
            "inputs": ["main"],
            "outputs": ["main"],
            "properties": []
        }
    ])
}

#[derive(Debug, Deserialize)]
pub struct CommunityPackageInputDto {
    pub name: String,
    pub version: Option<String>,
    pub author: Option<String>,
    pub description: Option<String>,
    pub manifest: Option<Value>,
}

pub async fn node_types_catalog(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut catalog = get_core_node_types_array();
    let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();

    if let Some(cat_arr) = catalog.as_array() {
        for node in cat_arr {
            if let Some(name) = node.get("name").and_then(|n| n.as_str()) {
                seen_names.insert(name.to_string());
            }
        }
    }

    if let Ok(packages) = state.db.list_community_packages() {
        if let Some(cat_arr) = catalog.as_array_mut() {
            for pkg in packages {
                if let Some(nodes) = pkg.manifest.get("nodeTypes").and_then(|n| n.as_array()) {
                    for node in nodes {
                        if let Some(name) = node.get("name").and_then(|n| n.as_str()) {
                            if !seen_names.contains(name) {
                                seen_names.insert(name.to_string());
                                cat_arr.push(node.clone());
                            }
                        }
                    }
                }
            }
        }
    }
    Json(json!({ "data": catalog }))
}

pub async fn list_community_packages(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.list_community_packages() {
        Ok(list) => (StatusCode::OK, Json(json!({ "data": list }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn install_community_package(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CommunityPackageInputDto>,
) -> impl IntoResponse {
    if payload.name.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "message": "Package name cannot be empty" })),
        )
            .into_response();
    }
    if let Some(ref m) = payload.manifest {
        let valid_manifest = m
            .get("nodeTypes")
            .and_then(|n| n.as_array())
            .is_some_and(|arr| {
                arr.iter().all(|item| {
                    item.get("name")
                        .and_then(|n| n.as_str())
                        .is_some_and(|s| !s.trim().is_empty())
                })
            });
        if !valid_manifest {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "message": "Invalid manifest: nodeTypes must be an array of objects with non-empty 'name'" })),
            )
                .into_response();
        }
    }
    let now = Utc::now().to_rfc3339();
    let pkg = CommunityPackageRecord {
        name: payload.name.trim().to_string(),
        version: payload.version.unwrap_or_else(|| "1.0.0".to_string()),
        author: payload.author.unwrap_or_default(),
        description: payload.description.unwrap_or_default(),
        manifest: payload.manifest.unwrap_or_else(|| json!({ "nodeTypes": [] })),
        installed_at: now,
    };
    match state.db.save_community_package(&pkg) {
        Ok(()) => (StatusCode::OK, Json(json!({ "data": pkg }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn delete_community_package(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.db.delete_community_package(&name) {
        Ok(true) => (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Community package not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn credential_types_catalog() -> impl IntoResponse {
    Json(json!({
        "data": [
            {
                "name": "stripeApi",
                "displayName": "Stripe API",
                "properties": []
            },
            {
                "name": "httpHeaderAuth",
                "displayName": "Header Auth",
                "properties": []
            },
            {
                "name": "httpBasicAuth",
                "displayName": "Basic Auth",
                "properties": []
            }
        ]
    }))
}

pub async fn module_settings() -> impl IntoResponse {
    Json(json!({ "data": {} }))
}

pub async fn active_workflows() -> impl IntoResponse {
    Json(json!({ "data": [] }))
}

// ---------------------------------------------------------------------------
// Workflows CRUD (100% n8n 2.43.x JSON compatible)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowInputDto {
    pub name: Option<String>,
    pub active: Option<bool>,
    pub nodes: Option<Value>,
    pub connections: Option<Value>,
    pub settings: Option<Value>,
    pub static_data: Option<Value>,
    pub pin_data: Option<Value>,
    pub meta: Option<Value>,
    pub tags: Option<Value>,
    pub parent_folder_id: Option<String>,
}

pub async fn list_workflows(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.list_workflows() {
        Ok(list) => {
            let count = list.len();
            (
                StatusCode::OK,
                Json(json!({ "data": list, "count": count })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct ViewportQuery {
    pub viewport: Option<bool>,
    #[serde(rename = "minX")]
    pub min_x: Option<f64>,
    #[serde(rename = "maxX")]
    pub max_x: Option<f64>,
    #[serde(rename = "minY")]
    pub min_y: Option<f64>,
    #[serde(rename = "maxY")]
    pub max_y: Option<f64>,
    #[serde(rename = "gridSize")]
    pub grid_size: Option<f64>,
}

pub fn slice_workflow_spatial(
    wf: &WorkflowRecord,
    q: &ViewportQuery,
) -> (Vec<Value>, usize, usize, bool) {
    let nodes_arr = match wf.nodes.as_array() {
        Some(arr) => arr,
        None => return (Vec::new(), 0, 0, false),
    };

    let total_nodes = nodes_arr.len();
    let mut min_x = q.min_x.unwrap_or(-1000.0);
    let mut max_x = q.max_x.unwrap_or(2000.0);
    let mut min_y = q.min_y.unwrap_or(-1000.0);
    let mut max_y = q.max_y.unwrap_or(2000.0);

    if !min_x.is_finite() { min_x = -1000.0; }
    if !max_x.is_finite() { max_x = 2000.0; }
    if !min_y.is_finite() { min_y = -1000.0; }
    if !max_y.is_finite() { max_y = 2000.0; }

    if min_x > max_x {
        std::mem::swap(&mut min_x, &mut max_x);
    }
    if min_y > max_y {
        std::mem::swap(&mut min_y, &mut max_y);
    }

    let grid_size = match q.grid_size {
        Some(g) if g.is_finite() && g >= 10.0 => g,
        _ => 500.0,
    };

    let mut inside_nodes = Vec::new();
    let mut clusters: HashMap<(i64, i64), Vec<Value>> = HashMap::new();

    for node in nodes_arr {
        let (x, y) = if let Some(pos) = node.get("position").and_then(|p| p.as_array()) {
            let px = pos.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let py = pos.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
            (
                if px.is_finite() { px } else { 0.0 },
                if py.is_finite() { py } else { 0.0 },
            )
        } else {
            (0.0, 0.0)
        };

        if x >= min_x && x <= max_x && y >= min_y && y <= max_y {
            inside_nodes.push(node.clone());
        } else {
            let gx_f = (x / grid_size).floor();
            let gy_f = (y / grid_size).floor();
            let gx = if gx_f > 1_000_000_000.0 {
                1_000_000_000
            } else if gx_f < -1_000_000_000.0 {
                -1_000_000_000
            } else {
                gx_f as i64
            };
            let gy = if gy_f > 1_000_000_000.0 {
                1_000_000_000
            } else if gy_f < -1_000_000_000.0 {
                -1_000_000_000
            } else {
                gy_f as i64
            };
            clusters.entry((gx, gy)).or_default().push(node.clone());
        }
    }

    let mut final_nodes = inside_nodes;
    for ((gx, gy), c_nodes) in clusters {
        let count = c_nodes.len();
        if count == 1 {
            final_nodes.push(c_nodes[0].clone());
        } else {
            let center_x = (gx as f64) * grid_size + grid_size / 2.0;
            let center_y = (gy as f64) * grid_size + grid_size / 2.0;
            final_nodes.push(json!({
                "id": format!("cluster-{gx}-{gy}"),
                "name": format!("Cluster ({count} nodes)"),
                "type": "n8n-nodes-base.clusterSuperNode",
                "position": [center_x, center_y],
                "parameters": {
                    "count": count,
                    "gridX": gx,
                    "gridY": gy,
                    "bounds": [
                        (gx as f64) * grid_size,
                        ((gx as f64) + 1.0) * grid_size,
                        (gy as f64) * grid_size,
                        ((gy as f64) + 1.0) * grid_size,
                    ]
                }
            }));
        }
    }

    let rendered_nodes = final_nodes.len();
    let is_clustered = rendered_nodes < total_nodes;
    (final_nodes, total_nodes, rendered_nodes, is_clustered)
}

pub async fn get_workflow_spatial_map(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(q): Query<ViewportQuery>,
) -> impl IntoResponse {
    match state.db.get_workflow(&id) {
        Ok(Some(wf)) => {
            let (nodes, total, rendered, is_clustered) = slice_workflow_spatial(&wf, &q);
            (
                StatusCode::OK,
                Json(json!({
                    "data": {
                        "id": wf.id,
                        "name": wf.name,
                        "nodes": nodes,
                        "connections": wf.connections,
                        "totalOriginalNodes": total,
                        "renderedNodes": rendered,
                        "isClustered": is_clustered
                    }
                })),
            )
                .into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {e}") })),
        )
            .into_response(),
    }
}

pub async fn get_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(q): Query<ViewportQuery>,
) -> impl IntoResponse {
    match state.db.get_workflow(&id) {
        Ok(Some(mut wf)) => {
            let node_count = wf.nodes.as_array().map_or(0, |a| a.len());
            if q.viewport == Some(true) || node_count > 2000 {
                let (sliced_nodes, total, rendered, is_clustered) = slice_workflow_spatial(&wf, &q);
                wf.nodes = Value::Array(sliced_nodes);
                if let Some(meta_obj) = wf.meta.as_object_mut() {
                    meta_obj.insert("lodClustered".to_string(), json!(is_clustered));
                    meta_obj.insert("totalOriginalNodes".to_string(), json!(total));
                    meta_obj.insert("renderedNodes".to_string(), json!(rendered));
                }
            }
            (StatusCode::OK, Json(json!({ "data": wf }))).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {e}") })),
        )
            .into_response(),
    }
}

pub async fn create_workflow(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<WorkflowInputDto>,
) -> impl IntoResponse {
    let now = Utc::now().to_rfc3339();
    let wf = WorkflowRecord {
        id: new_id(),
        name: payload.name.unwrap_or_else(|| "My workflow".to_string()),
        active: payload.active.unwrap_or(false),
        is_archived: false,
        nodes: payload.nodes.unwrap_or(Value::Array(vec![])),
        connections: payload
            .connections
            .unwrap_or(Value::Object(serde_json::Map::new())),
        settings: payload
            .settings
            .unwrap_or_else(|| json!({ "executionOrder": "v1" })),
        static_data: payload
            .static_data
            .unwrap_or(Value::Object(serde_json::Map::new())),
        pin_data: payload
            .pin_data
            .unwrap_or(Value::Object(serde_json::Map::new())),
        meta: payload
            .meta
            .unwrap_or(Value::Object(serde_json::Map::new())),
        version_id: new_id(),
        trigger_count: 0,
        tags: payload.tags.unwrap_or(Value::Array(vec![])),
        parent_folder_id: payload.parent_folder_id,
        created_at: now.clone(),
        updated_at: now,
    };

    match state.db.insert_or_update_workflow(&wf) {
        Ok(()) => {
            crate::engine::register_subworkflow(
                &wf.id,
                json!({ "nodes": wf.nodes, "connections": wf.connections }),
            );
            (StatusCode::OK, Json(json!({ "data": wf }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn update_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<WorkflowInputDto>,
) -> impl IntoResponse {
    let existing = match state.db.get_workflow(&id) {
        Ok(Some(wf)) => wf,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Workflow not found" })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Database error: {}", e) })),
            )
                .into_response();
        }
    };

    let updated = WorkflowRecord {
        id: existing.id,
        name: payload.name.unwrap_or(existing.name),
        active: payload.active.unwrap_or(existing.active),
        is_archived: existing.is_archived,
        nodes: payload.nodes.unwrap_or(existing.nodes),
        connections: payload.connections.unwrap_or(existing.connections),
        settings: payload.settings.unwrap_or(existing.settings),
        static_data: payload.static_data.unwrap_or(existing.static_data),
        pin_data: payload.pin_data.unwrap_or(existing.pin_data),
        meta: payload.meta.unwrap_or(existing.meta),
        version_id: new_id(),
        trigger_count: existing.trigger_count,
        tags: payload.tags.unwrap_or(existing.tags),
        parent_folder_id: payload.parent_folder_id.or(existing.parent_folder_id),
        created_at: existing.created_at,
        updated_at: Utc::now().to_rfc3339(),
    };

    match state.db.insert_or_update_workflow(&updated) {
        Ok(()) => {
            crate::engine::register_subworkflow(
                &updated.id,
                json!({ "nodes": updated.nodes, "connections": updated.connections }),
            );
            (StatusCode::OK, Json(json!({ "data": updated }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn delete_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.delete_workflow(&id) {
        Ok(true) => {
            crate::engine::unregister_subworkflow(&id);
            (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response()
        }
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn archive_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.archive_workflow(&id, true) {
        Ok(true) => match state.db.get_workflow(&id) {
            Ok(Some(wf)) => (StatusCode::OK, Json(json!({ "data": wf }))).into_response(),
            Ok(None) => (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Workflow not found" })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Database error: {}", e) })),
            )
                .into_response(),
        },
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn unarchive_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.archive_workflow(&id, false) {
        Ok(true) => match state.db.get_workflow(&id) {
            Ok(Some(wf)) => (StatusCode::OK, Json(json!({ "data": wf }))).into_response(),
            Ok(None) => (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Workflow not found" })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Database error: {}", e) })),
            )
                .into_response(),
        },
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Workflow not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Credentials CRUD
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialInputDto {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub cred_type: Option<String>,
    pub data: Option<Value>,
}

pub async fn list_credentials(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.list_credentials() {
        Ok(list) => (StatusCode::OK, Json(json!({ "data": list }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn get_credential(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.get_credential(&id) {
        Ok(Some(cred)) => (StatusCode::OK, Json(json!({ "data": cred }))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Credential not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn create_credential(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CredentialInputDto>,
) -> impl IntoResponse {
    let now = Utc::now().to_rfc3339();
    let cred = CredentialRecord {
        id: new_id(),
        name: payload.name.unwrap_or_else(|| "My Credential".to_string()),
        cred_type: payload
            .cred_type
            .unwrap_or_else(|| "customAuth".to_string()),
        data: payload
            .data
            .unwrap_or(Value::Object(serde_json::Map::new())),
        created_at: now.clone(),
        updated_at: now,
    };

    match state.db.save_credential(&cred) {
        Ok(()) => {
            crate::engine::register_credential(&cred.id, cred.data.clone());
            (StatusCode::OK, Json(json!({ "data": cred }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn update_credential(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<CredentialInputDto>,
) -> impl IntoResponse {
    let existing = match state.db.get_credential(&id) {
        Ok(Some(c)) => c,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Credential not found" })),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Database error: {}", e) })),
            )
                .into_response()
        }
    };

    let updated = CredentialRecord {
        id: existing.id,
        name: payload.name.unwrap_or(existing.name),
        cred_type: payload.cred_type.unwrap_or(existing.cred_type),
        data: payload.data.unwrap_or(existing.data),
        created_at: existing.created_at,
        updated_at: Utc::now().to_rfc3339(),
    };

    match state.db.save_credential(&updated) {
        Ok(()) => {
            crate::engine::register_credential(&updated.id, updated.data.clone());
            (StatusCode::OK, Json(json!({ "data": updated }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn delete_credential(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.delete_credential(&id) {
        Ok(true) => {
            crate::engine::unregister_credential(&id);
            (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response()
        }
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Credential not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Tags CRUD
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct TagInputDto {
    pub name: String,
}

pub async fn list_tags(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.list_tags() {
        Ok(list) => (StatusCode::OK, Json(json!({ "data": list }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn create_tag(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<TagInputDto>,
) -> impl IntoResponse {
    let now = Utc::now().to_rfc3339();
    let tag = TagRecord {
        id: new_id(),
        name: payload.name,
        created_at: now.clone(),
        updated_at: now,
    };
    match state.db.save_tag(&tag) {
        Ok(()) => (StatusCode::OK, Json(json!({ "data": tag }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn update_tag(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<TagInputDto>,
) -> impl IntoResponse {
    let now = Utc::now().to_rfc3339();
    let tag = TagRecord {
        id,
        name: payload.name,
        created_at: now.clone(),
        updated_at: now,
    };
    match state.db.save_tag(&tag) {
        Ok(()) => (StatusCode::OK, Json(json!({ "data": tag }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn delete_tag(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.delete_tag(&id) {
        Ok(true) => (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Tag not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Variables CRUD
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct VariableInputDto {
    pub key: String,
    pub value: String,
    #[serde(rename = "type")]
    pub var_type: Option<String>,
}

pub async fn list_variables(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.list_variables() {
        Ok(list) => (StatusCode::OK, Json(json!({ "data": list }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn create_variable(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VariableInputDto>,
) -> impl IntoResponse {
    let var = VariableRecord {
        id: new_id(),
        key: payload.key,
        value: payload.value,
        var_type: payload.var_type.unwrap_or_else(|| "string".to_string()),
    };
    match state.db.save_variable(&var) {
        Ok(()) => {
            crate::engine::register_variable(&var.key, &var.value);
            (StatusCode::OK, Json(json!({ "data": var }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct VariableUpdateDto {
    pub key: Option<String>,
    pub value: Option<String>,
    #[serde(rename = "type")]
    pub var_type: Option<String>,
}

pub async fn update_variable(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<VariableUpdateDto>,
) -> impl IntoResponse {
    let existing = match state.db.get_variable(&id) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Variable not found" })),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Database error: {}", e) })),
            )
                .into_response()
        }
    };

    let existing_key = existing.key.clone();
    let var = VariableRecord {
        id,
        key: payload.key.unwrap_or(existing.key),
        value: payload.value.unwrap_or(existing.value),
        var_type: payload.var_type.unwrap_or(existing.var_type),
    };
    match state.db.save_variable(&var) {
        Ok(()) => {
            if existing_key != var.key {
                crate::engine::unregister_variable(&existing_key);
            }
            crate::engine::register_variable(&var.key, &var.value);
            (StatusCode::OK, Json(json!({ "data": var }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn delete_variable(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let existing_key = state.db.get_variable(&id).ok().flatten().map(|v| v.key);
    match state.db.delete_variable(&id) {
        Ok(true) => {
            if let Some(k) = existing_key {
                crate::engine::unregister_variable(&k);
            }
            (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response()
        }
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Variable not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Folders CRUD
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderInputDto {
    pub name: String,
    pub parent_folder_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FolderUpdateDto {
    pub name: Option<String>,
    pub parent_folder_id: Option<String>,
}

pub async fn list_folders(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.list_folders() {
        Ok(list) => (StatusCode::OK, Json(json!({ "data": list }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn create_folder(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<FolderInputDto>,
) -> impl IntoResponse {
    let now = Utc::now().to_rfc3339();
    let folder = FolderRecord {
        id: new_id(),
        name: payload.name,
        parent_folder_id: payload.parent_folder_id,
        created_at: now.clone(),
        updated_at: now,
    };
    match state.db.save_folder(&folder) {
        Ok(()) => (StatusCode::OK, Json(json!({ "data": folder }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn update_folder(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<FolderUpdateDto>,
) -> impl IntoResponse {
    let existing = match state.db.get_folder(&id) {
        Ok(Some(f)) => f,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Folder not found" })),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Database error: {}", e) })),
            )
                .into_response()
        }
    };

    let folder = FolderRecord {
        id,
        name: payload.name.unwrap_or(existing.name),
        parent_folder_id: payload.parent_folder_id.or(existing.parent_folder_id),
        created_at: existing.created_at,
        updated_at: Utc::now().to_rfc3339(),
    };
    match state.db.save_folder(&folder) {
        Ok(()) => (StatusCode::OK, Json(json!({ "data": folder }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn delete_folder(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.delete_folder(&id) {
        Ok(true) => (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Folder not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Executions (Zstd-compressed storage)
// ---------------------------------------------------------------------------

pub async fn list_executions(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.list_executions() {
        Ok(list) => {
            let count = list.len();
            (
                StatusCode::OK,
                Json(json!({ "data": list, "count": count })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn get_execution(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.get_execution(&id) {
        Ok(Some(exec)) => (StatusCode::OK, Json(json!({ "data": exec }))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Execution not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

pub async fn delete_execution(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.delete_execution(&id) {
        Ok(true) => (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Execution not found" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("Database error: {}", e) })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Real-time Push WebSocket & SSE Endpoint (/rest/push)
// ---------------------------------------------------------------------------

pub async fn rest_push(
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
    State(state): State<Arc<AppState>>,
) -> Response {
    match ws {
        Ok(ws) => {
            let hub = Arc::clone(&state.push_hub);
            ws.on_upgrade(move |socket| handle_websocket(socket, hub))
                .into_response()
        }
        Err(_) => {
            let rx = state.push_hub.subscribe();
            let stream = futures_util::stream::unfold(rx, |mut rx| async move {
                match rx.recv().await {
                    Ok(msg) => Some((Ok::<Event, Infallible>(Event::default().data(msg)), rx)),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => Some((
                        Ok::<Event, Infallible>(Event::default().comment("lagged")),
                        rx,
                    )),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
                }
            });
            Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response()
        }
    }
}

async fn handle_websocket(mut socket: WebSocket, hub: Arc<PushHub>) {
    let mut rx = hub.subscribe();
    while let Ok(msg) = rx.recv().await {
        if socket.send(WsMessage::Text(msg.into())).await.is_err() {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Workflow Execution Endpoints (HyperScaleEngine)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RunWorkflowPayload {
    pub workflow_data: Option<Value>,
    pub run_data: Option<Value>,
    pub pin_data: Option<Value>,
    pub nodes: Option<Value>,
    pub connections: Option<Value>,
    pub settings: Option<Value>,
}

pub fn apply_pin_data_to_nodes(nodes: &mut Value, pin_data: Option<&Value>) {
    if let (Some(pin), Some(nodes_arr)) = (pin_data, nodes.as_array_mut()) {
        for n in nodes_arr {
            let name = n.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if let Some(pin_val) = pin.get(name) {
                let items: Vec<Value> = if let Some(arr) = pin_val.as_array() {
                    arr.iter().map(|item| {
                        if let Some(j) = item.get("json") {
                            j.clone()
                        } else {
                            item.clone()
                        }
                    }).collect()
                } else if let Some(j) = pin_val.get("json") {
                    vec![j.clone()]
                } else {
                    vec![pin_val.clone()]
                };
                if let Some(params) = n.get_mut("parameters").and_then(|p| p.as_object_mut()) {
                    params.entry("data".to_string()).or_insert_with(|| json!(items));
                }
            }
        }
    }
}

pub async fn run_workflow_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<Option<RunWorkflowPayload>>,
) -> impl IntoResponse {
    let wf = match state.db.get_workflow(&id) {
        Ok(Some(w)) => w,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Workflow not found" })),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Database error: {}", e) })),
            )
                .into_response()
        }
    };

    let mut nodes = payload
        .as_ref()
        .and_then(|p| {
            p.nodes.clone().or_else(|| {
                p.workflow_data
                    .as_ref()
                    .and_then(|w| w.get("nodes").cloned())
            })
        })
        .unwrap_or(wf.nodes);

    let pin_data = payload
        .as_ref()
        .and_then(|p| p.pin_data.clone().or_else(|| p.workflow_data.as_ref().and_then(|w| w.get("pinData").cloned())))
        .or_else(|| Some(wf.pin_data.clone()));
    apply_pin_data_to_nodes(&mut nodes, pin_data.as_ref());

    let connections = payload
        .as_ref()
        .and_then(|p| {
            p.connections.clone().or_else(|| {
                p.workflow_data
                    .as_ref()
                    .and_then(|w| w.get("connections").cloned())
            })
        })
        .unwrap_or(wf.connections);

    let settings = payload
        .as_ref()
        .and_then(|p| {
            p.settings.clone().or_else(|| {
                p.workflow_data
                    .as_ref()
                    .and_then(|w| w.get("settings").cloned())
            })
        })
        .or(Some(wf.settings));

    execute_workflow_helper(state, Some(id), nodes, connections, settings).await
}

pub async fn run_workflow_direct(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Option<RunWorkflowPayload>>,
) -> impl IntoResponse {
    let payload = payload.unwrap_or_default();
    let pin_data = payload.pin_data.clone().or_else(|| {
        payload
            .workflow_data
            .as_ref()
            .and_then(|w| w.get("pinData").cloned())
    });

    let mut nodes = payload
        .nodes
        .clone()
        .or_else(|| {
            payload
                .workflow_data
                .as_ref()
                .and_then(|w| w.get("nodes").cloned())
        })
        .unwrap_or_else(|| json!([]));

    apply_pin_data_to_nodes(&mut nodes, pin_data.as_ref());

    let connections = payload
        .connections
        .clone()
        .or_else(|| {
            payload
                .workflow_data
                .as_ref()
                .and_then(|w| w.get("connections").cloned())
        })
        .unwrap_or_else(|| json!({}));

    let settings = payload
        .settings
        .clone()
        .or_else(|| {
            payload
                .workflow_data
                .as_ref()
                .and_then(|w| w.get("settings").cloned())
        });

    execute_workflow_helper(state, None, nodes, connections, settings).await
}

pub async fn trigger_error_workflow_if_needed(
    state: &Arc<AppState>,
    failed_execution_id: &str,
    failed_workflow_id: Option<&str>,
    settings: Option<&Value>,
    failed_final_output: &Value,
    notifier: &Arc<dyn ExecutionNotifier>,
) -> Option<Value> {
    let error_workflow_id = settings
        .and_then(|s| s.get("errorWorkflow"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .or_else(|| {
            failed_workflow_id.and_then(|w_id| {
                state.db.get_workflow(w_id).ok().flatten().and_then(|w| {
                    w.settings.get("errorWorkflow").and_then(|v| v.as_str()).map(|s| s.to_string())
                })
            })
        })?;

    if failed_workflow_id == Some(error_workflow_id.as_str()) {
        return None;
    }

    let err_wf_data = crate::engine::get_subworkflow(&error_workflow_id)
        .map(|val| {
            let err_nodes = val.get("nodes").cloned().unwrap_or_else(|| json!([]));
            let err_conns = val.get("connections").cloned().unwrap_or_else(|| json!({}));
            (err_nodes, err_conns)
        })
        .or_else(|| {
            state
                .db
                .get_workflow(&error_workflow_id)
                .ok()
                .flatten()
                .map(|wf_rec| (wf_rec.nodes, wf_rec.connections))
        })?;

    let (err_nodes, err_conns) = err_wf_data;
    let err_graph = CompiledGraph::compile(&err_nodes, &err_conns).ok()?;
    let err_execution_id = format!("exec-{}", new_id());
    let err_cancel_token = Arc::new(AtomicBool::new(false));
    state
        .active_executions
        .insert(err_execution_id.clone(), Arc::clone(&err_cancel_token));

    let err_started_at = Utc::now().to_rfc3339();
    let err_input = json!({
        "execution": { "id": failed_execution_id, "status": "error" },
        "workflow": { "id": failed_workflow_id.unwrap_or_default() },
        "error": failed_final_output.get("error").cloned().unwrap_or_else(|| failed_final_output.clone())
    });

    let exec_outcome = state
        .engine
        .execute(
            &err_execution_id,
            Some(&error_workflow_id),
            &err_graph,
            Arc::new(err_input),
            Some(Arc::clone(notifier)),
            Some(err_cancel_token),
        )
        .await;

    state.active_executions.remove(&err_execution_id);
    let err_exec_res = exec_outcome.ok()?;
    let err_stopped_at = Utc::now().to_rfc3339();

    let write_msg = ExecutionWriteMessage {
        id: err_execution_id.clone(),
        workflow_id: Some(error_workflow_id.clone()),
        status: err_exec_res.status.clone(),
        mode: "error".to_string(),
        started_at: err_started_at,
        stopped_at: Some(err_stopped_at),
        data_json: err_exec_res.payload.clone(),
    };
    let _ = state.db.record_execution_async(write_msg);

    let result_data = err_exec_res
        .payload
        .get("resultData")
        .cloned()
        .unwrap_or_else(|| err_exec_res.payload.clone());

    Some(json!({
        "executionId": err_execution_id,
        "workflowId": error_workflow_id,
        "status": err_exec_res.status,
        "executedNodes": err_exec_res.executed_nodes,
        "durationMs": err_exec_res.duration_ms,
        "finalOutput": err_exec_res.final_output,
        "resultData": result_data
    }))
}

async fn execute_workflow_helper(
    state: Arc<AppState>,
    workflow_id: Option<String>,
    nodes: Value,
    connections: Value,
    settings: Option<Value>,
) -> axum::response::Response {
    let graph = match CompiledGraph::compile(&nodes, &connections) {
        Ok(g) => g,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "message": format!("Failed to compile workflow DAG: {}", e) })),
            )
                .into_response()
        }
    };

    let execution_id = format!("exec-{}", new_id());
    let cancel_token = Arc::new(AtomicBool::new(false));
    state
        .active_executions
        .insert(execution_id.clone(), Arc::clone(&cancel_token));

    let notifier: Arc<dyn ExecutionNotifier> =
        Arc::new(PushHubNotifier::new(Arc::clone(&state.push_hub)));

    let started_at = Utc::now().to_rfc3339();

    let exec_res = match state
        .engine
        .execute(
            &execution_id,
            workflow_id.as_deref(),
            &graph,
            Arc::new(json!({})),
            Some(Arc::clone(&notifier)),
            Some(Arc::clone(&cancel_token)),
        )
        .await
    {
        Ok(res) => res,
        Err(e) => {
            state.active_executions.remove(&execution_id);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Execution engine failure: {}", e) })),
            )
                .into_response();
        }
    };

    let stopped_at = Utc::now().to_rfc3339();
    state.active_executions.remove(&execution_id);

    // Record to write-behind buffer SQLite
    let write_msg = ExecutionWriteMessage {
        id: execution_id.clone(),
        workflow_id: workflow_id.clone(),
        status: exec_res.status.clone(),
        mode: "manual".to_string(),
        started_at,
        stopped_at: Some(stopped_at),
        data_json: exec_res.payload.clone(),
    };
    let _ = state.db.record_execution_async(write_msg);

    let error_workflow_execution = if exec_res.status == "error" {
        trigger_error_workflow_if_needed(
            &state,
            &execution_id,
            workflow_id.as_deref(),
            settings.as_ref(),
            &exec_res.final_output,
            &notifier,
        )
        .await
    } else {
        None
    };

    let mut data_obj = json!({
        "executionId": execution_id,
        "status": exec_res.status,
        "executedNodes": exec_res.executed_nodes,
        "durationMs": exec_res.duration_ms,
        "resultData": exec_res
            .payload
            .get("resultData")
            .cloned()
            .unwrap_or_else(|| exec_res.payload.clone())
    });

    if let Some(err_exec) = error_workflow_execution {
        if let Some(obj) = data_obj.as_object_mut() {
            obj.insert("errorWorkflowExecution".to_string(), err_exec);
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "data": data_obj
        })),
    )
        .into_response()
}

pub async fn stop_execution(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Some(token) = state.active_executions.get(&id) {
        token.store(true, Ordering::Relaxed);
        (
            StatusCode::OK,
            Json(json!({
                "data": {
                    "success": true,
                    "message": "Execution cancellation signaled"
                }
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "No active execution found with that ID" })),
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Webhook Trigger Endpoints (/webhook/:path & /webhook-test/:path)
// ---------------------------------------------------------------------------

pub async fn handle_webhook(
    State(state): State<Arc<AppState>>,
    Path(path): Path<String>,
    headers: axum::http::HeaderMap,
    axum::extract::Query(query_params): axum::extract::Query<HashMap<String, String>>,
    body_bytes: axum::body::Bytes,
) -> impl IntoResponse {
    let body_val: Value = if body_bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&body_bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body_bytes).to_string()))
    };

    let mut headers_obj = serde_json::Map::new();
    for (k, v) in headers.iter() {
        if let Ok(v_str) = v.to_str() {
            headers_obj.insert(k.as_str().to_string(), json!(v_str));
        }
    }

    let query_obj = json!(query_params);

    let initial_input = json!({
        "body": body_val,
        "headers": headers_obj,
        "query": query_obj,
        "webhookPath": path
    });

    let workflows = match state.db.list_workflows() {
        Ok(w) => w,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Database error: {e}") })),
            )
                .into_response();
        }
    };

    let mut matched_wf = None;
    for wf in workflows {
        if let Some(nodes) = wf.nodes.as_array() {
            for n in nodes {
                let n_type = n.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if n_type == "n8n-nodes-base.webhook" {
                    let n_path = n
                        .get("parameters")
                        .and_then(|p| p.get("path"))
                        .and_then(|p| p.as_str())
                        .unwrap_or("");
                    if n_path == path
                        || n_path.trim_start_matches('/') == path.trim_start_matches('/')
                    {
                        matched_wf = Some(wf);
                        break;
                    }
                }
            }
        }
        if matched_wf.is_some() {
            break;
        }
    }

    let wf = match matched_wf {
        Some(w) => w,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    json!({ "message": format!("Workflow with webhook path '{path}' not found") }),
                ),
            )
                .into_response();
        }
    };

    let graph = match CompiledGraph::compile(&wf.nodes, &wf.connections) {
        Ok(g) => g,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "message": format!("Failed to compile webhook workflow DAG: {e}") })),
            )
                .into_response();
        }
    };

    let execution_id = format!("exec-{}", new_id());
    let cancel_token = Arc::new(AtomicBool::new(false));
    state
        .active_executions
        .insert(execution_id.clone(), Arc::clone(&cancel_token));

    let notifier: Arc<dyn ExecutionNotifier> =
        Arc::new(PushHubNotifier::new(Arc::clone(&state.push_hub)));
    let started_at = Utc::now().to_rfc3339();

    let exec_res = match state
        .engine
        .execute(
            &execution_id,
            Some(&wf.id),
            &graph,
            Arc::new(initial_input),
            Some(Arc::clone(&notifier)),
            Some(cancel_token),
        )
        .await
    {
        Ok(res) => res,
        Err(e) => {
            state.active_executions.remove(&execution_id);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("Webhook execution failure: {e}") })),
            )
                .into_response();
        }
    };

    let stopped_at = Utc::now().to_rfc3339();
    state.active_executions.remove(&execution_id);

    let write_msg = ExecutionWriteMessage {
        id: execution_id.clone(),
        workflow_id: Some(wf.id.clone()),
        status: exec_res.status.clone(),
        mode: "webhook".to_string(),
        started_at,
        stopped_at: Some(stopped_at),
        data_json: exec_res.payload.clone(),
    };
    let _ = state.db.record_execution_async(write_msg);

    if exec_res.status == "error" {
        let _ = trigger_error_workflow_if_needed(
            &state,
            &execution_id,
            Some(&wf.id),
            Some(&wf.settings),
            &exec_res.final_output,
            &notifier,
        )
        .await;
    }

    // Check if the workflow produced a custom webhook response via RespondToWebhook node
    let mut custom_webhook_response = None;

    if let Some(obj) = exec_res.final_output.as_object() {
        if let Some(r) = obj.get("__webhook_response") {
            custom_webhook_response = Some(r.clone());
        }
    } else if let Some(arr) = exec_res.final_output.as_array() {
        for item in arr {
            if let Some(r) = item.get("__webhook_response").or_else(|| {
                item.get("json").and_then(|j| j.get("__webhook_response"))
            }) {
                custom_webhook_response = Some(r.clone());
                break;
            }
        }
    }

    if custom_webhook_response.is_none() {
        if let Some(run_data) = exec_res
            .payload
            .get("resultData")
            .and_then(|rd| rd.get("runData"))
            .and_then(|rd| rd.as_object())
        {
            for runs in run_data.values() {
                if let Some(run_arr) = runs.as_array() {
                    for run_entry in run_arr {
                        if let Some(main_outputs) = run_entry
                            .get("data")
                            .and_then(|d| d.get("main"))
                            .and_then(|m| m.as_array())
                        {
                            for port_items in main_outputs {
                                if let Some(items) = port_items.as_array() {
                                    for item in items {
                                        if let Some(r) = item.get("__webhook_response").or_else(|| {
                                            item.get("json").and_then(|j| j.get("__webhook_response"))
                                        }) {
                                            custom_webhook_response = Some(r.clone());
                                            break;
                                        }
                                    }
                                }
                                if custom_webhook_response.is_some() {
                                    break;
                                }
                            }
                        }
                        if custom_webhook_response.is_some() {
                            break;
                        }
                    }
                }
                if custom_webhook_response.is_some() {
                    break;
                }
            }
        }
    }

    if let Some(resp_val) = custom_webhook_response {
        let status_code = resp_val
            .get("statusCode")
            .and_then(|c| c.as_u64())
            .and_then(|c| StatusCode::from_u16(c as u16).ok())
            .unwrap_or(StatusCode::OK);

        let mut res_headers = axum::http::HeaderMap::new();
        if let Some(h_obj) = resp_val.get("headers").and_then(|h| h.as_object()) {
            for (k, v) in h_obj {
                if let (Ok(h_name), Some(h_str)) = (
                    axum::http::HeaderName::from_bytes(k.as_bytes()),
                    v.as_str(),
                ) {
                    if let Ok(h_val) = axum::http::HeaderValue::from_str(h_str) {
                        res_headers.insert(h_name, h_val);
                    }
                }
            }
        }

        let mut body_val = resp_val.get("body").cloned().unwrap_or(Value::Null);
        if let Value::Object(ref mut map) = body_val {
            map.remove("__webhook_response");
        }

        if !res_headers.contains_key(axum::http::header::CONTENT_TYPE) {
            match &body_val {
                Value::String(_) => {
                    if let Ok(ct) = axum::http::HeaderValue::from_str("text/plain; charset=utf-8") {
                        res_headers.insert(axum::http::header::CONTENT_TYPE, ct);
                    }
                }
                _ => {
                    if let Ok(ct) = axum::http::HeaderValue::from_str("application/json") {
                        res_headers.insert(axum::http::header::CONTENT_TYPE, ct);
                    }
                }
            }
        }

        let body_bytes = match &body_val {
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Null => Vec::new(),
            other => serde_json::to_vec(other).unwrap_or_default(),
        };

        return (status_code, res_headers, body_bytes).into_response();
    }

    (StatusCode::OK, Json(exec_res.final_output)).into_response()
}

// ---------------------------------------------------------------------------
// Main Router Assembly
// ---------------------------------------------------------------------------

pub fn router(app_state: Arc<AppState>) -> Router {
    Router::new()
        // Health
        .route("/healthz", get(healthz))
        .route("/rest/healthz", get(healthz))
        // Settings & License
        .route("/rest/settings", get(settings))
        .route("/rest/license", get(license_status))
        // Auth & Identity
        .route("/rest/login", get(auth_user_info).post(auth_user_info))
        .route("/rest/logout", post(auth_logout))
        .route("/rest/me", get(auth_user_info).patch(auth_user_info))
        // Projects & Roles
        .route("/rest/projects", get(list_projects))
        .route("/rest/projects/personal", get(get_personal_project))
        .route("/rest/roles", get(list_roles))
        // Real-Time Push & Events
        .route("/rest/push", get(rest_push))
        // Webhooks Trigger Engine
        .route("/webhook/{path}", get(handle_webhook).post(handle_webhook))
        .route(
            "/webhook-test/{path}",
            get(handle_webhook).post(handle_webhook),
        )
        // Node & Credential Types Catalogs
        .route(
            "/rest/node-types",
            get(node_types_catalog).post(node_types_catalog),
        )
        .route("/rest/credential-types", get(credential_types_catalog))
        .route("/rest/module-settings", get(module_settings))
        .route("/rest/active-workflows", get(active_workflows))
        // Workflows CRUD & Run
        .route("/rest/workflows", get(list_workflows).post(create_workflow))
        .route("/rest/workflows/run", post(run_workflow_direct))
        .route(
            "/rest/workflows/{id}",
            get(get_workflow)
                .patch(update_workflow)
                .put(update_workflow)
                .delete(delete_workflow),
        )
        .route(
            "/rest/workflows/{id}/spatial-map",
            get(get_workflow_spatial_map),
        )
        .route("/rest/workflows/{id}/run", post(run_workflow_by_id))
        .route("/rest/workflows/{id}/archive", post(archive_workflow))
        .route("/rest/workflows/{id}/unarchive", post(unarchive_workflow))
        // Community Packages CRUD
        .route(
            "/rest/community-packages",
            get(list_community_packages).post(install_community_package),
        )
        .route(
            "/rest/community-packages/{name}",
            delete(delete_community_package),
        )
        // Credentials CRUD
        .route(
            "/rest/credentials",
            get(list_credentials).post(create_credential),
        )
        .route(
            "/rest/credentials/{id}",
            get(get_credential)
                .patch(update_credential)
                .delete(delete_credential),
        )
        // Tags CRUD
        .route("/rest/tags", get(list_tags).post(create_tag))
        .route("/rest/tags/{id}", patch(update_tag).delete(delete_tag))
        // Variables CRUD
        .route("/rest/variables", get(list_variables).post(create_variable))
        .route(
            "/rest/variables/{id}",
            patch(update_variable)
                .put(update_variable)
                .delete(delete_variable),
        )
        // Folders CRUD
        .route("/rest/folders", get(list_folders).post(create_folder))
        .route(
            "/rest/folders/{id}",
            patch(update_folder).delete(delete_folder),
        )
        // Executions CRUD & Stop
        .route("/rest/executions", get(list_executions))
        .route(
            "/rest/executions/{id}",
            get(get_execution).delete(delete_execution),
        )
        .route("/rest/executions/{id}/stop", post(stop_execution))
        .with_state(app_state)
}

/// Spawns a background Tokio task to evaluate and trigger active workflows with scheduleTrigger
pub fn spawn_cron_scheduler_daemon(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    let weak_state = Arc::downgrade(&state);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        let last_runs: Arc<DashMap<String, std::time::Instant>> = Arc::new(DashMap::new());
        let in_flight: Arc<DashSet<String>> = Arc::new(DashSet::new());

        loop {
            interval.tick().await;

            let state = match weak_state.upgrade() {
                Some(s) => s,
                None => {
                    // AppState dropped: server shut down or tests finished, terminate daemon cleanly
                    break;
                }
            };

            let active_wfs = match state.db.list_active_workflows() {
                Ok(wfs) => wfs,
                Err(e) => {
                    tracing::warn!("Cron scheduler db error: {}", e);
                    continue;
                }
            };

            last_runs.retain(|id, _| active_wfs.iter().any(|w| &w.id == id));

            for wf in active_wfs {
                if in_flight.contains(&wf.id) {
                    continue;
                }

                let has_schedule = wf.nodes.as_array().is_some_and(|nodes| {
                    nodes.iter().any(|n| {
                        n.get("type").and_then(|t| t.as_str()).is_some_and(|t| {
                            t == "n8n-nodes-base.scheduleTrigger" || t == "scheduleTrigger"
                        })
                    })
                });

                if !has_schedule {
                    continue;
                }

                let interval_secs = wf
                    .nodes
                    .as_array()
                    .and_then(|nodes| {
                        nodes.iter().find_map(|n| {
                            let t = n.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            if t == "n8n-nodes-base.scheduleTrigger" || t == "scheduleTrigger" {
                                let params = n.get("parameters")?;
                                if let Some(sec) =
                                    params.get("secondsInterval").and_then(|s| s.as_u64())
                                {
                                    return Some(sec);
                                }
                                if let Some(sec) =
                                    params.get("intervalSecs").and_then(|s| s.as_u64())
                                {
                                    return Some(sec);
                                }
                            }
                            None
                        })
                    })
                    .unwrap_or(60);

                let should_run = match last_runs.get(&wf.id) {
                    Some(entry) => entry.elapsed().as_secs() >= interval_secs,
                    None => true,
                };

                if should_run {
                    last_runs.insert(wf.id.clone(), std::time::Instant::now());
                    in_flight.insert(wf.id.clone());
                    let state_clone = Arc::clone(&state);
                    let in_flight_clone = Arc::clone(&in_flight);
                    let wf_id = wf.id.clone();
                    let nodes = wf.nodes.clone();
                    let connections = wf.connections.clone();
                    let settings = Some(wf.settings.clone());

                    tokio::spawn(async move {
                        let _ = execute_workflow_helper(state_clone, Some(wf_id.clone()), nodes, connections, settings)
                            .await;
                        in_flight_clone.remove(&wf_id);
                    });
                }
            }
        }
    })
}

