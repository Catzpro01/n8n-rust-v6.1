use crate::db::{
    new_id, CredentialRecord, FolderRecord, StorageEngine, TagRecord, VariableRecord,
    WorkflowRecord,
};
use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::{get, patch, post},
    Json, Router,
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub db: StorageEngine,
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
            "icon": "fa:directions",
            "group": ["transform"],
            "version": 3.2,
            "description": "Routes items across multiple rules",
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
        }
    ])
}

pub async fn node_types_catalog() -> impl IntoResponse {
    Json(json!({
        "data": get_core_node_types_array()
    }))
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

pub async fn get_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.get_workflow(&id) {
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
        Ok(()) => (StatusCode::OK, Json(json!({ "data": wf }))).into_response(),
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
        Ok(()) => (StatusCode::OK, Json(json!({ "data": updated }))).into_response(),
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
        Ok(true) => (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response(),
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
        Ok(()) => (StatusCode::OK, Json(json!({ "data": cred }))).into_response(),
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
        Ok(()) => (StatusCode::OK, Json(json!({ "data": updated }))).into_response(),
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
        Ok(true) => (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response(),
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
        Ok(()) => (StatusCode::OK, Json(json!({ "data": var }))).into_response(),
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

    let var = VariableRecord {
        id,
        key: payload.key.unwrap_or(existing.key),
        value: payload.value.unwrap_or(existing.value),
        var_type: payload.var_type.unwrap_or(existing.var_type),
    };
    match state.db.save_variable(&var) {
        Ok(()) => (StatusCode::OK, Json(json!({ "data": var }))).into_response(),
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
    match state.db.delete_variable(&id) {
        Ok(true) => (StatusCode::OK, Json(json!({ "data": { "success": true } }))).into_response(),
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
        // Node & Credential Types Catalogs
        .route(
            "/rest/node-types",
            get(node_types_catalog).post(node_types_catalog),
        )
        .route("/rest/credential-types", get(credential_types_catalog))
        .route("/rest/module-settings", get(module_settings))
        .route("/rest/active-workflows", get(active_workflows))
        // Workflows CRUD
        .route("/rest/workflows", get(list_workflows).post(create_workflow))
        .route(
            "/rest/workflows/{id}",
            get(get_workflow)
                .patch(update_workflow)
                .put(update_workflow)
                .delete(delete_workflow),
        )
        .route("/rest/workflows/{id}/archive", post(archive_workflow))
        .route("/rest/workflows/{id}/unarchive", post(unarchive_workflow))
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
            patch(update_variable).delete(delete_variable),
        )
        // Folders CRUD
        .route("/rest/folders", get(list_folders).post(create_folder))
        .route(
            "/rest/folders/{id}",
            patch(update_folder).delete(delete_folder),
        )
        // Executions CRUD
        .route("/rest/executions", get(list_executions))
        .route(
            "/rest/executions/{id}",
            get(get_execution).delete(delete_execution),
        )
        .with_state(app_state)
}
