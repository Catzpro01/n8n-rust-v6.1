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
                "nodeName": node_name,
                "executionIndex": 0
            }
        }));
    }

    fn notify_node_after(&self, execution_id: &str, node_name: &str, output: &Value) {
        let now_ms = Utc::now().timestamp_millis();
        let main_items: Vec<Value> = if let Some(arr) = output.as_array() {
            arr.iter().map(|item| json!({ "json": item })).collect()
        } else {
            vec![json!({ "json": output })]
        };
        let items_count = main_items.len().max(1);

        self.hub.broadcast(&json!({
            "type": "nodeExecuteAfter",
            "data": {
                "executionId": execution_id,
                "nodeName": node_name,
                "executionIndex": 0,
                "itemCountByConnectionType": {
                    "main": [[ items_count ]]
                },
                "data": {
                    "startTime": now_ms,
                    "executionTime": 5,
                    "executionStatus": "success",
                    "executionIndex": 0,
                    "source": []
                }
            }
        }));

        self.hub.broadcast(&json!({
            "type": "nodeExecuteAfterData",
            "data": {
                "executionId": execution_id,
                "nodeName": node_name,
                "executionIndex": 0,
                "executionStatus": "success",
                "data": {
                    "startTime": now_ms,
                    "executionTime": 5,
                    "executionStatus": "success",
                    "executionIndex": 0,
                    "source": [],
                    "data": {
                        "main": [ main_items ]
                    }
                }
            }
        }));
    }

    fn notify_finished(
        &self,
        execution_id: &str,
        status: &str,
        duration_ms: u64,
        workflow_id: Option<&str>,
    ) {
        self.hub.broadcast(&json!({
            "type": "executionFinished",
            "data": {
                "executionId": execution_id,
                "workflowId": workflow_id,
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
    "endpointHealth": "/healthz",
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
      "type": "default"
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
      "collectionsEnabled": false,
      "configEvalsEnabled": false,
      "agentEvalsEnabled": false
    },
    "activeModules": [
      "workflow-builder",
      "policy-infrastructure",
      "inbound-auth-core",
      "insights",
      "community-packages",
      "data-table",
      "oauth-server",
      "mcp",
      "breaking-changes",
      "chat-hub",
      "quick-connect",
      "favorites",
      "redaction",
      "instance-registry",
      "otel",
      "instance-version-history",
      "encryption-key-manager",
      "oauth-jwe",
      "n8n-packages",
      "runtime-credentials",
      "mcp-registry",
      "instance-ai",
      "agents"
    ],
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

pub fn project_scopes_array() -> Vec<&'static str> {
    vec![
        "agent:create", "agent:read", "agent:update", "agent:delete", "agent:list", "agent:execute", "agent:unpublish",
        "workflow:create", "workflow:read", "workflow:export", "workflow:import", "workflow:update", "workflow:delete", "workflow:list", "workflow:execute", "workflow:execute-chat", "workflow:move", "workflow:unpublish", "workflow:unshare", "workflow:enableRedaction", "workflow:disableRedaction",
        "credential:create", "credential:read", "credential:update", "credential:delete", "credential:list", "credential:move", "credential:unshare", "credential:connect", "credential:createEndUser",
        "execution:read", "execution:list", "execution:reveal", "execution:delete",
        "project:list", "project:read", "project:export",
        "folder:create", "folder:read", "folder:update", "folder:delete", "folder:list", "folder:move",
        "dataTable:create", "dataTable:delete", "dataTable:read", "dataTable:update", "dataTable:listProject", "dataTable:readRow", "dataTable:writeRow", "dataTable:readColumn", "dataTable:writeColumn",
        "projectAiPreference:list", "projectAiPreference:read", "projectAiPreference:create", "projectAiPreference:update", "projectAiPreference:delete",
    ]
}

pub fn global_scopes_array() -> Vec<&'static str> {
    vec![
        "agent:create", "agent:read", "agent:update", "agent:delete", "agent:list", "agent:execute", "agent:publish", "agent:unpublish", "agent:manage",
        "aiAssistant:manage", "annotationTag:create", "annotationTag:read", "annotationTag:update", "annotationTag:delete", "annotationTag:list", "auditLogs:manage", "banner:dismiss",
        "credential:create", "credential:read", "credential:update", "credential:delete", "credential:list", "credential:share", "credential:unshare", "credential:shareGlobally", "credential:move", "credential:connect", "credential:createEndUser", "credential:manageInstance", "credential:use", "credentialTypePolicy:manage",
        "community:register", "communityPackage:install", "communityPackage:uninstall", "communityPackage:update", "communityPackage:list",
        "eventBusDestination:create", "eventBusDestination:read", "eventBusDestination:update", "eventBusDestination:delete", "eventBusDestination:list", "eventBusDestination:test",
        "externalSecretsProvider:create", "externalSecretsProvider:read", "externalSecretsProvider:update", "externalSecretsProvider:delete", "externalSecretsProvider:list", "externalSecretsProvider:sync", "externalSecret:list",
        "ldap:manage", "ldap:sync", "license:manage", "logStreaming:manage", "nodeTypePolicy:manage", "orchestration:read", "orchestration:manage", "saml:manage", "securityAudit:generate", "securitySettings:manage",
        "sourceControl:pull", "sourceControl:push", "sourceControl:manage", "sourceControl:read",
        "gitConnection:create", "gitConnection:read", "gitConnection:update", "gitConnection:delete", "gitConnection:list", "gitConnection:clone", "gitConnection:push", "gitConnection:manageProjects", "gitConnection:pull",
        "tag:create", "tag:read", "tag:update", "tag:delete", "tag:list",
        "user:create", "user:read", "user:update", "user:delete", "user:list", "user:resetPassword", "user:generateInviteLink", "user:changeRole", "user:enforceMfa",
        "variable:create", "variable:read", "variable:update", "variable:delete", "variable:list", "projectVariable:create", "projectVariable:read", "projectVariable:update", "projectVariable:delete", "projectVariable:list",
        "aiPreference:create", "aiPreference:read", "aiPreference:update", "aiPreference:delete", "aiPreference:list", "projectAiPreference:create", "projectAiPreference:read", "projectAiPreference:update", "projectAiPreference:delete", "projectAiPreference:list",
        "workflow:create", "workflow:read", "workflow:export", "workflow:import", "workflow:update", "workflow:publish", "workflow:unpublish", "workflow:delete", "workflow:list", "workflow:share", "workflow:unshare", "workflow:execute", "workflow:execute-chat", "workflow:move", "workflow:enableRedaction", "workflow:disableRedaction",
        "workersView:manage", "project:list", "project:create", "project:read", "project:update", "project:delete", "project:export", "project:manageMembers",
        "insights:list", "insights:read", "folder:move", "folder:read", "folder:update", "folder:delete", "folder:create", "folder:list", "oidc:manage", "provisioning:manage",
        "dataTable:create", "dataTable:delete", "dataTable:read", "dataTable:update", "dataTable:list", "dataTable:listProject", "dataTable:readRow", "dataTable:writeRow", "dataTable:readColumn", "dataTable:writeColumn",
        "role:manage", "role:read", "role:list", "mcp:manage", "mcp:oauth", "mcpApiKey:create", "mcpApiKey:rotate",
        "chatHub:manage", "chatHub:message", "chatHubAgent:create", "chatHubAgent:read", "chatHubAgent:update", "chatHubAgent:delete", "chatHubAgent:list",
        "breakingChanges:list", "breakingChanges:migrate", "execution:read", "execution:list", "execution:reveal", "execution:delete",
        "apiKey:manage", "apiKey:create", "apiKey:update", "encryptionKey:manage",
        "credentialResolver:create", "credentialResolver:read", "credentialResolver:update", "credentialResolver:delete", "credentialResolver:list",
        "instanceAi:message", "instanceAi:manage", "instanceAi:gateway", "instanceAi:eval", "roleMappingRule:create", "roleMappingRule:read", "roleMappingRule:update", "roleMappingRule:delete", "roleMappingRule:list", "otel:manage"
    ]
}

pub fn personal_project_payload() -> Value {
    json!({
        "id": "asT9jsAO0Pvbl5wZ",
        "name": "Personal",
        "type": "personal",
        "createdAt": "2026-10-14T00:00:00.000Z",
        "updatedAt": "2026-10-14T00:00:00.000Z",
        "role": "project:personalOwner",
        "icon": null,
        "scopes": project_scopes_array()
    })
}

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
            "globalScopes": global_scopes_array(),
            "personalProject": personal_project_payload(),
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
        "data": [ personal_project_payload() ]
    }))
}

pub async fn my_projects() -> impl IntoResponse {
    Json(json!({
        "data": [ personal_project_payload() ]
    }))
}

pub async fn get_projects_count() -> impl IntoResponse {
    Json(json!({
        "data": 1
    }))
}

pub async fn get_personal_project() -> impl IntoResponse {
    Json(json!({
        "data": personal_project_payload()
    }))
}

pub async fn get_project_by_id(Path(_id): Path<String>) -> impl IntoResponse {
    Json(json!({
        "data": personal_project_payload()
    }))
}

pub async fn get_sharing_candidates() -> impl IntoResponse {
    Json(json!({
        "data": []
    }))
}

pub async fn list_users() -> impl IntoResponse {
    Json(json!({
        "data": {
            "items": [
                {
                    "id": "owner-admin-id",
                    "email": "admin@n8n-rust.local",
                    "firstName": "Admin",
                    "lastName": "Owner",
                    "isOwner": true,
                    "isPending": false,
                    "role": "global:owner"
                }
            ],
            "count": 1
        }
    }))
}

pub async fn get_favorites() -> impl IntoResponse {
    Json(json!({ "data": [] }))
}

pub async fn toggle_favorite() -> impl IntoResponse {
    Json(json!({ "data": true }))
}

pub async fn insights_summary() -> impl IntoResponse {
    Json(json!({
        "data": {
            "total": { "value": 0, "deviation": 0 },
            "failed": { "value": 0, "deviation": 0 },
            "timeSaved": { "value": 0, "deviation": 0 },
            "averageRunTime": { "value": 0, "deviation": 0 },
            "failureRate": { "value": 0, "deviation": 0 }
        }
    }))
}

pub async fn data_tables_global_limits() -> impl IntoResponse {
    Json(json!({
        "data": {
            "dataTables": {},
            "totalSize": 0,
            "maxTableCount": -1,
            "maxRowCount": -1
        }
    }))
}

pub async fn list_data_tables_global() -> impl IntoResponse {
    Json(json!({
        "data": [],
        "count": 0
    }))
}

pub async fn instance_ai_threads() -> impl IntoResponse {
    Json(json!({ "data": [] }))
}

pub async fn mcp_settings_stub() -> impl IntoResponse {
    Json(json!({
        "data": {
            "mcpAccessEnabled": false,
            "autoExposeNewWorkflows": false
        }
    }))
}

pub async fn mcp_api_key_stub() -> impl IntoResponse {
    Json(json!({ "data": null }))
}

pub async fn saml_config_stub() -> impl IntoResponse {
    Json(json!({
        "data": {
            "loginEnabled": false,
            "loginLabel": ""
        }
    }))
}

pub async fn ldap_config_stub() -> impl IntoResponse {
    Json(json!({
        "data": {
            "loginEnabled": false,
            "loginLabel": ""
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

fn get_code_node_type() -> Value {
    json!({
        "name": "n8n-nodes-base.code",
        "displayName": "Code",
        "icon": "fa:code",
        "group": ["transform"],
        "version": 2,
        "description": "Runs custom JavaScript or Python code",
        "defaults": { "name": "Code" },
        "inputs": ["main"],
        "outputs": ["main"],
        "properties": [
            {
                "displayName": "Language",
                "name": "language",
                "type": "options",
                "default": "javascript",
                "options": [
                    { "name": "JavaScript", "value": "javascript" },
                    { "name": "Python", "value": "python" },
                    { "name": "WebAssembly (WASM)", "value": "wasm" },
                    { "name": "Rust Script", "value": "rust-script" }
                ]
            },
            {
                "displayName": "Runtime Version",
                "name": "runtimeVersion",
                "type": "options",
                "default": "es2022-quickjs",
                "options": [
                    { "name": "JavaScript ES2022 (QuickJS)", "value": "es2022-quickjs" },
                    { "name": "JavaScript ES2024 (V8 Compat)", "value": "es2024-v8-compat" },
                    { "name": "Python 3.11 Sandbox", "value": "3.11-sandbox" },
                    { "name": "Python 3.12 (PyO3)", "value": "3.12-pyo3" },
                    { "name": "WASM (Wasmer 4.0)", "value": "wasm32-wasmer-4.0" },
                    { "name": "WASM (WASI Preview 2)", "value": "wasm32-wasi" },
                    { "name": "Rust Script Nightly", "value": "rust-script-nightly" }
                ]
            },
            {
                "displayName": "Mode",
                "name": "mode",
                "type": "options",
                "default": "runOnceForAllItems",
                "options": [
                    { "name": "Run Once for All Items", "value": "runOnceForAllItems" },
                    { "name": "Run Once for Each Item", "value": "runOnceForEachItem" }
                ]
            },
            {
                "displayName": "JavaScript Code",
                "name": "jsCode",
                "type": "string",
                "default": "return $input.all();"
            },
            {
                "displayName": "Python Code",
                "name": "pythonCode",
                "type": "string",
                "default": "return _input.all()"
            },
            {
                "displayName": "WASM / WASI Module Code",
                "name": "wasmCode",
                "type": "string",
                "default": "return $input.all();"
            },
            {
                "displayName": "Polyglot Script Code",
                "name": "code",
                "type": "string",
                "default": ""
            }
        ]
    })
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
            "properties": [
                {
                    "displayName": "Notice",
                    "name": "notice",
                    "type": "notice",
                    "default": ""
                }
            ]
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
            "properties": [
                {
                    "displayName": "Trigger Rules",
                    "name": "rule",
                    "type": "fixedCollection",
                    "default": {}
                },
                {
                    "displayName": "Interval (Seconds)",
                    "name": "intervalSecs",
                    "type": "number",
                    "default": 60
                },
                {
                    "displayName": "Cron Expression",
                    "name": "cronExpression",
                    "type": "string",
                    "default": ""
                }
            ]
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
            "properties": [
                {
                    "displayName": "Path",
                    "name": "path",
                    "type": "string",
                    "default": ""
                },
                {
                    "displayName": "HTTP Method",
                    "name": "httpMethod",
                    "type": "options",
                    "default": "GET",
                    "options": [
                        { "name": "GET", "value": "GET" },
                        { "name": "POST", "value": "POST" },
                        { "name": "PUT", "value": "PUT" },
                        { "name": "DELETE", "value": "DELETE" },
                        { "name": "PATCH", "value": "PATCH" }
                    ]
                },
                {
                    "displayName": "Response Mode",
                    "name": "responseMode",
                    "type": "options",
                    "default": "onReceived",
                    "options": [
                        { "name": "On Received", "value": "onReceived" },
                        { "name": "When Last Node Finishes", "value": "responseNode" }
                    ]
                }
            ]
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
            "properties": [
                {
                    "displayName": "URL",
                    "name": "url",
                    "type": "string",
                    "default": "https://api.example.com"
                },
                {
                    "displayName": "Method",
                    "name": "method",
                    "type": "options",
                    "default": "GET",
                    "options": [
                        { "name": "GET", "value": "GET" },
                        { "name": "POST", "value": "POST" },
                        { "name": "PUT", "value": "PUT" },
                        { "name": "DELETE", "value": "DELETE" },
                        { "name": "PATCH", "value": "PATCH" },
                        { "name": "HEAD", "value": "HEAD" }
                    ]
                },
                {
                    "displayName": "Headers",
                    "name": "headers",
                    "type": "fixedCollection",
                    "default": {}
                },
                {
                    "displayName": "Body",
                    "name": "body",
                    "type": "string",
                    "default": ""
                },
                {
                    "displayName": "Max Tries",
                    "name": "maxTries",
                    "type": "number",
                    "default": 1
                },
                {
                    "displayName": "Wait Between Tries (ms)",
                    "name": "waitBetweenTries",
                    "type": "number",
                    "default": 1000
                }
            ]
        },
        get_code_node_type(),
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
            "properties": [
                {
                    "displayName": "Values to Set",
                    "name": "values",
                    "type": "fixedCollection",
                    "default": {}
                },
                {
                    "displayName": "Keep Only Set",
                    "name": "keepOnlySet",
                    "type": "boolean",
                    "default": false
                }
            ]
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
            "properties": [
                {
                    "displayName": "Conditions",
                    "name": "conditions",
                    "type": "filter",
                    "default": {}
                }
            ]
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
            "properties": [
                {
                    "displayName": "Rules",
                    "name": "rules",
                    "type": "fixedCollection",
                    "default": []
                },
                {
                    "displayName": "Fallback Output",
                    "name": "fallbackOutput",
                    "type": "options",
                    "default": -1,
                    "options": [
                        { "name": "None", "value": -1 },
                        { "name": "Output 0", "value": 0 },
                        { "name": "Output 1", "value": 1 },
                        { "name": "Output 2", "value": 2 },
                        { "name": "Output 3", "value": 3 }
                    ]
                }
            ]
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
            "properties": [
                {
                    "displayName": "Mode",
                    "name": "mode",
                    "type": "options",
                    "default": "append",
                    "options": [
                        { "name": "Append", "value": "append" },
                        { "name": "Combine", "value": "combine" },
                        { "name": "Choose Branch", "value": "chooseBranch" }
                    ]
                }
            ]
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
            "properties": [
                {
                    "displayName": "Workflow ID",
                    "name": "workflowId",
                    "type": "string",
                    "default": ""
                }
            ]
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
            "properties": [
                {
                    "displayName": "Response Code",
                    "name": "responseCode",
                    "type": "number",
                    "default": 200
                },
                {
                    "displayName": "Response Headers",
                    "name": "responseHeaders",
                    "type": "fixedCollection",
                    "default": {}
                },
                {
                    "displayName": "Response Body",
                    "name": "responseBody",
                    "type": "string",
                    "default": ""
                }
            ]
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
            "properties": [
                {
                    "displayName": "Field to Split Out",
                    "name": "fieldToSplitOut",
                    "type": "string",
                    "default": "items"
                }
            ]
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
            "properties": [
                {
                    "displayName": "Destination Field Name",
                    "name": "destinationFieldName",
                    "type": "string",
                    "default": "data"
                }
            ]
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
            "properties": [
                {
                    "displayName": "Compare Field",
                    "name": "compareField",
                    "type": "string",
                    "default": ""
                }
            ]
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
            "properties": [
                {
                    "displayName": "Batch Size",
                    "name": "batchSize",
                    "type": "number",
                    "default": 10
                },
                {
                    "displayName": "Batch Index",
                    "name": "batchIndex",
                    "type": "number",
                    "default": 0
                }
            ]
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
            "properties": [
                {
                    "displayName": "Wait Amount (ms)",
                    "name": "amount",
                    "type": "number",
                    "default": 1000
                }
            ]
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
            "properties": [
                {
                    "displayName": "Error Message",
                    "name": "errorMessage",
                    "type": "string",
                    "default": "Workflow stopped by StopAndError node"
                }
            ]
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
            "properties": [
                {
                    "displayName": "Notice",
                    "name": "notice",
                    "type": "notice",
                    "default": ""
                }
            ]
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
            "properties": [
                {
                    "displayName": "Sort Field",
                    "name": "sortField",
                    "type": "string",
                    "default": "id"
                },
                {
                    "displayName": "Order",
                    "name": "order",
                    "type": "options",
                    "default": "ascending",
                    "options": [
                        { "name": "Ascending", "value": "ascending" },
                        { "name": "Descending", "value": "descending" }
                    ]
                }
            ]
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
            "properties": [
                {
                    "displayName": "Max Items",
                    "name": "maxItems",
                    "type": "number",
                    "default": 1
                },
                {
                    "displayName": "Keep",
                    "name": "keep",
                    "type": "options",
                    "default": "firstItems",
                    "options": [
                        { "name": "First Items", "value": "firstItems" },
                        { "name": "Last Items", "value": "lastItems" }
                    ]
                }
            ]
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
            "properties": [
                {
                    "displayName": "Field",
                    "name": "field",
                    "type": "string",
                    "default": ""
                },
                {
                    "displayName": "Operator",
                    "name": "operator",
                    "type": "options",
                    "default": "equals",
                    "options": [
                        { "name": "Equals", "value": "equals" },
                        { "name": "Not Equals", "value": "notEquals" },
                        { "name": "Contains", "value": "contains" },
                        { "name": "Larger", "value": "larger" },
                        { "name": "Larger Equal", "value": "largerEqual" },
                        { "name": "Smaller", "value": "smaller" },
                        { "name": "Smaller Equal", "value": "smallerEqual" },
                        { "name": "Exists", "value": "exists" }
                    ]
                },
                {
                    "displayName": "Value",
                    "name": "value",
                    "type": "string",
                    "default": ""
                },
                {
                    "displayName": "Conditions",
                    "name": "conditions",
                    "type": "filter",
                    "default": {}
                }
            ]
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

// ---------------------------------------------------------------------------
// Dynamic Modular Runtime Manager Handlers
// ---------------------------------------------------------------------------

pub async fn list_runtimes_handler() -> impl IntoResponse {
    let runtimes = crate::runtime::list_runtimes();
    Json(json!({ "data": runtimes }))
}

pub async fn download_runtime_handler(Json(payload): Json<Value>) -> impl IntoResponse {
    let language = match payload.get("language").and_then(|v| v.as_str()) {
        Some(l) if !l.trim().is_empty() => l.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Missing or empty 'language'" })),
            )
                .into_response()
        }
    };
    let version = match payload.get("version").and_then(|v| v.as_str()) {
        Some(v) if !v.trim().is_empty() => v.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Missing or empty 'version'" })),
            )
                .into_response()
        }
    };

    match crate::runtime::download_runtime(language, version) {
        Ok(pkg) => (
            StatusCode::OK,
            Json(json!({ "success": true, "data": pkg })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": err })),
        )
            .into_response(),
    }
}

pub async fn delete_runtime_path_handler(
    Path((language, version)): Path<(String, String)>,
) -> impl IntoResponse {
    match crate::runtime::delete_runtime(&language, &version) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "success": true })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": err })),
        )
            .into_response(),
    }
}

pub async fn delete_runtime_post_handler(Json(payload): Json<Value>) -> impl IntoResponse {
    let language = match payload.get("language").and_then(|v| v.as_str()) {
        Some(l) if !l.trim().is_empty() => l.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Missing or empty 'language'" })),
            )
                .into_response()
        }
    };
    let version = match payload.get("version").and_then(|v| v.as_str()) {
        Some(v) if !v.trim().is_empty() => v.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Missing or empty 'version'" })),
            )
                .into_response()
        }
    };

    match crate::runtime::delete_runtime(language, version) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "success": true })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": err })),
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
    Json(json!({
        "data": {
            "instance-ai": {
                "enabled": true,
                "setupCompleted": true
            },
            "agents": {
                "enabled": true,
                "modules": ["tools", "memory", "subagents"]
            },
            "chat-hub": {
                "enabled": false
            },
            "data-table": {
                "enabled": true
            },
            "insights": {
                "enabled": true
            },
            "mcp": {
                "enabled": true,
                "mcpAccessEnabled": true
            }
        }
    }))
}

pub async fn source_control_preferences() -> impl IntoResponse {
    Json(json!({
        "data": {
            "branchName": "main",
            "branches": [],
            "repositoryUrl": "",
            "branchReadOnly": false,
            "connected": false,
            "publicKey": ""
        }
    }))
}

pub async fn source_control_status() -> impl IntoResponse {
    Json(json!({
        "data": {
            "ahead": 0,
            "behind": 0,
            "status": "clean",
            "files": []
        }
    }))
}

pub async fn source_control_branches() -> impl IntoResponse {
    Json(json!({ "data": [] }))
}

pub async fn workflow_dependencies_counts() -> impl IntoResponse {
    Json(json!({
        "data": {
            "workflows": {},
            "credentials": {}
        }
    }))
}

fn get_types_file_path(filename: &str) -> std::path::PathBuf {
    if let Ok(env_path) = std::env::var("N8N_UI_DIST") {
        let p = std::path::PathBuf::from(env_path).join("types").join(filename);
        if p.is_file() {
            return p;
        }
    }
    let local = std::path::PathBuf::from("ui-dist").join("types").join(filename);
    if local.is_file() {
        return local;
    }
    local
}

pub async fn types_nodes_json(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    let cached_path = get_types_file_path("nodes.json");
    if cached_path.is_file() {
        if let Ok(content) = std::fs::read_to_string(&cached_path) {
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
                content,
            )
                .into_response();
        }
    }
    let catalog = get_core_node_types_array();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        serde_json::to_string(&catalog).unwrap_or_else(|_| "[]".to_string()),
    )
        .into_response()
}

pub async fn types_credentials_json() -> impl IntoResponse {
    let cached_path = get_types_file_path("credentials.json");
    if cached_path.is_file() {
        if let Ok(content) = std::fs::read_to_string(&cached_path) {
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
                content,
            )
                .into_response();
        }
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        "[]".to_string(),
    )
        .into_response()
}

pub async fn types_node_versions_json() -> impl IntoResponse {
    let cached_path = get_types_file_path("node-versions.json");
    if cached_path.is_file() {
        if let Ok(content) = std::fs::read_to_string(&cached_path) {
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
                content,
            )
                .into_response();
        }
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        "[]".to_string(),
    )
        .into_response()
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

pub fn enrich_workflow(wf: &WorkflowRecord) -> Value {
    let mut val = serde_json::to_value(wf).unwrap_or(Value::Null);
    if let Value::Object(ref mut map) = val {
        map.entry("homeProject".to_string()).or_insert_with(|| {
            json!({
                "id": "asT9jsAO0Pvbl5wZ",
                "name": "Personal",
                "type": "personal",
                "icon": null
            })
        });
        map.entry("sharedWithProjects".to_string()).or_insert_with(|| json!([]));
        map.entry("scopes".to_string()).or_insert_with(|| {
            json!([
                "workflow:create",
                "workflow:delete",
                "workflow:execute",
                "workflow:move",
                "workflow:read",
                "workflow:share",
                "workflow:update"
            ])
        });
        map.entry("usedCredentials".to_string()).or_insert_with(|| json!([]));
        map.entry("projectId".to_string()).or_insert_with(|| json!("asT9jsAO0Pvbl5wZ"));
        map.insert(
            "activeVersionId".to_string(),
            if wf.active {
                json!(wf.version_id)
            } else {
                Value::Null
            },
        );
        map.insert(
            "publicationStatus".to_string(),
            if wf.active {
                json!("published")
            } else {
                Value::Null
            },
        );
    }
    val
}

pub async fn get_workflow_new_name() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({ "data": { "name": "My workflow" } })),
    )
}

pub async fn get_workflow_history() -> impl IntoResponse {
    Json(json!({ "data": [], "count": 0 }))
}

pub async fn list_evaluations() -> impl IntoResponse {
    Json(json!({ "data": [] }))
}

pub async fn list_workflows(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.db.list_workflows() {
        Ok(list) => {
            let enriched: Vec<Value> = list.iter().map(enrich_workflow).collect();
            let count = enriched.len();
            (
                StatusCode::OK,
                Json(json!({ "data": enriched, "count": count })),
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
            (StatusCode::OK, Json(json!({ "data": enrich_workflow(&wf) }))).into_response()
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

pub async fn check_workflow_exists(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let exists = matches!(state.db.get_workflow(&id), Ok(Some(_)));
    (
        StatusCode::OK,
        Json(json!({
            "data": {
                "exists": exists
            }
        })),
    )
        .into_response()
}

pub async fn ai_build_credits() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "data": {
                "creditsQuota": 100,
                "creditsClaimed": 0
            }
        })),
    )
        .into_response()
}

pub async fn community_node_types() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "data": []
        })),
    )
        .into_response()
}

pub async fn project_available_types() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "data": []
        })),
    )
        .into_response()
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
            (StatusCode::OK, Json(json!({ "data": enrich_workflow(&wf) }))).into_response()
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
            (StatusCode::OK, Json(json!({ "data": enrich_workflow(&updated) }))).into_response()
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
    if id == "for-workflow" {
        return (StatusCode::OK, Json(json!({ "data": [] }))).into_response();
    }
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

pub fn serde_to_flatted(v: &Value) -> flatted::Value {
    match v {
        Value::Null => flatted::Value::Null,
        Value::Bool(b) => flatted::Value::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                flatted::Value::Number(flatted::Number::Int(i))
            } else if let Some(u) = n.as_u64() {
                flatted::Value::Number(flatted::Number::UInt(u))
            } else if let Some(f) = n.as_f64() {
                flatted::Value::Number(flatted::Number::Float(f))
            } else {
                flatted::Value::Null
            }
        }
        Value::String(s) => flatted::Value::Str(s.clone()),
        Value::Array(arr) => {
            let items: Vec<flatted::Value> = arr.iter().map(serde_to_flatted).collect();
            flatted::Value::Array(std::rc::Rc::new(std::cell::RefCell::new(items)))
        }
        Value::Object(map) => {
            let mut obj = flatted::Object::new();
            for (k, val) in map {
                obj.insert(k.clone(), serde_to_flatted(val));
            }
            flatted::Value::Object(std::rc::Rc::new(std::cell::RefCell::new(obj)))
        }
    }
}

pub fn flatted_to_serde(v: &flatted::Value) -> Value {
    match v {
        flatted::Value::Null => Value::Null,
        flatted::Value::Bool(b) => Value::Bool(*b),
        flatted::Value::Number(n) => match n {
            flatted::Number::Int(i) => json!(i),
            flatted::Number::UInt(u) => json!(u),
            flatted::Number::Float(f) => json!(f),
        },
        flatted::Value::Str(s) => Value::String(s.clone()),
        flatted::Value::Array(arr) => {
            let borrowed = arr.borrow();
            let items: Vec<Value> = borrowed.iter().map(flatted_to_serde).collect();
            Value::Array(items)
        }
        flatted::Value::Object(obj) => {
            let borrowed = obj.borrow();
            let mut map = serde_json::Map::new();
            for (k, val) in borrowed.iter() {
                map.insert(k.clone(), flatted_to_serde(val));
            }
            Value::Object(map)
        }
    }
}

pub fn parse_flatted_data(raw: &str) -> Result<Value, String> {
    flatted::parse(raw, None)
        .map(|v| flatted_to_serde(&v))
        .map_err(|e| format!("Failed to parse flatted data: {:?}", e))
}

pub async fn list_executions(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let target_workflow_id = params.get("filter").and_then(|raw| {
        serde_json::from_str::<Value>(raw)
            .ok()
            .and_then(|v| v.get("workflowId").and_then(|w| w.as_str()).map(|s| s.to_string()))
    });

    match state.db.list_executions() {
        Ok(list) => {
            let filtered: Vec<_> = list
                .into_iter()
                .filter(|e| {
                    if let Some(ref w_id) = target_workflow_id {
                        e.workflow_id.as_deref() == Some(w_id)
                    } else {
                        true
                    }
                })
                .collect();

            let mut results = Vec::new();
            for exec in &filtered {
                let workflow_name = if let Some(ref w_id) = exec.workflow_id {
                    state
                        .db
                        .get_workflow(w_id)
                        .ok()
                        .flatten()
                        .map(|w| w.name)
                        .unwrap_or_else(|| "My workflow".to_string())
                } else {
                    "My workflow".to_string()
                };

                results.push(json!({
                    "id": exec.id,
                    "workflowId": exec.workflow_id,
                    "workflowVersionId": null,
                    "jsonSizeBytes": 2714,
                    "binaryDataSizeBytes": 0,
                    "mode": exec.mode,
                    "retryOf": null,
                    "status": exec.status,
                    "createdAt": exec.started_at,
                    "startedAt": exec.started_at,
                    "stoppedAt": exec.stopped_at,
                    "usedPrivateCredentials": false,
                    "waitTill": null,
                    "retrySuccessId": null,
                    "workflowName": workflow_name,
                    "annotation": {
                        "vote": null,
                        "tags": []
                    },
                    "scopes": [
                        "execution:delete",
                        "execution:list",
                        "execution:read",
                        "execution:reveal",
                        "workflow:create",
                        "workflow:delete",
                        "workflow:disableRedaction",
                        "workflow:enableRedaction",
                        "workflow:execute",
                        "workflow:execute-chat",
                        "workflow:export",
                        "workflow:import",
                        "workflow:list",
                        "workflow:move",
                        "workflow:publish",
                        "workflow:read",
                        "workflow:share",
                        "workflow:unpublish",
                        "workflow:unshare",
                        "workflow:update"
                    ]
                }));
            }

            let count = results.len();
            (
                StatusCode::OK,
                Json(json!({
                    "data": {
                        "results": results,
                        "nextCursor": null,
                        "count": count,
                        "estimated": false,
                        "concurrentExecutionsCount": 0
                    }
                })),
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
        Ok(Some(exec)) => {
            let wf_data = if let Some(ref w_id) = exec.workflow_id {
                state
                    .db
                    .get_workflow(w_id)
                    .ok()
                    .flatten()
                    .map(|w| {
                        json!({
                            "id": w.id,
                            "name": w.name,
                            "nodes": w.nodes,
                            "connections": w.connections,
                            "settings": w.settings,
                            "pinData": w.pin_data,
                            "nodeGroups": []
                        })
                    })
                    .unwrap_or_else(|| {
                        json!({
                            "id": w_id,
                            "name": "My workflow",
                            "nodes": [],
                            "connections": {},
                            "settings": {},
                            "pinData": {},
                            "nodeGroups": []
                        })
                    })
            } else {
                json!({
                    "id": "",
                    "name": "My workflow",
                    "nodes": [],
                    "connections": {},
                    "settings": {},
                    "pinData": {},
                    "nodeGroups": []
                })
            };

            let mut full_execution_data = if let Value::Object(map) = exec.data.clone() {
                map
            } else {
                serde_json::Map::new()
            };
            full_execution_data
                .entry("version".to_string())
                .or_insert_with(|| json!(1));
            full_execution_data
                .entry("startData".to_string())
                .or_insert_with(|| json!({}));
            if !full_execution_data.contains_key("resultData") {
                full_execution_data.insert("resultData".to_string(), json!({}));
            }
            full_execution_data
                .entry("executionData".to_string())
                .or_insert_with(|| {
                    json!({
                        "contextData": {},
                        "nodeExecutionStack": [],
                        "metadata": {},
                        "waitingExecution": {},
                        "waitingExecutionSource": {}
                    })
                });

            let full_val = Value::Object(full_execution_data);
            let flatted_val = serde_to_flatted(&full_val);
            let flatted_data_str = flatted::stringify(&flatted_val, None, None);

            (
                StatusCode::OK,
                Json(json!({
                    "data": {
                        "id": exec.id,
                        "finished": true,
                        "mode": exec.mode,
                        "retryOf": null,
                        "retrySuccessId": null,
                        "status": exec.status,
                        "createdAt": exec.started_at,
                        "startedAt": exec.started_at,
                        "stoppedAt": exec.stopped_at,
                        "deletedAt": null,
                        "workflowId": exec.workflow_id,
                        "waitTill": null,
                        "storedAt": "db",
                        "tracingContext": null,
                        "deduplicationKey": null,
                        "jsonSizeBytes": flatted_data_str.len(),
                        "binaryDataSizeBytes": 0,
                        "workflowVersionId": null,
                        "usedPrivateCredentials": false,
                        "customData": {},
                        "workflowData": wf_data,
                        "data": flatted_data_str
                    }
                })),
            )
                .into_response()
        }
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

pub async fn workflow_last_successful_execution(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let executions = state.db.list_executions().unwrap_or_default();
    let last = executions
        .into_iter()
        .find(|e| e.workflow_id.as_deref() == Some(&id) && e.status == "success");
    if let Some(exec) = last {
        if let Ok(Some(detail)) = state.db.get_execution(&exec.id) {
            return Json(json!({ "data": detail })).into_response();
        }
    }
    Json(json!({ "data": null })).into_response()
}

pub async fn workflow_write_lock_stub() -> impl IntoResponse {
    Json(json!({ "data": null }))
}

pub async fn workflow_publication_status() -> impl IntoResponse {
    Json(json!({ "data": { "status": "published", "failures": [] } }))
}

pub async fn workflow_history_version_stub() -> impl IntoResponse {
    Json(json!({ "data": null }))
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
            let initial_stream = futures_util::stream::once(async {
                Ok::<Event, Infallible>(Event::default().comment("ok"))
            });
            let broadcast_stream = futures_util::stream::unfold(rx, |mut rx| async move {
                match rx.recv().await {
                    Ok(msg) => Some((Ok::<Event, Infallible>(Event::default().data(msg)), rx)),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => Some((
                        Ok::<Event, Infallible>(Event::default().comment("lagged")),
                        rx,
                    )),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
                }
            });
            let stream = futures_util::stream::select(initial_stream, broadcast_stream);
            Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response()
        }
    }
}

async fn handle_websocket(mut socket: WebSocket, hub: Arc<PushHub>) {
    let mut rx = hub.subscribe();
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(WsMessage::Ping(payload))) => {
                        if socket.send(WsMessage::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | None | Some(Err(_)) => {
                        break;
                    }
                    Some(Ok(WsMessage::Text(_))) => {
                        // Client heartbeat / message received, keep socket alive
                    }
                    _ => {}
                }
            }
            broadcast = rx.recv() => {
                match broadcast {
                    Ok(msg) => {
                        if socket.send(WsMessage::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
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
                if !n.get("parameters").is_some_and(|p| p.is_object()) {
                    n["parameters"] = json!({});
                }
                if let Some(params) = n.get_mut("parameters").and_then(|p| p.as_object_mut()) {
                    params.insert("data".to_string(), json!(items));
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
    let workflow_id = payload
        .workflow_data
        .as_ref()
        .and_then(|w| w.get("id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    execute_workflow_helper(state, workflow_id, nodes, connections, settings).await
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
        .route("/rest/projects", get(list_projects).post(list_projects))
        .route("/rest/projects/count", get(get_projects_count))
        .route("/rest/projects/my-projects", get(my_projects))
        .route("/rest/projects/personal", get(get_personal_project))
        .route("/rest/projects/sharing-candidates", get(get_sharing_candidates))
        .route("/rest/projects/{id}", get(get_project_by_id).patch(get_project_by_id))
        .route("/rest/projects/{id}/available-types", get(project_available_types))
        .route("/rest/roles", get(list_roles))
        // Users, Favorites, Insights, Limits & MCP Stubs
        .route("/rest/users", get(list_users))
        .route("/rest/favorites", get(get_favorites).post(toggle_favorite))
        .route("/rest/favorites/{type}/{id}", delete(toggle_favorite))
        .route("/rest/insights/summary", get(insights_summary))
        .route("/rest/data-tables-global", get(list_data_tables_global))
        .route("/rest/data-tables-global/limits", get(data_tables_global_limits))
        .route("/rest/instance-ai/threads", get(instance_ai_threads))
        .route("/rest/mcp/settings", get(mcp_settings_stub).patch(mcp_settings_stub))
        .route("/rest/mcp/api-key", get(mcp_api_key_stub))
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
        .route("/rest/source-control/preferences", get(source_control_preferences))
        .route("/rest/source-control/status", get(source_control_status))
        .route("/rest/source-control/branches", get(source_control_branches))
        .route("/rest/workflow-dependencies/counts", get(workflow_dependencies_counts).post(workflow_dependencies_counts))
        .route("/types/nodes.json", get(types_nodes_json))
        .route("/types/credentials.json", get(types_credentials_json))
        .route("/types/node-versions.json", get(types_node_versions_json))
        // Workflows CRUD & Run
        .route("/rest/workflows", get(list_workflows).post(create_workflow))
        .route("/rest/workflows/run", post(run_workflow_direct))
        .route("/rest/workflows/new", get(get_workflow_new_name))
        .route("/rest/workflows/{id}/exists", get(check_workflow_exists))
        .route("/rest/ai/build/credits", get(ai_build_credits))
        .route("/rest/community-node-types", get(community_node_types))
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
        .route("/rest/workflows/{id}/history", get(get_workflow_history))
        .route("/rest/workflows/{id}/executions/last-successful", get(workflow_last_successful_execution))
        .route("/rest/workflows/{id}/collaboration/write-lock", get(workflow_write_lock_stub).post(workflow_write_lock_stub).delete(workflow_write_lock_stub))
        .route("/rest/workflows/{id}/publication-status", get(workflow_publication_status))
        .route("/rest/workflow-history/workflow/{id}/version/{version_id}", get(workflow_history_version_stub))
        .route("/rest/workflows/{id}/test-runs", get(list_evaluations).post(list_evaluations))
        .route("/rest/evaluations", get(list_evaluations))
        .route("/rest/evaluations/runs", get(list_evaluations))
        .route("/rest/evaluations/metrics", get(list_evaluations))
        // SSO SAML & LDAP Config Stubs
        .route("/rest/sso/saml/config", get(saml_config_stub).post(saml_config_stub))
        .route("/rest/ldap/config", get(ldap_config_stub).post(ldap_config_stub))
        // Community Packages CRUD
        .route(
            "/rest/community-packages",
            get(list_community_packages).post(install_community_package),
        )
        .route(
            "/rest/community-packages/{name}",
            delete(delete_community_package),
        )
        // Dynamic Runtime Manager
        .route("/rest/runtimes", get(list_runtimes_handler))
        .route("/rest/runtimes/download", post(download_runtime_handler))
        .route(
            "/rest/runtimes/{language}/{version}",
            delete(delete_runtime_path_handler),
        )
        .route("/rest/runtimes/delete", post(delete_runtime_post_handler))
        // Credentials CRUD
        .route(
            "/rest/credentials/for-workflow",
            get(list_credentials).post(list_credentials),
        )
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

