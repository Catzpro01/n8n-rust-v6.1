use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde_json::{json, Value};

/// Static JSON payload representing full n8n settings
pub const SETTINGS_PAYLOAD: &str = r#"{
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
    "instanceId": "n8n-rust-v5-instance",
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
      "smtpSetup": false,
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
      "enabled": false,
      "latestVersion": 1,
      "path": "api/v1",
      "swaggerUi": {
        "enabled": false
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
      "enabled": false,
      "setup": false,
      "cloudUbbEnabled": false
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
      "sharing": false,
      "ldap": false,
      "saml": false,
      "oidc": false,
      "mfaEnforcement": false,
      "logStreaming": false,
      "advancedExecutionFilters": false,
      "variables": false,
      "sourceControl": false,
      "auditLogs": false,
      "externalSecrets": false,
      "showNonProdBanner": false,
      "debugInEditor": false,
      "binaryDataS3": false,
      "workerView": false,
      "advancedPermissions": false,
      "workflowDiffs": false,
      "namedVersions": false,
      "provisioning": false,
      "projects": {
        "team": {
          "limit": 0
        }
      },
      "customRoles": false,
      "personalSpacePolicy": false,
      "dataRedaction": false,
      "otelCustomSpanAttributes": false,
      "workflowReviews": false
    },
    "mfa": {
      "enabled": false,
      "enforced": false
    },
    "hideUsagePage": false,
    "license": {
      "consumerId": "unknown",
      "environment": "production"
    },
    "variables": {
      "limit": 0
    },
    "banners": {
      "dismissed": []
    },
    "askAi": {
      "enabled": false
    },
    "aiBuilder": {
      "enabled": false,
      "setup": false
    },
    "aiCredits": {
      "enabled": false,
      "credits": 0,
      "setup": false
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
    "easyAIWorkflowOnboarded": false,
    "folders": {
      "enabled": false
    },
    "workerPools": {
      "enabled": false
    },
    "evaluation": {
      "quota": 0,
      "collectionsEnabled": false,
      "configEvalsEnabled": false,
      "agentEvalsEnabled": false
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

/// Handler for `/healthz` and `/rest/healthz`
pub async fn healthz() -> Json<Value> {
    Json(json!({
        "status": "ok"
    }))
}

/// Handler for `/rest/settings` returning full expected n8n settings
pub async fn settings() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        SETTINGS_PAYLOAD,
    )
}

/// Handler for `/rest/login` returning 401 Unauthorized for initial guest state
pub async fn login_check() -> impl IntoResponse {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "code": 401,
            "message": "Unauthorized"
        })),
    )
}

/// Build api router for rest and health routes
pub fn router() -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/rest/healthz", get(healthz))
        .route("/rest/settings", get(settings))
        .route("/rest/login", get(login_check))
}
