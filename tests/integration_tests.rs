use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use n8n_rust_v5::build_app;
use std::path::Path;
use tower::ServiceExt;

#[tokio::test]
async fn test_health_and_settings_endpoints() {
    let app = build_app(Path::new("ui-dist"));

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

    // Test GET /rest/settings
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
    assert_eq!(body["data"]["versionCli"], "2.43.5");
    assert_eq!(body["data"]["endpointHealth"], "healthz");

    // Test GET /rest/login (401 for unauthenticated)
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

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_spa_index_html_clean_placeholders() {
    let app = build_app(Path::new("ui-dist"));

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

    // Test SPA fallback on arbitrary route (e.g. /workflow/new)
    let res_fallback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/workflow/new")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res_fallback.status(), StatusCode::OK);
    let bytes_fb = res_fallback.into_body().collect().await.unwrap().to_bytes();
    let html_fb = String::from_utf8(bytes_fb.to_vec()).unwrap();
    assert!(html_fb.contains("id=\"app\""));
}

#[tokio::test]
async fn test_static_asset_serving() {
    let app = build_app(Path::new("ui-dist"));

    // Test GET /static/base-path.js
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/static/base-path.js")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let js = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        js.contains("window.BASE_PATH = '/'"),
        "base-path.js should define root BASE_PATH"
    );
}
