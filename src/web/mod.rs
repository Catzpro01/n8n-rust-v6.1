use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode, Uri},
    response::{Html, IntoResponse, Json, Response},
    Router,
};
use dashmap::DashMap;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Process template placeholders in index.html to produce clean browser-ready HTML.
pub fn process_index_html(raw_html: &str, base_path: &str) -> String {
    let clean_base = if base_path == "/" || base_path.is_empty() {
        "/"
    } else {
        base_path
    };

    let config_tags = r#"<meta name="n8n:config:rest-endpoint" content="cmVzdA=="><meta name="n8n:config:sentry" content="eyJkc24iOiIiLCJlbnZpcm9ubWVudCI6ImRldmVsb3BtZW50IiwicmVsZWFzZSI6Im44bkAyLjQzLjUifQ==">"#;

    let mut result = raw_html.to_string();
    result = result.replace("/{{BASE_PATH}}/", clean_base);
    result = result.replace("/%7B%7BBASE_PATH%7D%7D/", clean_base);
    result = result.replace("/%257B%257BBASE_PATH%257D%257D/", clean_base);
    result = result.replace(
        "{{BASE_PATH}}",
        if clean_base == "/" { "" } else { clean_base },
    );
    result = result.replace("%CONFIG_TAGS%", config_tags);
    result = result.replace("{{CSP_NONCE}}", "KCdxw_Zff9j5iTmpwxv0rQ");
    result = result.replace("{{REST_ENDPOINT}}", "rest");
    result
}

pub type CachedFile = (String, Arc<[u8]>);

#[derive(Clone)]
pub struct WebState {
    pub index_html: Arc<str>,
    pub ui_dist_path: PathBuf,
    pub file_cache: Arc<DashMap<PathBuf, CachedFile>>,
}

impl WebState {
    pub fn new(ui_dist_path: impl AsRef<Path>) -> Self {
        let ui_dist_path = ui_dist_path.as_ref().to_path_buf();
        let index_file = ui_dist_path.join("index.html");
        let raw_html = std::fs::read_to_string(&index_file).unwrap_or_else(|_| {
            tracing::warn!(path = ?index_file, "index.html not found, using fallback stub");
            "<!DOCTYPE html><html><head><title>n8n</title></head><body><div id=\"app\"></div></body></html>".to_string()
        });

        let processed = process_index_html(&raw_html, "/");
        Self {
            index_html: Arc::from(processed),
            ui_dist_path,
            file_cache: Arc::new(DashMap::new()),
        }
    }
}

fn get_mime_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).unwrap_or("") {
        "js" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "wasm" => "application/wasm",
        "json" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "ico" => "image/x-icon",
        "html" => "text/html; charset=utf-8",
        _ => "application/octet-stream",
    }
}

pub async fn static_or_spa_handler(
    State(state): State<WebState>,
    uri: Uri,
) -> Response {
    let mut path = uri.path();

    // 1. API routes that reach web router -> 404 JSON
    if path.starts_with("/rest/") || path.starts_with("/api/") {
        return (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "application/json")],
            Json(json!({ "message": "Fitur belum tersedia di engine n8n-rust" })),
        )
            .into_response();
    }

    // Health check fast-path for editor frontend polling (e.g. /healthz or /workflow/healthz)
    if path == "/healthz" || path.ends_with("/healthz") {
        return (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
            Json(json!({ "status": "ok" })),
        )
            .into_response();
    }

    // 2. Strip any BASE_PATH prefix patterns
    loop {
        if let Some(rest) = path.strip_prefix("/%7B%7BBASE_PATH%7D%7D") {
            path = rest;
        } else if let Some(rest) = path.strip_prefix("/{{BASE_PATH}}") {
            path = rest;
        } else if let Some(rest) = path.strip_prefix("/%257B%257BBASE_PATH%257D%257D") {
            path = rest;
        } else {
            break;
        }
    }
    if path.is_empty() {
        path = "/";
    }

    // 3. Root path -> serve processed index.html
    if path == "/" {
        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (
                    header::CACHE_CONTROL,
                    "no-cache, no-store, must-revalidate, proxy-revalidate",
                ),
            ],
            Html(state.index_html.to_string()),
        )
            .into_response();
    }

    // 4. Special handler for base-path.js
    if path == "/static/base-path.js" {
        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/javascript; charset=utf-8"),
                (
                    header::CACHE_CONTROL,
                    "no-cache, no-store, must-revalidate",
                ),
            ],
            "window.BASE_PATH = '/';",
        )
            .into_response();
    }

    // 5. Look for file on disk in ui_dist_path
    let rel = path.trim_start_matches('/');
    let file_path = state.ui_dist_path.join(rel);

    if file_path.is_file() {
        if let Some(cached) = state.file_cache.get(&file_path) {
            let (mime, bytes) = cached.value();
            return (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, mime.as_str()),
                    (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
                ],
                Body::from(Arc::clone(bytes).to_vec()),
            )
                .into_response();
        }

        let mime = get_mime_type(&file_path);
        let is_transformable = file_path
            .extension()
            .is_some_and(|ext| ext == "css" || ext == "js" || ext == "html");

        let bytes = if is_transformable {
            match std::fs::read_to_string(&file_path) {
                Ok(content) => {
                    let mut transformed = content;
                    transformed = transformed.replace("/{{BASE_PATH}}/", "/");
                    transformed = transformed.replace("/%7B%7BBASE_PATH%7D%7D/", "/");
                    transformed = transformed.replace("/%257B%257BBASE_PATH%257D%257D/", "/");
                    transformed = transformed.replace("{{BASE_PATH}}", "");
                    Arc::from(transformed.into_bytes().into_boxed_slice())
                }
                Err(_) => match std::fs::read(&file_path) {
                    Ok(raw) => Arc::from(raw.into_boxed_slice()),
                    Err(_) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Failed to read static asset",
                        )
                            .into_response()
                    }
                },
            }
        } else {
            match std::fs::read(&file_path) {
                Ok(raw) => Arc::from(raw.into_boxed_slice()),
                Err(_) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to read static asset",
                    )
                        .into_response()
                }
            }
        };

        state
            .file_cache
            .insert(file_path, (mime.to_string(), Arc::clone(&bytes)));

        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, mime),
                (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
            ],
            Body::from(bytes.to_vec()),
        )
            .into_response();
    }

    // 6. SPA fallback for application routes (e.g. /home/workflows, /workflow/new)
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (
                header::CACHE_CONTROL,
                "no-cache, no-store, must-revalidate, proxy-revalidate",
            ),
        ],
        Html(state.index_html.to_string()),
    )
        .into_response()
}

pub fn router(ui_dist_path: &Path) -> Router {
    let state = WebState::new(ui_dist_path);

    Router::new()
        .fallback(static_or_spa_handler)
        .with_state(state)
}
