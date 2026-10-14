use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tower_http::services::{ServeDir, ServeFile};

/// Process template placeholders in index.html to produce clean browser-ready HTML.
pub fn process_index_html(raw_html: &str, base_path: &str) -> String {
    let clean_base = if base_path == "/" || base_path.is_empty() {
        "/"
    } else {
        base_path
    };

    let config_tags = r#"<meta name="n8n:config:rest-endpoint" content="cmVzdA=="><meta name="n8n:config:sentry" content="eyJkc24iOiIiLCJlbnZpcm9ubWVudCI6InByb2R1Y3Rpb24iLCJyZWxlYXNlIjoibjhuQDIuNDMuNSJ9">"#;

    let mut result = raw_html.to_string();
    result = result.replace("/{{BASE_PATH}}/", clean_base);
    result = result.replace("/%7B%7BBASE_PATH%7D%7D/", clean_base);
    result = result.replace("/%257B%257BBASE_PATH%257D%257D/", clean_base);
    result = result.replace(
        "{{BASE_PATH}}",
        if clean_base == "/" { "" } else { clean_base },
    );
    result = result.replace("%CONFIG_TAGS%", config_tags);
    result = result.replace("nonce=\"{{CSP_NONCE}}\"", "");
    result = result.replace("{{CSP_NONCE}}", "");
    result = result.replace("{{REST_ENDPOINT}}", "rest");
    result
}

#[derive(Clone)]
pub struct WebState {
    pub index_html: Arc<str>,
    pub ui_dist_path: PathBuf,
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
        }
    }
}

pub async fn spa_index_handler(State(state): State<WebState>) -> impl IntoResponse {
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
}

pub fn router(ui_dist_path: &Path) -> Router {
    let state = WebState::new(ui_dist_path);

    let assets_dir = ui_dist_path.join("assets");
    let static_dir = ui_dist_path.join("static");
    let favicon_file = ui_dist_path.join("favicon.ico");
    let ts_wasm_file = ui_dist_path.join("tree-sitter.wasm");
    let ts_bash_wasm_file = ui_dist_path.join("tree-sitter-bash.wasm");

    Router::new()
        .route("/", get(spa_index_handler))
        .nest_service("/assets", ServeDir::new(assets_dir))
        .nest_service("/static", ServeDir::new(static_dir))
        .route_service("/favicon.ico", ServeFile::new(favicon_file))
        .route_service("/tree-sitter.wasm", ServeFile::new(ts_wasm_file))
        .route_service("/tree-sitter-bash.wasm", ServeFile::new(ts_bash_wasm_file))
        .fallback(spa_index_handler)
        .with_state(state)
}
