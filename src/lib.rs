pub mod api;
pub mod config;
pub mod db;
pub mod web;

use api::AppState;
use axum::Router;
use db::StorageEngine;
use std::path::Path;
use std::sync::Arc;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

/// Build the root Axum application with all API and Web static/SPA routes
pub fn build_app(ui_dist_path: &Path, db: StorageEngine) -> Router {
    let app_state = Arc::new(AppState { db });
    let api_router = api::router(app_state);
    let web_router = web::router(ui_dist_path);

    Router::new()
        .merge(api_router)
        .merge(web_router)
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}
