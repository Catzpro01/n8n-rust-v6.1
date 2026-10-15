pub mod api;
pub mod config;
pub mod db;
pub mod engine;
pub mod expr;
pub mod web;

use api::{AppState, PushHub};
use axum::Router;
use dashmap::DashMap;
use db::StorageEngine;
use engine::HyperScaleEngine;
use std::path::Path;
use std::sync::Arc;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

/// Build the root Axum application with all API and Web static/SPA routes
pub fn build_app(ui_dist_path: &Path, db: StorageEngine) -> Router {
    if let Ok(workflows) = db.list_workflows() {
        for wf in workflows {
            engine::register_subworkflow(
                &wf.id,
                serde_json::json!({ "nodes": wf.nodes, "connections": wf.connections }),
            );
        }
    }

    let engine = HyperScaleEngine::new(None)
        .unwrap_or_else(|e| panic!("Failed to initialize HyperScaleEngine: {e}"));
    let push_hub = Arc::new(PushHub::new());
    let active_executions = Arc::new(DashMap::new());
    let app_state = Arc::new(AppState {
        db,
        engine,
        push_hub,
        active_executions,
    });

    if tokio::runtime::Handle::try_current().is_ok() {
        api::spawn_cron_scheduler_daemon(Arc::clone(&app_state));
    }

    let api_router = api::router(app_state);
    let web_router = web::router(ui_dist_path);

    Router::new()
        .merge(api_router)
        .merge(web_router)
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}
