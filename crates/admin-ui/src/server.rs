//! HTTP server helpers for the admin UI.

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;

pub fn health_router() -> Router {
    Router::new().route("/healthz", get(|| async { StatusCode::OK }))
}
