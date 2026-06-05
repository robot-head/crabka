//! HTTP surface: `AppState` + the merged Confluent route table.

pub mod compatibility;
pub mod config;
pub mod response;
pub mod schemas;
pub mod subjects;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use crate::kafkastore::KafkaStore;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<KafkaStore>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(|| async { response::ok_json(&serde_json::json!({})) }))
        .route("/schemas/types", get(schemas::types))
        .route("/schemas/ids/{id}", get(schemas::get_by_id))
        .route("/subjects", get(subjects::list))
        .route("/subjects/{subject}", post(subjects::lookup))
        .route(
            "/subjects/{subject}/versions",
            get(subjects::versions).post(subjects::register),
        )
        .route("/subjects/{subject}/versions/{version}", get(subjects::get_version))
        .route(
            "/subjects/{subject}/versions/{version}/schema",
            get(subjects::get_version_schema),
        )
        .route("/config", get(config::get_global).put(config::put_global))
        .route("/config/{subject}", get(config::get_subject).put(config::put_subject))
        .route(
            "/compatibility/subjects/{subject}/versions/{version}",
            post(compatibility::check),
        )
        .with_state(state)
}
