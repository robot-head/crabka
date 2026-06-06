//! HTTP surface: `AppState` + the merged Confluent route table.

pub mod compatibility;
pub mod config;
pub mod delete;
pub mod mode;
pub mod response;
pub mod schemas;
pub mod subjects;

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};

use crate::kafkastore::KafkaStore;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<KafkaStore>,
}

/// `?deleted=true` query toggle shared by the GET endpoints that can surface
/// soft-deleted rows.
#[derive(serde::Deserialize, Default)]
pub struct DeletedQ {
    #[serde(default)]
    pub deleted: bool,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { response::ok_json(&serde_json::json!({})) }),
        )
        .route("/schemas/types", get(schemas::types))
        .route("/schemas", get(schemas::list_schemas))
        .route("/schemas/ids/{id}", get(schemas::get_by_id))
        .route(
            "/schemas/ids/{id}/versions",
            get(schemas::get_by_id_versions),
        )
        .route("/subjects", get(subjects::list))
        .route(
            "/subjects/{subject}",
            post(subjects::lookup).delete(delete::delete_subject),
        )
        .route(
            "/subjects/{subject}/versions",
            get(subjects::versions).post(subjects::register),
        )
        .route(
            "/subjects/{subject}/versions/{version}",
            get(subjects::get_version).delete(delete::delete_version),
        )
        .route(
            "/subjects/{subject}/versions/{version}/schema",
            get(subjects::get_version_schema),
        )
        .route(
            "/subjects/{subject}/versions/{version}/referencedby",
            get(subjects::referencedby),
        )
        .route("/mode", get(mode::get_global).put(mode::put_global))
        .route(
            "/mode/{subject}",
            get(mode::get_subject)
                .put(mode::put_subject)
                .delete(mode::delete_subject),
        )
        .route("/config", get(config::get_global).put(config::put_global))
        .route(
            "/config/{subject}",
            get(config::get_subject).put(config::put_subject),
        )
        .route(
            "/compatibility/subjects/{subject}/versions/{version}",
            post(compatibility::check),
        )
        .with_state(state)
}
