//! HTTP surface: `AppState` + the merged Confluent route table.

pub mod compatibility;
pub mod config;
pub mod delete;
pub mod forward;
pub mod import;
pub mod mode;
pub mod response;
pub mod schemas;
pub mod serve;
pub mod subjects;

use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    response::Response,
    routing::{get, post},
};

use crate::{error::SrError, ids::SchemaVersion, kafkastore::KafkaStore};

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

fn schemas_types(state: State<AppState>) -> std::future::Ready<Response> {
    std::future::ready(schemas::types(state))
}

fn list_schemas(state: State<AppState>, query: Query<DeletedQ>) -> std::future::Ready<Response> {
    std::future::ready(schemas::list_schemas(state, query))
}

fn schema_versions(
    state: State<AppState>,
    id: Path<i32>,
    query: Query<DeletedQ>,
) -> std::future::Ready<Result<Response, SrError>> {
    std::future::ready(schemas::get_by_id_versions(state, id, query))
}

fn list_subjects(state: State<AppState>, query: Query<DeletedQ>) -> std::future::Ready<Response> {
    std::future::ready(subjects::list(state, query))
}

fn global_mode(state: State<AppState>) -> std::future::Ready<Response> {
    std::future::ready(mode::get_global(state))
}

fn subject_mode(
    state: State<AppState>,
    subject: Path<String>,
) -> std::future::Ready<Result<Response, SrError>> {
    std::future::ready(mode::get_subject(state, subject))
}

fn global_config(state: State<AppState>) -> std::future::Ready<Response> {
    std::future::ready(config::get_global(state))
}

fn subject_config(
    state: State<AppState>,
    subject: Path<String>,
) -> std::future::Ready<Result<Response, SrError>> {
    std::future::ready(config::get_subject(state, subject))
}

/// `latest` -> `None`; a positive integer -> `Some(n)`; else 42202.
fn parse_optional_version(v: &str) -> Result<Option<SchemaVersion>, SrError> {
    if v == "latest" {
        return Ok(None);
    }
    parse_concrete_version(v).map(Some)
}

fn parse_concrete_version(v: &str) -> Result<SchemaVersion, SrError> {
    match v.parse::<i32>() {
        Ok(n) if n >= 1 => Ok(SchemaVersion(n)),
        _ => Err(SrError::InvalidVersion(v.to_string())),
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { response::ok_json(&serde_json::json!({})) }),
        )
        .route("/schemas/types", get(schemas_types))
        .route("/schemas", get(list_schemas))
        .route("/schemas/import", post(import::file_descriptor_set))
        .route("/schemas/ids/{id}", get(schemas::get_by_id))
        .route("/schemas/ids/{id}/versions", get(schema_versions))
        .route("/schemas/ids/{id}/schema", get(schemas::get_by_id_schema))
        .route(
            "/schemas/ids/{id}/subjects",
            get(schemas::get_by_id_subjects),
        )
        .route("/subjects", get(list_subjects))
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
        .route("/mode", get(global_mode).put(mode::put_global))
        .route(
            "/mode/{subject}",
            get(subject_mode)
                .put(mode::put_subject)
                .delete(mode::delete_subject),
        )
        .route("/config", get(global_config).put(config::put_global))
        .route(
            "/config/{subject}",
            get(subject_config)
                .put(config::put_subject)
                .delete(config::delete_subject),
        )
        .route(
            "/compatibility/subjects/{subject}/versions/{version}",
            post(compatibility::check),
        )
        .with_state(state)
}

/// Wrap the router with the write-forwarding middleware (secondary → primary).
pub fn router_with_forwarding(state: AppState, fwd: forward::ForwardState) -> Router {
    router(state).layer(axum::middleware::from_fn_with_state(
        fwd,
        forward::forward_layer,
    ))
}

/// The three middleware components composed by [`router_with_security`].
pub struct SecurityLayers {
    /// Authentication (mTLS → Bearer → Basic → Anonymous).
    pub auth: crate::auth::AuthState,
    /// Topic-ACL authorization. `None` disables authz entirely (allow-all).
    pub authz: Option<std::sync::Arc<crate::authz::SchemaRegistryAuthz>>,
    /// Secondary → primary write-forwarding.
    pub forward: forward::ForwardState,
}

/// Router wrapped with the full security stack.
///
/// axum runs the *last*-added `.layer()` first, so to get an execution order of
/// auth → authz → forward → handler we add them in the reverse order (forward,
/// then authz, then auth on the outside). Authentication therefore runs first
/// and inserts the [`crabka_security::Principal`] that authorization reads.
pub fn router_with_security(state: AppState, sec: SecurityLayers) -> Router {
    let mut r = router(state).layer(axum::middleware::from_fn_with_state(
        sec.forward,
        forward::forward_layer,
    ));
    if let Some(az) = sec.authz {
        r = r.layer(axum::middleware::from_fn_with_state(
            az,
            crate::authz::authz_layer,
        ));
    }
    r.layer(axum::middleware::from_fn_with_state(
        std::sync::Arc::new(sec.auth),
        crate::auth::auth_layer,
    ))
}
