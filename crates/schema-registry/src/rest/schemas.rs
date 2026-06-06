//! `/schemas/*` read endpoints.

use axum::extract::{Path, Query, State};
use axum::response::Response;

use crate::error::SrError;
use crate::rest::{AppState, DeletedQ, response::ok_json};

/// GET /schemas/ids/{id}
pub async fn get_by_id(
    State(st): State<AppState>,
    Path(id): Path<i32>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let (ty, schema) = st
        .store
        .store
        .read()
        .schema_by_id(id, q.deleted)
        .ok_or(SrError::SchemaNotFound)?;
    let mut body = serde_json::Map::new();
    if let Some(t) = ty.wire_name() {
        body.insert("schemaType".into(), t.into());
    }
    body.insert("schema".into(), schema.into());
    Ok(ok_json(&serde_json::Value::Object(body)))
}

/// GET /schemas/types
// axum requires async handlers even when the body is synchronous.
#[allow(clippy::unused_async)]
pub async fn types(State(_st): State<AppState>) -> Response {
    ok_json(&serde_json::json!(["AVRO", "JSON", "PROTOBUF"]))
}

/// GET /schemas/ids/{id}/versions -> [{"subject":..,"version":..}]
#[allow(clippy::unused_async)]
pub async fn get_by_id_versions(
    State(st): State<AppState>,
    Path(id): Path<i32>,
    Query(q): Query<DeletedQ>,
) -> Response {
    let pairs = st
        .store
        .store
        .read()
        .schema_id_subject_versions(id, q.deleted);
    let arr: Vec<serde_json::Value> = pairs
        .into_iter()
        .map(|(subject, version)| serde_json::json!({ "subject": subject, "version": version }))
        .collect();
    ok_json(&serde_json::Value::Array(arr))
}

/// GET /schemas -> [{subject,version,id,schemaType,schema}]
#[allow(clippy::unused_async)]
pub async fn list_schemas(State(st): State<AppState>, Query(q): Query<DeletedQ>) -> Response {
    let rows = st.store.store.read().all_schemas(q.deleted);
    let arr: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(subject, version, id, ty, schema)| {
            let mut m = serde_json::Map::new();
            m.insert("subject".into(), subject.into());
            m.insert("version".into(), version.into());
            m.insert("id".into(), id.into());
            if let Some(t) = ty.wire_name() {
                m.insert("schemaType".into(), t.into());
            }
            m.insert("schema".into(), schema.into());
            serde_json::Value::Object(m)
        })
        .collect();
    ok_json(&serde_json::Value::Array(arr))
}
