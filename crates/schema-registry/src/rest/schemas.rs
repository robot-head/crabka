//! `/schemas/*` read endpoints.

use axum::{
    extract::{Path, Query, State},
    response::Response,
};

use crate::{
    error::SrError,
    rest::{
        AppState, DeletedQ,
        response::{ok_json, ok_raw},
    },
};

/// GET /schemas/ids/{id}
#[tracing::instrument(level = "debug", name = "sr.get_by_id", skip_all, fields(schema_id = id, deleted = q.deleted), err)]
pub async fn get_by_id(
    State(st): State<AppState>,
    Path(id): Path<i32>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let (ty, schema, references, message_type) = st
        .store
        .store
        .read()
        .schema_by_id(id, q.deleted)
        .ok_or(SrError::SchemaNotFound)?;
    let mut body = serde_json::Map::new();
    if let Some(t) = ty.wire_name() {
        body.insert("schemaType".into(), t.into());
    }
    if let Some(t) = message_type {
        body.insert("messageType".into(), t.into());
    }
    body.insert("schema".into(), schema.into());
    if !references.is_empty() {
        body.insert(
            "references".into(),
            serde_json::to_value(&references).expect("refs serialise"),
        );
    }
    Ok(ok_json(&serde_json::Value::Object(body)))
}

/// GET /schemas/types
// axum requires async handlers even when the body is synchronous.
#[allow(clippy::unused_async)]
pub async fn types(State(_st): State<AppState>) -> Response {
    ok_json(&serde_json::json!(["AVRO", "JSON", "PROTOBUF"]))
}

/// GET /schemas/ids/{id}/versions -> [{"subject":..,"version":..}] | 404 when the
/// id has no qualifying versions (cp returns 40403 Schema Not Found).
#[allow(clippy::unused_async)]
#[tracing::instrument(level = "debug", name = "sr.get_by_id_versions", skip_all, fields(schema_id = id, deleted = q.deleted), err)]
pub async fn get_by_id_versions(
    State(st): State<AppState>,
    Path(id): Path<i32>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let pairs = st
        .store
        .store
        .read()
        .schema_id_subject_versions(id, q.deleted);
    if pairs.is_empty() {
        return Err(SrError::SchemaNotFound);
    }
    let arr: Vec<serde_json::Value> = pairs
        .into_iter()
        .map(|(subject, version)| serde_json::json!({ "subject": subject, "version": version }))
        .collect();
    Ok(ok_json(&serde_json::Value::Array(arr)))
}

/// GET /schemas/ids/{id}/schema — return the raw schema string (not JSON-wrapped).
#[tracing::instrument(level = "debug", name = "sr.get_by_id_schema", skip_all, fields(schema_id = id), err)]
pub async fn get_by_id_schema(
    State(st): State<AppState>,
    Path(id): Path<i32>,
) -> Result<Response, SrError> {
    let (_, schema, _, _) = st
        .store
        .store
        .read()
        .schema_by_id(id, false)
        .ok_or(SrError::SchemaNotFound)?;
    Ok(ok_raw(schema))
}

/// GET /schemas/ids/{id}/subjects[?deleted=true] — list subjects referencing this id.
#[tracing::instrument(level = "debug", name = "sr.get_by_id_subjects", skip_all, fields(schema_id = id, deleted = q.deleted), err)]
pub async fn get_by_id_subjects(
    State(st): State<AppState>,
    Path(id): Path<i32>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let pairs = st
        .store
        .store
        .read()
        .schema_id_subject_versions(id, q.deleted);
    if pairs.is_empty() {
        return Err(SrError::SchemaNotFound);
    }
    let mut subjects: Vec<String> = pairs
        .into_iter()
        .map(|(s, _)| s)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    subjects.sort_unstable();
    Ok(ok_json(&subjects))
}

/// GET /schemas -> [{subject,version,id,schemaType,schema}]
#[allow(clippy::unused_async)]
#[tracing::instrument(level = "debug", name = "sr.list_schemas", skip_all, fields(deleted = q.deleted))]
pub async fn list_schemas(State(st): State<AppState>, Query(q): Query<DeletedQ>) -> Response {
    let rows = st.store.store.read().all_schemas(q.deleted);
    let arr: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|row| {
            let mut m = serde_json::Map::new();
            m.insert("subject".into(), row.subject.into());
            m.insert("version".into(), row.version.into());
            m.insert("id".into(), row.id.into());
            if let Some(t) = row.ty.wire_name() {
                m.insert("schemaType".into(), t.into());
            }
            if let Some(t) = row.message_type {
                m.insert("messageType".into(), t.into());
            }
            m.insert("schema".into(), row.schema.into());
            serde_json::Value::Object(m)
        })
        .collect();
    ok_json(&serde_json::Value::Array(arr))
}
