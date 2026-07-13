//! `/subjects/*` endpoints.

use axum::{
    extract::{Path, Query, State},
    response::Response,
};
use serde::Deserialize;

use crate::{
    error::SrError,
    format::SchemaType,
    ids::{SchemaId, SchemaVersion},
    kafkastore::RegisterSchema,
    rest::{
        AppState, DeletedQ, parse_optional_version,
        response::{ok_json, ok_raw},
    },
};

#[derive(Deserialize)]
struct RegisterBody {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<crate::kafkastore::record::SchemaReference>,
    #[serde(rename = "messageType", default)]
    message_type: Option<String>,
    #[serde(default)]
    id: Option<i32>,
    #[serde(default)]
    version: Option<i32>,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct NormalizeQuery {
    #[serde(default)]
    normalize: bool,
}

/// Normalize a schema string to its canonical form for the given type.
/// - Avro: Parsing Canonical Form via `apache_avro::Schema::canonical_form()`
/// - JSON Schema: round-trip through `serde_json` (strips whitespace, sorts keys)
/// - Protobuf: no-op (Confluent SR does not define textual normalization for proto)
fn normalize_schema(ty: SchemaType, schema: &str) -> Result<String, SrError> {
    match ty {
        SchemaType::Avro => apache_avro::Schema::parse_str(schema)
            .map(|s| s.canonical_form())
            .map_err(|e| SrError::InvalidSchema(e.to_string())),
        SchemaType::Json => serde_json::from_str::<serde_json::Value>(schema)
            .map(|v| v.to_string())
            .map_err(|e| SrError::InvalidSchema(e.to_string())),
        SchemaType::Protobuf => Ok(schema.to_string()),
    }
}

/// POST /subjects/{subject}/versions[?normalize=true] -> `{"id":N}`
#[tracing::instrument(
    level = "info",
    name = "sr.register",
    skip_all,
    fields(subject = %subject, normalize = n.normalize, schema_type = tracing::field::Empty, id = tracing::field::Empty),
    err
)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub async fn register(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    Query(n): Query<NormalizeQuery>,
    body: String,
) -> Result<Response, SrError> {
    let req: RegisterBody =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    tracing::Span::current().record("schema_type", tracing::field::debug(ty));
    let schema = if n.normalize {
        normalize_schema(ty, &req.schema)?
    } else {
        req.schema.clone()
    };
    let reg = st
        .store
        .register(RegisterSchema {
            subject: &subject,
            ty,
            schema: &schema,
            references: &req.references,
            message_type: req.message_type.as_deref(),
            import_id: req.id.map(SchemaId),
            import_version: req.version.map(SchemaVersion),
        })
        .await?;
    tracing::Span::current().record("id", reg.id.0);
    Ok(ok_json(&serde_json::json!({ "id": reg.id.0 })))
}

/// `POST /subjects/{subject}?normalize=true&deleted=true` returns
/// `{subject,id,version,schema}` or 404.
#[tracing::instrument(
    level = "debug",
    name = "sr.lookup_by_schema",
    skip_all,
    fields(subject = %subject, deleted = q.deleted, normalize = n.normalize, schema_type = tracing::field::Empty, id = tracing::field::Empty, version = tracing::field::Empty),
    err
)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub async fn lookup(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    Query(q): Query<DeletedQ>,
    Query(n): Query<NormalizeQuery>,
    body: String,
) -> Result<Response, SrError> {
    let req: RegisterBody =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    tracing::Span::current().record("schema_type", tracing::field::debug(ty));
    let schema_to_find = if n.normalize {
        normalize_schema(ty, &req.schema)?
    } else {
        req.schema.clone()
    };
    let s = st.store.store.read();
    if s.versions(&subject, q.deleted).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    let Some(found) = s.find_under_subject(
        &subject,
        ty,
        &schema_to_find,
        &req.references,
        req.message_type.as_deref(),
        q.deleted,
    ) else {
        return Err(SrError::SchemaNotFound);
    };
    let (sty, schema, _references, message_type) = s
        .schema_by_id(found.id, q.deleted)
        .ok_or(SrError::SchemaNotFound)?;
    tracing::Span::current().record("id", found.id.0);
    tracing::Span::current().record("version", found.version.0);
    let mut m = serde_json::Map::new();
    m.insert("subject".into(), subject.into());
    m.insert("id".into(), found.id.0.into());
    m.insert("version".into(), found.version.0.into());
    if let Some(t) = sty.wire_name() {
        m.insert("schemaType".into(), t.into());
    }
    if let Some(t) = message_type {
        m.insert("messageType".into(), t.into());
    }
    m.insert("schema".into(), schema.into());
    Ok(ok_json(&serde_json::Value::Object(m)))
}

/// GET /subjects
// axum requires async handlers even when the body is synchronous.
#[tracing::instrument(level = "debug", name = "sr.list_subjects", skip_all, fields(deleted = q.deleted))]
pub fn list(State(st): State<AppState>, Query(q): Query<DeletedQ>) -> Response {
    ok_json(&st.store.store.read().subjects(q.deleted))
}

/// GET /subjects/{subject}/versions
#[tracing::instrument(level = "debug", name = "sr.list_versions", skip_all, fields(subject = %subject, deleted = q.deleted), err)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub async fn versions(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let vs = st
        .store
        .store
        .read()
        .versions(&subject, q.deleted)
        .ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?;
    Ok(ok_json(&vs))
}

/// GET /subjects/{subject}/versions/{version}
#[tracing::instrument(level = "debug", name = "sr.get_version", skip_all, fields(subject = %subject, version = %version, deleted = q.deleted), err)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
/// # Panics
/// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
pub async fn get_version(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let want = parse_optional_version(&version)?;
    let s = st.store.store.read();
    if s.versions(&subject, q.deleted).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    let found = s
        .version(&subject, want, q.deleted)
        .ok_or(SrError::VersionNotFound)?;
    let mut m = serde_json::Map::new();
    m.insert("subject".into(), subject.into());
    m.insert("version".into(), found.version.0.into());
    m.insert("id".into(), found.id.0.into());
    if let Some(t) = found.ty.wire_name() {
        m.insert("schemaType".into(), t.into());
    }
    if let Some(t) = found.message_type {
        m.insert("messageType".into(), t.into());
    }
    m.insert("schema".into(), found.schema.into());
    if !found.references.is_empty() {
        m.insert(
            "references".into(),
            serde_json::to_value(&found.references).expect("refs serialise"),
        );
    }
    Ok(ok_json(&serde_json::Value::Object(m)))
}

/// GET /subjects/{subject}/versions/{version}/schema -> raw schema text
#[tracing::instrument(level = "debug", name = "sr.get_version_schema", skip_all, fields(subject = %subject, version = %version, deleted = q.deleted), err)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub async fn get_version_schema(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let want = parse_optional_version(&version)?;
    let s = st.store.store.read();
    if s.versions(&subject, q.deleted).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    let found = s
        .version(&subject, want, q.deleted)
        .ok_or(SrError::VersionNotFound)?;
    Ok(ok_raw(found.schema))
}

/// GET /subjects/{subject}/versions/{version}/referencedby -> ids of the live
/// schemas that reference this `(subject, version)` (ascending; empty if none).
#[tracing::instrument(level = "debug", name = "sr.referenced_by", skip_all, fields(subject = %subject, version = %version), err)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub async fn referencedby(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Response, SrError> {
    let want = parse_optional_version(&version)?;
    let s = st.store.store.read();
    if s.versions(&subject, true).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    // Resolve `latest`/numeric to a concrete version (404 if absent), then list
    // its live referrers.
    let found = s
        .version(&subject, want, true)
        .ok_or(SrError::VersionNotFound)?;
    let ids = s.referenced_by(&subject, found.version, false);
    Ok(ok_json(&ids))
}
