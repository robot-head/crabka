//! `/subjects/*` endpoints.

use axum::extract::{Path, Query, State};
use axum::response::Response;
use serde::Deserialize;

use crate::error::SrError;
use crate::format::SchemaType;
use crate::rest::{
    AppState, DeletedQ,
    response::{ok_json, ok_raw},
};

#[derive(Deserialize)]
struct RegisterBody {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<crate::kafkastore::record::SchemaReference>,
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
pub async fn register(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    Query(n): Query<NormalizeQuery>,
    body: String,
) -> Result<Response, SrError> {
    let req: RegisterBody =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    let schema = if n.normalize {
        normalize_schema(ty, &req.schema)?
    } else {
        req.schema.clone()
    };
    let reg = st
        .store
        .register(&subject, ty, &schema, &req.references, req.id, req.version)
        .await?;
    Ok(ok_json(&serde_json::json!({ "id": reg.id })))
}

/// POST /subjects/{subject}[?normalize=true][&deleted=true] -> `{subject,id,version,schema}` | 404
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
    let schema_to_find = if n.normalize {
        normalize_schema(ty, &req.schema)?
    } else {
        req.schema.clone()
    };
    let s = st.store.store.read();
    if s.versions(&subject, q.deleted).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    let Some(found) =
        s.find_under_subject(&subject, ty, &schema_to_find, &req.references, q.deleted)
    else {
        return Err(SrError::SchemaNotFound);
    };
    let (sty, schema, _references) = s
        .schema_by_id(found.id, q.deleted)
        .ok_or(SrError::SchemaNotFound)?;
    let mut m = serde_json::Map::new();
    m.insert("subject".into(), subject.into());
    m.insert("id".into(), found.id.into());
    m.insert("version".into(), found.version.into());
    if let Some(t) = sty.wire_name() {
        m.insert("schemaType".into(), t.into());
    }
    m.insert("schema".into(), schema.into());
    Ok(ok_json(&serde_json::Value::Object(m)))
}

/// GET /subjects
// axum requires async handlers even when the body is synchronous.
#[allow(clippy::unused_async)]
pub async fn list(State(st): State<AppState>, Query(q): Query<DeletedQ>) -> Response {
    ok_json(&st.store.store.read().subjects(q.deleted))
}

/// GET /subjects/{subject}/versions
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

fn parse_version(v: &str) -> Result<Option<i32>, SrError> {
    if v == "latest" {
        return Ok(None);
    }
    match v.parse::<i32>() {
        Ok(n) if n >= 1 => Ok(Some(n)),
        _ => Err(SrError::InvalidVersion(v.to_string())),
    }
}

/// GET /subjects/{subject}/versions/{version}
pub async fn get_version(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let want = parse_version(&version)?;
    let s = st.store.store.read();
    if s.versions(&subject, q.deleted).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    let (id, ver, ty, schema, references) = s
        .version(&subject, want, q.deleted)
        .ok_or(SrError::VersionNotFound)?;
    let mut m = serde_json::Map::new();
    m.insert("subject".into(), subject.into());
    m.insert("version".into(), ver.into());
    m.insert("id".into(), id.into());
    if let Some(t) = ty.wire_name() {
        m.insert("schemaType".into(), t.into());
    }
    m.insert("schema".into(), schema.into());
    if !references.is_empty() {
        m.insert(
            "references".into(),
            serde_json::to_value(&references).expect("refs serialise"),
        );
    }
    Ok(ok_json(&serde_json::Value::Object(m)))
}

/// GET /subjects/{subject}/versions/{version}/schema -> raw schema text
pub async fn get_version_schema(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let want = parse_version(&version)?;
    let s = st.store.store.read();
    if s.versions(&subject, q.deleted).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    let (_, _, _, schema, _) = s
        .version(&subject, want, q.deleted)
        .ok_or(SrError::VersionNotFound)?;
    Ok(ok_raw(schema))
}

/// GET /subjects/{subject}/versions/{version}/referencedby -> ids of the live
/// schemas that reference this `(subject, version)` (ascending; empty if none).
pub async fn referencedby(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Response, SrError> {
    let want = parse_version(&version)?;
    let s = st.store.store.read();
    if s.versions(&subject, true).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    // Resolve `latest`/numeric to a concrete version (404 if absent), then list
    // its live referrers.
    let (_, concrete, _, _, _) = s
        .version(&subject, want, true)
        .ok_or(SrError::VersionNotFound)?;
    let ids = s.referenced_by(&subject, concrete, false);
    Ok(ok_json(&ids))
}
