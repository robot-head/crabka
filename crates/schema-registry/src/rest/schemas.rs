//! `/schemas/*` read endpoints.

use axum::extract::{Path, State};
use axum::response::Response;

use crate::error::SrError;
use crate::rest::{response::ok_json, AppState};

/// GET /schemas/ids/{id}
pub async fn get_by_id(
    State(st): State<AppState>,
    Path(id): Path<i32>,
) -> Result<Response, SrError> {
    let (ty, schema) =
        st.store.store.read().schema_by_id(id).ok_or(SrError::SchemaNotFound)?;
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
