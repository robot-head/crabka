//! Compatibility check. Slice 1: NONE engine — always compatible for a
//! well-formed schema. Real checks arrive in slice 2.

use axum::extract::{Path, State};
use axum::response::Response;
use serde::Deserialize;

use crate::error::SrError;
use crate::format::{self, SchemaType};
use crate::rest::{AppState, response::ok_json};

#[derive(Deserialize)]
struct Body {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
}

/// POST /compatibility/subjects/{subject}/versions/{version}
pub async fn check(
    State(_st): State<AppState>,
    Path((_subject, _version)): Path<(String, String)>,
    body: String,
) -> Result<Response, SrError> {
    let req: Body =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    format::parse(ty, &req.schema)?; // returns 42201 if unparseable
    Ok(ok_json(&serde_json::json!({ "is_compatible": true })))
}
