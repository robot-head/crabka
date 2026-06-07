//! Compatibility check endpoint using the subject's effective level against
//! the named version.

use axum::extract::{Path, Query, State};
use axum::response::Response;
use serde::Deserialize;

use crate::compat;
use crate::error::SrError;
use crate::format::SchemaType;
use crate::rest::{AppState, response::ok_json};

#[derive(Deserialize)]
struct Body {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<crate::kafkastore::record::SchemaReference>,
}

#[derive(Deserialize, Default)]
pub struct VerboseQ {
    #[serde(default)]
    verbose: bool,
}

/// POST /compatibility/subjects/{subject}/versions/{version}
pub async fn check(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    Query(q): Query<VerboseQ>,
    body: String,
) -> Result<Response, SrError> {
    let req: Body =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    let want = parse_version(&version)?;
    let verdict = {
        let snap = st.store.store.read();
        // Resolve the candidate's references (42201 on a missing ref), then
        // validate it parses with them (42201 if unparseable, matches Confluent).
        let resolved = snap.resolve_closure(&req.references)?;
        crate::format::parse(ty, &req.schema, &resolved)?;
        compat::check_against_version(&snap, &subject, ty, &req.schema, &resolved, want)?
    };
    if q.verbose {
        Ok(ok_json(&serde_json::json!({
            "is_compatible": verdict.is_compatible,
            "messages": verdict.messages,
        })))
    } else {
        Ok(ok_json(
            &serde_json::json!({ "is_compatible": verdict.is_compatible }),
        ))
    }
}

/// `latest` -> None; a positive integer -> Some(n); else 42202.
fn parse_version(v: &str) -> Result<Option<i32>, SrError> {
    if v == "latest" {
        return Ok(None);
    }
    match v.parse::<i32>() {
        Ok(n) if n >= 1 => Ok(Some(n)),
        _ => Err(SrError::InvalidVersion(v.to_string())),
    }
}
