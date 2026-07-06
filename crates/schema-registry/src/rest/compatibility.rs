//! Compatibility check endpoint using the subject's effective level against
//! the named version.

use axum::{
    extract::{Path, Query, State},
    response::Response,
};
use serde::Deserialize;

use crate::{
    compat,
    error::SrError,
    format::SchemaType,
    rest::{AppState, parse_optional_version as parse_version, response::ok_json},
};

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
#[tracing::instrument(
    level = "debug",
    name = "sr.compatibility_check",
    skip_all,
    fields(subject = %subject, version = %version, verbose = q.verbose, schema_type = tracing::field::Empty, is_compatible = tracing::field::Empty),
    err
)]
pub async fn check(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    Query(q): Query<VerboseQ>,
    body: String,
) -> Result<Response, SrError> {
    let req: Body =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    tracing::Span::current().record("schema_type", tracing::field::debug(ty));
    let want = parse_version(&version)?;
    let verdict = {
        let snap = st.store.store.read();
        // Resolve the candidate's references (42201 on a missing ref), then
        // validate it parses with them (42201 if unparseable, matches Confluent).
        let resolved = snap.resolve_closure(&req.references)?;
        crate::format::parse(ty, &req.schema, &resolved)?;
        compat::check_against_version(&snap, &subject, ty, &req.schema, &resolved, want)?
    };
    tracing::Span::current().record("is_compatible", verdict.is_compatible);
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

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::ids::SchemaVersion;

    #[test]
    fn parse_version_latest_is_none() {
        check!(matches!(parse_version("latest"), Ok(None)));
    }

    #[test]
    fn parse_version_positive_is_some() {
        check!(matches!(parse_version("1"), Ok(Some(SchemaVersion(1)))));
        check!(matches!(parse_version("42"), Ok(Some(SchemaVersion(42)))));
    }

    #[test]
    fn parse_version_zero_is_rejected() {
        // 0 parses as i32 but fails the `n >= 1` guard: must be InvalidVersion,
        // not Ok(Some(SchemaVersion(0))).
        check!(matches!(
            parse_version("0"),
            Err(SrError::InvalidVersion(_))
        ));
    }

    #[test]
    fn parse_version_negative_is_rejected() {
        check!(matches!(
            parse_version("-5"),
            Err(SrError::InvalidVersion(_))
        ));
    }
}
