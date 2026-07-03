//! DELETE endpoints for versions and subjects (soft + permanent).

use axum::{
    extract::{Path, Query, State},
    response::Response,
};
use serde::Deserialize;

use crate::{
    error::SrError,
    ids::SchemaVersion,
    rest::{AppState, response::ok_json},
};

#[derive(Deserialize, Default)]
pub struct PermanentQ {
    #[serde(default)]
    permanent: bool,
}

fn parse_concrete_version(v: &str) -> Result<SchemaVersion, SrError> {
    match v.parse::<i32>() {
        Ok(n) if n >= 1 => Ok(SchemaVersion(n)),
        _ => Err(SrError::InvalidVersion(v.to_string())),
    }
}

/// `DELETE /subjects/{subject}/versions/{version}[?permanent=true] -> <version:int>`
#[tracing::instrument(level = "info", name = "sr.delete_version", skip_all, fields(subject = %subject, version = %version, permanent = q.permanent), err)]
pub async fn delete_version(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    Query(q): Query<PermanentQ>,
) -> Result<Response, SrError> {
    let v = if version == "latest" {
        st.store
            .store
            .read()
            .version(&subject, None, false)
            .map(|found| found.version)
            .ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?
    } else {
        parse_concrete_version(&version)?
    };
    let deleted = if q.permanent {
        st.store.permanent_delete_version(&subject, v).await?
    } else {
        st.store.soft_delete_version(&subject, v).await?
    };
    Ok(ok_json(&deleted))
}

/// `DELETE /subjects/{subject}[?permanent=true] -> [<versions>]`
#[tracing::instrument(level = "info", name = "sr.delete_subject", skip_all, fields(subject = %subject, permanent = q.permanent), err)]
pub async fn delete_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    Query(q): Query<PermanentQ>,
) -> Result<Response, SrError> {
    let versions = if q.permanent {
        st.store.permanent_delete_subject(&subject).await?
    } else {
        st.store.soft_delete_subject(&subject).await?
    };
    Ok(ok_json(&versions))
}
