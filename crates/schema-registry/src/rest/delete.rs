//! DELETE endpoints for versions and subjects (soft + permanent).

use axum::{
    extract::{Path, Query, State},
    response::Response,
};
use serde::Deserialize;

use crate::{
    error::SrError,
    rest::{AppState, parse_concrete_version, response::ok_json},
};

#[derive(Deserialize, Default)]
pub struct PermanentQ {
    #[serde(default)]
    permanent: bool,
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

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::ids::SchemaVersion;

    #[test]
    fn parse_concrete_version_cases() {
        for (name, input, expected) in [
            ("one", "1", SchemaVersion(1)),
            ("seven", "7", SchemaVersion(7)),
        ] {
            check!(
                parse_concrete_version(input).unwrap() == expected,
                "case {name}"
            );
        }
        for (name, input) in [("zero", "0"), ("negative", "-3"), ("non_numeric", "latest")] {
            check!(
                matches!(
                    parse_concrete_version(input),
                    Err(SrError::InvalidVersion(_))
                ),
                "case {name}"
            );
        }
    }
}
