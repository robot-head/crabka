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
    fn parse_concrete_version_positive_is_ok() {
        check!(matches!(parse_concrete_version("1"), Ok(SchemaVersion(1))));
        check!(matches!(parse_concrete_version("7"), Ok(SchemaVersion(7))));
    }

    #[test]
    fn parse_concrete_version_zero_is_rejected() {
        // 0 parses as i32 but fails the `n >= 1` guard: must be InvalidVersion,
        // not Ok(SchemaVersion(0)).
        check!(matches!(
            parse_concrete_version("0"),
            Err(SrError::InvalidVersion(_))
        ));
    }

    #[test]
    fn parse_concrete_version_negative_is_rejected() {
        check!(matches!(
            parse_concrete_version("-3"),
            Err(SrError::InvalidVersion(_))
        ));
    }

    #[test]
    fn parse_concrete_version_non_numeric_is_rejected() {
        check!(matches!(
            parse_concrete_version("latest"),
            Err(SrError::InvalidVersion(_))
        ));
    }
}
