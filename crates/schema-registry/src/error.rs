//! Confluent-compatible error model: numeric `error_code` + HTTP status,
//! serialised as `{"error_code":N,"message":"..."}` with the vendor
//! content-type. Serdes branch on `error_code`, so the numbers are exact.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

pub const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

#[derive(Debug, thiserror::Error)]
pub enum SrError {
    #[error("Subject '{0}' not found.")]
    SubjectNotFound(String),
    #[error("Version not found.")]
    VersionNotFound,
    #[error("Schema not found")]
    SchemaNotFound,
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    #[error("Invalid version: {0}")]
    InvalidVersion(String),
    #[error("Invalid compatibility level: {0}")]
    InvalidCompatibilityLevel(String),
    #[error("Error in the backend data store: {0}")]
    Backend(String),
    /// Schema incompatible with prior version(s) under the subject. The strings
    /// are best-effort reasons (Avro's wording, not Confluent's).
    #[error("Schema being registered is incompatible with an earlier schema; details: {0:?}")]
    Incompatible(Vec<String>),
    /// A write was attempted on a subject/registry in `READONLY` mode.
    #[error("Subject '{0}' is in read-only mode.")]
    OperationNotPermitted(String),
    /// Permanent subject delete attempted before a soft delete.
    #[error("Subject '{0}' was not deleted first before being permanently deleted.")]
    SubjectNotSoftDeleted(String),
    /// Permanent version delete attempted before a soft delete.
    #[error(
        "Version {1} of subject '{0}' was not soft-deleted first before being permanently deleted."
    )]
    VersionNotSoftDeleted(String, i32),
    /// Unknown mode string on PUT /mode.
    #[error("Invalid mode: {0}")]
    InvalidMode(String),
    /// A soft-deleted subject was soft-deleted again (cp: use `permanent=true`).
    #[error("Subject '{0}' was soft deleted. Set permanent=true to delete permanently.")]
    SubjectSoftDeleted(String),
    /// GET/DELETE `/mode/{subject}` when the subject has no mode override.
    #[error("Subject '{0}' does not have subject-level mode configured")]
    SubjectModeNotConfigured(String),
    /// A registration referenced a (subject, version) that does not exist.
    #[error("Reference {0} not found.")]
    ReferenceNotFound(String),
    /// A delete was blocked because a live schema still references the target.
    #[error("One or more references exist to the schema {0}.")]
    ReferencedByOthers(String),
}

impl SrError {
    #[must_use]
    pub fn error_code(&self) -> i32 {
        match self {
            Self::SubjectNotFound(_) => 40401,
            Self::VersionNotFound => 40402,
            Self::SchemaNotFound => 40403,
            // cp uses 42201 for both an unparseable schema and a missing reference.
            Self::InvalidSchema(_) | Self::ReferenceNotFound(_) => 42201,
            Self::InvalidVersion(_) => 42202,
            Self::InvalidCompatibilityLevel(_) => 42203,
            Self::Backend(_) => 50001,
            Self::Incompatible(_) => 409,
            Self::OperationNotPermitted(_) => 42205,
            Self::SubjectNotSoftDeleted(_) => 40405,
            Self::VersionNotSoftDeleted(..) => 40407,
            Self::InvalidMode(_) => 42204,
            Self::SubjectSoftDeleted(_) => 40404,
            Self::SubjectModeNotConfigured(_) => 40409,
            Self::ReferencedByOthers(_) => 42206,
        }
    }

    #[must_use]
    pub fn http_status(&self) -> StatusCode {
        match self {
            Self::SubjectNotFound(_)
            | Self::VersionNotFound
            | Self::SchemaNotFound
            | Self::SubjectNotSoftDeleted(_)
            | Self::VersionNotSoftDeleted(..)
            | Self::SubjectSoftDeleted(_)
            | Self::SubjectModeNotConfigured(_) => StatusCode::NOT_FOUND,
            Self::InvalidSchema(_)
            | Self::InvalidVersion(_)
            | Self::InvalidCompatibilityLevel(_)
            | Self::OperationNotPermitted(_)
            | Self::InvalidMode(_)
            | Self::ReferenceNotFound(_)
            | Self::ReferencedByOthers(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Incompatible(_) => StatusCode::CONFLICT,
        }
    }
}

impl IntoResponse for SrError {
    fn into_response(self) -> Response {
        let body =
            serde_json::json!({ "error_code": self.error_code(), "message": self.to_string() });
        (
            self.http_status(),
            [("content-type", CONTENT_TYPE)],
            body.to_string(),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::{http::StatusCode, response::IntoResponse};

    use super::*;

    #[test]
    fn incompatible_is_409_conflict() {
        let e = SrError::Incompatible(vec!["reader missing default".into()]);
        assert_eq!(
            (
                e.error_code(),
                e.http_status(),
                e.to_string().contains("incompatible")
            ),
            (409, StatusCode::CONFLICT, true)
        );
    }

    #[test]
    fn codes_map_to_status() {
        for (name, error, code, status) in [
            (
                "subject_not_found",
                SrError::SubjectNotFound("s".into()),
                40401,
                StatusCode::NOT_FOUND,
            ),
            (
                "version_not_found",
                SrError::VersionNotFound,
                40402,
                StatusCode::NOT_FOUND,
            ),
            (
                "schema_not_found",
                SrError::SchemaNotFound,
                40403,
                StatusCode::NOT_FOUND,
            ),
            (
                "invalid_schema",
                SrError::InvalidSchema("bad".into()),
                42201,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                "backend",
                SrError::Backend("x".into()),
                50001,
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                "operation_not_permitted",
                SrError::OperationNotPermitted("s".into()),
                42205,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                "subject_not_soft_deleted",
                SrError::SubjectNotSoftDeleted("s".into()),
                40405,
                StatusCode::NOT_FOUND,
            ),
            (
                "version_not_soft_deleted",
                SrError::VersionNotSoftDeleted("s".into(), 2),
                40407,
                StatusCode::NOT_FOUND,
            ),
            (
                "invalid_mode",
                SrError::InvalidMode("X".into()),
                42204,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                "subject_soft_deleted",
                SrError::SubjectSoftDeleted("s".into()),
                40404,
                StatusCode::NOT_FOUND,
            ),
            (
                "subject_mode_not_configured",
                SrError::SubjectModeNotConfigured("s".into()),
                40409,
                StatusCode::NOT_FOUND,
            ),
            (
                "reference_not_found",
                SrError::ReferenceNotFound("r".into()),
                42201,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                "referenced_by_others",
                SrError::ReferencedByOthers("s:1".into()),
                42206,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
        ] {
            assert_eq!(
                (error.error_code(), error.http_status()),
                (code, status),
                "case {name}"
            );
        }
    }

    #[tokio::test]
    async fn body_is_confluent_json() {
        let resp = SrError::SubjectNotFound("av-value".into()).into_response();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            (
                status,
                v["error_code"].as_i64(),
                v["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("av-value")),
            ),
            (StatusCode::NOT_FOUND, Some(40401), true)
        );
    }
}
