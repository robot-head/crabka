//! Confluent-compatible error model: numeric `error_code` + HTTP status,
//! serialised as `{"error_code":N,"message":"..."}` with the vendor
//! content-type. Serdes branch on `error_code`, so the numbers are exact.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

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
}

impl SrError {
    #[must_use]
    pub fn error_code(&self) -> i32 {
        match self {
            Self::SubjectNotFound(_) => 40401,
            Self::VersionNotFound => 40402,
            Self::SchemaNotFound => 40403,
            Self::InvalidSchema(_) => 42201,
            Self::InvalidVersion(_) => 42202,
            Self::InvalidCompatibilityLevel(_) => 42203,
            Self::Backend(_) => 50001,
        }
    }

    #[must_use]
    pub fn http_status(&self) -> StatusCode {
        match self {
            Self::SubjectNotFound(_) | Self::VersionNotFound | Self::SchemaNotFound => {
                StatusCode::NOT_FOUND
            }
            Self::InvalidSchema(_) | Self::InvalidVersion(_) | Self::InvalidCompatibilityLevel(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            Self::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for SrError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "error_code": self.error_code(), "message": self.to_string() });
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
    use super::*;
    use axum::response::IntoResponse;
    use axum::http::StatusCode;

    #[test]
    fn codes_map_to_status() {
        assert_eq!(SrError::SubjectNotFound("s".into()).http_status(), StatusCode::NOT_FOUND);
        assert_eq!(SrError::SubjectNotFound("s".into()).error_code(), 40401);
        assert_eq!(SrError::VersionNotFound.error_code(), 40402);
        assert_eq!(SrError::SchemaNotFound.error_code(), 40403);
        assert_eq!(SrError::InvalidSchema("bad".into()).error_code(), 42201);
        assert_eq!(SrError::InvalidSchema("bad".into()).http_status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(SrError::Backend("x".into()).error_code(), 50001);
    }

    #[tokio::test]
    async fn body_is_confluent_json() {
        let resp = SrError::SubjectNotFound("av-value".into()).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error_code"], 40401);
        assert!(v["message"].as_str().unwrap().contains("av-value"));
    }
}
