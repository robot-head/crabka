//! Crate-wide error type and ingest-edge HTTP status mapping.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::limits::LimitError;

/// Errors across the traces ingest and query pipeline.
#[derive(Debug, thiserror::Error)]
pub enum TracesError {
    #[error("unsupported content-type: {0}")]
    UnsupportedContentType(String),
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("limit exceeded: {0}")]
    Limit(String),
    #[error("rate limit exceeded: {0}")]
    RateLimit(String),
    #[error("payload exceeds limit {limit} bytes")]
    TooLarge { limit: usize },
    #[error("wal codec: {0}")]
    Wal(String),
    #[error("produce failed: {0}")]
    Produce(String),
    #[error("block build failed: {0}")]
    Block(String),
}

impl TracesError {
    /// Map to the ingest-edge HTTP status used by Tempo-shaped push endpoints.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::UnsupportedContentType(_) => 415,
            Self::Decode(_) | Self::Invalid(_) | Self::Limit(_) | Self::TooLarge { .. } => 400,
            Self::RateLimit(_) => 429,
            Self::Wal(_) | Self::Produce(_) | Self::Block(_) => 500,
        }
    }
}

#[must_use]
pub fn tempo_error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({
            "status": "error",
            "error": message.into(),
        })),
    )
        .into_response()
}

#[must_use]
pub fn tempo_limit_error_response(err: &LimitError) -> Response {
    tempo_error_response(
        StatusCode::from_u16(err.http_status()).unwrap_or(StatusCode::BAD_REQUEST),
        err.message(),
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn status_codes_map_to_ingest_edge_failures() {
        assert!(TracesError::UnsupportedContentType("x".into()).status_code() == 415);
        assert!(TracesError::Decode("x".into()).status_code() == 400);
        assert!(TracesError::Invalid("x".into()).status_code() == 400);
        assert!(TracesError::Limit("x".into()).status_code() == 400);
        assert!(TracesError::RateLimit("x".into()).status_code() == 429);
        assert!(TracesError::TooLarge { limit: 1 }.status_code() == 400);
        assert!(TracesError::Wal("x".into()).status_code() == 500);
        assert!(TracesError::Produce("x".into()).status_code() == 500);
        assert!(TracesError::Block("x".into()).status_code() == 500);
    }
}
