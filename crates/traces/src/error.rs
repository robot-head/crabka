//! Crate-wide error type and ingest-edge HTTP status mapping.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
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
    /// Map to the ingest-edge HTTP status that Tempo-shaped push endpoints
    /// use.
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

    use super::*;

    #[test]
    fn status_codes_map_to_ingest_edge_failures() {
        for (err, want) in [
            (TracesError::UnsupportedContentType("x".into()), 415),
            (TracesError::Decode("x".into()), 400),
            (TracesError::Invalid("x".into()), 400),
            (TracesError::Limit("x".into()), 400),
            (TracesError::RateLimit("x".into()), 429),
            (TracesError::TooLarge { limit: 1 }, 400),
            (TracesError::Wal("x".into()), 500),
            (TracesError::Produce("x".into()), 500),
            (TracesError::Block("x".into()), 500),
        ] {
            assert2::assert!(err.status_code() == want);
        }
    }
}
