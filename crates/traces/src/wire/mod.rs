//! Push-door wire surfaces for trace ingest.

pub mod jaeger;
pub mod jaeger_grpc;
pub mod otlp;
pub mod zipkin;

use crate::error::TracesError;

/// Which push door a request arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    Otlp,
    Zipkin,
    Jaeger,
}

/// Ingest-edge wire error.
pub type WireError = TracesError;

/// Pick the decoder from the request path.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn negotiate(path: &str, content_type: Option<&str>) -> Result<WireFormat, WireError> {
    match path {
        "/v1/traces" | "/api/push" => Ok(WireFormat::Otlp),
        "/api/v2/spans" => Ok(WireFormat::Zipkin),
        "/api/traces" => Ok(WireFormat::Jaeger),
        other => Err(WireError::UnsupportedContentType(format!(
            "{other} (content-type {})",
            content_type.unwrap_or("none")
        ))),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn negotiate_trace_push_paths() {
        for (path, content_type, want) in [
            (
                "/v1/traces",
                Some("application/x-protobuf"),
                WireFormat::Otlp,
            ),
            ("/api/push", None, WireFormat::Otlp),
            (
                "/api/v2/spans",
                Some("application/json"),
                WireFormat::Zipkin,
            ),
            ("/api/traces", None, WireFormat::Jaeger),
        ] {
            assert2::assert!(negotiate(path, content_type).unwrap() == want);
        }
    }

    #[test]
    fn negotiate_unknown_path_is_415() {
        let err = negotiate("/nope", Some("text/plain")).unwrap_err();
        assert2::assert!(err.status_code() == 415);
    }
}
