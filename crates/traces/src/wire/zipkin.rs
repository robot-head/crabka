//! Zipkin v2 JSON to internal spans.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::WireError;
use crate::span::{AttrValue, KeyValue, Span, SpanKind, StatusCode};

#[derive(Deserialize)]
struct ZipkinEndpoint {
    #[serde(rename = "serviceName")]
    service_name: Option<String>,
}

#[derive(Deserialize)]
struct ZipkinAnnotation {
    timestamp: i64,
    value: String,
}

#[derive(Deserialize)]
struct ZipkinSpan {
    #[serde(rename = "traceId")]
    trace_id: String,
    id: String,
    #[serde(rename = "parentId")]
    parent_id: Option<String>,
    #[serde(default)]
    name: String,
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    duration: i64,
    kind: Option<String>,
    #[serde(rename = "localEndpoint")]
    local_endpoint: Option<ZipkinEndpoint>,
    #[serde(rename = "remoteEndpoint")]
    remote_endpoint: Option<ZipkinEndpoint>,
    #[serde(default)]
    tags: BTreeMap<String, String>,
    #[serde(default)]
    annotations: Vec<ZipkinAnnotation>,
}

fn hex_fixed<const N: usize>(hex: &str) -> Result<[u8; N], WireError> {
    if hex.is_empty() || !hex.len().is_multiple_of(2) || hex.len() > N * 2 {
        return Err(WireError::Invalid(format!("bad hex id {hex:?}")));
    }

    let bytes = hex::decode(hex).map_err(|err| WireError::Invalid(err.to_string()))?;
    let mut out = [0; N];
    out[N - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}

fn zipkin_kind(kind: Option<&str>) -> SpanKind {
    match kind {
        Some("SERVER") => SpanKind::Server,
        Some("CLIENT") => SpanKind::Client,
        Some("PRODUCER") => SpanKind::Producer,
        Some("CONSUMER") => SpanKind::Consumer,
        _ => SpanKind::Internal,
    }
}

fn zipkin_status(tags: &BTreeMap<String, String>) -> (StatusCode, String) {
    match tags.get("error") {
        Some(value) => {
            let message = if value == "true" || value == "false" {
                String::new()
            } else {
                value.clone()
            };
            (StatusCode::Error, message)
        }
        None => (StatusCode::Unset, String::new()),
    }
}

/// Decode a Zipkin v2 JSON span array.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_zipkin(body: &[u8]) -> Result<Vec<Span>, WireError> {
    let raw: Vec<ZipkinSpan> =
        serde_json::from_slice(body).map_err(|err| WireError::Decode(err.to_string()))?;
    let mut out = Vec::with_capacity(raw.len());

    for span in raw {
        let resource_attrs = span
            .local_endpoint
            .and_then(|endpoint| endpoint.service_name)
            .map(|service| {
                vec![KeyValue {
                    key: "service.name".into(),
                    value: AttrValue::Str(service),
                }]
            })
            .unwrap_or_default();
        let (status, status_message) = zipkin_status(&span.tags);
        let mut span_attrs = span
            .tags
            .into_iter()
            .map(|(key, value)| KeyValue {
                key,
                value: AttrValue::Str(value),
            })
            .collect::<Vec<_>>();
        if let Some(service) = span
            .remote_endpoint
            .and_then(|endpoint| endpoint.service_name)
        {
            span_attrs.push(KeyValue {
                key: "peer.service".into(),
                value: AttrValue::Str(service),
            });
        }
        let events = span
            .annotations
            .into_iter()
            .map(|annotation| crate::span::EventRecord {
                time_unix_nano: annotation.timestamp.saturating_mul(1_000),
                name: annotation.value,
                attrs: Vec::new(),
            })
            .collect();

        out.push(Span {
            trace_id: hex_fixed::<16>(&span.trace_id)?,
            span_id: hex_fixed::<8>(&span.id)?,
            parent_span_id: span.parent_id.as_deref().map(hex_fixed::<8>).transpose()?,
            name: span.name,
            kind: zipkin_kind(span.kind.as_deref()),
            start_ns: span.timestamp.saturating_mul(1_000),
            duration_ns: span.duration.saturating_mul(1_000),
            status,
            status_message,
            resource_attrs,
            span_attrs,
            events,
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::span::EventRecord;

    const BODY: &str = r#"[
      {
        "traceId": "0000000000000001",
        "id": "0000000000000002",
        "name": "get /",
        "timestamp": 1000,
        "duration": 500,
        "kind": "SERVER",
        "localEndpoint": { "serviceName": "api" },
        "tags": { "http.method": "GET" },
        "annotations": [{ "timestamp": 1100, "value": "cache miss" }]
      }
    ]"#;

    #[test]
    fn decodes_zipkin_span() {
        let spans = decode_zipkin(BODY.as_bytes()).unwrap();
        assert2::assert!(
            spans
                == vec![Span {
                    trace_id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                    span_id: [0, 0, 0, 0, 0, 0, 0, 2],
                    parent_span_id: None,
                    name: "get /".into(),
                    kind: SpanKind::Server,
                    start_ns: 1_000_000,
                    duration_ns: 500_000,
                    status: StatusCode::Unset,
                    status_message: String::new(),
                    resource_attrs: vec![KeyValue {
                        key: "service.name".into(),
                        value: AttrValue::Str("api".into()),
                    }],
                    span_attrs: vec![KeyValue {
                        key: "http.method".into(),
                        value: AttrValue::Str("GET".into()),
                    }],
                    events: vec![EventRecord {
                        time_unix_nano: 1_100_000,
                        name: "cache miss".into(),
                        attrs: Vec::new(),
                    }],
                    links: Vec::new(),
                    instrumentation_scope: String::new(),
                    instrumentation_version: String::new(),
                }]
        );
    }

    #[test]
    fn error_tag_sets_status_error() {
        let body = r#"[
          {
            "traceId": "0000000000000001",
            "id": "0000000000000002",
            "tags": { "error": "true" }
          }
        ]"#;

        let spans = decode_zipkin(body.as_bytes()).unwrap();

        assert2::assert!(
            (
                spans[0].status,
                spans[0]
                    .span_attrs
                    .iter()
                    .map(|attr| (attr.key.as_str(), &attr.value))
                    .collect::<Vec<_>>(),
            ) == (
                StatusCode::Error,
                vec![("error", &AttrValue::Str("true".into()))]
            )
        );
    }

    #[test]
    fn error_tag_description_sets_status_message() {
        let body = r#"[
          {
            "traceId": "0000000000000001",
            "id": "0000000000000002",
            "tags": { "error": "deadline exceeded" }
          }
        ]"#;

        let spans = decode_zipkin(body.as_bytes()).unwrap();

        assert2::assert!(
            (spans[0].status, spans[0].status_message.as_str())
                == (StatusCode::Error, "deadline exceeded")
        );
    }

    #[test]
    fn remote_endpoint_service_name_becomes_peer_service_attribute() {
        let body = r#"[
          {
            "traceId": "0000000000000001",
            "id": "0000000000000002",
            "remoteEndpoint": { "serviceName": "postgres" }
          }
        ]"#;

        let spans = decode_zipkin(body.as_bytes()).unwrap();

        assert2::assert!(
            spans[0]
                .span_attrs
                .iter()
                .any(|attr| attr.key == "peer.service"
                    && attr.value == AttrValue::Str("postgres".into()))
        );
    }

    #[test]
    fn rejects_odd_length_hex_id() {
        let bad = r#"[{ "traceId": "xyz", "id": "0000000000000002", "name": "x" }]"#;
        assert2::assert!(decode_zipkin(bad.as_bytes()).is_err());
    }
}
