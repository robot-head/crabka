//! OTLP `TracesData` to internal spans.

use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue as OtlpKv, any_value::Value},
    trace::v1::{Status, TracesData, span::SpanKind as OtlpKind},
};

use super::WireError;
use crate::span::{AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode};

fn fixed16(bytes: &[u8], field: &str) -> Result<[u8; 16], WireError> {
    bytes
        .try_into()
        .map_err(|_| WireError::Invalid(format!("{field} must be 16 bytes, got {}", bytes.len())))
}

fn fixed8(bytes: &[u8], field: &str) -> Result<[u8; 8], WireError> {
    bytes
        .try_into()
        .map_err(|_| WireError::Invalid(format!("{field} must be 8 bytes, got {}", bytes.len())))
}

fn any_to_attr(value: &AnyValue) -> Option<AttrValue> {
    match value.value.as_ref()? {
        Value::StringValue(value) => Some(AttrValue::Str(value.clone())),
        Value::StringValueStrindex(value) => Some(AttrValue::Str(format!("strindex:{value}"))),
        Value::IntValue(value) => Some(AttrValue::Int(*value)),
        Value::DoubleValue(value) => Some(AttrValue::Double(*value)),
        Value::BoolValue(value) => Some(AttrValue::Bool(*value)),
        Value::BytesValue(value) => Some(AttrValue::Bytes(value.clone())),
        Value::ArrayValue(_) => None,
        Value::KvlistValue(value) => Some(AttrValue::Str(format!(
            "{{{}}}",
            value
                .values
                .iter()
                .filter_map(|kv| Some(format!("{}:{}", kv.key, any_to_text(kv.value.as_ref()?)?)))
                .collect::<Vec<_>>()
                .join(",")
        ))),
    }
}

fn any_to_text(value: &AnyValue) -> Option<String> {
    match any_to_attr(value)? {
        AttrValue::Str(value) => Some(value),
        AttrValue::Int(value) => Some(value.to_string()),
        AttrValue::Double(value) => Some(value.to_string()),
        AttrValue::Bool(value) => Some(value.to_string()),
        AttrValue::Bytes(value) => Some(hex::encode(value)),
    }
}

fn kv_to_attrs(attr: &OtlpKv) -> Vec<KeyValue> {
    let Some(value) = attr.value.as_ref() else {
        return Vec::new();
    };
    match value.value.as_ref() {
        Some(Value::ArrayValue(array)) => array
            .values
            .iter()
            .filter_map(|value| {
                Some(KeyValue {
                    key: attr.key.clone(),
                    value: any_to_attr(value)?,
                })
            })
            .collect(),
        _ => any_to_attr(value)
            .map(|value| {
                vec![KeyValue {
                    key: attr.key.clone(),
                    value,
                }]
            })
            .unwrap_or_default(),
    }
}

fn kvs(attrs: &[OtlpKv]) -> Vec<KeyValue> {
    attrs.iter().flat_map(kv_to_attrs).collect()
}

fn status_of(status: Option<&Status>) -> (StatusCode, String) {
    match status {
        Some(status) => (StatusCode::from_i32(status.code), status.message.clone()),
        None => (StatusCode::Unset, String::new()),
    }
}

fn kind_of(kind: i32) -> SpanKind {
    if kind == OtlpKind::Unspecified as i32 {
        SpanKind::Unspecified
    } else {
        SpanKind::from_i32(kind)
    }
}

/// Decode OTLP `TracesData` into internal spans.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_otlp(data: &TracesData) -> Result<Vec<Span>, WireError> {
    let mut out = Vec::new();
    for resource_spans in &data.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .as_ref()
            .map(|resource| kvs(&resource.attributes))
            .unwrap_or_default();

        for scope_spans in &resource_spans.scope_spans {
            let scope_name = scope_spans
                .scope
                .as_ref()
                .map(|scope| scope.name.clone())
                .unwrap_or_default();
            let scope_version = scope_spans
                .scope
                .as_ref()
                .map(|scope| scope.version.clone())
                .unwrap_or_default();
            let instrumentation_attrs = scope_spans.scope.as_ref().map_or_else(Vec::new, |scope| {
                kvs(&scope.attributes)
                    .into_iter()
                    .map(|mut attribute| {
                        attribute.key = format!(
                            "{}{}",
                            crabka_traceql::INSTRUMENTATION_ATTR_PREFIX,
                            attribute.key
                        );
                        attribute
                    })
                    .collect::<Vec<_>>()
            });

            for span in &scope_spans.spans {
                let parent_span_id = if span.parent_span_id.is_empty() {
                    None
                } else {
                    Some(fixed8(&span.parent_span_id, "parent_span_id")?)
                };
                let (status, status_message) = status_of(span.status.as_ref());
                let events = span
                    .events
                    .iter()
                    .map(|event| EventRecord {
                        time_unix_nano: i64::try_from(event.time_unix_nano).unwrap_or(i64::MAX),
                        name: event.name.clone(),
                        attrs: kvs(&event.attributes),
                    })
                    .collect();
                let links = span
                    .links
                    .iter()
                    .map(|link| {
                        Ok(LinkRecord {
                            trace_id: fixed16(&link.trace_id, "link.trace_id")?,
                            span_id: fixed8(&link.span_id, "link.span_id")?,
                            attrs: kvs(&link.attributes),
                        })
                    })
                    .collect::<Result<Vec<_>, WireError>>()?;

                let mut span_attrs = kvs(&span.attributes);
                span_attrs.extend(instrumentation_attrs.clone());
                out.push(Span {
                    trace_id: fixed16(&span.trace_id, "trace_id")?,
                    span_id: fixed8(&span.span_id, "span_id")?,
                    parent_span_id,
                    name: span.name.clone(),
                    kind: kind_of(span.kind),
                    start_ns: i64::try_from(span.start_time_unix_nano).unwrap_or(i64::MAX),
                    duration_ns: i64::try_from(
                        span.end_time_unix_nano
                            .saturating_sub(span.start_time_unix_nano),
                    )
                    .unwrap_or(i64::MAX),
                    status,
                    status_message,
                    resource_attrs: resource_attrs.clone(),
                    span_attrs,
                    events,
                    links,
                    instrumentation_scope: scope_name.clone(),
                    instrumentation_version: scope_version.clone(),
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {

    use opentelemetry_proto::tonic::{
        common::v1::{
            AnyValue, ArrayValue, InstrumentationScope, KeyValue as OtlpKv, any_value::Value,
        },
        resource::v1::Resource,
        trace::v1::{
            ResourceSpans, ScopeSpans, Span as OtlpSpan, Status, TracesData,
            span::SpanKind as OtlpKind,
        },
    };

    use super::*;

    fn kv(key: &str, value: &str) -> OtlpKv {
        OtlpKv {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.into())),
            }),
            ..OtlpKv::default()
        }
    }

    fn data() -> TracesData {
        let otlp_span = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            parent_span_id: Vec::new(),
            name: "GET /".into(),
            kind: OtlpKind::Server as i32,
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 1_500,
            attributes: vec![kv("http.method", "GET")],
            status: Some(Status {
                code: 1,
                message: String::new(),
            }),
            ..OtlpSpan::default()
        };

        TracesData {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "api")],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![otlp_span],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
    }

    #[test]
    fn decodes_one_span_with_resource_attrs() {
        let spans = decode_otlp(&data()).unwrap();
        assert2::assert!(
            spans
                == vec![Span {
                    trace_id: [1; 16],
                    span_id: [2; 8],
                    parent_span_id: None,
                    name: "GET /".into(),
                    kind: SpanKind::Server,
                    start_ns: 1_000,
                    duration_ns: 500,
                    status: StatusCode::Ok,
                    status_message: String::new(),
                    resource_attrs: vec![KeyValue {
                        key: "service.name".into(),
                        value: AttrValue::Str("api".into()),
                    }],
                    span_attrs: vec![KeyValue {
                        key: "http.method".into(),
                        value: AttrValue::Str("GET".into()),
                    }],
                    events: Vec::new(),
                    links: Vec::new(),
                    instrumentation_scope: String::new(),
                    instrumentation_version: String::new(),
                }]
        );
    }

    #[test]
    fn decodes_array_attributes_as_repeated_values() {
        let mut data = data();
        data.resource_spans[0].scope_spans[0].spans[0]
            .attributes
            .push(OtlpKv {
                key: "http.method".into(),
                value: Some(AnyValue {
                    value: Some(Value::ArrayValue(ArrayValue {
                        values: vec![
                            AnyValue {
                                value: Some(Value::StringValue("GET".into())),
                            },
                            AnyValue {
                                value: Some(Value::StringValue("POST".into())),
                            },
                        ],
                    })),
                }),
                ..OtlpKv::default()
            });

        let spans = decode_otlp(&data).unwrap();
        let methods = spans[0]
            .span_attrs
            .iter()
            .filter(|attr| attr.key == "http.method")
            .map(|attr| &attr.value)
            .collect::<Vec<_>>();

        assert2::assert!(methods.contains(&&AttrValue::Str("GET".into())));
        assert2::assert!(methods.contains(&&AttrValue::Str("POST".into())));
    }

    #[test]
    fn decodes_instrumentation_scope_version() {
        let mut data = data();
        data.resource_spans[0].scope_spans[0].scope = Some(InstrumentationScope {
            name: "tracer".into(),
            version: "1.2.3".into(),
            attributes: vec![OtlpKv {
                key: "library.language".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("rust".into())),
                }),
                ..OtlpKv::default()
            }],
            ..InstrumentationScope::default()
        });

        let spans = decode_otlp(&data).unwrap();

        assert2::assert!(
            (
                spans[0].instrumentation_scope.as_str(),
                spans[0].instrumentation_version.as_str(),
            ) == ("tracer", "1.2.3")
        );
        assert2::assert!(spans[0].span_attrs.iter().any(|attribute| {
            attribute.key == "__instrumentation.library.language"
                && attribute.value == AttrValue::Str("rust".into())
        }));
    }

    #[test]
    fn rejects_wrong_length_trace_id() {
        let mut data = data();
        data.resource_spans[0].scope_spans[0].spans[0].trace_id = vec![1; 8];
        assert2::assert!(decode_otlp(&data).is_err());
    }
}
