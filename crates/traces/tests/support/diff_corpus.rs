use assert2::assert;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span as OtlpSpan, Status, TracesData,
    span::{Event, Link},
};
use serde_json::Value as JsonValue;

pub type OtlpTracesPayload = TracesData;

#[derive(Clone, Debug, PartialEq)]
pub struct SeedTrace {
    pub trace_id: [u8; 16],
    pub service: &'static str,
    pub root_name: &'static str,
    pub spans: Vec<SeedSpan>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SeedSpan {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: &'static str,
    pub start_ns: u64,
    pub duration_ns: u64,
    pub attrs: Vec<SeedAttr>,
    pub status_code: i32,
    pub events: Vec<SeedEvent>,
    pub links: Vec<SeedLink>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SeedAttr {
    Str(&'static str, &'static str),
    StrArray(&'static str, Vec<&'static str>),
    Bool(&'static str, bool),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SeedEvent {
    pub name: &'static str,
    pub time_ns: u64,
    pub attrs: Vec<SeedAttr>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SeedLink {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attrs: Vec<SeedAttr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchCase {
    pub name: &'static str,
    pub traceql: &'static str,
}

#[must_use]
pub fn seed_dataset() -> Vec<SeedTrace> {
    vec![checkout_trace(), payments_trace()]
}

fn checkout_trace() -> SeedTrace {
    SeedTrace {
        trace_id: [0x11; 16],
        service: "checkout",
        root_name: "GET /checkout",
        spans: vec![
            SeedSpan {
                span_id: [0x01; 8],
                parent_span_id: None,
                name: "GET /checkout",
                start_ns: 1_700_000_000_000_000_000,
                duration_ns: 500_000_000,
                attrs: vec![
                    SeedAttr::Str("http.method", "GET"),
                    SeedAttr::Str("span.kind", "server"),
                    SeedAttr::StrArray("cart.items", vec!["sku-1", "sku-2"]),
                ],
                status_code: 0,
                events: vec![SeedEvent {
                    name: "cache.miss",
                    time_ns: 1_700_000_000_100_000_000,
                    attrs: vec![SeedAttr::Str("cache.key", "cart-42")],
                }],
                links: Vec::new(),
            },
            SeedSpan {
                span_id: [0x02; 8],
                parent_span_id: Some([0x01; 8]),
                name: "POST /payments",
                start_ns: 1_700_000_000_150_000_000,
                duration_ns: 180_000_000,
                attrs: vec![
                    SeedAttr::Str("span.kind", "client"),
                    SeedAttr::Str("peer.service", "payments"),
                ],
                status_code: 0,
                events: Vec::new(),
                links: Vec::new(),
            },
            SeedSpan {
                span_id: [0x03; 8],
                parent_span_id: Some([0x01; 8]),
                name: "SELECT cart",
                start_ns: 1_700_000_000_220_000_000,
                duration_ns: 110_000_000,
                attrs: vec![
                    SeedAttr::Str("db.system", "postgresql"),
                    SeedAttr::Str("span.kind", "client"),
                ],
                status_code: 0,
                events: Vec::new(),
                links: Vec::new(),
            },
            SeedSpan {
                span_id: [0x04; 8],
                parent_span_id: Some([0x03; 8]),
                name: "decode row",
                start_ns: 1_700_000_000_260_000_000,
                duration_ns: 15_000_000,
                attrs: vec![SeedAttr::Bool("row.cache_hit", false)],
                status_code: 0,
                events: Vec::new(),
                links: vec![SeedLink {
                    trace_id: [0x22; 16],
                    span_id: [0x09; 8],
                    attrs: vec![SeedAttr::Str("link.type", "async")],
                }],
            },
        ],
    }
}

fn payments_trace() -> SeedTrace {
    SeedTrace {
        trace_id: [0x22; 16],
        service: "payments",
        root_name: "POST /payments",
        spans: vec![
            SeedSpan {
                span_id: [0x09; 8],
                parent_span_id: None,
                name: "POST /payments",
                start_ns: 1_700_000_001_000_000_000,
                duration_ns: 240_000_000,
                attrs: vec![
                    SeedAttr::Str("http.method", "POST"),
                    SeedAttr::Str("span.kind", "server"),
                ],
                status_code: 2,
                events: vec![SeedEvent {
                    name: "payment.failed",
                    time_ns: 1_700_000_001_120_000_000,
                    attrs: vec![SeedAttr::Str("error.type", "card_declined")],
                }],
                links: Vec::new(),
            },
            SeedSpan {
                span_id: [0x0a; 8],
                parent_span_id: Some([0x09; 8]),
                name: "authorize card",
                start_ns: 1_700_000_001_050_000_000,
                duration_ns: 120_000_000,
                attrs: vec![SeedAttr::Str("span.kind", "client")],
                status_code: 2,
                events: Vec::new(),
                links: Vec::new(),
            },
        ],
    }
}

#[must_use]
pub fn to_otlp(traces: &[SeedTrace]) -> OtlpTracesPayload {
    TracesData {
        resource_spans: traces
            .iter()
            .map(|trace| ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_kv("service.name", trace.service)],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "crabka-diff-corpus".into(),
                        version: "1.0.0".into(),
                        ..InstrumentationScope::default()
                    }),
                    spans: trace
                        .spans
                        .iter()
                        .map(|span| otlp_span(trace.trace_id, span))
                        .collect(),
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            })
            .collect(),
    }
}

#[must_use]
pub fn search_corpus() -> Vec<SearchCase> {
    vec![
        SearchCase {
            name: "resource-service",
            traceql: r#"{ resource.service.name = "checkout" }"#,
        },
        SearchCase {
            name: "error-status",
            traceql: "{ span:status = error }",
        },
        SearchCase {
            name: "descendant",
            traceql: r#"{ .http.method = "GET" } >> { .db.system = "postgresql" }"#,
        },
        SearchCase {
            name: "child",
            traceql: r#"{ .http.method = "GET" } > { .peer.service = "payments" }"#,
        },
        SearchCase {
            name: "sibling",
            traceql: r#"{ .peer.service = "payments" } ~ { .db.system = "postgresql" }"#,
        },
        SearchCase {
            name: "negated",
            traceql: r#"{ .db.system != "redis" }"#,
        },
        SearchCase {
            name: "pipeline-count",
            traceql: "{ .span.kind != nil } | count() > 1",
        },
        SearchCase {
            name: "metrics-rate",
            traceql: "{ .span.kind != nil } | rate()",
        },
        SearchCase {
            name: "metrics-by-service",
            traceql: "{ .span.kind != nil } | count_over_time() | by(resource.service.name)",
        },
    ]
}

#[must_use]
pub fn by_id_corpus() -> Vec<[u8; 16]> {
    seed_dataset()
        .into_iter()
        .map(|trace| trace.trace_id)
        .collect()
}

#[must_use]
pub fn normalize_search(resp: &JsonValue) -> JsonValue {
    let mut out = resp.clone();
    if let Some(obj) = out.as_object_mut() {
        obj.remove("metrics");
        obj.remove("inspectedTraces");
        obj.remove("inspectedBytes");
    }
    if let Some(traces) = out.get_mut("traces").and_then(JsonValue::as_array_mut) {
        traces.sort_by_key(trace_sort_key);
        for trace in traces {
            drop_trace_timing(trace);
            if let Some(span_sets) = trace.get_mut("spanSets").and_then(JsonValue::as_array_mut) {
                for span_set in span_sets {
                    if let Some(spans) = span_set.get_mut("spans").and_then(JsonValue::as_array_mut)
                    {
                        spans.sort_by_key(span_sort_key);
                    }
                }
            }
        }
    }
    out
}

#[must_use]
pub fn normalize_trace(resp: &JsonValue) -> JsonValue {
    let mut out = resp.clone();
    if let Some(resource_spans) = out
        .pointer_mut("/trace/resourceSpans")
        .and_then(JsonValue::as_array_mut)
    {
        resource_spans.sort_by_key(resource_sort_key);
        for resource in resource_spans {
            sort_attrs_at(resource, "/resource/attributes");
            if let Some(scope_spans) = resource
                .get_mut("scopeSpans")
                .and_then(JsonValue::as_array_mut)
            {
                scope_spans.sort_by_key(scope_sort_key);
                for scope in scope_spans {
                    if let Some(spans) = scope.get_mut("spans").and_then(JsonValue::as_array_mut) {
                        spans.sort_by_key(span_sort_key);
                        for span in spans {
                            sort_attrs_at(span, "/attributes");
                            sort_attrs_at(span, "/events/0/attributes");
                        }
                    }
                }
            }
        }
    }
    out
}

pub fn assert_trace_query_equal(name: &str, a: &JsonValue, b: &JsonValue) {
    let normalized_a = normalize_response(a);
    let normalized_b = normalize_response(b);
    assert!(
        normalized_a == normalized_b,
        "{name} differed after normalization\nleft: {normalized_a}\nright: {normalized_b}"
    );
}

fn normalize_response(value: &JsonValue) -> JsonValue {
    if value.get("traces").is_some() {
        normalize_search(value)
    } else {
        normalize_trace(value)
    }
}

fn otlp_span(trace_id: [u8; 16], span: &SeedSpan) -> OtlpSpan {
    OtlpSpan {
        trace_id: trace_id.to_vec(),
        span_id: span.span_id.to_vec(),
        parent_span_id: span.parent_span_id.map_or_else(Vec::new, |id| id.to_vec()),
        name: span.name.into(),
        start_time_unix_nano: span.start_ns,
        end_time_unix_nano: span.start_ns + span.duration_ns,
        attributes: span.attrs.iter().map(otlp_attr).collect(),
        events: span
            .events
            .iter()
            .map(|event| Event {
                time_unix_nano: event.time_ns,
                name: event.name.into(),
                attributes: event.attrs.iter().map(otlp_attr).collect(),
                ..Event::default()
            })
            .collect(),
        links: span
            .links
            .iter()
            .map(|link| Link {
                trace_id: link.trace_id.to_vec(),
                span_id: link.span_id.to_vec(),
                attributes: link.attrs.iter().map(otlp_attr).collect(),
                ..Link::default()
            })
            .collect(),
        status: Some(Status {
            code: span.status_code,
            ..Status::default()
        }),
        ..OtlpSpan::default()
    }
}

fn otlp_attr(attr: &SeedAttr) -> KeyValue {
    match attr {
        SeedAttr::Str(key, value) => string_kv(key, value),
        SeedAttr::StrArray(key, values) => KeyValue {
            key: (*key).into(),
            value: Some(AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: values
                        .iter()
                        .map(|value| AnyValue {
                            value: Some(Value::StringValue((*value).into())),
                        })
                        .collect(),
                })),
            }),
            ..KeyValue::default()
        },
        SeedAttr::Bool(key, value) => KeyValue {
            key: (*key).into(),
            value: Some(AnyValue {
                value: Some(Value::BoolValue(*value)),
            }),
            ..KeyValue::default()
        },
    }
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..KeyValue::default()
    }
}

fn trace_sort_key(value: &JsonValue) -> String {
    value
        .get("traceID")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string()
}

fn resource_sort_key(value: &JsonValue) -> String {
    value
        .pointer("/resource/attributes")
        .and_then(JsonValue::as_array)
        .and_then(|attrs| attrs.first())
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string()
}

fn scope_sort_key(value: &JsonValue) -> String {
    value
        .pointer("/scope/name")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string()
}

fn span_sort_key(value: &JsonValue) -> String {
    value
        .get("spanID")
        .or_else(|| value.get("spanId"))
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string()
}

fn drop_trace_timing(trace: &mut JsonValue) {
    if let Some(obj) = trace.as_object_mut() {
        for key in [
            "startTimeUnixNano",
            "startTimeUnixMs",
            "durationMs",
            "durationNanos",
        ] {
            obj.remove(key);
        }
    }
}

fn sort_attrs_at(value: &mut JsonValue, pointer: &str) {
    if let Some(attrs) = value.pointer_mut(pointer).and_then(JsonValue::as_array_mut) {
        attrs.sort_by_key(attr_sort_key);
    }
}

fn attr_sort_key(value: &JsonValue) -> String {
    value
        .get("key")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string()
}
