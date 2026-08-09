//! Tempo-shaped `TraceQL` result model.

use crabka_units::{ByteSize, Time};

/// A typed attribute value.
#[derive(Clone, Debug, PartialEq)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// One span event attached to a returned span.
#[derive(Clone, Debug, PartialEq)]
pub struct EventRef {
    /// How long after the span started the event fired.
    pub time_since_start: Time,
    pub name: String,
    pub attributes: Vec<(String, AttrValue)>,
}

/// One linked span reference attached to a returned span.
#[derive(Clone, Debug, PartialEq)]
pub struct LinkRef {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attributes: Vec<(String, AttrValue)>,
}

/// One matched span in a result span set.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanRef {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: i32,
    pub nested_set_left: i32,
    pub nested_set_right: i32,
    pub nested_set_parent: i32,
    pub start_time_unix_nano: u64,
    /// How long the span ran.
    pub duration: Time,
    pub status_code: i32,
    pub status_message: String,
    pub instrumentation_name: String,
    pub instrumentation_version: String,
    pub resource_attributes: Vec<(String, AttrValue)>,
    pub attributes: Vec<(String, AttrValue)>,
    pub events: Vec<EventRef>,
    pub links: Vec<LinkRef>,
}

/// A matched span set.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanSet {
    pub spans: Vec<SpanRef>,
    pub matched: u32,
}

/// One trace in a search response.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceResult {
    pub trace_id: [u8; 16],
    pub root_service_name: String,
    pub root_trace_name: String,
    pub start_time_unix_nano: u64,
    /// How long the trace ran, from the earliest span start to the latest span
    /// end. The Tempo search JSON shows this duration twice, as `durationMs`
    /// and as the span-set `durationNanos`. Both come from this one field.
    pub duration: Time,
    pub span_sets: Vec<SpanSet>,
}

/// Search response.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchResponse {
    pub traces: Vec<TraceResult>,
    pub inspected_traces: usize,
    /// Approximate span data the query inspected: the decoded size of the
    /// scanned cold and live batches, before filtering. The engine reports this
    /// value as the Tempo search `metrics.inspectedBytes`.
    pub inspected: ByteSize,
}

/// Full span set for one trace.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceSpans {
    pub trace_id: [u8; 16],
    pub root_service_name: String,
    pub root_trace_name: String,
    pub resource_attributes: Vec<(String, AttrValue)>,
    pub spans: Vec<SpanRef>,
}

/// Tag discovery scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagScope {
    Resource,
    Span,
    Intrinsic,
    Event,
    Link,
    Instrumentation,
}

/// Tag names grouped by scope.
#[derive(Clone, Debug, PartialEq)]
pub struct ScopedTag {
    pub scope: TagScope,
    pub tags: Vec<String>,
}

/// One typed tag value.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedValue {
    pub type_: String,
    pub value: String,
}

/// One `TraceQL` metrics series.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceMetricSeries {
    pub labels: Vec<(String, String)>,
    pub points: Vec<(i64, f64)>,
    pub exemplars: Vec<TraceMetricExemplar>,
}

/// One Prometheus-style exemplar attached to a `TraceQL` metrics series.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceMetricExemplar {
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub timestamp_ns: i64,
}

/// `TraceQL` metrics response.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceMetricsResponse {
    pub series: Vec<TraceMetricSeries>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::{bytes, millis, nanos};

    use super::*;

    #[test]
    fn span_ref_holds_typed_attributes() {
        let s = SpanRef {
            span_id: [1; 8],
            parent_span_id: None,
            name: "op".into(),
            kind: 0,
            nested_set_left: 0,
            nested_set_right: 0,
            nested_set_parent: 0,
            start_time_unix_nano: 1000,
            duration: nanos(42),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            resource_attributes: Vec::new(),
            attributes: vec![
                ("http.status_code".into(), AttrValue::Int(200)),
                ("ok".into(), AttrValue::Bool(true)),
            ],
            events: Vec::new(),
            links: Vec::new(),
        };
        assert!(s.attributes[0].1 == AttrValue::Int(200));
        assert!(s.attributes[1].1 == AttrValue::Bool(true));
    }

    #[test]
    fn search_response_nests_span_sets() {
        let resp = SearchResponse {
            traces: vec![TraceResult {
                trace_id: [0xAB; 16],
                root_service_name: "checkout".into(),
                root_trace_name: "POST /pay".into(),
                start_time_unix_nano: 5,
                duration: millis(12),
                span_sets: vec![SpanSet {
                    spans: vec![],
                    matched: 3,
                }],
            }],
            inspected_traces: 1,
            inspected: bytes(4096),
        };
        assert!(resp.traces[0].span_sets[0].matched == 3);
        assert!(resp.traces[0].trace_id == [0xAB; 16]);
        assert!(resp.inspected_traces == 1);
        assert!(resp.inspected == bytes(4096));
    }

    #[test]
    fn tag_scope_is_copy() {
        let s = TagScope::Span;
        let c = s;
        assert!(s == TagScope::Span);
        assert!(c == TagScope::Span);
    }
}
