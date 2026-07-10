//! The Tempo HTTP-API JSON edge model the query-frontend renders and parses.
//!
//! This is the same body shape the querier (Slice 5, `querier/http`) emits; the
//! frontend parses per-job partials, merges them (respecting `limit`/`spss`),
//! accumulates the `metrics{}` job-accounting block, and re-emits this exact
//! shape. The trace values it carries (`TraceResult`/`SpanSet`/`SpanRef`) are the
//! pinned `crabka-traceql` (Slice 2) result types; this module is their HTTP
//! projection.
//!
//! Note: the `crabka-traceql` result types do **not** derive serde, so the
//! search edge model is a standalone serde mirror with lossless `From` /
//! reverse-`From` projections; the by-id edge model is a minimal typed OTLP-JSON
//! mirror (`TraceByIdResponseJson`) shaped to the querier's v2 body.

use crabka_traceql::{AttrValue, SpanRef, SpanSet, TraceResult};
use serde::{Deserialize, Serialize};

/// The `/api/search` response: matched traces + the job-accounting metrics.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchResponseJson {
    #[serde(default)]
    pub traces: Vec<TraceJson>,
    #[serde(default)]
    pub metrics: Metrics,
}

/// One matched trace in the search response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceJson {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    #[serde(default)]
    pub root_service_name: String,
    #[serde(default)]
    pub root_trace_name: String,
    /// Nanos since epoch, **string-encoded** (Tempo quirk).
    pub start_time_unix_nano: String,
    /// Whole milliseconds, integer.
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub span_sets: Vec<SpanSetJson>,
}

/// A spanSet: the spans this trace matched plus the matched count.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpanSetJson {
    #[serde(default)]
    pub spans: Vec<SpanJson>,
    #[serde(default)]
    pub matched: u32,
}

/// A single matched span (string-encoded nanos, OTLP-KV attributes).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpanJson {
    #[serde(rename = "spanID")]
    pub span_id: String,
    pub start_time_unix_nano: String,
    pub duration_nanos: String,
    #[serde(default)]
    pub attributes: Vec<KeyValueJson>,
}

/// OTLP key/value attribute form (matches the querier's `attrs_json`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValueJson {
    pub key: String,
    pub value: AnyValueJson,
}

/// OTLP `AnyValue` (the variants `TraceQL` surfaces). Tempo emits `intValue` as a
/// string and groups multi-valued attributes under `arrayValue`, matching the
/// querier's `attr_value_json` / `attr_values_json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AnyValueJson {
    #[serde(rename = "stringValue")]
    StringValue(String),
    #[serde(rename = "intValue")]
    IntValue(String),
    #[serde(rename = "doubleValue")]
    DoubleValue(f64),
    #[serde(rename = "boolValue")]
    BoolValue(bool),
    #[serde(rename = "arrayValue")]
    ArrayValue(ArrayValueJson),
}

/// OTLP `ArrayValue` body.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArrayValueJson {
    #[serde(default)]
    pub values: Vec<AnyValueJson>,
}

/// The job-accounting `metrics{}` block. Additive over completed jobs.
///
/// The querier (Slice 5) only populates `total_blocks`/`inspected_traces`/
/// `inspected_bytes` today; `total_jobs`/`completed_jobs`/`inspected_spans` are
/// frontend-owned (seeded from the plan / summed across jobs) — all serialize so
/// the merged body carries the full accounting block.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub total_jobs: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub completed_jobs: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub total_blocks: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub inspected_traces: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub inspected_bytes: u64,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    pub inspected_spans: u64,
}

impl Metrics {
    /// Fold another job's accounting into this one (field-wise saturating sum).
    pub fn add(&mut self, other: &Metrics) {
        self.total_jobs = self.total_jobs.saturating_add(other.total_jobs);
        self.completed_jobs = self.completed_jobs.saturating_add(other.completed_jobs);
        self.total_blocks = self.total_blocks.saturating_add(other.total_blocks);
        self.inspected_traces = self.inspected_traces.saturating_add(other.inspected_traces);
        self.inspected_bytes = self.inspected_bytes.saturating_add(other.inspected_bytes);
        self.inspected_spans = self.inspected_spans.saturating_add(other.inspected_spans);
    }
}

/// Deserialize a `u64` that the querier may encode as a JSON number **or** a
/// string (Tempo encodes some accounting counters as strings).
fn de_u64_lenient<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize as _;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(u64),
        Str(String),
    }

    match NumOrStr::deserialize(deserializer)? {
        NumOrStr::Num(n) => Ok(n),
        NumOrStr::Str(s) => Ok(s.parse().unwrap_or(0)),
    }
}

/// Lowercase hex for a 16-byte trace id.
#[must_use]
pub fn hex16(id: &[u8; 16]) -> String {
    hex::encode(id)
}

/// Lowercase hex for an 8-byte span id.
#[must_use]
pub fn hex8(id: &[u8; 8]) -> String {
    hex::encode(id)
}

/// Parse a lowercase-hex 16-byte trace id (lossless inverse of [`hex16`]).
#[must_use]
pub fn parse_hex16(s: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let _ = hex::decode_to_slice(s, &mut out);
    out
}

/// Parse a lowercase-hex 8-byte span id (lossless inverse of [`hex8`]).
#[must_use]
pub fn parse_hex8(s: &str) -> [u8; 8] {
    let mut out = [0u8; 8];
    let _ = hex::decode_to_slice(s, &mut out);
    out
}

impl From<&AttrValue> for AnyValueJson {
    fn from(v: &AttrValue) -> Self {
        match v {
            AttrValue::Str(s) => AnyValueJson::StringValue(s.clone()),
            AttrValue::Int(i) => AnyValueJson::IntValue(i.to_string()),
            AttrValue::Float(f) => AnyValueJson::DoubleValue(*f),
            AttrValue::Bool(b) => AnyValueJson::BoolValue(*b),
        }
    }
}

impl From<&AnyValueJson> for AttrValue {
    fn from(v: &AnyValueJson) -> Self {
        match v {
            AnyValueJson::StringValue(s) => AttrValue::Str(s.clone()),
            AnyValueJson::IntValue(i) => AttrValue::Int(i.parse().unwrap_or(0)),
            AnyValueJson::DoubleValue(f) => AttrValue::Float(*f),
            AnyValueJson::BoolValue(b) => AttrValue::Bool(*b),
            // An OTLP array attribute has no single scalar form; project its
            // first scalar (`TraceQL` search attributes are scalar in practice).
            AnyValueJson::ArrayValue(a) => a
                .values
                .first()
                .map_or(AttrValue::Str(String::new()), AttrValue::from),
        }
    }
}

impl From<&SpanRef> for SpanJson {
    fn from(s: &SpanRef) -> Self {
        SpanJson {
            span_id: hex8(&s.span_id),
            start_time_unix_nano: s.start_time_unix_nano.to_string(),
            duration_nanos: s.duration_nanos.to_string(),
            attributes: s
                .attributes
                .iter()
                .map(|(k, v)| KeyValueJson {
                    key: k.clone(),
                    value: AnyValueJson::from(v),
                })
                .collect(),
        }
    }
}

impl From<&SpanSet> for SpanSetJson {
    fn from(ss: &SpanSet) -> Self {
        SpanSetJson {
            spans: ss.spans.iter().map(SpanJson::from).collect(),
            matched: ss.matched,
        }
    }
}

impl From<&TraceResult> for TraceJson {
    fn from(t: &TraceResult) -> Self {
        TraceJson {
            trace_id: hex16(&t.trace_id),
            root_service_name: t.root_service_name.clone(),
            root_trace_name: t.root_trace_name.clone(),
            start_time_unix_nano: t.start_time_unix_nano.to_string(),
            duration_ms: t.duration_ms,
            span_sets: t.span_sets.iter().map(SpanSetJson::from).collect(),
        }
    }
}

impl From<&SpanJson> for SpanRef {
    fn from(s: &SpanJson) -> Self {
        SpanRef {
            span_id: parse_hex8(&s.span_id),
            parent_span_id: None,
            name: String::new(),
            kind: 0,
            nested_set_left: 0,
            nested_set_right: 0,
            nested_set_parent: 0,
            start_time_unix_nano: s.start_time_unix_nano.parse().unwrap_or(0),
            duration_nanos: s.duration_nanos.parse().unwrap_or(0),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            resource_attributes: Vec::new(),
            attributes: s
                .attributes
                .iter()
                .map(|kv| (kv.key.clone(), AttrValue::from(&kv.value)))
                .collect(),
            events: Vec::new(),
            links: Vec::new(),
        }
    }
}

impl From<&SpanSetJson> for SpanSet {
    fn from(ss: &SpanSetJson) -> Self {
        SpanSet {
            spans: ss.spans.iter().map(SpanRef::from).collect(),
            matched: ss.matched,
        }
    }
}

impl From<&TraceJson> for TraceResult {
    fn from(t: &TraceJson) -> Self {
        TraceResult {
            trace_id: parse_hex16(&t.trace_id),
            root_service_name: t.root_service_name.clone(),
            root_trace_name: t.root_trace_name.clone(),
            start_time_unix_nano: t.start_time_unix_nano.parse().unwrap_or(0),
            duration_nanos: t.duration_ms.saturating_mul(1_000_000),
            duration_ms: t.duration_ms,
            span_sets: t.span_sets.iter().map(SpanSet::from).collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Trace-by-id v2 edge model.
//
// A minimal typed OTLP-JSON mirror of the querier's `/api/v2/traces/{id}` body:
// `{ trace: { resourceSpans: [...] }, status, message }`. Just enough nested
// structure (resourceSpans -> scopeSpans -> spans with spanId) to union
// resourceSpans across queriers, dedupe spans by spanId, and size-estimate the
// assembled trace. We carry the rest of each span as an opaque
// `serde_json::Value` so we round-trip the querier's exact span JSON (kind /
// status / events / links / nanos) without re-stating its full shape.
// ---------------------------------------------------------------------------

/// The querier's v2 by-id response body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TraceByIdResponseJson {
    #[serde(default)]
    pub trace: TraceEnvelopeJson,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub message: String,
}

/// The `trace` envelope: the OTLP `resourceSpans` array.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceEnvelopeJson {
    #[serde(default)]
    pub resource_spans: Vec<ResourceSpansJson>,
}

/// One OTLP `ResourceSpans` group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSpansJson {
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub resource: serde_json::Value,
    #[serde(default)]
    pub scope_spans: Vec<ScopeSpansJson>,
}

/// One OTLP `ScopeSpans` group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeSpansJson {
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub scope: serde_json::Value,
    #[serde(default)]
    pub spans: Vec<OtlpSpanJson>,
}

/// One OTLP span. `span_id` is extracted for dedup; the whole span is preserved
/// in `rest` so serialization re-emits the querier's exact span shape.
///
/// GAP5 (confirmed correct, not a bug): the by-id span key is `spanId`
/// **base64**-encoded — the standard OTLP protobuf-JSON byte-field encoding the
/// querier's `trace_json` emits — whereas search results (`SpanJson`) key on
/// `spanID` **hex** (Tempo's search shape). The two are different Tempo response
/// formats, and each pipeline is internally consistent (by-id is base64
/// end-to-end, search is hex end-to-end), so the respective dedup keys never mix
/// encodings. No conversion is needed or correct here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OtlpSpanJson {
    #[serde(rename = "spanId", default)]
    pub span_id: String,
    #[serde(flatten)]
    pub rest: serde_json::Map<String, serde_json::Value>,
}

impl TraceByIdResponseJson {
    /// Total spans across all resource/scope groups.
    #[must_use]
    pub fn span_count(&self) -> usize {
        self.trace
            .resource_spans
            .iter()
            .flat_map(|rs| rs.scope_spans.iter())
            .map(|ss| ss.spans.len())
            .sum()
    }

    /// Cheap byte-size estimate of the assembled trace (serialized length).
    #[must_use]
    pub fn approx_size_bytes(&self) -> u64 {
        serde_json::to_vec(&self.trace).map_or(0, |v| v.len() as u64)
    }

    /// True when this body carries no spans (a querier that did not hold the
    /// trace returns an empty/None body, which we model as no resourceSpans).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.span_count() == 0
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn search_response_serializes_as_tempo_json() {
        let resp = SearchResponseJson {
            traces: vec![TraceJson {
                trace_id: "0a".repeat(16),
                root_service_name: "checkout".to_string(),
                root_trace_name: "POST /pay".to_string(),
                start_time_unix_nano: "1700000000000000000".to_string(),
                duration_ms: 42,
                span_sets: vec![SpanSetJson {
                    spans: vec![SpanJson {
                        span_id: "0b".repeat(8),
                        start_time_unix_nano: "1700000000000000000".to_string(),
                        duration_nanos: "42000000".to_string(),
                        attributes: vec![],
                    }],
                    matched: 1,
                }],
            }],
            metrics: Metrics {
                total_jobs: 3,
                completed_jobs: 3,
                total_blocks: 2,
                inspected_traces: 10,
                inspected_bytes: 4096,
                inspected_spans: 50,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert2::assert!(
            json == serde_json::json!({
                "traces": [{
                    "traceID": "0a".repeat(16),
                    "rootServiceName": "checkout",
                    "rootTraceName": "POST /pay",
                    "startTimeUnixNano": "1700000000000000000",
                    "durationMs": 42,
                    "spanSets": [{
                        "spans": [{
                            "spanID": "0b".repeat(8),
                            "startTimeUnixNano": "1700000000000000000",
                            "durationNanos": "42000000",
                            "attributes": []
                        }],
                        "matched": 1
                    }]
                }],
                "metrics": {
                    "totalJobs": 3,
                    "completedJobs": 3,
                    "totalBlocks": 2,
                    "inspectedTraces": 10,
                    "inspectedBytes": 4096,
                    "inspectedSpans": 50
                }
            })
        );
    }

    #[test]
    fn search_response_parses_querier_body() {
        // The shape the querier's `search_json` emits, with string-encoded
        // metrics counters.
        let body = serde_json::json!({
            "traces": [{
                "traceID": "ab".repeat(16),
                "rootServiceName": "svc",
                "rootTraceName": "GET /",
                "startTimeUnixNano": "5",
                "durationMs": 12,
                "spanSets": [{
                    "spans": [{
                        "spanID": "cd".repeat(8),
                        "startTimeUnixNano": "5",
                        "durationNanos": "1000",
                        "attributes": [
                            { "key": "http.method", "value": { "stringValue": "GET" } },
                            { "key": "http.status", "value": { "intValue": "200" } }
                        ]
                    }],
                    "matched": 3
                }]
            }],
            "metrics": { "totalBlocks": "2", "inspectedTraces": "3", "inspectedBytes": "5" }
        });
        let resp: SearchResponseJson = serde_json::from_value(body).unwrap();
        assert2::assert!(
            resp == SearchResponseJson {
                traces: vec![TraceJson {
                    trace_id: "ab".repeat(16),
                    root_service_name: "svc".to_string(),
                    root_trace_name: "GET /".to_string(),
                    start_time_unix_nano: "5".to_string(),
                    duration_ms: 12,
                    span_sets: vec![SpanSetJson {
                        spans: vec![SpanJson {
                            span_id: "cd".repeat(8),
                            start_time_unix_nano: "5".to_string(),
                            duration_nanos: "1000".to_string(),
                            attributes: vec![
                                KeyValueJson {
                                    key: "http.method".to_string(),
                                    value: AnyValueJson::StringValue("GET".to_string()),
                                },
                                KeyValueJson {
                                    key: "http.status".to_string(),
                                    value: AnyValueJson::IntValue("200".to_string()),
                                },
                            ],
                        }],
                        matched: 3,
                    }],
                }],
                metrics: Metrics {
                    total_jobs: 0,
                    completed_jobs: 0,
                    total_blocks: 2,
                    inspected_traces: 3,
                    inspected_bytes: 5,
                    inspected_spans: 0,
                },
            }
        );
    }

    #[test]
    fn metrics_add_is_additive() {
        let mut a = Metrics::default();
        a.add(&Metrics {
            total_jobs: 1,
            completed_jobs: 1,
            total_blocks: 1,
            inspected_traces: 2,
            inspected_bytes: 100,
            inspected_spans: 9,
        });
        a.add(&Metrics {
            total_jobs: 1,
            completed_jobs: 1,
            total_blocks: 1,
            inspected_traces: 3,
            inspected_bytes: 200,
            inspected_spans: 11,
        });
        assert2::assert!(
            a == Metrics {
                total_jobs: 2,
                completed_jobs: 2,
                total_blocks: 2,
                inspected_traces: 5,
                inspected_bytes: 300,
                inspected_spans: 20,
            }
        );
    }

    #[test]
    fn hex_encodes_lowercase() {
        assert2::assert!(hex16(&[0xab; 16]) == "ab".repeat(16));
        assert2::assert!(hex8(&[0x0f; 8]) == "0f".repeat(8));
    }

    #[test]
    fn trace_result_round_trips_through_json_projection() {
        use crabka_traceql::{AttrValue, SpanRef, SpanSet, TraceResult};

        let span = SpanRef {
            span_id: [7; 8],
            parent_span_id: None,
            name: "op".into(),
            kind: 0,
            nested_set_left: 0,
            nested_set_right: 0,
            nested_set_parent: 0,
            start_time_unix_nano: 1234,
            duration_nanos: 56,
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            resource_attributes: Vec::new(),
            attributes: vec![("k".into(), AttrValue::Int(9))],
            events: Vec::new(),
            links: Vec::new(),
        };
        let trace = TraceResult {
            trace_id: [3; 16],
            root_service_name: "svc".into(),
            root_trace_name: "GET /".into(),
            start_time_unix_nano: 1234,
            duration_nanos: 5_000_000,
            duration_ms: 5,
            span_sets: vec![SpanSet {
                spans: vec![span],
                matched: 1,
            }],
        };
        let json = TraceJson::from(&trace);
        let back = TraceResult::from(&json);
        assert2::assert!(
            back == TraceResult {
                trace_id: [3; 16],
                root_service_name: "svc".into(),
                root_trace_name: "GET /".into(),
                start_time_unix_nano: 1234,
                duration_nanos: 5_000_000,
                duration_ms: 5,
                span_sets: vec![SpanSet {
                    spans: vec![SpanRef {
                        span_id: [7; 8],
                        parent_span_id: None,
                        name: String::new(),
                        kind: 0,
                        nested_set_left: 0,
                        nested_set_right: 0,
                        nested_set_parent: 0,
                        start_time_unix_nano: 1234,
                        duration_nanos: 56,
                        status_code: 0,
                        status_message: String::new(),
                        instrumentation_name: String::new(),
                        instrumentation_version: String::new(),
                        resource_attributes: Vec::new(),
                        attributes: vec![("k".into(), AttrValue::Int(9))],
                        events: Vec::new(),
                        links: Vec::new(),
                    }],
                    matched: 1,
                }],
            }
        );
    }
}
