//! Read-side wrapper over the traces hot tier.

use std::sync::Arc;

use arrow::{
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use crabka_traceql::{
    AttrValue, EventRef, LinkRef, ScopedTag, SpanRef, TagScope, TraceSpans, TraceqlError,
    TypedValue,
};
use crabka_units::{Time, convert::TimeExt as _};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, any_value::Value as OtlpValue},
    trace::v1::TracesData,
};
use prost::Message as _;
use reqwest::Url;

use super::store::SharedTraceIndex;

pub type Result<T> = std::result::Result<T, TraceqlError>;
const LIVE_SPAN_BATCHES_PATH: &str = "/api/crabka/live/span-batches";

/// OTLP carries nanosecond fields as `uint64`. Saturate rather than wrap when
/// one exceeds what a `Time` extent can be built from.
fn time_from_nanos_u64(nanos: u64) -> Time {
    Time::from_nanos(i64::try_from(nanos).unwrap_or(i64::MAX))
}

#[async_trait::async_trait]
pub trait LiveSource: Send + Sync {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>>;

    async fn trace_spans(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>>;

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>>;

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>>;

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64;
}

///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn encode_span_batches(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut out, &first.schema())
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        }
        writer
            .finish()
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
    }
    Ok(out)
}

fn decode_span_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader =
        StreamReader::try_new(bytes, None).map_err(|err| TraceqlError::Plan(err.to_string()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| TraceqlError::Plan(err.to_string()))
}

pub struct RemoteLiveSource {
    base_url: Url,
    trace_index: SharedTraceIndex,
    http: reqwest::Client,
}

impl RemoteLiveSource {
    #[must_use]
    pub fn new(base_url: Url, trace_index: SharedTraceIndex) -> Self {
        Self {
            base_url,
            trace_index,
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl LiveSource for RemoteLiveSource {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>> {
        let mut url = self
            .base_url
            .join(LIVE_SPAN_BATCHES_PATH)
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("start", &start_ns.to_string())
            .append_pair("end", &end_ns.to_string());
        let resp = self
            .http
            .get(url)
            .header("x-scope-orgid", tenant)
            .send()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        if !resp.status().is_success() {
            return Err(TraceqlError::Plan(format!(
                "remote live-store returned {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        decode_span_batches(&bytes)
    }

    async fn trace_spans(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>> {
        // Use the v1 endpoint for internal federation: it returns the bare OTLP
        // `TracesData` we decode below. The v2 endpoint wraps the trace in a
        // Tempo `TraceByIDResponse` for Grafana's backend datasource.
        let path = format!("/api/traces/{}", hex::encode(trace_id));
        let url = self
            .base_url
            .join(&path)
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        let resp = self
            .http
            .get(url)
            .header("x-scope-orgid", tenant)
            .header("accept", "application/x-protobuf")
            .send()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(TraceqlError::Plan(format!(
                "remote live-store returned {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        let data = TracesData::decode(bytes).map_err(|err| TraceqlError::Plan(err.to_string()))?;
        trace_spans_from_otlp(trace_id, data).map(Some)
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        let mut url = self
            .base_url
            .join("/api/v2/search/tags")
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("start", &ns_floor_seconds(start_ns).to_string())
                .append_pair("end", &ns_ceil_seconds(end_ns).to_string());
            if let Some(scope) = scope {
                query.append_pair("scope", tag_scope_name(scope));
            }
        }
        let json = self.get_json(tenant, url).await?;
        scoped_tags_from_json(&json)
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        let mut url = self
            .base_url
            .join(&format!("/api/v2/search/tag/{tag}/values"))
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("start", &ns_floor_seconds(start_ns).to_string())
            .append_pair("end", &ns_ceil_seconds(end_ns).to_string());
        let json = self.get_json(tenant, url).await?;
        typed_values_from_json(&json)
    }

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        let trace_index = self.trace_index.load();
        trace_index
            .trace_blocks(tenant)
            .iter()
            .map(|block| block.max_ts.saturating_add(1))
            .max()
            .unwrap_or_default()
    }
}

impl RemoteLiveSource {
    async fn get_json(&self, tenant: &str, url: Url) -> Result<serde_json::Value> {
        let resp = self
            .http
            .get(url)
            .header("x-scope-orgid", tenant)
            .send()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        if !resp.status().is_success() {
            return Err(TraceqlError::Plan(format!(
                "remote live-store returned {}",
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))
    }
}

fn tag_scope_name(scope: TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}

fn ns_floor_seconds(ns: i64) -> i64 {
    ns.div_euclid(1_000_000_000)
}

fn ns_ceil_seconds(ns: i64) -> i64 {
    ns.div_euclid(1_000_000_000) + i64::from(ns.rem_euclid(1_000_000_000) != 0)
}

fn tag_scope_from_name(value: &str) -> Option<TagScope> {
    match value {
        "resource" => Some(TagScope::Resource),
        "span" => Some(TagScope::Span),
        "intrinsic" => Some(TagScope::Intrinsic),
        "event" => Some(TagScope::Event),
        "link" => Some(TagScope::Link),
        "instrumentation" => Some(TagScope::Instrumentation),
        _ => None,
    }
}

fn scoped_tags_from_json(json: &serde_json::Value) -> Result<Vec<ScopedTag>> {
    let scopes = json
        .get("scopes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            TraceqlError::Plan("remote live-store tags response missing scopes".into())
        })?;
    let mut out = Vec::new();
    for scope in scopes {
        let Some(name) = scope.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(scope_name) = tag_scope_from_name(name) else {
            continue;
        };
        let tags = scope
            .get("tags")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|tag| tag.as_str().map(ToString::to_string))
            .collect();
        out.push(ScopedTag {
            scope: scope_name,
            tags,
        });
    }
    Ok(out)
}

fn typed_values_from_json(json: &serde_json::Value) -> Result<Vec<TypedValue>> {
    let values = json
        .get("tagValues")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            TraceqlError::Plan("remote live-store tag values response missing tagValues".into())
        })?;
    Ok(values
        .iter()
        .filter_map(|value| {
            Some(TypedValue {
                type_: value.get("type")?.as_str()?.to_string(),
                value: value.get("value")?.as_str()?.to_string(),
            })
        })
        .collect())
}

fn trace_spans_from_otlp(trace_id: &[u8; 16], data: TracesData) -> Result<TraceSpans> {
    let mut trace = TraceSpans {
        trace_id: *trace_id,
        root_service_name: String::new(),
        root_trace_name: String::new(),
        resource_attributes: Vec::new(),
        spans: Vec::new(),
    };
    for resource_spans in data.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .as_ref()
            .map_or_else(Vec::new, |resource| attrs_from_otlp(&resource.attributes));
        if trace.resource_attributes.is_empty() {
            trace.resource_attributes.clone_from(&resource_attrs);
        }
        if trace.root_service_name.is_empty() {
            trace.root_service_name = resource_attrs
                .iter()
                .find_map(|(key, value)| {
                    (key == "service.name").then(|| match value {
                        AttrValue::Str(value) => Some(value.clone()),
                        _ => None,
                    })?
                })
                .unwrap_or_default();
        }
        for scope_spans in resource_spans.scope_spans {
            let (instrumentation_name, instrumentation_version) = scope_spans
                .scope
                .map_or_else(Default::default, |scope| (scope.name, scope.version));
            for span in scope_spans.spans {
                let span_id = fixed_8(&span.span_id)?;
                let parent_span_id = if span.parent_span_id.is_empty() {
                    None
                } else {
                    Some(fixed_8(&span.parent_span_id)?)
                };
                let duration = time_from_nanos_u64(
                    span.end_time_unix_nano
                        .saturating_sub(span.start_time_unix_nano),
                );
                if trace.root_trace_name.is_empty() && parent_span_id.is_none() {
                    trace.root_trace_name.clone_from(&span.name);
                }
                let status = span.status.unwrap_or_default();
                trace.spans.push(SpanRef {
                    span_id,
                    parent_span_id,
                    name: span.name,
                    kind: span.kind,
                    nested_set_left: 0,
                    nested_set_right: 0,
                    nested_set_parent: 0,
                    start_time_unix_nano: span.start_time_unix_nano,
                    duration,
                    status_code: status.code,
                    status_message: status.message,
                    instrumentation_name: instrumentation_name.clone(),
                    instrumentation_version: instrumentation_version.clone(),
                    resource_attributes: resource_attrs.clone(),
                    attributes: attrs_from_otlp(&span.attributes),
                    events: span
                        .events
                        .into_iter()
                        .map(|event| EventRef {
                            time_since_start: time_from_nanos_u64(
                                event
                                    .time_unix_nano
                                    .saturating_sub(span.start_time_unix_nano),
                            ),
                            name: event.name,
                            attributes: attrs_from_otlp(&event.attributes),
                        })
                        .collect(),
                    links: span
                        .links
                        .into_iter()
                        .map(|link| {
                            Ok(LinkRef {
                                trace_id: fixed_16(&link.trace_id)?,
                                span_id: fixed_8(&link.span_id)?,
                                attributes: attrs_from_otlp(&link.attributes),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                });
            }
        }
    }
    if trace.root_trace_name.is_empty() {
        trace.root_trace_name = trace
            .spans
            .first()
            .map(|span| span.name.clone())
            .unwrap_or_default();
    }
    Ok(trace)
}

fn attrs_from_otlp(
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Vec<(String, AttrValue)> {
    attrs
        .iter()
        .filter_map(|attr| {
            attr.value
                .as_ref()
                .and_then(attr_value_from_otlp)
                .map(|value| (attr.key.clone(), value))
        })
        .collect()
}

fn attr_value_from_otlp(value: &AnyValue) -> Option<AttrValue> {
    match value.value.as_ref()? {
        OtlpValue::StringValue(value) => Some(AttrValue::Str(value.clone())),
        OtlpValue::IntValue(value) => Some(AttrValue::Int(*value)),
        OtlpValue::DoubleValue(value) => Some(AttrValue::Float(*value)),
        OtlpValue::BoolValue(value) => Some(AttrValue::Bool(*value)),
        OtlpValue::BytesValue(value) => Some(AttrValue::Str(hex::encode(value))),
        OtlpValue::ArrayValue(array) => array.values.first().and_then(attr_value_from_otlp),
        OtlpValue::KvlistValue(_) | OtlpValue::StringValueStrindex(_) => None,
    }
}

fn fixed_16(bytes: &[u8]) -> Result<[u8; 16]> {
    bytes
        .try_into()
        .map_err(|_| TraceqlError::Plan("expected 16-byte trace id".into()))
}

fn fixed_8(bytes: &[u8]) -> Result<[u8; 8]> {
    bytes
        .try_into()
        .map_err(|_| TraceqlError::Plan("expected 8-byte span id".into()))
}

pub struct LiveTier {
    source: Arc<dyn LiveSource>,
}

impl LiveTier {
    #[must_use]
    pub fn new(source: Arc<dyn LiveSource>) -> Self {
        Self { source }
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>> {
        self.source.span_batches(tenant, start_ns, end_ns).await
    }

    ///
    /// # Errors
    /// Returns an error when the live source query fails.
    pub async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>> {
        self.source.trace_spans(tenant, trace_id).await
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        self.source.tag_names(tenant, scope, start_ns, end_ns).await
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        self.source.tag_values(tenant, tag, start_ns, end_ns).await
    }

    #[must_use]
    pub fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        self.source.block_builder_frontier_ns(tenant)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow::record_batch::RecordBatch;
    use assert2::check;
    use crabka_traceql::{AttrValue, ScopedTag, SpanRef, TagScope, TraceSpans, TypedValue};
    use crabka_units::nanos;

    use super::*;

    #[derive(Default)]
    struct FakeLiveSource {
        batches: Vec<RecordBatch>,
        trace: Option<TraceSpans>,
        tags: Vec<ScopedTag>,
        values: Vec<TypedValue>,
        frontiers: BTreeMap<String, i64>,
    }

    #[async_trait::async_trait]
    impl LiveSource for FakeLiveSource {
        async fn span_batches(
            &self,
            _tenant: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<RecordBatch>> {
            Ok(self.batches.clone())
        }

        async fn trace_spans(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<TraceSpans>> {
            Ok(self.trace.clone())
        }

        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<ScopedTag>> {
            Ok(self.tags.clone())
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<TypedValue>> {
            Ok(self.values.clone())
        }

        fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
            self.frontiers.get(tenant).copied().unwrap_or_default()
        }
    }

    fn trace() -> TraceSpans {
        TraceSpans {
            trace_id: [1; 16],
            root_service_name: "api".into(),
            root_trace_name: "GET /".into(),
            resource_attributes: vec![("service.name".into(), AttrValue::Str("api".into()))],
            spans: vec![SpanRef {
                span_id: [2; 8],
                parent_span_id: None,
                name: "GET /".into(),
                kind: 0,
                nested_set_left: 1,
                nested_set_right: 2,
                nested_set_parent: 0,
                start_time_unix_nano: 2_000,
                duration: nanos(50),
                status_code: 0,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                resource_attributes: vec![("service.name".into(), AttrValue::Str("api".into()))],
                attributes: vec![("svc".into(), AttrValue::Str("api".into()))],
                events: Vec::new(),
                links: Vec::new(),
            }],
        }
    }

    #[tokio::test]
    async fn live_tier_delegates_reads_to_source() {
        let mut source = FakeLiveSource {
            trace: Some(trace()),
            tags: vec![ScopedTag {
                scope: TagScope::Span,
                tags: vec!["svc".into()],
            }],
            values: vec![TypedValue {
                type_: "string".into(),
                value: "api".into(),
            }],
            ..FakeLiveSource::default()
        };
        source.frontiers.insert("tenant-a".into(), 1_500);
        let live = LiveTier::new(Arc::new(source));

        check!(live.block_builder_frontier_ns("tenant-a") == 1_500);
        check!(
            live.span_batches("tenant-a", 0, 5_000)
                .await
                .unwrap()
                .is_empty()
        );
        check!(
            live.trace_spans("tenant-a", &[1; 16])
                .await
                .unwrap()
                .unwrap()
                .spans
                .len()
                == 1
        );
        check!(
            live.tag_names("tenant-a", Some(TagScope::Span), 0, 5_000)
                .await
                .unwrap()[0]
                .tags
                == vec!["svc"]
        );
        check!(live.tag_values("tenant-a", ".svc", 0, 5_000).await.unwrap()[0].value == "api");
    }
}
