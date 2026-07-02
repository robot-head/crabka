//! In-memory `SpanStore` used by engine and planner tests.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::error::{Result, TraceqlError};
use crate::result::{
    AttrValue, EventRef, LinkRef, ScopedTag, SpanRef, TagScope, TraceSpans, TypedValue,
};
use crate::span_columns::{
    EVENT_ATTR_PREFIX, InputSpan, LINK_ATTR_PREFIX, NestedSet, assign_nested_set,
    span_schema_with_attrs,
};
use crate::store::{MatchCmp, MatchScope, MatchValue, ScanResult, SpanMatcher, SpanStore};

const INTRINSIC_TAGS: &[&str] = &[
    "span:childCount",
    "span:duration",
    "span:id",
    "span:kind",
    "span:name",
    "span:parentID",
    "span:nestedSetLeft",
    "span:nestedSetParent",
    "span:nestedSetRight",
    "span:status",
    "span:statusMessage",
    "trace:duration",
    "trace:id",
    "trace:rootName",
    "trace:rootService",
];
const EVENT_TAGS: &[&str] = &["event:name", "event:timeSinceStart"];
const LINK_TAGS: &[&str] = &["link:spanID", "link:traceID"];

struct StoredTrace {
    trace_id: [u8; 16],
    root_service_name: String,
    root_span_name: String,
    trace_start_unix_nano: i64,
    trace_duration_nanos: i64,
    spans: Vec<InputSpan>,
    nested: Vec<NestedSet>,
}

/// In-memory span store keyed by tenant.
#[derive(Default)]
pub struct InMemorySpanStore {
    traces: HashMap<String, Vec<StoredTrace>>,
}

impl InMemorySpanStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_trace(
        &mut self,
        tenant: &str,
        root_service_name: &str,
        root_span_name: &str,
        spans: Vec<InputSpan>,
    ) {
        let trace_id = spans.first().map_or([0; 16], |s| s.trace_id);
        let trace_start_unix_nano = spans.iter().map(|s| s.start_unix_nano).min().unwrap_or(0);
        let trace_end_unix_nano = spans
            .iter()
            .map(|s| s.start_unix_nano + s.duration_nanos)
            .max()
            .unwrap_or(trace_start_unix_nano);
        let nested = assign_nested_set(&spans);

        self.traces
            .entry(tenant.to_string())
            .or_default()
            .push(StoredTrace {
                trace_id,
                root_service_name: root_service_name.to_string(),
                root_span_name: root_span_name.to_string(),
                trace_start_unix_nano,
                trace_duration_nanos: trace_end_unix_nano - trace_start_unix_nano,
                spans,
                nested,
            });
    }

    fn attr_columns(
        traces: &[&StoredTrace],
        projection_matchers: &[SpanMatcher],
    ) -> Vec<(String, DataType)> {
        let mut cols = BTreeMap::new();
        for trace in traces {
            for span in &trace.spans {
                for (key, value) in &span.attrs {
                    cols.entry(key.clone()).or_insert_with(|| match value {
                        AttrValue::Str(_) => DataType::Utf8,
                        AttrValue::Int(_) => DataType::Int64,
                        AttrValue::Float(_) => DataType::Float64,
                        AttrValue::Bool(_) => DataType::Boolean,
                    });
                }
                for matcher in projection_matchers {
                    match matcher.scope {
                        MatchScope::Event => {
                            let Some((_, value)) = span
                                .events
                                .iter()
                                .flat_map(|event| event.attributes.iter())
                                .find(|(key, _)| key == &matcher.key)
                            else {
                                continue;
                            };
                            cols.entry(format!("{EVENT_ATTR_PREFIX}{}", matcher.key))
                                .or_insert_with(|| attr_data_type(value));
                        }
                        MatchScope::Link => {
                            let Some((_, value)) = span
                                .links
                                .iter()
                                .flat_map(|link| link.attributes.iter())
                                .find(|(key, _)| key == &matcher.key)
                            else {
                                continue;
                            };
                            cols.entry(format!("{LINK_ATTR_PREFIX}{}", matcher.key))
                                .or_insert_with(|| attr_data_type(value));
                        }
                        MatchScope::Both
                        | MatchScope::Span
                        | MatchScope::Resource
                        | MatchScope::Parent
                        | MatchScope::Instrumentation
                        | MatchScope::Intrinsic => {}
                    }
                }
            }
        }
        cols.into_iter().collect()
    }
}

fn span_ref(span: &InputSpan, nested: &NestedSet) -> SpanRef {
    SpanRef {
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name.clone(),
        kind: span.kind,
        nested_set_left: nested.left,
        nested_set_right: nested.right,
        nested_set_parent: nested.parent_id,
        start_time_unix_nano: u64::try_from(span.start_unix_nano).unwrap_or(0),
        duration_nanos: u64::try_from(span.duration_nanos).unwrap_or(0),
        status_code: span.status_code,
        status_message: span.status_message.clone(),
        instrumentation_name: span.instrumentation_name.clone(),
        instrumentation_version: span.instrumentation_version.clone(),
        resource_attributes: Vec::new(),
        attributes: span.attrs.clone(),
        events: span.events.clone(),
        links: span.links.clone(),
    }
}

enum AttrBuilder {
    Str(StringBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Bool(BooleanBuilder),
}

impl AttrBuilder {
    fn new(dt: &DataType) -> Self {
        match dt {
            DataType::Utf8 => Self::Str(StringBuilder::new()),
            DataType::Int64 => Self::Int(Int64Builder::new()),
            DataType::Float64 => Self::Float(Float64Builder::new()),
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            other => panic!("unsupported attribute data type {other:?}"),
        }
    }

    fn append(&mut self, value: Option<&AttrValue>) {
        match (self, value) {
            (Self::Str(b), Some(AttrValue::Str(v))) => b.append_value(v),
            (Self::Str(b), _) => b.append_null(),
            (Self::Int(b), Some(AttrValue::Int(v))) => b.append_value(*v),
            (Self::Int(b), _) => b.append_null(),
            (Self::Float(b), Some(AttrValue::Float(v))) => b.append_value(*v),
            (Self::Float(b), _) => b.append_null(),
            (Self::Bool(b), Some(AttrValue::Bool(v))) => b.append_value(*v),
            (Self::Bool(b), _) => b.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Str(mut b) => Arc::new(b.finish()),
            Self::Int(mut b) => Arc::new(b.finish()),
            Self::Float(mut b) => Arc::new(b.finish()),
            Self::Bool(mut b) => Arc::new(b.finish()),
        }
    }
}

impl InMemorySpanStore {
    #[allow(clippy::too_many_lines)]
    fn scan_with_projection(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        projection_matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult> {
        let in_range: Vec<&StoredTrace> = self
            .traces
            .get(tenant)
            .into_iter()
            .flatten()
            .filter(|trace| {
                start_ns <= trace.trace_start_unix_nano && trace.trace_start_unix_nano <= end_ns
            })
            .collect();
        let row_count: usize = in_range.iter().map(|trace| trace.spans.len()).sum();
        let attr_cols = Self::attr_columns(&in_range, projection_matchers);
        let schema = span_schema_with_attrs(&attr_cols);

        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(row_count, 16);
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(row_count, 8);
        let mut parent_span_id = FixedSizeBinaryBuilder::with_capacity(row_count, 8);
        let mut ns_left = Int32Builder::new();
        let mut ns_right = Int32Builder::new();
        let mut parent_id = Int32Builder::new();
        let mut child_count = Int32Builder::new();
        let mut root_service = StringBuilder::new();
        let mut root_span = StringBuilder::new();
        let mut trace_start = Int64Builder::new();
        let mut trace_duration = Int64Builder::new();
        let mut name = StringBuilder::new();
        let mut kind = Int32Builder::new();
        let mut start = Int64Builder::new();
        let mut duration = Int64Builder::new();
        let mut status_code = Int32Builder::new();
        let mut status_message = StringBuilder::new();
        let mut instrumentation_name = StringBuilder::new();
        let mut instrumentation_version = StringBuilder::new();
        let mut event_name = StringBuilder::new();
        let mut event_time_since_start = Int64Builder::new();
        let mut link_trace_id = FixedSizeBinaryBuilder::with_capacity(row_count, 16);
        let mut link_span_id = FixedSizeBinaryBuilder::with_capacity(row_count, 8);
        let mut attr_builders: Vec<(String, AttrBuilder)> = attr_cols
            .iter()
            .map(|(key, dt)| (key.clone(), AttrBuilder::new(dt)))
            .collect();

        for trace in &in_range {
            for (i, span) in trace.spans.iter().enumerate() {
                if !span_matches(trace, span, &trace.nested, i, matchers) {
                    continue;
                }
                let expansion_matchers = expansion_matchers(matchers, projection_matchers);
                let event_rows = matching_events_for_scan(span, &expansion_matchers);
                let link_rows = matching_links_for_scan(span, &expansion_matchers);
                for event in event_rows {
                    for link in &link_rows {
                        trace_id
                            .append_value(span.trace_id)
                            .map_err(|e| TraceqlError::Store(e.to_string()))?;
                        span_id
                            .append_value(span.span_id)
                            .map_err(|e| TraceqlError::Store(e.to_string()))?;
                        if let Some(parent) = span.parent_span_id {
                            parent_span_id
                                .append_value(parent)
                                .map_err(|e| TraceqlError::Store(e.to_string()))?;
                        } else {
                            parent_span_id.append_null();
                        }
                        ns_left.append_value(trace.nested[i].left);
                        ns_right.append_value(trace.nested[i].right);
                        parent_id.append_value(trace.nested[i].parent_id);
                        child_count.append_value(child_count_for(&trace.nested, i));
                        root_service.append_value(&trace.root_service_name);
                        root_span.append_value(&trace.root_span_name);
                        trace_start.append_value(trace.trace_start_unix_nano);
                        trace_duration.append_value(trace.trace_duration_nanos);
                        name.append_value(&span.name);
                        kind.append_value(span.kind);
                        start.append_value(span.start_unix_nano);
                        duration.append_value(span.duration_nanos);
                        status_code.append_value(span.status_code);
                        status_message.append_value(&span.status_message);
                        instrumentation_name.append_value(&span.instrumentation_name);
                        instrumentation_version.append_value(&span.instrumentation_version);
                        if let Some(event) = event {
                            event_name.append_value(&event.name);
                            event_time_since_start.append_value(
                                i64::try_from(event.time_since_start_nano).unwrap_or(i64::MAX),
                            );
                        } else {
                            event_name.append_null();
                            event_time_since_start.append_null();
                        }
                        if let Some(link) = link {
                            link_trace_id
                                .append_value(link.trace_id)
                                .map_err(|e| TraceqlError::Store(e.to_string()))?;
                            link_span_id
                                .append_value(link.span_id)
                                .map_err(|e| TraceqlError::Store(e.to_string()))?;
                        } else {
                            link_trace_id.append_null();
                            link_span_id.append_null();
                        }

                        for (key, builder) in &mut attr_builders {
                            let value = nested_attr_value(key, span, event, *link);
                            builder.append(value);
                        }
                    }
                }
            }
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(trace_id.finish()),
            Arc::new(span_id.finish()),
            Arc::new(parent_span_id.finish()),
            Arc::new(ns_left.finish()),
            Arc::new(ns_right.finish()),
            Arc::new(parent_id.finish()),
            Arc::new(child_count.finish()),
            Arc::new(root_service.finish()),
            Arc::new(root_span.finish()),
            Arc::new(trace_start.finish()),
            Arc::new(trace_duration.finish()),
            Arc::new(name.finish()),
            Arc::new(kind.finish()),
            Arc::new(start.finish()),
            Arc::new(duration.finish()),
            Arc::new(status_code.finish()),
            Arc::new(status_message.finish()),
            Arc::new(instrumentation_name.finish()),
            Arc::new(instrumentation_version.finish()),
            Arc::new(event_name.finish()),
            Arc::new(event_time_since_start.finish()),
            Arc::new(link_trace_id.finish()),
            Arc::new(link_span_id.finish()),
        ];
        columns.extend(attr_builders.into_iter().map(|(_, b)| b.finish()));

        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| TraceqlError::Store(e.to_string()))?;
        let inspected_bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("spans", Arc::new(table))?;
        Ok(ScanResult {
            ctx,
            span_table: "spans".into(),
            inspected_bytes,
        })
    }
}

#[async_trait::async_trait]
impl SpanStore for InMemorySpanStore {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult> {
        self.scan_with_projection(tenant, matchers, &[], start_ns, end_ns)
    }

    async fn scan_with_options(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
        options: &crate::store::ScanOptions,
    ) -> Result<ScanResult> {
        self.scan_with_projection(
            tenant,
            matchers,
            &options.projection_matchers,
            start_ns,
            end_ns,
        )
    }

    async fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>> {
        let found = self
            .traces
            .get(tenant)
            .into_iter()
            .flatten()
            .find(|trace| &trace.trace_id == trace_id);
        Ok(found.map(|trace| TraceSpans {
            trace_id: trace.trace_id,
            root_service_name: trace.root_service_name.clone(),
            root_trace_name: trace.root_span_name.clone(),
            resource_attributes: if trace.root_service_name.is_empty() {
                Vec::new()
            } else {
                vec![(
                    "service.name".to_string(),
                    AttrValue::Str(trace.root_service_name.clone()),
                )]
            },
            spans: trace
                .spans
                .iter()
                .zip(&trace.nested)
                .map(|(span, nested)| span_ref(span, nested))
                .collect(),
        }))
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        let mut resource = BTreeSet::new();
        let mut span = BTreeSet::new();
        let mut event = BTreeSet::new();
        let mut link = BTreeSet::new();
        let mut instrumentation = BTreeSet::new();
        let traces = self.traces_in_range(tenant, start_ns, end_ns);
        for trace in &traces {
            resource.insert("service.name".to_string());
            for input in &trace.spans {
                span.extend(input.attrs.iter().map(|(key, _)| key.clone()));
                if !input.events.is_empty() {
                    event.extend(EVENT_TAGS.iter().map(|tag| (*tag).to_string()));
                }
                if !input.links.is_empty() {
                    link.extend(LINK_TAGS.iter().map(|tag| (*tag).to_string()));
                }
                for event_ref in &input.events {
                    event.extend(event_ref.attributes.iter().map(|(key, _)| key.clone()));
                }
                for link_ref in &input.links {
                    link.extend(link_ref.attributes.iter().map(|(key, _)| key.clone()));
                }
                if !input.instrumentation_name.is_empty() {
                    instrumentation.insert("instrumentation:name".to_string());
                }
                if !input.instrumentation_version.is_empty() {
                    instrumentation.insert("instrumentation:version".to_string());
                }
            }
        }

        let mut out = Vec::new();
        if matches!(scope, None | Some(TagScope::Resource)) && !resource.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Resource,
                tags: resource.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Span)) && !span.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Span,
                tags: span.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Intrinsic)) && !traces.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Intrinsic,
                tags: INTRINSIC_TAGS
                    .iter()
                    .map(|tag| (*tag).to_string())
                    .collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Event)) && !event.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Event,
                tags: event.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Link)) && !link.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Link,
                tags: link.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Instrumentation)) && !instrumentation.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Instrumentation,
                tags: instrumentation.into_iter().collect(),
            });
        }
        Ok(out)
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        let tag = tag.strip_prefix('.').unwrap_or(tag);
        let (attr_tag, attr_scope) = scoped_attribute_tag(tag);
        let mut values = BTreeSet::new();
        for trace in self.traces_in_range(tenant, start_ns, end_ns) {
            collect_trace_intrinsic_values(trace, tag, &mut values);
            if matches!(attr_scope, None | Some(TagScope::Resource)) && attr_tag == "service.name" {
                values.insert(("string".to_string(), trace.root_service_name.clone()));
            }
            for (idx, input) in trace.spans.iter().enumerate() {
                collect_span_intrinsic_values(input, &trace.nested, idx, tag, &mut values);
                collect_event_values(input, tag, &mut values);
                collect_link_values(input, tag, &mut values);
                if matches!(attr_scope, None | Some(TagScope::Span)) {
                    values.extend(
                        input
                            .attrs
                            .iter()
                            .filter(|(key, _)| key == attr_tag)
                            .map(|(_, value)| typed_value_parts(value)),
                    );
                }
            }
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| TypedValue { type_, value })
            .collect())
    }
}

fn scoped_attribute_tag(tag: &str) -> (&str, Option<TagScope>) {
    if let Some(tag) = tag.strip_prefix("resource.") {
        (tag, Some(TagScope::Resource))
    } else if let Some(tag) = tag.strip_prefix("span.") {
        (tag, Some(TagScope::Span))
    } else {
        (tag, None)
    }
}

fn span_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matchers: &[SpanMatcher],
) -> bool {
    if !nested_event_matchers_match(span, matchers) || !nested_link_matchers_match(span, matchers) {
        return false;
    }
    matchers
        .iter()
        .filter(|matcher| !is_event_matcher(matcher) && !is_link_matcher(matcher))
        .all(|matcher| matcher_matches(trace, span, nested_sets, idx, matcher))
}

fn attr_data_type(value: &AttrValue) -> DataType {
    match value {
        AttrValue::Str(_) => DataType::Utf8,
        AttrValue::Int(_) => DataType::Int64,
        AttrValue::Float(_) => DataType::Float64,
        AttrValue::Bool(_) => DataType::Boolean,
    }
}

fn nested_attr_value<'a>(
    key: &str,
    span: &'a InputSpan,
    event: Option<&'a EventRef>,
    link: Option<&'a LinkRef>,
) -> Option<&'a AttrValue> {
    if let Some(key) = key.strip_prefix(EVENT_ATTR_PREFIX) {
        return event.and_then(|event| {
            event
                .attributes
                .iter()
                .find(|(attr_key, _)| attr_key == key)
                .map(|(_, value)| value)
        });
    }
    if let Some(key) = key.strip_prefix(LINK_ATTR_PREFIX) {
        return link.and_then(|link| {
            link.attributes
                .iter()
                .find(|(attr_key, _)| attr_key == key)
                .map(|(_, value)| value)
        });
    }
    span.attrs
        .iter()
        .find(|(attr_key, _)| attr_key == key)
        .map(|(_, value)| value)
}

fn expansion_matchers(
    matchers: &[SpanMatcher],
    projection_matchers: &[SpanMatcher],
) -> Vec<SpanMatcher> {
    let mut out = Vec::with_capacity(matchers.len() + projection_matchers.len());
    out.extend_from_slice(matchers);
    out.extend_from_slice(projection_matchers);
    out
}

fn matching_events_for_scan<'a>(
    span: &'a InputSpan,
    matchers: &[SpanMatcher],
) -> Vec<Option<&'a EventRef>> {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    if event_matchers.is_empty() {
        return vec![span.events.first()];
    }
    if span.events.is_empty() {
        return vec![None];
    }
    span.events
        .iter()
        .filter(|event| {
            event_matchers
                .iter()
                .all(|matcher| event_matcher_matches_event(event, matcher))
        })
        .map(Some)
        .collect()
}

fn matching_links_for_scan<'a>(
    span: &'a InputSpan,
    matchers: &[SpanMatcher],
) -> Vec<Option<&'a LinkRef>> {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    if link_matchers.is_empty() {
        return vec![span.links.first()];
    }
    if span.links.is_empty() {
        return vec![None];
    }
    span.links
        .iter()
        .filter(|link| {
            link_matchers
                .iter()
                .all(|matcher| link_matcher_matches_link(link, matcher))
        })
        .map(Some)
        .collect()
}

fn nested_event_matchers_match(span: &InputSpan, matchers: &[SpanMatcher]) -> bool {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    if event_matchers.is_empty() {
        return true;
    }
    if span.events.is_empty() {
        return event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_absence(matcher));
    }
    span.events.iter().any(|event| {
        event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_event(event, matcher))
    })
}

fn nested_link_matchers_match(span: &InputSpan, matchers: &[SpanMatcher]) -> bool {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    if link_matchers.is_empty() {
        return true;
    }
    if span.links.is_empty() {
        return link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_absence(matcher));
    }
    span.links.iter().any(|link| {
        link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_link(link, matcher))
    })
}

fn is_event_matcher(matcher: &SpanMatcher) -> bool {
    matcher.scope == MatchScope::Event
        || (matcher.scope == MatchScope::Intrinsic && matcher.key.starts_with("event:"))
}

fn is_link_matcher(matcher: &SpanMatcher) -> bool {
    matcher.scope == MatchScope::Link
        || (matcher.scope == MatchScope::Intrinsic && matcher.key.starts_with("link:"))
}

fn event_matcher_matches_event(event: &EventRef, matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => {
            let values = event
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "event:name" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| string_matches(&event.name, matcher.op, &matcher.value)),
            "event:timeSinceStart" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    int_matches(
                        i64::try_from(event.time_since_start_nano).unwrap_or(i64::MAX),
                        matcher.op,
                        &matcher.value,
                    )
                }),
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

fn event_matcher_matches_absence(matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => nil_matches(matcher.op, &matcher.value),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "event:name" | "event:timeSinceStart" => {
                nested_presence_matches(false, matcher.op, &matcher.value).unwrap_or(false)
            }
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

fn link_matcher_matches_link(link: &LinkRef, matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Link => {
            let values = link
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "link:traceID" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    string_matches(&bytes_to_hex(&link.trace_id), matcher.op, &matcher.value)
                }),
            "link:spanID" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    string_matches(&bytes_to_hex(&link.span_id), matcher.op, &matcher.value)
                }),
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

fn link_matcher_matches_absence(matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Link => nil_matches(matcher.op, &matcher.value),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "link:traceID" | "link:spanID" => {
                nested_presence_matches(false, matcher.op, &matcher.value).unwrap_or(false)
            }
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

fn matcher_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matcher: &SpanMatcher,
) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => span.events.iter().any(|event| {
            let values = event
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }),
        MatchScope::Link => span.links.iter().any(|link| {
            let values = link
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }),
        MatchScope::Intrinsic => intrinsic_matches(trace, span, nested_sets, idx, matcher),
        MatchScope::Resource => resource_matches(trace, matcher),
        MatchScope::Instrumentation => instrumentation_matches(span, matcher),
        MatchScope::Both => {
            resource_matches(trace, matcher)
                || span_attr_matches(span, &matcher.key, matcher.op, &matcher.value)
        }
        MatchScope::Span => span_attr_matches(span, &matcher.key, matcher.op, &matcher.value),
        MatchScope::Parent => true,
    };
    is_match != matcher.negated
}

fn span_attr_matches(span: &InputSpan, key: &str, op: MatchCmp, expected: &MatchValue) -> bool {
    let values = span
        .attrs
        .iter()
        .filter(|(attr_key, _)| attr_key == key)
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    attr_values_match(&values, op, expected)
}

fn resource_matches(trace: &StoredTrace, matcher: &SpanMatcher) -> bool {
    match matcher.key.as_str() {
        "service.name" => string_matches(&trace.root_service_name, matcher.op, &matcher.value),
        _ => nil_matches(matcher.op, &matcher.value),
    }
}

fn instrumentation_matches(span: &InputSpan, matcher: &SpanMatcher) -> bool {
    match matcher.key.as_str() {
        "name" | "instrumentation:name" => {
            string_matches(&span.instrumentation_name, matcher.op, &matcher.value)
        }
        "version" | "instrumentation:version" => {
            string_matches(&span.instrumentation_version, matcher.op, &matcher.value)
        }
        _ => nil_matches(matcher.op, &matcher.value),
    }
}

fn intrinsic_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matcher: &SpanMatcher,
) -> bool {
    match matcher.key.as_str() {
        "name" | "span:name" => string_matches(&span.name, matcher.op, &matcher.value),
        "event:name" => {
            nested_presence_matches(!span.events.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.events
                        .iter()
                        .any(|event| string_matches(&event.name, matcher.op, &matcher.value))
                })
        }
        "event:timeSinceStart" => {
            nested_presence_matches(!span.events.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.events.iter().any(|event| {
                        int_matches(
                            i64::try_from(event.time_since_start_nano).unwrap_or(i64::MAX),
                            matcher.op,
                            &matcher.value,
                        )
                    })
                })
        }
        "link:traceID" => {
            nested_presence_matches(!span.links.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.trace_id), matcher.op, &matcher.value)
                    })
                })
        }
        "link:spanID" => {
            nested_presence_matches(!span.links.is_empty(), matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    span.links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.span_id), matcher.op, &matcher.value)
                    })
                })
        }
        "trace:id" => string_matches(&bytes_to_hex(&trace.trace_id), matcher.op, &matcher.value),
        "trace:rootService" => string_matches(&trace.root_service_name, matcher.op, &matcher.value),
        "trace:rootName" => string_matches(&trace.root_span_name, matcher.op, &matcher.value),
        "trace:duration" => int_matches(trace.trace_duration_nanos, matcher.op, &matcher.value),
        "duration" | "span:duration" => {
            int_matches(span.duration_nanos, matcher.op, &matcher.value)
        }
        "span:id" => string_matches(&bytes_to_hex(&span.span_id), matcher.op, &matcher.value),
        "span:parentID" => span.parent_span_id.map_or_else(
            || nil_matches(matcher.op, &matcher.value),
            |parent| string_matches(&bytes_to_hex(&parent), matcher.op, &matcher.value),
        ),
        "kind" | "span:kind" => enum_int_matches(
            i64::from(span.kind),
            matcher.op,
            &matcher.value,
            kind_enum_value,
        ),
        "status" | "span:status" => enum_int_matches(
            i64::from(span.status_code),
            matcher.op,
            &matcher.value,
            status_enum_value,
        ),
        "statusMessage" | "span:statusMessage" => {
            string_matches(&span.status_message, matcher.op, &matcher.value)
        }
        "span:childCount" => int_matches(
            i64::from(child_count_for(nested_sets, idx)),
            matcher.op,
            &matcher.value,
        ),
        "span:nestedSetLeft" => nested_sets
            .get(idx)
            .is_some_and(|nested| int_matches(i64::from(nested.left), matcher.op, &matcher.value)),
        "span:nestedSetRight" => nested_sets
            .get(idx)
            .is_some_and(|nested| int_matches(i64::from(nested.right), matcher.op, &matcher.value)),
        "span:nestedSetParent" => nested_sets.get(idx).is_some_and(|nested| {
            int_matches(i64::from(nested.parent_id), matcher.op, &matcher.value)
        }),
        "instrumentation:name" => {
            string_matches(&span.instrumentation_name, matcher.op, &matcher.value)
        }
        "instrumentation:version" => {
            string_matches(&span.instrumentation_version, matcher.op, &matcher.value)
        }
        _ => true,
    }
}

fn attr_matches(value: &AttrValue, op: MatchCmp, expected: &MatchValue) -> bool {
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    match value {
        AttrValue::Str(value) => string_matches(value, op, expected),
        AttrValue::Int(value) => int_matches(*value, op, expected),
        AttrValue::Float(value) => float_matches(*value, op, expected),
        AttrValue::Bool(value) => bool_matches(*value, op, expected),
    }
}

fn attr_values_match(values: &[&AttrValue], op: MatchCmp, expected: &MatchValue) -> bool {
    if values.is_empty() {
        return nil_matches(op, expected);
    }
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    match op {
        MatchCmp::Neq | MatchCmp::Nre => {
            values.iter().all(|value| attr_matches(value, op, expected))
        }
        MatchCmp::Eq
        | MatchCmp::Re
        | MatchCmp::Lt
        | MatchCmp::Lte
        | MatchCmp::Gt
        | MatchCmp::Gte => values.iter().any(|value| attr_matches(value, op, expected)),
    }
}

fn nested_presence_matches(has_values: bool, op: MatchCmp, expected: &MatchValue) -> Option<bool> {
    match (op, expected) {
        (MatchCmp::Eq, MatchValue::Nil) => Some(!has_values),
        (MatchCmp::Neq, MatchValue::Nil) => Some(has_values),
        _ => None,
    }
}

fn present_value_matches(op: MatchCmp, expected: &MatchValue) -> Option<bool> {
    match (op, expected) {
        (MatchCmp::Eq, MatchValue::Nil) => Some(false),
        (MatchCmp::Neq, MatchValue::Nil) => Some(true),
        _ => None,
    }
}

fn nil_matches(op: MatchCmp, expected: &MatchValue) -> bool {
    matches!((op, expected), (MatchCmp::Eq, MatchValue::Nil))
}

fn string_matches(value: &str, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Str(expected) = expected else {
        return false;
    };
    match op {
        MatchCmp::Eq => value == expected,
        MatchCmp::Neq => value != expected,
        MatchCmp::Re => {
            regex::Regex::new(&format!("^(?:{expected})$")).is_ok_and(|re| re.is_match(value))
        }
        MatchCmp::Nre => {
            regex::Regex::new(&format!("^(?:{expected})$")).is_ok_and(|re| !re.is_match(value))
        }
        MatchCmp::Lt | MatchCmp::Lte | MatchCmp::Gt | MatchCmp::Gte => false,
    }
}

fn int_matches(value: i64, op: MatchCmp, expected: &MatchValue) -> bool {
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    let expected = match expected {
        MatchValue::Int(value) => *value,
        _ => return false,
    };
    match op {
        MatchCmp::Eq => value == expected,
        MatchCmp::Neq => value != expected,
        MatchCmp::Lt => value < expected,
        MatchCmp::Lte => value <= expected,
        MatchCmp::Gt => value > expected,
        MatchCmp::Gte => value >= expected,
        MatchCmp::Re | MatchCmp::Nre => false,
    }
}

fn enum_int_matches(
    value: i64,
    op: MatchCmp,
    expected: &MatchValue,
    enum_value: fn(&str) -> Option<i32>,
) -> bool {
    let expected = match expected {
        MatchValue::Str(name) => enum_value(&name.to_ascii_lowercase()).map(i64::from),
        MatchValue::Int(value) => Some(*value),
        MatchValue::Nil => return present_value_matches(op, expected).unwrap_or(false),
        MatchValue::Float(_) | MatchValue::Bool(_) => None,
    };
    expected.is_some_and(|expected| int_matches(value, op, &MatchValue::Int(expected)))
}

fn status_enum_value(name: &str) -> Option<i32> {
    match name {
        "unset" => Some(0),
        "ok" => Some(1),
        "error" => Some(2),
        _ => None,
    }
}

fn kind_enum_value(name: &str) -> Option<i32> {
    match name {
        "unspecified" => Some(0),
        "internal" => Some(1),
        "server" => Some(2),
        "client" => Some(3),
        "producer" => Some(4),
        "consumer" => Some(5),
        _ => None,
    }
}

#[allow(clippy::float_cmp)]
fn float_matches(value: f64, op: MatchCmp, expected: &MatchValue) -> bool {
    let expected = match expected {
        MatchValue::Float(value) => *value,
        _ => return false,
    };
    match op {
        MatchCmp::Eq => value == expected,
        MatchCmp::Neq => value != expected,
        MatchCmp::Lt => value < expected,
        MatchCmp::Lte => value <= expected,
        MatchCmp::Gt => value > expected,
        MatchCmp::Gte => value >= expected,
        MatchCmp::Re | MatchCmp::Nre => false,
    }
}

fn bool_matches(value: bool, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Bool(expected) = expected else {
        return false;
    };
    match op {
        MatchCmp::Eq => value == *expected,
        MatchCmp::Neq => value != *expected,
        MatchCmp::Lt
        | MatchCmp::Lte
        | MatchCmp::Gt
        | MatchCmp::Gte
        | MatchCmp::Re
        | MatchCmp::Nre => false,
    }
}

impl InMemorySpanStore {
    fn traces_in_range(&self, tenant: &str, start_ns: i64, end_ns: i64) -> Vec<&StoredTrace> {
        self.traces
            .get(tenant)
            .into_iter()
            .flatten()
            .filter(|trace| {
                start_ns <= trace.trace_start_unix_nano && trace.trace_start_unix_nano <= end_ns
            })
            .collect()
    }
}

fn collect_trace_intrinsic_values(
    trace: &StoredTrace,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    match tag {
        "trace:duration" => {
            values.insert((
                "duration".to_string(),
                trace.trace_duration_nanos.to_string(),
            ));
        }
        "trace:id" => {
            values.insert(("string".to_string(), bytes_to_hex(&trace.trace_id)));
        }
        "trace:rootName" => {
            values.insert(("string".to_string(), trace.root_span_name.clone()));
        }
        "trace:rootService" => {
            values.insert(("string".to_string(), trace.root_service_name.clone()));
        }
        _ => {}
    }
}

fn collect_span_intrinsic_values(
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    let nested = nested_sets.get(idx);
    match tag {
        "span:childCount" => {
            if let Some(nested) = nested {
                let count = nested_sets
                    .iter()
                    .filter(|other| other.parent_id == nested.left)
                    .count();
                values.insert(("int".to_string(), count.to_string()));
            }
        }
        "span:duration" => {
            values.insert(("duration".to_string(), span.duration_nanos.to_string()));
        }
        "span:id" => {
            values.insert(("string".to_string(), bytes_to_hex(&span.span_id)));
        }
        "span:kind" => {
            values.insert(("int".to_string(), span.kind.to_string()));
        }
        "span:name" => {
            values.insert(("string".to_string(), span.name.clone()));
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(("string".to_string(), bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            values.insert(("int".to_string(), span.status_code.to_string()));
        }
        "span:statusMessage" => {
            if !span.status_message.is_empty() {
                values.insert(("string".to_string(), span.status_message.clone()));
            }
        }
        "span:nestedSetLeft" => {
            if let Some(nested) = nested {
                values.insert(("int".to_string(), nested.left.to_string()));
            }
        }
        "span:nestedSetParent" => {
            if let Some(nested) = nested {
                values.insert(("int".to_string(), nested.parent_id.to_string()));
            }
        }
        "span:nestedSetRight" => {
            if let Some(nested) = nested {
                values.insert(("int".to_string(), nested.right.to_string()));
            }
        }
        "instrumentation:name" if !span.instrumentation_name.is_empty() => {
            values.insert(("string".to_string(), span.instrumentation_name.clone()));
        }
        "instrumentation:version" if !span.instrumentation_version.is_empty() => {
            values.insert(("string".to_string(), span.instrumentation_version.clone()));
        }
        _ => {}
    }
}

fn collect_event_values(span: &InputSpan, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for event in &span.events {
        match tag {
            "event:name" => {
                values.insert(("string".to_string(), event.name.clone()));
            }
            "event:timeSinceStart" => {
                values.insert((
                    "duration".to_string(),
                    event.time_since_start_nano.to_string(),
                ));
            }
            _ => {}
        }
        values.extend(
            event
                .attributes
                .iter()
                .filter(|(key, _)| nested_attribute_key_matches(key, tag, "event."))
                .map(|(_, value)| typed_value_parts(value)),
        );
    }
}

fn collect_link_values(span: &InputSpan, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for link in &span.links {
        match tag {
            "link:traceID" => {
                values.insert(("string".to_string(), bytes_to_hex(&link.trace_id)));
            }
            "link:spanID" => {
                values.insert(("string".to_string(), bytes_to_hex(&link.span_id)));
            }
            _ => {}
        }
        values.extend(
            link.attributes
                .iter()
                .filter(|(key, _)| nested_attribute_key_matches(key, tag, "link."))
                .map(|(_, value)| typed_value_parts(value)),
        );
    }
}

fn nested_attribute_key_matches(key: &str, tag: &str, scope_prefix: &str) -> bool {
    key == tag || tag.strip_prefix(scope_prefix).is_some_and(|tag| key == tag)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn typed_value_parts(value: &AttrValue) -> (String, String) {
    match value {
        AttrValue::Str(v) => ("string".to_string(), v.clone()),
        AttrValue::Int(v) => ("int".to_string(), v.to_string()),
        AttrValue::Float(v) => ("float".to_string(), v.to_string()),
        AttrValue::Bool(v) => ("bool".to_string(), v.to_string()),
    }
}

fn child_count_for(nested_sets: &[NestedSet], idx: usize) -> i32 {
    let Some(nested) = nested_sets.get(idx) else {
        return 0;
    };
    i32::try_from(
        nested_sets
            .iter()
            .filter(|other| other.parent_id == nested.left)
            .count(),
    )
    .unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use datafusion::arrow::array::AsArray;

    use crate::result::{AttrValue, EventRef, LinkRef};
    use crate::span_columns::{COL_NS_LEFT, COL_PARENT_ID, InputSpan};

    fn span(id: u8, parent: Option<u8>, name: &str, attrs: Vec<(&str, AttrValue)>) -> InputSpan {
        InputSpan {
            trace_id: [7; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: name.into(),
            kind: 0,
            start_unix_nano: 1000,
            duration_nanos: 5,
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    #[tokio::test]
    async fn scan_registers_span_table_with_nested_set_columns() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "checkout",
            "POST /pay",
            vec![
                span(1, None, "root", vec![]),
                span(
                    2,
                    Some(1),
                    "db",
                    vec![("http.method", AttrValue::Str("GET".into()))],
                ),
            ],
        );
        let r = s.scan("t", &[], 0, 5000).await.unwrap();
        let table = &r.span_table;
        let df = r
            .ctx
            .sql(&format!("SELECT count(*) AS c FROM {table}"))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let c = out[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(0);
        assert!(c == 2);

        let df = r
            .ctx
            .sql(&format!(
                "SELECT {COL_PARENT_ID} FROM {table} ORDER BY {COL_NS_LEFT}"
            ))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let pid = out[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        assert!(pid.value(0) == -1); // root: Tempo nestedSetParent sentinel
        assert!(pid.value(1) == 1);
    }

    #[tokio::test]
    async fn trace_by_id_returns_stored_spans() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "svc", "op", vec![span(1, None, "root", vec![])]);
        let got = s.trace_by_id("t", &[7; 16]).await.unwrap().unwrap();
        assert!(
            got == TraceSpans {
                trace_id: [7; 16],
                root_service_name: "svc".into(),
                root_trace_name: "op".into(),
                resource_attributes: vec![("service.name".into(), AttrValue::Str("svc".into()))],
                spans: vec![SpanRef {
                    span_id: [1; 8],
                    parent_span_id: None,
                    name: "root".into(),
                    kind: 0,
                    nested_set_left: 1,
                    nested_set_right: 2,
                    nested_set_parent: -1,
                    start_time_unix_nano: 1000,
                    duration_nanos: 5,
                    status_code: 0,
                    status_message: String::new(),
                    instrumentation_name: String::new(),
                    instrumentation_version: String::new(),
                    resource_attributes: Vec::new(),
                    attributes: Vec::new(),
                    events: Vec::new(),
                    links: Vec::new(),
                }],
            }
        );
        assert!(s.trace_by_id("t", &[9; 16]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tag_names_return_default_scopes() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "svc",
            "op",
            vec![span(
                1,
                None,
                "root",
                vec![("svc", AttrValue::Str("a".into()))],
            )],
        );

        let got = s.tag_names("t", None, 0, 10_000).await.unwrap();
        assert!(
            got == vec![
                ScopedTag {
                    scope: TagScope::Resource,
                    tags: vec!["service.name".into()],
                },
                ScopedTag {
                    scope: TagScope::Span,
                    tags: vec!["svc".into()],
                },
                ScopedTag {
                    scope: TagScope::Intrinsic,
                    tags: INTRINSIC_TAGS
                        .iter()
                        .map(|tag| (*tag).to_string())
                        .collect(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn tag_names_return_instrumentation_scope() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "svc",
            "op",
            vec![InputSpan {
                instrumentation_name: "tracer".into(),
                instrumentation_version: "1.2.3".into(),
                ..span(1, None, "root", vec![("svc", AttrValue::Str("a".into()))])
            }],
        );

        let got = s
            .tag_names("t", Some(TagScope::Instrumentation), 0, 10_000)
            .await
            .unwrap();
        assert!(
            got == vec![ScopedTag {
                scope: TagScope::Instrumentation,
                tags: vec![
                    "instrumentation:name".into(),
                    "instrumentation:version".into()
                ],
            }]
        );
    }

    #[tokio::test]
    async fn tag_names_and_values_return_event_and_link_metadata() {
        let mut input = span(1, None, "root", vec![]);
        input.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "exception".into(),
            attributes: vec![("cache.key".into(), AttrValue::Str("users".into()))],
        }];
        input.links = vec![LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            attributes: vec![("link.kind".into(), AttrValue::Str("retry".into()))],
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "svc", "op", vec![input]);

        let event_names = s
            .tag_names("t", Some(TagScope::Event), 0, 10_000)
            .await
            .unwrap();
        assert!(
            event_names
                == vec![ScopedTag {
                    scope: TagScope::Event,
                    tags: vec![
                        "cache.key".into(),
                        "event:name".into(),
                        "event:timeSinceStart".into()
                    ],
                }]
        );

        let link_names = s
            .tag_names("t", Some(TagScope::Link), 0, 10_000)
            .await
            .unwrap();
        assert!(
            link_names
                == vec![ScopedTag {
                    scope: TagScope::Link,
                    tags: vec![
                        "link.kind".into(),
                        "link:spanID".into(),
                        "link:traceID".into()
                    ],
                }]
        );

        let cases = [
            ("event:name", "string", "exception"),
            ("event:timeSinceStart", "duration", "50"),
            ("cache.key", "string", "users"),
            ("event.cache.key", "string", "users"),
            ("link:traceID", "string", "09090909090909090909090909090909"),
            ("link:spanID", "string", "0808080808080808"),
            ("link.kind", "string", "retry"),
            ("link.link.kind", "string", "retry"),
        ];
        for (tag, type_, value) in cases {
            let got = s.tag_values("t", tag, 0, 10_000).await.unwrap();
            assert!(
                got == vec![TypedValue {
                    type_: type_.into(),
                    value: value.into(),
                }],
                "tag {tag}"
            );
        }
    }

    #[tokio::test]
    async fn tag_names_return_intrinsic_scope() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "svc", "op", vec![span(1, None, "root", vec![])]);

        let got = s
            .tag_names("t", Some(TagScope::Intrinsic), 0, 10_000)
            .await
            .unwrap();
        assert!(
            got == vec![ScopedTag {
                scope: TagScope::Intrinsic,
                tags: vec![
                    "span:childCount".into(),
                    "span:duration".into(),
                    "span:id".into(),
                    "span:kind".into(),
                    "span:name".into(),
                    "span:parentID".into(),
                    "span:nestedSetLeft".into(),
                    "span:nestedSetParent".into(),
                    "span:nestedSetRight".into(),
                    "span:status".into(),
                    "span:statusMessage".into(),
                    "trace:duration".into(),
                    "trace:id".into(),
                    "trace:rootName".into(),
                    "trace:rootService".into(),
                ],
            }]
        );
    }

    #[tokio::test]
    async fn tag_values_return_typed_unique_values() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "svc",
            "op",
            vec![
                span(1, None, "root", vec![("svc", AttrValue::Str("a".into()))]),
                InputSpan {
                    instrumentation_name: "tracer".into(),
                    ..span(
                        2,
                        Some(1),
                        "child",
                        vec![("svc", AttrValue::Str("a".into()))],
                    )
                },
                span(
                    3,
                    Some(1),
                    "child",
                    vec![("svc", AttrValue::Str("b".into()))],
                ),
            ],
        );

        let resource = s.tag_values("t", "service.name", 0, 10_000).await.unwrap();
        assert!(
            resource
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "svc".into(),
                }]
        );

        let span = s.tag_values("t", ".svc", 0, 10_000).await.unwrap();
        assert!(
            span == vec![
                TypedValue {
                    type_: "string".into(),
                    value: "a".into(),
                },
                TypedValue {
                    type_: "string".into(),
                    value: "b".into(),
                },
            ]
        );

        let instrumentation = s
            .tag_values("t", "instrumentation:name", 0, 10_000)
            .await
            .unwrap();
        assert!(
            instrumentation
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "tracer".into(),
                }]
        );

        let child_count = s
            .tag_values("t", "span:childCount", 0, 10_000)
            .await
            .unwrap();
        assert!(
            child_count
                == vec![
                    TypedValue {
                        type_: "int".into(),
                        value: "0".into(),
                    },
                    TypedValue {
                        type_: "int".into(),
                        value: "2".into(),
                    },
                ]
        );
    }

    // ---------------------------------------------------------------------
    // Helpers for scan/matcher-driven tests.
    // ---------------------------------------------------------------------

    use crate::store::{MatchCmp, MatchScope, MatchValue, ScanOptions, SpanMatcher};

    fn matcher(scope: MatchScope, key: &str, op: MatchCmp, value: MatchValue) -> SpanMatcher {
        SpanMatcher {
            scope,
            key: key.into(),
            op,
            value,
            negated: false,
        }
    }

    fn rich_span() -> InputSpan {
        InputSpan {
            trace_id: [7; 16],
            span_id: [1; 8],
            parent_span_id: Some([2; 8]),
            name: "checkout".into(),
            kind: 2,
            start_unix_nano: 1000,
            duration_nanos: 5_000_000,
            status_code: 2,
            status_message: "boom".into(),
            instrumentation_name: "tracer".into(),
            instrumentation_version: "1.2.3".into(),
            attrs: vec![
                ("http.method".into(), AttrValue::Str("GET".into())),
                ("http.status".into(), AttrValue::Int(500)),
                ("ratio".into(), AttrValue::Float(0.5)),
                ("ok".into(), AttrValue::Bool(true)),
            ],
            events: vec![EventRef {
                time_since_start_nano: 50,
                name: "exception".into(),
                attributes: vec![("ev.attr".into(), AttrValue::Str("kaboom".into()))],
            }],
            links: vec![LinkRef {
                trace_id: [9; 16],
                span_id: [8; 8],
                attributes: vec![("ln.attr".into(), AttrValue::Int(42))],
            }],
        }
    }

    async fn row_count(r: &ScanResult) -> i64 {
        let df = r
            .ctx
            .sql(&format!("SELECT count(*) AS c FROM {}", r.span_table))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        out[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(0)
    }

    // ---- scan with projection: event/link attr columns + AttrBuilder ----

    #[tokio::test]
    async fn scan_projects_typed_attr_columns_and_nested_attrs() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "POST /pay", vec![rich_span()]);

        // Presence-style projection matchers (`!= nil`) request the event/link
        // attribute columns without filtering out the rows that carry them.
        let projection = vec![
            matcher(MatchScope::Event, "ev.attr", MatchCmp::Neq, MatchValue::Nil),
            matcher(MatchScope::Link, "ln.attr", MatchCmp::Neq, MatchValue::Nil),
        ];
        let options = ScanOptions {
            job: None,
            projection_matchers: projection,
        };
        let r = s
            .scan_with_options("t", &[], 0, 10_000, &options)
            .await
            .unwrap();
        assert!(row_count(&r).await == 1);

        // The typed span attributes (Int/Float/Bool) and the projected event &
        // link attribute columns are present and carry the right values.
        let df = r
            .ctx
            .sql(&format!(
                "SELECT \"attr.http.status\" AS hs, \"attr.ratio\" AS ra, \"attr.ok\" AS ok, \
                 \"attr.__event.ev.attr\" AS ev, \"attr.__link.ln.attr\" AS ln FROM {}",
                r.span_table
            ))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let batch = &out[0];
        assert!(
            batch
                .column(0)
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .value(0)
                == 500
        );
        #[allow(clippy::float_cmp)]
        let ratio_ok = batch
            .column(1)
            .as_primitive::<datafusion::arrow::datatypes::Float64Type>()
            .value(0)
            == 0.5;
        assert!(ratio_ok);
        assert!(batch.column(2).as_boolean().value(0));
        assert!(batch.column(3).as_string::<i32>().value(0) == "kaboom");
        assert!(
            batch
                .column(4)
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
                .value(0)
                == 42
        );
    }

    // ---- span matchers across scopes (matcher_matches / intrinsic_matches) ----

    async fn scan_matches(matchers: &[SpanMatcher]) -> i64 {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "POST /pay", vec![rich_span()]);
        let r = s.scan("t", matchers, 0, 10_000).await.unwrap();
        row_count(&r).await
    }

    #[tokio::test]
    async fn intrinsic_matchers_match_each_intrinsic() {
        // trace:id and span:id are hex strings. Nested-set left/right are
        // >= 0; a root span's nestedSetParent is -1 (Tempo's no-parent
        // sentinel), so it matches < 0 — the same predicate Grafana's Traces
        // Drilldown uses to find roots.
        let cases = [
            // String intrinsics.
            (
                "span:name",
                MatchCmp::Eq,
                MatchValue::Str("checkout".into()),
            ),
            (
                "span:statusMessage",
                MatchCmp::Eq,
                MatchValue::Str("boom".into()),
            ),
            (
                "trace:rootService",
                MatchCmp::Eq,
                MatchValue::Str("checkout".into()),
            ),
            (
                "trace:rootName",
                MatchCmp::Eq,
                MatchValue::Str("POST /pay".into()),
            ),
            (
                "trace:id",
                MatchCmp::Eq,
                MatchValue::Str("07070707070707070707070707070707".into()),
            ),
            (
                "span:id",
                MatchCmp::Eq,
                MatchValue::Str("0101010101010101".into()),
            ),
            // span:parentID present.
            (
                "span:parentID",
                MatchCmp::Eq,
                MatchValue::Str("0202020202020202".into()),
            ),
            // Numeric / duration intrinsics.
            ("span:duration", MatchCmp::Gt, MatchValue::Int(1_000_000)),
            ("trace:duration", MatchCmp::Gte, MatchValue::Int(0)),
            ("span:childCount", MatchCmp::Gte, MatchValue::Int(0)),
            // Nested-set intrinsics.
            ("span:nestedSetLeft", MatchCmp::Gte, MatchValue::Int(0)),
            ("span:nestedSetRight", MatchCmp::Gte, MatchValue::Int(0)),
            ("span:nestedSetParent", MatchCmp::Lt, MatchValue::Int(0)),
            // Instrumentation intrinsics.
            (
                "instrumentation:name",
                MatchCmp::Eq,
                MatchValue::Str("tracer".into()),
            ),
            (
                "instrumentation:version",
                MatchCmp::Eq,
                MatchValue::Str("1.2.3".into()),
            ),
        ];
        for (key, op, value) in cases {
            let desc = format!("{key} {op:?} {value:?}");
            assert!(
                scan_matches(&[matcher(MatchScope::Intrinsic, key, op, value)]).await == 1,
                "intrinsic {desc} should match"
            );
        }
    }

    #[tokio::test]
    async fn enum_intrinsics_match_by_name_and_int() {
        // kind=server (2) and status=error (2) via enum string names.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "span:kind",
                MatchCmp::Eq,
                MatchValue::Str("server".into()),
            )])
            .await
                == 1
        );
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "span:status",
                MatchCmp::Eq,
                MatchValue::Str("error".into()),
            )])
            .await
                == 1
        );
        // Same intrinsics via integer enum values.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "span:kind",
                MatchCmp::Eq,
                MatchValue::Int(2),
            )])
            .await
                == 1
        );
        // An unknown enum name yields no match.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "span:status",
                MatchCmp::Eq,
                MatchValue::Str("nonsense".into()),
            )])
            .await
                == 0
        );
        // A float/bool expected value cannot match an enum intrinsic.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "span:kind",
                MatchCmp::Eq,
                MatchValue::Bool(true),
            )])
            .await
                == 0
        );
    }

    #[tokio::test]
    async fn scope_matchers_cover_resource_instrumentation_span_and_both() {
        // Resource service.name.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Resource,
                "service.name",
                MatchCmp::Eq,
                MatchValue::Str("checkout".into()),
            )])
            .await
                == 1
        );
        // Resource non-service.name key falls back to nil matching (no match
        // for an Eq-with-value).
        assert!(
            scan_matches(&[matcher(
                MatchScope::Resource,
                "other",
                MatchCmp::Eq,
                MatchValue::Str("x".into()),
            )])
            .await
                == 0
        );
        // Instrumentation by bare name/version keys.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Instrumentation,
                "name",
                MatchCmp::Eq,
                MatchValue::Str("tracer".into()),
            )])
            .await
                == 1
        );
        assert!(
            scan_matches(&[matcher(
                MatchScope::Instrumentation,
                "version",
                MatchCmp::Eq,
                MatchValue::Str("1.2.3".into()),
            )])
            .await
                == 1
        );
        // Span attribute, typed Int.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "http.status",
                MatchCmp::Eq,
                MatchValue::Int(500),
            )])
            .await
                == 1
        );
        // Both scope: resource OR span attribute. The span attribute matches.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Both,
                "http.method",
                MatchCmp::Eq,
                MatchValue::Str("GET".into()),
            )])
            .await
                == 1
        );
        // Parent scope always matches (returns true).
        assert!(
            scan_matches(&[matcher(
                MatchScope::Parent,
                "anything",
                MatchCmp::Eq,
                MatchValue::Str("x".into()),
            )])
            .await
                == 1
        );
    }

    #[tokio::test]
    async fn span_attr_matchers_cover_all_value_types() {
        // Float attribute.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "ratio",
                MatchCmp::Lt,
                MatchValue::Float(1.0),
            )])
            .await
                == 1
        );
        // Bool attribute.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "ok",
                MatchCmp::Eq,
                MatchValue::Bool(true),
            )])
            .await
                == 1
        );
        // String regex.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "http.method",
                MatchCmp::Re,
                MatchValue::Str("GE.".into()),
            )])
            .await
                == 1
        );
        // Negated regex (no row matches -> 0).
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "http.method",
                MatchCmp::Nre,
                MatchValue::Str("GE.".into()),
            )])
            .await
                == 0
        );
        // Eq against Nil on a present attribute -> no match.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "http.method",
                MatchCmp::Eq,
                MatchValue::Nil,
            )])
            .await
                == 0
        );
        // Neq against Nil on a present attribute -> matches (present).
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "http.method",
                MatchCmp::Neq,
                MatchValue::Nil,
            )])
            .await
                == 1
        );
        // Eq against Nil on an absent attribute -> matches (absent).
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "missing",
                MatchCmp::Eq,
                MatchValue::Nil,
            )])
            .await
                == 1
        );
    }

    // ---- nested event/link selectors (scan rows) ----

    #[tokio::test]
    async fn event_and_link_scope_matchers_select_rows() {
        // Event-scope attribute matcher selects the matching event row.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Event,
                "ev.attr",
                MatchCmp::Eq,
                MatchValue::Str("kaboom".into()),
            )])
            .await
                == 1
        );
        // Event intrinsic name matcher.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "event:name",
                MatchCmp::Eq,
                MatchValue::Str("exception".into()),
            )])
            .await
                == 1
        );
        // Event intrinsic timeSinceStart matcher.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "event:timeSinceStart",
                MatchCmp::Eq,
                MatchValue::Int(50),
            )])
            .await
                == 1
        );
        // Link-scope attribute matcher.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Link,
                "ln.attr",
                MatchCmp::Eq,
                MatchValue::Int(42),
            )])
            .await
                == 1
        );
        // Link intrinsic traceID / spanID matchers (hex).
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "link:traceID",
                MatchCmp::Eq,
                MatchValue::Str("09090909090909090909090909090909".into()),
            )])
            .await
                == 1
        );
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "link:spanID",
                MatchCmp::Eq,
                MatchValue::Str("0808080808080808".into()),
            )])
            .await
                == 1
        );
    }

    #[tokio::test]
    async fn nested_matchers_against_span_without_events_or_links() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "svc", "op", vec![span(1, None, "root", vec![])]);

        // An event-presence (`event:name != nil`) matcher must fail when the
        // span has no events, exercising the absence path.
        let r = s
            .scan(
                "t",
                &[matcher(
                    MatchScope::Intrinsic,
                    "event:name",
                    MatchCmp::Neq,
                    MatchValue::Nil,
                )],
                0,
                10_000,
            )
            .await
            .unwrap();
        assert!(row_count(&r).await == 0);

        // `event:name = nil` must match a span with no events (absence).
        let r = s
            .scan(
                "t",
                &[matcher(
                    MatchScope::Intrinsic,
                    "event:name",
                    MatchCmp::Eq,
                    MatchValue::Nil,
                )],
                0,
                10_000,
            )
            .await
            .unwrap();
        assert!(row_count(&r).await == 1);

        // Same for links.
        let r = s
            .scan(
                "t",
                &[matcher(
                    MatchScope::Intrinsic,
                    "link:spanID",
                    MatchCmp::Eq,
                    MatchValue::Nil,
                )],
                0,
                10_000,
            )
            .await
            .unwrap();
        assert!(row_count(&r).await == 1);
    }

    #[tokio::test]
    async fn event_matcher_against_present_events_but_no_match_yields_zero() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "POST /pay", vec![rich_span()]);

        // The span has an event named "exception"; requiring name "other"
        // matches no event row.
        let r = s
            .scan(
                "t",
                &[matcher(
                    MatchScope::Intrinsic,
                    "event:name",
                    MatchCmp::Eq,
                    MatchValue::Str("other".into()),
                )],
                0,
                10_000,
            )
            .await
            .unwrap();
        assert!(row_count(&r).await == 0);
    }

    // ---- tag_values: drive every intrinsic/value collector branch ----

    async fn tag_values_for(tag: &str) -> Vec<TypedValue> {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "POST /pay", vec![rich_span()]);
        s.tag_values("t", tag, 0, 10_000).await.unwrap()
    }

    #[tokio::test]
    async fn tag_values_cover_trace_and_span_intrinsics() {
        assert!(
            tag_values_for("trace:duration").await
                == vec![TypedValue {
                    type_: "duration".into(),
                    value: "5000000".into(),
                }]
        );
        assert!(
            tag_values_for("trace:id").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "07070707070707070707070707070707".into(),
                }]
        );
        assert!(
            tag_values_for("trace:rootName").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "POST /pay".into(),
                }]
        );
        assert!(
            tag_values_for("trace:rootService").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "checkout".into(),
                }]
        );
        assert!(
            tag_values_for("span:duration").await
                == vec![TypedValue {
                    type_: "duration".into(),
                    value: "5000000".into(),
                }]
        );
        assert!(
            tag_values_for("span:id").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "0101010101010101".into(),
                }]
        );
        assert!(
            tag_values_for("span:kind").await
                == vec![TypedValue {
                    type_: "int".into(),
                    value: "2".into(),
                }]
        );
        assert!(
            tag_values_for("span:name").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "checkout".into(),
                }]
        );
        assert!(
            tag_values_for("span:parentID").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "0202020202020202".into(),
                }]
        );
        assert!(
            tag_values_for("span:status").await
                == vec![TypedValue {
                    type_: "int".into(),
                    value: "2".into(),
                }]
        );
        assert!(
            tag_values_for("span:statusMessage").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "boom".into(),
                }]
        );
        assert!(
            tag_values_for("instrumentation:name").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "tracer".into(),
                }]
        );
        assert!(
            tag_values_for("instrumentation:version").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "1.2.3".into(),
                }]
        );
    }

    #[tokio::test]
    async fn tag_values_cover_nested_set_intrinsics() {
        assert!(
            tag_values_for("span:nestedSetLeft").await
                == vec![TypedValue {
                    type_: "int".into(),
                    value: "1".into(),
                }]
        );
        assert!(
            tag_values_for("span:nestedSetRight").await
                == vec![TypedValue {
                    type_: "int".into(),
                    value: "2".into(),
                }]
        );
        assert!(
            tag_values_for("span:nestedSetParent").await
                == vec![TypedValue {
                    type_: "int".into(),
                    value: "-1".into(), // root: Tempo nestedSetParent sentinel
                }]
        );
    }

    #[tokio::test]
    async fn tag_values_cover_typed_span_attributes() {
        // Int, Float, and Bool span attributes round-trip through
        // typed_value_parts with the right type tags.
        assert!(
            tag_values_for(".http.status").await
                == vec![TypedValue {
                    type_: "int".into(),
                    value: "500".into(),
                }]
        );
        assert!(
            tag_values_for(".ratio").await
                == vec![TypedValue {
                    type_: "float".into(),
                    value: "0.5".into(),
                }]
        );
        assert!(
            tag_values_for(".ok").await
                == vec![TypedValue {
                    type_: "bool".into(),
                    value: "true".into(),
                }]
        );
    }

    #[tokio::test]
    async fn tag_values_cover_scoped_attribute_prefixes() {
        // `span.` and `resource.` prefixes route to the right scope.
        assert!(
            tag_values_for("span.http.method").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "GET".into(),
                }]
        );
        assert!(
            tag_values_for("resource.service.name").await
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "checkout".into(),
                }]
        );
    }

    #[tokio::test]
    async fn tag_values_empty_for_unknown_tag() {
        assert!(tag_values_for("does.not.exist").await.is_empty());
    }

    // ---- time-window filtering ----

    #[tokio::test]
    async fn out_of_range_traces_are_excluded() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "svc", "op", vec![span(1, None, "root", vec![])]);
        // The trace starts at 1000ns; a window entirely after it returns nothing.
        let r = s.scan("t", &[], 2000, 5000).await.unwrap();
        assert!(row_count(&r).await == 0);
        assert!(
            s.tag_values("t", ".svc", 2000, 5000)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(s.tag_names("t", None, 2000, 5000).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn scan_with_negated_matcher_inverts_result() {
        // A negated span-name matcher excludes the matching span.
        let neg = SpanMatcher {
            scope: MatchScope::Intrinsic,
            key: "span:name".into(),
            op: MatchCmp::Eq,
            value: MatchValue::Str("checkout".into()),
            negated: true,
        };
        assert!(scan_matches(&[neg]).await == 0);
    }

    // ---- comparison operator coverage for typed matchers ----

    #[tokio::test]
    async fn int_intrinsic_matches_every_operator() {
        // rich_span has span:duration == 5_000_000.
        let cases = [
            (MatchCmp::Eq, 5_000_000, 1),
            (MatchCmp::Eq, 5_000_001, 0),
            (MatchCmp::Neq, 5_000_001, 1),
            (MatchCmp::Neq, 5_000_000, 0),
            (MatchCmp::Lt, 5_000_001, 1),
            (MatchCmp::Lt, 5_000_000, 0),
            (MatchCmp::Lte, 5_000_000, 1),
            (MatchCmp::Gt, 4_999_999, 1),
            (MatchCmp::Gte, 5_000_000, 1),
            // A regex op against an int intrinsic is always false.
            (MatchCmp::Re, 5_000_000, 0),
        ];
        for (op, val, expected) in cases {
            let got = scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "span:duration",
                op,
                MatchValue::Int(val),
            )])
            .await;
            assert!(
                got == expected,
                "duration {op:?} {val} -> {got}, want {expected}"
            );
        }
        // A non-int expected value against an int intrinsic does not match.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Intrinsic,
                "span:duration",
                MatchCmp::Eq,
                MatchValue::Str("x".into()),
            )])
            .await
                == 0
        );
    }

    #[tokio::test]
    async fn float_attr_matches_every_operator() {
        // rich_span has attr ratio == 0.5.
        let cases = [
            (MatchCmp::Eq, 0.5, 1),
            (MatchCmp::Neq, 0.25, 1),
            (MatchCmp::Lt, 1.0, 1),
            // Boundary: value == expected. `<` is strict, so 0.5 < 0.5 is false.
            // Distinguishes `<` from `<=`.
            (MatchCmp::Lt, 0.5, 0),
            (MatchCmp::Lte, 0.5, 1),
            (MatchCmp::Gt, 0.25, 1),
            // Boundary: 0.5 > 0.5 is false. Distinguishes `>` from `>=`.
            (MatchCmp::Gt, 0.5, 0),
            (MatchCmp::Gte, 0.5, 1),
            (MatchCmp::Gt, 0.75, 0),
        ];
        for (op, val, expected) in cases {
            let got = scan_matches(&[matcher(
                MatchScope::Span,
                "ratio",
                op,
                MatchValue::Float(val),
            )])
            .await;
            assert!(
                got == expected,
                "ratio {op:?} {val} -> {got}, want {expected}"
            );
        }
        // A non-float expected value against a float attribute does not match.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "ratio",
                MatchCmp::Eq,
                MatchValue::Int(1),
            )])
            .await
                == 0
        );
    }

    #[tokio::test]
    async fn bool_attr_matches_eq_neq_and_rejects_ordering() {
        // rich_span has attr ok == true.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "ok",
                MatchCmp::Neq,
                MatchValue::Bool(false),
            )])
            .await
                == 1
        );
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "ok",
                MatchCmp::Neq,
                MatchValue::Bool(true),
            )])
            .await
                == 0
        );
        // Ordering operators against a bool attribute are always false.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "ok",
                MatchCmp::Gt,
                MatchValue::Bool(false),
            )])
            .await
                == 0
        );
        // A non-bool expected value against a bool attribute does not match.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "ok",
                MatchCmp::Eq,
                MatchValue::Int(1),
            )])
            .await
                == 0
        );
    }

    #[tokio::test]
    async fn string_attr_ordering_ops_and_negated_regex_are_false() {
        // Ordering operators against a string attribute are always false.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "http.method",
                MatchCmp::Lt,
                MatchValue::Str("Z".into()),
            )])
            .await
                == 0
        );
        // Nre that does NOT match the pattern -> the value passes the negated
        // regex, so the row matches.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "http.method",
                MatchCmp::Nre,
                MatchValue::Str("POST".into()),
            )])
            .await
                == 1
        );
        // A non-string expected value against a string attribute does not match.
        assert!(
            scan_matches(&[matcher(
                MatchScope::Span,
                "http.method",
                MatchCmp::Eq,
                MatchValue::Int(1),
            )])
            .await
                == 0
        );
    }

    fn span_with_kind_and_status(kind: i32, status: i32) -> InputSpan {
        InputSpan {
            kind,
            status_code: status,
            ..rich_span()
        }
    }

    #[tokio::test]
    async fn kind_and_status_enum_names_resolve() {
        for (name, kind) in [
            ("unspecified", 0),
            ("internal", 1),
            ("server", 2),
            ("client", 3),
            ("producer", 4),
            ("consumer", 5),
        ] {
            let mut s = InMemorySpanStore::new();
            s.push_trace("t", "svc", "op", vec![span_with_kind_and_status(kind, 0)]);
            let r = s
                .scan(
                    "t",
                    &[matcher(
                        MatchScope::Intrinsic,
                        "span:kind",
                        MatchCmp::Eq,
                        MatchValue::Str(name.into()),
                    )],
                    0,
                    10_000,
                )
                .await
                .unwrap();
            assert!(row_count(&r).await == 1, "kind name {name} should resolve");
        }

        for (name, status) in [("unset", 0), ("ok", 1), ("error", 2)] {
            let mut s = InMemorySpanStore::new();
            s.push_trace("t", "svc", "op", vec![span_with_kind_and_status(1, status)]);
            let r = s
                .scan(
                    "t",
                    &[matcher(
                        MatchScope::Intrinsic,
                        "span:status",
                        MatchCmp::Eq,
                        MatchValue::Str(name.into()),
                    )],
                    0,
                    10_000,
                )
                .await
                .unwrap();
            assert!(
                row_count(&r).await == 1,
                "status name {name} should resolve"
            );
        }
    }

    #[tokio::test]
    async fn enum_intrinsic_nil_comparison_uses_presence() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "svc", "op", vec![span_with_kind_and_status(2, 0)]);
        // kind != nil matches a present span (enum_int_matches Nil branch).
        let r = s
            .scan(
                "t",
                &[matcher(
                    MatchScope::Intrinsic,
                    "span:kind",
                    MatchCmp::Neq,
                    MatchValue::Nil,
                )],
                0,
                10_000,
            )
            .await
            .unwrap();
        assert!(row_count(&r).await == 1);
    }

    #[tokio::test]
    async fn span_without_parent_id_omits_parent_value_and_matches_nil() {
        let mut s = InMemorySpanStore::new();
        // A root span has no parent_span_id.
        s.push_trace("t", "svc", "op", vec![span(1, None, "root", vec![])]);

        // tag_values for span:parentID yields nothing for a parentless span.
        assert!(
            s.tag_values("t", "span:parentID", 0, 10_000)
                .await
                .unwrap()
                .is_empty()
        );
        // span:parentID == nil matches the parentless span.
        let r = s
            .scan(
                "t",
                &[matcher(
                    MatchScope::Intrinsic,
                    "span:parentID",
                    MatchCmp::Eq,
                    MatchValue::Nil,
                )],
                0,
                10_000,
            )
            .await
            .unwrap();
        assert!(row_count(&r).await == 1);
    }

    #[tokio::test]
    async fn empty_status_message_is_omitted_from_tag_values() {
        let mut s = InMemorySpanStore::new();
        // span() leaves status_message empty.
        s.push_trace("t", "svc", "op", vec![span(1, None, "root", vec![])]);
        assert!(
            s.tag_values("t", "span:statusMessage", 0, 10_000)
                .await
                .unwrap()
                .is_empty()
        );
    }

    // ---- direct intrinsic_matches coverage (drives every match arm) ----

    fn stored_trace_with(span: InputSpan) -> StoredTrace {
        let trace_id = span.trace_id;
        StoredTrace {
            trace_id,
            root_service_name: "rootsvc".into(),
            root_span_name: "rootname".into(),
            trace_start_unix_nano: 0,
            trace_duration_nanos: 1234,
            spans: vec![span],
            nested: vec![NestedSet {
                left: 1,
                right: 2,
                parent_id: 0,
            }],
        }
    }

    fn intrinsic(trace: &StoredTrace, key: &str, op: MatchCmp, value: MatchValue) -> bool {
        let m = matcher(MatchScope::Intrinsic, key, op, value);
        intrinsic_matches(trace, &trace.spans[0], &trace.nested, 0, &m)
    }

    #[test]
    fn intrinsic_matches_event_name_arm_presence_and_value() {
        let mut sp = span(1, None, "root", vec![]);
        sp.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let trace = stored_trace_with(sp);

        // Presence: span HAS events -> `event:name != nil` is true. The `!` on
        // `!span.events.is_empty()` is load-bearing: removing it flips this.
        assert!(intrinsic(
            &trace,
            "event:name",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
        // Value match on the event name distinguishes this arm from the `_ => true`
        // fallthrough.
        assert!(intrinsic(
            &trace,
            "event:name",
            MatchCmp::Eq,
            MatchValue::Str("cache.miss".into())
        ));
        assert!(!intrinsic(
            &trace,
            "event:name",
            MatchCmp::Eq,
            MatchValue::Str("other".into())
        ));

        // A span with NO events: `event:name != nil` is false (absence). This is
        // the other side of the `!`.
        let empty = stored_trace_with(span(1, None, "root", vec![]));
        assert!(!intrinsic(
            &empty,
            "event:name",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
    }

    #[test]
    fn intrinsic_matches_event_time_since_start_arm() {
        let mut sp = span(1, None, "root", vec![]);
        sp.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "e".into(),
            attributes: Vec::new(),
        }];
        let trace = stored_trace_with(sp);

        assert!(intrinsic(
            &trace,
            "event:timeSinceStart",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
        assert!(intrinsic(
            &trace,
            "event:timeSinceStart",
            MatchCmp::Eq,
            MatchValue::Int(50)
        ));
        assert!(!intrinsic(
            &trace,
            "event:timeSinceStart",
            MatchCmp::Eq,
            MatchValue::Int(51)
        ));

        let empty = stored_trace_with(span(1, None, "root", vec![]));
        assert!(!intrinsic(
            &empty,
            "event:timeSinceStart",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
    }

    #[test]
    fn intrinsic_matches_link_trace_id_and_span_id_arms() {
        let mut sp = span(1, None, "root", vec![]);
        sp.links = vec![LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            attributes: Vec::new(),
        }];
        let trace = stored_trace_with(sp);

        // link:traceID presence + value.
        assert!(intrinsic(
            &trace,
            "link:traceID",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
        assert!(intrinsic(
            &trace,
            "link:traceID",
            MatchCmp::Eq,
            MatchValue::Str("09090909090909090909090909090909".into())
        ));
        assert!(!intrinsic(
            &trace,
            "link:traceID",
            MatchCmp::Eq,
            MatchValue::Str("00000000000000000000000000000000".into())
        ));

        // link:spanID presence + value.
        assert!(intrinsic(
            &trace,
            "link:spanID",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
        assert!(intrinsic(
            &trace,
            "link:spanID",
            MatchCmp::Eq,
            MatchValue::Str("0808080808080808".into())
        ));
        assert!(!intrinsic(
            &trace,
            "link:spanID",
            MatchCmp::Eq,
            MatchValue::Str("0000000000000000".into())
        ));

        // No links: presence is false (other side of the `!`).
        let empty = stored_trace_with(span(1, None, "root", vec![]));
        assert!(!intrinsic(
            &empty,
            "link:traceID",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
        assert!(!intrinsic(
            &empty,
            "link:spanID",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
    }

    #[test]
    fn intrinsic_matches_trace_and_span_string_arms() {
        let mut sp = span(1, Some(2), "root", vec![]);
        sp.status_message = "boom".into();
        sp.instrumentation_name = "tracer".into();
        sp.instrumentation_version = "1.2.3".into();
        let trace = stored_trace_with(sp);

        // trace:rootService / trace:rootName / trace:duration distinct values
        // (each arm differs from the `_ => true` fallthrough).
        assert!(intrinsic(
            &trace,
            "trace:rootService",
            MatchCmp::Eq,
            MatchValue::Str("rootsvc".into())
        ));
        assert!(!intrinsic(
            &trace,
            "trace:rootService",
            MatchCmp::Eq,
            MatchValue::Str("nope".into())
        ));
        assert!(intrinsic(
            &trace,
            "trace:rootName",
            MatchCmp::Eq,
            MatchValue::Str("rootname".into())
        ));
        assert!(!intrinsic(
            &trace,
            "trace:rootName",
            MatchCmp::Eq,
            MatchValue::Str("nope".into())
        ));
        assert!(intrinsic(
            &trace,
            "trace:duration",
            MatchCmp::Eq,
            MatchValue::Int(1234)
        ));
        assert!(!intrinsic(
            &trace,
            "trace:duration",
            MatchCmp::Eq,
            MatchValue::Int(5)
        ));

        // statusMessage arm.
        assert!(intrinsic(
            &trace,
            "span:statusMessage",
            MatchCmp::Eq,
            MatchValue::Str("boom".into())
        ));
        assert!(!intrinsic(
            &trace,
            "span:statusMessage",
            MatchCmp::Eq,
            MatchValue::Str("nope".into())
        ));

        // instrumentation:name / instrumentation:version arms.
        assert!(intrinsic(
            &trace,
            "instrumentation:name",
            MatchCmp::Eq,
            MatchValue::Str("tracer".into())
        ));
        assert!(!intrinsic(
            &trace,
            "instrumentation:name",
            MatchCmp::Eq,
            MatchValue::Str("nope".into())
        ));
        assert!(intrinsic(
            &trace,
            "instrumentation:version",
            MatchCmp::Eq,
            MatchValue::Str("1.2.3".into())
        ));
        assert!(!intrinsic(
            &trace,
            "instrumentation:version",
            MatchCmp::Eq,
            MatchValue::Str("9.9.9".into())
        ));
    }

    #[test]
    fn intrinsic_matches_nested_set_left_and_right_arms() {
        let trace = stored_trace_with(span(1, None, "root", vec![]));
        // nested set is { left: 1, right: 2, parent_id: 0 }.
        assert!(intrinsic(
            &trace,
            "span:nestedSetLeft",
            MatchCmp::Eq,
            MatchValue::Int(1)
        ));
        assert!(!intrinsic(
            &trace,
            "span:nestedSetLeft",
            MatchCmp::Eq,
            MatchValue::Int(2)
        ));
        assert!(intrinsic(
            &trace,
            "span:nestedSetRight",
            MatchCmp::Eq,
            MatchValue::Int(2)
        ));
        assert!(!intrinsic(
            &trace,
            "span:nestedSetRight",
            MatchCmp::Eq,
            MatchValue::Int(1)
        ));
    }

    #[test]
    fn collect_span_intrinsic_values_omits_empty_instrumentation_version() {
        // The `if !span.instrumentation_version.is_empty()` guard must suppress an
        // empty version. Replacing the guard with `true` would insert an empty
        // value here.
        let mut values = BTreeSet::new();
        let empty = span(1, None, "root", vec![]); // version is empty
        collect_span_intrinsic_values(&empty, &[], 0, "instrumentation:version", &mut values);
        assert!(values.is_empty());

        // A non-empty version is collected.
        let mut values = BTreeSet::new();
        let with_version = InputSpan {
            instrumentation_version: "1.2.3".into(),
            ..span(1, None, "root", vec![])
        };
        collect_span_intrinsic_values(
            &with_version,
            &[],
            0,
            "instrumentation:version",
            &mut values,
        );
        assert!(values == BTreeSet::from([("string".to_string(), "1.2.3".to_string())]));
    }

    #[test]
    fn present_value_matches_eq_nil_is_false_neq_nil_is_true() {
        // A PRESENT value: `= nil` must be false, `!= nil` must be true.
        assert!(present_value_matches(MatchCmp::Eq, &MatchValue::Nil) == Some(false));
        assert!(present_value_matches(MatchCmp::Neq, &MatchValue::Nil) == Some(true));
        // Non-nil comparisons defer to the typed path.
        assert!(present_value_matches(MatchCmp::Eq, &MatchValue::Int(1)).is_none());
    }

    #[test]
    fn event_matcher_matches_absence_event_attr_uses_nil_semantics() {
        // An Event-scope attribute matcher `= nil` matches an absent event attr;
        // `!= nil` does not. Deleting the MatchScope::Event arm would force both
        // to false.
        let eq_nil = matcher(MatchScope::Event, "x", MatchCmp::Eq, MatchValue::Nil);
        let neq_nil = matcher(MatchScope::Event, "x", MatchCmp::Neq, MatchValue::Nil);
        assert!(event_matcher_matches_absence(&eq_nil));
        assert!(!event_matcher_matches_absence(&neq_nil));
    }

    #[test]
    fn link_matcher_matches_absence_link_attr_uses_nil_semantics() {
        let eq_nil = matcher(MatchScope::Link, "x", MatchCmp::Eq, MatchValue::Nil);
        let neq_nil = matcher(MatchScope::Link, "x", MatchCmp::Neq, MatchValue::Nil);
        assert!(link_matcher_matches_absence(&eq_nil));
        assert!(!link_matcher_matches_absence(&neq_nil));
    }

    #[test]
    fn link_matcher_matches_link_rejects_non_matching_value() {
        // A link attr matcher that does NOT match must return false; replacing the
        // whole function body with `true` would break this.
        let link = LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            attributes: vec![("ln.kind".into(), AttrValue::Str("retry".into()))],
        };
        let hit = matcher(
            MatchScope::Link,
            "ln.kind",
            MatchCmp::Eq,
            MatchValue::Str("retry".into()),
        );
        let miss = matcher(
            MatchScope::Link,
            "ln.kind",
            MatchCmp::Eq,
            MatchValue::Str("wrong".into()),
        );
        assert!(link_matcher_matches_link(&link, &hit));
        assert!(!link_matcher_matches_link(&link, &miss));
    }

    #[test]
    fn instrumentation_matches_rejects_non_matching_value() {
        // instrumentation_matches must return false for a mismatched name;
        // replacing the body with `true` would break this.
        let span = InputSpan {
            instrumentation_name: "tracer".into(),
            ..span(1, None, "root", vec![])
        };
        let hit = matcher(
            MatchScope::Instrumentation,
            "name",
            MatchCmp::Eq,
            MatchValue::Str("tracer".into()),
        );
        let miss = matcher(
            MatchScope::Instrumentation,
            "name",
            MatchCmp::Eq,
            MatchValue::Str("other".into()),
        );
        assert!(instrumentation_matches(&span, &hit));
        assert!(!instrumentation_matches(&span, &miss));
    }

    #[test]
    fn matcher_matches_event_and_link_arms_filter_by_key() {
        // matcher_matches' Event/Link arms select attribute values by exact key
        // (`key == &matcher.key`). With `!=` they would read the wrong attribute.
        let mut sp = span(1, None, "root", vec![]);
        sp.events = vec![EventRef {
            time_since_start_nano: 1,
            name: "e".into(),
            attributes: vec![
                ("want".into(), AttrValue::Str("yes".into())),
                ("other".into(), AttrValue::Str("no".into())),
            ],
        }];
        sp.links = vec![LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            attributes: vec![
                ("lwant".into(), AttrValue::Str("yes".into())),
                ("lother".into(), AttrValue::Str("no".into())),
            ],
        }];
        let trace = stored_trace_with(sp);

        let event_m = matcher(
            MatchScope::Event,
            "want",
            MatchCmp::Eq,
            MatchValue::Str("yes".into()),
        );
        assert!(matcher_matches(
            &trace,
            &trace.spans[0],
            &trace.nested,
            0,
            &event_m,
        ));
        // The "other" attribute's value is "no", so matching key "want" against
        // "yes" depends on selecting the correct key.
        let event_wrong = matcher(
            MatchScope::Event,
            "want",
            MatchCmp::Eq,
            MatchValue::Str("no".into()),
        );
        assert!(!matcher_matches(
            &trace,
            &trace.spans[0],
            &trace.nested,
            0,
            &event_wrong,
        ));

        let link_m = matcher(
            MatchScope::Link,
            "lwant",
            MatchCmp::Eq,
            MatchValue::Str("yes".into()),
        );
        assert!(matcher_matches(
            &trace,
            &trace.spans[0],
            &trace.nested,
            0,
            &link_m,
        ));
        let link_wrong = matcher(
            MatchScope::Link,
            "lwant",
            MatchCmp::Eq,
            MatchValue::Str("no".into()),
        );
        assert!(!matcher_matches(
            &trace,
            &trace.spans[0],
            &trace.nested,
            0,
            &link_wrong,
        ));
    }

    #[test]
    fn span_matches_excludes_event_matchers_from_attribute_pass() {
        // span_matches keeps only non-event, non-link matchers for the per-span
        // `.all()` attribute pass: `!is_event && !is_link`. With `||` instead of
        // `&&`, a NEGATED event-intrinsic matcher would be (wrongly) re-evaluated
        // via matcher_matches and drop a span that the nested same-event logic
        // accepted.
        let mut sp = span(1, None, "root", vec![]);
        sp.events = vec![
            EventRef {
                time_since_start_nano: 10,
                name: "cache.miss".into(),
                attributes: Vec::new(),
            },
            EventRef {
                time_since_start_nano: 20,
                name: "cache.hit".into(),
                attributes: Vec::new(),
            },
        ];
        let trace = stored_trace_with(sp);
        // `!event:name = "cache.miss"`: the "cache.hit" event satisfies the
        // negated same-event matcher, so the span matches.
        let neg = SpanMatcher {
            scope: MatchScope::Intrinsic,
            key: "event:name".into(),
            op: MatchCmp::Eq,
            value: MatchValue::Str("cache.miss".into()),
            negated: true,
        };
        assert!(span_matches(
            &trace,
            &trace.spans[0],
            &trace.nested,
            0,
            &[neg],
        ));
    }
}
