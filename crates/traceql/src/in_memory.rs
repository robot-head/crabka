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
use crate::span_columns::{InputSpan, NestedSet, assign_nested_set, span_schema_with_attrs};
use crate::store::{MatchCmp, MatchScope, MatchValue, ScanResult, SpanMatcher, SpanStore};

const INTRINSIC_TAGS: &[&str] = &[
    "span:childCount",
    "span:duration",
    "span:id",
    "span:kind",
    "span:name",
    "span:Parent",
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

    fn attr_columns(traces: &[&StoredTrace]) -> Vec<(String, DataType)> {
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

#[async_trait::async_trait]
#[allow(clippy::too_many_lines)]
impl SpanStore for InMemorySpanStore {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
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
        let attr_cols = Self::attr_columns(&in_range);
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
        let mut attr_builders: Vec<(String, AttrBuilder)> = attr_cols
            .iter()
            .map(|(key, dt)| (key.clone(), AttrBuilder::new(dt)))
            .collect();

        for trace in &in_range {
            for (i, span) in trace.spans.iter().enumerate() {
                if !span_matches(trace, span, &trace.nested, i, matchers) {
                    continue;
                }
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

                for (key, builder) in &mut attr_builders {
                    let value = span.attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v);
                    builder.append(value);
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
        ];
        columns.extend(attr_builders.into_iter().map(|(_, b)| b.finish()));

        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| TraceqlError::Store(e.to_string()))?;
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("spans", Arc::new(table))?;
        Ok(ScanResult {
            ctx,
            span_table: "spans".into(),
        })
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
        let mut values = BTreeSet::new();
        for trace in self.traces_in_range(tenant, start_ns, end_ns) {
            collect_trace_intrinsic_values(trace, tag, &mut values);
            if tag == "service.name" {
                values.insert(("string".to_string(), trace.root_service_name.clone()));
            }
            for (idx, input) in trace.spans.iter().enumerate() {
                collect_span_intrinsic_values(input, &trace.nested, idx, tag, &mut values);
                collect_event_values(input, tag, &mut values);
                collect_link_values(input, tag, &mut values);
                values.extend(
                    input
                        .attrs
                        .iter()
                        .filter(|(key, _)| key == tag)
                        .map(|(_, value)| typed_value_parts(value)),
                );
            }
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| TypedValue { type_, value })
            .collect())
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
        "span:nestedSetParent" | "span:Parent" => nested_sets.get(idx).is_some_and(|nested| {
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
        "span:nestedSetParent" | "span:Parent" => {
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
                .filter(|(key, _)| key == tag)
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
                .filter(|(key, _)| key == tag)
                .map(|(_, value)| typed_value_parts(value)),
        );
    }
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
        assert!(pid.value(0) == 0);
        assert!(pid.value(1) == 1);
    }

    #[tokio::test]
    async fn trace_by_id_returns_stored_spans() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "svc", "op", vec![span(1, None, "root", vec![])]);
        let got = s.trace_by_id("t", &[7; 16]).await.unwrap().unwrap();
        assert!(got.trace_id == [7; 16]);
        assert!(got.root_service_name == "svc");
        assert!(got.root_trace_name == "op");
        assert!(got.spans.len() == 1);
        assert!(got.spans[0].name == "root");
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
        assert!(got.len() == 3);
        assert!(got[0].scope == TagScope::Resource);
        assert!(got[0].tags == vec!["service.name"]);
        assert!(got[1].scope == TagScope::Span);
        assert!(got[1].tags == vec!["svc"]);
        assert!(got[2].scope == TagScope::Intrinsic);
        assert!(got[2].tags == INTRINSIC_TAGS);
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
        assert!(got.len() == 1);
        assert!(got[0].scope == TagScope::Instrumentation);
        assert!(got[0].tags == vec!["instrumentation:name", "instrumentation:version"]);
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
        assert!(event_names.len() == 1);
        assert!(event_names[0].scope == TagScope::Event);
        assert!(event_names[0].tags == vec!["cache.key", "event:name", "event:timeSinceStart"]);

        let link_names = s
            .tag_names("t", Some(TagScope::Link), 0, 10_000)
            .await
            .unwrap();
        assert!(link_names.len() == 1);
        assert!(link_names[0].scope == TagScope::Link);
        assert!(link_names[0].tags == vec!["link.kind", "link:spanID", "link:traceID"]);

        assert!(
            s.tag_values("t", "event:name", 0, 10_000).await.unwrap()
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "exception".into(),
                }]
        );
        assert!(
            s.tag_values("t", "event:timeSinceStart", 0, 10_000)
                .await
                .unwrap()
                == vec![TypedValue {
                    type_: "duration".into(),
                    value: "50".into(),
                }]
        );
        assert!(
            s.tag_values("t", "cache.key", 0, 10_000).await.unwrap()
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "users".into(),
                }]
        );
        assert!(
            s.tag_values("t", "link:traceID", 0, 10_000).await.unwrap()
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "09090909090909090909090909090909".into(),
                }]
        );
        assert!(
            s.tag_values("t", "link:spanID", 0, 10_000).await.unwrap()
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "0808080808080808".into(),
                }]
        );
        assert!(
            s.tag_values("t", "link.kind", 0, 10_000).await.unwrap()
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "retry".into(),
                }]
        );
    }

    #[tokio::test]
    async fn tag_names_return_intrinsic_scope() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "svc", "op", vec![span(1, None, "root", vec![])]);

        let got = s
            .tag_names("t", Some(TagScope::Intrinsic), 0, 10_000)
            .await
            .unwrap();
        assert!(got.len() == 1);
        assert!(got[0].scope == TagScope::Intrinsic);
        assert!(
            got[0].tags
                == vec![
                    "span:childCount",
                    "span:duration",
                    "span:id",
                    "span:kind",
                    "span:name",
                    "span:Parent",
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
                ]
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
}
