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
use crate::result::{AttrValue, ScopedTag, SpanRef, TagScope, TraceSpans, TypedValue};
use crate::span_columns::{InputSpan, NestedSet, assign_nested_set, span_schema_with_attrs};
use crate::store::{ScanResult, SpanMatcher, SpanStore};

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
        _matchers: &[SpanMatcher],
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
        for trace in self.traces_in_range(tenant, start_ns, end_ns) {
            resource.insert("service.name".to_string());
            for input in &trace.spans {
                span.extend(input.attrs.iter().map(|(key, _)| key.clone()));
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

    use crate::result::AttrValue;
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
    async fn tag_names_return_resource_and_span_scopes() {
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
        assert!(got.len() == 2);
        assert!(got[0].scope == TagScope::Resource);
        assert!(got[0].tags == vec!["service.name"]);
        assert!(got[1].scope == TagScope::Span);
        assert!(got[1].tags == vec!["svc"]);
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
