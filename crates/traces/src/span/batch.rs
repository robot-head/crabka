//! Build span block `RecordBatch` values from internal spans.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    AttrValue as BlockAttrValue, NestedSet as BlockNestedSet, PromotedSpanAttr, SpanAttr,
    SpanEvent, SpanKind, SpanLink, SpanRow, StatusCode, encode_span_rows_with_promoted_attrs,
};

use super::{AttrValue, KeyValue, Span, nested_set::assign_nested_set};
use crate::error::TracesError;

pub const RESOURCE_ATTR_PREFIX: &str = "__resource.";

/// Build one span-block `RecordBatch` from spans of one trace.
pub fn span_batch(spans: &[Span]) -> Result<RecordBatch, TracesError> {
    span_batch_with_promoted_attrs(spans, &[])
}

/// Build one span-block `RecordBatch` from spans of one trace with configured
/// attributes duplicated into dedicated columns.
///
/// `spans` must be the complete per-trace span set: the trace-level columns
/// (root service/name, start, duration) are computed over exactly these spans.
pub fn span_batch_with_promoted_attrs(
    spans: &[Span],
    promoted_attrs: &[PromotedSpanAttr],
) -> Result<RecordBatch, TracesError> {
    span_batch_for_window(spans, spans, promoted_attrs)
}

/// Build one span-block `RecordBatch` whose rows are `row_spans` but whose
/// trace-level columns are computed over `trace_spans`.
///
/// Use this when a query window clips a trace: `row_spans` is the in-window
/// subset, while `trace_spans` is the trace's full span set so that
/// `root_service_name` / `root_span_name` / `trace_start_unix_nano` /
/// `trace_duration_nanos` reflect the whole trace rather than only the window.
/// Pass the same slice for both to materialize a complete trace.
pub fn span_batch_for_window(
    row_spans: &[Span],
    trace_spans: &[Span],
    promoted_attrs: &[PromotedSpanAttr],
) -> Result<RecordBatch, TracesError> {
    // Nested-set intervals and child counts describe the rows themselves, so
    // they are computed over `row_spans`. Trace-level columns describe the
    // whole trace, so they come from `trace_spans`.
    let nested = assign_nested_set(row_spans);
    let child_counts = child_counts(&nested);
    let (root_service_name, root_span_name, trace_start, trace_duration) = root_info(trace_spans);
    let spans = row_spans;
    let rows = spans
        .iter()
        .zip(nested)
        .zip(child_counts)
        .map(|((span, nested_set), child_count)| SpanRow {
            trace_id: span.trace_id,
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            nested_set: BlockNestedSet {
                nested_set_left: nested_set.left,
                nested_set_right: nested_set.right,
                parent_id: nested_set.parent_id,
            },
            child_count,
            root_service_name: Some(root_service_name.clone()),
            root_span_name: Some(root_span_name.clone()),
            trace_start_unix_nano: trace_start,
            trace_duration_nanos: trace_duration,
            name: Some(span.name.clone()),
            kind: block_kind(span.kind),
            start_unix_nano: span.start_ns,
            duration_nanos: span.duration_ns,
            status_code: block_status(span.status),
            status_message: Some(span.status_message.clone()),
            instrumentation_name: Some(span.instrumentation_scope.clone()),
            instrumentation_version: Some(span.instrumentation_version.clone()),
            attrs: span_attrs(span),
            events: span_events(span),
            links: span_links(span),
        })
        .collect::<Vec<_>>();

    encode_span_rows_with_promoted_attrs(&rows, promoted_attrs)
        .map_err(|err| TracesError::Block(err.to_string()))
}

fn child_counts(nested: &[crate::span::nested_set::NestedSet]) -> Vec<i32> {
    // Single O(n) pass: tally how many nodes name each `parent_id`, then each
    // node's child count is the tally for its own `left` interval.
    let mut counts: HashMap<i32, i32> = HashMap::with_capacity(nested.len());
    for node in nested {
        let count = counts.entry(node.parent_id).or_insert(0);
        *count = count.saturating_add(1);
    }
    nested
        .iter()
        .map(|node| counts.get(&node.left).copied().unwrap_or(0))
        .collect()
}

/// Compute the trace-level columns (root service/name, trace start, trace
/// duration) for one trace.
///
/// CONTRACT: `spans` must be the COMPLETE per-trace span set. Passing a
/// time-windowed or otherwise filtered subset yields trace-level values that
/// reflect only the subset, not the trace. Callers that materialize rows from a
/// clipped window must use [`span_batch_for_window`] and pass the full trace's
/// spans here.
fn root_info(spans: &[Span]) -> (String, String, i64, i64) {
    let root = spans
        .iter()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().min_by_key(|span| span.start_ns));
    let service = root
        .and_then(|span| service_name(&span.resource_attrs))
        .unwrap_or_default();
    let name = root.map(|span| span.name.clone()).unwrap_or_default();
    let start = spans.iter().map(|span| span.start_ns).min().unwrap_or(0);
    let end = spans
        .iter()
        .map(|span| span.start_ns.saturating_add(span.duration_ns))
        .max()
        .unwrap_or(start);
    (service, name, start, end.saturating_sub(start))
}

fn service_name(attrs: &[KeyValue]) -> Option<String> {
    attrs.iter().find_map(|attr| {
        (attr.key == "service.name").then(|| match &attr.value {
            AttrValue::Str(value) => Some(value.clone()),
            _ => None,
        })?
    })
}

fn span_attrs(span: &Span) -> Vec<SpanAttr> {
    let mut attrs = Vec::new();
    for attr in &span.resource_attrs {
        push_span_attr(
            &mut attrs,
            format!("{RESOURCE_ATTR_PREFIX}{}", attr.key),
            &attr.value,
        );
    }
    for attr in &span.span_attrs {
        // Reserve the `__resource.` namespace for true resource attributes. A
        // client span attribute keyed under this prefix would otherwise be
        // indistinguishable downstream from a resource-scoped attribute,
        // letting a client spoof `resource.`-scoped values (TraceQL scope
        // bypass / tenant data-integrity). Drop such span attributes.
        if attr.key.starts_with(RESOURCE_ATTR_PREFIX) {
            continue;
        }
        push_span_attr(&mut attrs, attr.key.clone(), &attr.value);
    }
    attrs
}

fn push_span_attr(attrs: &mut Vec<SpanAttr>, key: String, value: &AttrValue) {
    let value = block_attr_value(value);
    if let Some(existing) = attrs
        .iter_mut()
        .find(|attr| attr.key == key && same_block_attr_type(&attr.value, &value))
    {
        extend_block_attr_value(&mut existing.value, value);
        existing.is_array = true;
        return;
    }
    attrs.push(SpanAttr {
        key,
        is_array: false,
        value,
    });
}

fn block_attr_value(value: &AttrValue) -> BlockAttrValue {
    match value {
        AttrValue::Str(value) => BlockAttrValue::Str(vec![value.clone()]),
        AttrValue::Int(value) => BlockAttrValue::Int(vec![*value]),
        AttrValue::Double(value) => BlockAttrValue::Double(vec![*value]),
        AttrValue::Bool(value) => BlockAttrValue::Bool(vec![*value]),
        AttrValue::Bytes(value) => BlockAttrValue::Str(vec![hex::encode(value)]),
    }
}

fn same_block_attr_type(lhs: &BlockAttrValue, rhs: &BlockAttrValue) -> bool {
    matches!(
        (lhs, rhs),
        (BlockAttrValue::Str(_), BlockAttrValue::Str(_))
            | (BlockAttrValue::Int(_), BlockAttrValue::Int(_))
            | (BlockAttrValue::Double(_), BlockAttrValue::Double(_))
            | (BlockAttrValue::Bool(_), BlockAttrValue::Bool(_))
    )
}

fn extend_block_attr_value(existing: &mut BlockAttrValue, next: BlockAttrValue) {
    match (existing, next) {
        (BlockAttrValue::Str(existing), BlockAttrValue::Str(next)) => existing.extend(next),
        (BlockAttrValue::Int(existing), BlockAttrValue::Int(next)) => existing.extend(next),
        (BlockAttrValue::Double(existing), BlockAttrValue::Double(next)) => existing.extend(next),
        (BlockAttrValue::Bool(existing), BlockAttrValue::Bool(next)) => existing.extend(next),
        _ => unreachable!("same_block_attr_type guards extension"),
    }
}

fn event_attr_value(value: &AttrValue) -> String {
    match value {
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Double(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
        AttrValue::Bytes(value) => hex::encode(value),
    }
}

fn event_attrs(attrs: &[KeyValue]) -> Vec<(String, String)> {
    attrs
        .iter()
        .map(|attr| (attr.key.clone(), event_attr_value(&attr.value)))
        .collect()
}

fn span_events(span: &Span) -> Vec<SpanEvent> {
    span.events
        .iter()
        .map(|event| SpanEvent {
            name: event.name.clone(),
            time_since_start_nano: event.time_unix_nano.saturating_sub(span.start_ns),
            attrs: event_attrs(&event.attrs),
        })
        .collect()
}

fn span_links(span: &Span) -> Vec<SpanLink> {
    span.links
        .iter()
        .map(|link| SpanLink {
            linked_trace_id: link.trace_id,
            linked_span_id: link.span_id,
            attrs: event_attrs(&link.attrs),
        })
        .collect()
}

fn block_kind(kind: super::SpanKind) -> SpanKind {
    match kind {
        super::SpanKind::Unspecified => SpanKind::Unspecified,
        super::SpanKind::Internal => SpanKind::Internal,
        super::SpanKind::Server => SpanKind::Server,
        super::SpanKind::Client => SpanKind::Client,
        super::SpanKind::Producer => SpanKind::Producer,
        super::SpanKind::Consumer => SpanKind::Consumer,
    }
}

fn block_status(status: super::StatusCode) -> StatusCode {
    match status {
        super::StatusCode::Unset => StatusCode::Unset,
        super::StatusCode::Ok => StatusCode::Ok,
        super::StatusCode::Error => StatusCode::Error,
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, Int32Array, ListArray, StringArray,
    };
    use assert2::assert;
    use crabka_blockstore::{
        SCOL_ATTR_IS_ARRAY, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SCOL_NESTED_SET_LEFT,
        SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID, SCOL_ROOT_SERVICE_NAME, SCOL_SPAN_ID, SCOL_TRACE_ID,
        span_block_schema,
    };

    use super::*;
    use crate::span::{
        EventRecord, KeyValue, LinkRecord, SpanKind as TraceKind, StatusCode as TraceStatus,
    };

    fn span(id: u8, parent: Option<u8>, root_svc: &str) -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("s{id}"),
            kind: TraceKind::Server,
            start_ns: i64::from(id) * 10,
            duration_ns: 5,
            status: TraceStatus::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str(root_svc.into()),
            }],
            span_attrs: vec![KeyValue {
                key: "http.status_code".into(),
                value: AttrValue::Int(200),
            }],
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        }
    }

    fn col<'a, A: 'static>(batch: &'a RecordBatch, name: &str) -> &'a A {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<A>()
            .unwrap()
    }

    #[test]
    fn builds_batch_with_identity_and_nested_set() {
        let spans = vec![span(1, None, "api"), span(2, Some(1), "api")];
        let batch = span_batch(&spans).unwrap();
        assert_eq!(batch.schema(), span_block_schema());
        assert_eq!(batch.num_rows(), 2);

        let trace_ids = col::<FixedSizeBinaryArray>(&batch, SCOL_TRACE_ID);
        assert!(trace_ids.value(0) == [1; 16]);
        let span_ids = col::<FixedSizeBinaryArray>(&batch, SCOL_SPAN_ID);
        assert!(span_ids.value(0) == [1; 8]);

        let left = col::<Int32Array>(&batch, SCOL_NESTED_SET_LEFT);
        let right = col::<Int32Array>(&batch, SCOL_NESTED_SET_RIGHT);
        let parent_id = col::<Int32Array>(&batch, SCOL_PARENT_ID);
        assert_eq!(left.values().as_ref(), &[1, 2]);
        assert_eq!(right.values().as_ref(), &[4, 3]);
        // Root parent is -1 (Tempo nestedSetParent sentinel); the child's
        // parent_id equals the root's left value.
        assert_eq!(parent_id.values().as_ref(), &[-1, 1]);

        let service = col::<StringArray>(&batch, SCOL_ROOT_SERVICE_NAME);
        assert!(service.value(0) == "api");
    }

    #[test]
    fn groups_repeated_attribute_keys_as_array_values() {
        let mut s = span(1, None, "api");
        s.span_attrs = vec![
            KeyValue {
                key: "http.method".into(),
                value: AttrValue::Str("GET".into()),
            },
            KeyValue {
                key: "http.method".into(),
                value: AttrValue::Str("POST".into()),
            },
        ];

        let batch = span_batch(&[s]).unwrap();

        let keys = col::<ListArray>(&batch, SCOL_ATTR_KEYS);
        let keys_row = keys.value(0);
        let keys = keys_row.as_any().downcast_ref::<StringArray>().unwrap();
        let methods_idx = (0..keys.len())
            .find(|idx| keys.value(*idx) == "http.method")
            .unwrap();
        assert!(
            (0..keys.len())
                .filter(|idx| keys.value(*idx) == "http.method")
                .count()
                == 1
        );

        let is_array = col::<ListArray>(&batch, SCOL_ATTR_IS_ARRAY);
        let is_array_row = is_array.value(0);
        let is_array = is_array_row
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(is_array.value(methods_idx));

        let values = col::<ListArray>(&batch, SCOL_ATTR_VALUE);
        let row_values = values.value(0);
        let row_values = row_values
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap_or_else(|| panic!("{SCOL_ATTR_VALUE} row is not a list"));
        let method_values = row_values.value(methods_idx);
        let method_values = method_values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(method_values.value(0), "GET");
        assert_eq!(method_values.value(1), "POST");
    }

    fn attr_keys_of_row(batch: &RecordBatch, row: usize) -> Vec<String> {
        let keys = col::<ListArray>(batch, SCOL_ATTR_KEYS);
        let keys_row = keys.value(row);
        let keys = keys_row.as_any().downcast_ref::<StringArray>().unwrap();
        (0..keys.len())
            .map(|idx| keys.value(idx).to_string())
            .collect()
    }

    #[test]
    fn child_counts_match_tree_shape() {
        use crabka_blockstore::SCOL_CHILD_COUNT;

        // span 1 is root with two children (2, 3); span 2 has one child (4).
        let spans = vec![
            span(1, None, "api"),
            span(2, Some(1), "api"),
            span(3, Some(1), "api"),
            span(4, Some(2), "api"),
        ];
        let batch = span_batch(&spans).unwrap();
        let counts = col::<Int32Array>(&batch, SCOL_CHILD_COUNT);
        // Rows are index-aligned with input order: root has children 2 and 3,
        // span 2 has child 4, spans 3 and 4 are leaves.
        assert_eq!(counts.values().as_ref(), &[2, 1, 0, 0]);
    }

    #[test]
    fn span_attr_cannot_spoof_resource_scope() {
        let mut s = span(1, None, "api");
        // A real resource attr legitimately gets the `__resource.` prefix downstream.
        s.resource_attrs.push(KeyValue {
            key: "deployment.environment".into(),
            value: AttrValue::Str("prod".into()),
        });
        // A client span attr keyed to look like a resource attr must NOT be encoded
        // into the resource namespace (TraceQL `resource.` scope bypass / spoof).
        s.span_attrs.push(KeyValue {
            key: format!("{RESOURCE_ATTR_PREFIX}service.name"),
            value: AttrValue::Str("evil".into()),
        });

        let batch = span_batch(&[s]).unwrap();
        let keys = attr_keys_of_row(&batch, 0);

        // The real resource attr is present under the resource namespace.
        assert!(keys.contains(&format!("{RESOURCE_ATTR_PREFIX}deployment.environment")));
        // The legitimate resource service.name is present exactly once.
        let resource_service_key = format!("{RESOURCE_ATTR_PREFIX}service.name");
        assert!(
            keys.iter()
                .filter(|key| **key == resource_service_key)
                .count()
                == 1
        );
        // The spoofed span attr (whose value was "evil") did NOT land in the
        // resource namespace: there is no second `__resource.service.name` entry,
        // and the resource value remains the true "api".
        let values = col::<ListArray>(&batch, SCOL_ATTR_VALUE);
        let row_values = values.value(0);
        let row_values = row_values.as_any().downcast_ref::<ListArray>().unwrap();
        let resource_service_idx = keys
            .iter()
            .position(|key| *key == resource_service_key)
            .unwrap();
        let service_values = row_values.value(resource_service_idx);
        let service_values = service_values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(service_values.len(), 1);
        assert_eq!(service_values.value(0), "api");
    }

    #[test]
    fn carries_events_and_links_through_schema() {
        let mut s = span(1, None, "api");
        s.events.push(EventRecord {
            time_unix_nano: 15,
            name: "exception".into(),
            attrs: vec![KeyValue {
                key: "exception.type".into(),
                value: AttrValue::Str("IO".into()),
            }],
        });
        s.links.push(LinkRecord {
            trace_id: [9; 16],
            span_id: [8; 8],
            attrs: Vec::new(),
        });

        let batch = span_batch(&[s]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name(crabka_blockstore::SCOL_EVENTS)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            batch
                .column_by_name(crabka_blockstore::SCOL_LINKS)
                .unwrap()
                .len(),
            1
        );
    }
}
