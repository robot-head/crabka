//! Encode in-memory span rows into the flattened span block Arrow schema.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder,
    ListBuilder, StringBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;

use crate::error::{BlockStoreError, Result};
use crate::nested_set::NestedSet;
use crate::span_schema::{
    SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SpanKind, StatusCode, span_block_schema,
};

/// A generic attribute value list. Scalars are represented as one-element lists.
#[derive(Clone, Debug, PartialEq)]
pub enum AttrValue {
    Str(Vec<String>),
    Int(Vec<i64>),
    Double(Vec<f64>),
    Bool(Vec<bool>),
}

/// One generic span attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanAttr {
    pub key: String,
    pub is_array: bool,
    pub value: AttrValue,
}

/// One nested span event.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanEvent {
    pub name: String,
    pub time_since_start_nano: i64,
    pub attrs: Vec<(String, String)>,
}

/// One nested span link.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanLink {
    pub linked_trace_id: [u8; 16],
    pub linked_span_id: [u8; 8],
    pub attrs: Vec<(String, String)>,
}

/// One flattened span row.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub nested_set: NestedSet,
    pub child_count: i32,
    pub root_service_name: Option<String>,
    pub root_span_name: Option<String>,
    pub trace_start_unix_nano: i64,
    pub trace_duration_nanos: i64,
    pub name: Option<String>,
    pub kind: SpanKind,
    pub start_unix_nano: i64,
    pub duration_nanos: i64,
    pub status_code: StatusCode,
    pub status_message: Option<String>,
    pub instrumentation_name: Option<String>,
    pub instrumentation_version: Option<String>,
    pub attrs: Vec<SpanAttr>,
    pub events: Vec<SpanEvent>,
    pub links: Vec<SpanLink>,
}

fn new_str_list() -> ListBuilder<StringBuilder> {
    ListBuilder::new(StringBuilder::new())
}

fn new_str_list_list() -> ListBuilder<ListBuilder<StringBuilder>> {
    ListBuilder::new(new_str_list())
}

/// Encode rows into a record batch matching [`span_block_schema`].
pub fn encode_span_rows(rows: &[SpanRow]) -> Result<RecordBatch> {
    let mut trace_id = FixedSizeBinaryBuilder::new(16);
    let mut span_id = FixedSizeBinaryBuilder::new(8);
    let mut parent_span_id = FixedSizeBinaryBuilder::new(8);
    let mut ns_left = Int32Builder::new();
    let mut ns_right = Int32Builder::new();
    let mut parent_id = Int32Builder::new();
    let mut child_count = Int32Builder::new();
    let mut root_svc = StringBuilder::new();
    let mut root_name = StringBuilder::new();
    let mut trace_start = Int64Builder::new();
    let mut trace_dur = Int64Builder::new();
    let mut name = StringBuilder::new();
    let mut kind = Int32Builder::new();
    let mut start = Int64Builder::new();
    let mut dur = Int64Builder::new();
    let mut status = Int32Builder::new();
    let mut status_msg = StringBuilder::new();
    let mut instrumentation_name = StringBuilder::new();
    let mut instrumentation_version = StringBuilder::new();
    let mut attr_keys = new_str_list();
    let mut attr_is_array = ListBuilder::new(BooleanBuilder::new());
    let mut attr_value = new_str_list_list();
    let mut attr_value_int = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    let mut attr_value_double = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
    let mut attr_value_bool = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));
    let mut events = ListBuilder::new(new_event_struct_builder());
    let mut links = ListBuilder::new(new_link_struct_builder());

    for row in rows {
        trace_id
            .append_value(row.trace_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        span_id
            .append_value(row.span_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        match row.parent_span_id {
            Some(parent) => parent_span_id
                .append_value(parent)
                .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?,
            None => parent_span_id.append_null(),
        }
        ns_left.append_value(row.nested_set.nested_set_left);
        ns_right.append_value(row.nested_set.nested_set_right);
        parent_id.append_value(row.nested_set.parent_id);
        child_count.append_value(row.child_count);
        root_svc.append_option(row.root_service_name.as_deref());
        root_name.append_option(row.root_span_name.as_deref());
        trace_start.append_value(row.trace_start_unix_nano);
        trace_dur.append_value(row.trace_duration_nanos);
        name.append_option(row.name.as_deref());
        kind.append_value(row.kind.as_i32());
        start.append_value(row.start_unix_nano);
        dur.append_value(row.duration_nanos);
        status.append_value(row.status_code.as_i32());
        status_msg.append_option(row.status_message.as_deref());
        instrumentation_name.append_option(row.instrumentation_name.as_deref());
        instrumentation_version.append_option(row.instrumentation_version.as_deref());

        append_attrs(
            &row.attrs,
            &mut attr_keys,
            &mut attr_is_array,
            &mut attr_value,
            &mut attr_value_int,
            &mut attr_value_double,
            &mut attr_value_bool,
        );
        append_events(&mut events, &row.events);
        append_links(&mut links, &row.links)?;
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(parent_span_id.finish()),
        Arc::new(ns_left.finish()),
        Arc::new(ns_right.finish()),
        Arc::new(parent_id.finish()),
        Arc::new(child_count.finish()),
        Arc::new(root_svc.finish()),
        Arc::new(root_name.finish()),
        Arc::new(trace_start.finish()),
        Arc::new(trace_dur.finish()),
        Arc::new(name.finish()),
        Arc::new(kind.finish()),
        Arc::new(start.finish()),
        Arc::new(dur.finish()),
        Arc::new(status.finish()),
        Arc::new(status_msg.finish()),
        Arc::new(instrumentation_name.finish()),
        Arc::new(instrumentation_version.finish()),
        Arc::new(attr_keys.finish()),
        Arc::new(attr_is_array.finish()),
        Arc::new(attr_value.finish()),
        Arc::new(attr_value_int.finish()),
        Arc::new(attr_value_double.finish()),
        Arc::new(attr_value_bool.finish()),
        Arc::new(events.finish()),
        Arc::new(links.finish()),
    ];

    RecordBatch::try_new(span_block_schema(), columns)
        .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))
}

fn append_attrs(
    attrs: &[SpanAttr],
    keys: &mut ListBuilder<StringBuilder>,
    is_array: &mut ListBuilder<BooleanBuilder>,
    str_values: &mut ListBuilder<ListBuilder<StringBuilder>>,
    int_values: &mut ListBuilder<ListBuilder<Int64Builder>>,
    double_values: &mut ListBuilder<ListBuilder<Float64Builder>>,
    bool_values: &mut ListBuilder<ListBuilder<BooleanBuilder>>,
) {
    for attr in attrs {
        keys.values().append_value(&attr.key);
        is_array.values().append_value(attr.is_array);

        match &attr.value {
            AttrValue::Str(values) => {
                for value in values {
                    str_values.values().values().append_value(value);
                }
            }
            AttrValue::Int(values) => {
                for &value in values {
                    int_values.values().values().append_value(value);
                }
            }
            AttrValue::Double(values) => {
                for &value in values {
                    double_values.values().values().append_value(value);
                }
            }
            AttrValue::Bool(values) => {
                for &value in values {
                    bool_values.values().values().append_value(value);
                }
            }
        }

        str_values.values().append(true);
        int_values.values().append(true);
        double_values.values().append(true);
        bool_values.values().append(true);
    }
    keys.append(true);
    is_array.append(true);
    str_values.append(true);
    int_values.append(true);
    double_values.append(true);
    bool_values.append(true);
}

fn new_event_struct_builder() -> StructBuilder {
    StructBuilder::new(
        Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("time_since_start_nano", DataType::Int64, true),
            Field::new(
                SCOL_ATTR_KEYS,
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                SCOL_ATTR_VALUE,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ))),
                true,
            ),
        ]),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int64Builder::new()),
            Box::new(new_str_list()),
            Box::new(new_str_list_list()),
        ],
    )
}

fn new_link_struct_builder() -> StructBuilder {
    StructBuilder::new(
        Fields::from(vec![
            Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
            Field::new(
                SCOL_ATTR_KEYS,
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                SCOL_ATTR_VALUE,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ))),
                true,
            ),
        ]),
        vec![
            Box::new(FixedSizeBinaryBuilder::new(16)),
            Box::new(FixedSizeBinaryBuilder::new(8)),
            Box::new(new_str_list()),
            Box::new(new_str_list_list()),
        ],
    )
}

fn append_events(events: &mut ListBuilder<StructBuilder>, rows: &[SpanEvent]) {
    let sb = events.values();
    for event in rows {
        sb.field_builder::<StringBuilder>(0)
            .expect("event name builder")
            .append_value(&event.name);
        sb.field_builder::<Int64Builder>(1)
            .expect("event time builder")
            .append_value(event.time_since_start_nano);
        append_kv(sb, &event.attrs);
        sb.append(true);
    }
    events.append(true);
}

fn append_links(links: &mut ListBuilder<StructBuilder>, rows: &[SpanLink]) -> Result<()> {
    let sb = links.values();
    for link in rows {
        sb.field_builder::<FixedSizeBinaryBuilder>(0)
            .expect("linked trace id builder")
            .append_value(link.linked_trace_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        sb.field_builder::<FixedSizeBinaryBuilder>(1)
            .expect("linked span id builder")
            .append_value(link.linked_span_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        append_kv(sb, &link.attrs);
        sb.append(true);
    }
    links.append(true);
    Ok(())
}

fn append_kv(sb: &mut StructBuilder, attrs: &[(String, String)]) {
    let keys = sb
        .field_builder::<ListBuilder<StringBuilder>>(2)
        .expect("attr key list builder");
    for (key, _) in attrs {
        keys.values().append_value(key);
    }
    keys.append(true);

    let values = sb
        .field_builder::<ListBuilder<ListBuilder<StringBuilder>>>(3)
        .expect("attr value list builder");
    for (_, value) in attrs {
        values.values().values().append_value(value);
        values.values().append(true);
    }
    values.append(true);
}

#[cfg(test)]
mod tests {
    use arrow::array::{FixedSizeBinaryArray, Int32Array};
    use assert2::assert;

    use super::*;
    use crate::span_schema::{SCOL_KIND, SCOL_NESTED_SET_LEFT, SCOL_TRACE_ID, span_block_schema};

    fn tid() -> [u8; 16] {
        [1; 16]
    }

    fn sample_row(span: u8, parent: Option<u8>, left: i32) -> SpanRow {
        SpanRow {
            trace_id: tid(),
            span_id: [span; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            nested_set: NestedSet {
                nested_set_left: left,
                nested_set_right: left + 1,
                parent_id: 0,
            },
            child_count: 0,
            root_service_name: Some("checkout".into()),
            root_span_name: Some("POST /pay".into()),
            trace_start_unix_nano: 1_000,
            trace_duration_nanos: 500,
            name: Some("db.query".into()),
            kind: SpanKind::Client,
            start_unix_nano: 1_100,
            duration_nanos: 50,
            status_code: StatusCode::Error,
            status_message: Some("timeout".into()),
            instrumentation_name: Some("tracer".into()),
            instrumentation_version: None,
            attrs: vec![SpanAttr {
                key: "http.method".into(),
                is_array: false,
                value: AttrValue::Str(vec!["GET".into()]),
            }],
            events: vec![SpanEvent {
                name: "exception".into(),
                time_since_start_nano: 10,
                attrs: vec![("exception.type".into(), "IOError".into())],
            }],
            links: vec![SpanLink {
                linked_trace_id: [2; 16],
                linked_span_id: [3; 8],
                attrs: vec![],
            }],
        }
    }

    #[test]
    fn encode_matches_schema_and_columns() {
        let rows = vec![sample_row(1, None, 1), sample_row(2, Some(1), 2)];
        let batch = encode_span_rows(&rows).unwrap();
        assert!(batch.schema() == span_block_schema());
        assert!(batch.num_rows() == 2);

        let tids = batch
            .column_by_name(SCOL_TRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!(tids.value(0) == [1_u8; 16]);

        let kinds = batch
            .column_by_name(SCOL_KIND)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(kinds.value(0) == SpanKind::Client.as_i32());

        let lefts = batch
            .column_by_name(SCOL_NESTED_SET_LEFT)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(lefts.value(1) == 2);
    }
}
