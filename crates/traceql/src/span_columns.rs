//! `TraceQL` span column names and structural interval helpers.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::result::AttrValue;

pub const COL_TRACE_ID: &str = "trace_id";
pub const COL_SPAN_ID: &str = "span_id";
pub const COL_PARENT_SPAN_ID: &str = "parent_span_id";
pub const COL_NS_LEFT: &str = "nested_set_left";
pub const COL_NS_RIGHT: &str = "nested_set_right";
pub const COL_PARENT_ID: &str = "parent_id";
pub const COL_ROOT_SERVICE_NAME: &str = "root_service_name";
pub const COL_ROOT_SPAN_NAME: &str = "root_span_name";
pub const COL_TRACE_START: &str = "trace_start_unix_nano";
pub const COL_TRACE_DURATION: &str = "trace_duration_nanos";
pub const COL_NAME: &str = "name";
pub const COL_KIND: &str = "kind";
pub const COL_START: &str = "start_unix_nano";
pub const COL_DURATION: &str = "duration_nanos";
pub const COL_STATUS_CODE: &str = "status_code";
pub const COL_STATUS_MESSAGE: &str = "status_message";
pub const ATTR_PREFIX: &str = "attr.";

#[derive(Clone, Debug, PartialEq)]
pub struct InputSpan {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: i32,
    pub start_unix_nano: i64,
    pub duration_nanos: i64,
    pub status_code: i32,
    pub status_message: String,
    pub attrs: Vec<(String, AttrValue)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NestedSet {
    pub left: i32,
    pub right: i32,
    pub parent_id: i32,
}

#[must_use]
pub fn span_schema() -> SchemaRef {
    span_schema_with_attrs(&[])
}

#[must_use]
pub fn span_schema_with_attrs(attr_cols: &[(String, DataType)]) -> SchemaRef {
    let mut fields = vec![
        Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
        Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
        Field::new(COL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(COL_NS_LEFT, DataType::Int32, false),
        Field::new(COL_NS_RIGHT, DataType::Int32, false),
        Field::new(COL_PARENT_ID, DataType::Int32, false),
        Field::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
        Field::new(COL_ROOT_SPAN_NAME, DataType::Utf8, true),
        Field::new(COL_TRACE_START, DataType::Int64, false),
        Field::new(COL_TRACE_DURATION, DataType::Int64, false),
        Field::new(COL_NAME, DataType::Utf8, true),
        Field::new(COL_KIND, DataType::Int32, false),
        Field::new(COL_START, DataType::Int64, false),
        Field::new(COL_DURATION, DataType::Int64, false),
        Field::new(COL_STATUS_CODE, DataType::Int32, false),
        Field::new(COL_STATUS_MESSAGE, DataType::Utf8, true),
    ];

    fields.extend(
        attr_cols
            .iter()
            .map(|(key, dt)| Field::new(format!("{ATTR_PREFIX}{key}"), dt.clone(), true)),
    );

    Arc::new(Schema::new(fields))
}

#[must_use]
pub fn assign_nested_set(spans: &[InputSpan]) -> Vec<NestedSet> {
    enum Frame {
        Enter { idx: usize, parent_left: i32 },
        Exit { idx: usize },
    }

    let pos: HashMap<[u8; 8], usize> = spans
        .iter()
        .enumerate()
        .map(|(i, span)| (span.span_id, i))
        .collect();
    let mut children = vec![Vec::new(); spans.len()];
    let mut roots = Vec::new();

    for (i, span) in spans.iter().enumerate() {
        match span.parent_span_id.and_then(|p| pos.get(&p).copied()) {
            Some(parent_idx) if parent_idx != i => children[parent_idx].push(i),
            _ => roots.push(i),
        }
    }

    let mut out = vec![
        NestedSet {
            left: 0,
            right: 0,
            parent_id: 0,
        };
        spans.len()
    ];
    let mut counter = 1_i32;
    let mut stack = Vec::new();

    for &root in roots.iter().rev() {
        stack.push(Frame::Enter {
            idx: root,
            parent_left: 0,
        });
    }

    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter { idx, parent_left } => {
                let left = counter;
                counter += 1;
                out[idx].left = left;
                out[idx].parent_id = parent_left;
                stack.push(Frame::Exit { idx });
                for &child in children[idx].iter().rev() {
                    stack.push(Frame::Enter {
                        idx: child,
                        parent_left: left,
                    });
                }
            }
            Frame::Exit { idx } => {
                out[idx].right = counter;
                counter += 1;
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use assert2::assert;

    fn sid(n: u8) -> [u8; 8] {
        [n, 0, 0, 0, 0, 0, 0, 0]
    }

    fn span(id: u8, parent: Option<u8>) -> InputSpan {
        InputSpan {
            trace_id: [7; 16],
            span_id: sid(id),
            parent_span_id: parent.map(sid),
            name: format!("span-{id}"),
            kind: 0,
            start_unix_nano: i64::from(id) * 100,
            duration_nanos: 10,
            status_code: 0,
            status_message: String::new(),
            attrs: vec![],
        }
    }

    fn idx(spans: &[InputSpan], id: u8) -> usize {
        spans.iter().position(|s| s.span_id == sid(id)).unwrap()
    }

    #[test]
    fn schema_contains_traceql_planning_columns() {
        let schema = span_schema();
        assert!(
            schema.column_with_name(COL_TRACE_ID).unwrap().1.data_type()
                == &DataType::FixedSizeBinary(16)
        );
        assert!(schema.column_with_name(COL_NAME).unwrap().1.data_type() == &DataType::Utf8);
        assert!(schema.column_with_name(COL_NS_LEFT).unwrap().1.data_type() == &DataType::Int32);
        assert!(
            schema
                .column_with_name(COL_TRACE_DURATION)
                .unwrap()
                .1
                .data_type()
                == &DataType::Int64
        );
    }

    #[test]
    fn attr_prefix_matches_tempo_virtual_attribute_shape() {
        assert!(ATTR_PREFIX == "attr.");
    }

    #[test]
    fn nested_set_parent_id_is_parent_left() {
        let spans = vec![
            span(1, None),
            span(2, Some(1)),
            span(3, Some(1)),
            span(4, Some(3)),
        ];
        let ns = assign_nested_set(&spans);
        let root_left = ns[idx(&spans, 1)].left;
        assert!(ns[idx(&spans, 1)].parent_id == 0);
        assert!(ns[idx(&spans, 2)].parent_id == root_left);
        assert!(ns[idx(&spans, 3)].parent_id == root_left);
        assert!(ns[idx(&spans, 4)].parent_id == ns[idx(&spans, 3)].left);
    }

    #[test]
    fn nested_set_intervals_identify_ancestors() {
        let spans = vec![
            span(1, None),
            span(2, Some(1)),
            span(3, Some(1)),
            span(4, Some(3)),
        ];
        let ns = assign_nested_set(&spans);
        let root = ns[idx(&spans, 1)];
        let peer = ns[idx(&spans, 2)];
        let parent = ns[idx(&spans, 3)];
        let child = ns[idx(&spans, 4)];

        assert!(root.left < child.left && child.right < root.right);
        assert!(parent.left < child.left && child.right < parent.right);
        assert!(!(peer.left < child.left && child.right < peer.right));
    }

    #[test]
    fn orphan_parent_is_treated_as_root() {
        let spans = vec![span(9, Some(99))];
        let ns = assign_nested_set(&spans);
        assert!(ns[0].parent_id == 0);
        assert!(ns[0].left == 1);
        assert!(ns[0].right == 2);
    }
}
