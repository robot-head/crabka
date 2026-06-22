//! Flattened span-per-row block schema.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

use crate::block_index::{BlockSchema, RequiredColumn};

pub const SCOL_TRACE_ID: &str = "trace_id";
pub const SCOL_SPAN_ID: &str = "span_id";
pub const SCOL_PARENT_SPAN_ID: &str = "parent_span_id";
pub const SCOL_NESTED_SET_LEFT: &str = "nested_set_left";
pub const SCOL_NESTED_SET_RIGHT: &str = "nested_set_right";
pub const SCOL_PARENT_ID: &str = "parent_id";
pub const SCOL_CHILD_COUNT: &str = "child_count";
pub const SCOL_ROOT_SERVICE_NAME: &str = "root_service_name";
pub const SCOL_ROOT_SPAN_NAME: &str = "root_span_name";
pub const SCOL_TRACE_START_NANO: &str = "trace_start_unix_nano";
pub const SCOL_TRACE_DURATION_NANOS: &str = "trace_duration_nanos";
pub const SCOL_NAME: &str = "name";
pub const SCOL_KIND: &str = "kind";
pub const SCOL_START_NANO: &str = "start_unix_nano";
pub const SCOL_DURATION_NANOS: &str = "duration_nanos";
pub const SCOL_STATUS_CODE: &str = "status_code";
pub const SCOL_STATUS_MESSAGE: &str = "status_message";
pub const SCOL_INSTRUMENTATION_NAME: &str = "instrumentation_name";
pub const SCOL_INSTRUMENTATION_VERSION: &str = "instrumentation_version";
pub const SCOL_ATTR_KEYS: &str = "attr_keys";
pub const SCOL_ATTR_IS_ARRAY: &str = "attr_is_array";
pub const SCOL_ATTR_VALUE: &str = "attr_value";
pub const SCOL_ATTR_VALUE_INT: &str = "attr_value_int";
pub const SCOL_ATTR_VALUE_DOUBLE: &str = "attr_value_double";
pub const SCOL_ATTR_VALUE_BOOL: &str = "attr_value_bool";
pub const SCOL_PROMOTED_ATTR_PREFIX: &str = "attr.";
pub const SCOL_EVENTS: &str = "events";
pub const SCOL_LINKS: &str = "links";

/// A configured attribute column promoted out of the generic attribute lists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromotedSpanAttr {
    pub key: String,
    pub value_type: PromotedSpanAttrType,
}

impl PromotedSpanAttr {
    #[must_use]
    pub fn string(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value_type: PromotedSpanAttrType::String,
        }
    }

    #[must_use]
    pub fn int(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value_type: PromotedSpanAttrType::Int,
        }
    }

    #[must_use]
    pub fn double(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value_type: PromotedSpanAttrType::Double,
        }
    }

    #[must_use]
    pub fn bool(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value_type: PromotedSpanAttrType::Bool,
        }
    }

    #[must_use]
    pub fn column_name(&self) -> String {
        format!("{SCOL_PROMOTED_ATTR_PREFIX}{}", self.key)
    }

    #[must_use]
    pub fn data_type(&self) -> DataType {
        self.value_type.data_type()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromotedSpanAttrType {
    String,
    Int,
    Double,
    Bool,
}

impl PromotedSpanAttrType {
    #[must_use]
    pub fn data_type(self) -> DataType {
        match self {
            Self::String => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
            Self::Int => DataType::Int64,
            Self::Double => DataType::Float64,
            Self::Bool => DataType::Boolean,
        }
    }
}

/// OTLP span kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanKind {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        match self {
            SpanKind::Unspecified => 0,
            SpanKind::Internal => 1,
            SpanKind::Server => 2,
            SpanKind::Client => 3,
            SpanKind::Producer => 4,
            SpanKind::Consumer => 5,
        }
    }

    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => SpanKind::Internal,
            2 => SpanKind::Server,
            3 => SpanKind::Client,
            4 => SpanKind::Producer,
            5 => SpanKind::Consumer,
            _ => SpanKind::Unspecified,
        }
    }
}

/// OTLP status code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusCode {
    Unset,
    Ok,
    Error,
}

impl StatusCode {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        match self {
            StatusCode::Unset => 0,
            StatusCode::Ok => 1,
            StatusCode::Error => 2,
        }
    }

    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => StatusCode::Ok,
            2 => StatusCode::Error,
            _ => StatusCode::Unset,
        }
    }
}

fn list_of(name: &str, inner: DataType, nullable: bool) -> DataType {
    DataType::List(Arc::new(Field::new(name, inner, nullable)))
}

fn list_list_of(inner: DataType) -> DataType {
    list_of("item", list_of("item", inner, true), true)
}

fn event_struct() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("time_since_start_nano", DataType::Int64, true),
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
    ]))
}

fn link_struct() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
    ]))
}

/// The flattened span-per-row Arrow schema.
#[must_use]
pub fn span_block_schema() -> SchemaRef {
    span_block_schema_with_promoted_attrs(&[])
}

#[must_use]
pub fn span_block_schema_with_promoted_attrs(promoted_attrs: &[PromotedSpanAttr]) -> SchemaRef {
    let mut fields = vec![
        Field::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
        Field::new(SCOL_SPAN_ID, DataType::FixedSizeBinary(8), false),
        Field::new(SCOL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(SCOL_NESTED_SET_LEFT, DataType::Int32, false),
        Field::new(SCOL_NESTED_SET_RIGHT, DataType::Int32, false),
        Field::new(SCOL_PARENT_ID, DataType::Int32, false),
        Field::new(SCOL_CHILD_COUNT, DataType::Int32, false),
        Field::new(SCOL_ROOT_SERVICE_NAME, DataType::Utf8, true),
        Field::new(SCOL_ROOT_SPAN_NAME, DataType::Utf8, true),
        Field::new(SCOL_TRACE_START_NANO, DataType::Int64, false),
        Field::new(SCOL_TRACE_DURATION_NANOS, DataType::Int64, false),
        Field::new(SCOL_NAME, DataType::Utf8, true),
        Field::new(SCOL_KIND, DataType::Int32, false),
        Field::new(SCOL_START_NANO, DataType::Int64, false),
        Field::new(SCOL_DURATION_NANOS, DataType::Int64, false),
        Field::new(SCOL_STATUS_CODE, DataType::Int32, false),
        Field::new(SCOL_STATUS_MESSAGE, DataType::Utf8, true),
        Field::new(SCOL_INSTRUMENTATION_NAME, DataType::Utf8, true),
        Field::new(SCOL_INSTRUMENTATION_VERSION, DataType::Utf8, true),
    ];
    fields.extend(
        promoted_attrs
            .iter()
            .map(|attr| Field::new(attr.column_name(), attr.data_type(), true)),
    );
    fields.extend([
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(
            SCOL_ATTR_IS_ARRAY,
            list_of("item", DataType::Boolean, true),
            true,
        ),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
        Field::new(SCOL_ATTR_VALUE_INT, list_list_of(DataType::Int64), true),
        Field::new(
            SCOL_ATTR_VALUE_DOUBLE,
            list_list_of(DataType::Float64),
            true,
        ),
        Field::new(SCOL_ATTR_VALUE_BOOL, list_list_of(DataType::Boolean), true),
        Field::new(SCOL_EVENTS, list_of("item", event_struct(), true), true),
        Field::new(SCOL_LINKS, list_of("item", link_struct(), true), true),
    ]);
    Arc::new(Schema::new(fields))
}

/// Span block declaration used by generic schema validation.
#[must_use]
pub fn span_block_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            RequiredColumn::new(SCOL_START_NANO, DataType::Int64, false),
        ],
        sort_key: vec![SCOL_TRACE_ID.to_string(), SCOL_START_NANO.to_string()],
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::assert;

    use super::*;

    #[test]
    fn identity_columns_are_fixed_size_binary() {
        let s = span_block_schema();
        assert!(
            s.column_with_name(SCOL_TRACE_ID).unwrap().1.data_type()
                == &DataType::FixedSizeBinary(16)
        );
        assert!(
            s.column_with_name(SCOL_SPAN_ID).unwrap().1.data_type()
                == &DataType::FixedSizeBinary(8)
        );
        assert!(
            s.column_with_name(SCOL_PARENT_SPAN_ID)
                .unwrap()
                .1
                .data_type()
                == &DataType::FixedSizeBinary(8)
        );
    }

    #[test]
    fn nested_set_columns_are_int32() {
        let s = span_block_schema();
        for c in [
            SCOL_NESTED_SET_LEFT,
            SCOL_NESTED_SET_RIGHT,
            SCOL_PARENT_ID,
            SCOL_CHILD_COUNT,
        ] {
            assert!(s.column_with_name(c).unwrap().1.data_type() == &DataType::Int32);
        }
    }

    #[test]
    fn generic_attr_value_is_list_of_utf8() {
        let s = span_block_schema();
        let (_, f) = s.column_with_name(SCOL_ATTR_VALUE).unwrap();
        match f.data_type() {
            DataType::List(inner) => match inner.data_type() {
                DataType::List(scalar) => assert!(scalar.data_type() == &DataType::Utf8),
                other => panic!("expected List<List<Utf8>>, inner {other:?}"),
            },
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn events_and_links_are_list_of_struct() {
        let s = span_block_schema();
        for c in [SCOL_EVENTS, SCOL_LINKS] {
            let (_, f) = s.column_with_name(c).unwrap();
            match f.data_type() {
                DataType::List(inner) => assert!(matches!(inner.data_type(), DataType::Struct(_))),
                other => panic!("expected List<Struct>, got {other:?}"),
            }
        }
    }

    #[test]
    fn kind_and_status_enums_round_trip_i32() {
        for k in [
            SpanKind::Unspecified,
            SpanKind::Internal,
            SpanKind::Server,
            SpanKind::Client,
            SpanKind::Producer,
            SpanKind::Consumer,
        ] {
            assert!(SpanKind::from_i32(k.as_i32()) == k);
        }
        for s in [StatusCode::Unset, StatusCode::Ok, StatusCode::Error] {
            assert!(StatusCode::from_i32(s.as_i32()) == s);
        }
    }

    #[test]
    fn span_decl_sort_key_is_trace_id_then_start() {
        let d = span_block_decl();
        assert!(d.sort_key == vec![SCOL_TRACE_ID.to_string(), SCOL_START_NANO.to_string()]);
    }
}
