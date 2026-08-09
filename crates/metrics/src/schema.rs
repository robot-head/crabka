//! Arrow schemas for metric block types.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

/// Mandatory blockstore column for the series fingerprint (`UInt64`).
pub const COL_FINGERPRINT: &str = "series_fingerprint";
/// Mandatory blockstore column for the sample timestamp in epoch milliseconds
/// (`Int64`).
pub const COL_TIMESTAMP: &str = "timestamp";

/// Native histogram schema column (`Int8`).
pub const COL_NH_SCHEMA: &str = "schema";
/// Native histogram float/integer flavor column (`Boolean`).
pub const COL_NH_IS_FLOAT: &str = "is_float";
/// Native histogram reset hint column (`Int8`).
pub const COL_NH_RESET_HINT: &str = "reset_hint";
/// Native histogram zero threshold column (`Float64`).
pub const COL_NH_ZERO_THRESHOLD: &str = "zero_threshold";
/// Native histogram zero bucket count column (`Float64`).
pub const COL_NH_ZERO_COUNT: &str = "zero_count";
/// Native histogram total count column (`Float64`).
pub const COL_NH_COUNT: &str = "count";
/// Native histogram sum column (`Float64`).
pub const COL_NH_SUM: &str = "sum";
/// Native histogram positive bucket spans column.
pub const COL_NH_POS_SPANS: &str = "positive_spans";
/// Native histogram positive bucket counts column.
pub const COL_NH_POS_COUNTS: &str = "positive_counts";
/// Native histogram negative bucket spans column.
pub const COL_NH_NEG_SPANS: &str = "negative_spans";
/// Native histogram negative bucket counts column.
pub const COL_NH_NEG_COUNTS: &str = "negative_counts";
/// Native histogram custom bucket boundary values column.
pub const COL_NH_CUSTOM_VALUES: &str = "custom_values";
/// Native histogram start timestamp in epoch milliseconds column.
pub const COL_NH_START_TS: &str = "start_timestamp_ms";

fn fingerprint_field() -> Field {
    Field::new(COL_FINGERPRINT, DataType::UInt64, false)
}

fn timestamp_field() -> Field {
    Field::new(COL_TIMESTAMP, DataType::Int64, false)
}

fn f64_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float64, false)))
}

fn span_list_type() -> DataType {
    let struct_fields = Fields::from(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new("length", DataType::UInt32, false),
    ]);

    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(struct_fields),
        false,
    )))
}

fn utf8_map_field(name: &str, nullable: bool) -> Field {
    Field::new_map(
        name,
        "entries",
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        false,
        nullable,
    )
}

/// Float samples, which are counters, gauges, and classic histogram bucket
/// series.
#[must_use]
pub fn float_sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// Native histogram samples with absolute bucket counts.
#[must_use]
pub fn native_histogram_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new(COL_NH_SCHEMA, DataType::Int8, false),
        Field::new(COL_NH_IS_FLOAT, DataType::Boolean, false),
        Field::new(COL_NH_RESET_HINT, DataType::Int8, false),
        Field::new(COL_NH_ZERO_THRESHOLD, DataType::Float64, false),
        Field::new(COL_NH_ZERO_COUNT, DataType::Float64, false),
        Field::new(COL_NH_COUNT, DataType::Float64, false),
        Field::new(COL_NH_SUM, DataType::Float64, false),
        Field::new(COL_NH_POS_SPANS, span_list_type(), false),
        Field::new(COL_NH_POS_COUNTS, f64_list_type(), false),
        Field::new(COL_NH_NEG_SPANS, span_list_type(), false),
        Field::new(COL_NH_NEG_COUNTS, f64_list_type(), false),
        Field::new(COL_NH_CUSTOM_VALUES, f64_list_type(), true),
        Field::new(COL_NH_START_TS, DataType::Int64, true),
    ]))
}

/// Exemplars whose trace and span identifiers are first-class columns.
#[must_use]
pub fn exemplar_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("value", DataType::Float64, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        utf8_map_field("labels", false),
    ]))
}

/// Metric metadata rows used by the per-tenant metadata index.
#[must_use]
pub fn metadata_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("metric_family_name", DataType::Utf8, false),
        Field::new("metric_type", DataType::Utf8, false),
        Field::new("help", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn float_schema_has_mandatory_and_value() {
        let s = float_sample_schema();
        for (column, data_type) in [
            (COL_FINGERPRINT, DataType::UInt64),
            (COL_TIMESTAMP, DataType::Int64),
            ("value", DataType::Float64),
        ] {
            check!(
                s.column_with_name(column).unwrap().1.data_type() == &data_type,
                "column {column}",
            );
        }
    }

    #[test]
    fn native_histogram_span_columns_are_list_of_struct() {
        let s = native_histogram_schema();
        let (_, field) = s.column_with_name(COL_NH_POS_SPANS).unwrap();
        // List<Struct<offset:Int32, length:UInt32>>
        match field.data_type() {
            DataType::List(inner) => match inner.data_type() {
                DataType::Struct(fields) => {
                    assert!(fields.len() == 2);
                    check!(fields[0].name() == "offset");
                    check!(fields[1].name() == "length");
                }
                other => panic!("expected Struct, got {other:?}"),
            },
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn exemplar_schema_promotes_trace_and_span() {
        let s = exemplar_schema();
        assert!(s.column_with_name("trace_id").unwrap().1.data_type() == &DataType::Utf8);
        assert!(s.column_with_name("span_id").unwrap().1.data_type() == &DataType::Utf8);
    }

    #[test]
    fn metadata_schema_has_metric_metadata_columns() {
        let s = metadata_schema();
        for (column, data_type) in [
            (COL_FINGERPRINT, DataType::UInt64),
            (COL_TIMESTAMP, DataType::Int64),
            ("metric_family_name", DataType::Utf8),
            ("metric_type", DataType::Utf8),
            ("help", DataType::Utf8),
            ("unit", DataType::Utf8),
        ] {
            check!(
                s.column_with_name(column).unwrap().1.data_type() == &data_type,
                "column {column}",
            );
        }
    }
}
