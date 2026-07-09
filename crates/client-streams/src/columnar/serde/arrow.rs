//! `ArrowIpcSerde`: Arrow-IPC stream encoding of an arrow-rs `RecordBatch`.

use std::collections::BTreeMap;

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, DictionaryArray, Float64Array,
        Float64Builder, Int32Array, Int64Array, Int64Builder, RecordBatch, RecordBatchOptions,
        StringArray, StringBuilder,
    },
    datatypes::{DataType, Field, Int32Type, Schema},
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use bytes::Bytes;
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::processor::serde::{DefaultSerde, Serde, SerdeAssociate, SerdeError};

/// `Serde<RecordBatch>` using the Arrow IPC stream format (schema embedded per message).
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrowIpcSerde;

impl Serde<RecordBatch> for ArrowIpcSerde {
    fn serialize(&self, _topic: &str, value: &RecordBatch) -> Bytes {
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &value.schema())
                .expect("arrow IPC StreamWriter init on in-memory buffer");
            writer.write(value).expect("arrow IPC write batch");
            writer.finish().expect("arrow IPC finish");
        }
        Bytes::from(buf)
    }

    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<RecordBatch, SerdeError> {
        let mut reader = StreamReader::try_new(bytes, None)
            .map_err(|e| SerdeError(format!("arrow IPC read: {e}")))?;
        match reader.next() {
            Some(Ok(batch)) => Ok(batch),
            Some(Err(e)) => Err(SerdeError(format!("arrow IPC decode: {e}"))),
            None => Err(SerdeError(
                "arrow IPC stream contained no record batch".into(),
            )),
        }
    }
}

impl SerdeAssociate for ArrowIpcSerde {
    type Target = RecordBatch;
}
impl DefaultSerde for RecordBatch {
    type Serde = ArrowIpcSerde;
}

/// Errors returned by the Arrow-to-filter-row bridge.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ArrowFilterRowBridgeError {
    /// The requested row is outside the batch bounds.
    #[error("arrow filter row {row} is out of bounds for batch with {row_count} rows")]
    RowOutOfBounds { row: usize, row_count: usize },
    /// The Arrow field uses a data type that the narrow scalar filter seam does not support.
    #[error("arrow field {field} has unsupported filter data type {data_type}")]
    UnsupportedDataType { field: String, data_type: DataType },
    /// The Arrow array's runtime type does not match its schema data type.
    #[error("arrow field {field} expected {data_type} array values")]
    UnexpectedArrayType { field: String, data_type: DataType },
    /// A dictionary key does not point at a dictionary value.
    #[error("arrow dictionary field {field} key {key} is out of bounds for {value_count} values")]
    DictionaryKeyOutOfBounds {
        field: String,
        key: i32,
        value_count: usize,
    },
    /// An integer enum dictionary needs descriptor symbols to resolve numbers to names.
    #[error(
        "arrow enum field {field} stores numeric values but has no crabka.enum.symbols descriptor metadata"
    )]
    EnumSymbolsMissing { field: String },
    /// Enum descriptor metadata is not a JSON object mapping numbers to names.
    #[error("arrow enum field {field} has invalid crabka.enum.symbols metadata")]
    InvalidEnumSymbols { field: String },
    /// A decoded schema-registry JSON record contains an object or array shape the bridge cannot map safely.
    #[error("schema-registry row bridge field {field} has unsupported JSON value {value_kind}")]
    UnsupportedJsonValue { field: String, value_kind: String },
    /// A decoded schema-registry JSON column changes type across rows.
    #[error("schema-registry row bridge field {field} mixes incompatible JSON scalar types")]
    MixedJsonTypes { field: String },
    /// A decoded schema-registry JSON value cannot be represented in Arrow.
    #[error("schema-registry row bridge failed to build Arrow batch: {message}")]
    BuildRecordBatch { message: String },
}

/// Converts schema-registry decoded JSON rows to an Arrow batch for `DataFusion` filtering.
///
/// Nested objects are flattened into dotted field names (`customer.status`).
/// Repeated objects/scalars are flattened by explicit zero-based index
/// (`items[0].price`). Delivered records still use the original broker bytes;
/// this batch is only an evaluation surface.
pub fn json_rows_to_arrow_filter_batch(
    rows: &[Value],
) -> Result<RecordBatch, ArrowFilterRowBridgeError> {
    let mut flattened_rows = Vec::with_capacity(rows.len());
    let mut fields = BTreeMap::new();
    for row in rows {
        let mut flattened = BTreeMap::new();
        flatten_json_value("", row, &mut flattened)?;
        for (field, value) in &flattened {
            fields
                .entry(field.clone())
                .and_modify(|data_type| merge_json_scalar_type(field, data_type, value))
                .or_insert_with(|| JsonScalarType::from_value(value));
        }
        flattened_rows.push(flattened);
    }

    let columns = fields
        .iter()
        .map(|(field, data_type)| build_json_column(field, *data_type, &flattened_rows))
        .collect::<Result<Vec<_>, _>>()?;
    let schema_fields = fields
        .iter()
        .map(|(name, data_type)| Field::new(name, data_type.arrow_data_type(), true))
        .collect::<Vec<_>>();

    let schema = Schema::new(schema_fields);
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(std::sync::Arc::new(schema), columns, &options).map_err(
        |error| ArrowFilterRowBridgeError::BuildRecordBatch {
            message: error.to_string(),
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonScalarType {
    Null,
    Bool,
    Int64,
    Float64,
    Utf8,
}

impl JsonScalarType {
    fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Bool,
            Value::Number(number) if number.as_i64().is_some() => Self::Int64,
            Value::Number(_) => Self::Float64,
            Value::String(_) | Value::Array(_) | Value::Object(_) => Self::Utf8,
        }
    }

    const fn arrow_data_type(self) -> DataType {
        match self {
            Self::Null | Self::Utf8 => DataType::Utf8,
            Self::Bool => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
        }
    }
}

fn flatten_json_value(
    path: &str,
    value: &Value,
    out: &mut BTreeMap<String, Value>,
) -> Result<(), ArrowFilterRowBridgeError> {
    match value {
        Value::Object(object) => {
            if object.is_empty() && !path.is_empty() {
                return Err(unsupported_json_value(path, "empty object"));
            }
            for (name, nested) in object {
                let nested_path = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}.{name}")
                };
                flatten_json_value(&nested_path, nested, out)?;
            }
            Ok(())
        }
        Value::Array(values) => {
            if path.is_empty() {
                return Err(unsupported_json_value(path, "top-level array"));
            }
            for (index, nested) in values.iter().enumerate() {
                flatten_json_value(&format!("{path}[{index}]"), nested, out)?;
            }
            Ok(())
        }
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {
            if path.is_empty() {
                return Err(unsupported_json_value(path, "top-level scalar"));
            }
            out.insert(path.to_string(), value.clone());
            Ok(())
        }
    }
}

fn merge_json_scalar_type(field: &str, data_type: &mut JsonScalarType, value: &Value) {
    let next = JsonScalarType::from_value(value);
    *data_type = match (*data_type, next) {
        (current, JsonScalarType::Null) => current,
        (JsonScalarType::Null, next) => next,
        (JsonScalarType::Int64, JsonScalarType::Float64)
        | (JsonScalarType::Float64, JsonScalarType::Int64) => JsonScalarType::Float64,
        (left, right) if left == right => left,
        _ => JsonScalarType::Utf8,
    };
    let _ = field;
}

fn build_json_column(
    field: &str,
    data_type: JsonScalarType,
    rows: &[BTreeMap<String, Value>],
) -> Result<ArrayRef, ArrowFilterRowBridgeError> {
    match data_type {
        JsonScalarType::Null | JsonScalarType::Utf8 => build_string_json_column(field, rows),
        JsonScalarType::Bool => build_bool_json_column(field, rows),
        JsonScalarType::Int64 => build_int64_json_column(field, rows),
        JsonScalarType::Float64 => build_float64_json_column(field, rows),
    }
}

fn build_string_json_column(
    field: &str,
    rows: &[BTreeMap<String, Value>],
) -> Result<ArrayRef, ArrowFilterRowBridgeError> {
    let mut builder = StringBuilder::new();
    for row in rows {
        match row.get(field) {
            None | Some(Value::Null) => builder.append_null(),
            Some(Value::String(value)) => builder.append_value(value),
            Some(Value::Number(value)) => builder.append_value(value.to_string()),
            Some(Value::Bool(value)) => builder.append_value(value.to_string()),
            Some(Value::Array(_) | Value::Object(_)) => {
                return Err(unsupported_json_value(field, "nested value"));
            }
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_bool_json_column(
    field: &str,
    rows: &[BTreeMap<String, Value>],
) -> Result<ArrayRef, ArrowFilterRowBridgeError> {
    let mut builder = BooleanBuilder::new();
    for row in rows {
        match row.get(field) {
            None | Some(Value::Null) => builder.append_null(),
            Some(Value::Bool(value)) => builder.append_value(*value),
            Some(_) => {
                return Err(ArrowFilterRowBridgeError::MixedJsonTypes {
                    field: field.to_string(),
                });
            }
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_int64_json_column(
    field: &str,
    rows: &[BTreeMap<String, Value>],
) -> Result<ArrayRef, ArrowFilterRowBridgeError> {
    let mut builder = Int64Builder::new();
    for row in rows {
        match row.get(field) {
            None | Some(Value::Null) => builder.append_null(),
            Some(Value::Number(value)) => {
                let Some(value) = value.as_i64() else {
                    return Err(ArrowFilterRowBridgeError::MixedJsonTypes {
                        field: field.to_string(),
                    });
                };
                builder.append_value(value);
            }
            Some(_) => {
                return Err(ArrowFilterRowBridgeError::MixedJsonTypes {
                    field: field.to_string(),
                });
            }
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_float64_json_column(
    field: &str,
    rows: &[BTreeMap<String, Value>],
) -> Result<ArrayRef, ArrowFilterRowBridgeError> {
    let mut builder = Float64Builder::new();
    for row in rows {
        match row.get(field) {
            None | Some(Value::Null) => builder.append_null(),
            Some(Value::Number(value)) => {
                let Some(value) = value.as_f64().filter(|value| value.is_finite()) else {
                    return Err(ArrowFilterRowBridgeError::MixedJsonTypes {
                        field: field.to_string(),
                    });
                };
                builder.append_value(value);
            }
            Some(_) => {
                return Err(ArrowFilterRowBridgeError::MixedJsonTypes {
                    field: field.to_string(),
                });
            }
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn unsupported_json_value(field: &str, value_kind: &str) -> ArrowFilterRowBridgeError {
    ArrowFilterRowBridgeError::UnsupportedJsonValue {
        field: field.to_string(),
        value_kind: value_kind.to_string(),
    }
}

/// Converts an Arrow [`RecordBatch`] into JSON rows accepted by the gateway filter seam.
pub fn arrow_batch_to_filter_json_rows(
    batch: &RecordBatch,
) -> Result<Vec<Value>, ArrowFilterRowBridgeError> {
    (0..batch.num_rows())
        .map(|row| arrow_row_to_filter_json(batch, row))
        .collect()
}

/// Converts one Arrow row into the JSON shape accepted by the gateway filter seam.
pub fn arrow_row_to_filter_json(
    batch: &RecordBatch,
    row: usize,
) -> Result<Value, ArrowFilterRowBridgeError> {
    if row >= batch.num_rows() {
        return Err(ArrowFilterRowBridgeError::RowOutOfBounds {
            row,
            row_count: batch.num_rows(),
        });
    }

    let mut row_value = Map::new();
    for (column_index, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(column_index);
        let value = arrow_cell_to_filter_json(field, column, row)?;
        row_value.insert(field.name().clone(), value);
    }
    Ok(Value::Object(row_value))
}

fn arrow_cell_to_filter_json(
    field: &Field,
    column: &ArrayRef,
    row: usize,
) -> Result<Value, ArrowFilterRowBridgeError> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }

    match field.data_type() {
        DataType::Utf8 => string_cell(field, column, row).map(Value::String),
        DataType::Int32 => int32_cell(field, column, row).map(|value| json!(value)),
        DataType::Int64 => int64_cell(field, column, row).map(|value| json!(value)),
        DataType::Float64 => float64_cell(field, column, row).map(|value| json!(value)),
        DataType::Boolean => bool_cell(field, column, row).map(Value::Bool),
        DataType::Dictionary(_, _) => dictionary_cell_to_filter_json(field, column, row),
        data_type => Err(unsupported_data_type(field, data_type)),
    }
}

fn dictionary_cell_to_filter_json(
    field: &Field,
    column: &ArrayRef,
    row: usize,
) -> Result<Value, ArrowFilterRowBridgeError> {
    let dictionary = column
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| ArrowFilterRowBridgeError::UnexpectedArrayType {
            field: field.name().clone(),
            data_type: field.data_type().clone(),
        })?;
    let key = dictionary.keys().value(row);
    let value_index =
        usize::try_from(key).map_err(|_| ArrowFilterRowBridgeError::DictionaryKeyOutOfBounds {
            field: field.name().clone(),
            key,
            value_count: dictionary.values().len(),
        })?;
    if value_index >= dictionary.values().len() {
        return Err(ArrowFilterRowBridgeError::DictionaryKeyOutOfBounds {
            field: field.name().clone(),
            key,
            value_count: dictionary.values().len(),
        });
    }

    let value = dictionary_value_to_filter_json(field, dictionary.values(), value_index)?;
    if is_enum_field(field) {
        return Ok(json!({
            "$type": "enum",
            "key": key,
            "name": value,
        }));
    }

    Ok(json!({
        "$type": "dictionary",
        "key": key,
        "value": value,
    }))
}

fn dictionary_value_to_filter_json(
    field: &Field,
    values: &ArrayRef,
    value_index: usize,
) -> Result<Value, ArrowFilterRowBridgeError> {
    if values.is_null(value_index) {
        return Ok(Value::Null);
    }

    match values.data_type() {
        DataType::Utf8 => string_cell(field, values, value_index).map(Value::String),
        DataType::Int32 if is_enum_field(field) => int32_cell(field, values, value_index)
            .and_then(|number| enum_symbol_name(field, i64::from(number)))
            .map(Value::String),
        DataType::Int32 => int32_cell(field, values, value_index).map(|number| json!(number)),
        DataType::Int64 if is_enum_field(field) => int64_cell(field, values, value_index)
            .and_then(|number| enum_symbol_name(field, number))
            .map(Value::String),
        DataType::Int64 => int64_cell(field, values, value_index).map(|number| json!(number)),
        DataType::Float64 => float64_cell(field, values, value_index).map(|number| json!(number)),
        DataType::Boolean => bool_cell(field, values, value_index).map(Value::Bool),
        data_type => Err(unsupported_data_type(field, data_type)),
    }
}

fn is_enum_field(field: &Field) -> bool {
    field
        .metadata()
        .get("crabka.enum")
        .is_some_and(|value| value == "true")
}

fn enum_symbol_name(field: &Field, number: i64) -> Result<String, ArrowFilterRowBridgeError> {
    let Some(symbols) = field.metadata().get("crabka.enum.symbols") else {
        return Err(ArrowFilterRowBridgeError::EnumSymbolsMissing {
            field: field.name().clone(),
        });
    };
    let parsed: Value = serde_json::from_str(symbols).map_err(|_| {
        ArrowFilterRowBridgeError::InvalidEnumSymbols {
            field: field.name().clone(),
        }
    })?;
    let Some(object) = parsed.as_object() else {
        return Err(ArrowFilterRowBridgeError::InvalidEnumSymbols {
            field: field.name().clone(),
        });
    };

    Ok(object
        .get(&number.to_string())
        .and_then(Value::as_str)
        .map_or_else(|| format!("UNKNOWN_{number}"), ToString::to_string))
}

fn string_cell(
    field: &Field,
    column: &ArrayRef,
    row: usize,
) -> Result<String, ArrowFilterRowBridgeError> {
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| array.value(row).to_string())
        .ok_or_else(|| unexpected_array_type(field))
}

fn int32_cell(
    field: &Field,
    column: &ArrayRef,
    row: usize,
) -> Result<i32, ArrowFilterRowBridgeError> {
    column
        .as_any()
        .downcast_ref::<Int32Array>()
        .map(|array| array.value(row))
        .ok_or_else(|| unexpected_array_type(field))
}

fn int64_cell(
    field: &Field,
    column: &ArrayRef,
    row: usize,
) -> Result<i64, ArrowFilterRowBridgeError> {
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .map(|array| array.value(row))
        .ok_or_else(|| unexpected_array_type(field))
}

fn float64_cell(
    field: &Field,
    column: &ArrayRef,
    row: usize,
) -> Result<f64, ArrowFilterRowBridgeError> {
    let value = column
        .as_any()
        .downcast_ref::<Float64Array>()
        .map(|array| array.value(row))
        .ok_or_else(|| unexpected_array_type(field))?;
    if !value.is_finite() {
        return Err(ArrowFilterRowBridgeError::UnsupportedDataType {
            field: field.name().clone(),
            data_type: field.data_type().clone(),
        });
    }
    Ok(value)
}

fn bool_cell(
    field: &Field,
    column: &ArrayRef,
    row: usize,
) -> Result<bool, ArrowFilterRowBridgeError> {
    column
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(|array| array.value(row))
        .ok_or_else(|| unexpected_array_type(field))
}

fn unsupported_data_type(field: &Field, data_type: &DataType) -> ArrowFilterRowBridgeError {
    ArrowFilterRowBridgeError::UnsupportedDataType {
        field: field.name().clone(),
        data_type: data_type.clone(),
    }
}

fn unexpected_array_type(field: &Field) -> ArrowFilterRowBridgeError {
    ArrowFilterRowBridgeError::UnexpectedArrayType {
        field: field.name().clone(),
        data_type: field.data_type().clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{BooleanArray, DictionaryArray, Float64Array, Int32Array, StringDictionaryBuilder},
        datatypes::{DataType, Field, Schema},
    };
    use assert2::check;
    use serde_json::json;

    use super::*;

    fn sample() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("total", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Float64Array::from(vec![1.0_f64, 2.5])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn arrow_ipc_round_trips() {
        let s = ArrowIpcSerde;
        let batch = sample();
        let bytes = s.serialize("t", &batch);
        let back = s.deserialize("t", &bytes).unwrap();
        check!(back.num_rows() == batch.num_rows());
        check!(back.num_columns() == batch.num_columns());
        check!(back.schema() == batch.schema());
        check!(back == batch);
    }

    #[test]
    fn arrow_ipc_is_readable_by_standalone_stream_reader() {
        // Cross-reader portability: the bytes parse as a standalone Arrow IPC stream.
        let s = ArrowIpcSerde;
        let bytes = s.serialize("t", &sample());
        let reader = StreamReader::try_new(&bytes[..], None).unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        check!(batches.len() == 1);
        check!(batches[0].num_rows() == 2);
    }

    #[test]
    fn arrow_ipc_rejects_garbage() {
        let s = ArrowIpcSerde;
        check!(s.deserialize("t", b"not-ipc").is_err());
    }

    #[test]
    fn arrow_ipc_round_trips_enum_dictionary_values_unknowns_and_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let mut statuses = StringDictionaryBuilder::<Int32Type>::new();
        statuses.append_value("NETWORK_NODE");
        statuses.append_value("UNKNOWN_7");
        statuses.append_null();
        statuses.append_value("UNKNOWN_7");
        let batch = RecordBatch::try_new(schema, vec![Arc::new(statuses.finish())]).unwrap();

        let serde = ArrowIpcSerde;
        let back = serde
            .deserialize("enum-status", &serde.serialize("enum-status", &batch))
            .unwrap();
        let status = back
            .column_by_name("status")
            .expect("status column")
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("status stays dictionary encoded");
        let values = status
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dictionary values are strings");

        let decoded = (0..status.len())
            .map(|row| {
                if status.is_null(row) {
                    return None;
                }

                let key = usize::try_from(status.keys().value(row)).expect("non-negative key");
                Some(values.value(key))
            })
            .collect::<Vec<_>>();

        assert_eq!(
            decoded,
            vec![
                Some("NETWORK_NODE"),
                Some("UNKNOWN_7"),
                None,
                Some("UNKNOWN_7")
            ]
        );
    }

    #[test]
    fn arrow_filter_row_bridge_converts_dictionary_scalars_nulls_and_unknown_enums() {
        let mut enum_field = Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
            true,
        );
        enum_field.set_metadata(std::collections::HashMap::from([
            ("crabka.enum".to_string(), "true".to_string()),
            (
                "crabka.enum.symbols".to_string(),
                r#"{"1":"NETWORK_NODE"}"#.to_string(),
            ),
        ]));
        let schema = Arc::new(Schema::new(vec![
            enum_field,
            Field::new(
                "profile_type",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("priority", DataType::Float64, true),
            Field::new("deleted", DataType::Boolean, true),
        ]));
        let statuses = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), None]),
            Arc::new(Int32Array::from(vec![1, 7])),
        )
        .unwrap();
        let mut profile_types = StringDictionaryBuilder::<Int32Type>::new();
        profile_types.append_value("cpu");
        profile_types.append_value("disk");
        profile_types.append_null();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(statuses),
                Arc::new(profile_types.finish()),
                Arc::new(Float64Array::from(vec![Some(9.0), Some(4.0), None])),
                Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
            ],
        )
        .unwrap();

        let rows = arrow_batch_to_filter_json_rows(&batch).unwrap();

        assert_eq!(
            rows,
            vec![
                json!({
                    "status": {"$type": "enum", "key": 0, "name": "NETWORK_NODE"},
                    "profile_type": {"$type": "dictionary", "key": 0, "value": "cpu"},
                    "priority": 9.0,
                    "deleted": false,
                }),
                json!({
                    "status": {"$type": "enum", "key": 1, "name": "UNKNOWN_7"},
                    "profile_type": {"$type": "dictionary", "key": 1, "value": "disk"},
                    "priority": 4.0,
                    "deleted": null,
                }),
                json!({
                    "status": null,
                    "profile_type": null,
                    "priority": null,
                    "deleted": true,
                }),
            ]
        );
    }

    #[test]
    fn arrow_filter_row_bridge_requires_symbols_for_numeric_enum_dictionaries() {
        let mut enum_field = Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
            true,
        );
        enum_field.set_metadata(std::collections::HashMap::from([(
            "crabka.enum".to_string(),
            "true".to_string(),
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![enum_field])),
            vec![Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(0)]),
                    Arc::new(Int32Array::from(vec![1])),
                )
                .unwrap(),
            )],
        )
        .unwrap();

        let error = arrow_batch_to_filter_json_rows(&batch)
            .expect_err("numeric enum dictionaries need descriptor symbols");

        assert_eq!(
            error,
            ArrowFilterRowBridgeError::EnumSymbolsMissing {
                field: "status".to_string(),
            }
        );
    }

    #[test]
    fn arrow_filter_row_bridge_rejects_nested_and_repeated_columns_loudly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ACTIVE"])),
                Arc::new(::arrow::array::ListArray::from_iter_primitive::<
                    ::arrow::datatypes::Int32Type,
                    _,
                    _,
                >(vec![Some(vec![
                    Some(1_i32),
                    Some(2_i32),
                ])])),
            ],
        )
        .unwrap();

        let error =
            arrow_batch_to_filter_json_rows(&batch).expect_err("list columns are unsupported");

        assert_eq!(
            error,
            ArrowFilterRowBridgeError::UnsupportedDataType {
                field: "tags".to_string(),
                data_type: DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            }
        );
    }

    #[test]
    fn arrow_filter_row_bridge_flattens_schema_registry_nested_and_repeated_rows() {
        let rows = vec![
            json!({
                "customer": {"status": "ACTIVE"},
                "items": [{"price": 125}, {"price": 5}],
                "status": "PAID"
            }),
            json!({
                "customer": {"status": "INACTIVE"},
                "items": [{"price": 25}],
                "status": "PENDING"
            }),
        ];

        let batch = json_rows_to_arrow_filter_batch(&rows).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert!(batch.column_by_name("customer.status").is_some());
        assert!(batch.column_by_name("items[0].price").is_some());
        assert!(batch.column_by_name("items[1].price").is_some());
        assert_eq!(
            batch
                .schema()
                .field_with_name("items[0].price")
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn arrow_filter_row_bridge_preserves_row_count_for_empty_objects() {
        let rows = vec![json!({}), json!({})];

        let batch = json_rows_to_arrow_filter_batch(&rows).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 0);
    }

    #[test]
    fn arrow_filter_row_bridge_preserves_row_count_for_empty_arrays_only() {
        let rows = vec![json!({"items": []}), json!({"items": []})];

        let batch = json_rows_to_arrow_filter_batch(&rows).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 0);
    }
}
