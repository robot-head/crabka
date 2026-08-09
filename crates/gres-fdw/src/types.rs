//! Schema→column and `Value`→`Datum` type mapping for the Kafka FDW.
//!
//! Three entry-points:
//! - `avro_schema_to_columns`, `json_schema_to_columns`, and
//!   `protobuf_message_to_columns` derive the logical column list from a
//!   writer schema or a message descriptor.
//! - `project` maps a decoded value to `Vec<Datum>` in column order.

use std::fmt::Write as _;

use apache_avro::{
    Schema as AvroSchema,
    schema::{DecimalSchema, RecordSchema, UnionSchema},
    types::Value as AvroValue,
};
use crabka_pgcatalog::Column;
use crabka_pgtypes::{ColumnType, Datum};
use prost_reflect::{Kind, Value as ProtoValue};
use serde_json::Value as JsonValue;

use crate::decode::DecodedValue;

// ---------------------------------------------------------------------------
// Avro epoch helpers
// ---------------------------------------------------------------------------

/// Unix epoch (1970-01-01) as a `jiff` civil date. It converts an Avro
/// `Date`, which is days since the Unix epoch, to a `jiff::civil::Date`.
fn unix_epoch_date() -> jiff::civil::Date {
    jiff::civil::Date::constant(1970, 1, 1)
}

// ---------------------------------------------------------------------------
// Avro schema → ColumnType
// ---------------------------------------------------------------------------

/// Maps an Avro schema node to a `ColumnType`. Returns `None` for types this
/// crate cannot represent, such as unsupported unions, enums, and fixed.
fn avro_to_column_type(schema: &AvroSchema) -> Option<ColumnType> {
    match schema {
        AvroSchema::Null => None,
        AvroSchema::Boolean => Some(ColumnType::Bool),
        AvroSchema::Int => Some(ColumnType::Int4),
        AvroSchema::Long => Some(ColumnType::Int8),
        // Both `float` and `double` map to Float8 (see the mapping rules in
        // Component D). `Float4` now exists as a scalar type, so narrowing the
        // Avro `float` case onto it is a deliberate open question for the FDW
        // mapping wave rather than an absent capability.
        AvroSchema::Float | AvroSchema::Double => Some(ColumnType::Float8),
        AvroSchema::Bytes => Some(ColumnType::Bytea),
        // Logical timestamp types — both millis and micros → Timestamptz.
        AvroSchema::TimestampMillis | AvroSchema::TimestampMicros => Some(ColumnType::Timestamptz),
        AvroSchema::Date => Some(ColumnType::Date),
        // Decimal → unconstrained Numeric.
        AvroSchema::Decimal(_) => Some(ColumnType::Numeric(None)),
        // Nullable union `[null, T]` → the type of T (nullable column).
        AvroSchema::Union(u) => nullable_union_type(u),
        // Everything else (String, Enum, Fixed, nested complex types,
        // LocalTimestamp*, BigDecimal, Ref, …) → Text
        // as a safe catch-all so the schema walk never silently drops a field.
        _ => Some(ColumnType::Text),
    }
}

/// Returns the type of `T` if `u` is exactly `[null, T]` or `[T, null]`.
/// Any other union shape gives `None`.
fn nullable_union_type(u: &UnionSchema) -> Option<ColumnType> {
    let variants = u.variants();
    if variants.len() != 2 {
        return None;
    }
    let ((AvroSchema::Null, non_null) | (non_null, AvroSchema::Null)) =
        (&variants[0], &variants[1])
    else {
        return None;
    };
    avro_to_column_type(non_null)
}

/// Derives the column list from an Avro Record schema.
///
/// This function expects only top-level Record schemas. A non-Record schema,
/// such as a primitive or a union, returns an empty list.
#[must_use]
pub fn avro_schema_to_columns(schema: &AvroSchema) -> Vec<Column> {
    let AvroSchema::Record(RecordSchema { fields, .. }) = schema else {
        return Vec::new();
    };
    fields
        .iter()
        .filter_map(|f| avro_to_column_type(&f.schema).map(|ty| Column::new(f.name.clone(), ty)))
        .collect()
}

// ---------------------------------------------------------------------------
// JSON schema → ColumnType
// ---------------------------------------------------------------------------

/// Maps a JSON Schema `type` string to a `ColumnType`.
fn json_type_to_column_type(type_str: &str) -> ColumnType {
    match type_str {
        "boolean" => ColumnType::Bool,
        "integer" => ColumnType::Int8,
        "number" => ColumnType::Float8,
        _ => ColumnType::Text,
    }
}

/// Derives columns from a JSON Schema object (draft-04 / Confluent subset).
///
/// The function inspects the top-level `"properties"` map and maps the
/// `"type"` of each property to a `ColumnType`.
#[must_use]
pub fn json_schema_to_columns(schema: &JsonValue) -> Vec<Column> {
    let Some(props) = schema.get("properties").and_then(|p| p.as_object()) else {
        return Vec::new();
    };
    props
        .iter()
        .map(|(name, prop)| {
            let ty = prop
                .get("type")
                .and_then(|t| t.as_str())
                .map_or(ColumnType::Text, json_type_to_column_type);
            Column::new(name.clone(), ty)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Protobuf schema → ColumnType
// ---------------------------------------------------------------------------

/// Maps a Protobuf field [`Kind`] to a [`ColumnType`].
///
/// The call site handles repeated and map fields. This function maps the
/// scalar kind only. A complex type, that is a message, falls back to
/// JSON-serialised `Text`, so no field is silently dropped.
fn proto_kind_to_column_type(kind: &Kind) -> ColumnType {
    match kind {
        // Signed 32-bit variants
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 | Kind::Enum(_) => ColumnType::Int4,
        // Unsigned 32-bit variants — widen to Int8 (no Uint4 in this codebase)
        Kind::Uint32 | Kind::Fixed32 => ColumnType::Int8,
        // All 64-bit integer variants
        Kind::Int64 | Kind::Sint64 | Kind::Uint64 | Kind::Fixed64 | Kind::Sfixed64 => {
            ColumnType::Int8
        }
        Kind::Float | Kind::Double => ColumnType::Float8,
        Kind::Bool => ColumnType::Bool,
        Kind::String | Kind::Message(_) => ColumnType::Text,
        Kind::Bytes => ColumnType::Bytea,
    }
}

/// Derives columns from a [`prost_reflect::MessageDescriptor`].
///
/// Repeated and map fields map to JSON-serialised `Text`, because this
/// codebase has no native array or map `ColumnType`. All other fields use the
/// scalar mapping from [`proto_kind_to_column_type`].
#[must_use]
pub fn protobuf_message_to_columns(descriptor: &prost_reflect::MessageDescriptor) -> Vec<Column> {
    descriptor
        .fields()
        .map(|field| {
            let ty = if field.is_list() || field.is_map() {
                // Repeated / map → JSON text.
                ColumnType::Text
            } else {
                proto_kind_to_column_type(&field.kind())
            };
            Column::new(field.name(), ty)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Protobuf Value → Datum
// ---------------------------------------------------------------------------

/// Converts a single [`prost_reflect::Value`] to a [`Datum`].
///
/// The List and Map variants become a JSON-serialised `Text` datum. The
/// caller represents a missing, unset field as `None` → `Datum::Null`.
pub fn proto_value_to_datum(value: &ProtoValue) -> Datum {
    match value {
        ProtoValue::Bool(b) => Datum::Bool(*b),
        ProtoValue::I32(i) => Datum::Int4(*i),
        ProtoValue::I64(i) => Datum::Int8(*i),
        // Unsigned 32-bit — widen to Int8.
        ProtoValue::U32(u) => Datum::Int8(i64::from(*u)),
        // Unsigned 64-bit — return NULL when it cannot fit in Int8.
        ProtoValue::U64(u) => i64::try_from(*u).map_or(Datum::Null, Datum::Int8),
        ProtoValue::F32(f) => Datum::Float8(f64::from(*f)),
        ProtoValue::F64(f) => Datum::Float8(*f),
        ProtoValue::String(s) => Datum::Text(s.clone()),
        ProtoValue::Bytes(b) => Datum::Bytea(b.to_vec()),
        // Enum number → Int4.
        ProtoValue::EnumNumber(n) => Datum::Int4(*n),
        // Nested message → JSON-serialised text.
        ProtoValue::Message(msg) => {
            // Walk the set fields and build a JSON object.
            let obj: serde_json::Map<String, JsonValue> = msg
                .fields()
                .map(|(fd, v)| (fd.name().to_owned(), proto_value_to_json(v)))
                .collect();
            Datum::Text(serde_json::to_string(&JsonValue::Object(obj)).unwrap_or_default())
        }
        // Repeated field → JSON array.
        ProtoValue::List(list) => {
            let arr: Vec<JsonValue> = list.iter().map(proto_value_to_json).collect();
            Datum::Text(serde_json::to_string(&arr).unwrap_or_default())
        }
        // Map field → JSON object (keys coerced to strings).
        ProtoValue::Map(map) => {
            let obj: serde_json::Map<String, JsonValue> = map
                .iter()
                .map(|(k, v)| (format!("{k:?}"), proto_value_to_json(v)))
                .collect();
            Datum::Text(serde_json::to_string(&JsonValue::Object(obj)).unwrap_or_default())
        }
    }
}

/// Best-effort conversion of a [`prost_reflect::Value`] to a
/// [`serde_json::Value`] for nested serialisation.
fn proto_value_to_json(value: &ProtoValue) -> JsonValue {
    match value {
        ProtoValue::Bool(b) => JsonValue::Bool(*b),
        ProtoValue::I32(i) => JsonValue::from(*i),
        ProtoValue::I64(i) => JsonValue::from(*i),
        ProtoValue::U32(u) => JsonValue::from(*u),
        ProtoValue::U64(u) => JsonValue::from(*u),
        ProtoValue::F32(f) => {
            serde_json::Number::from_f64(f64::from(*f)).map_or(JsonValue::Null, JsonValue::Number)
        }
        ProtoValue::F64(f) => {
            serde_json::Number::from_f64(*f).map_or(JsonValue::Null, JsonValue::Number)
        }
        ProtoValue::String(s) => JsonValue::String(s.clone()),
        ProtoValue::Bytes(b) => JsonValue::String(to_hex_text(b)),
        ProtoValue::EnumNumber(n) => JsonValue::from(*n),
        ProtoValue::Message(msg) => {
            let obj: serde_json::Map<String, JsonValue> = msg
                .fields()
                .map(|(fd, v)| (fd.name().to_owned(), proto_value_to_json(v)))
                .collect();
            JsonValue::Object(obj)
        }
        ProtoValue::List(list) => JsonValue::Array(list.iter().map(proto_value_to_json).collect()),
        ProtoValue::Map(map) => {
            let obj: serde_json::Map<String, JsonValue> = map
                .iter()
                .map(|(k, v)| (format!("{k:?}"), proto_value_to_json(v)))
                .collect();
            JsonValue::Object(obj)
        }
    }
}

fn to_hex_text(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(2 + bytes.len() * 2);
    text.push_str("\\x");
    for byte in bytes {
        write!(&mut text, "{byte:02x}").expect("writing to String cannot fail");
    }
    text
}

// ---------------------------------------------------------------------------
// Avro Value → Datum
// ---------------------------------------------------------------------------

/// Maps an Avro `Value` to a `Datum` for the given column type.
///
/// The function unwraps `Union(_, inner)`. `Null` produces `Datum::Null`.
///
/// `decimal_scale` is the scale from the Avro field's `DecimalSchema`, that
/// is the number of fractional digits. You must supply it when the value may
/// be `AvroValue::Decimal`. Pass `None` for non-decimal fields.
fn avro_value_to_datum(value: &AvroValue, col_ty: ColumnType, decimal_scale: Option<u32>) -> Datum {
    match value {
        AvroValue::Null => Datum::Null,
        // Unwrap nullable union — recurse on the inner value.
        AvroValue::Union(_, inner) => avro_value_to_datum(inner, col_ty, decimal_scale),

        AvroValue::Boolean(b) => Datum::Bool(*b),
        AvroValue::Int(i) => Datum::Int4(*i),
        AvroValue::Long(l) => Datum::Int8(*l),
        AvroValue::Float(f) => Datum::Float8(f64::from(*f)),
        AvroValue::Double(d) => Datum::Float8(*d),
        AvroValue::Bytes(b) | AvroValue::Fixed(_, b) => Datum::Bytea(b.clone()),
        AvroValue::String(s) => Datum::Text(s.clone()),

        // Logical date: i32 days since the Unix epoch (1970-01-01).
        //
        // A malicious or buggy producer can send an `int` value that overflows
        // jiff's representable range when added to the epoch.  Fall back to
        // `Datum::Null` rather than panicking — unrepresentable values are
        // NULL in phase-1; correctness for in-range values is unchanged.
        //
        // NOTE: `jiff::Span::new().days(n)` panics for |n| > 7304484, so we
        // must NOT pass the raw i32 to Span; instead convert to seconds first
        // (i64 arithmetic is safe for all i32 values) and use SignedDuration
        // so that out-of-range values produce an Err via checked_add.
        AvroValue::Date(days) => {
            let secs = i64::from(*days) * 86_400;
            let dur = jiff::SignedDuration::from_secs(secs);
            match unix_epoch_date().checked_add(dur) {
                Ok(date) => Datum::Date(date),
                Err(_) => Datum::Null,
            }
        }

        // Logical timestamp-millis: i64 milliseconds since Unix epoch → Timestamptz.
        //
        // Same rationale as `Date`: an out-of-range value from an untrusted
        // broker falls back to `Datum::Null` instead of panicking.
        AvroValue::TimestampMillis(ms) => match jiff::Timestamp::from_millisecond(*ms) {
            Ok(ts) => Datum::Timestamptz(ts),
            Err(_) => Datum::Null,
        },

        // Logical timestamp-micros: i64 microseconds since Unix epoch → Timestamptz.
        //
        // Same rationale: out-of-range → `Datum::Null`, no panic.
        AvroValue::TimestampMicros(us) => match jiff::Timestamp::from_microsecond(*us) {
            Ok(ts) => Datum::Timestamptz(ts),
            Err(_) => Datum::Null,
        },

        // Decimal → Numeric (bigdecimal).
        // apache-avro's `Decimal` wraps a `BigInt` (unscaled value). The real
        // scale lives in the field's schema (`DecimalSchema::scale`), threaded
        // in via `decimal_scale`. `BigDecimal::new(bigint, scale)` represents
        // `bigint * 10^-scale`, so we pass the Avro scale directly.
        AvroValue::Decimal(d) => {
            use bigdecimal::num_bigint::BigInt;
            let big_int: BigInt = BigInt::from(d.clone());
            let scale = i64::from(decimal_scale.unwrap_or(0));
            let bd = bigdecimal::BigDecimal::new(big_int, scale);
            Datum::Numeric(bd.into())
        }
        AvroValue::BigDecimal(bd) => Datum::Numeric(bd.clone().into()),

        // Nested complex types (Record, Array, Map) → JSON-serialised text.
        AvroValue::Record(fields) => {
            // Re-serialise as a JSON object for the text column.
            let map: serde_json::Map<String, JsonValue> = fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            Datum::Text(serde_json::to_string(&JsonValue::Object(map)).unwrap_or_default())
        }
        AvroValue::Array(arr) => {
            let arr: Vec<JsonValue> = arr.iter().map(avro_value_to_json).collect();
            Datum::Text(serde_json::to_string(&arr).unwrap_or_default())
        }
        AvroValue::Map(m) => {
            let map: serde_json::Map<String, JsonValue> = m
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            Datum::Text(serde_json::to_string(&JsonValue::Object(map)).unwrap_or_default())
        }

        // Catch-all: serialise as text.
        _ => {
            let _ = (col_ty, decimal_scale); // suppress unused warnings
            Datum::Text(format!("{value:?}"))
        }
    }
}

/// Converts an Avro `Value` to a `serde_json::Value`. The conversion is
/// best-effort and serves nested Avro values that go into a text column.
fn avro_value_to_json(value: &AvroValue) -> JsonValue {
    match value {
        AvroValue::Union(_, inner) => avro_value_to_json(inner),
        AvroValue::Boolean(b) => JsonValue::Bool(*b),
        AvroValue::Int(i) => JsonValue::from(*i),
        AvroValue::Long(l) => JsonValue::from(*l),
        AvroValue::Float(f) => {
            serde_json::Number::from_f64(f64::from(*f)).map_or(JsonValue::Null, JsonValue::Number)
        }
        AvroValue::Double(d) => {
            serde_json::Number::from_f64(*d).map_or(JsonValue::Null, JsonValue::Number)
        }
        AvroValue::Bytes(b) | AvroValue::Fixed(_, b) => {
            // Encode as hex string for JSON serialisation of nested bytea.
            JsonValue::String(to_hex_text(b))
        }
        AvroValue::String(s) => JsonValue::String(s.clone()),
        AvroValue::Date(d) => JsonValue::from(*d),
        AvroValue::TimestampMillis(ms) => JsonValue::from(*ms),
        AvroValue::TimestampMicros(us) => JsonValue::from(*us),
        AvroValue::Record(fields) => {
            let map: serde_json::Map<String, JsonValue> = fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            JsonValue::Object(map)
        }
        AvroValue::Array(arr) => JsonValue::Array(arr.iter().map(avro_value_to_json).collect()),
        AvroValue::Map(m) => {
            let map: serde_json::Map<String, JsonValue> = m
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect();
            JsonValue::Object(map)
        }
        _ => JsonValue::Null,
    }
}

// ---------------------------------------------------------------------------
// JSON Value → Datum
// ---------------------------------------------------------------------------

fn json_value_to_datum(value: &JsonValue, col_ty: ColumnType) -> Datum {
    match (value, col_ty) {
        (JsonValue::Null, _) => Datum::Null,
        (JsonValue::Bool(b), ColumnType::Bool) => Datum::Bool(*b),
        // NOTE: `json_schema_to_columns` maps JSON "integer" → Int8 only, so
        // the Int4 arm below is never reached in practice. It is kept as a
        // defensive fallback in case a caller supplies a hand-crafted Int4
        // column for JSON data.
        (JsonValue::Number(n), ColumnType::Int4) => n
            .as_i64()
            .and_then(|i| i32::try_from(i).ok())
            .map_or(Datum::Null, Datum::Int4),
        (JsonValue::Number(n), ColumnType::Int8) => n.as_i64().map_or(Datum::Null, Datum::Int8),
        (JsonValue::Number(n), ColumnType::Float8) => n.as_f64().map_or(Datum::Null, Datum::Float8),
        (JsonValue::String(s), ColumnType::Text) => Datum::Text(s.clone()),
        // Cross-type: serialise as text.
        (v, _) => Datum::Text(v.to_string()),
    }
}

// ---------------------------------------------------------------------------
// project
// ---------------------------------------------------------------------------

/// Projects a decoded Kafka value onto `value_columns` and returns one
/// `Datum` per column, in order.
///
/// * `DecodedValue::Avro(Record(...))`: the function looks up each column by
///   name in the Avro Record fields.
/// * `DecodedValue::Json(Object {...})`: the function looks up each column by
///   name in the JSON object.
/// * `DecodedValue::Raw(bytes)`: the function returns a single
///   `Datum::Bytea` whatever `value_columns` holds. The raw fallback is
///   always one bytea column.
///
/// `avro_schema` is the parsed writer schema for the Avro case. When you
/// supply it and the schema is a Record, the function reads the field-level
/// sub-schemas to recover the `scale` of `decimal` fields, so it scales
/// `AvroValue::Decimal` correctly. Pass `None` for the JSON and Raw paths.
#[must_use]
pub fn project(
    decoded: &DecodedValue,
    value_columns: &[Column],
    avro_schema: Option<&AvroSchema>,
) -> Vec<Datum> {
    match decoded {
        DecodedValue::Raw(bytes) => vec![Datum::Bytea(bytes.clone())],

        DecodedValue::Avro(AvroValue::Record(fields)) => {
            // Build a lookup of field-name → DecimalSchema scale from the
            // writer schema so `Decimal` values can be correctly scaled.
            let decimal_scale_for = |name: &str| -> Option<u32> {
                let schema = avro_schema?;
                let AvroSchema::Record(RecordSchema {
                    fields: schema_fields,
                    ..
                }) = schema
                else {
                    return None;
                };
                let field_schema = schema_fields
                    .iter()
                    .find(|f| f.name == name)
                    .map(|f| &f.schema)?;
                // The field schema may be wrapped in a nullable union `[null, decimal]`.
                let inner = match field_schema {
                    AvroSchema::Union(u) => u
                        .variants()
                        .iter()
                        .find(|s| !matches!(s, AvroSchema::Null))?,
                    other => other,
                };
                if let AvroSchema::Decimal(DecimalSchema { scale, .. }) = inner {
                    u32::try_from(*scale).ok()
                } else {
                    None
                }
            };

            value_columns
                .iter()
                .map(|col| {
                    fields
                        .iter()
                        .find(|(name, _)| name == &col.name)
                        .map_or(Datum::Null, |(name, v)| {
                            avro_value_to_datum(v, col.ty, decimal_scale_for(name))
                        })
                })
                .collect()
        }

        DecodedValue::Avro(_) => value_columns.iter().map(|_| Datum::Null).collect(),

        DecodedValue::Json(JsonValue::Object(obj)) => value_columns
            .iter()
            .map(|col| {
                obj.get(&col.name)
                    .map_or(Datum::Null, |v| json_value_to_datum(v, col.ty))
            })
            .collect(),

        DecodedValue::Json(_) => value_columns.iter().map(|_| Datum::Null).collect(),

        // Protobuf: look up each value column by field name in the DynamicMessage.
        //
        // `get_field_by_name` always returns `Some(default_value)` for fields
        // that exist in the descriptor — even for unset optional fields.  We
        // therefore gate on `has_field_by_name` first:
        //
        // * For `optional` (proto3 singular with presence) fields: if the
        //   field was not explicitly set, `has_field_by_name` returns `false`
        //   → `Datum::Null`.
        // * For regular proto3 fields (no `optional`): `has_field_by_name` is
        //   always `true` because they have an implicit default → the default
        //   value is projected (e.g. `""` for string, `0` for integers).
        // * If the column name does not exist in the descriptor at all,
        //   `has_field_by_name` returns `false` → `Datum::Null`.
        DecodedValue::Protobuf(msg) => value_columns
            .iter()
            .map(|col| {
                if msg.has_field_by_name(&col.name) {
                    msg.get_field_by_name(&col.name)
                        .map_or(Datum::Null, |v| proto_value_to_datum(&v))
                } else {
                    Datum::Null
                }
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use bigdecimal::num_bigint::BigInt;
    use crabka_pgtypes::{ColumnType, Datum};

    use super::*;

    // -----------------------------------------------------------------------
    // Brief's required test (verbatim from task-9-brief.md)
    // -----------------------------------------------------------------------

    #[test]
    fn avro_record_projects_to_datums() {
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"R","fields":[
            {"name":"id","type":"long"},{"name":"name","type":["null","string"]}]}"#,
        )
        .expect("schema parses");
        let cols = crate::types::avro_schema_to_columns(&schema);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].ty, crabka_pgtypes::ColumnType::Int8);
        assert_eq!(cols[1].ty, crabka_pgtypes::ColumnType::Text);

        let mut record = apache_avro::types::Record::new(&schema).expect("record created");
        record.put("id", 7i64);
        record.put("name", Some("x".to_string()));
        let val = apache_avro::types::Value::from(record);
        let datums = crate::types::project(
            &crate::decode::DecodedValue::Avro(val),
            &cols,
            Some(&schema),
        );
        assert_eq!(datums[0], crabka_pgtypes::Datum::Int8(7));
        assert_eq!(datums[1], crabka_pgtypes::Datum::Text("x".into()));
    }

    // -----------------------------------------------------------------------
    // Nullable field → Datum::Null when absent or null
    // -----------------------------------------------------------------------

    #[test]
    fn avro_nullable_field_null_when_missing() {
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"S","fields":[
            {"name":"val","type":["null","string"],"default":null}]}"#,
        )
        .expect("schema parses");
        let cols = avro_schema_to_columns(&schema);
        assert_eq!(cols[0].ty, ColumnType::Text);

        // Build a record with the field explicitly set to None (null).
        let mut record = apache_avro::types::Record::new(&schema).expect("record");
        record.put("val", Option::<String>::None);
        let val = apache_avro::types::Value::from(record);
        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        assert_eq!(datums[0], Datum::Null);
    }

    // -----------------------------------------------------------------------
    // Bytes field → Datum::Bytea
    // -----------------------------------------------------------------------

    #[test]
    fn avro_bytes_field_to_bytea() {
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"B","fields":[
            {"name":"blob","type":"bytes"}]}"#,
        )
        .expect("schema parses");
        let cols = avro_schema_to_columns(&schema);
        assert_eq!(cols[0].ty, ColumnType::Bytea);

        let mut record = apache_avro::types::Record::new(&schema).expect("record");
        record.put("blob", vec![0xCAu8, 0xFEu8]);
        let val = apache_avro::types::Value::from(record);
        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        assert_eq!(datums[0], Datum::Bytea(vec![0xCA, 0xFE]));
    }

    // -----------------------------------------------------------------------
    // JSON object → typed columns
    // -----------------------------------------------------------------------

    #[test]
    fn json_object_projects_to_typed_datums() {
        let json_schema: JsonValue = serde_json::json!({
            "type": "object",
            "properties": {
                "count": {"type": "integer"},
                "label": {"type": "string"}
            }
        });
        let cols = json_schema_to_columns(&json_schema);
        // Order from serde_json::Map iteration — find by name.
        let count_col = cols.iter().find(|c| c.name == "count").expect("count col");
        let label_col = cols.iter().find(|c| c.name == "label").expect("label col");
        assert_eq!(count_col.ty, ColumnType::Int8);
        assert_eq!(label_col.ty, ColumnType::Text);

        let payload: JsonValue = serde_json::json!({"count": 42, "label": "hello"});
        let datums = project(&DecodedValue::Json(payload), &cols, None);
        // Project in column order.
        let count_datum = datums
            .iter()
            .zip(cols.iter())
            .find(|(_, c)| c.name == "count")
            .map(|(d, _)| d)
            .expect("count datum");
        let label_datum = datums
            .iter()
            .zip(cols.iter())
            .find(|(_, c)| c.name == "label")
            .map(|(d, _)| d)
            .expect("label datum");
        assert_eq!(*count_datum, Datum::Int8(42));
        assert_eq!(*label_datum, Datum::Text("hello".into()));
    }

    // -----------------------------------------------------------------------
    // Missing field → Datum::Null
    // -----------------------------------------------------------------------

    #[test]
    fn avro_missing_field_produces_null() {
        // Schema has two fields; record only has `id`.
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"T","fields":[
            {"name":"id","type":"long"},
            {"name":"opt","type":["null","string"],"default":null}]}"#,
        )
        .expect("schema parses");
        let cols = avro_schema_to_columns(&schema);

        let mut record = apache_avro::types::Record::new(&schema).expect("record");
        record.put("id", 99i64);
        record.put("opt", Option::<String>::None);
        let val = apache_avro::types::Value::from(record);
        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        assert_eq!(datums[0], Datum::Int8(99));
        assert_eq!(datums[1], Datum::Null);
    }

    // -----------------------------------------------------------------------
    // Finding 1: Avro decimal with scale=2 projects correctly (RED→GREEN)
    // Unscaled value 1999 with scale 2 must produce Datum::Numeric "19.99".
    // -----------------------------------------------------------------------

    #[test]
    fn avro_decimal_scale_applied_correctly() {
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"D","fields":[
            {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}"#,
        )
        .expect("schema parses");
        let cols = avro_schema_to_columns(&schema);
        assert_eq!(cols[0].name, "price");
        assert_eq!(cols[0].ty, ColumnType::Numeric(None));

        // Build an Avro Decimal value with unscaled integer 1999 (represents 19.99).
        let unscaled = BigInt::from(1999i64);
        let decimal_val = apache_avro::Decimal::from(unscaled.to_signed_bytes_be());
        let val = AvroValue::Record(vec![("price".to_string(), AvroValue::Decimal(decimal_val))]);

        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        let Datum::Numeric(ref bd) = datums[0] else {
            panic!("expected Datum::Numeric, got {:?}", datums[0]);
        };
        assert_eq!(
            bd.to_string(),
            "19.99",
            "decimal scale must be applied: 1999 * 10^-2 = 19.99"
        );
    }

    // -----------------------------------------------------------------------
    // Finding 2: truly-absent field (not in record field list) → Datum::Null
    // -----------------------------------------------------------------------

    #[test]
    fn avro_truly_absent_field_produces_null() {
        // value_columns requests a field "extra" that is not present in the
        // Avro Record's fields list at all (not merely present-but-null).
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"A","fields":[
            {"name":"id","type":"long"}]}"#,
        )
        .expect("schema parses");

        let mut record = apache_avro::types::Record::new(&schema).expect("record");
        record.put("id", 5i64);
        let val = apache_avro::types::Value::from(record);

        // Ask for both "id" and a field "extra" that does not exist in the record.
        let cols = vec![
            Column::new("id", ColumnType::Int8),
            Column::new("extra", ColumnType::Text),
        ];

        let datums = project(&DecodedValue::Avro(val), &cols, Some(&schema));
        assert_eq!(datums[0], Datum::Int8(5), "id field must project correctly");
        assert_eq!(
            datums[1],
            Datum::Null,
            "absent field must produce Datum::Null via unwrap_or fallback"
        );
    }

    // -----------------------------------------------------------------------
    // Protobuf: helper to build a MessageDescriptor from inline .proto source
    // -----------------------------------------------------------------------

    /// Compiles a small `.proto` source string at test time with the
    /// pure-Rust `protox` compiler. There is no file I/O and no protoc
    /// binary. It then loads the resulting `FileDescriptorSet` into a
    /// `prost_reflect::DescriptorPool` and returns the descriptor for the
    /// named message.
    fn make_descriptor(proto_src: &str, message_name: &str) -> prost_reflect::MessageDescriptor {
        struct InMemoryResolver {
            name: String,
            src: String,
        }

        impl protox::file::FileResolver for InMemoryResolver {
            fn open_file(&self, name: &str) -> Result<protox::file::File, protox::Error> {
                if name == self.name {
                    protox::file::File::from_source(name, &self.src)
                } else {
                    Err(protox::Error::file_not_found(name))
                }
            }
        }

        let resolver = InMemoryResolver {
            name: "test.proto".to_owned(),
            src: proto_src.to_owned(),
        };
        let mut compiler = protox::Compiler::with_file_resolver(resolver);
        compiler
            .open_file("test.proto")
            .expect("protox compiles test.proto");
        let fds = compiler.file_descriptor_set();

        let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(fds)
            .expect("DescriptorPool from FileDescriptorSet");
        pool.get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("message {message_name} found in pool"))
    }

    // -----------------------------------------------------------------------
    // Protobuf: proto_value_to_datum — unit-test the Value→Datum conversion
    // directly with hand-built prost_reflect::Value variants.
    // -----------------------------------------------------------------------

    #[test]
    fn proto_value_bool_to_datum() {
        assert_eq!(
            proto_value_to_datum(&ProtoValue::Bool(true)),
            Datum::Bool(true),
            "Bool(true) → Datum::Bool(true)"
        );
        assert_eq!(
            proto_value_to_datum(&ProtoValue::Bool(false)),
            Datum::Bool(false),
            "Bool(false) → Datum::Bool(false)"
        );
    }

    #[test]
    fn proto_value_i32_to_datum() {
        assert_eq!(
            proto_value_to_datum(&ProtoValue::I32(42)),
            Datum::Int4(42),
            "I32(42) → Datum::Int4(42)"
        );
    }

    #[test]
    fn proto_value_i64_to_datum() {
        assert_eq!(
            proto_value_to_datum(&ProtoValue::I64(9_876_543_210)),
            Datum::Int8(9_876_543_210),
            "I64 → Datum::Int8"
        );
    }

    #[test]
    fn proto_value_string_to_datum() {
        assert_eq!(
            proto_value_to_datum(&ProtoValue::String("hello".to_owned())),
            Datum::Text("hello".to_owned()),
            "String → Datum::Text"
        );
    }

    #[test]
    fn proto_value_bytes_to_datum() {
        assert_eq!(
            proto_value_to_datum(&ProtoValue::Bytes(
                prost_reflect::prost::bytes::Bytes::from_static(b"\xCA\xFE"),
            )),
            Datum::Bytea(vec![0xCA, 0xFE]),
            "Bytes → Datum::Bytea"
        );
    }

    #[test]
    fn proto_value_f64_to_datum() {
        let input = std::f64::consts::E;
        let Datum::Float8(v) = proto_value_to_datum(&ProtoValue::F64(input)) else {
            panic!("expected Datum::Float8");
        };
        assert!((v - input).abs() < f64::EPSILON, "F64 → Datum::Float8");
    }

    // -----------------------------------------------------------------------
    // Protobuf: protobuf_message_to_columns — verify field-kind mapping via a
    // real descriptor built from inline .proto source with protox.
    // -----------------------------------------------------------------------

    #[test]
    fn protobuf_columns_from_descriptor() {
        let proto_src = r#"
            syntax = "proto3";
            package test;
            message Event {
                int64  id      = 1;
                string label   = 2;
                bool   active  = 3;
                double score   = 4;
                bytes  payload = 5;
                int32  count   = 6;
            }
        "#;
        let descriptor = make_descriptor(proto_src, "test.Event");
        let cols = protobuf_message_to_columns(&descriptor);

        let find = |name: &str| {
            cols.iter()
                .find(|c| c.name == name)
                .unwrap_or_else(|| panic!("column {name} present"))
                .ty
        };

        assert_eq!(find("id"), ColumnType::Int8, "int64 → Int8");
        assert_eq!(find("label"), ColumnType::Text, "string → Text");
        assert_eq!(find("active"), ColumnType::Bool, "bool → Bool");
        assert_eq!(find("score"), ColumnType::Float8, "double → Float8");
        assert_eq!(find("payload"), ColumnType::Bytea, "bytes → Bytea");
        assert_eq!(find("count"), ColumnType::Int4, "int32 → Int4");
    }

    // -----------------------------------------------------------------------
    // Protobuf: project over DynamicMessage — end-to-end Value→Datum pipeline.
    // -----------------------------------------------------------------------

    #[test]
    fn protobuf_project_dynamic_message() {
        use prost_reflect::prost::Message as _;

        let proto_src = r#"
            syntax = "proto3";
            package test;
            message Row {
                int64  id    = 1;
                string name  = 2;
            }
        "#;
        let descriptor = make_descriptor(proto_src, "test.Row");
        let cols = protobuf_message_to_columns(&descriptor);

        // Build a DynamicMessage with known field values.
        let mut msg = prost_reflect::DynamicMessage::new(descriptor.clone());
        msg.try_set_field_by_name("id", ProtoValue::I64(777))
            .expect("set id");
        msg.try_set_field_by_name("name", ProtoValue::String("kafka".to_owned()))
            .expect("set name");

        // Round-trip through protobuf encode/decode to exercise the decode path.
        let encoded = msg.encode_to_vec();
        let decoded_msg = prost_reflect::DynamicMessage::decode(descriptor, encoded.as_slice())
            .expect("DynamicMessage decodes");

        let datums = project(&DecodedValue::Protobuf(decoded_msg), &cols, None);

        let id_datum = datums
            .iter()
            .zip(cols.iter())
            .find(|(_, c)| c.name == "id")
            .map(|(d, _)| d)
            .expect("id datum present");
        let name_datum = datums
            .iter()
            .zip(cols.iter())
            .find(|(_, c)| c.name == "name")
            .map(|(d, _)| d)
            .expect("name datum present");

        assert_eq!(*id_datum, Datum::Int8(777), "int64 field → Datum::Int8");
        assert_eq!(
            *name_datum,
            Datum::Text("kafka".to_owned()),
            "string field → Datum::Text"
        );
    }

    // -----------------------------------------------------------------------
    // Protobuf: proto3 optional field not present → Datum::Null
    //
    // In proto3, a field declared without `optional` always has a default
    // value (e.g. "" for string) and `get_field_by_name` returns
    // Some(String("")) rather than None.  To get nullable semantics, the
    // field must be declared `optional`, which enables field-presence tracking
    // and causes unset fields to return None from get_field_by_name.
    // -----------------------------------------------------------------------

    #[test]
    fn protobuf_optional_absent_field_produces_null() {
        let proto_src = r#"
            syntax = "proto3";
            package test;
            message Sparse {
                int64           id      = 1;
                optional string comment = 2;
            }
        "#;
        let descriptor = make_descriptor(proto_src, "test.Sparse");

        // Only set `id`; leave optional `comment` unset — it should project as Null.
        let mut msg = prost_reflect::DynamicMessage::new(descriptor.clone());
        msg.try_set_field_by_name("id", ProtoValue::I64(1))
            .expect("set id");

        let cols = vec![
            Column::new("id", ColumnType::Int8),
            Column::new("comment", ColumnType::Text),
        ];

        let datums = project(&DecodedValue::Protobuf(msg), &cols, None);
        assert_eq!(datums[0], Datum::Int8(1), "id → Int8(1)");
        assert_eq!(datums[1], Datum::Null, "unset optional comment → Null");
    }

    // -----------------------------------------------------------------------
    // Fix 1: out-of-range Avro temporal values → Datum::Null, NOT a panic.
    //
    // Avro date/timestamp-millis/timestamp-micros are bare int/long on the
    // wire, so a malicious or buggy producer can send a value outside jiff's
    // representable range.  We must not panic; the result must be Datum::Null.
    // -----------------------------------------------------------------------

    #[test]
    fn avro_out_of_range_timestamp_millis_produces_null() {
        // i64::MAX milliseconds is far outside jiff's Timestamp range.
        let val = AvroValue::Record(vec![(
            "ts".to_string(),
            AvroValue::TimestampMillis(i64::MAX),
        )]);
        let cols = vec![Column::new("ts", ColumnType::Timestamptz)];
        let datums = project(&DecodedValue::Avro(val), &cols, None);
        assert_eq!(
            datums[0],
            Datum::Null,
            "out-of-range TimestampMillis must produce Datum::Null, not panic"
        );
    }

    #[test]
    fn avro_out_of_range_date_produces_null() {
        // i32::MAX days since epoch is ~5.8 million years — outside jiff's range.
        let val = AvroValue::Record(vec![("d".to_string(), AvroValue::Date(i32::MAX))]);
        let cols = vec![Column::new("d", ColumnType::Date)];
        let datums = project(&DecodedValue::Avro(val), &cols, None);
        assert_eq!(
            datums[0],
            Datum::Null,
            "out-of-range Date must produce Datum::Null, not panic"
        );
    }

    #[test]
    fn avro_out_of_range_timestamp_micros_produces_null() {
        // i64::MAX microseconds is also outside jiff's range.
        let val = AvroValue::Record(vec![(
            "ts".to_string(),
            AvroValue::TimestampMicros(i64::MAX),
        )]);
        let cols = vec![Column::new("ts", ColumnType::Timestamptz)];
        let datums = project(&DecodedValue::Avro(val), &cols, None);
        assert_eq!(
            datums[0],
            Datum::Null,
            "out-of-range TimestampMicros must produce Datum::Null, not panic"
        );
    }

    // -----------------------------------------------------------------------
    // Protobuf: proto3 non-optional field not explicitly set → Datum::Null
    //
    // `has_field_by_name` returns false for non-optional proto3 fields that
    // haven't been explicitly set (prost-reflect tracks presence even for
    // non-optional fields at the DynamicMessage level).  Our `project` uses
    // `has_field_by_name` as the gate, so unset non-optional fields produce
    // Datum::Null — which is the correct database semantic (absent = NULL).
    // Use `optional` in the schema if you want field-presence semantics with
    // wire-encoded default values.
    // -----------------------------------------------------------------------

    #[test]
    fn protobuf_non_optional_absent_field_produces_null() {
        let proto_src = r#"
            syntax = "proto3";
            package test;
            message Dense {
                int64  id      = 1;
                string comment = 2;
            }
        "#;
        let descriptor = make_descriptor(proto_src, "test.Dense");

        // Only set `id`; leave non-optional `comment` unset.
        // has_field_by_name returns false → project yields Datum::Null.
        let mut msg = prost_reflect::DynamicMessage::new(descriptor.clone());
        msg.try_set_field_by_name("id", ProtoValue::I64(2))
            .expect("set id");

        let cols = vec![
            Column::new("id", ColumnType::Int8),
            Column::new("comment", ColumnType::Text),
        ];

        let datums = project(&DecodedValue::Protobuf(msg), &cols, None);
        assert_eq!(datums[0], Datum::Int8(2), "id → Int8(2)");
        assert_eq!(
            datums[1],
            Datum::Null,
            "unset non-optional field: has_field_by_name is false → Datum::Null"
        );
    }
}
