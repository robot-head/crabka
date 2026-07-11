//! Avro format: serialize / deserialize / validate.
//!
//! Wire payload is **Avro binary datum** (no Confluent framing — that is added
//! separately by `wire.rs`). The gateway's structured input/output is JSON.
//!
//! Implementation uses [`apache_avro`] 0.21 for schema parsing, binary
//! encoding, and value round-tripping.

use apache_avro::Schema;
use bytes::Bytes;

use crate::codec::CodecError;

/// Serialize `json` (a JSON-encoded value) into Avro binary using `schema`.
///
/// Steps:
/// 1. Parse the Avro schema string.
/// 2. Parse the JSON bytes into a [`serde_json::Value`].
/// 3. Convert to [`apache_avro::types::Value`] via `apache_avro::to_value`,
///    then `Value::resolve` to coerce types (ints, enums, unions) to the schema.
/// 4. Encode as Avro binary datum via `apache_avro::to_avro_datum`.
pub fn serialize(schema: &str, json: &[u8]) -> Result<Bytes, CodecError> {
    let avro_schema = Schema::parse_str(schema)
        .map_err(|e| CodecError::Serialize(format!("Avro schema parse: {e}")))?;

    let json_value: serde_json::Value = serde_json::from_slice(json)
        .map_err(|e| CodecError::Serialize(format!("JSON parse: {e}")))?;

    // `apache_avro::to_value` converts a serde-serializable value into an
    // `apache_avro::types::Value`; then `resolve` coerces it against the schema
    // (e.g. JSON numbers become `Value::Long` for an Avro `long` field).
    let avro_value = apache_avro::to_value(json_value)
        .map_err(|e| CodecError::Serialize(format!("to_value: {e}")))?
        .resolve(&avro_schema)
        .map_err(|e| CodecError::Serialize(format!("resolve: {e}")))?;

    let datum = apache_avro::to_avro_datum(&avro_schema, avro_value)
        .map_err(|e| CodecError::Serialize(format!("to_avro_datum: {e}")))?;

    Ok(Bytes::from(datum))
}

/// Deserialize Avro binary `payload` back to JSON bytes using `schema`.
///
/// Steps:
/// 1. Parse the Avro schema string.
/// 2. Decode the binary datum into an [`apache_avro::types::Value`] via
///    `apache_avro::from_avro_datum`.
/// 3. Convert to [`serde_json::Value`] via `TryFrom` (provided by
///    `apache_avro` 0.21).
/// 4. Serialize to JSON bytes.
pub fn deserialize(schema: &str, payload: &[u8]) -> Result<Bytes, CodecError> {
    let avro_schema = Schema::parse_str(schema)
        .map_err(|e| CodecError::Serialize(format!("Avro schema parse: {e}")))?;

    let mut cursor = std::io::Cursor::new(payload);
    let avro_value = apache_avro::from_avro_datum(&avro_schema, &mut cursor, None)
        .map_err(|e| CodecError::Serialize(format!("from_avro_datum: {e}")))?;

    // `TryFrom<Value> for serde_json::Value` is implemented in apache-avro 0.21.
    let json_value = serde_json::Value::try_from(avro_value)
        .map_err(|e| CodecError::Serialize(format!("avro→json conversion: {e}")))?;

    let json_bytes = serde_json::to_vec(&json_value)
        .map_err(|e| CodecError::Serialize(format!("JSON serialize: {e}")))?;

    Ok(Bytes::from(json_bytes))
}

/// Validate that `json` is a valid Avro value for `schema`.
///
/// Implemented as a round-trip through [`serialize`]: if the JSON can be
/// encoded as an Avro datum, it is valid for the schema.
pub fn validate(schema: &str, json: &[u8]) -> Result<(), CodecError> {
    serialize(schema, json)
        .map(|_| ())
        .map_err(|e| CodecError::Validate(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"{
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "id",   "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;

    #[test]
    fn round_trip_serialize_deserialize() {
        let json_input = br#"{"id": 1, "name": "a"}"#;

        let bytes = serialize(SCHEMA, json_input).expect("serialize should succeed");
        assert2::assert!(!bytes.is_empty());

        let json_out = deserialize(SCHEMA, &bytes).expect("deserialize should succeed");

        let expected: serde_json::Value = serde_json::from_slice(json_input).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&json_out).unwrap();
        assert2::assert!(expected == actual);
    }

    #[test]
    fn validation_cases() {
        for (name, json, valid) in [
            ("valid", br#"{"id": 42, "name": "hello"}"#.as_slice(), true),
            (
                "wrong_type",
                br#"{"id": "notlong", "name": "a"}"#.as_slice(),
                false,
            ),
            ("missing_field", br#"{"id": 1}"#.as_slice(), false),
        ] {
            assert2::assert!(validate(SCHEMA, json).is_ok() == valid);
        }
    }

    #[test]
    fn codec_error_cases() {
        for (name, result) in [
            ("corrupt_datum", deserialize(SCHEMA, b"\x00\x01\x02bad")),
            (
                "invalid_schema",
                serialize("not-a-valid-schema", br#"{"id":1,"name":"a"}"#),
            ),
        ] {
            assert2::assert!(result.is_err());
        }
    }
}
