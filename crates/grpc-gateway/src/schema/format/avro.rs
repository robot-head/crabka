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
        assert!(!bytes.is_empty(), "encoded bytes should not be empty");

        let json_out = deserialize(SCHEMA, &bytes).expect("deserialize should succeed");

        let expected: serde_json::Value = serde_json::from_slice(json_input).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&json_out).unwrap();
        assert_eq!(expected, actual, "round-tripped JSON should match input");
    }

    #[test]
    fn validate_accepts_valid_json() {
        let json = br#"{"id": 42, "name": "hello"}"#;
        assert!(
            validate(SCHEMA, json).is_ok(),
            "valid JSON should pass validation"
        );
    }

    #[test]
    fn validate_rejects_wrong_type() {
        // "id" is declared as `long`; supplying a string should fail.
        let json = br#"{"id": "notlong", "name": "a"}"#;
        assert!(
            validate(SCHEMA, json).is_err(),
            "wrong type for `id` field should fail validation"
        );
    }

    #[test]
    fn validate_rejects_missing_field() {
        // Missing the required `name` field (no default).
        let json = br#"{"id": 1}"#;
        assert!(
            validate(SCHEMA, json).is_err(),
            "missing required field should fail validation"
        );
    }

    #[test]
    fn deserialize_bad_bytes_returns_err() {
        let result = deserialize(SCHEMA, b"\x00\x01\x02bad");
        // Malformed datum; error expected (not a panic).
        assert!(result.is_err(), "corrupt datum should return Err");
    }

    #[test]
    fn serialize_bad_schema_returns_err() {
        let result = serialize("not-a-valid-schema", br#"{"id":1,"name":"a"}"#);
        assert!(result.is_err(), "invalid schema string should return Err");
    }
}
