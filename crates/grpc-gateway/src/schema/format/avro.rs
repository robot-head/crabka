//! Avro format: serialize / deserialize / validate.
//!
//! The wire payload is an Avro binary datum, with no Confluent framing.
//! `wire.rs` adds that framing separately. The gateway's structured input and
//! output are JSON.
//!
//! This module uses [`apache_avro`] 0.22 to parse a schema, to encode binary,
//! and to round-trip a value.

use apache_avro::{Schema, reader::datum::GenericDatumReader, writer::datum::GenericDatumWriter};
use bytes::Bytes;

use crate::codec::CodecError;

/// Serialize `json`, a JSON-encoded value, into Avro binary with `schema`.
///
/// Steps:
/// 1. Parse the Avro schema string.
/// 2. Parse the JSON bytes into a [`serde_json::Value`].
/// 3. Convert to [`apache_avro::types::Value`] with its JSON conversion, then
///    call `Value::resolve` to coerce ints, enums, and unions to the schema.
/// 4. Encode as an Avro binary datum with [`GenericDatumWriter`].
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn serialize(schema: &str, json: &[u8]) -> Result<Bytes, CodecError> {
    let avro_schema = Schema::parse_str(schema)
        .map_err(|e| CodecError::Serialize(format!("Avro schema parse: {e}")))?;

    let json_value: serde_json::Value = serde_json::from_slice(json)
        .map_err(|e| CodecError::Serialize(format!("JSON parse: {e}")))?;

    // Use the JSON-specific conversion rather than the generic serde
    // serializer. In apache-avro 0.22, serde_json numbers pass through the
    // generic serializer's private representation and are interpreted as
    // fixed bytes. The JSON conversion preserves their numeric Avro type.
    let avro_value = apache_avro::types::Value::try_from(json_value)
        .map_err(|e| CodecError::Serialize(format!("JSON to Avro value: {e}")))?
        .resolve(&avro_schema)
        .map_err(|e| CodecError::Serialize(format!("resolve: {e}")))?;

    let datum = GenericDatumWriter::builder(&avro_schema)
        .build()
        .and_then(|writer| writer.write_value_to_vec(avro_value))
        .map_err(|e| CodecError::Serialize(format!("Avro datum write: {e}")))?;

    Ok(Bytes::from(datum))
}

/// Deserialize Avro binary `payload` back to JSON bytes with `schema`.
///
/// Steps:
/// 1. Parse the Avro schema string.
/// 2. Decode the binary datum into an [`apache_avro::types::Value`] with
///    [`GenericDatumReader`].
/// 3. Convert to [`serde_json::Value`] with the `TryFrom` that
///    `apache_avro` supplies.
/// 4. Serialize to JSON bytes.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn deserialize(schema: &str, payload: &[u8]) -> Result<Bytes, CodecError> {
    let avro_schema = Schema::parse_str(schema)
        .map_err(|e| CodecError::Serialize(format!("Avro schema parse: {e}")))?;

    let mut cursor = std::io::Cursor::new(payload);
    let avro_value = GenericDatumReader::builder(&avro_schema)
        .build()
        .and_then(|reader| reader.read_value(&mut cursor))
        .map_err(|e| CodecError::Serialize(format!("Avro datum read: {e}")))?;

    // `TryFrom<Value> for serde_json::Value` is implemented by apache-avro.
    let json_value = serde_json::Value::try_from(avro_value)
        .map_err(|e| CodecError::Serialize(format!("avro→json conversion: {e}")))?;

    let json_bytes = serde_json::to_vec(&json_value)
        .map_err(|e| CodecError::Serialize(format!("JSON serialize: {e}")))?;

    Ok(Bytes::from(json_bytes))
}

/// Validate that `json` is a valid Avro value for `schema`.
///
/// This function is a round-trip through [`serialize`]. If the JSON encodes as
/// an Avro datum, it is valid for the schema.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
            assert2::assert!(validate(SCHEMA, json).is_ok() == valid, "case {name}");
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
            assert2::assert!(result.is_err(), "case {name}");
        }
    }
}
