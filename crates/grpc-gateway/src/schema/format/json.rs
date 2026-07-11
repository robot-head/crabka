//! JSON Schema format: serialize / deserialize / validate.
//!
//! For JSON Schema the wire payload IS JSON (no binary transcoding), so
//! `serialize` / `deserialize` are near-identity: they validate the bytes
//! against the schema and return them unchanged.
//!
//! Validation is performed via [`jsonschema`] (draft 2020-12 by default in
//! 0.26) using [`jsonschema::validator_for`].

use bytes::Bytes;
use serde_json::Value;

use crate::codec::CodecError;

/// Validate that `json` is a valid instance of JSON Schema `schema`.
///
/// Returns `Ok(())` on success.  On failure, all validation error messages are
/// joined and returned as [`CodecError::Validate`].
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn validate(schema: &str, json: &[u8]) -> Result<(), CodecError> {
    // Parse the schema string as JSON.
    let schema_value: Value = serde_json::from_str(schema)
        .map_err(|e| CodecError::Validate(format!("schema is not valid JSON: {e}")))?;

    // Compile the schema into a validator.
    let validator = jsonschema::validator_for(&schema_value)
        .map_err(|e| CodecError::Validate(format!("invalid JSON Schema: {e}")))?;

    // Parse the payload as JSON.
    let instance: Value = serde_json::from_slice(json)
        .map_err(|e| CodecError::Validate(format!("payload is not valid JSON: {e}")))?;

    // Collect all validation errors.
    let errors: Vec<String> = validator
        .iter_errors(&instance)
        .map(|e| e.to_string())
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(CodecError::Validate(errors.join("; ")))
    }
}

/// Serialize `json` against JSON Schema `schema`.
///
/// The JSON wire format is the JSON document itself (the Confluent JSON serde
/// puts JSON bytes on the wire; framing is added by `wire.rs`).  This function
/// validates the bytes and returns them unchanged.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn serialize(schema: &str, json: &[u8]) -> Result<Bytes, CodecError> {
    validate(schema, json)?;
    Ok(Bytes::copy_from_slice(json))
}

/// Deserialize a JSON-format `payload`.
///
/// Validates `payload` against `schema` and returns the bytes unchanged — the
/// payload is already JSON.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn deserialize(schema: &str, payload: &[u8]) -> Result<Bytes, CodecError> {
    validate(schema, payload)?;
    Ok(Bytes::copy_from_slice(payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str =
        r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}}}"#;

    #[test]
    fn validation_cases() {
        for (name, schema, payload, valid) in [
            ("valid", SCHEMA, br#"{"id":1}"#.as_slice(), true),
            ("wrong_type", SCHEMA, br#"{"id":"x"}"#.as_slice(), false),
            ("missing_required", SCHEMA, br"{}".as_slice(), false),
            ("invalid_payload", SCHEMA, b"not json".as_slice(), false),
            (
                "invalid_schema",
                "not json",
                br#"{"id":1}"#.as_slice(),
                false,
            ),
        ] {
            let result = validate(schema, payload);
            assert2::assert!(result.is_ok() == valid, "case {name}");
        }
    }

    #[test]
    fn codec_operation_cases() {
        for (name, result, expected) in [
            (
                "serialize_valid",
                serialize(SCHEMA, br#"{"id":42}"#),
                Some(br#"{"id":42}"#.as_slice()),
            ),
            (
                "serialize_invalid",
                serialize(SCHEMA, br#"{"id":"not-an-int"}"#),
                None,
            ),
            (
                "deserialize_valid",
                deserialize(SCHEMA, br#"{"id":99}"#),
                Some(br#"{"id":99}"#.as_slice()),
            ),
            ("deserialize_invalid", deserialize(SCHEMA, br"{}"), None),
        ] {
            assert2::assert!(result.as_deref().ok() == expected, "case {name}");
        }
    }
}
