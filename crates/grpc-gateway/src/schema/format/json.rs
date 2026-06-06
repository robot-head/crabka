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
pub fn serialize(schema: &str, json: &[u8]) -> Result<Bytes, CodecError> {
    validate(schema, json)?;
    Ok(Bytes::copy_from_slice(json))
}

/// Deserialize a JSON-format `payload`.
///
/// Validates `payload` against `schema` and returns the bytes unchanged — the
/// payload is already JSON.
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
    fn validate_valid_instance() {
        assert!(validate(SCHEMA, br#"{"id":1}"#).is_ok());
    }

    #[test]
    fn validate_wrong_type() {
        let err = validate(SCHEMA, br#"{"id":"x"}"#).unwrap_err();
        assert!(matches!(err, CodecError::Validate(_)));
    }

    #[test]
    fn validate_missing_required() {
        let err = validate(SCHEMA, br"{}").unwrap_err();
        assert!(matches!(err, CodecError::Validate(_)));
    }

    #[test]
    fn validate_invalid_json_payload() {
        let err = validate(SCHEMA, b"not json").unwrap_err();
        assert!(matches!(err, CodecError::Validate(_)));
    }

    #[test]
    fn validate_invalid_schema_string() {
        let err = validate("not json", br#"{"id":1}"#).unwrap_err();
        assert!(matches!(err, CodecError::Validate(_)));
    }

    #[test]
    fn serialize_returns_input_bytes_on_valid() {
        let input = br#"{"id":42}"#;
        let result = serialize(SCHEMA, input).unwrap();
        assert_eq!(result.as_ref(), input);
    }

    #[test]
    fn serialize_rejects_invalid() {
        assert!(serialize(SCHEMA, br#"{"id":"not-an-int"}"#).is_err());
    }

    #[test]
    fn deserialize_returns_input_bytes_on_valid() {
        let input = br#"{"id":99}"#;
        let result = deserialize(SCHEMA, input).unwrap();
        assert_eq!(result.as_ref(), input);
    }

    #[test]
    fn deserialize_rejects_invalid() {
        assert!(deserialize(SCHEMA, br"{}").is_err());
    }
}
