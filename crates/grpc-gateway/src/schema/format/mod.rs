//! Per-format serialize / deserialize / validate dispatch.
//!
//! [`serialize`], [`deserialize`], and [`validate`] are the three operations
//! the gateway codec needs; each dispatches to the appropriate sub-module
//! based on the [`SchemaFormat`].

pub mod avro;
pub mod json;
pub mod protobuf;

use bytes::Bytes;

use crate::codec::{CodecError, SchemaFormat};

/// Serialize a JSON-encoded value (`json`) into the wire format for `fmt`
/// using `schema` as the schema descriptor.
///
/// Returns the serialized bytes ready for Confluent framing.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn serialize(fmt: SchemaFormat, schema: &str, json: &[u8]) -> Result<Bytes, CodecError> {
    match fmt {
        SchemaFormat::Avro => avro::serialize(schema, json),
        SchemaFormat::Json => json::serialize(schema, json),
        SchemaFormat::Protobuf => protobuf::serialize(schema, json),
    }
}

/// Deserialize a wire payload (`payload`) in `fmt` back to a JSON-encoded
/// value using `schema` as the schema descriptor.
///
/// Returns the JSON bytes.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn deserialize(fmt: SchemaFormat, schema: &str, payload: &[u8]) -> Result<Bytes, CodecError> {
    match fmt {
        SchemaFormat::Avro => avro::deserialize(schema, payload),
        SchemaFormat::Json => json::deserialize(schema, payload),
        SchemaFormat::Protobuf => protobuf::deserialize(schema, payload),
    }
}

/// Validate that `json` is a valid instance of `schema` in `fmt`.
///
/// Returns `Ok(())` on success, [`CodecError::Validate`] on failure.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub fn validate(fmt: SchemaFormat, schema: &str, json: &[u8]) -> Result<(), CodecError> {
    match fmt {
        SchemaFormat::Avro => avro::validate(schema, json),
        SchemaFormat::Json => json::validate(schema, json),
        SchemaFormat::Protobuf => protobuf::validate(schema, json),
    }
}
