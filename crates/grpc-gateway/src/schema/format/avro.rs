//! Avro format: serialize / deserialize / validate stubs.
//!
//! Implementation uses [`apache_avro`] for schema parsing, binary encoding,
//! and value round-tripping. The `schema` argument is a JSON Avro schema
//! string; `json` / `payload` are JSON-encoded / Avro-binary respectively.

#![allow(clippy::todo, unused_variables)]

use bytes::Bytes;

use crate::codec::CodecError;

/// Serialize `json` (a JSON-encoded value) into Avro binary using `schema`.
pub fn serialize(_schema: &str, _json: &[u8]) -> Result<Bytes, CodecError> {
    todo!()
}

/// Deserialize Avro binary `payload` back to JSON bytes using `schema`.
pub fn deserialize(_schema: &str, _payload: &[u8]) -> Result<Bytes, CodecError> {
    todo!()
}

/// Validate that `json` is a valid Avro value for `schema`.
pub fn validate(_schema: &str, _json: &[u8]) -> Result<(), CodecError> {
    todo!()
}
