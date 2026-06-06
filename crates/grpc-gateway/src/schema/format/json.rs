//! JSON Schema format: serialize / deserialize / validate stubs.
//!
//! For JSON Schema the wire payload IS JSON (no binary transcoding), so
//! `serialize` / `deserialize` are near-identity; `validate` applies the
//! schema constraint via [`jsonschema`].

#![allow(clippy::todo, unused_variables)]

use bytes::Bytes;

use crate::codec::CodecError;

/// Serialize `json` against JSON Schema `schema`.
///
/// For the JSON format the payload is JSON already; this validates and
/// returns the bytes unchanged (or re-serialized canonically).
pub fn serialize(_schema: &str, _json: &[u8]) -> Result<Bytes, CodecError> {
    todo!()
}

/// Deserialize a JSON-format `payload` — effectively the identity for
/// well-formed JSON, but validates against `schema`.
pub fn deserialize(_schema: &str, _payload: &[u8]) -> Result<Bytes, CodecError> {
    todo!()
}

/// Validate that `json` is a valid instance of JSON Schema `schema`.
pub fn validate(_schema: &str, _json: &[u8]) -> Result<(), CodecError> {
    todo!()
}
