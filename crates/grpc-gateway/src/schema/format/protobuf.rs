//! Protobuf format: serialize / deserialize / validate stubs.
//!
//! Implementation uses [`protox_parse`] for schema parsing (`.proto` text →
//! [`prost_reflect::DescriptorPool`]) and [`prost_reflect::DynamicMessage`]
//! for JSON↔binary transcoding.
//!
//! After Confluent framing the wire format carries a message-index prefix
//! (handled by [`crate::schema::wire::strip_proto_index`] /
//! [`crate::schema::wire::prepend_proto_index`]) before the raw proto bytes.

#![allow(clippy::todo, unused_variables)]

use bytes::Bytes;

use crate::codec::CodecError;

/// Serialize `json` (a JSON-encoded proto value) into Protobuf binary using
/// the `.proto` schema in `schema`.
pub fn serialize(_schema: &str, _json: &[u8]) -> Result<Bytes, CodecError> {
    todo!()
}

/// Deserialize Protobuf binary `payload` back to JSON bytes using the `.proto`
/// schema in `schema`.
pub fn deserialize(_schema: &str, _payload: &[u8]) -> Result<Bytes, CodecError> {
    todo!()
}

/// Validate that `json` round-trips cleanly through the Protobuf schema
/// (i.e. `json → proto → json` produces a semantically equivalent value).
pub fn validate(_schema: &str, _json: &[u8]) -> Result<(), CodecError> {
    todo!()
}
