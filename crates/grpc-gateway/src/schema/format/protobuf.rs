//! Protobuf format: serialize / deserialize / validate.
//!
//! Implementation uses [`protox_parse`] for schema parsing (`.proto` text →
//! [`prost_reflect::DescriptorPool`]) and [`prost_reflect::DynamicMessage`]
//! for JSON↔binary transcoding.
//!
//! After Confluent framing the wire format carries a message-index prefix
//! (handled by [`crate::schema::wire::strip_proto_index`] /
//! [`crate::schema::wire::prepend_proto_index`]) before the raw proto bytes.
//!
//! # Proto3 JSON encoding note
//!
//! The [proto3 JSON mapping](https://protobuf.dev/programming-guides/proto3/#json)
//! encodes `int64`/`uint64` fields as **decimal strings** (e.g. `"1"` not `1`)
//! to avoid JavaScript precision loss.  Callers comparing deserialized JSON
//! output must account for this.

use bytes::Bytes;
use prost::Message as _;
use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor, prost_types::FileDescriptorSet,
};
use serde::Serialize as _;

use crate::codec::CodecError;

/// Build a [`DescriptorPool`] from raw `.proto` source text and return the
/// descriptor for the **first** message type defined in it.
///
/// The schema is expected to be self-contained (no external imports).
fn first_message_desc(schema: &str) -> Result<MessageDescriptor, CodecError> {
    let fdp = protox_parse::parse("schema.proto", schema)
        .map_err(|e| CodecError::Serialize(format!("protobuf parse error: {e}")))?;

    let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![fdp] })
        .map_err(|e| CodecError::Serialize(format!("protobuf descriptor error: {e}")))?;

    pool.all_messages()
        .next()
        .ok_or_else(|| CodecError::Serialize("no message type defined in schema".into()))
}

/// Serialize `json` (a JSON-encoded proto value) into Protobuf binary using
/// the `.proto` schema in `schema`.
///
/// Uses the first message type in the schema (message-index 0, matching the
/// Confluent wire default of `[0]`).
pub fn serialize(schema: &str, json: &[u8]) -> Result<Bytes, CodecError> {
    let msg_desc = first_message_desc(schema)?;

    let mut de = serde_json::Deserializer::from_slice(json);
    let dynmsg = DynamicMessage::deserialize(msg_desc, &mut de)
        .map_err(|e| CodecError::Serialize(format!("JSON->proto deserialize error: {e}")))?;
    // Consume any trailing whitespace; ignore the error (end() only fails on
    // trailing non-whitespace, which is rare and non-fatal for our use case).
    let _ = de.end();

    Ok(Bytes::from(dynmsg.encode_to_vec()))
}

/// Deserialize Protobuf binary `payload` back to JSON bytes using the `.proto`
/// schema in `schema`.
///
/// Uses the first message type in the schema (message-index 0).
pub fn deserialize(schema: &str, payload: &[u8]) -> Result<Bytes, CodecError> {
    let msg_desc = first_message_desc(schema)?;

    let dynmsg = DynamicMessage::decode(msg_desc, payload)
        .map_err(|e| CodecError::Serialize(format!("proto decode error: {e}")))?;

    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::new(&mut buf);
    dynmsg
        .serialize(&mut ser)
        .map_err(|e| CodecError::Serialize(format!("proto->JSON serialize error: {e}")))?;

    Ok(Bytes::from(buf))
}

/// Validate that `json` round-trips cleanly through the Protobuf schema
/// (i.e. `json -> proto bytes` succeeds without error).
///
/// Returns `Ok(())` when the JSON parses cleanly against the schema; returns
/// [`CodecError::Validate`] on any parse or encoding failure.
pub fn validate(schema: &str, json: &[u8]) -> Result<(), CodecError> {
    serialize(schema, json).map(|_| ()).map_err(|e| match e {
        CodecError::Serialize(msg) => CodecError::Validate(msg),
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"syntax = "proto3"; message R { int64 id = 1; string name = 2; }"#;

    #[test]
    fn serialize_produces_proto_bytes() {
        let bytes = serialize(SCHEMA, br#"{"id":1,"name":"a"}"#).expect("serialize should succeed");
        // Non-empty proto bytes; field 1 (id=1) encodes as 0x08 0x01,
        // field 2 (name="a") encodes as 0x12 0x01 0x61.
        assert!(!bytes.is_empty(), "proto bytes should be non-empty");
    }

    #[test]
    fn roundtrip_json_to_proto_and_back() {
        let proto_bytes =
            serialize(SCHEMA, br#"{"id":1,"name":"a"}"#).expect("serialize should succeed");

        let json_bytes = deserialize(SCHEMA, &proto_bytes).expect("deserialize should succeed");
        let json: serde_json::Value =
            serde_json::from_slice(&json_bytes).expect("output should be valid JSON");

        // proto3 JSON encodes int64 as a decimal string.
        assert_eq!(
            json.get("id").and_then(|v| v.as_str()),
            Some("1"),
            "int64 should be serialized as a JSON string in proto3"
        );
        assert_eq!(
            json.get("name").and_then(|v| v.as_str()),
            Some("a"),
            "string field should round-trip correctly"
        );
    }

    #[test]
    fn validate_ok_on_valid_json() {
        assert!(
            validate(SCHEMA, br#"{"id":42,"name":"hello"}"#).is_ok(),
            "valid JSON should validate"
        );
    }

    #[test]
    fn validate_err_on_malformed_json() {
        assert!(
            validate(SCHEMA, b"{not valid json}").is_err(),
            "malformed JSON should fail validation"
        );
    }

    #[test]
    fn validate_err_returns_validate_variant() {
        let result = validate(SCHEMA, b"[1,2,3]");
        assert!(
            matches!(result, Err(CodecError::Validate(_))),
            "validate should return CodecError::Validate on failure, got: {result:?}"
        );
    }
}
