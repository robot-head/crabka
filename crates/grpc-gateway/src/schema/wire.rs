//! Confluent binary framing helpers.
//!
//! All Confluent-framed values share a 5-byte header:
//!
//! ```text
//! +--------+--------------------------------+
//! | 0x00   |  magic byte                    |
//! | i32 BE |  schema id                     |
//! +--------+--------------------------------+
//! ```
//!
//! Protobuf values additionally carry a variable-length message-index prefix
//! immediately after the 5-byte header (see [`strip_proto_index`] /
//! [`prepend_proto_index`]).

#![allow(clippy::todo, unused_variables)]

use bytes::Bytes;

use crate::codec::{CodecError, SchemaFormat};

/// Encode a payload with the Confluent 5-byte magic+id framing header.
///
/// For `Protobuf`, the caller is responsible for prepending the message-index
/// prefix (via [`prepend_proto_index`]) *before* calling this function.
#[must_use]
pub fn encode_frame(_id: i32, _fmt: SchemaFormat, _payload: &[u8]) -> Bytes {
    todo!("Task: wire framing")
}

/// Decode a Confluent-framed value.
///
/// Returns `(schema_id, payload)` where `payload` is the bytes *after* the
/// 5-byte header.  For Protobuf, the caller must strip the message-index
/// prefix from the returned payload via [`strip_proto_index`].
pub fn decode_frame(_bytes: &[u8]) -> Result<(i32, Vec<u8>), CodecError> {
    todo!()
}

/// Strip the variable-length Protobuf message-index zigzag prefix from a
/// post-header payload and return the raw proto bytes.
pub fn strip_proto_index(_payload: &[u8]) -> Result<Vec<u8>, CodecError> {
    todo!()
}

/// Prepend the Protobuf message-index prefix (a single `[0x00]` for top-level
/// schema, i.e. index 0) to `payload`.
#[must_use]
pub fn prepend_proto_index(_payload: &[u8]) -> Vec<u8> {
    todo!()
}
