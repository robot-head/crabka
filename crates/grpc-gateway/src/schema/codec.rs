//! [`RecordCodec`] implementation backed by the Confluent Schema Registry.
//!
//! [`SchemaRegistryCodec`] wraps a [`super::client::SchemaRegistryClient`]
//! and, on the encode path, fetches (or registers) the schema, serializes the
//! structured body into the wire format, and prepends the Confluent frame.
//! On the decode path it strips the frame, looks up the schema by id, and
//! optionally deserializes the payload to a JSON view.

#![allow(clippy::todo, unused_variables)]

use std::sync::Arc;

use bytes::Bytes;

use super::client::SchemaRegistryClient;
use crate::codec::{CodecError, Decoded, EncodeBody};

/// A [`crate::codec::RecordCodec`] that encodes/decodes record values via a
/// Confluent Schema Registry.
///
/// Constructed once at gateway startup and shared across all connection
/// handlers via `Arc`.
pub struct SchemaRegistryCodec {
    /// The Schema Registry HTTP client (shared, owns the caches).
    pub client: Arc<SchemaRegistryClient>,
    /// When `true`, the codec emits the raw (Confluent-framed) bytes on the
    /// decode path without transcoding to JSON; the gateway forwards them
    /// verbatim.  When `false` (default) a JSON view is synthesized.
    pub frame_raw: bool,
}

#[async_trait::async_trait]
impl crate::codec::RecordCodec for SchemaRegistryCodec {
    async fn encode(&self, _topic: &str, _body: EncodeBody) -> Result<Bytes, CodecError> {
        todo!()
    }

    async fn decode(&self, _topic: &str, _value: Bytes) -> Result<Decoded, CodecError> {
        todo!()
    }
}
