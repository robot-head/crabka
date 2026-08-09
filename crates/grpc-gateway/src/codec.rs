//! Pluggable record codec.
//!
//! `RawCodec` is the identity codec over opaque bytes. A schema-aware codec
//! implements the same trait, so the front-ends and the produce and consume
//! cores need no format-specific branches.
//!
//! The seam is async and fallible. A schema-bound codec talks to a remote
//! registry over the network, and it can reject a payload during validation or
//! serialization. `encode` and `decode` are therefore `async` and return
//! [`CodecError`]. `RawCodec` is a pure pass-through and never errors.

use bytes::Bytes;

/// Payload format a schema is expressed in. This is the Confluent `schemaType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaFormat {
    Avro,
    Json,
    Protobuf,
}

/// Selects the schema that the produce path serializes a structured JSON value
/// against. A `None` `subject` resolves through `TopicNameStrategy`, which is
/// `<topic>-value`. An explicit `id` pins a registered schema version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSelector {
    pub subject: Option<String>,
    pub id: Option<i32>,
    pub format: SchemaFormat,
}

/// Resolved schema metadata attached to a decoded value. It holds the id read
/// from the Confluent frame plus the resolved subject and format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMeta {
    pub subject: String,
    pub id: i32,
    pub format: SchemaFormat,
}

/// The value to encode.
///
/// `Raw` is already-serialized opaque bytes. `Structured` is a JSON document
/// that the gateway serializes into `schema.format` before it frames the value.
/// The data is owned rather than borrowed, which keeps the type simple.
#[derive(Debug, Clone)]
pub enum EncodeBody {
    /// Already-serialized bytes. When a schema is bound, they are framed as
    /// they are.
    Raw(Bytes),
    /// A JSON document the codec serializes into the selected schema's format.
    Structured { json: Bytes, schema: SchemaSelector },
}

/// The result of decoding a wire value.
///
/// It holds the de-framed payload bytes. When the value was Confluent-framed,
/// it also holds the resolved schema metadata and an optional structured JSON
/// view.
#[derive(Debug, Clone)]
pub struct Decoded {
    pub value: Bytes,
    pub schema: Option<SchemaMeta>,
    pub json: Option<Bytes>,
}

/// A codec failure.
///
/// `Registry` is a transport or availability error against the remote registry,
/// and it is retriable. The other variants are payload-level faults and are not
/// retriable, because the same bytes fail the same way on a retry.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("schema registry error: {0}")]
    Registry(String),
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("validation error: {0}")]
    Validate(String),
    #[error("framing error: {0}")]
    Framing(String),
}

/// Encodes and decodes record values on the way to and from Kafka.
#[async_trait::async_trait]
pub trait RecordCodec: Send + Sync + 'static {
    /// Encode a record's value to the wire. A schema-bound codec adds Confluent
    /// framing. `RawCodec` returns the bytes unchanged.
    async fn encode(&self, topic: &str, body: EncodeBody) -> Result<Bytes, CodecError>;
    /// Decode a wire value. Strip the framing, then return the payload with
    /// optional schema metadata and a structured JSON view. `RawCodec` returns
    /// the bytes unchanged and no metadata.
    async fn decode(&self, topic: &str, value: Bytes) -> Result<Decoded, CodecError>;
}

/// Identity codec, an opaque pass-through. It is the default codec and it uses
/// no schema registry.
#[derive(Debug, Clone, Copy, Default)]
pub struct RawCodec;

#[async_trait::async_trait]
impl RecordCodec for RawCodec {
    async fn encode(&self, _topic: &str, body: EncodeBody) -> Result<Bytes, CodecError> {
        // RawCodec ignores schemas entirely: raw bytes pass through, and a
        // structured body's JSON is emitted as-is (the bytes the client sent).
        Ok(match body {
            EncodeBody::Raw(b) => b,
            EncodeBody::Structured { json, .. } => json,
        })
    }

    async fn decode(&self, _topic: &str, value: Bytes) -> Result<Decoded, CodecError> {
        Ok(Decoded {
            value,
            schema: None,
            json: None,
        })
    }
}
