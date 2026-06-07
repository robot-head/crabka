//! Pluggable record codec. v1 ships `RawCodec` (identity, opaque bytes).
//! The deferred Schema Registry component adds a `SchemaRegistryCodec`
//! that implements this same trait — front-ends/cores never change.
//!
//! The seam is **async + fallible**: a schema-bound codec talks to a remote
//! registry (network) and can reject a payload (validation/serialization), so
//! `encode`/`decode` are `async` and return [`CodecError`]. `RawCodec` is a
//! pure pass-through and never errors.

use bytes::Bytes;

/// Payload format a schema is expressed in (Confluent `schemaType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaFormat {
    Avro,
    Json,
    Protobuf,
}

/// Selects the schema to serialize a structured (JSON) value against on the
/// produce path. A `None` `subject` resolves via `TopicNameStrategy`
/// (`<topic>-value`); an explicit `id` pins a registered schema version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSelector {
    pub subject: Option<String>,
    pub id: Option<i32>,
    pub format: SchemaFormat,
}

/// Resolved schema metadata attached to a decoded value (the id read from the
/// Confluent frame plus its resolved subject/format).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMeta {
    pub subject: String,
    pub id: i32,
    pub format: SchemaFormat,
}

/// The value to encode. `Raw` is already-serialized opaque bytes; `Structured`
/// is a JSON document the gateway serializes into `schema.format` before
/// framing. Owned (not borrowed) for simplicity.
#[derive(Debug, Clone)]
pub enum EncodeBody {
    /// Already-serialized bytes (framed as-is when a schema is bound).
    Raw(Bytes),
    /// A JSON document the codec serializes into the selected schema's format.
    Structured { json: Bytes, schema: SchemaSelector },
}

/// The result of decoding a wire value: the (de-framed) payload bytes, plus —
/// when the value was Confluent-framed — the resolved schema metadata and an
/// optional structured (JSON) view.
#[derive(Debug, Clone)]
pub struct Decoded {
    pub value: Bytes,
    pub schema: Option<SchemaMeta>,
    pub json: Option<Bytes>,
}

/// A codec failure. `Registry` is a transport/availability error against the
/// remote registry (retriable); the rest are payload-level faults (not
/// retriable — retrying the same bytes fails identically).
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

/// Encodes/decodes record values on the way to/from Kafka.
#[async_trait::async_trait]
pub trait RecordCodec: Send + Sync + 'static {
    /// Encode a record's value to the wire (Confluent framing when
    /// schema-bound). `RawCodec` returns the bytes verbatim.
    async fn encode(&self, topic: &str, body: EncodeBody) -> Result<Bytes, CodecError>;
    /// Decode a wire value: strip framing, return the payload plus optional
    /// schema metadata and a structured (JSON) view. `RawCodec` returns the
    /// bytes verbatim with no metadata.
    async fn decode(&self, topic: &str, value: Bytes) -> Result<Decoded, CodecError>;
}

/// Identity codec — opaque pass-through. The default codec; no schema registry.
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
