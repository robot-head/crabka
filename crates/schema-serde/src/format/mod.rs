//! Per-format typed serializers and deserializers.
//!
//! Each format owns its body encoding. This module holds the framing and the id
//! resolution that all formats share.
//!
//! Serdes are **topic-aware**, like JVM Kafka's `serialize(topic, data)`. A
//! serde carries its key/value [`Role`], but it takes its registry subject from
//! the topic given at call time. The subject is `<topic>-key` or
//! `<topic>-value`, so one serde instance works across topics.

#[cfg(feature = "avro")]
pub mod avro;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "protobuf")]
pub mod protobuf;

use bytes::Bytes;
#[cfg(any(feature = "avro", feature = "json", feature = "protobuf"))]
use {
    crate::cache::SchemaCache,
    crate::subject::{Role, SchemaKind},
    std::sync::Arc,
};

use crate::error::SchemaSerdeError;

/// Serialize `T` to a Confluent-framed payload for `topic`.
///
/// The subject comes from the topic and the serde's role.
pub trait SchemaSerializer<T>: Send + Sync + 'static {
    /// Frame `value`: resolve the id from the cache, encode the body, then
    /// prepend the wire header.
    ///
    /// This method errors if pre-warm has not resolved the subject id.
    ///
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    fn serialize(&self, topic: &str, value: &T) -> Result<Bytes, SchemaSerdeError>;
}

/// Deserialize a Confluent-framed payload into `T`.
///
/// The method takes `topic` for symmetry and for diagnostics. The framed id
/// resolves the writer schema.
pub trait SchemaDeserializer<T>: Send + Sync + 'static {
    /// Decode `bytes`: strip the header, fetch the writer schema by id, then
    /// decode the body.
    ///
    /// A cache miss can return `WriterSchemaPending`. That error is retriable.
    ///
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<T, SchemaSerdeError>;
}

/// Pre-register a serde's subject for `topic`.
///
/// Pre-warm can then resolve the id before the client processes records. The
/// serialize hot path is then a synchronous cache read.
pub trait SchemaSubject: Send + Sync + 'static {
    /// Intern `<topic>-{key,value}` into the cache, per the serde's role.
    fn register_subject(&self, topic: &str);
}

/// Shared bound state every format serde carries.
///
/// The state holds the cache, the role, and the local type's schema. The schema
/// is a kind and a text. The register and lookup calls use it.
#[cfg(any(feature = "avro", feature = "json", feature = "protobuf"))]
#[derive(Clone)]
pub(crate) struct Binding {
    pub cache: Arc<SchemaCache>,
    pub role: Role,
    pub kind: SchemaKind,
    pub schema: String,
    pub message_type: Option<String>,
}

#[cfg(any(feature = "avro", feature = "json", feature = "protobuf"))]
impl Binding {
    /// The registry subject for `topic` under this serde's role.
    pub(crate) fn subject(&self, topic: &str) -> String {
        self.cache.subject(topic, self.role)
    }

    /// The resolved schema id for `topic`, or an error if pre-warm has not run.
    pub(crate) fn id(&self, topic: &str) -> Result<u32, SchemaSerdeError> {
        let subject = self.subject(topic);
        self.cache.id_for_subject(&subject).ok_or_else(|| {
            SchemaSerdeError::Schema(format!("id for {subject} not resolved (run prewarm)"))
        })
    }

    /// Intern this subject for pre-warm.
    pub(crate) fn register(&self, topic: &str) {
        let subject = self.subject(topic);
        self.cache.intern(
            &subject,
            self.kind,
            &self.schema,
            self.message_type.as_deref(),
        );
    }
}
