//! Per-format typed serializers/deserializers. Each format owns the body
//! encoding; framing + id resolution are shared here.
//!
//! Serdes are **topic-aware** (matching JVM Kafka's `serialize(topic, data)`):
//! a serde carries its key/value [`Role`] but derives its registry subject
//! (`<topic>-key` / `<topic>-value`) from the topic passed at call time, so one
//! serde instance works across topics.

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

/// Serialize `T` to a Confluent-framed payload for `topic` (subject derived
/// from the topic + the serde's role).
pub trait SchemaSerializer<T>: Send + Sync + 'static {
    /// Frame `value`: resolve the id from the cache, encode the body, prepend
    /// the wire header. Errors if pre-warm has not resolved the subject id.
    fn serialize(&self, topic: &str, value: &T) -> Result<Bytes, SchemaSerdeError>;
}

/// Deserialize a Confluent-framed payload into `T`. `topic` is accepted for
/// symmetry/diagnostics; the writer schema is resolved by the framed id.
pub trait SchemaDeserializer<T>: Send + Sync + 'static {
    /// Decode `bytes`: strip the header, fetch the writer schema by id, decode
    /// the body. May return `WriterSchemaPending` (retriable) on a cache miss.
    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<T, SchemaSerdeError>;
}

/// Pre-register a serde's subject for `topic` so pre-warm can resolve its id
/// before processing begins (the serialize hot path is then a sync cache read).
pub trait SchemaSubject: Send + Sync + 'static {
    /// Intern `<topic>-{key,value}` (per the serde's role) into the cache.
    fn register_subject(&self, topic: &str);
}

/// Shared bound state every format serde carries: the cache, the role, and the
/// local type's schema (kind + text) used for registration/lookup.
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

    /// The resolved schema id for `topic`, or an error if pre-warm hasn't run.
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
