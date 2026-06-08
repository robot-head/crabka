//! Per-format typed serializers/deserializers. Each format owns the body
//! encoding; framing + id resolution are shared here.

#[cfg(feature = "avro")]
pub mod avro;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "protobuf")]
pub mod protobuf;

use std::sync::Arc;

use bytes::Bytes;

use crate::cache::SchemaCache;
use crate::error::SchemaSerdeError;

/// Serialize `T` to a Confluent-framed payload for a bound subject.
pub trait SchemaSerializer<T>: Send + Sync + 'static {
    /// Frame `value`: resolve the id from the cache, encode the body, prepend
    /// the wire header. Errors if pre-warm has not resolved the subject id.
    fn serialize(&self, value: &T) -> Result<Bytes, SchemaSerdeError>;
}

/// Deserialize a Confluent-framed payload into `T`.
pub trait SchemaDeserializer<T>: Send + Sync + 'static {
    /// Decode `bytes`: strip the header, fetch the writer schema by id, decode
    /// the body. May return `WriterSchemaPending` (retriable) on a cache miss.
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SchemaSerdeError>;
}

/// Shared bound state every format serde carries.
// used by feature-gated format serdes
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct Binding {
    pub cache: Arc<SchemaCache>,
    pub subject: String,
}

impl Binding {
    // used by feature-gated format serdes
    #[allow(dead_code)]
    pub(crate) fn id(&self) -> Result<u32, SchemaSerdeError> {
        self.cache.id_for_subject(&self.subject).ok_or_else(|| {
            SchemaSerdeError::Schema(format!("id for {} not resolved", self.subject))
        })
    }
}
