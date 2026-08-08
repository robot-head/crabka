//! Confluent-compatible schema serdes for Crabka clients.
//!
//! This crate frames payloads as `magic(0x00) | schema_id(4 BE) | body`. A
//! Protobuf payload also carries a message-index. The crate registers schemas
//! with a Confluent-compatible Schema Registry and resolves them from it. The
//! typed serializers here do not depend on one client: `crabka-client-streams`
//! bridges them now, and other clients can bridge them later.

pub mod cache;
pub mod error;
pub mod registry;
pub mod subject;
pub mod wire;

pub mod format;

pub use cache::{
    CacheConfig, DEFAULT_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF,
    DEFAULT_SCHEMA_FETCH_RETRY_MAX_BACKOFF, RegisterMode, SchemaCache, SchemaFetchRetryPolicy,
    default_registry, set_default_registry,
};
pub use error::SchemaSerdeError;
#[cfg(feature = "avro")]
pub use format::avro::AvroSerde;
#[cfg(feature = "json")]
pub use format::json::JsonSerde;
#[cfg(feature = "protobuf")]
pub use format::protobuf::ProtobufSerde;
pub use registry::RegistryClient;
pub use subject::{Role, SchemaKind, SubjectStrategy, TopicNameStrategy};
