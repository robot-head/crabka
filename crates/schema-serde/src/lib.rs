//! Confluent-compatible schema serdes for Crabka clients.
//!
//! Frames payloads as `magic(0x00) | schema_id(4 BE) | body` (plus a Protobuf
//! message-index), with schemas registered against and resolved from a
//! Confluent-compatible Schema Registry. Client-agnostic: the typed serializers
//! here are bridged into `crabka-client-streams` (and later other clients).

pub mod cache;
pub mod error;
pub mod registry;
pub mod subject;
pub mod wire;

pub mod format;

pub use error::SchemaSerdeError;
pub use registry::RegistryClient;
