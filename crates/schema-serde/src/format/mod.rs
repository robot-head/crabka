//! Per-format typed serializers. Implemented in later tasks.

#[cfg(feature = "avro")]
pub mod avro;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "protobuf")]
pub mod protobuf;
