//! Schema Registry integration for the gRPC gateway.
//!
//! Provides Confluent-compatible schema-bound encode/decode on the produce and
//! consume paths. The seam with the rest of the gateway is [`crate::codec`]:
//! [`codec::SchemaRegistryCodec`] implements [`crate::codec::RecordCodec`] and
//! is swapped in at startup when `--schema-registry-url` is configured.
//!
//! Sub-modules:
//! - [`client`] — HTTP client against a Confluent Schema Registry REST API.
//! - [`wire`] — Confluent binary framing (5-byte magic+id header, Protobuf
//!   message-index prefix).
//! - [`mod@format`] — per-format serialize/deserialize/validate dispatch
//!   (Avro / JSON Schema / Protobuf).
//! - [`codec`] — [`crate::codec::RecordCodec`] impl that glues the above.

pub mod client;
pub mod codec;
pub mod format;
pub mod wire;
