//! Schema Registry integration for the gRPC gateway.
//!
//! This module gives Confluent-compatible, schema-bound encode and decode on
//! the produce and consume paths. The seam with the rest of the gateway is
//! [`crate::codec`]. [`codec::SchemaRegistryCodec`] implements
//! [`crate::codec::RecordCodec`], and the gateway swaps it in at startup when
//! `--schema-registry-url` is configured.
//!
//! Sub-modules:
//! - [`client`]: HTTP client against a Confluent Schema Registry REST API.
//! - [`wire`]: Confluent binary framing, which is the 5-byte magic and id
//!   header plus the Protobuf message-index prefix.
//! - [`mod@format`]: per-format serialize, deserialize, and validate dispatch
//!   for Avro, JSON Schema, and Protobuf.
//! - [`codec`]: the [`crate::codec::RecordCodec`] impl that joins the above.

pub mod client;
pub mod codec;
pub mod format;
pub mod wire;
