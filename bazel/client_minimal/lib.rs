//! Lean re-export facade over the Crabka Kafka client crates.
//!
//! This crate bundles the producer, consumer, admin, and core clients, and the
//! Avro and Protobuf schema serdes, behind one dependency. An embedder can thus
//! pull `@crabka//:client_minimal` and get a slim closure.
//!
//! The crate excludes these on purpose:
//!   * the columnar streams client (`polars`, `polars-arrow`, `arrow`)
//!   * the JSON-Schema serde path (`jsonschema`, `schemars`)
//!
//! This crate is Bazel-only. It is not a Cargo workspace member.

pub use crabka_client_admin as admin;
pub use crabka_client_consumer as consumer;
pub use crabka_client_core as core;
pub use crabka_client_producer as producer;
pub use crabka_schema_serde as schema_serde;
