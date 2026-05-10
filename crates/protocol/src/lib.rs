//! Kafka wire protocol codec.
//!
//! See the design document at
//! `docs/superpowers/specs/2026-05-10-crabka-rust-rewrite-design.md`
//! in the apache/kafka repo for the project rationale.

mod error;

pub use error::ProtocolError;
