//! Idempotent producer client for Apache Kafka in Rust.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-12-crabka-client-producer-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-client-producer/0.0.0")]

mod error;

pub use error::ProducerError;
