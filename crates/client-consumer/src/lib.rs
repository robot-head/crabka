//! Subscribe-style consumer client for Apache Kafka in Rust.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-11-crabka-consumer-groups-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-client-consumer/0.0.0")]

mod error;

pub use error::ConsumerError;
