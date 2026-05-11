//! Byte-compatible reader/writer for Apache Kafka's on-disk log format.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-11-crabka-log-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-log/0.0.0")]

mod config;
mod error;
mod index;
mod name;

pub use config::LogConfig;
pub use error::LogError;
