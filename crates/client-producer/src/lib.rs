//! Idempotent producer client for Apache Kafka in Rust.

#![doc(html_root_url = "https://docs.rs/crabka-client-producer/0.0.0")]

mod error;
mod record;

pub use error::ProducerError;
pub use record::{Header, ProducerRecord, RecordMetadata};
