//! Idempotent producer client for Apache Kafka in Rust.

#![doc(html_root_url = "https://docs.rs/crabka-client-producer/0.0.0")]

mod accumulator;
mod compression;
mod error;
mod partitioner;
mod producer;
mod record;
mod sender;

pub use compression::Compression;
pub use error::ProducerError;
pub use producer::{Acks, Producer};
pub use record::{Header, ProducerRecord, RecordMetadata};
