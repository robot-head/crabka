//! Idempotent producer client for Apache Kafka in Rust.
//!
//! Builds on [`crabka_client_core`] for transport. Adds full
//! idempotent-producer semantics: `InitProducerId` on connect, per-batch
//! `(producer_id, producer_epoch, base_sequence)`, retries that re-frame
//! the same `RecordBatch` so the broker's dedup catches them.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use bytes::Bytes;
//! use crabka_client_producer::{Acks, Compression, Producer, ProducerRecord};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let producer = Producer::builder()
//!     .bootstrap("localhost:9092")
//!     .compression(Compression::Lz4)
//!     .acks(Acks::All)
//!     .linger(Duration::from_millis(5))
//!     .build()
//!     .await?;
//!
//! let metadata = producer
//!     .send(ProducerRecord {
//!         topic: "my-topic".into(),
//!         value: Some(Bytes::from("hello")),
//!         ..Default::default()
//!     })
//!     .await
//!     .await??;
//!
//! producer.flush().await?;
//! producer.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Out of scope
//!
//! - Transactions (slice 9).
//! - Persisted producer-state snapshots (broker restart resets sequences).
//! - Custom partitioner trait — sticky+hash only; `ProducerRecord::partition`
//!   bypasses the partitioner per record.
//! - Schema registry / serde glue — `key` and `value` are `Bytes`.

#![doc(html_root_url = "https://docs.rs/crabka-client-producer/0.0.0")]

mod accumulator;
mod builder;
mod compression;
mod error;
mod partitioner;
mod producer;
mod record;
mod sender;
mod transactional;

pub use compression::Compression;
pub use error::ProducerError;
pub use producer::{Acks, Producer};
pub use record::{Header, ProducerRecord, RecordMetadata};
