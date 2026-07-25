//! Idempotent producer client for Apache Kafka in Rust.
//!
//! Builds on [`crabka_client_core`] for transport. Adds full
//! idempotent-producer semantics: `InitProducerId` on connect, per-batch
//! `(producer_id, producer_epoch, base_sequence)`, retries that re-frame
//! the same `RecordBatch` so the broker's dedup catches them.
//!
//! Also supports transactional (exactly-once) production —
//! `init_transactions`, `begin_transaction` (which returns a [`Transaction`]
//! guard whose `commit`/`abort` finishes it), and `send_offsets_to_transaction`
//! for the consume-process-produce (KIP-447) pattern.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//!
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
//! ## Capabilities and boundaries
//!
//! This crate owns producer-facing semantics: batching, compression,
//! idempotence, retries, per-record partition overrides, transactional RPCs,
//! and `send_offsets_to_transaction` for consume-process-produce flows. The
//! built-in partitioner is sticky/hash based; set `ProducerRecord::partition`
//! to pin an individual record. Serialization is deliberately caller-owned —
//! `key` and `value` are raw `Bytes`, so schema-registry or serde integration
//! can be layered without constraining the producer API.

#![doc(html_root_url = "https://docs.rs/crabka-client-producer/0.3.9")]

mod accumulator;
mod builder;
#[cfg(test)]
mod client_failover_model;
mod compression;
mod error;
mod partitioner;
mod producer;
mod record;
mod sender;
mod transactional;
mod transport;

pub use builder::{
    DEFAULT_PRODUCER_BATCH_BYTES, DEFAULT_PRODUCER_COMPRESSION, DEFAULT_PRODUCER_FLUSH_TIMEOUT,
    DEFAULT_PRODUCER_INIT_MAX_BACKOFF, DEFAULT_PRODUCER_INIT_RETRY_TIMEOUT,
    DEFAULT_PRODUCER_LINGER, DEFAULT_PRODUCER_MAX_IN_FLIGHT, DEFAULT_PRODUCER_REQUEST_TIMEOUT,
    DEFAULT_PRODUCER_RETRIES, DEFAULT_PRODUCER_RETRY_BACKOFF,
    DEFAULT_PRODUCER_ROUTING_RETRY_BUDGET, DEFAULT_PRODUCER_TRANSACTION_TIMEOUT,
    ProducerFlushTimeout, ProducerRetryPolicy, ProducerThroughputPolicy,
};
pub use compression::Compression;
pub use crabka_client_consumer::ConsumerGroupMetadata;
pub use error::ProducerError;
pub use producer::{Acks, Producer};
pub use record::{Header, ProducerRecord, RecordMetadata};
pub use transactional::{EndTransactionError, OwnedTransaction, Transaction};
