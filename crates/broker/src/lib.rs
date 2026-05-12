//! Single-node Apache Kafka-compatible broker (MVP).
//!
//! `crabka-broker` ships a library + binary that an unmodified JVM
//! Kafka client can produce records to and consume from. It is the
//! smallest demonstrable artifact in the Crabka stack.
//!
//! # What this crate does
//!
//! - Accepts TCP connections speaking the Kafka wire protocol.
//! - Handles `ApiVersions`, `Metadata`, `CreateTopics`, `DeleteTopics`,
//!   `Produce`, `Fetch` (with long-poll), `ListOffsets`,
//!   `DescribeConfigs`, and a stub `FindCoordinator`.
//! - Persists records via [`crabka_log`]; one [`Log`](crabka_log::Log)
//!   per (topic, partition) under `<log_dir>/<topic>-<partition>/`.
//! - Reconstructs its in-memory metadata image from the directory
//!   layout on startup.
//!
//! # What this crate doesn't do
//!
//! - Replication, leader election, ISR (slice 8).
//! - `KRaft` metadata quorum (slice 7) — the metadata image is in-memory.
//! - Consumer groups, offset commits, coordinators (slice 5) —
//!   `FindCoordinator` stubs to `COORDINATOR_NOT_AVAILABLE`; consumers
//!   must use `--partition` to bypass groups.
//! - Idempotent / transactional producers (slices 6, 9).
//! - Authentication, TLS, SASL, ACLs (slice 11).
//! - Log compaction, tiered storage, quotas.
//!
//! # Quick start
//!
//! ```no_run
//! use crabka_broker::{Broker, BrokerConfig};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let handle = Broker::start(BrokerConfig::default()).await?;
//! tokio::signal::ctrl_c().await?;
//! handle.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Public surface
//!
//! - [`Broker`] — owns the partition registry, metadata image, and
//!   handler table; constructed by [`Broker::start`].
//! - [`BrokerHandle`] — lifecycle handle returned by
//!   [`Broker::start`]; call [`BrokerHandle::shutdown`] to drain.
//! - [`BrokerConfig`] — listen address, advertised listener, log dir,
//!   broker id, per-log [`LogConfig`](crabka_log::LogConfig).
//! - [`BrokerError`] — error returned by [`Broker::start`].

#![doc(html_root_url = "https://docs.rs/crabka-broker/0.0.0")]

mod broker;
mod codes;
mod config;
mod coordinator;
mod error;
mod handlers;
mod log_dir;
mod metadata;
mod network;
mod partition;
mod partition_writer;

pub use broker::{Broker, BrokerHandle};
pub use config::BrokerConfig;
pub use error::BrokerError;
