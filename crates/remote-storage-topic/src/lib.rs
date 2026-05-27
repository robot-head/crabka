//! Topic-backed [`RemoteLogMetadataManager`] for Crabka — slice 48f
//! of the KIP-405 tiered-storage roadmap.
//!
//! This crate ships [`TopicBasedRemoteLogMetadataManager`], the
//! production replacement for [`crabka_remote_storage::InmemoryRemoteLogMetadataManager`].
//! Remote-segment lifecycle events (add / update / partition-delete)
//! are appended to an event log — in production the
//! `__remote_log_metadata` Kafka topic — and every broker's local
//! cache is rebuilt by consuming the same log. After a restart, a
//! broker re-reads the topic from offset 0 and re-applies the full
//! history to recover its cache.
//!
//! See the per-slice design at
//! `docs/superpowers/specs/2026-05-27-crabka-tiered-storage-topic-based-rlmm-48f-design.md`.
//!
//! ## What this crate provides
//!
//! - [`TopicBasedRemoteLogMetadataManager`] — the
//!   [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager)
//!   implementation.
//! - [`MetadataEventLog`] — the publish/subscribe seam between the
//!   manager and the underlying durable transport.
//! - [`InProcessMetadataEventLog`] — an in-memory fixture for unit
//!   tests and for modelling the multi-broker case without bringing
//!   up a real cluster (multiple managers cloned from the same `Arc`
//!   observe each other's writes).
//! - [`MetadataEvent`] + the [`serde`] module — the on-wire binary
//!   codec for the three event variants.
//! - [`metadata_partition_for`] — the
//!   `TopicIdPartition → metadata-topic-partition` hash.
//!
//! ## What this crate does NOT do (yet)
//!
//! - **No Kafka-backed `MetadataEventLog` adapter.** The production
//!   adapter that wires the trait to
//!   [`crabka_client_producer`] / [`crabka_client_consumer`] /
//!   [`crabka_client_admin`] lands in the follow-up broker-integration
//!   PR.
//! - **No broker wiring.** `Broker::start` still constructs
//!   [`crabka_remote_storage::InmemoryRemoteLogMetadataManager`] as
//!   today; the config knob that selects the topic-backed
//!   implementation lands with the Kafka adapter.
//! - **No log compaction or snapshot** of the metadata topic — every
//!   restart re-reads from offset 0. Snapshot/fast-bootstrap is a
//!   future optimization.

#![doc(html_root_url = "https://docs.rs/crabka-remote-storage-topic/0.1.1")]

pub mod error;
pub mod log;
pub mod manager;
pub mod partitioning;
pub mod serde;

pub use error::{CodecError, MetadataLogError};
pub use log::{
    InProcessMetadataEventLog, MetadataEventLog, MetadataEventRecord, MetadataEventStream,
};
pub use manager::TopicBasedRemoteLogMetadataManager;
pub use partitioning::metadata_partition_for;
pub use serde::{MetadataEvent, WIRE_VERSION};
