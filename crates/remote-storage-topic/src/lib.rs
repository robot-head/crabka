//! Topic-backed [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager) for Crabka, part of
//! the KIP-405 tiered-storage stack.
//!
//! This crate ships [`TopicBasedRemoteLogMetadataManager`], the
//! production replacement for [`crabka_remote_storage::InmemoryRemoteLogMetadataManager`].
//! The manager appends remote-segment lifecycle events, which are add,
//! update, and partition-delete, to an event log. In production that log is
//! the `__remote_log_metadata` Kafka topic. Every broker rebuilds its local
//! cache from the same log. After a restart, a broker re-reads the topic from
//! offset 0 and re-applies the full history to recover its cache.
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
//! - [`MetadataEvent`] and the [`serde`] module — the on-wire binary
//!   codec for the three event variants.
//! - [`metadata_partition_for`] — the
//!   `TopicIdPartition → metadata-topic-partition` hash.
//! - [`KafkaMetadataEventLog`] — the production [`MetadataEventLog`]
//!   adapter that wires the trait to
//!   [`crabka_client_producer`] / [`crabka_client_core`] /
//!   [`crabka_client_admin`], persisting events in the
//!   `__remote_log_metadata` topic. Reads use manual per-partition
//!   `Fetch` loops over `crabka_client_core`, with no consumer group.
//! - [`SwappableRlmm`] — the hot-swap facade the broker boots behind so
//!   it can start on the fail-closed `NotReadyRlmm` and upgrade to the
//!   topic-backed manager once its listener is serving.
//!   `Broker::start` selects the topic-backed manager when the
//!   `[remote_storage.kafka_metadata]` config section is present and
//!   `in_memory` is not set to `true`.
//!
//! ## Operational boundaries
//!
//! The metadata topic is an append-only event log created with delete cleanup
//! and infinite retention. The manager maintains a local snapshot cache so
//! restarts can resume from committed per-partition offsets instead of replaying
//! the full topic every time. It does not use a Kafka consumer group or broker
//! offset commits. The broker drives assignments explicitly with
//! [`TopicBasedRemoteLogMetadataManager::reconcile_assignment`]. Internal Kafka
//! clients use plaintext loopback by default, or the TLS/SASL settings supplied
//! through [`KafkaMetadataLogConfig::security`].
//!
//! ## In-process manager for tests and local tools
//!
//! ```no_run
//! use std::{path::PathBuf, time::Duration};
//!
//! use crabka_remote_storage_topic::{
//!     InProcessMetadataEventLog, TopicBasedRemoteLogMetadataManager,
//! };
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let event_log = InProcessMetadataEventLog::new(16);
//! let manager = TopicBasedRemoteLogMetadataManager::start(
//!     event_log,
//!     tokio::runtime::Handle::current(),
//!     PathBuf::from("/var/lib/crabka/rlmm-cache"),
//!     Duration::from_secs(30),
//! )?;
//!
//! manager.reconcile_assignment(&[0, 1]).await;
//! # Ok(())
//! # }
//! ```

#![doc(html_root_url = "https://docs.rs/crabka-remote-storage-topic/0.3.9")]

pub mod error;
pub mod kafka_log;
pub mod log;
pub mod manager;
pub mod not_ready;
pub mod partitioning;
pub mod serde;
pub mod snapshot;
pub mod swappable;

pub use error::{CodecError, MetadataLogError, SnapshotError};
pub use kafka_log::{
    DEFAULT_METADATA_EVENT_QUEUE_CAPACITY, DEFAULT_METADATA_FETCH_MAX_BYTES,
    DEFAULT_METADATA_FETCH_MAX_WAIT, DEFAULT_METADATA_FETCH_RETRY_BACKOFF,
    DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION,
    KafkaMetadataEventLog, KafkaMetadataLogConfig, METADATA_TOPIC, MetadataEventQueueCapacity,
};
pub use log::{
    AssignmentHandle, InProcessMetadataEventLog, MetadataEventLog, MetadataEventRecord,
    MetadataEventStream, PartitionStart,
};
pub use manager::TopicBasedRemoteLogMetadataManager;
pub use not_ready::NotReadyRlmm;
pub use partitioning::{metadata_partition_for, metadata_partitions_for};
pub use serde::MetadataEvent;
pub use snapshot::{SNAPSHOT_FILE_NAME, SNAPSHOT_FORMAT_VERSION, Snapshot};
pub use swappable::SwappableRlmm;
