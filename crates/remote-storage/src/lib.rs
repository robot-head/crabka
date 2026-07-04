//! KIP-405 tiered-storage SPI and reference implementations for Crabka.
//!
//! This crate is the foundation layer for Crabka's tiered storage: it
//! defines the two plugin SPIs and the data model exchanged across them,
//! and ships the two reference implementations that the rest of the
//! tiered-storage stack is built and tested against. It mirrors the shapes
//! of Apache Kafka's `storage-api` module
//! (`org.apache.kafka.server.log.remote.storage`).
//!
//! ## What this crate provides
//!
//! - [`RemoteStorageManager`] — copy / fetch / delete of segment data and
//!   indexes to and from the remote tier.
//! - [`RemoteLogMetadataManager`] — persistence + querying of
//!   remote-segment metadata, with a strict lifecycle state machine.
//! - The data model: [`TopicIdPartition`], [`RemoteLogSegmentId`],
//!   [`RemoteLogSegmentMetadata`] / [`RemoteLogSegmentMetadataUpdate`],
//!   [`RemoteLogSegmentState`], [`LogSegmentData`], [`IndexType`],
//!   [`CustomMetadata`], and the partition-delete lifecycle
//!   ([`RemotePartitionDeleteMetadata`] / [`RemotePartitionDeleteState`]).
//! - [`LocalTieredStorage`] — a filesystem [`RemoteStorageManager`].
//! - [`InmemoryRemoteLogMetadataManager`] — a process-memory
//!   [`RemoteLogMetadataManager`].
//!
//! ## Boundary with the broker
//!
//! This crate is the SPI and reference-implementation layer. Broker-specific
//! behavior such as segment-copy scheduling, `Fetch` remote reads,
//! local-vs-remote retention policy, and topic config parsing lives in the
//! broker.
//!
//! The SPIs are intentionally **synchronous** — they mirror Kafka's
//! blocking `RemoteStorageManager` / `RemoteLogMetadataManager`, which the
//! broker drives from a thread pool (the broker wraps calls in
//! `spawn_blocking`). Keeping them sync keeps this crate free of the async
//! runtime.
//!
//! ## Filesystem-backed remote tier
//!
//! ```no_run
//! use std::{collections::BTreeMap, path::PathBuf};
//!
//! use bytes::Bytes;
//! use crabka_ids::LeaderEpoch;
//! use crabka_remote_storage::{
//!     IndexType, LocalTieredStorage, LogSegmentData, RemoteLogSegmentId,
//!     RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
//! };
//! use uuid::Uuid;
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let storage = LocalTieredStorage::new(PathBuf::from("/var/lib/crabka-remote"));
//! let topic_partition = TopicIdPartition::new(Uuid::new_v4(), "orders", 0);
//! let segment_id = RemoteLogSegmentId::new(topic_partition, Uuid::new_v4());
//! let mut leader_epochs = BTreeMap::new();
//! leader_epochs.insert(LeaderEpoch(0), 0);
//! let metadata = RemoteLogSegmentMetadata::new(
//!     segment_id,
//!     0,
//!     999,
//!     1_713_000_000_000,
//!     1,
//!     1_713_000_000_000,
//!     1_048_576,
//!     RemoteLogSegmentState::CopySegmentStarted,
//!     leader_epochs,
//! )?;
//!
//! // The broker fills these paths from a closed local log segment.
//! let segment = LogSegmentData {
//!     log_segment: PathBuf::from("/var/lib/crabka/orders-0/00000000000000000000.log"),
//!     offset_index: PathBuf::from("/var/lib/crabka/orders-0/00000000000000000000.index"),
//!     time_index: PathBuf::from("/var/lib/crabka/orders-0/00000000000000000000.timeindex"),
//!     transaction_index: None,
//!     producer_snapshot_index: None,
//!     leader_epoch_index: Bytes::new(),
//! };
//! let _custom_metadata = storage.copy_log_segment_data(&metadata, &segment)?;
//! let bytes = storage.fetch_index(&metadata, IndexType::Offset)?;
//! # let _ = bytes;
//! # Ok(())
//! # }
//! ```

#![doc(html_root_url = "https://docs.rs/crabka-remote-storage/0.3.8")]

mod cache;
pub mod dump;
mod error;
mod gcs;
mod inmemory;
mod local;
mod metadata;
mod metadata_manager;
mod s3;
mod storage_manager;

pub use dump::{PartitionDump, RlmmCacheDump};
pub use error::RemoteStorageError;
pub use gcs::GcsConfig;
pub use inmemory::InmemoryRemoteLogMetadataManager;
pub use local::LocalTieredStorage;
pub use metadata::{
    CustomMetadata, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemoteLogSegmentState, RemotePartitionDeleteMetadata, RemotePartitionDeleteState,
    TopicIdPartition,
};
pub use metadata_manager::RemoteLogMetadataManager;
pub use s3::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, S3Config, S3RemoteStorage,
};
pub use storage_manager::{IndexType, LogSegmentData, RemoteStorageManager};
