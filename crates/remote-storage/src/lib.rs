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
//! ## What this crate does NOT do (yet)
//!
//! This crate is the foundation layer only. There is **no broker wiring** —
//! no copy task, no remote read path on `Fetch`, no local-vs-remote
//! retention split, and no broker/topic config. See
//! `docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`.
//!
//! The SPIs are intentionally **synchronous** — they mirror Kafka's
//! blocking `RemoteStorageManager` / `RemoteLogMetadataManager`, which the
//! broker drives from a thread pool (the broker wraps calls in
//! `spawn_blocking`). Keeping them sync keeps this crate free of the async
//! runtime.

#![doc(html_root_url = "https://docs.rs/crabka-remote-storage/0.1.1")]

mod cache;
pub mod dump;
mod error;
mod inmemory;
mod local;
mod metadata;
mod metadata_manager;
mod s3;
mod storage_manager;

pub use dump::{PartitionDump, RlmmCacheDump};
pub use error::RemoteStorageError;
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
