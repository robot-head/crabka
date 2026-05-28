//! Byte-compatible reader/writer for Apache Kafka's on-disk log format.
//!
//! This crate provides the storage layer beneath a future Crabka broker.
//! It reads and writes Kafka 4.x's on-disk log format byte-for-byte:
//! 20-digit zero-padded segment filenames, sparse `.index` and
//! `.timeindex` files, append-only `.log` files containing
//! [`crabka_protocol::records::RecordBatch`] v2 streams.
//!
//! ## What this crate does
//!
//! - Open + recover existing log directories.
//! - Append `RecordBatch`es to the active segment.
//! - Read sequentially from an absolute offset.
//! - Truncate the log to an offset (for replication / leader election).
//! - Time-based and size-based retention.
//!
//! ## What this crate doesn't do
//!
//! - Log compaction (separate subsystem; deferred).
//! - Transactional marker interpretation (broker concern).
//! - Tiered storage (broker concern).
//! - Concurrent writes (single-writer; broker enforces above).
//!
//! ## Quick start
//!
//! ```no_run
//! use crabka_log::{Log, LogConfig};
//! use crabka_protocol::records::RecordBatch;
//!
//! let mut log = Log::open("/var/kafka/my-topic-0", LogConfig::default()).unwrap();
//! let mut batch = RecordBatch::default();
//! // ... fill the batch ...
//! let assigned_offset = log.append(&mut batch).unwrap();
//!
//! let out = log.read(0, 1024 * 1024).unwrap();
//! # let _ = (assigned_offset, out);
//! ```
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-11-crabka-log-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-log/0.0.0")]

mod compact;
mod config;
mod error;
mod index;
mod leader_epoch_checkpoint;
mod log;
mod name;
mod recovery;
mod retention;
mod segment;
mod txn_index;

pub use config::{CleanupPolicy, LogConfig};
pub use error::LogError;
pub use leader_epoch_checkpoint::{EpochEntry, LeaderEpochCheckpoint};
pub use log::{Log, ReadOutput, SegmentExport};
pub use segment::Segment;
pub use txn_index::{AbortedTxn, TxnIndex};
