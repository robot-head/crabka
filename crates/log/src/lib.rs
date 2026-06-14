//! Byte-compatible reader/writer for Apache Kafka's on-disk log format.
//!
//! This crate provides the append-only storage layer used by the Crabka broker.
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
//! ## Scope and boundaries
//!
//! This crate is the byte-compatible segment/index layer. It exposes append,
//! read, truncation, leader-epoch checkpoint, transaction-index, retention, and
//! compaction primitives over a single log directory. Broker-level policy —
//! topic configuration, leader/follower ownership, tiered-storage scheduling,
//! transaction visibility rules, and write serialization — is applied by
//! `crabka-broker` above this storage layer.
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
//! ## Exporting a segment
//!
//! ```no_run
//! use crabka_log::{Log, LogConfig};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut log = Log::open("/var/kafka/my-topic-0", LogConfig::default())?;
//! for segment in log.tierable_segments() {
//!     println!(
//!         "segment {} starts at offset {}",
//!         segment.log_path.display(),
//!         segment.base_offset
//!     );
//! }
//! # Ok(())
//! # }
//! ```

#![doc(html_root_url = "https://docs.rs/crabka-log/0.3.6")]

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
pub use log::{CompactionContext, Log, RawRead, ReadOutput, SegmentExport, VerbatimBatch};
pub use segment::{RawSegmentRead, Segment};
pub use txn_index::{AbortedTxn, TxnIndex};
