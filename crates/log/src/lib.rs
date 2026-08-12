//! Byte-compatible reader/writer for Apache Kafka's on-disk log format.
//!
//! This crate is the append-only storage layer that the Crabka broker uses.
//! It reads and writes Kafka 4.x's on-disk log format byte-for-byte:
//! 20-digit zero-padded segment filenames, sparse `.index` and
//! `.timeindex` files, and append-only `.log` files that hold
//! [`crabka_protocol::records::RecordBatch`] v2 streams.
//!
//! ## What this crate does
//!
//! - Open and recover existing log directories.
//! - Append `RecordBatch`es to the active segment.
//! - Read sequentially from an absolute offset.
//! - Truncate the log to an offset, for replication and leader election.
//! - Time-based and size-based retention.
//!
//! ## Scope and boundaries
//!
//! This crate is the byte-compatible segment and index layer. It exposes
//! append, read, truncation, leader-epoch checkpoint, transaction-index,
//! retention, and compaction primitives over a single log directory.
//! `crabka-broker` applies broker-level policy above this storage layer. That
//! policy covers topic configuration, leader and follower ownership,
//! tiered-storage scheduling, transaction visibility rules, and write
//! serialization.
//!
//! ## Quick start
//!
//! ```no_run
//! use crabka_ids::Offset;
//! use crabka_log::{Log, LogConfig};
//! use crabka_protocol::records::RecordBatch;
//! use crabka_units::prelude::mebibytes;
//!
//! let mut log = Log::open("/var/kafka/my-topic-0", LogConfig::default()).unwrap();
//! let mut batch = RecordBatch::default();
//! // ... fill the batch ...
//! let assigned_offset = log.append(&mut batch).unwrap();
//!
//! let out = log.read(Offset(0), mebibytes(1)).unwrap();
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

#![doc(html_root_url = "https://docs.rs/crabka-log/0.4.0")]

/// Emit the wrapped items only on platforms with a usable file-to-socket
/// `sendfile(2)` for the zero-copy fetch path.
///
/// Those platforms are Linux, the Apple targets, and FreeBSD/DragonFly. They
/// are the "SENDFILE alias". Windows is excluded, because there is no safe
/// `TransmitFile` wrapper under `unsafe_code = "forbid"`. The fetch path there
/// uses `pread` and `write_all`, and never needs the file-region descriptors.
///
/// One macro per crate keeps the predicate identical at every sendfile-gated
/// site: the `read_raw_desc` descriptor types, their impls, and the re-exports.
/// The cfg set therefore cannot drift between them.
macro_rules! sendfile_cfg {
    ($($item:item)*) => {
        $(
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            $item
        )*
    };
}
pub(crate) use sendfile_cfg;

mod compact;
mod config;
mod error;
mod index;
mod leader_epoch_checkpoint;
mod log;
mod name;
mod producer_snapshot;
mod recovery;
mod retention;
mod segment;
mod stamp_index;
mod stamp_source;
mod txn_index;

pub use config::{CleanupPolicy, LogConfig};
pub use crabka_ids::{LeaderEpoch, Offset, ProducerId};
pub use error::LogError;
pub use leader_epoch_checkpoint::{
    EpochEntry, LeaderEpochCheckpoint, epoch_and_offset_for_entries,
};
sendfile_cfg! {
    pub use log::RawReadDesc;
}
pub use log::{CompactionContext, Log, RawRead, ReadOutput, SegmentExport, VerbatimBatch};
pub use producer_snapshot::ProducerSnapshotEntry;
sendfile_cfg! {
    pub use segment::RawSegmentDesc;
}
// Re-export the zero-copy fetch descriptor so broker code can name
// `crabka_log::FileRegion` without depending on the protocol crate's path.
pub use crabka_protocol::records::FileRegion;
pub use segment::{RawSegmentRead, Segment};
pub use stamp_index::{StampEntry, StampIndex};
#[cfg(any(test, feature = "test-helpers"))]
pub use stamp_source::MonotonicStampSource;
pub use stamp_source::StampSource;
pub use txn_index::{AbortedTxn, TxnIndex};
