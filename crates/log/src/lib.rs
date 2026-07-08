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
//! use crabka_ids::Offset;
//! use crabka_log::{Log, LogConfig};
//! use crabka_protocol::records::RecordBatch;
//!
//! let mut log = Log::open("/var/kafka/my-topic-0", LogConfig::default()).unwrap();
//! let mut batch = RecordBatch::default();
//! // ... fill the batch ...
//! let assigned_offset = log.append(&mut batch).unwrap();
//!
//! let out = log.read(Offset(0), 1024 * 1024).unwrap();
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

#![doc(html_root_url = "https://docs.rs/crabka-log/0.3.9")]

/// Emit the wrapped item(s) only on platforms with a usable file→socket
/// `sendfile(2)` for the zero-copy fetch path — Linux, the Apple targets, and
/// FreeBSD/DragonFly (the "SENDFILE alias"). Windows is excluded: there is no
/// safe `TransmitFile` wrapper under `unsafe_code = "forbid"`, so the fetch path
/// `pread`s + `write_all`s there and never needs the file-region descriptors.
///
/// One macro per crate keeps the predicate identical across every sendfile-gated
/// site (the `read_raw_desc` descriptor types, their impls, and the re-exports),
/// so the cfg set can't drift between them.
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
mod recovery;
mod retention;
mod segment;
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
sendfile_cfg! {
    pub use segment::RawSegmentDesc;
}
// Re-export the zero-copy fetch descriptor so broker code can name
// `crabka_log::FileRegion` without depending on the protocol crate's path.
pub use crabka_protocol::records::FileRegion;
pub use segment::{RawSegmentRead, Segment};
pub use txn_index::{AbortedTxn, TxnIndex};
