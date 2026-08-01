//! Error type for the substrate durability layer.

use crabka_units::{ByteSize, fmt::Human as _};

/// Errors from WAL framing, journaling, and recovery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubstrateError {
    /// A GRW1 frame could not be decoded.
    #[error("malformed GRW1 frame: {0}")]
    Frame(String),
    /// Replay found a missing or reordered journal sequence.
    #[error("journal sequence gap: expected {expected}, found {found} at offset {offset}")]
    SequenceGap {
        /// The next journal sequence replay expected.
        expected: u64,
        /// The sequence found in the frame.
        found: u64,
        /// The Kafka offset that carried `found`.
        offset: i64,
    },
    /// A newer compute generation owns this tenant.
    #[error("fenced: a newer compute generation owns this tenant")]
    Fenced,
    /// The WAL topic or broker path is not currently available.
    #[error("WAL topic unavailable: {0}")]
    Unavailable(String),
    /// A pause barrier is already reserving or holding the WAL writer.
    #[error("WAL writer is already paused")]
    AlreadyPaused,
    /// Local read-model storage failed.
    #[error(transparent)]
    Kv(#[from] crabka_pgkv::KvError),
    /// One operation cannot fit in the configured WAL frame size.
    #[error(
        "WAL operation is too large: encoded length {encoded_len} exceeds frame limit {}",
        .max_frame_size.human()
    )]
    OversizedOperation {
        /// Encoded bytes needed by the single operation frame.
        encoded_len: usize,
        /// Configured maximum frame size.
        max_frame_size: ByteSize,
    },
    /// A WAL topic operation failed.
    #[error("WAL topic operation failed: {0}")]
    Topic(String),
    /// A checkpoint manifest or part could not be decoded or validated.
    #[error("checkpoint invalid: {0}")]
    Checkpoint(String),
    /// A row-bearing key referenced a physical catalog id absent from the sealed mapping.
    #[error("unmapped physical table id {0} during filtered restore")]
    UnmappedPhysicalTable(u32),
    /// The WAL was truncated past the newest durable checkpoint.
    #[error("torn WAL truncation: log start {log_start} is past newest manifest {newest_manifest}")]
    TornTruncation {
        /// Current Kafka log start offset.
        log_start: i64,
        /// The newest durable checkpoint's covered offset.
        newest_manifest: i64,
    },
    /// A checkpoint part did not match the manifest digest.
    #[error("checkpoint part checksum mismatch: {part}")]
    ChecksumMismatch {
        /// Manifest part name whose bytes failed checksum validation.
        part: String,
    },
    /// The requested committed fold predates all retained reconstructible history.
    #[error(
        "pruned history: WAL starts at {log_start}, with no checkpoint covering sample {sample_offset}"
    )]
    PrunedHistory {
        /// Earliest retained WAL offset.
        log_start: i64,
        /// Stable committed offset sampled by the reader.
        sample_offset: i64,
    },
    /// A bounded fold snapshot exceeded its configured resource limit.
    #[error("committed fold limit exceeded: {0}")]
    FoldLimit(String),
}

impl From<SubstrateError> for crabka_pgexec::ExecError {
    fn from(error: SubstrateError) -> Self {
        match error {
            SubstrateError::Kv(source) => Self::Kv(source),
            error @ (SubstrateError::Fenced | SubstrateError::SequenceGap { .. }) => {
                tracing::warn!(%error, "substrate rejected operation as not-leader");
                Self::NotLeader
            }
            SubstrateError::Frame(_)
            | SubstrateError::Unavailable(_)
            | SubstrateError::AlreadyPaused
            | SubstrateError::OversizedOperation { .. }
            | SubstrateError::Topic(_)
            | SubstrateError::Checkpoint(_)
            | SubstrateError::UnmappedPhysicalTable(_)
            | SubstrateError::TornTruncation { .. }
            | SubstrateError::ChecksumMismatch { .. } => Self::Unavailable,
            SubstrateError::PrunedHistory { .. } | SubstrateError::FoldLimit(_) => {
                Self::Unavailable
            }
        }
    }
}
