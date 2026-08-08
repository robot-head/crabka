//! Errors surfaced by the topic-backed
//! [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager)
//! and the underlying [`MetadataEventLog`](crate::MetadataEventLog).

use crabka_remote_storage::RemoteStorageError;

/// Failures from the publish/subscribe transport that backs the
/// `__remote_log_metadata` topic.
#[derive(Debug, thiserror::Error)]
pub enum MetadataLogError {
    /// The transport refused the publish, for example because the broker
    /// was unreachable or the ack timed out. The log did not append the
    /// event.
    #[error("publish failed: {0}")]
    Publish(String),

    /// Asked about a partition outside `[0, partition_count())`.
    #[error("metadata partition {partition} out of range (count {count})")]
    PartitionOutOfRange {
        /// The offending partition index.
        partition: i32,
        /// The configured partition count.
        count: i32,
    },

    /// The transport is closed and no further operations are possible.
    #[error("metadata log closed")]
    Closed,

    /// Any other transport-layer failure.
    #[error("metadata log error: {0}")]
    Other(String),
}

impl MetadataLogError {
    /// Wrap as a [`RemoteStorageError`] so the manager can surface
    /// transport failures through the synchronous SPI.
    #[must_use]
    pub fn into_storage(self) -> RemoteStorageError {
        RemoteStorageError::Io(std::io::Error::other(self.to_string()))
    }
}

/// Errors from the binary wire codec.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// The buffer ended before the parser had finished a field.
    #[error("unexpected end of input at offset {0}")]
    UnexpectedEof(usize),

    /// A state byte the codec does not know how to read.
    #[error("unknown state byte {0} for {ctx}", ctx = .1)]
    UnknownState(u8, &'static str),

    /// A length prefix that does not fit in `usize`.
    #[error("length prefix {0} too large")]
    LengthOverflow(u64),

    /// The reconstruction of the event-domain type from the decoded fields
    /// violated a precondition, for example an empty leader-epoch map.
    #[error("decoded event rejected: {0}")]
    Domain(String),

    /// An error propagated from the generated protocol codec: envelope
    /// framing, schema mismatch, or trailing bytes.
    #[error("protocol codec: {0}")]
    Protocol(String),
}

/// Errors from the on-disk RLMM snapshot.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The snapshot file could not be read or written.
    #[error("snapshot io error: {0}")]
    Io(#[from] std::io::Error),

    /// A snapshot format version the loader does not understand.
    #[error("unsupported snapshot format version {0}")]
    UnsupportedVersion(u16),

    /// The snapshot bytes were malformed. They were truncated, badly framed,
    /// or a contained event failed to decode.
    #[error("malformed snapshot: {0}")]
    Malformed(#[from] CodecError),

    /// Trailing bytes remained after the declared entries were read.
    #[error("snapshot has {0} trailing bytes")]
    TrailingBytes(usize),
}
