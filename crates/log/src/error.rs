//! Errors returned by `Log` and `Segment`.

use crabka_ids::Offset;
use thiserror::Error;

/// Errors returned by [`Log`](crate::Log) and [`Segment`](crate::Segment).
#[derive(Debug, Error)]
pub enum LogError {
    /// An I/O operation against the filesystem failed.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    /// Recovery found a partial batch at the tail of a `.log` file. The log
    /// truncates the trailing bytes back to the last cleanly decoded batch.
    #[error("partial batch at offset {file_offset} in segment {segment}: truncating")]
    PartialBatch {
        /// Absolute base offset of the segment containing the partial batch.
        segment: Offset,
        /// Byte position within the `.log` file where the partial batch starts.
        file_offset: u64,
    },

    /// A batch's stored CRC did not match the one computed over its bytes.
    #[error(
        "CRC mismatch at offset {file_offset} in segment {segment}: \
         expected {expected:#x}, computed {computed:#x}"
    )]
    CrcMismatch {
        /// Absolute base offset of the segment.
        segment: Offset,
        /// Byte position within the `.log` file where the corrupt batch starts.
        file_offset: u64,
        /// CRC value embedded in the batch header.
        expected: u32,
        /// CRC value re-computed by the reader.
        computed: u32,
    },

    /// A caller requested an offset below [`Log::log_start_offset`](crate::Log::log_start_offset).
    #[error("offset {requested} below log start {log_start}")]
    OffsetTooLow {
        /// Offset the caller asked for.
        requested: Offset,
        /// Current log start.
        log_start: Offset,
    },

    /// The encode or decode of a `RecordBatch` failed.
    #[error("records: {0}")]
    Records(#[from] crabka_protocol::records::RecordsError),

    /// A segment filename would not parse. For example, it has the wrong
    /// length, or it is not all digits.
    #[error("invalid segment filename: {0}")]
    BadSegmentName(String),

    /// A caller supplied an explicit offset to [`Log::append_at`](crate::Log::append_at)
    /// that did not match the log's current end offset. Replication paths use
    /// this to detect divergence between leader-assigned offsets and the local
    /// log's expected next offset.
    #[error("offset mismatch: expected {expected}, got {actual}")]
    OffsetMismatch {
        /// The offset the log expected, that is, its current end offset.
        expected: Offset,
        /// The offset the caller actually supplied.
        actual: Offset,
    },

    /// A log file such as `.txnindex` is corrupt. It has the wrong size, a
    /// bad checksum, or a similar defect.
    #[error("corrupt log: {0}")]
    Corrupt(String),

    /// A caller supplied an invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_partial_batch() {
        let e = LogError::PartialBatch {
            segment: Offset(0),
            file_offset: 1024,
        };
        assert2::assert!(e.to_string() == "partial batch at offset 1024 in segment 0: truncating");
    }
}
