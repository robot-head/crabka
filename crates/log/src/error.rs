//! Errors returned by `Log` and `Segment`.

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LogError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("partial batch at offset {file_offset} in segment {segment}: truncating")]
    PartialBatch { segment: i64, file_offset: u64 },

    #[error(
        "CRC mismatch at offset {file_offset} in segment {segment}: \
         expected {expected:#x}, computed {computed:#x}"
    )]
    CrcMismatch {
        segment: i64,
        file_offset: u64,
        expected: u32,
        computed: u32,
    },

    #[error("offset {requested} below log start {log_start}")]
    OffsetTooLow { requested: i64, log_start: i64 },

    #[error("offset {requested} >= log end {log_end}")]
    OffsetTooHigh { requested: i64, log_end: i64 },

    #[error("records: {0}")]
    Records(#[from] crabka_protocol::records::RecordsError),

    #[error("invalid segment filename: {0}")]
    BadSegmentName(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_partial_batch() {
        let e = LogError::PartialBatch {
            segment: 0,
            file_offset: 1024,
        };
        assert!(e.to_string().contains("offset 1024"));
        assert!(e.to_string().contains("segment 0"));
    }
}
