//! Errors that the v0/v1 `MessageSet` codec returns.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LegacyRecordsError {
    #[error("buffer truncated: need {needed} more byte(s)")]
    Truncated { needed: usize },

    #[error("unsupported magic byte {found} (legacy codec accepts 0 or 1)")]
    UnsupportedMagic { found: i8 },

    #[error("message CRC mismatch (expected={expected:#010x} computed={computed:#010x})")]
    CrcMismatch { expected: u32, computed: u32 },

    #[error("negative length {len} for {label}")]
    NegativeLength { label: &'static str, len: i32 },

    #[error("inconsistent magic: outer={outer} inner={inner}")]
    InconsistentMagic { outer: i8, inner: i8 },

    #[error("recursive compressed wrapper not allowed")]
    NestedCompression,

    #[error("malformed legacy record: {0}")]
    Malformed(String),

    #[error(transparent)]
    Compression(#[from] crabka_compression::CompressionError),
}
