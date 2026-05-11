//! Zstd. Filled in by Task 7.

use bytes::Bytes;

use crate::CompressionError;

pub fn compress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData(
        "zstd not yet implemented".into(),
    ))
}

pub fn decompress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData(
        "zstd not yet implemented".into(),
    ))
}
