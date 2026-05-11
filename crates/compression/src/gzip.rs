//! Gzip (RFC-1952). Filled in by sub-plan 1b Task 4.

use bytes::Bytes;

use crate::CompressionError;

pub fn compress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData(
        "gzip not yet implemented".into(),
    ))
}

pub fn decompress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData(
        "gzip not yet implemented".into(),
    ))
}
