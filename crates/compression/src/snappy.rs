//! Xerial-snappy framing over snap raw blocks. Filled in by Task 5.

use bytes::Bytes;

use crate::CompressionError;

pub fn compress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData(
        "snappy not yet implemented".into(),
    ))
}

pub fn decompress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData(
        "snappy not yet implemented".into(),
    ))
}
