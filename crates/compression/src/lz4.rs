//! LZ4 frame format (independent blocks). Filled in by Task 6.

use bytes::Bytes;

use crate::CompressionError;

pub fn compress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData(
        "lz4 not yet implemented".into(),
    ))
}

pub fn decompress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData(
        "lz4 not yet implemented".into(),
    ))
}
