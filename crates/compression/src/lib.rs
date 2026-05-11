//! Kafka wire-protocol compression codecs.

mod codec_type;
mod error;

pub use codec_type::CompressionType;
pub use error::CompressionError;

use bytes::Bytes;

/// Compress `data` using the codec identified by `ct`.
///
/// For `CompressionType::None`, returns the input unchanged (wrapped in a
/// new `Bytes`). For other codecs, dispatches to the per-codec module.
/// If the codec's Cargo feature is not enabled, returns
/// `Err(CompressionError::FeatureDisabled(_))`.
pub fn compress(ct: CompressionType, data: &[u8]) -> Result<Bytes, CompressionError> {
    match ct {
        CompressionType::None => Ok(Bytes::copy_from_slice(data)),
        CompressionType::Gzip => gzip_compress(data),
        CompressionType::Snappy => snappy_compress(data),
        CompressionType::Lz4 => lz4_compress(data),
        CompressionType::Zstd => zstd_compress(data),
    }
}

/// Decompress `data` using the codec identified by `ct`. See `compress`.
pub fn decompress(ct: CompressionType, data: &[u8]) -> Result<Bytes, CompressionError> {
    match ct {
        CompressionType::None => Ok(Bytes::copy_from_slice(data)),
        CompressionType::Gzip => gzip_decompress(data),
        CompressionType::Snappy => snappy_decompress(data),
        CompressionType::Lz4 => lz4_decompress(data),
        CompressionType::Zstd => zstd_decompress(data),
    }
}

// --- per-codec dispatch, with feature-gated stubs ------------------------

#[cfg(feature = "gzip")]
mod gzip;
#[cfg(feature = "gzip")]
use crate::gzip::{compress as gzip_compress, decompress as gzip_decompress};
#[cfg(not(feature = "gzip"))]
fn gzip_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("gzip"))
}
#[cfg(not(feature = "gzip"))]
fn gzip_decompress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("gzip"))
}

#[cfg(feature = "snappy")]
mod snappy;
#[cfg(feature = "snappy")]
use crate::snappy::{compress as snappy_compress, decompress as snappy_decompress};
#[cfg(not(feature = "snappy"))]
fn snappy_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("snappy"))
}
#[cfg(not(feature = "snappy"))]
fn snappy_decompress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("snappy"))
}

#[cfg(feature = "lz4")]
mod lz4;
#[cfg(feature = "lz4")]
use crate::lz4::{compress as lz4_compress, decompress as lz4_decompress};
#[cfg(not(feature = "lz4"))]
fn lz4_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("lz4"))
}
#[cfg(not(feature = "lz4"))]
fn lz4_decompress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("lz4"))
}

#[cfg(feature = "zstd")]
mod zstd;
#[cfg(feature = "zstd")]
use crate::zstd::{compress as zstd_compress, decompress as zstd_decompress};
#[cfg(not(feature = "zstd"))]
fn zstd_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("zstd"))
}
#[cfg(not(feature = "zstd"))]
fn zstd_decompress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("zstd"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_none_compress() {
        let out = compress(CompressionType::None, b"abcdef").unwrap();
        assert_eq!(out.as_ref(), b"abcdef");
    }

    #[test]
    fn passthrough_none_decompress() {
        let out = decompress(CompressionType::None, b"abcdef").unwrap();
        assert_eq!(out.as_ref(), b"abcdef");
    }
}
