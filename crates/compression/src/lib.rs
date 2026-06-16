//! Kafka wire-protocol compression codecs.
//!
//! Kafka uses four codecs on the wire — gzip, snappy, lz4, zstd — each with
//! specific framing conventions:
//!
//! - **gzip**: standard RFC-1952 gzip via `flate2` (pure-Rust `miniz_oxide`
//!   backend).
//! - **snappy**: xerial-snappy framing over `snap` raw blocks. Kafka does not
//!   use the standard Google Snappy stream format; it uses the xerial framing
//!   (8-byte magic header, two 4-byte version fields, then a sequence of
//!   `u32-BE` length-prefixed raw snappy chunks).
//! - **lz4**: LZ4 frame format (magic `0x04 22 4D 18`) with independent blocks
//!   and 64 KiB block size, matching `KafkaLZ4BlockOutputStream`'s defaults.
//! - **zstd**: plain zstd at compression level 3 (Kafka's default).
//!
//! Each codec is behind a Cargo feature (`gzip`, `snappy`, `lz4`, `zstd`), all
//! enabled by default. Disabling a feature leaves the API stable but returns
//! `Err(`[`CompressionError::FeatureDisabled`]`)` at runtime.
//!
//! ## Compress and decompress a record payload
//!
//! ```rust
//! use crabka_compression::{CompressionType, compress, decompress};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let compressed = compress(CompressionType::Lz4, b"order-created")?;
//! let plain = decompress(CompressionType::Lz4, &compressed, 1024)?;
//! assert_eq!(plain.as_ref(), b"order-created");
//! # Ok(())
//! # }
//! ```

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
///
/// `max_output` bounds the size of the decompressed output: if decompression
/// would produce more than `max_output` bytes, returns
/// `Err(CompressionError::TooLarge { .. })` without materializing the oversized
/// buffer. This guards against decompression bombs on the untrusted decode
/// path. Callers that handle wire input should derive `max_output` from the
/// compressed length (e.g. a bounded ratio plus an absolute ceiling).
pub fn decompress(
    ct: CompressionType,
    data: &[u8],
    max_output: usize,
) -> Result<Bytes, CompressionError> {
    match ct {
        CompressionType::None => {
            if data.len() > max_output {
                Err(CompressionError::TooLarge { limit: max_output })
            } else {
                Ok(Bytes::copy_from_slice(data))
            }
        }
        CompressionType::Gzip => gzip_decompress(data, max_output),
        CompressionType::Snappy => snappy_decompress(data, max_output),
        CompressionType::Lz4 => lz4_decompress(data, max_output),
        CompressionType::Zstd => zstd_decompress(data, max_output),
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
fn gzip_decompress(_: &[u8], _: usize) -> Result<Bytes, CompressionError> {
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
fn snappy_decompress(_: &[u8], _: usize) -> Result<Bytes, CompressionError> {
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
fn lz4_decompress(_: &[u8], _: usize) -> Result<Bytes, CompressionError> {
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
fn zstd_decompress(_: &[u8], _: usize) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("zstd"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn passthrough_none_compress() {
        let out = compress(CompressionType::None, b"abcdef").unwrap();
        assert!(out.as_ref() == b"abcdef");
    }

    #[test]
    fn passthrough_none_decompress() {
        let out = decompress(CompressionType::None, b"abcdef", 1024).unwrap();
        assert!(out.as_ref() == b"abcdef");
    }

    #[test]
    fn passthrough_none_decompress_respects_cap() {
        // Input larger than the cap is rejected even for the None passthrough.
        assert!(matches!(
            decompress(CompressionType::None, b"abcdef", 3),
            Err(CompressionError::TooLarge { limit: 3 })
        ));
    }

    #[test]
    fn passthrough_none_decompress_at_exact_cap() {
        // Boundary: input of exactly `max_output` bytes is allowed (the cap
        // check is `len > max_output`, not `>=`).
        let out = decompress(CompressionType::None, b"abcdef", 6).unwrap();
        assert!(out.as_ref() == b"abcdef");
    }
}
