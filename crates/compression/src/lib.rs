//! Kafka wire-protocol compression codecs.
//!
//! Kafka uses four codecs on the wire: gzip, snappy, lz4, and zstd. Each codec
//! has specific framing conventions.
//!
//! - **gzip**: standard RFC-1952 gzip through `flate2`, which uses the
//!   pure-Rust `miniz_oxide` backend.
//! - **snappy**: xerial-snappy framing over `snap` raw blocks. Kafka does not
//!   use the standard Google Snappy stream format. It uses the xerial framing:
//!   an 8-byte magic header, two 4-byte version fields, then a sequence of
//!   `u32-BE` length-prefixed raw snappy chunks.
//! - **lz4**: LZ4 frame format with magic `0x04 22 4D 18`, independent blocks,
//!   and 64 KiB block size. These are the defaults of
//!   `KafkaLZ4BlockOutputStream`.
//! - **zstd**: plain zstd at compression level 3, which is Kafka's default.
//!
//! Each codec is behind a Cargo feature: `gzip`, `snappy`, `lz4`, and `zstd`.
//! All of them are enabled by default. If you disable a feature, the API stays
//! the same, but the call returns
//! `Err(`[`CompressionError::FeatureDisabled`]`)` at runtime.
//!
//! ## Compress and decompress a record payload
//!
//! ```rust
//! use crabka_compression::{CompressionType, compress, decompress};
//! use crabka_units::kibibytes;
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let compressed = compress(CompressionType::Lz4, b"order-created")?;
//! let plain = decompress(CompressionType::Lz4, &compressed, kibibytes(1))?;
//! assert_eq!(plain.as_ref(), b"order-created");
//! # Ok(())
//! # }
//! ```

mod codec_type;
mod error;

use bytes::Bytes;
pub use codec_type::CompressionType;
use crabka_units::{
    ByteSize, Ratio,
    convert::{ByteSizeExt as _, RatioExt as _},
    fraction, gibibytes, mebibytes,
};
pub use error::CompressionError;
use refined_type::rule::MinMaxU64;

/// Fixed security ceiling for record decompression expansion.
pub const RECORD_DECOMPRESSION_HARD_MAX_RATIO: Ratio = fraction(100.0);

/// Fixed security ceiling for a decompressed record payload.
pub const RECORD_DECOMPRESSION_HARD_MAX_OUTPUT: ByteSize = gibibytes(1);

const RECORD_DECOMPRESSION_HARD_MAX_OUTPUT_BYTES: u64 = 1_073_741_824;
type RefinedRecordDecompressionBytes = MinMaxU64<1, RECORD_DECOMPRESSION_HARD_MAX_OUTPUT_BYTES>;

/// Validated limits for decompressing untrusted Kafka record payloads.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecordDecompressionPolicy {
    max_ratio: Ratio,
    output_floor: ByteSize,
    output_ceiling: ByteSize,
}

impl RecordDecompressionPolicy {
    /// Validate policy values against the fixed decompression security bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-positive or non-finite ratio, fractional or
    /// non-positive byte limits, an inverted range, or a weakened hard bound.
    pub fn new(
        max_ratio: Ratio,
        output_floor: ByteSize,
        output_ceiling: ByteSize,
    ) -> Result<Self, String> {
        let ratio = max_ratio.as_f64();
        if !ratio.is_finite()
            || ratio <= 0.0
            || ratio > RECORD_DECOMPRESSION_HARD_MAX_RATIO.as_f64()
        {
            return Err(
                "record decompression ratio must be finite and within 0 < ratio <= 100".into(),
            );
        }
        let floor = validated_whole_bytes("record decompression output floor", output_floor)?;
        let ceiling = validated_whole_bytes("record decompression output ceiling", output_ceiling)?;
        if floor > ceiling {
            return Err("record decompression output floor exceeds ceiling".into());
        }
        Ok(Self {
            max_ratio,
            output_floor,
            output_ceiling,
        })
    }

    /// Return the maximum decompression expansion ratio.
    #[must_use]
    pub fn max_ratio(self) -> Ratio {
        self.max_ratio
    }

    /// Return the minimum decompression output budget.
    #[must_use]
    pub fn output_floor(self) -> ByteSize {
        self.output_floor
    }

    /// Return the maximum decompression output budget.
    #[must_use]
    pub fn output_ceiling(self) -> ByteSize {
        self.output_ceiling
    }

    /// Calculate the output budget for a compressed payload.
    #[must_use]
    pub fn output_limit(self, compressed: ByteSize) -> ByteSize {
        ByteSize::from_bytes_f64(
            (compressed.bytes_f64() * self.max_ratio.as_f64())
                .max(self.output_floor.bytes_f64())
                .min(self.output_ceiling.bytes_f64()),
        )
    }
}

impl Default for RecordDecompressionPolicy {
    fn default() -> Self {
        Self {
            max_ratio: RECORD_DECOMPRESSION_HARD_MAX_RATIO,
            output_floor: mebibytes(16),
            output_ceiling: RECORD_DECOMPRESSION_HARD_MAX_OUTPUT,
        }
    }
}

fn validated_whole_bytes(name: &str, value: ByteSize) -> Result<u64, String> {
    let raw = value.bytes_f64();
    if !raw.is_finite() || raw.fract() != 0.0 {
        return Err(format!("{name} must be a positive whole number of bytes"));
    }
    RefinedRecordDecompressionBytes::new(value.bytes_u64())
        .map(refined_type::Refined::into_value)
        .map_err(|error| format!("{name}: {error}"))
}

/// Compress `data` using the codec identified by `ct`.
///
/// For `CompressionType::None`, the function returns the input unchanged in a
/// new `Bytes`. For the other codecs, it dispatches to the per-codec module. If
/// the codec's Cargo feature is not enabled, it returns
/// `Err(CompressionError::FeatureDisabled(_))`.
/// # Errors
/// Returns an error when input is malformed, compression or decompression fails, or runtime rate-limit state cannot be updated.
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
/// `max_output` bounds the size of the decompressed output. If decompression
/// would produce more than `max_output` bytes, the function returns
/// `Err(CompressionError::TooLarge { .. })` and does not materialize the
/// oversized buffer. This guards against decompression bombs on the untrusted
/// decode path. Callers that handle wire input should derive `max_output` from
/// the compressed length, for example from a bounded ratio plus an absolute
/// ceiling.
/// # Errors
/// Returns an error when input is malformed, compression or decompression fails, or runtime rate-limit state cannot be updated.
pub fn decompress(
    ct: CompressionType,
    data: &[u8],
    max_output: ByteSize,
) -> Result<Bytes, CompressionError> {
    // The per-codec decoders compare against buffer lengths and size
    // allocations, so they keep the exact `usize` count.
    let max_output = max_output.bytes_usize();
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
    use crabka_units::{
        bytes, convert::ByteSizeExt as _, fraction, gibibytes, kibibytes, mebibytes,
    };

    use super::*;

    #[test]
    fn passthrough_none_compress() {
        let out = compress(CompressionType::None, b"abcdef").unwrap();
        assert2::assert!(out.as_ref() == b"abcdef");
    }

    #[test]
    fn passthrough_none_decompress() {
        let out = decompress(CompressionType::None, b"abcdef", kibibytes(1)).unwrap();
        assert2::assert!(out.as_ref() == b"abcdef");
    }

    #[test]
    fn passthrough_none_decompress_respects_cap() {
        // Input larger than the cap is rejected even for the None passthrough.
        assert2::assert!(matches!(
            decompress(CompressionType::None, b"abcdef", bytes(3)),
            Err(CompressionError::TooLarge { limit: 3 })
        ));
    }

    #[test]
    fn passthrough_none_decompress_at_exact_cap() {
        // Boundary: input of exactly `max_output` bytes is allowed (the cap
        // check is `len > max_output`, not `>=`).
        let out = decompress(CompressionType::None, b"abcdef", bytes(6)).unwrap();
        assert2::assert!(out.as_ref() == b"abcdef");
    }

    #[test]
    fn record_policy_preserves_existing_budget_curve() {
        let policy = RecordDecompressionPolicy::default();
        assert2::check!(policy.output_limit(bytes(1)) == mebibytes(16));
        assert2::check!(policy.output_limit(mebibytes(1)) == mebibytes(100));
        assert2::check!(policy.output_limit(mebibytes(11)) == gibibytes(1));
    }

    #[test]
    fn record_policy_rejects_invalid_or_weakened_security_bounds() {
        for result in [
            RecordDecompressionPolicy::new(fraction(0.0), mebibytes(16), gibibytes(1)),
            RecordDecompressionPolicy::new(fraction(101.0), mebibytes(16), gibibytes(1)),
            RecordDecompressionPolicy::new(fraction(100.0), gibibytes(1), mebibytes(16)),
            RecordDecompressionPolicy::new(fraction(100.0), mebibytes(16), gibibytes(2)),
            RecordDecompressionPolicy::new(
                fraction(100.0),
                ByteSize::from_bytes_f64(0.5),
                gibibytes(1),
            ),
        ] {
            assert2::check!(result.is_err());
        }
    }
}
