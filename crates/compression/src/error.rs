//! Errors returned by `compress` and `decompress`.

use thiserror::Error;

/// Errors that can occur during compression or decompression.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CompressionError {
    /// The requested codec was not enabled at compile time. The codec name
    /// is one of `"gzip"`, `"snappy"`, `"lz4"`, `"zstd"`.
    #[error("compression feature `{0}` not enabled at compile time")]
    FeatureDisabled(&'static str),

    /// The compressed payload could not be parsed (truncated input, bad
    /// framing, invalid checksum, etc.).
    #[error("invalid compressed data: {0}")]
    InvalidData(String),

    /// Decompression produced (or would produce) more output than the caller's
    /// `max_output` cap allows. Guards against decompression bombs: a small
    /// compressed payload that expands to gigabytes.
    #[error("decompressed output exceeds limit of {limit} bytes")]
    TooLarge { limit: usize },

    /// I/O error from one of the codec libraries.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn feature_disabled_display() {
        let e = CompressionError::FeatureDisabled("snappy");
        assert!(e.to_string() == "compression feature `snappy` not enabled at compile time");
    }
}
