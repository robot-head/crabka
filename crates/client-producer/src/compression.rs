//! The `Compression` enum, and the mapping from the producer's choice to a
//! `RecordBatch` v2 `attributes` value and a
//! `crabka-compression::CompressionType`.

use std::{fmt, str::FromStr};

use bytes::Bytes;

use crate::error::ProducerError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl fmt::Display for Compression {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        })
    }
}

impl FromStr for Compression {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            _ => Err(format!("unsupported producer compression: {value}")),
        }
    }
}

impl Compression {
    #[must_use]
    pub(crate) fn compression_type(self) -> crabka_compression::CompressionType {
        match self {
            Compression::None => crabka_compression::CompressionType::None,
            Compression::Gzip => crabka_compression::CompressionType::Gzip,
            Compression::Snappy => crabka_compression::CompressionType::Snappy,
            Compression::Lz4 => crabka_compression::CompressionType::Lz4,
            Compression::Zstd => crabka_compression::CompressionType::Zstd,
        }
    }

    /// The 3-bit `compression_type` field that goes into the `RecordBatch` v2
    /// `attributes`, at bits 0..3.
    #[must_use]
    pub fn attribute_bits(self) -> i16 {
        match self {
            Compression::None => 0,
            Compression::Gzip => 1,
            Compression::Snappy => 2,
            Compression::Lz4 => 3,
            Compression::Zstd => 4,
        }
    }

    /// Compress the encoded record body. Returns the byte payload that goes
    /// into the `RecordBatch.records_body` slot.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn compress(self, raw: &[u8]) -> Result<Bytes, ProducerError> {
        Ok(crabka_compression::compress(self.compression_type(), raw)?)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn none_round_trip_is_identity() {
        let raw = b"hello producer";
        let out = Compression::None.compress(raw).unwrap();
        assert2::assert!(out.as_ref() == raw);
    }

    #[test]
    fn attribute_bits_match_kafka_table() {
        for (_name, compression, want) in [
            ("none", Compression::None, 0),
            ("gzip", Compression::Gzip, 1),
            ("snappy", Compression::Snappy, 2),
            ("lz4", Compression::Lz4, 3),
            ("zstd", Compression::Zstd, 4),
        ] {
            assert2::assert!(compression.attribute_bits() == want);
        }
    }

    #[test]
    fn gzip_round_trip_via_decoder() {
        use crabka_compression::CompressionType;
        let raw = b"the quick brown fox";
        let compressed = Compression::Gzip.compress(raw).unwrap();
        let decoded = crabka_compression::decompress(
            CompressionType::Gzip,
            &compressed,
            crabka_units::convert::ByteSizeExt::from_bytes(u64::MAX),
        )
        .unwrap();
        assert2::assert!(decoded.as_ref() == raw);
    }

    #[test]
    fn compression_parses_and_displays_canonical_names() {
        for (name, compression) in [
            ("none", Compression::None),
            ("gzip", Compression::Gzip),
            ("snappy", Compression::Snappy),
            ("lz4", Compression::Lz4),
            ("zstd", Compression::Zstd),
        ] {
            assert_eq!(name.parse::<Compression>(), Ok(compression));
            assert_eq!(compression.to_string(), name);
        }
        assert_eq!(
            "brotli".parse::<Compression>(),
            Err("unsupported producer compression: brotli".to_owned())
        );
    }
}
