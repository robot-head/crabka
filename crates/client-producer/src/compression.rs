//! `Compression` enum + mapping from the producer's choice to a
//! `RecordBatch` v2 `attributes` value + a `crabka-compression::CompressionType`.

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

impl Compression {
    /// The 3-bit `compression_type` field that goes into the `RecordBatch`
    /// v2 `attributes` (bits 0..3).
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

    /// Compress the encoded record body. Returns the byte payload that
    /// goes into the `RecordBatch.records_body` slot.
    pub fn compress(self, raw: &[u8]) -> Result<Bytes, ProducerError> {
        use crabka_compression::CompressionType;
        let ct = match self {
            Compression::None => CompressionType::None,
            Compression::Gzip => CompressionType::Gzip,
            Compression::Snappy => CompressionType::Snappy,
            Compression::Lz4 => CompressionType::Lz4,
            Compression::Zstd => CompressionType::Zstd,
        };
        Ok(crabka_compression::compress(ct, raw)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn none_round_trip_is_identity() {
        let raw = b"hello producer";
        let out = Compression::None.compress(raw).unwrap();
        assert!(out.as_ref() == raw);
    }

    #[test]
    fn attribute_bits_match_kafka_table() {
        for (compression, want) in [
            (Compression::None, 0),
            (Compression::Gzip, 1),
            (Compression::Snappy, 2),
            (Compression::Lz4, 3),
            (Compression::Zstd, 4),
        ] {
            assert!(compression.attribute_bits() == want);
        }
    }

    #[test]
    fn gzip_round_trip_via_decoder() {
        use crabka_compression::CompressionType;
        let raw = b"the quick brown fox";
        let compressed = Compression::Gzip.compress(raw).unwrap();
        let decoded =
            crabka_compression::decompress(CompressionType::Gzip, &compressed, usize::MAX).unwrap();
        assert!(decoded.as_ref() == raw);
    }
}
