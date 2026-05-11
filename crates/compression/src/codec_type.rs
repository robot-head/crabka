//! `CompressionType` enum mapping to Kafka's record-batch attribute bits.

/// Codec identifier matching the lowest three bits of Kafka's record-batch
/// attribute byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum CompressionType {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl CompressionType {
    /// Decode the lowest three bits of a Kafka record-batch attribute byte.
    /// Returns `None` for codec ids outside `0..=4`.
    #[must_use]
    pub fn from_attribute_bits(b: u8) -> Option<Self> {
        match b & 0b0000_0111 {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Snappy),
            3 => Some(Self::Lz4),
            4 => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Encode this codec into the lowest three bits of an attribute byte.
    #[must_use]
    pub fn as_attribute_bits(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attribute_bits_roundtrip() {
        for ct in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            assert_eq!(
                CompressionType::from_attribute_bits(ct.as_attribute_bits()),
                Some(ct)
            );
        }
    }

    #[test]
    fn attribute_bits_mask() {
        // Only the low 3 bits define the codec; upper bits are other flags.
        assert_eq!(
            CompressionType::from_attribute_bits(0b1111_1000 | 0b0000_0001),
            Some(CompressionType::Gzip)
        );
    }

    #[test]
    fn attribute_bits_unknown() {
        assert_eq!(CompressionType::from_attribute_bits(5), None);
        assert_eq!(CompressionType::from_attribute_bits(7), None);
    }
}
