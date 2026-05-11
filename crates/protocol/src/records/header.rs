//! Record-batch v2 header types: `RecordBatchHeader` (zerocopy),
//! `Attributes`, `TimestampType`.

use crabka_compression::CompressionType;

/// Timestamp-type bit in the attributes word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime,
}

/// Packed batch-level attributes, encoded as a 16-bit big-endian field
/// in the wire header.
///
/// - bits 0-2: compression type (matches `CompressionType::as_attribute_bits`)
/// - bit 3:    timestamp type (0 = CreateTime, 1 = LogAppendTime)
/// - bit 4:    is_transactional
/// - bit 5:    is_control_batch
/// - bit 6:    has_delete_horizon_ms (Kafka 2.8+; not surfaced separately here)
/// - bits 7-15: reserved
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attributes(pub i16);

impl Attributes {
    pub const TIMESTAMP_TYPE_BIT: i16 = 1 << 3;
    pub const TRANSACTIONAL_BIT: i16 = 1 << 4;
    pub const CONTROL_BIT: i16 = 1 << 5;

    #[must_use]
    pub fn compression(self) -> CompressionType {
        // The low 3 bits are the codec id. Wider attribute bits are ignored.
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let byte = (self.0 & 0x07) as u8;
        CompressionType::from_attribute_bits(byte).unwrap_or(CompressionType::None)
    }

    #[must_use]
    pub fn timestamp_type(self) -> TimestampType {
        if self.0 & Self::TIMESTAMP_TYPE_BIT != 0 {
            TimestampType::LogAppendTime
        } else {
            TimestampType::CreateTime
        }
    }

    #[must_use]
    pub fn is_transactional(self) -> bool {
        self.0 & Self::TRANSACTIONAL_BIT != 0
    }

    #[must_use]
    pub fn is_control_batch(self) -> bool {
        self.0 & Self::CONTROL_BIT != 0
    }

    #[must_use]
    pub fn with_compression(self, c: CompressionType) -> Self {
        let cleared = self.0 & !0x07;
        Self(cleared | i16::from(c.as_attribute_bits()))
    }

    #[must_use]
    pub fn with_timestamp_type(self, t: TimestampType) -> Self {
        match t {
            TimestampType::CreateTime => Self(self.0 & !Self::TIMESTAMP_TYPE_BIT),
            TimestampType::LogAppendTime => Self(self.0 | Self::TIMESTAMP_TYPE_BIT),
        }
    }

    #[must_use]
    pub fn with_transactional(self, b: bool) -> Self {
        if b {
            Self(self.0 | Self::TRANSACTIONAL_BIT)
        } else {
            Self(self.0 & !Self::TRANSACTIONAL_BIT)
        }
    }

    #[must_use]
    pub fn with_control(self, b: bool) -> Self {
        if b {
            Self(self.0 | Self::CONTROL_BIT)
        } else {
            Self(self.0 & !Self::CONTROL_BIT)
        }
    }
}

impl Default for Attributes {
    fn default() -> Self {
        Self(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_compression::CompressionType;

    macro_rules! attr_case {
        ($name:ident, $bits:expr, $codec:expr, $ts:expr, $txn:expr, $ctrl:expr) => {
            #[test]
            fn $name() {
                let a = Attributes($bits);
                assert_eq!(
                    a.compression(),
                    $codec,
                    "compression mismatch in {}",
                    stringify!($name)
                );
                assert_eq!(
                    a.timestamp_type(),
                    $ts,
                    "timestamp_type mismatch in {}",
                    stringify!($name)
                );
                assert_eq!(
                    a.is_transactional(),
                    $txn,
                    "is_transactional mismatch in {}",
                    stringify!($name)
                );
                assert_eq!(
                    a.is_control_batch(),
                    $ctrl,
                    "is_control_batch mismatch in {}",
                    stringify!($name)
                );
            }
        };
    }

    attr_case!(zero,          0,                     CompressionType::None,   TimestampType::CreateTime,    false, false);
    attr_case!(gzip_only,     0b0000_0000_0000_0001, CompressionType::Gzip,   TimestampType::CreateTime,    false, false);
    attr_case!(snappy_only,   0b0000_0000_0000_0010, CompressionType::Snappy, TimestampType::CreateTime,    false, false);
    attr_case!(lz4_only,      0b0000_0000_0000_0011, CompressionType::Lz4,    TimestampType::CreateTime,    false, false);
    attr_case!(zstd_only,     0b0000_0000_0000_0100, CompressionType::Zstd,   TimestampType::CreateTime,    false, false);
    attr_case!(log_append,    0b0000_0000_0000_1000, CompressionType::None,   TimestampType::LogAppendTime, false, false);
    attr_case!(transactional, 0b0000_0000_0001_0000, CompressionType::None,   TimestampType::CreateTime,    true,  false);
    attr_case!(control,       0b0000_0000_0010_0000, CompressionType::None,   TimestampType::CreateTime,    false, true);
    attr_case!(
        all_set,
        0b0000_0000_0011_1100,
        CompressionType::Zstd,
        TimestampType::LogAppendTime,
        true,
        true
    );

    #[test]
    fn builder_round_trip() {
        let a = Attributes::default()
            .with_compression(CompressionType::Snappy)
            .with_timestamp_type(TimestampType::LogAppendTime)
            .with_transactional(true)
            .with_control(false);

        assert_eq!(a.compression(), CompressionType::Snappy);
        assert_eq!(a.timestamp_type(), TimestampType::LogAppendTime);
        assert!(a.is_transactional());
        assert!(!a.is_control_batch());
    }

    #[test]
    fn replacing_compression_clears_old_bits() {
        // Starting with Lz4 (bits 0-2 = 011), switching to Gzip (= 001)
        // must clear bit 1, not OR over it.
        let a = Attributes::default().with_compression(CompressionType::Lz4);
        let b = a.with_compression(CompressionType::Gzip);
        assert_eq!(b.compression(), CompressionType::Gzip);
        assert_eq!(b.0 & 0x07, 1);
    }
}
