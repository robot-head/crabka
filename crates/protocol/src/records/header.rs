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
/// - bit 3:    timestamp type (0 = `CreateTime`, 1 = `LogAppendTime`)
/// - bit 4:    `is_transactional`
/// - bit 5:    `is_control_batch`
/// - bit 6:    `has_delete_horizon_ms` (Kafka 2.8+; not surfaced separately here)
/// - bits 7-15: reserved
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

use std::mem::size_of;
use zerocopy::byteorder::{I16, I32, I64, U32};
use zerocopy::{BigEndian, FromBytes, Immutable, KnownLayout, Unaligned};

/// The fixed 61-byte v2 record-batch header, reinterpreted in place from
/// the wire bytes via `zerocopy`.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct RecordBatchHeader {
    pub base_offset: I64<BigEndian>,
    pub batch_length: I32<BigEndian>,
    pub partition_leader_epoch: I32<BigEndian>,
    pub magic: i8,
    pub crc: U32<BigEndian>,
    pub attributes: I16<BigEndian>,
    pub last_offset_delta: I32<BigEndian>,
    pub base_timestamp: I64<BigEndian>,
    pub max_timestamp: I64<BigEndian>,
    pub producer_id: I64<BigEndian>,
    pub producer_epoch: I16<BigEndian>,
    pub base_sequence: I32<BigEndian>,
    pub records_count: I32<BigEndian>,
}

/// Size of the v2 record-batch header in bytes.
pub const HEADER_LEN: usize = 61;

// Compile-time assertion that the layout is exactly 61 bytes.
const _: () = assert!(size_of::<RecordBatchHeader>() == HEADER_LEN);

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

    attr_case!(
        zero,
        0,
        CompressionType::None,
        TimestampType::CreateTime,
        false,
        false
    );
    attr_case!(
        gzip_only,
        0b0000_0000_0000_0001,
        CompressionType::Gzip,
        TimestampType::CreateTime,
        false,
        false
    );
    attr_case!(
        snappy_only,
        0b0000_0000_0000_0010,
        CompressionType::Snappy,
        TimestampType::CreateTime,
        false,
        false
    );
    attr_case!(
        lz4_only,
        0b0000_0000_0000_0011,
        CompressionType::Lz4,
        TimestampType::CreateTime,
        false,
        false
    );
    attr_case!(
        zstd_only,
        0b0000_0000_0000_0100,
        CompressionType::Zstd,
        TimestampType::CreateTime,
        false,
        false
    );
    attr_case!(
        log_append,
        0b0000_0000_0000_1000,
        CompressionType::None,
        TimestampType::LogAppendTime,
        false,
        false
    );
    attr_case!(
        transactional,
        0b0000_0000_0001_0000,
        CompressionType::None,
        TimestampType::CreateTime,
        true,
        false
    );
    attr_case!(
        control,
        0b0000_0000_0010_0000,
        CompressionType::None,
        TimestampType::CreateTime,
        false,
        true
    );
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

    /// Build a sample 61-byte header with known values. Reused across the
    /// header table tests below.
    fn sample_header_bytes() -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..8].copy_from_slice(&100i64.to_be_bytes()); // base_offset
        buf[8..12].copy_from_slice(&77i32.to_be_bytes()); // batch_length
        buf[12..16].copy_from_slice(&1i32.to_be_bytes()); // partition_leader_epoch
        buf[16] = 2; // magic
        buf[17..21].copy_from_slice(&0x1234_5678u32.to_be_bytes()); // crc
        buf[21..23].copy_from_slice(&0i16.to_be_bytes()); // attributes
        buf[23..27].copy_from_slice(&3i32.to_be_bytes()); // last_offset_delta
        buf[27..35].copy_from_slice(&111i64.to_be_bytes()); // base_timestamp
        buf[35..43].copy_from_slice(&222i64.to_be_bytes()); // max_timestamp
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes()); // producer_id
        buf[51..53].copy_from_slice(&7i16.to_be_bytes()); // producer_epoch
        buf[53..57].copy_from_slice(&(-1i32).to_be_bytes()); // base_sequence
        buf[57..61].copy_from_slice(&4i32.to_be_bytes()); // records_count
        buf
    }

    macro_rules! header_field {
        ($name:ident, $field:ident, $expected:expr) => {
            #[test]
            fn $name() {
                let buf = sample_header_bytes();
                let h = RecordBatchHeader::ref_from_bytes(&buf[..]).expect("header reinterpret");
                assert_eq!(h.$field.get(), $expected);
            }
        };
    }

    header_field!(reads_base_offset, base_offset, 100i64);
    header_field!(reads_batch_length, batch_length, 77i32);
    header_field!(reads_partition_leader_epoch, partition_leader_epoch, 1i32);
    header_field!(reads_crc, crc, 0x1234_5678u32);
    header_field!(reads_last_offset_delta, last_offset_delta, 3i32);
    header_field!(reads_base_timestamp, base_timestamp, 111i64);
    header_field!(reads_max_timestamp, max_timestamp, 222i64);
    header_field!(reads_producer_id, producer_id, -1i64);
    header_field!(reads_producer_epoch, producer_epoch, 7i16);
    header_field!(reads_base_sequence, base_sequence, -1i32);
    header_field!(reads_records_count, records_count, 4i32);

    #[test]
    fn reads_magic_directly() {
        let buf = sample_header_bytes();
        let h = RecordBatchHeader::ref_from_bytes(&buf[..]).unwrap();
        assert_eq!(h.magic, 2);
    }

    #[test]
    fn header_is_exactly_61_bytes() {
        assert_eq!(std::mem::size_of::<RecordBatchHeader>(), HEADER_LEN);
    }

    #[test]
    fn too_short_buffer_errors() {
        let buf = [0u8; HEADER_LEN - 1];
        assert!(RecordBatchHeader::ref_from_bytes(&buf[..]).is_err());
    }
}
