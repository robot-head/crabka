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
/// - bit 6:    `has_delete_horizon` (KIP-534; `base_timestamp` carries the horizon)
/// - bits 7-15: reserved
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Attributes(pub i16);

impl Attributes {
    pub const TIMESTAMP_TYPE_BIT: i16 = 1 << 3;
    pub const TRANSACTIONAL_BIT: i16 = 1 << 4;
    pub const CONTROL_BIT: i16 = 1 << 5;
    pub const DELETE_HORIZON_BIT: i16 = 1 << 6; // 0x40

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
    pub fn has_delete_horizon(self) -> bool {
        self.0 & Self::DELETE_HORIZON_BIT != 0
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

    #[must_use]
    pub fn with_delete_horizon(self, set: bool) -> Self {
        if set {
            Self(self.0 | Self::DELETE_HORIZON_BIT)
        } else {
            Self(self.0 & !Self::DELETE_HORIZON_BIT)
        }
    }
}

use std::mem::size_of;

use zerocopy::{
    BigEndian, FromBytes, Immutable, KnownLayout, Unaligned,
    byteorder::{I16, I32, I64, U32},
};

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
const _: [(); HEADER_LEN] = [(); size_of::<RecordBatchHeader>()];

/// Byte offset of the `base_offset` field (i64 BE, 8 bytes) within a v2
/// record-batch header.
pub const BASE_OFFSET_RANGE: std::ops::Range<usize> = 0..8;

/// Byte offset of the `partition_leader_epoch` field (i32 BE, 4 bytes)
/// within a v2 record-batch header.
pub const LEADER_EPOCH_RANGE: std::ops::Range<usize> = 12..16;

/// First byte covered by the v2 batch CRC (`attributes` onward). Bytes
/// `0..21` — `base_offset`, `batch_length`, `partition_leader_epoch`,
/// `magic`, and the `crc` field itself — are **outside** the CRC region.
pub const CRC_COVERAGE_START: usize = 21;

/// Patch `base_offset` (bytes 0..8) and `partition_leader_epoch`
/// (bytes 12..16) in place in a writable copy of the verbatim batch
/// bytes, writing both as big-endian.
///
/// Both fields lie **before** [`CRC_COVERAGE_START`], so this never
/// invalidates the producer's CRC — the broker can stamp the assigned
/// offset and leader epoch without recomputing CRC or touching the body.
///
/// # Panics
///
/// Panics if `buf` is shorter than [`HEADER_LEN`]; callers must validate
/// the batch header (e.g. via borrowed decode) first.
pub fn patch_base_offset_and_leader_epoch(buf: &mut [u8], base_offset: i64, leader_epoch: i32) {
    assert2::assert!(buf.len() >= HEADER_LEN);
    buf[BASE_OFFSET_RANGE].copy_from_slice(&base_offset.to_be_bytes());
    buf[LEADER_EPOCH_RANGE].copy_from_slice(&leader_epoch.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_compression::CompressionType;

    use super::*;

    struct AttributeCase {
        bits: i16,
        compression: CompressionType,
        timestamp_type: TimestampType,
        transactional: bool,
        control_batch: bool,
        delete_horizon: bool,
    }

    fn assert_attribute_case(case: &AttributeCase) {
        let attributes = Attributes(case.bits);
        assert2::assert!(attributes.compression() == case.compression);
        assert2::assert!(attributes.timestamp_type() == case.timestamp_type);
        assert2::assert!(attributes.is_transactional() == case.transactional);
        assert2::assert!(attributes.is_control_batch() == case.control_batch);
        assert2::assert!(attributes.has_delete_horizon() == case.delete_horizon);
    }

    #[test]
    fn attribute_compression_cases() {
        for case in [
            AttributeCase {
                bits: 0,
                compression: CompressionType::None,
                timestamp_type: TimestampType::CreateTime,
                transactional: false,
                control_batch: false,
                delete_horizon: false,
            },
            AttributeCase {
                bits: 0x01,
                compression: CompressionType::Gzip,
                timestamp_type: TimestampType::CreateTime,
                transactional: false,
                control_batch: false,
                delete_horizon: false,
            },
            AttributeCase {
                bits: 0x02,
                compression: CompressionType::Snappy,
                timestamp_type: TimestampType::CreateTime,
                transactional: false,
                control_batch: false,
                delete_horizon: false,
            },
            AttributeCase {
                bits: 0x03,
                compression: CompressionType::Lz4,
                timestamp_type: TimestampType::CreateTime,
                transactional: false,
                control_batch: false,
                delete_horizon: false,
            },
            AttributeCase {
                bits: 0x04,
                compression: CompressionType::Zstd,
                timestamp_type: TimestampType::CreateTime,
                transactional: false,
                control_batch: false,
                delete_horizon: false,
            },
        ] {
            assert_attribute_case(&case);
        }
    }

    #[test]
    fn attribute_flag_cases() {
        for case in [
            AttributeCase {
                bits: 0x08,
                compression: CompressionType::None,
                timestamp_type: TimestampType::LogAppendTime,
                transactional: false,
                control_batch: false,
                delete_horizon: false,
            },
            AttributeCase {
                bits: 0x10,
                compression: CompressionType::None,
                timestamp_type: TimestampType::CreateTime,
                transactional: true,
                control_batch: false,
                delete_horizon: false,
            },
            AttributeCase {
                bits: 0x20,
                compression: CompressionType::None,
                timestamp_type: TimestampType::CreateTime,
                transactional: false,
                control_batch: true,
                delete_horizon: false,
            },
            AttributeCase {
                bits: 0x40,
                compression: CompressionType::None,
                timestamp_type: TimestampType::CreateTime,
                transactional: false,
                control_batch: false,
                delete_horizon: true,
            },
            AttributeCase {
                bits: 0x7c,
                compression: CompressionType::Zstd,
                timestamp_type: TimestampType::LogAppendTime,
                transactional: true,
                control_batch: true,
                delete_horizon: true,
            },
        ] {
            assert_attribute_case(&case);
        }
    }

    #[test]
    fn builder_round_trip() {
        let a = Attributes::default()
            .with_compression(CompressionType::Snappy)
            .with_timestamp_type(TimestampType::LogAppendTime)
            .with_transactional(true)
            .with_control(false);

        // Snappy = bits 0-2 = 010, LogAppendTime = bit 3, transactional = bit 4.
        assert2::assert!(a == Attributes(0b0000_0000_0001_1010));
    }

    #[test]
    fn replacing_compression_clears_old_bits() {
        // Starting with Lz4 (bits 0-2 = 011), switching to Gzip (= 001)
        // must clear bit 1, not OR over it.
        let a = Attributes::default().with_compression(CompressionType::Lz4);
        let b = a.with_compression(CompressionType::Gzip);
        assert2::assert!(b.compression() == CompressionType::Gzip);
        assert2::assert!(b.0 & 0x07 == 1);
    }

    #[test]
    fn delete_horizon_bit_round_trips() {
        // Default has no delete horizon.
        let base = Attributes::default();
        assert2::assert!(!base.has_delete_horizon());

        // Setting it flips exactly bit 6 (mask 0x40).
        let set = base.with_delete_horizon(true);
        assert2::assert!(set.has_delete_horizon());
        assert2::assert!(set.0 & Attributes::DELETE_HORIZON_BIT == 0x40);

        // Orthogonal to control / transactional: setting those does not touch
        // bit 6, and bit 6 does not touch them.
        let combo = Attributes::default()
            .with_control(true)
            .with_transactional(true)
            .with_delete_horizon(true);
        // control = bit 5, transactional = bit 4, delete horizon = bit 6.
        assert2::assert!(combo == Attributes(0b0000_0000_0111_0000));

        // Clearing bit 6 leaves the others intact.
        let cleared = combo.with_delete_horizon(false);
        assert2::assert!(cleared == Attributes(0b0000_0000_0011_0000));
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
        buf[21..23].copy_from_slice(&0x1234i16.to_be_bytes()); // attributes
        buf[23..27].copy_from_slice(&3i32.to_be_bytes()); // last_offset_delta
        buf[27..35].copy_from_slice(&111i64.to_be_bytes()); // base_timestamp
        buf[35..43].copy_from_slice(&222i64.to_be_bytes()); // max_timestamp
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes()); // producer_id
        buf[51..53].copy_from_slice(&7i16.to_be_bytes()); // producer_epoch
        buf[53..57].copy_from_slice(&(-1i32).to_be_bytes()); // base_sequence
        buf[57..61].copy_from_slice(&4i32.to_be_bytes()); // records_count
        buf
    }

    #[derive(Debug, PartialEq)]
    struct HeaderProjection {
        base_offset: i64,
        batch_length: i32,
        partition_leader_epoch: i32,
        magic: i8,
        crc: u32,
        attributes: i16,
        last_offset_delta: i32,
        base_timestamp: i64,
        max_timestamp: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records_count: i32,
    }

    #[test]
    fn reads_complete_header() {
        let buf = sample_header_bytes();
        let h = RecordBatchHeader::ref_from_bytes(&buf[..]).unwrap();
        let actual = HeaderProjection {
            base_offset: h.base_offset.get(),
            batch_length: h.batch_length.get(),
            partition_leader_epoch: h.partition_leader_epoch.get(),
            magic: h.magic,
            crc: h.crc.get(),
            attributes: h.attributes.get(),
            last_offset_delta: h.last_offset_delta.get(),
            base_timestamp: h.base_timestamp.get(),
            max_timestamp: h.max_timestamp.get(),
            producer_id: h.producer_id.get(),
            producer_epoch: h.producer_epoch.get(),
            base_sequence: h.base_sequence.get(),
            records_count: h.records_count.get(),
        };
        let expected = HeaderProjection {
            base_offset: 100,
            batch_length: 77,
            partition_leader_epoch: 1,
            magic: 2,
            crc: 0x1234_5678,
            attributes: 0x1234,
            last_offset_delta: 3,
            base_timestamp: 111,
            max_timestamp: 222,
            producer_id: -1,
            producer_epoch: 7,
            base_sequence: -1,
            records_count: 4,
        };
        assert2::assert!(actual == expected);
    }

    #[test]
    fn header_is_exactly_61_bytes() {
        assert2::assert!(std::mem::size_of::<RecordBatchHeader>() == HEADER_LEN);
    }

    #[test]
    fn too_short_buffer_errors() {
        let buf = [0u8; HEADER_LEN - 1];
        assert2::assert!(RecordBatchHeader::ref_from_bytes(&buf[..]).is_err());
    }

    #[test]
    fn patch_writes_only_pre_crc_fields() {
        let mut buf = sample_header_bytes().to_vec();
        let crc_region_before = buf[CRC_COVERAGE_START..].to_vec();
        let crc_field_before = buf[17..21].to_vec();

        patch_base_offset_and_leader_epoch(&mut buf, 9_001, 42);

        // The two stamped fields changed to the expected big-endian values.
        let h = RecordBatchHeader::ref_from_bytes(&buf[..]).unwrap();
        check!(h.base_offset.get() == 9_001);
        check!(h.partition_leader_epoch.get() == 42);

        // The CRC field itself is untouched (no recompute).
        check!(&buf[17..21] == &crc_field_before[..]);
        // Everything in the CRC-covered region is byte-identical.
        check!(&buf[CRC_COVERAGE_START..] == &crc_region_before[..]);
    }

    #[test]
    fn crc_coverage_constants_match_field_layout() {
        // base_offset and leader_epoch are entirely below the CRC start;
        // attributes (the first CRC-covered field) begins exactly at 21.
        check!(BASE_OFFSET_RANGE.end <= CRC_COVERAGE_START);
        check!(LEADER_EPOCH_RANGE.end <= CRC_COVERAGE_START);
        check!(CRC_COVERAGE_START == 21);
    }
}
