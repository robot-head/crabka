//! `KRaft` control-record framing. Control records (`LeaderChange`,
//! `SnapshotHeader`, `SnapshotFooter`) live in a batch with the control bit set;
//! the record key is `version (i16) + type (i16)` and the value is the message
//! body.

use bytes::{BufMut, Bytes, BytesMut};

use crate::records::{Attributes, Record, RecordBatch};

/// `KRaft` control record types (the i16 written after the i16 version in the
/// key).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ControlRecordType {
    LeaderChange = 2,
    SnapshotHeader = 3,
    SnapshotFooter = 4,
}

/// Control record key version (Kafka writes 0).
const CONTROL_KEY_VERSION: i16 = 0;

/// Build a control record key: `version(i16) + type(i16)`.
#[must_use]
pub fn control_record_key(ty: ControlRecordType) -> Bytes {
    let mut key = BytesMut::with_capacity(4);
    key.put_i16(CONTROL_KEY_VERSION);
    key.put_i16(ty as i16);
    key.freeze()
}

/// Encode a single-record control batch at `base_offset` with the control bit
/// set, returning the full v2 `RecordBatch` bytes (CRC computed by the encoder).
///
/// # Panics
/// Panics only if the underlying record-batch encoder fails, which cannot happen
/// for an uncompressed in-range single-record batch.
#[must_use]
pub fn encode_control_batch(base_offset: i64, key: Bytes, value: Bytes) -> Bytes {
    let batch = RecordBatch {
        base_offset,
        attributes: Attributes::default().with_control(true),
        records: vec![Record {
            key: Some(key),
            value: Some(value),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut out = BytesMut::new();
    batch
        .encode(&mut out)
        .expect("control batch encodes (no compression, in-range)");
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::Buf;

    #[test]
    fn control_key_is_version_then_type() {
        let key = control_record_key(ControlRecordType::SnapshotHeader);
        let mut cur: &[u8] = &key;
        assert!(cur.get_i16() == 0); // version
        assert!(cur.get_i16() == 3); // SnapshotHeader type
    }

    #[test]
    fn control_batch_sets_control_bit() {
        let key = control_record_key(ControlRecordType::LeaderChange);
        let batch = encode_control_batch(42, key, bytes::Bytes::from_static(b"\x00\x00"));
        let mut cur: &[u8] = &batch;
        let decoded = RecordBatch::decode(&mut cur).expect("control batch decodes");
        assert!(decoded.base_offset == 42);
        assert!(cur.is_empty());
        // magic byte at offset 16, attributes i16 at offset 21..23; control bit = 0x20.
        let attrs = i16::from_be_bytes([batch[21], batch[22]]);
        assert!(attrs & 0x20 != 0);
    }
}
