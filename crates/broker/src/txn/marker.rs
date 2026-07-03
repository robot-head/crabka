//! Control-record construction. A commit/abort marker is a single-
//! record `RecordBatch` with `is_control_batch=true` +
//! `is_transactional=true` in attributes.
//!
//! Record key layout (matches Apache Kafka `EndTransactionMarker`):
//!   version: i16 (big-endian) = 0
//!   type:    i16 (big-endian) — 0 = ABORT, 1 = COMMIT
//! Record value is empty.

use bytes::Bytes;
use crabka_log::{Offset, ProducerId};
use crabka_protocol::records::{Attributes, Record, RecordBatch};

// `MarkerType` and `build_marker_batch` are intentionally `pub` for reuse
// by the EndTxn handler. Suppress dead_code until then.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerType {
    Commit,
    Abort,
}

impl MarkerType {
    #[allow(dead_code)]
    fn type_code(self) -> i16 {
        match self {
            MarkerType::Commit => 1,
            MarkerType::Abort => 0,
        }
    }
}

#[allow(dead_code)]
pub fn build_marker_batch(
    producer_id: ProducerId,
    producer_epoch: i16,
    base_offset: Offset,
    marker_type: MarkerType,
) -> RecordBatch {
    let mut key = Vec::with_capacity(4);
    key.extend_from_slice(&0i16.to_be_bytes()); // version
    key.extend_from_slice(&marker_type.type_code().to_be_bytes());

    let attrs = Attributes::default()
        .with_transactional(true)
        .with_control(true);

    RecordBatch {
        attributes: attrs,
        base_offset: base_offset.0,
        last_offset_delta: 0,
        // Unwrap into the raw-`i64` protocol `RecordBatch` field at the wire seam.
        producer_id: producer_id.get(),
        producer_epoch,
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::from(key)),
            value: None,
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn commit_marker_attribute_bits_set() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(7), MarkerType::Commit);
        assert!(b.attributes.is_transactional());
        assert!(b.attributes.is_control_batch());
    }

    #[test]
    fn abort_marker_key_starts_with_version_zero_then_type_zero() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Abort);
        let key = b.records[0].key.as_ref().unwrap();
        // i16 BE version 0, then i16 BE control type 0 (abort).
        assert!(&key[..] == &[0u8, 0, 0, 0][..]);
    }

    #[test]
    fn commit_marker_key_type_is_one() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Commit);
        let key = b.records[0].key.as_ref().unwrap();
        assert!(&key[2..] == &1i16.to_be_bytes());
    }
}
