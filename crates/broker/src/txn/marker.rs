//! Control-record construction. A commit/abort marker is a single-
//! record `RecordBatch` with `is_control_batch=true` and
//! `is_transactional=true` in attributes.
//!
//! The record key layout matches Apache Kafka `EndTransactionMarker`:
//!   version: i16 (big-endian) = 0
//!   type:    i16 (big-endian), 0 = ABORT, 1 = COMMIT
//! The record value matches Kafka's `EndTxnMarker` schema:
//!   version:           i16 (big-endian) = 0
//!   `coordinator_epoch`: i32 (big-endian)

use bytes::Bytes;
use crabka_log::{Offset, ProducerId};
use crabka_protocol::records::{Attributes, Record, RecordBatch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerType {
    Commit,
    Abort,
}

impl MarkerType {
    fn type_code(self) -> i16 {
        match self {
            MarkerType::Commit => 1,
            MarkerType::Abort => 0,
        }
    }
}

pub fn build_marker_batch(
    producer_id: ProducerId,
    producer_epoch: i16,
    base_offset: Offset,
    marker_type: MarkerType,
    coordinator_epoch: i32,
) -> RecordBatch {
    let mut key = Vec::with_capacity(4);
    key.extend_from_slice(&0i16.to_be_bytes()); // version
    key.extend_from_slice(&marker_type.type_code().to_be_bytes());

    let mut value = Vec::with_capacity(6);
    value.extend_from_slice(&0i16.to_be_bytes()); // version
    value.extend_from_slice(&coordinator_epoch.to_be_bytes());

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
            value: Some(Bytes::from(value)),
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
        let b = build_marker_batch(ProducerId(1000), 0, Offset(7), MarkerType::Commit, 19);
        assert!(b.attributes.is_transactional());
        assert!(b.attributes.is_control_batch());
    }

    #[test]
    fn abort_marker_key_starts_with_version_zero_then_type_zero() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Abort, 19);
        let key = b.records[0].key.as_ref().unwrap();
        // i16 BE version 0, then i16 BE control type 0 (abort).
        assert!(&key[..] == &[0u8, 0, 0, 0][..]);
    }

    #[test]
    fn commit_marker_key_type_is_one() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Commit, 19);
        let key = b.records[0].key.as_ref().unwrap();
        assert!(&key[2..] == &1i16.to_be_bytes());
    }

    #[test]
    fn marker_value_contains_version_and_coordinator_epoch() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Commit, 19);
        let value = b.records[0].value.as_ref().unwrap();
        assert!(&value[..2] == &0i16.to_be_bytes());
        assert!(&value[2..] == &19i32.to_be_bytes());
    }
}
