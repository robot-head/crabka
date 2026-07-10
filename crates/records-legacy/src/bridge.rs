//! Convert between v2 `RecordBatch` (crabka-protocol) and v0/v1 MessageSet
//! payloads.
//!
//! Down-conversion (v2 → v0/v1): produce a [`ParsedRecord`] stream from a
//! v2 batch (decompressing the v2 body once), then re-emit it as a flat
//! or compressed MessageSet.
//!
//! Up-conversion (v0/v1 → v2): decode a MessageSet into ParsedRecords,
//! then synthesize a v2 RecordBatch with one record per legacy message.
//!
//! Control records (v2 `is_control_batch == true`) are filtered: v0/v1
//! has no concept of them, and Kafka's reference broker behavior is to
//! drop them on down-conversion.

use bytes::{Bytes, BytesMut};
use crabka_compression::CompressionType;
use crabka_ids::Offset;
use crabka_protocol::records::{Attributes, Record, RecordBatch, RecordsError};

use crate::{
    error::LegacyRecordsError,
    message::Magic,
    set::{
        ParsedRecord, decode_message_set, encode_compressed_message_set, encode_flat_message_set,
    },
};

/// Iterate the v2 batch's records, dropping control batches entirely.
///
/// Each emitted `ParsedRecord` has its absolute offset reconstructed as
/// `base_offset + offset_delta`, and (for v1 target) its absolute
/// timestamp as `base_timestamp + timestamp_delta`.
#[must_use]
pub fn parsed_from_v2(batch: &RecordBatch, target: Magic) -> Vec<ParsedRecord> {
    if batch.attributes.is_control_batch() {
        return Vec::new();
    }
    batch
        .records
        .iter()
        .map(|r| ParsedRecord {
            offset: Offset(batch.base_offset + i64::from(r.offset_delta)),
            timestamp: match target {
                Magic::V0 => None,
                Magic::V1 => Some(batch.base_timestamp + r.timestamp_delta),
            },
            key: r.key.clone(),
            value: r.value.clone(),
        })
        .collect()
}

/// Down-convert a v2 RecordBatch to v0/v1 MessageSet bytes.
///
/// If the v2 batch carried gzip or snappy compression, the output also
/// uses that codec (wrapped MessageSet). LZ4 is preserved as well. Zstd
/// — not representable in v0/v1 — is emitted as an uncompressed
/// MessageSet (the records have already been decompressed in the v2
/// decode path).
pub fn v2_to_legacy(batch: &RecordBatch, target: Magic) -> Result<Bytes, LegacyRecordsError> {
    let records = parsed_from_v2(batch, target);
    let mut out = BytesMut::new();
    if records.is_empty() {
        return Ok(out.freeze());
    }
    let codec = batch.attributes.compression();
    match codec {
        CompressionType::Gzip | CompressionType::Snappy | CompressionType::Lz4 => {
            encode_compressed_message_set(&records, target, codec, &mut out)?;
        }
        // None, Zstd, or any future variant: emit uncompressed. Zstd can't
        // be represented in v0/v1; the v2 decode path already produced
        // decompressed records for us, so emitting flat is correct.
        _ => {
            encode_flat_message_set(records, target, &mut out);
        }
    }
    Ok(out.freeze())
}

/// Up-convert a v0/v1 MessageSet to a v2 RecordBatch suitable for the
/// log write path.
///
/// `partition_leader_epoch` is set to `-1` (Kafka's sentinel for
/// "unknown"). The caller — typically the Produce handler — should
/// overwrite it with the current leader epoch before append.
pub fn legacy_to_v2(set_bytes: &[u8]) -> Result<RecordBatch, LegacyRecordsError> {
    let mut cur = set_bytes;
    let records = decode_message_set(&mut cur, set_bytes.len())?;
    if records.is_empty() {
        return Ok(RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
        });
    }
    let base_offset = records[0].offset;
    let base_timestamp = records
        .iter()
        .filter_map(|r| r.timestamp)
        .min()
        .unwrap_or(-1);
    let max_timestamp = records
        .iter()
        .filter_map(|r| r.timestamp)
        .max()
        .unwrap_or(-1);
    let last_offset_delta = (records.last().unwrap().offset.0 - base_offset.0) as i32;

    let out_records: Vec<Record> = records
        .iter()
        .map(|r| Record {
            attributes: 0,
            timestamp_delta: r.timestamp.map_or(0, |ts| ts - base_timestamp),
            offset_delta: (r.offset.0 - base_offset.0) as i32,
            key: r.key.clone(),
            value: r.value.clone(),
            headers: Vec::new(),
        })
        .collect();

    Ok(RecordBatch {
        base_offset: base_offset.0,
        partition_leader_epoch: -1,
        attributes: Attributes::default(),
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: out_records,
    })
}

impl From<RecordsError> for LegacyRecordsError {
    fn from(e: RecordsError) -> Self {
        LegacyRecordsError::Malformed(format!("v2 records error: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use crabka_protocol::records::{Record, RecordBatch};

    use super::*;

    fn v2_batch(codec: CompressionType) -> RecordBatch {
        RecordBatch {
            base_offset: 1000,
            partition_leader_epoch: 5,
            attributes: Attributes::default().with_compression(codec),
            last_offset_delta: 2,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_500,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![
                Record {
                    attributes: 0,
                    offset_delta: 0,
                    timestamp_delta: 0,
                    key: Some(Bytes::from_static(b"a")),
                    value: Some(Bytes::from_static(b"1")),
                    headers: vec![],
                },
                Record {
                    attributes: 0,
                    offset_delta: 1,
                    timestamp_delta: 100,
                    key: Some(Bytes::from_static(b"b")),
                    value: Some(Bytes::from_static(b"2")),
                    headers: vec![],
                },
                Record {
                    attributes: 0,
                    offset_delta: 2,
                    timestamp_delta: 500,
                    key: None,
                    value: Some(Bytes::from_static(b"3")),
                    headers: vec![],
                },
            ],
        }
    }

    #[test]
    fn down_then_up_round_trips_complete_batches() {
        for (name, magic, codec) in [
            ("v1 uncompressed", Magic::V1, CompressionType::None),
            ("v0 uncompressed", Magic::V0, CompressionType::None),
            ("v1 gzip", Magic::V1, CompressionType::Gzip),
            ("v1 snappy", Magic::V1, CompressionType::Snappy),
        ] {
            let v2 = v2_batch(codec);
            let legacy_bytes = v2_to_legacy(&v2, magic).unwrap();
            let round = legacy_to_v2(&legacy_bytes).unwrap();

            let mut expected = v2;
            expected.partition_leader_epoch = -1;
            expected.attributes = Attributes::default();
            if magic == Magic::V0 {
                expected.base_timestamp = -1;
                expected.max_timestamp = -1;
                for record in &mut expected.records {
                    record.timestamp_delta = 0;
                }
            }
            assert_eq!(round, expected, "case {name}");
        }
    }

    #[test]
    fn control_batch_filtered() {
        let mut v2 = v2_batch(CompressionType::None);
        v2.attributes = v2.attributes.with_control(true);
        let legacy_bytes = v2_to_legacy(&v2, Magic::V1).unwrap();
        assert!(
            legacy_bytes.is_empty(),
            "control batch must produce empty MessageSet"
        );
    }

    #[test]
    fn empty_records_no_output() {
        let mut v2 = v2_batch(CompressionType::None);
        v2.records.clear();
        v2.last_offset_delta = 0;
        let legacy_bytes = v2_to_legacy(&v2, Magic::V1).unwrap();
        assert!(legacy_bytes.is_empty());
    }

    #[test]
    fn headers_are_dropped_on_down_conversion() {
        let mut v2 = v2_batch(CompressionType::None);
        v2.records[0].headers = vec![crabka_protocol::records::RecordHeader {
            key: "x".to_string(),
            value: Some(Bytes::from_static(b"y")),
        }];
        let legacy_bytes = v2_to_legacy(&v2, Magic::V1).unwrap();
        let mut cur: &[u8] = &legacy_bytes;
        let recs = decode_message_set(&mut cur, legacy_bytes.len()).unwrap();
        // No structural representation of headers in v0/v1.
        assert_eq!(
            recs,
            vec![
                ParsedRecord {
                    offset: Offset(1000),
                    timestamp: Some(1_700_000_000),
                    key: Some(Bytes::from_static(b"a")),
                    value: Some(Bytes::from_static(b"1")),
                },
                ParsedRecord {
                    offset: Offset(1001),
                    timestamp: Some(1_700_000_100),
                    key: Some(Bytes::from_static(b"b")),
                    value: Some(Bytes::from_static(b"2")),
                },
                ParsedRecord {
                    offset: Offset(1002),
                    timestamp: Some(1_700_000_500),
                    key: None,
                    value: Some(Bytes::from_static(b"3")),
                },
            ]
        );
    }

    // --- mutation-coverage tests --------------------------------------------
    //
    // The round-trips above unwrap compression transparently and never assert
    // the v2 batch's sentinel/offset fields, so they pass through a flat-vs-
    // compressed swap and through sentinel/arithmetic flips. These pin them.

    #[test]
    fn down_convert_gzip_emits_compressed_wrapper() {
        // A gzip v2 batch must down-convert to a COMPRESSED wrapper, not a flat
        // (uncompressed) set: deleting the Gzip|Snappy|Lz4 arm would fall
        // through to the flat encoder, which still round-trips but is wrong.
        use bytes::Buf;
        let bytes = v2_to_legacy(&v2_batch(CompressionType::Gzip), Magic::V1).unwrap();
        let mut cur: &[u8] = &bytes;
        let _offset = cur.get_i64();
        let size = cur.get_i32() as usize;
        let msg = crate::message::Message::decode_from(&mut cur, size).unwrap();
        assert!(msg.compression() == CompressionType::Gzip);
    }

    #[test]
    fn up_convert_empty_set_returns_sentinels() {
        let rb = legacy_to_v2(&[]).unwrap();
        assert_eq!(
            rb,
            RecordBatch {
                base_offset: 0,
                partition_leader_epoch: -1,
                attributes: Attributes::default(),
                last_offset_delta: 0,
                base_timestamp: 0,
                max_timestamp: 0,
                producer_id: -1,
                producer_epoch: -1,
                base_sequence: -1,
                records: Vec::new(),
            }
        );
    }
}
