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
use crabka_protocol::records::{Attributes, Record, RecordBatch, RecordsError};

use crate::error::LegacyRecordsError;
use crate::message::Magic;
use crate::set::{
    ParsedRecord, decode_message_set, encode_compressed_message_set, encode_flat_message_set,
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
            offset: batch.base_offset + i64::from(r.offset_delta),
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
    let last_offset_delta = (records.last().unwrap().offset - base_offset) as i32;

    let out_records: Vec<Record> = records
        .iter()
        .map(|r| Record {
            attributes: 0,
            timestamp_delta: r.timestamp.map_or(0, |ts| ts - base_timestamp),
            offset_delta: (r.offset - base_offset) as i32,
            key: r.key.clone(),
            value: r.value.clone(),
            headers: Vec::new(),
        })
        .collect();

    Ok(RecordBatch {
        base_offset,
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
    use super::*;
    use assert2::assert;
    use assert2::check;
    use bytes::Bytes;
    use crabka_protocol::records::{Record, RecordBatch};

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
    fn down_then_up_v1_uncompressed() {
        let v2 = v2_batch(CompressionType::None);
        let legacy_bytes = v2_to_legacy(&v2, Magic::V1).unwrap();
        let round = legacy_to_v2(&legacy_bytes).unwrap();
        assert!(round.base_offset == 1000);
        assert!(round.records.len() == 3);
        for (i, r) in round.records.iter().enumerate() {
            check!(r.offset_delta == i as i32);
            check!(r.key == v2.records[i].key);
            check!(r.value == v2.records[i].value);
            check!(r.timestamp_delta == v2.records[i].timestamp_delta);
            check!(r.headers.is_empty(), "headers dropped in down-conversion");
        }
    }

    #[test]
    fn down_then_up_v0_no_timestamps() {
        let v2 = v2_batch(CompressionType::None);
        let legacy_bytes = v2_to_legacy(&v2, Magic::V0).unwrap();
        let round = legacy_to_v2(&legacy_bytes).unwrap();
        assert!(round.records.len() == 3);
        // v0 has no timestamps, so all deltas collapse to 0.
        for r in &round.records {
            assert!(r.timestamp_delta == 0);
        }
    }

    #[test]
    fn down_v1_gzip_then_up_preserves_offsets() {
        let v2 = v2_batch(CompressionType::Gzip);
        let legacy_bytes = v2_to_legacy(&v2, Magic::V1).unwrap();
        let round = legacy_to_v2(&legacy_bytes).unwrap();
        assert!(round.base_offset == 1000);
        let offsets: Vec<_> = round
            .records
            .iter()
            .map(|r| 1000i64 + i64::from(r.offset_delta))
            .collect();
        assert!(offsets == vec![1000, 1001, 1002]);
    }

    #[test]
    fn down_v1_snappy_then_up_preserves_payloads() {
        let v2 = v2_batch(CompressionType::Snappy);
        let legacy_bytes = v2_to_legacy(&v2, Magic::V1).unwrap();
        let round = legacy_to_v2(&legacy_bytes).unwrap();
        assert!(round.records.len() == 3);
        check!(round.records[2].value == Some(Bytes::from_static(b"3")));
        check!(round.records[2].key == None);
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
        assert!(recs.len() == 3);
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
        check!(rb.records.is_empty());
        check!(rb.base_offset == 0);
        check!(rb.partition_leader_epoch == -1);
        check!(rb.producer_id == -1);
        check!(rb.producer_epoch == -1);
        check!(rb.base_sequence == -1);
    }

    #[test]
    fn up_convert_v0_timestamps_default_to_minus_one() {
        // v0 carries no timestamps, so base/max timestamp fall back to -1.
        let v2 = v2_batch(CompressionType::None);
        let legacy_bytes = v2_to_legacy(&v2, Magic::V0).unwrap();
        let round = legacy_to_v2(&legacy_bytes).unwrap();
        assert!(round.base_timestamp == -1);
        assert!(round.max_timestamp == -1);
    }

    #[test]
    fn up_convert_sets_offsets_and_sentinels() {
        // Offsets 1000..=1002 -> last_offset_delta = 1002 - 1000 = 2, and the
        // producer/leader-epoch sentinels are -1.
        let v2 = v2_batch(CompressionType::None);
        let legacy_bytes = v2_to_legacy(&v2, Magic::V1).unwrap();
        let round = legacy_to_v2(&legacy_bytes).unwrap();
        check!(round.last_offset_delta == 2);
        check!(round.partition_leader_epoch == -1);
        check!(round.producer_id == -1);
        check!(round.producer_epoch == -1);
        check!(round.base_sequence == -1);
    }
}
