//! KIP-630 metadata snapshot artifact: `<offset>-<epoch>.checkpoint`.
//!
//! The format layer: image ⇄ record sequence, the `.checkpoint` filename
//! grammar, and the canonical on-disk bytes (header/data/footer Kafka
//! `RecordBatch`es). The engine ([`crate::kraft::KraftController`]) writes the
//! `.checkpoint` directly (no `.meta` sidecar) and recovers from it via
//! [`SnapshotReader::read_records`].

use bytes::{BufMut, Bytes, BytesMut};
use serde_wincode::SerdeCompat;
use wincode::{Deserialize as _, Serialize as _};

use crabka_metadata::{MetadataImage, MetadataRecord};
use crabka_protocol::records::header::Attributes;
use crabka_protocol::records::{Record, RecordBatch};

use crate::error::RaftError;

/// Control-record key type codes (KIP-630). The key of a control record
/// is `i16 version` + `i16 type`.
const CONTROL_TYPE_SNAPSHOT_HEADER: i16 = 3;
const CONTROL_TYPE_SNAPSHOT_FOOTER: i16 = 4;

/// Version stamped into the snapshot header/footer control records.
const SNAPSHOT_HEADER_VERSION: i16 = 0;
const SNAPSHOT_FOOTER_VERSION: i16 = 0;

/// Identifies a snapshot by the log position it covers: `end_offset` is
/// the offset of the last record contained in the snapshot, and `epoch`
/// is the leader epoch at that offset. The engine names the on-disk artifact
/// `<end_offset>-<epoch>.checkpoint` (both fields zero-padded so lexical sort
/// matches numeric sort) and parses it back directly.
///
/// Serializes a [`MetadataImage`] into the canonical KIP-630
/// `.checkpoint` byte layout: a header control batch, one data batch of
/// `MetadataRecord` values, then a footer control batch — concatenated
/// encoded Kafka `RecordBatch`es.
pub(crate) struct SnapshotWriter;

impl SnapshotWriter {
    /// Produce the full `.checkpoint` bytes for `image`.
    /// `last_contained_log_timestamp` is the create-time of the last log
    /// record folded into this snapshot (recorded in the header).
    pub(crate) fn serialize(
        image: &MetadataImage,
        last_contained_log_timestamp: i64,
    ) -> Result<Bytes, RaftError> {
        let records = image.to_records();
        let mut out = BytesMut::new();

        // (1) Header control batch at base_offset 0.
        let mut header_key = Vec::with_capacity(4);
        header_key.put_i16(SNAPSHOT_HEADER_VERSION);
        header_key.put_i16(CONTROL_TYPE_SNAPSHOT_HEADER);
        let mut header_value = Vec::with_capacity(10);
        header_value.put_i16(SNAPSHOT_HEADER_VERSION);
        header_value.put_i64(last_contained_log_timestamp);
        encode_control_batch(&mut out, 0, header_key, header_value)?;

        // (2) Data batch at base_offset 1: one record per MetadataRecord.
        if !records.is_empty() {
            let last_offset_delta = i32::try_from(records.len() - 1).unwrap_or(i32::MAX);
            let mut data_records = Vec::with_capacity(records.len());
            for (i, rec) in records.iter().enumerate() {
                let payload = <SerdeCompat<MetadataRecord>>::serialize(rec)?;
                data_records.push(Record {
                    offset_delta: i32::try_from(i).unwrap_or(i32::MAX),
                    value: Some(Bytes::from(payload)),
                    ..Default::default()
                });
            }
            let data_batch = RecordBatch {
                base_offset: 1,
                last_offset_delta,
                records: data_records,
                ..Default::default()
            };
            data_batch.encode(&mut out)?;
        }

        // (3) Footer control batch.
        let footer_base_offset = if records.is_empty() {
            1
        } else {
            1 + i64::try_from(records.len()).unwrap_or(i64::MAX)
        };
        let mut footer_key = Vec::with_capacity(4);
        footer_key.put_i16(SNAPSHOT_FOOTER_VERSION);
        footer_key.put_i16(CONTROL_TYPE_SNAPSHOT_FOOTER);
        let mut footer_value = Vec::with_capacity(2);
        footer_value.put_i16(SNAPSHOT_FOOTER_VERSION);
        encode_control_batch(&mut out, footer_base_offset, footer_key, footer_value)?;

        Ok(out.freeze())
    }
}

/// Encode a single-record control `RecordBatch` (the control-batch
/// attribute bit set) carrying `key`/`value` into `out`.
fn encode_control_batch(
    out: &mut BytesMut,
    base_offset: i64,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), RaftError> {
    let batch = RecordBatch {
        base_offset,
        attributes: Attributes::default().with_control(true),
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Default::default()
        }],
        ..Default::default()
    };
    batch.encode(out)?;
    Ok(())
}

/// Reads a canonical `.checkpoint` byte stream back into the sequence of
/// `MetadataRecord`s it contains (skipping the header/footer control
/// batches), plus a raw byte-range accessor for `FetchSnapshot` serving.
pub(crate) struct SnapshotReader;

impl SnapshotReader {
    /// Decode all `MetadataRecord`s from a `.checkpoint` byte stream.
    /// Control batches (header/footer) are skipped.
    pub(crate) fn read_records(bytes: &[u8]) -> Result<Vec<MetadataRecord>, RaftError> {
        let mut cursor: &[u8] = bytes;
        let mut records = Vec::new();
        while !cursor.is_empty() {
            let batch = RecordBatch::decode(&mut cursor)?;
            if batch.attributes.is_control_batch() {
                continue;
            }
            for rec in &batch.records {
                let Some(value) = rec.value.as_ref() else {
                    continue;
                };
                records.push(<SerdeCompat<MetadataRecord>>::deserialize(value)?);
            }
        }
        Ok(records)
    }

    /// Return the `[position, position + max)` slice of `bytes`, clamped
    /// to the buffer length. A `position` at or past EOF yields an empty
    /// slice. Used to serve `FetchSnapshot` byte-range requests (KIP-595
    /// §`FetchSnapshot`).
    pub(crate) fn byte_range(bytes: &[u8], position: usize, max: usize) -> &[u8] {
        let start = position.min(bytes.len());
        let end = start.saturating_add(max).min(bytes.len());
        &bytes[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    use uuid::Uuid;

    #[test]
    fn writer_reader_round_trips_image() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 2,
        }));

        let bytes = SnapshotWriter::serialize(&image, 1_700_000_000_000).unwrap();
        let records = SnapshotReader::read_records(&bytes).unwrap();
        assert!(MetadataImage::from_records(cid, &records) == image);
    }

    #[test]
    fn writer_reader_round_trips_empty_image() {
        let cid = Uuid::new_v4();
        let image = MetadataImage::new(cid);

        let bytes = SnapshotWriter::serialize(&image, 0).unwrap();
        let records = SnapshotReader::read_records(&bytes).unwrap();
        assert!(records.is_empty());
        assert!(MetadataImage::from_records(cid, &records) == image);
    }

    #[test]
    fn byte_range_returns_expected_slice() {
        let buf: Vec<u8> = (0u8..=255).collect();
        // In-range read.
        assert!(SnapshotReader::byte_range(&buf, 10, 5) == &buf[10..15]);
        // Position past EOF → empty.
        assert!(SnapshotReader::byte_range(&buf, 1000, 5).is_empty());
        // Length clamps to buffer end.
        assert!(SnapshotReader::byte_range(&buf, 250, 100) == &buf[250..]);
    }
}
