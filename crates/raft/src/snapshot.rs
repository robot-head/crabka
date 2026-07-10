//! KIP-630 metadata snapshot artifact: `<offset>-<epoch>.checkpoint`.
//!
//! The format layer: image ⇄ record sequence, the `.checkpoint` filename
//! grammar, and the canonical on-disk bytes (header/data/footer Kafka
//! `RecordBatch`es). The engine ([`crate::kraft::KraftController`]) writes the
//! `.checkpoint` directly (no `.meta` sidecar) and recovers from it via
//! [`SnapshotReader::read_records`].

use bytes::{BufMut, Bytes, BytesMut};
use crabka_metadata::{MetadataImage, MetadataRecord, from_kraft_value, to_kraft_values};
use crabka_protocol::{
    Encode,
    owned::{
        snapshot_footer_record::SnapshotFooterRecord, snapshot_header_record::SnapshotHeaderRecord,
    },
    records::{
        Record, RecordBatch,
        metadata::control::{ControlRecordType, control_record_key, encode_control_batch},
    },
};
use uuid::Uuid;

use crate::error::RaftError;

const SNAPSHOT_HEADER_BASE_OFFSET: i64 = 0;
const SNAPSHOT_DATA_BASE_OFFSET: i64 = 1;
/// Message version of the KIP-630 `SnapshotHeaderRecord` / `SnapshotFooterRecord`
/// bodies written into the checkpoint's control batches (Kafka writes v0).
const SNAPSHOT_CONTROL_RECORD_VERSION: i16 = 0;

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

        // (1) SnapshotHeader control batch at base_offset 0 — the real KIP-630
        // `SnapshotHeaderRecord` (flexible message), encoded via the protocol
        // control-batch builder so the JVM `kafka-dump-log` decoder parses it.
        let header = SnapshotHeaderRecord {
            last_contained_log_timestamp,
            ..Default::default()
        };
        out.put_slice(&snapshot_control_batch(
            SNAPSHOT_HEADER_BASE_OFFSET,
            ControlRecordType::SnapshotHeader,
            &header,
        )?);

        // (2) Data batch at base_offset 1: one record per KIP-631 value blob.
        // Each `MetadataRecord` is translated against the very image being
        // snapshotted (a whole-map V1TopicConfig diffs against its own image and
        // so emits all-sets-no-tombstones — correct for a from-scratch snapshot).
        let mut value_blobs: Vec<Bytes> = Vec::new();
        for rec in &records {
            // `V1Voters` / `V1KRaftVersion` are raft-control state living in the
            // controller's `QuorumState` (config-seeded under KIP-595 static
            // voters), NOT KIP-631 metadata-log records — they have no wire
            // counterpart and are not JVM-readable as snapshot records. A
            // follower catching up via `FetchSnapshot` re-applies the live voter
            // set after install (see `install_fetched_snapshot`), and a
            // restarting controller re-derives it on boot (see `spawn_with_image`),
            // so omitting them here keeps the snapshot a faithful, JVM-readable
            // image of the KIP-631 metadata log.
            if matches!(
                rec,
                MetadataRecord::V1Voters(_) | MetadataRecord::V1KRaftVersion(_)
            ) {
                continue;
            }
            let mut blobs = to_kraft_values(rec, image)
                .map_err(|e| RaftError::ChangeRejected(format!("snapshot encode: {e}")))?;
            value_blobs.append(&mut blobs);
        }
        let total_blobs = value_blobs.len();
        if !value_blobs.is_empty() {
            let last_offset_delta = total_blobs
                .checked_sub(1)
                .and_then(|delta| i32::try_from(delta).ok())
                .unwrap_or(i32::MAX);
            let data_records = value_blobs
                .into_iter()
                .enumerate()
                .map(|(i, blob)| {
                    let mut record = Record {
                        value: Some(blob),
                        ..Default::default()
                    };
                    record.offset_delta = i32::try_from(i).unwrap_or(i32::MAX);
                    record
                })
                .collect();
            let mut data_batch = RecordBatch {
                records: data_records,
                ..Default::default()
            };
            data_batch.base_offset = SNAPSHOT_DATA_BASE_OFFSET;
            data_batch.last_offset_delta = last_offset_delta;
            data_batch.encode(&mut out)?;
        }

        // (3) SnapshotFooter control batch (real KIP-630 `SnapshotFooterRecord`).
        let footer_base_offset = SNAPSHOT_DATA_BASE_OFFSET
            .saturating_add(i64::try_from(total_blobs).unwrap_or(i64::MAX));
        let footer = SnapshotFooterRecord::default();
        out.put_slice(&snapshot_control_batch(
            footer_base_offset,
            ControlRecordType::SnapshotFooter,
            &footer,
        )?);

        Ok(out.freeze())
    }
}

fn snapshot_control_batch<R: Encode>(
    base_offset: i64,
    record_type: ControlRecordType,
    record: &R,
) -> Result<Bytes, RaftError> {
    let mut body = BytesMut::new();
    record.encode(&mut body, SNAPSHOT_CONTROL_RECORD_VERSION)?;
    Ok(encode_control_batch(
        base_offset,
        control_record_key(record_type),
        body.freeze(),
    ))
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
        // A context image accumulating decoded records in log order so each
        // subsequent `from_kraft_value` resolves topic ids / whole-map config
        // merges / ACL ids against prior records. The cluster id is irrelevant
        // to translation, so a nil placeholder suffices.
        let mut ctx = MetadataImage::new(Uuid::nil());
        while !cursor.is_empty() {
            let batch = RecordBatch::decode(&mut cursor)?;
            if batch.attributes.is_control_batch() {
                continue;
            }
            for rec in &batch.records {
                let Some(value) = rec.value.as_ref() else {
                    continue;
                };
                let decoded = from_kraft_value(value, &ctx)
                    .map_err(|e| RaftError::ChangeRejected(format!("snapshot decode: {e}")))?;
                ctx.apply(&decoded);
                records.push(decoded);
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
    use assert2::{assert, check};
    use crabka_metadata::{
        FeatureLevelRecord, LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionRecord,
        TopicRecord,
    };
    use crabka_protocol::Decode;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn writer_reader_round_trips_image() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        // A realistic topic: the `V1Topic` plus its partition records. KIP-631
        // framing carries no partition count on the `TopicRecord`, so the round
        // trip derives partitions/RF from the partition records — a bare
        // `V1Topic` (declaring partitions but with no `V1Partition`s) would not
        // round-trip its declared count.
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 2,
        }));
        for p in 0..3 {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "orders".into(),
                partition: p,
                leader: NodeId(1),
                replicas: vec![NodeId(1), NodeId(2)],
                isr: vec![NodeId(1), NodeId(2)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }

        let bytes = SnapshotWriter::serialize(&image, 1_700_000_000_000).unwrap();
        let records = SnapshotReader::read_records(&bytes).unwrap();
        assert!(MetadataImage::from_records(cid, &records) == image);
    }

    #[test]
    fn writer_emits_canonical_header_data_offsets_and_footer() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 2,
            replication_factor: 1,
        }));
        for p in 0..2 {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "orders".into(),
                partition: p,
                leader: NodeId(1),
                replicas: vec![NodeId(1)],
                isr: vec![NodeId(1)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }

        let timestamp = 1_700_000_000_123;
        let bytes = SnapshotWriter::serialize(&image, timestamp).unwrap();
        let mut cur: &[u8] = &bytes;

        let header = RecordBatch::decode(&mut cur).expect("header batch");
        check!(
            (
                header.base_offset,
                header.attributes.is_control_batch(),
                header.records.len()
            ) == (0, true, 1)
        );
        let header_value = header.records[0].value.as_ref().expect("header value");
        let mut header_cur = &header_value[..];
        let header_record =
            SnapshotHeaderRecord::decode(&mut header_cur, 0).expect("snapshot header");
        let expected_header = SnapshotHeaderRecord {
            version: 0,
            last_contained_log_timestamp: timestamp,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(header_record == expected_header);
        assert!(header_cur.is_empty());

        let data = RecordBatch::decode(&mut cur).expect("data batch");
        check!(
            (
                data.base_offset,
                data.attributes.is_control_batch(),
                data.records.len() >= 2
            ) == (1, false, true)
        );
        check!(
            data.last_offset_delta
                == i32::try_from(data.records.len() - 1).expect("record count fits")
        );
        for (i, record) in data.records.iter().enumerate() {
            assert_eq!(
                (record.offset_delta, record.value.is_some()),
                (i32::try_from(i).expect("index fits"), true),
                "record {i}"
            );
        }

        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        check!(
            footer.base_offset == 1 + i64::try_from(data.records.len()).expect("record count fits")
        );
        check!((footer.attributes.is_control_batch(), footer.records.len()) == (true, 1));
        let footer_value = footer.records[0].value.as_ref().expect("footer value");
        let mut footer_cur = &footer_value[..];
        let footer_record =
            SnapshotFooterRecord::decode(&mut footer_cur, 0).expect("snapshot footer");
        let expected_footer = SnapshotFooterRecord {
            version: 0,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(footer_record == expected_footer);
        check!(footer_cur.is_empty());
        check!(cur.is_empty());
    }

    /// A snapshot of an image carrying finalized KIP-584 features must
    /// reproduce both the feature levels AND the finalized-features epoch
    /// exactly on read-back. Regression guard for the bug where `to_records`
    /// emitted no `V1FeatureLevel` records: `metadata.version` (range guard /
    /// SCRAM + delegation-token gates) and `group.version` (next-gen consumer
    /// groups) silently vanished after any compaction or learner snapshot
    /// install. The epoch (3 here, from a re-finalize) exceeds the live feature
    /// count (2), so a naive "replay one record per feature" fix would
    /// reconstruct epoch=1 and fail this assertion.
    #[test]
    fn writer_reader_round_trips_image_with_features() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        for (name, level) in [
            ("metadata.version", 24),
            ("metadata.version", 25),
            ("group.version", 1),
            ("metadata.version", 25),
        ] {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: name.into(),
                level,
            }));
        }
        assert!(image.finalized_features_epoch() == 3);

        let bytes = SnapshotWriter::serialize(&image, 1_700_000_000_000).unwrap();
        let records = SnapshotReader::read_records(&bytes).unwrap();
        let rebuilt = MetadataImage::from_records(cid, &records);
        assert!(rebuilt == image);
        check!(
            (
                rebuilt.finalized_features().get("metadata.version"),
                rebuilt.finalized_features().get("group.version"),
                rebuilt.finalized_features_epoch(),
            ) == (Some(&25), Some(&1), 3)
        );
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
    fn writer_emits_empty_snapshot_header_and_footer_offsets() {
        let image = MetadataImage::new(Uuid::new_v4());

        let bytes = SnapshotWriter::serialize(&image, 99).unwrap();
        let mut cur: &[u8] = &bytes;

        let header = RecordBatch::decode(&mut cur).expect("header batch");
        check!(
            (
                header.base_offset,
                header.attributes.is_control_batch(),
                header.records.len()
            ) == (0, true, 1)
        );
        let header_value = header.records[0].value.as_ref().expect("header value");
        let mut header_cur = &header_value[..];
        let header_record =
            SnapshotHeaderRecord::decode(&mut header_cur, 0).expect("snapshot header");
        let expected_header = SnapshotHeaderRecord {
            version: 0,
            last_contained_log_timestamp: 99,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(header_record == expected_header);
        assert!(header_cur.is_empty());

        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        check!(
            (
                footer.base_offset,
                footer.attributes.is_control_batch(),
                footer.records.len()
            ) == (1, true, 1)
        );
        let footer_value = footer.records[0].value.as_ref().expect("footer value");
        let mut footer_cur = &footer_value[..];
        let footer_record =
            SnapshotFooterRecord::decode(&mut footer_cur, 0).expect("snapshot footer");
        let expected_footer = SnapshotFooterRecord {
            version: 0,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(footer_record == expected_footer);
        check!(footer_cur.is_empty());
        check!(cur.is_empty());
    }

    /// Docker-gated: a Crabka engine-produced KIP-630 snapshot (built by
    /// `SnapshotWriter` from a real `MetadataImage` through the KIP-631
    /// translation boundary) is parsed cleanly by the JVM
    /// `kafka-dump-log --cluster-metadata-decoder`, proving the on-checkpoint
    /// bytes are genuine KIP-631 records (`RegisterBroker` / `Topic` /
    /// `Partition` / `Config`), not Crabka-private wincode.
    ///
    /// ```text
    /// cargo test -p crabka-raft --lib snapshot -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "requires Docker"]
    fn jvm_dump_log_parses_engine_snapshot() {
        use std::{io::Write as _, process::Command};

        use crabka_metadata::{BrokerConfigRecord, BrokerRegistrationRecord, TopicConfigRecord};

        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        // RegisterBroker (apiKey 0).
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(1),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10),
                host: "broker-1".into(),
                port: 9092,
                rack: Some("rack-a".into()),
                endpoints: vec![],
            },
        ));
        // Config (apiKey 4), broker scope.
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(1),
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("1048576".into()),
        }));
        // Topic (apiKey 2) + Partition (apiKey 3) ×2.
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 2,
            replication_factor: 1,
        }));
        for p in 0..2 {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "orders".into(),
                partition: p,
                leader: NodeId(1),
                replicas: vec![NodeId(1)],
                isr: vec![NodeId(1)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }
        // Config (apiKey 4), topic scope.
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: [("retention.ms".to_string(), "604800000".to_string())].into(),
        }));

        let bytes = SnapshotWriter::serialize(&image, 1_700_000_000_000).unwrap();
        let dir = tempfile::tempdir().expect("tempdir");
        // kafka-dump-log infers the snapshot base offset from the file name.
        let path = dir
            .path()
            .join("00000000000000000000-0000000000.checkpoint");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();

        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-v",
                &format!("{}:/work", dir.path().display()),
                "mirror.gcr.io/apache/kafka:4.0.0",
                "/opt/kafka/bin/kafka-dump-log.sh",
                "--cluster-metadata-decoder",
                "--files",
                "/work/00000000000000000000-0000000000.checkpoint",
            ])
            .output()
            .expect("docker run kafka-dump-log");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        eprintln!("{text}");
        assert!(out.status.success(), "kafka-dump-log failed: {text}");
        // The JVM decoder names each record by its KIP-631 type. Their presence
        // (and a clean exit) proves the translated bytes decode as real records.
        // The JVM decoder prints each record's KIP-631 type in SCREAMING_SNAKE.
        for needle in [
            "REGISTER_BROKER_RECORD",
            "TOPIC_RECORD",
            "PARTITION_RECORD",
            "CONFIG_RECORD",
        ] {
            assert!(text.contains(needle), "missing {needle} in dump: {text}");
        }
        // Three text-shape checks over the dump output:
        // 1. No record may fail the decoder's CRC / schema check.
        // 2. All RegisterBroker records must have a non-nil incarnationId
        //    (kafka-dump-log prints lines like `RegisterBrokerRecord(brokerId=1,
        //    incarnationId=00000000-0000-0000-0000-000000000000, ...)` where a
        //    nil UUID is all-zeros).
        // 3. All Partition records must have partitionEpoch >= 0 after Slice 6
        //    (not -1, the schema default).
        check!(
            !text.contains("isvalid: false") && !text.to_lowercase().contains("could not"),
            "dump-log record-validity check failed: {text}"
        );
        check!(
            !text.contains("incarnationId=00000000-0000-0000-0000-000000000000"),
            "dump-log incarnationId check failed: {text}"
        );
        check!(
            !text
                .lines()
                .any(|l| l.contains("PartitionRecord") && l.contains("partitionEpoch=-1")),
            "dump-log partitionEpoch check failed: {text}"
        );
    }

    #[test]
    fn byte_range_returns_expected_slice() {
        let buf: Vec<u8> = (0u8..=255).collect();
        let cases: [(&str, usize, usize, &[u8]); 3] = [
            // In-range read.
            ("in-range read", 10, 5, &buf[10..15]),
            // Position past EOF → empty.
            ("position past EOF", 1000, 5, &[]),
            // Length clamps to buffer end.
            ("length clamped to end", 250, 100, &buf[250..]),
        ];
        for (case, position, max, want) in cases {
            assert!(
                SnapshotReader::byte_range(&buf, position, max) == want,
                "case {case}"
            );
        }
    }
}
