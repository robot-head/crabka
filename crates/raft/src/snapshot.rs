//! KIP-630 metadata snapshot artifact: `<offset>-<epoch>.checkpoint`.
//!
//! The format layer: image ⇄ record sequence, the `.checkpoint` filename
//! grammar, and the canonical on-disk bytes (header/data/footer Kafka
//! `RecordBatch`es). The engine ([`crate::kraft::KraftController`]) writes the
//! `.checkpoint` directly (no `.meta` sidecar) and recovers from it via
//! [`SnapshotReader::read`].

use bytes::{BufMut, Bytes, BytesMut};
use crabka_metadata::{
    MetadataImage, MetadataRecord, NodeId, Voter, VoterEndpoint, VoterSet, from_kraft_value,
    to_kraft_values, voters::KRaftVersionRange,
};
use crabka_protocol::{
    owned::{
        k_raft_version_record::KRaftVersionRecord as WireKRaftVersionRecord,
        snapshot_footer_record::SnapshotFooterRecord,
        snapshot_header_record::SnapshotHeaderRecord,
        voters_record::{
            Endpoint as WireVoterEndpoint, KRaftVersionFeature as WireKRaftVersionFeature,
            Voter as WireVoter, VotersRecord as WireVotersRecord,
        },
    },
    records::{
        Record, RecordBatch,
        metadata::control::{ControlRecord, encode_typed_control_batch},
    },
};
use uuid::Uuid;

use crate::error::RaftError;

const SNAPSHOT_HEADER_BASE_OFFSET: i64 = 0;
const SNAPSHOT_KRAFT_VERSION_BASE_OFFSET: i64 = 1;
const SNAPSHOT_VOTERS_BASE_OFFSET: i64 = 2;
const SNAPSHOT_DATA_BASE_OFFSET: i64 = 3;

/// KIP-853 control state carried at the front of every metadata snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotControlState {
    pub(crate) kraft_version: u16,
    pub(crate) voters: VoterSet,
}

impl SnapshotControlState {
    fn from_image(image: &MetadataImage) -> Self {
        Self {
            kraft_version: image.kraft_version(),
            voters: image.voters().clone(),
        }
    }
}

/// A decoded snapshot, with Raft control state kept separate from KIP-631
/// metadata records.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SnapshotContents {
    /// KIP-853 controls. Snapshots written before dynamic membership omit
    /// these batches and recover membership from the level-0 configuration.
    pub(crate) control_state: Option<SnapshotControlState>,
    pub(crate) metadata_records: Vec<MetadataRecord>,
}

/// Identifies a snapshot by the log position it covers: `end_offset` is
/// the offset of the last record contained in the snapshot, and `epoch`
/// is the leader epoch at that offset. The engine names the on-disk artifact
/// `<end_offset>-<epoch>.checkpoint` (both fields zero-padded so lexical sort
/// matches numeric sort) and parses it back directly.
///
/// Serializes a [`MetadataImage`] into the canonical KIP-630
/// `.checkpoint` byte layout: header, `KRaftVersion`, and `Voters` control
/// batches, one data batch of `MetadataRecord` values, then a footer control
/// batch — concatenated encoded Kafka `RecordBatch`es.
pub(crate) struct SnapshotWriter;

impl SnapshotWriter {
    /// Produce the full `.checkpoint` bytes for `image`.
    /// `last_contained_log_timestamp` is the create-time of the last log
    /// record folded into this snapshot (recorded in the header).
    pub(crate) fn serialize(
        image: &MetadataImage,
        last_contained_log_timestamp: i64,
    ) -> Result<Bytes, RaftError> {
        Self::serialize_with_control_state(
            image,
            last_contained_log_timestamp,
            &SnapshotControlState::from_image(image),
        )
    }

    /// Produce a checkpoint with an explicitly supplied committed KIP-853
    /// control state.
    pub(crate) fn serialize_with_control_state(
        image: &MetadataImage,
        last_contained_log_timestamp: i64,
        control_state: &SnapshotControlState,
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
        out.put_slice(&encode_typed_control_batch(
            SNAPSHOT_HEADER_BASE_OFFSET,
            &ControlRecord::SnapshotHeader(header),
        )?);

        // (2) KIP-853 control state. Kafka snapshots place the finalized
        // kraft.version before the voter set that it governs.
        let kraft_version = i16::try_from(control_state.kraft_version).map_err(|_| {
            RaftError::ChangeRejected("snapshot kraft.version exceeds int16".into())
        })?;
        out.put_slice(&encode_typed_control_batch(
            SNAPSHOT_KRAFT_VERSION_BASE_OFFSET,
            &ControlRecord::KRaftVersion(WireKRaftVersionRecord {
                version: 0,
                k_raft_version: kraft_version,
                ..Default::default()
            }),
        )?);
        out.put_slice(&encode_typed_control_batch(
            SNAPSHOT_VOTERS_BASE_OFFSET,
            &ControlRecord::Voters(voter_set_to_wire(&control_state.voters)?),
        )?);

        // (3) Data batch at base_offset 3: one record per KIP-631 value blob.
        // Each `MetadataRecord` is translated against the very image being
        // snapshotted (a whole-map V1TopicConfig diffs against its own image and
        // so emits all-sets-no-tombstones — correct for a from-scratch snapshot).
        let mut value_blobs: Vec<Bytes> = Vec::new();
        for rec in &records {
            // `V1Voters` / `V1KRaftVersion` are encoded above as KIP-853 control
            // records, never as KIP-631 metadata values.
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

        // (4) SnapshotFooter control batch (real KIP-630 `SnapshotFooterRecord`).
        let footer_base_offset = SNAPSHOT_DATA_BASE_OFFSET
            .saturating_add(i64::try_from(total_blobs).unwrap_or(i64::MAX));
        let footer = SnapshotFooterRecord::default();
        out.put_slice(&encode_typed_control_batch(
            footer_base_offset,
            &ControlRecord::SnapshotFooter(footer),
        )?);

        Ok(out.freeze())
    }
}

fn voter_set_to_wire(voters: &VoterSet) -> Result<WireVotersRecord, RaftError> {
    let voters = voters
        .iter()
        .map(|voter| {
            Ok(WireVoter {
                voter_id: i32::try_from(voter.id.0).map_err(|_| {
                    RaftError::ChangeRejected("snapshot voter id exceeds int32".into())
                })?,
                voter_directory_id: crabka_protocol::primitives::uuid::Uuid(
                    *voter.directory_id.as_bytes(),
                ),
                endpoints: voter
                    .endpoints
                    .iter()
                    .map(|endpoint| WireVoterEndpoint {
                        name: endpoint.name.clone(),
                        host: endpoint.host.clone(),
                        port: endpoint.port,
                        ..Default::default()
                    })
                    .collect(),
                k_raft_version_feature: WireKRaftVersionFeature {
                    min_supported_version: i16::try_from(voter.kraft_version.min).map_err(
                        |_| {
                            RaftError::ChangeRejected(
                                "snapshot minimum kraft.version exceeds int16".into(),
                            )
                        },
                    )?,
                    max_supported_version: i16::try_from(voter.kraft_version.max).map_err(
                        |_| {
                            RaftError::ChangeRejected(
                                "snapshot maximum kraft.version exceeds int16".into(),
                            )
                        },
                    )?,
                    ..Default::default()
                },
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>, RaftError>>()?;
    Ok(WireVotersRecord {
        version: 0,
        voters,
        ..Default::default()
    })
}

fn voter_set_from_wire(record: &WireVotersRecord) -> Result<VoterSet, RaftError> {
    let voters = record
        .voters
        .iter()
        .map(|voter| {
            let id = u64::try_from(voter.voter_id)
                .map_err(|_| RaftError::ChangeRejected("negative voter id in snapshot".into()))?;
            let min = u16::try_from(voter.k_raft_version_feature.min_supported_version).map_err(
                |_| RaftError::ChangeRejected("negative minimum kraft.version in snapshot".into()),
            )?;
            let max = u16::try_from(voter.k_raft_version_feature.max_supported_version).map_err(
                |_| RaftError::ChangeRejected("negative maximum kraft.version in snapshot".into()),
            )?;
            if min > max {
                return Err(RaftError::ChangeRejected(
                    "inverted kraft.version range in snapshot".into(),
                ));
            }
            Ok(Voter {
                id: NodeId(id),
                directory_id: Uuid::from_bytes(voter.voter_directory_id.0),
                endpoints: voter
                    .endpoints
                    .iter()
                    .map(|endpoint| VoterEndpoint {
                        name: endpoint.name.clone(),
                        host: endpoint.host.clone(),
                        port: endpoint.port,
                    })
                    .collect(),
                kraft_version: KRaftVersionRange { min, max },
            })
        })
        .collect::<Result<Vec<_>, RaftError>>()?;
    let voter_count = voters.len();
    let voters = VoterSet::from_voters(voters);
    if voters.len() != voter_count {
        return Err(RaftError::ChangeRejected(
            "duplicate voter id in snapshot".into(),
        ));
    }
    Ok(voters)
}

/// Reads a canonical `.checkpoint` byte stream back into the sequence of
/// `MetadataRecord`s it contains (skipping the header/footer control
/// batches), plus a raw byte-range accessor for `FetchSnapshot` serving.
pub(crate) struct SnapshotReader;

impl SnapshotReader {
    /// Decode a canonical checkpoint, separating KIP-853 control state from
    /// KIP-631 metadata records.
    pub(crate) fn read(bytes: &[u8]) -> Result<SnapshotContents, RaftError> {
        let mut cursor: &[u8] = bytes;
        let mut records = Vec::new();
        let mut stage = SnapshotReadStage::Header;
        let mut kraft_version = None;
        let mut voters = None;
        // A context image accumulating decoded records in log order so each
        // subsequent `from_kraft_value` resolves topic ids / whole-map config
        // merges / ACL ids against prior records. The cluster id is irrelevant
        // to translation, so a nil placeholder suffices.
        let mut ctx = MetadataImage::new(Uuid::nil());
        while !cursor.is_empty() {
            let batch = RecordBatch::decode(&mut cursor)?;
            if batch.attributes.is_control_batch() {
                for record in &batch.records {
                    let (Some(key), Some(value)) = (&record.key, &record.value) else {
                        return Err(invalid_snapshot_order());
                    };
                    match (stage, ControlRecord::decode(key, value)?) {
                        (SnapshotReadStage::Header, ControlRecord::SnapshotHeader(_)) => {
                            stage = SnapshotReadStage::KRaftVersion;
                        }
                        (SnapshotReadStage::KRaftVersion, ControlRecord::KRaftVersion(record)) => {
                            kraft_version =
                                Some(u16::try_from(record.k_raft_version).map_err(|_| {
                                    RaftError::ChangeRejected(
                                        "negative kraft.version in snapshot".into(),
                                    )
                                })?);
                            stage = SnapshotReadStage::Voters;
                        }
                        (SnapshotReadStage::Voters, ControlRecord::Voters(record)) => {
                            voters = Some(voter_set_from_wire(&record)?);
                            stage = SnapshotReadStage::MetadataOrFooter;
                        }
                        (
                            SnapshotReadStage::MetadataOrFooter
                            | SnapshotReadStage::KRaftVersion
                            | SnapshotReadStage::LegacyMetadataOrFooter,
                            ControlRecord::SnapshotFooter(_),
                        ) => {
                            stage = SnapshotReadStage::Done;
                        }
                        _ => return Err(invalid_snapshot_order()),
                    }
                }
                continue;
            }
            if stage == SnapshotReadStage::KRaftVersion {
                stage = SnapshotReadStage::LegacyMetadataOrFooter;
            }
            if !matches!(
                stage,
                SnapshotReadStage::MetadataOrFooter | SnapshotReadStage::LegacyMetadataOrFooter
            ) {
                return Err(invalid_snapshot_order());
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
        if stage != SnapshotReadStage::Done {
            return Err(invalid_snapshot_order());
        }
        Ok(SnapshotContents {
            control_state: match (kraft_version, voters) {
                (Some(kraft_version), Some(voters)) => Some(SnapshotControlState {
                    kraft_version,
                    voters,
                }),
                (None, None) => None,
                _ => return Err(invalid_snapshot_order()),
            },
            metadata_records: records,
        })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotReadStage {
    Header,
    KRaftVersion,
    Voters,
    MetadataOrFooter,
    LegacyMetadataOrFooter,
    Done,
}

fn invalid_snapshot_order() -> RaftError {
    RaftError::ChangeRejected(
        "snapshot must contain header, kraft.version, voters, metadata, footer in order".into(),
    )
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_metadata::{
        FeatureLevelRecord, LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionRecord,
        TopicRecord,
    };
    use uuid::Uuid;

    use super::*;

    fn sample_voter(id: NodeId, port: u16) -> Voter {
        Voter {
            id,
            directory_id: Uuid::from_u128(u128::from(id.0)),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port,
            }],
            kraft_version: KRaftVersionRange { min: 0, max: 1 },
        }
    }

    fn decode_single_control(batch: &RecordBatch) -> ControlRecord {
        let record = batch.records.first().expect("one control record");
        ControlRecord::decode(
            record.key.as_ref().expect("control key"),
            record.value.as_ref().expect("control value"),
        )
        .expect("decode control")
    }

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
        let records = SnapshotReader::read(&bytes).unwrap().metadata_records;
        assert2::assert!(MetadataImage::from_records(cid, &records) == image);
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
        assert2::assert!(
            decode_single_control(&header)
                == ControlRecord::SnapshotHeader(SnapshotHeaderRecord {
                    version: 0,
                    last_contained_log_timestamp: timestamp,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
                })
        );

        let kraft_version = RecordBatch::decode(&mut cur).expect("kraft.version batch");
        check!(
            (
                kraft_version.base_offset,
                kraft_version.attributes.is_control_batch(),
                kraft_version.records.len(),
            ) == (1, true, 1)
        );
        assert2::assert!(
            decode_single_control(&kraft_version)
                == ControlRecord::KRaftVersion(WireKRaftVersionRecord {
                    version: 0,
                    k_raft_version: 0,
                    ..Default::default()
                })
        );

        let voters = RecordBatch::decode(&mut cur).expect("voters batch");
        check!(
            (
                voters.base_offset,
                voters.attributes.is_control_batch(),
                voters.records.len(),
            ) == (2, true, 1)
        );
        assert2::assert!(
            decode_single_control(&voters) == ControlRecord::Voters(WireVotersRecord::default())
        );

        let data = RecordBatch::decode(&mut cur).expect("data batch");
        check!(
            (
                data.base_offset,
                data.attributes.is_control_batch(),
                data.records.len() >= 2
            ) == (3, false, true)
        );
        check!(
            data.last_offset_delta
                == i32::try_from(data.records.len() - 1).expect("record count fits")
        );
        for (i, record) in data.records.iter().enumerate() {
            assert2::assert!(record.offset_delta == i32::try_from(i).expect("index fits"));
            assert2::assert!(record.value.is_some());
        }

        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        check!(
            footer.base_offset == 3 + i64::try_from(data.records.len()).expect("record count fits")
        );
        check!((footer.attributes.is_control_batch(), footer.records.len()) == (true, 1));
        assert2::assert!(
            decode_single_control(&footer)
                == ControlRecord::SnapshotFooter(SnapshotFooterRecord::default())
        );
        check!(cur.is_empty());
    }

    #[test]
    fn writer_reader_round_trips_kip853_control_state_separately() {
        let cid = Uuid::new_v4();
        let voters = VoterSet::from_voters([
            sample_voter(NodeId(1), 9_093),
            sample_voter(NodeId(2), 9_094),
        ]);
        let mut image = MetadataImage::new(cid);
        image.apply(&MetadataRecord::V1KRaftVersion(
            crabka_metadata::KRaftVersionRecord { kraft_version: 1 },
        ));
        image.apply(&MetadataRecord::V1Voters(crabka_metadata::VotersRecord {
            voters: voters.clone(),
        }));
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 0,
            replication_factor: 1,
        }));

        let bytes = SnapshotWriter::serialize(&image, 123).expect("serialize");
        let snapshot = SnapshotReader::read(&bytes).expect("read snapshot");

        assert2::assert!(
            snapshot.control_state
                == Some(SnapshotControlState {
                    kraft_version: 1,
                    voters,
                })
        );
        assert2::assert!(snapshot.metadata_records.iter().all(|record| !matches!(
            record,
            MetadataRecord::V1KRaftVersion(_) | MetadataRecord::V1Voters(_)
        )));
        assert2::assert!(snapshot.metadata_records.len() == 1);
    }

    #[test]
    fn reader_accepts_legacy_header_data_footer_snapshot() {
        let mut image = MetadataImage::new(Uuid::new_v4());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "legacy".into(),
            topic_id: Uuid::new_v4(),
            partitions: 0,
            replication_factor: 1,
        }));
        let encoded = SnapshotWriter::serialize(&image, 0).expect("serialize current snapshot");
        let mut input = encoded.as_ref();
        let mut legacy = BytesMut::new();
        while !input.is_empty() {
            let batch = RecordBatch::decode(&mut input).expect("decode current batch");
            if !matches!(
                batch.base_offset,
                SNAPSHOT_KRAFT_VERSION_BASE_OFFSET | SNAPSHOT_VOTERS_BASE_OFFSET
            ) {
                batch.encode(&mut legacy).expect("encode legacy batch");
            }
        }

        let snapshot = SnapshotReader::read(&legacy).expect("read legacy snapshot");
        assert2::assert!(snapshot.control_state.is_none());
        assert2::assert!(snapshot.metadata_records.len() == 1);
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
        assert2::assert!(image.finalized_features_epoch() == 3);

        let bytes = SnapshotWriter::serialize(&image, 1_700_000_000_000).unwrap();
        let records = SnapshotReader::read(&bytes).unwrap().metadata_records;
        let rebuilt = MetadataImage::from_records(cid, &records);
        assert2::assert!(rebuilt == image);
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
        let records = SnapshotReader::read(&bytes).unwrap().metadata_records;
        assert2::assert!(records.is_empty());
        assert2::assert!(MetadataImage::from_records(cid, &records) == image);
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
        assert2::assert!(
            decode_single_control(&header)
                == ControlRecord::SnapshotHeader(SnapshotHeaderRecord {
                    version: 0,
                    last_contained_log_timestamp: 99,
                    ..Default::default()
                })
        );

        let kraft_version = RecordBatch::decode(&mut cur).expect("kraft.version batch");
        check!(
            (
                kraft_version.base_offset,
                kraft_version.attributes.is_control_batch()
            ) == (1, true)
        );
        let voters = RecordBatch::decode(&mut cur).expect("voters batch");
        check!((voters.base_offset, voters.attributes.is_control_batch()) == (2, true));

        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        check!(
            (
                footer.base_offset,
                footer.attributes.is_control_batch(),
                footer.records.len()
            ) == (3, true, 1)
        );
        assert2::assert!(
            decode_single_control(&footer)
                == ControlRecord::SnapshotFooter(SnapshotFooterRecord::default())
        );
        check!(cur.is_empty());
    }

    #[test]
    fn reader_rejects_out_of_order_kip853_controls() {
        let mut bytes = BytesMut::new();
        bytes.put_slice(
            &encode_typed_control_batch(
                0,
                &ControlRecord::SnapshotHeader(SnapshotHeaderRecord::default()),
            )
            .expect("header"),
        );
        bytes.put_slice(
            &encode_typed_control_batch(1, &ControlRecord::Voters(WireVotersRecord::default()))
                .expect("voters"),
        );
        bytes.put_slice(
            &encode_typed_control_batch(
                2,
                &ControlRecord::KRaftVersion(WireKRaftVersionRecord::default()),
            )
            .expect("kraft.version"),
        );
        bytes.put_slice(
            &encode_typed_control_batch(
                3,
                &ControlRecord::SnapshotFooter(SnapshotFooterRecord::default()),
            )
            .expect("footer"),
        );

        assert2::assert!(SnapshotReader::read(&bytes).is_err());
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
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
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
        assert2::assert!(out.status.success());
        // The JVM decoder names each record by its KIP-631 type. Their presence
        // (and a clean exit) proves the translated bytes decode as real records.
        // The JVM decoder prints each record's KIP-631 type in SCREAMING_SNAKE.
        for needle in [
            "REGISTER_BROKER_RECORD",
            "TOPIC_RECORD",
            "PARTITION_RECORD",
            "CONFIG_RECORD",
        ] {
            assert2::assert!(text.contains(needle));
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
        type TestCase1<'a> = (&'a str, usize, usize, &'a [u8]);
        let buf: Vec<u8> = (0u8..=255).collect();
        let cases: [TestCase1<'_>; 3] = [
            // In-range read.
            ("in-range read", 10, 5, &buf[10..15]),
            // Position past EOF → empty.
            ("position past EOF", 1000, 5, &[]),
            // Length clamps to buffer end.
            ("length clamped to end", 250, 100, &buf[250..]),
        ];
        for (_case, position, max, want) in cases {
            assert2::assert!(SnapshotReader::byte_range(&buf, position, max) == want);
        }
    }
}
