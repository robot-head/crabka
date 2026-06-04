//! `AssignReplicasToDirs` (`api_key=73`, KIP-858). A broker reports, for each
//! of its replicas, which log-directory UUID currently hosts it. The
//! controller records this in `PartitionRecord.directories[broker_slot]` so a
//! later `offline_log_dirs` heartbeat can be mapped back to exactly the
//! affected partitions for failover.
//!
//! Leader-only (`NOT_CONTROLLER` otherwise), mirroring `alter_partition`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataImage, MetadataRecord, PartitionDirAssignmentRecord};
use crabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest;
use crabka_protocol::owned::assign_replicas_to_dirs_response::{
    AssignReplicasToDirsResponse, DirectoryData as RespDirData, PartitionData as RespPartData,
    TopicData as RespTopicData,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AssignReplicasToDirsRequest::decode(&mut cur, version)?;

        let is_leader = controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == node_id);
        if !is_leader {
            return encode_resp(
                version,
                &AssignReplicasToDirsResponse {
                    error_code: codes::NOT_CONTROLLER,
                    ..Default::default()
                },
            );
        }

        let Ok(broker_slot_id) = u64::try_from(req.broker_id) else {
            return encode_resp(
                version,
                &AssignReplicasToDirsResponse {
                    error_code: codes::NONE,
                    ..Default::default()
                },
            );
        };
        let image = controller.current_image();
        let changes = collect_assignment_changes(&image, broker_slot_id, &req);

        if !changes.is_empty()
            && let Err(e) = controller.submit_change(changes).await
        {
            return Err(BrokerError::Replication(format!("submit_change: {e}")));
        }

        encode_resp(version, &build_echo_response(&req))
    })
}

/// Collect all `MetadataRecord` changes from the directories/topics/partitions
/// in `req`, calling `assignment_changes` for each partition entry. Pure; no
/// I/O.
pub(crate) fn collect_assignment_changes(
    image: &MetadataImage,
    broker_id: u64,
    req: &AssignReplicasToDirsRequest,
) -> Vec<MetadataRecord> {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    for dir in &req.directories {
        let dir_uuid = uuid::Uuid::from_bytes(dir.id.0);
        for t in &dir.topics {
            let topic_uuid = uuid::Uuid::from_bytes(t.topic_id.0);
            for p in &t.partitions {
                changes.extend(assignment_changes(
                    image,
                    broker_id,
                    topic_uuid,
                    p.partition_index,
                    dir_uuid,
                ));
            }
        }
    }
    changes
}

/// Build the success-path echo response from `req`: mirrors the request
/// directory/topic/partition structure, filling every partition's
/// `error_code` with `NONE`. Pure; no I/O.
pub(crate) fn build_echo_response(
    req: &AssignReplicasToDirsRequest,
) -> AssignReplicasToDirsResponse {
    let directories = req
        .directories
        .iter()
        .map(|dir| RespDirData {
            id: dir.id,
            topics: dir
                .topics
                .iter()
                .map(|t| RespTopicData {
                    topic_id: t.topic_id,
                    partitions: t
                        .partitions
                        .iter()
                        .map(|p| RespPartData {
                            partition_index: p.partition_index,
                            error_code: codes::NONE,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    AssignReplicasToDirsResponse {
        error_code: codes::NONE,
        directories,
        ..Default::default()
    }
}

/// Pure: compute the (0 or 1) directory-assignment delta that records
/// `broker_id`'s replica of `(topic_id, partition)` living on `dir_uuid`.
/// Empty when the topic/partition is unknown, the broker isn't a replica,
/// or the slot already holds `dir_uuid` (idempotent — avoids churn).
///
/// Emits a [`MetadataRecord::V1PartitionDirAssignment`] DELTA rather than a
/// full `V1Partition`: on apply it merges ONLY the one replica's slot in
/// `directories`, never touching leader/isr/replicas/adding/removing. A full
/// read-modify-write here, built from a slightly-stale image read, would race
/// a concurrent `AlterPartitionReassignments` and revert `adding_replicas`;
/// the delta is order-independent (KIP-858).
fn assignment_changes(
    image: &MetadataImage,
    broker_id: u64,
    topic_id: uuid::Uuid,
    partition: i32,
    dir_uuid: uuid::Uuid,
) -> Vec<MetadataRecord> {
    let Some(topic_name) = image
        .topics()
        .find(|tr| tr.topic_id == topic_id)
        .map(|tr| tr.name.clone())
    else {
        return Vec::new();
    };
    let Some(pr) = image.partition(&topic_name, partition) else {
        return Vec::new();
    };
    let Some(slot) = pr.replicas.iter().position(|n| *n == broker_id) else {
        return Vec::new();
    };
    // Idempotent: skip if the slot already holds this dir (avoids churn).
    if pr.directories.get(slot) == Some(&dir_uuid) {
        return Vec::new();
    }
    vec![MetadataRecord::V1PartitionDirAssignment(
        PartitionDirAssignmentRecord {
            topic: topic_name,
            partition,
            replica: broker_id,
            directory: dir_uuid,
        },
    )]
}

fn encode_resp(version: i16, resp: &AssignReplicasToDirsResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
    use crabka_protocol::owned::assign_replicas_to_dirs_request::{
        DirectoryData as ReqDirData, PartitionData as ReqPartData, TopicData as ReqTopicData,
    };
    use crabka_protocol::primitives::uuid::Uuid as ProtocolUuid;

    #[test]
    fn sets_reporting_brokers_directory_slot() {
        let topic_id = uuid::Uuid::from_u128(0x7);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), uuid::Uuid::nil()],
        }));
        let dir = uuid::Uuid::from_u128(0xAA);
        let changes = assignment_changes(&image, 2, topic_id, 0, dir);
        let MetadataRecord::V1PartitionDirAssignment(r) = &changes[0] else {
            panic!("expected V1PartitionDirAssignment")
        };
        assert!(r.topic == "t");
        assert!(r.partition == 0);
        assert!(r.replica == 2);
        assert!(r.directory == dir);
    }

    #[test]
    fn idempotent_when_slot_already_set() {
        let topic_id = uuid::Uuid::from_u128(0x7);
        let dir = uuid::Uuid::from_u128(0xAA);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), dir],
        }));
        assert!(assignment_changes(&image, 2, topic_id, 0, dir).is_empty());
    }

    #[test]
    fn empty_when_broker_not_a_replica() {
        let topic_id = uuid::Uuid::from_u128(0x7);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), uuid::Uuid::nil()],
        }));
        assert!(
            assignment_changes(&image, 99, topic_id, 0, uuid::Uuid::from_u128(0xAA)).is_empty()
        );
    }

    // ── collect_assignment_changes ────────────────────────────────────────────

    /// Build a minimal image with one topic + partition where broker 2 is a
    /// replica. Return the topic UUID so callers can put it in the request.
    fn make_image_with_broker2_replica() -> (MetadataImage, uuid::Uuid) {
        let topic_id = uuid::Uuid::from_u128(0x42);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), uuid::Uuid::nil()],
        }));
        (image, topic_id)
    }

    #[test]
    fn collect_assignment_changes_produces_one_change_for_known_partition() {
        let (image, topic_id) = make_image_with_broker2_replica();
        let dir_uuid = uuid::Uuid::from_u128(0xAA);

        // Build a request where broker 2 reports partition 0 on dir 0xAA.
        let req = AssignReplicasToDirsRequest {
            broker_id: 2,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(dir_uuid.into_bytes()),
                topics: vec![ReqTopicData {
                    topic_id: ProtocolUuid(topic_id.into_bytes()),
                    partitions: vec![ReqPartData {
                        partition_index: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let changes = collect_assignment_changes(&image, 2, &req);
        assert!(
            changes.len() == 1,
            "expected one change, got {}",
            changes.len()
        );
        let MetadataRecord::V1PartitionDirAssignment(r) = &changes[0] else {
            panic!("expected V1PartitionDirAssignment");
        };
        // The delta names broker 2's replica of (t, 0) on dir_uuid; on apply it
        // merges only slot 1, leaving slot 0 (broker 1) untouched.
        assert!(r.topic == "t");
        assert!(r.partition == 0);
        assert!(r.replica == 2);
        assert!(r.directory == dir_uuid);
    }

    #[test]
    fn collect_assignment_changes_empty_for_unknown_partition() {
        let (image, topic_id) = make_image_with_broker2_replica();
        let dir_uuid = uuid::Uuid::from_u128(0xAA);

        // Request a partition index that doesn't exist (partition 99).
        let req = AssignReplicasToDirsRequest {
            broker_id: 2,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(dir_uuid.into_bytes()),
                topics: vec![ReqTopicData {
                    topic_id: ProtocolUuid(topic_id.into_bytes()),
                    partitions: vec![ReqPartData {
                        partition_index: 99,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let changes = collect_assignment_changes(&image, 2, &req);
        assert!(
            changes.is_empty(),
            "unknown partition must yield no changes"
        );
    }

    // ── build_echo_response ───────────────────────────────────────────────────

    #[test]
    fn build_echo_response_mirrors_request_structure_with_none_error_codes() {
        let dir_id_bytes = uuid::Uuid::from_u128(0xBB).into_bytes();
        let topic_id_bytes = uuid::Uuid::from_u128(0x5).into_bytes();

        let req = AssignReplicasToDirsRequest {
            broker_id: 1,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(dir_id_bytes),
                topics: vec![ReqTopicData {
                    topic_id: ProtocolUuid(topic_id_bytes),
                    partitions: vec![
                        ReqPartData {
                            partition_index: 0,
                            ..Default::default()
                        },
                        ReqPartData {
                            partition_index: 1,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let resp = build_echo_response(&req);

        assert!(
            resp.error_code == 0,
            "top-level error_code must be NONE (0)"
        );
        assert!(resp.directories.len() == 1, "must echo one directory");

        let dir = &resp.directories[0];
        assert!(dir.id.0 == dir_id_bytes, "directory id must be echoed");
        assert!(dir.topics.len() == 1, "must echo one topic");

        let topic = &dir.topics[0];
        assert!(
            topic.topic_id.0 == topic_id_bytes,
            "topic id must be echoed"
        );
        assert!(topic.partitions.len() == 2, "must echo both partitions");

        for (i, p) in topic.partitions.iter().enumerate() {
            assert!(
                p.error_code == 0,
                "partition {i} error_code must be NONE (0), got {}",
                p.error_code
            );
        }
        assert!(topic.partitions[0].partition_index == 0);
        assert!(topic.partitions[1].partition_index == 1);
    }
}
