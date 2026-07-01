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
            .is_some_and(|n| is_controller_leader(Some(n), node_id));
        if !is_leader {
            return encode_resp(version, &not_controller_response());
        }

        let Ok(broker_slot_id) = u64::try_from(req.broker_id) else {
            return encode_resp(version, &AssignReplicasToDirsResponse::default());
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

fn is_controller_leader(leader: Option<u64>, node_id: u64) -> bool {
    leader == Some(node_id)
}

fn not_controller_response() -> AssignReplicasToDirsResponse {
    AssignReplicasToDirsResponse {
        error_code: codes::NOT_CONTROLLER,
        ..Default::default()
    }
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

    use crate::broker::Broker;
    use crate::config::BrokerConfig;

    const VERSION: i16 = 0;

    fn request(dir_uuid: uuid::Uuid, topic_uuid: uuid::Uuid, partition_index: i32) -> Bytes {
        let req = AssignReplicasToDirsRequest {
            broker_id: 1,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(dir_uuid.into_bytes()),
                topics: vec![ReqTopicData {
                    topic_id: ProtocolUuid(topic_uuid.into_bytes()),
                    partitions: vec![ReqPartData {
                        partition_index,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
        req.encode(&mut buf, VERSION)
            .expect("encode AssignReplicasToDirsRequest");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes) -> AssignReplicasToDirsResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = AssignReplicasToDirsResponse::decode(&mut cur, VERSION)
            .expect("decode AssignReplicasToDirsResponse");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    async fn wait_for_leader(broker: &Broker) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if broker
                .controller
                .watch_leader()
                .borrow()
                .is_some_and(|n| n == broker.config.node_id)
            {
                return;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "broker did not become controller leader"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[test]
    fn leader_predicate_matches_current_node_only() {
        assert!(is_controller_leader(Some(1), 1));
        assert!(!is_controller_leader(Some(2), 1));
        assert!(!is_controller_leader(None, 1));
    }

    #[test]
    fn not_controller_response_preserves_error_code() {
        let resp = not_controller_response();
        assert!(resp.error_code == codes::NOT_CONTROLLER, "{resp:?}");
        assert!(resp.directories.is_empty(), "{resp:?}");
    }

    #[test]
    fn encode_resp_preserves_encoded_body() {
        let req = AssignReplicasToDirsRequest {
            broker_id: 1,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(uuid::Uuid::from_u128(0xAA).into_bytes()),
                topics: vec![ReqTopicData {
                    topic_id: ProtocolUuid(uuid::Uuid::from_u128(0xBB).into_bytes()),
                    partitions: vec![ReqPartData {
                        partition_index: 3,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = build_echo_response(&req);

        let bytes = encode_resp(VERSION, &resp).expect("encode response");
        let decoded = decode_response(&bytes);

        assert!(decoded.error_code == codes::NONE, "{decoded:?}");
        assert!(decoded.directories.len() == 1, "{decoded:?}");
        assert!(decoded.directories[0].topics.len() == 1, "{decoded:?}");
        let partition = &decoded.directories[0].topics[0].partitions[0];
        assert!(partition.partition_index == 3, "{decoded:?}");
        assert!(partition.error_code == codes::NONE, "{decoded:?}");
    }

    #[tokio::test]
    async fn handle_leader_echoes_request_shape() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let dir_uuid = uuid::Uuid::from_u128(0xAA);
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        let req = request(dir_uuid, topic_uuid, 7);

        let bytes = handle(&broker, VERSION, 9, &req)
            .await
            .expect("AssignReplicasToDirs handler");
        let resp = decode_response(&bytes);

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert!(resp.directories.len() == 1, "{resp:?}");
        let dir = &resp.directories[0];
        assert!(dir.id.0 == dir_uuid.into_bytes(), "{resp:?}");
        assert!(dir.topics.len() == 1, "{resp:?}");
        let topic = &dir.topics[0];
        assert!(topic.topic_id.0 == topic_uuid.into_bytes(), "{resp:?}");
        assert!(topic.partitions.len() == 1, "{resp:?}");
        let partition = &topic.partitions[0];
        assert!(partition.partition_index == 7, "{resp:?}");
        assert!(partition.error_code == codes::NONE, "{resp:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_leader_commits_known_directory_assignment() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let dir_uuid = uuid::Uuid::from_u128(0xAA);
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        broker
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: topic_uuid,
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "t".into(),
                    partition: 0,
                    leader: 1,
                    replicas: vec![1],
                    isr: vec![1],
                    leader_epoch: 0,
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![uuid::Uuid::nil()],
                    partition_epoch: 0,
                }),
            ])
            .await
            .expect("seed partition");
        let req = request(dir_uuid, topic_uuid, 0);

        let bytes = handle(&broker, VERSION, 9, &req)
            .await
            .expect("AssignReplicasToDirs handler");
        let resp = decode_response(&bytes);

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert!(resp.directories[0].topics[0].partitions[0].error_code == codes::NONE);
        let image = broker.controller.current_image();
        let partition = image.partition("t", 0).expect("partition");
        assert!(partition.directories == vec![dir_uuid]);
        broker_handle.shutdown().await;
    }

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
            partition_epoch: 0,
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
            partition_epoch: 0,
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
            partition_epoch: 0,
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
            partition_epoch: 0,
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
