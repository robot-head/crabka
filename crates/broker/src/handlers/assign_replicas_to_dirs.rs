//! `AssignReplicasToDirs` (`api_key=73`, KIP-858). A broker reports, for each
//! of its replicas, which log-directory UUID currently hosts it. The
//! controller records this in `PartitionRecord.directories[broker_slot]` so a
//! later `offline_log_dirs` heartbeat can be mapped back to exactly the
//! affected partitions for failover.
//!
//! Leader-only (`NOT_CONTROLLER` otherwise), mirroring `alter_partition`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
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
        let mut changes: Vec<MetadataRecord> = Vec::new();

        for dir in &req.directories {
            let dir_uuid = uuid::Uuid::from_bytes(dir.id.0);
            for t in &dir.topics {
                let topic_uuid = uuid::Uuid::from_bytes(t.topic_id.0);
                for p in &t.partitions {
                    changes.extend(assignment_changes(
                        &image,
                        broker_slot_id,
                        topic_uuid,
                        p.partition_index,
                        dir_uuid,
                    ));
                }
            }
        }

        if !changes.is_empty()
            && let Err(e) = controller.submit_change(changes).await
        {
            return Err(BrokerError::Replication(format!("submit_change: {e}")));
        }

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
        encode_resp(
            version,
            &AssignReplicasToDirsResponse {
                error_code: codes::NONE,
                directories,
                ..Default::default()
            },
        )
    })
}

/// Pure: compute the (0 or 1) `PartitionRecord` change that records
/// `broker_id`'s replica of `(topic_id, partition)` living on `dir_uuid`.
/// Empty when the topic/partition is unknown, the broker isn't a replica,
/// or the slot already holds `dir_uuid` (idempotent — avoids churn).
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
    let mut directories = pr.directories.clone();
    if directories.len() < pr.replicas.len() {
        directories.resize(pr.replicas.len(), uuid::Uuid::nil());
    }
    if directories[slot] == dir_uuid {
        return Vec::new();
    }
    directories[slot] = dir_uuid;
    vec![MetadataRecord::V1Partition(PartitionRecord {
        topic: topic_name,
        partition: pr.partition,
        leader: pr.leader,
        replicas: pr.replicas.clone(),
        isr: pr.isr.clone(),
        leader_epoch: pr.leader_epoch,
        adding_replicas: pr.adding_replicas.clone(),
        removing_replicas: pr.removing_replicas.clone(),
        directories,
    })]
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
        let MetadataRecord::V1Partition(pr) = &changes[0] else {
            panic!("expected V1Partition")
        };
        assert!(pr.directories == vec![uuid::Uuid::nil(), dir]);
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
}
