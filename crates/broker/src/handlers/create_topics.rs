//! `CreateTopics` (`api_key=19`). Routes through `Controller::submit_change`
//! so every topic/partition creation goes through the metadata quorum before
//! the partition directories are materialized on disk.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use crabka_protocol::owned::create_topics_request::CreateTopicsRequest;
use crabka_protocol::owned::create_topics_response::{CreatableTopicResult, CreateTopicsResponse};
use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;
use uuid::Uuid;

use crate::broker::{Broker, spawn_partition};
use crate::codes;
use crate::error::BrokerError;
use crate::log_dir;

/// Round-robin replica placement.
///
/// Given a sorted broker set `bs = [b0, b1, …, bk-1]` and a partition
/// count `P`, returns a `Vec<Vec<NodeId>>` of length `P`, where each
/// inner vec is `R = replication_factor` long. Partition `p`'s leader
/// is `bs[(p) % k]`; the remaining replicas are `bs[(p + i) % k]` for
/// `i in 1..R`. Caller must guarantee `R <= k` (else returns an empty
/// outer vec and the caller surfaces `INVALID_REPLICATION_FACTOR`).
fn round_robin_replicas(
    sorted_brokers: &[crabka_raft::NodeId],
    num_partitions: i32,
    replication_factor: i16,
) -> Vec<Vec<crabka_raft::NodeId>> {
    let k = sorted_brokers.len();
    let r = usize::try_from(replication_factor).unwrap_or(0);
    if r == 0 || r > k {
        return Vec::new();
    }
    let p_count = usize::try_from(num_partitions).unwrap_or(0);
    (0..p_count)
        .map(|p| {
            (0..r)
                .map(|i| sorted_brokers[(p + i) % k])
                .collect::<Vec<_>>()
        })
        .collect()
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    let log_dir = broker.config.log_dir.clone();
    let log_config = broker.config.log_config.clone();
    let partitions_map = broker.partitions.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = CreateTopicsRequest::decode(&mut cur, version)?;
        let mut results: Vec<CreatableTopicResult> = Vec::with_capacity(req.topics.len());

        for topic_req in req.topics {
            let name = topic_req.name.clone();
            let partition_count = topic_req.num_partitions;
            let replication_factor = topic_req.replication_factor;

            // Reject invalid partition counts before attempting placement.
            if partition_count <= 0 {
                results.push(CreatableTopicResult {
                    name,
                    topic_id: ProtoUuid([0u8; 16]),
                    error_code: codes::INVALID_PARTITIONS,
                    error_message: None,
                    ..Default::default()
                });
                continue;
            }

            // Read the current broker set from the controller's image; sort by
            // node_id for determinism.
            //
            // Race-tolerance: on a freshly-started cluster, the self-registration
            // V1BrokerRegistration record may not have made it into the local
            // MetadataImage yet when this handler runs (the controller's apply is
            // mostly synchronous on the leader but observable timing has slipped
            // on slow runners). If `brokers()` is empty, fall back to "this broker
            // is the only known broker" so the single-broker case (which is by
            // far the most common) doesn't silently degrade to
            // INVALID_REPLICATION_FACTOR.
            let image = controller.current_image();
            let mut sorted_brokers: Vec<crabka_raft::NodeId> =
                image.brokers().map(|b| b.node_id).collect();
            if sorted_brokers.is_empty() {
                sorted_brokers.push(node_id);
            }
            sorted_brokers.sort_unstable();

            let assignments =
                round_robin_replicas(&sorted_brokers, partition_count, replication_factor);

            if assignments.is_empty() {
                // RF > broker count. Surface INVALID_REPLICATION_FACTOR per Apache
                // Kafka semantics.
                results.push(CreatableTopicResult {
                    name,
                    topic_id: ProtoUuid([0u8; 16]),
                    error_code: codes::INVALID_REPLICATION_FACTOR,
                    error_message: None,
                    ..Default::default()
                });
                continue;
            }

            let topic_id = Uuid::new_v4();

            // Build the batch: one TopicRecord + N PartitionRecords.
            let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
                name: name.clone(),
                topic_id,
                partitions: partition_count,
                replication_factor,
            })];
            for (p, replicas) in assignments.iter().enumerate() {
                let p_i32 = i32::try_from(p).unwrap_or(0);
                records.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: name.clone(),
                    partition: p_i32,
                    leader: replicas[0],
                    replicas: replicas.clone(),
                    isr: replicas.clone(),
                }));
            }

            let result = controller.submit_change(records).await;

            let error_code = match result {
                Ok(()) => {
                    // Committed to quorum — materialize on-disk partitions for
                    // every assignment where THIS broker is in `replicas`,
                    // whether as leader or follower. The replicator supervisor
                    // materializes the same partitions on the OTHER brokers
                    // lazily via the metadata-watch; this handler-side path is
                    // an optimization so producers that immediately follow the
                    // CreateTopics ack don't race the supervisor.
                    for (p, replicas) in assignments.iter().enumerate() {
                        let p_i32 = i32::try_from(p).unwrap_or(0);
                        if replicas.contains(&node_id) {
                            let dir = log_dir::partition_dir(&log_dir, &name, p_i32);
                            match std::fs::create_dir_all(&dir)
                                .map_err(BrokerError::from)
                                .and_then(|()| {
                                    crabka_log::Log::open(&dir, log_config.clone())
                                        .map_err(BrokerError::from)
                                }) {
                                Ok(log) => {
                                    let part = spawn_partition(name.clone(), p_i32, log);
                                    partitions_map.insert((name.clone(), p_i32), part);
                                }
                                Err(e) => {
                                    tracing::error!(
                                        topic = %name,
                                        partition = p_i32,
                                        error = %e,
                                        "CreateTopics: disk failure after quorum commit"
                                    );
                                    // Quorum already committed — we cannot roll back.
                                    // Partition will be recovered on next broker restart.
                                }
                            }
                        }
                    }
                    codes::NONE
                }
                Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => {
                    codes::TOPIC_ALREADY_EXISTS
                }
                Err(RaftError::Metadata(crabka_metadata::MetadataError::InvalidRecord(_))) => {
                    // E.g., `partitions <= 0` rejected by image::validate.
                    codes::INVALID_PARTITIONS
                }
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                    codes::NOT_CONTROLLER
                }
                Err(e) => {
                    tracing::error!(topic = %name, error = %e, "CreateTopics submit_change failed");
                    codes::UNKNOWN_SERVER_ERROR
                }
            };

            // Convert uuid::Uuid → crabka_protocol::primitives::uuid::Uuid.
            let proto_uuid = ProtoUuid(topic_id.into_bytes());

            let mut result = CreatableTopicResult {
                name,
                topic_id: proto_uuid,
                error_code,
                error_message: None,
                ..Default::default()
            };

            if error_code == codes::NONE {
                result.num_partitions = partition_count;
                result.replication_factor = replication_factor;
                // KIP-525 (v5+): return an empty configs list to satisfy
                // clients that unconditionally call `configs().stream()`.
                result.configs = Some(Vec::new());
            }
            results.push(result);
        }

        let resp = CreateTopicsResponse {
            topics: results,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

#[cfg(test)]
mod replica_assignment_tests {
    use super::round_robin_replicas;

    #[test]
    fn three_brokers_three_partitions_rf_three() {
        let bs = vec![1u64, 2, 3];
        let out = round_robin_replicas(&bs, 3, 3);
        // Every broker should lead exactly one partition.
        let leaders: Vec<_> = out.iter().map(|r| r[0]).collect();
        let mut sorted = leaders.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 2, 3]);
        // Each partition has all three brokers as replicas.
        for replicas in &out {
            let mut s = replicas.clone();
            s.sort_unstable();
            assert_eq!(s, vec![1, 2, 3]);
        }
    }

    #[test]
    fn offset_per_partition_means_distinct_leaders() {
        let bs = vec![1u64, 2, 3];
        let out = round_robin_replicas(&bs, 3, 1);
        assert_eq!(out[0], vec![1]);
        assert_eq!(out[1], vec![2]);
        assert_eq!(out[2], vec![3]);
    }

    #[test]
    fn rf_too_high_returns_empty() {
        let bs = vec![1u64, 2, 3];
        let out = round_robin_replicas(&bs, 1, 5);
        assert!(out.is_empty());
    }

    #[test]
    fn rf_one_single_broker_preserves_slice7_shape() {
        let bs = vec![1u64];
        let out = round_robin_replicas(&bs, 2, 1);
        assert_eq!(out, vec![vec![1u64], vec![1u64]]);
    }
}
