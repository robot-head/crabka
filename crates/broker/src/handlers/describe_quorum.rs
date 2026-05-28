//! `DescribeQuorum` (`api_key=55`, KIP-595). Returns raft-quorum state
//! for the cluster-metadata topic.
//!
//! Crabka's `KRaft` setup runs a single raft log (the controller quorum
//! configured via `controller_quorum_voters`) and applies committed
//! records to `MetadataImage`. Clients (the JVM
//! `kafka-metadata-quorum --describe` admin tool) ask for
//! `__cluster_metadata` partition 0; we respond with:
//!
//! - `leader_id` = current openraft leader from `ControllerHandle::watch_leader`
//!   (`-1` when unknown — e.g. mid-election).
//! - `leader_epoch` = `-1` (sentinel). Real epoch surface is a follow-up
//!   that needs to expose `openraft::Metrics::current_term`.
//! - `high_watermark` = `-1` (sentinel). Same follow-up.
//! - `current_voters` = `node_id`s from
//!   `BrokerConfig::controller_quorum_voters`, each with `log_end_offset = -1`.
//! - `observers` = empty (Crabka has no observer-role concept yet).
//!
//! Sentinel values are honest about what we know today — the JVM admin
//! tool prints `-1` as "Unknown", which is the right operator-facing
//! signal. Wiring real `openraft::Metrics` through `ControllerHandle`
//! is deferred to a sub-slice; the structural-only response is enough
//! for `kafka-metadata-quorum --describe` to surface the voter set.
//!
//! For any topic OTHER than `__cluster_metadata`, the per-partition row
//! gets `INVALID_TOPIC_EXCEPTION` (17) — matches the JVM behavior on
//! a non-metadata topic.

use bytes::{Bytes, BytesMut};

use crabka_metadata::AclOperation;
use crabka_protocol::owned::common::replica_state::ReplicaState;
use crabka_protocol::owned::describe_quorum_request::DescribeQuorumRequest;
use crabka_protocol::owned::describe_quorum_response::{
    DescribeQuorumResponse, PartitionData, TopicData,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

/// Sentinel `log_end_offset` for a voter whose progress we don't yet
/// surface. JVM admin tool renders `-1` as "Unknown".
const UNKNOWN_LOG_END_OFFSET: i64 = -1;

/// Sentinel `leader_epoch` / `high_watermark`. Crabka doesn't yet expose
/// `openraft::Metrics::current_term` or the last-applied log index
/// through `ControllerHandle`; emit `-1` (the JVM "Unknown" sentinel)
/// until that wiring lands.
const UNKNOWN_RAFT_PROGRESS: i64 = -1;

/// The single Kafka-side topic name that represents the `KRaft` metadata
/// log. Mirrors `org.apache.kafka.common.Topic.CLUSTER_METADATA_TOPIC_NAME`.
const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

#[allow(clippy::unused_async)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let image = broker.controller.current_image();

    let mut cur: &[u8] = req_bytes;
    let req = DescribeQuorumRequest::decode(&mut cur, version)?;

    // Whole-request Cluster Describe gate. DescribeQuorum is
    // cluster-wide raft introspection — same gate as DescribeCluster.
    let allow = broker.config.authorizer.authorize(
        &image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: AclOperation::Describe,
        },
    );
    if allow == AuthorizationResult::Deny {
        let resp = DescribeQuorumResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        return Ok(buf.freeze());
    }

    // Snapshot raft state once. `watch_leader().borrow()` is a cheap
    // read of the cached leader id; we cast to `i32` for the wire
    // (clamping to `-1` on overflow — operator-visible "Unknown").
    let leader_id_i32: i32 = broker
        .controller
        .watch_leader()
        .borrow()
        .map_or(-1, |n| i32::try_from(n).unwrap_or(-1));

    let voter_ids: Vec<u64> = broker
        .config
        .controller_quorum_voters
        .iter()
        .map(|(id, _addr)| *id)
        .collect();

    let topics = build_topic_responses(&req.topics, leader_id_i32, &voter_ids);

    let resp = DescribeQuorumResponse {
        error_code: codes::NONE,
        topics,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Build a `TopicData` row per requested topic. The metadata raft topic
/// gets a populated `PartitionData` for partition 0; any other topic
/// gets a per-partition `INVALID_TOPIC_EXCEPTION` row. Pure — testable
/// without a controller.
fn build_topic_responses(
    requested: &[crabka_protocol::owned::describe_quorum_request::TopicData],
    leader_id: i32,
    voter_ids: &[u64],
) -> Vec<TopicData> {
    requested
        .iter()
        .map(|t| {
            let partitions: Vec<PartitionData> = t
                .partitions
                .iter()
                .map(|p| {
                    if t.topic_name == CLUSTER_METADATA_TOPIC && p.partition_index == 0 {
                        PartitionData {
                            partition_index: 0,
                            error_code: codes::NONE,
                            error_message: None,
                            leader_id,
                            leader_epoch: i32::try_from(UNKNOWN_RAFT_PROGRESS).unwrap_or(-1),
                            high_watermark: UNKNOWN_RAFT_PROGRESS,
                            current_voters: voter_ids
                                .iter()
                                .map(|&id| ReplicaState {
                                    replica_id: i32::try_from(id).unwrap_or(-1),
                                    log_end_offset: UNKNOWN_LOG_END_OFFSET,
                                    last_fetch_timestamp: -1,
                                    last_caught_up_timestamp: -1,
                                    ..Default::default()
                                })
                                .collect(),
                            observers: Vec::new(),
                            ..Default::default()
                        }
                    } else {
                        PartitionData {
                            partition_index: p.partition_index,
                            error_code: codes::INVALID_TOPIC_EXCEPTION,
                            error_message: Some(format!(
                                "DescribeQuorum supports only `{CLUSTER_METADATA_TOPIC}`",
                            )),
                            leader_id: -1,
                            leader_epoch: -1,
                            high_watermark: -1,
                            current_voters: Vec::new(),
                            observers: Vec::new(),
                            ..Default::default()
                        }
                    }
                })
                .collect();
            TopicData {
                topic_name: t.topic_name.clone(),
                partitions,
                ..Default::default()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_protocol::owned::describe_quorum_request::{
        PartitionData as ReqPartitionData, TopicData as ReqTopicData,
    };

    fn req_for(topic: &str, partition: i32) -> Vec<ReqTopicData> {
        vec![ReqTopicData {
            topic_name: topic.into(),
            partitions: vec![ReqPartitionData {
                partition_index: partition,
                ..Default::default()
            }],
            ..Default::default()
        }]
    }

    #[test]
    fn metadata_topic_partition_zero_returns_voter_list_with_leader() {
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let out = build_topic_responses(&req, /*leader=*/ 2, &[1, 2, 3]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].topic_name, CLUSTER_METADATA_TOPIC);
        assert_eq!(out[0].partitions.len(), 1);
        let pd = &out[0].partitions[0];
        assert_eq!(pd.error_code, codes::NONE);
        assert_eq!(pd.leader_id, 2, "leader_id must propagate from caller");
        assert_eq!(pd.current_voters.len(), 3, "all voters present");
        let voter_ids: Vec<i32> = pd.current_voters.iter().map(|v| v.replica_id).collect();
        assert_eq!(voter_ids, vec![1, 2, 3]);
        for v in &pd.current_voters {
            assert_eq!(
                v.log_end_offset, UNKNOWN_LOG_END_OFFSET,
                "log_end_offset is the `Unknown` sentinel until raft metrics are wired in"
            );
        }
        assert!(pd.observers.is_empty(), "no observers in Crabka yet");
    }

    #[test]
    fn unknown_topic_returns_invalid_topic_exception() {
        let req = req_for("__consumer_offsets", 0);
        let out = build_topic_responses(&req, 1, &[1]);
        let pd = &out[0].partitions[0];
        assert_eq!(pd.error_code, codes::INVALID_TOPIC_EXCEPTION);
        assert!(pd.current_voters.is_empty());
        assert!(
            pd.error_message
                .as_deref()
                .unwrap_or("")
                .contains(CLUSTER_METADATA_TOPIC),
            "error_message names the only supported topic",
        );
    }

    #[test]
    fn metadata_topic_partition_nonzero_returns_invalid_topic_exception() {
        // KRaft cluster-metadata topic has exactly one partition (id 0).
        let req = req_for(CLUSTER_METADATA_TOPIC, 7);
        let out = build_topic_responses(&req, 1, &[1]);
        let pd = &out[0].partitions[0];
        assert_eq!(
            pd.error_code,
            codes::INVALID_TOPIC_EXCEPTION,
            "partition != 0 is not the metadata partition; reject"
        );
        assert_eq!(pd.partition_index, 7, "echo the requested index back");
    }

    #[test]
    fn unknown_leader_emits_minus_one() {
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let out = build_topic_responses(&req, /*leader=*/ -1, &[1, 2]);
        let pd = &out[0].partitions[0];
        assert_eq!(pd.leader_id, -1, "leader unknown surfaces as -1 sentinel");
        // Voter list still populated even when leader is unknown.
        assert_eq!(pd.current_voters.len(), 2);
    }

    #[test]
    fn empty_request_returns_no_topics() {
        let out = build_topic_responses(&[], 1, &[1]);
        assert!(out.is_empty());
    }

    #[test]
    fn multiple_topics_each_get_their_own_row() {
        let req = vec![
            ReqTopicData {
                topic_name: CLUSTER_METADATA_TOPIC.into(),
                partitions: vec![ReqPartitionData {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            },
            ReqTopicData {
                topic_name: "other".into(),
                partitions: vec![ReqPartitionData {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            },
        ];
        let out = build_topic_responses(&req, 1, &[1]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].partitions[0].error_code, codes::NONE);
        assert_eq!(
            out[1].partitions[0].error_code,
            codes::INVALID_TOPIC_EXCEPTION
        );
    }
}
