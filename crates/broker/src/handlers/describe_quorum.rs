//! `DescribeQuorum` (`api_key=55`, KIP-595). It returns the raft-quorum state
//! for the cluster-metadata topic.
//!
//! Crabka's `KRaft` setup runs one raft log, the controller quorum that
//! `controller_quorum_voters` configures, and applies committed records to
//! `MetadataImage`. Clients, such as the JVM `kafka-metadata-quorum
//! --describe` admin tool, ask for `__cluster_metadata` partition 0. The
//! broker answers from [`crabka_raft::ControllerHandle::quorum_state`]:
//!
//! - `leader_id` is `current_leader`. It is `-1` when the leader is unknown,
//!   for example during an election.
//! - `leader_epoch` is `current_term`, capped at `i32::MAX`.
//! - `high_watermark` is `last_applied_index` on this node's state machine,
//!   capped at `i64::MAX`.
//! - `current_voters` is openraft's voter set. Each voter's `log_end_offset`
//!   is openraft's `replication.matched.index`. openraft fills the per-voter
//!   replication map only on the leader, so on a follower every voter falls
//!   back to the JVM `-1` "Unknown" sentinel. Callers are meant to route
//!   `kafka-metadata-quorum --describe` to the leader.
//! - `observers` is empty, because Crabka has no observer role yet.
//!
//! For any topic OTHER than `__cluster_metadata`, the per-partition row gets
//! `INVALID_TOPIC_EXCEPTION` (17). That matches the JVM behavior on a
//! non-metadata topic.

use bytes::Bytes;
use crabka_metadata::AclOperation;
use crabka_protocol::{
    Decode,
    owned::{
        common::describe_quorum_response::replica_state::ReplicaState,
        describe_quorum_request::DescribeQuorumRequest,
        describe_quorum_response::{
            DescribeQuorumResponse, Listener, Node, PartitionData, TopicData,
        },
    },
    primitives::uuid::Uuid,
};
use crabka_raft::QuorumState;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
};

/// JVM "Unknown" sentinel for a voter's `log_end_offset` when openraft does
/// not track peer progress. That happens when this node is a follower, because
/// openraft fills the `replication` map only on the leader.
const UNKNOWN_LOG_END_OFFSET: i64 = -1;

/// The one Kafka-side topic name that represents the `KRaft` metadata log. It
/// mirrors `org.apache.kafka.common.Topic.CLUSTER_METADATA_TOPIC_NAME`.
const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

#[tracing::instrument(
    name = "handle_describe_quorum",
    level = "info",
    skip_all,
    fields(api = "DescribeQuorum", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) fn handle(
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
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Describe,
        },
    );
    if allow == AuthorizationResult::Deny {
        let resp = DescribeQuorumResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };
        return crate::handlers::encode_response(&resp, version);
    }

    // Snapshot raft state once — cheap clone of openraft's metrics
    // watch value. Carries the live current_term, last_applied_index,
    // and per-voter matched-log indexes (the last one populated only
    // when this node is the leader).
    let quorum = broker.controller.quorum_state();

    let topics = build_topic_responses(&req.topics, &quorum);

    // KIP-853 (v2+) adds a top-level `Nodes` block carrying each voter's
    // directory id + listeners. Encoding skips it on v0/v1 (the fields are
    // gated `versions: "2+"`), so populating it unconditionally stays
    // byte-exact for older clients.
    let nodes = build_nodes(&quorum);

    let resp = DescribeQuorumResponse {
        error_code: codes::NONE,
        topics,
        nodes,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

/// Builds one `TopicData` row per requested topic. The metadata raft topic
/// gets a full `PartitionData` for partition 0. Any other topic gets a
/// per-partition `INVALID_TOPIC_EXCEPTION` row. The function is pure, so a
/// test can drive it with a hand-built `QuorumState` and no controller.
fn build_topic_responses(
    requested: &[crabka_protocol::owned::describe_quorum_request::TopicData],
    quorum: &QuorumState,
) -> Vec<TopicData> {
    let leader_id = quorum
        .current_leader
        .map_or(-1, |n| i32::try_from(n.0).unwrap_or(-1));
    let leader_epoch = i32::try_from(quorum.current_term).unwrap_or(i32::MAX);
    let high_watermark = i64::try_from(quorum.last_applied_index).unwrap_or(i64::MAX);

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
                            leader_epoch,
                            high_watermark,
                            current_voters: quorum
                                .voters
                                .iter()
                                .map(|&id| {
                                    let matched = quorum
                                        .per_voter_matched_index
                                        .get(&id)
                                        .map_or(UNKNOWN_LOG_END_OFFSET, |&idx| {
                                            i64::try_from(idx).unwrap_or(i64::MAX)
                                        });
                                    // KIP-853 (v2+): the voter's directory
                                    // id, read from the replicated
                                    // membership. Zero (`Uuid::ZERO`) when
                                    // unknown — and skipped entirely on
                                    // v0/v1 encode.
                                    let replica_directory_id = quorum
                                        .voter_nodes
                                        .get(&id)
                                        .map_or(Uuid::ZERO, |n| Uuid(*n.directory_id.as_bytes()));
                                    ReplicaState {
                                        replica_id: i32::try_from(id.0).unwrap_or(-1),
                                        replica_directory_id,
                                        log_end_offset: matched,
                                        last_fetch_timestamp: -1,
                                        last_caught_up_timestamp: -1,
                                        ..Default::default()
                                    }
                                })
                                .collect(),
                            observers: quorum
                                .per_voter_matched_index
                                .iter()
                                .filter(|(id, _)| !quorum.voters.contains(id))
                                .map(|(id, offset)| ReplicaState {
                                    replica_id: i32::try_from(id.0).unwrap_or(-1),
                                    replica_directory_id: Uuid::ZERO,
                                    log_end_offset: i64::try_from(*offset).unwrap_or(i64::MAX),
                                    last_fetch_timestamp: -1,
                                    last_caught_up_timestamp: -1,
                                    ..Default::default()
                                })
                                .collect(),
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

/// Builds the KIP-853 (v2+) `Nodes` block: one entry per voter, with the
/// listeners of that voter's directory id.
///
/// The data comes from `quorum.voter_nodes`, which the raft layer fills from
/// the replicated membership config. Only the leader knows that config in
/// full, and a follower can carry a partial map. The encoder drops this whole
/// field on v0 and v1, so building it every time is harmless.
fn build_nodes(quorum: &QuorumState) -> Vec<Node> {
    quorum
        .voter_nodes
        .iter()
        .map(|(&id, node)| Node {
            node_id: i32::try_from(id.0).unwrap_or(-1),
            listeners: node
                .endpoints
                .iter()
                .map(|e| Listener {
                    name: e.name.clone(),
                    host: e.host.clone(),
                    port: e.port,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::describe_quorum_request::{
            PartitionData as ReqPartitionData, TopicData as ReqTopicData,
        },
    };

    use super::*;

    /// A fully specified expected voter row, with no struct-update syntax.
    fn expected_voter(replica_id: i32, log_end_offset: i64) -> ReplicaState {
        ReplicaState {
            replica_id,
            replica_directory_id: Uuid::ZERO,
            log_end_offset,
            last_fetch_timestamp: -1,
            last_caught_up_timestamp: -1,
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }
    }

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

    /// Helper that builds a `QuorumState` for a test.
    fn quorum_state(
        leader: Option<u64>,
        term: u64,
        applied: u64,
        voters: &[u64],
        matched: &[(u64, u64)],
    ) -> QuorumState {
        QuorumState {
            current_term: term,
            last_applied_index: applied,
            current_leader: leader.map(crabka_raft::NodeId),
            voters: voters.iter().copied().map(crabka_raft::NodeId).collect(),
            voter_nodes: BTreeMap::new(),
            per_voter_matched_index: matched
                .iter()
                .map(|&(v, m)| (crabka_raft::NodeId(v), m))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[test]
    fn metadata_topic_partition_zero_returns_voter_list_with_leader() {
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let q = quorum_state(
            Some(2),
            /*term=*/ 7,
            /*applied=*/ 42,
            &[1, 2, 3],
            &[(1, 40), (2, 42), (3, 38)],
        );
        let out = build_topic_responses(&req, &q);
        let expected = vec![TopicData {
            topic_name: CLUSTER_METADATA_TOPIC.to_string(),
            partitions: vec![PartitionData {
                partition_index: 0,
                error_code: codes::NONE,
                error_message: None,
                leader_id: 2,
                // current_term surfaces as leader_epoch.
                leader_epoch: 7,
                // last_applied_index surfaces as HW.
                high_watermark: 42,
                // Each voter's `log_end_offset` comes from the per-voter map.
                current_voters: vec![
                    expected_voter(1, 40),
                    expected_voter(2, 42),
                    expected_voter(3, 38),
                ],
                // No observers in Crabka yet.
                observers: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }];
        assert!(out == expected);
    }

    #[test]
    fn voters_missing_from_replication_map_get_unknown_sentinel() {
        // Follower case: replication map is empty (only the leader knows
        // peers' progress). Every voter's log_end_offset should be -1.
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let q = quorum_state(
            Some(1),
            /*term=*/ 3,
            /*applied=*/ 10,
            &[1, 2, 3],
            &[],
        );
        let out = build_topic_responses(&req, &q);
        let pd = &out[0].partitions[0];
        for v in &pd.current_voters {
            assert!(
                v.log_end_offset == UNKNOWN_LOG_END_OFFSET,
                "follower replication map empty → voter LEOs all -1"
            );
        }
    }

    #[test]
    fn voter_with_partial_replication_map_uses_per_voter_value_where_available() {
        // Mixed: leader knows progress for voter 1 only.
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let q = quorum_state(Some(1), 4, 50, &[1, 2, 3], &[(1, 50)]);
        let out = build_topic_responses(&req, &q);
        let pd = &out[0].partitions[0];
        let by_id: BTreeMap<i32, i64> = pd
            .current_voters
            .iter()
            .map(|v| (v.replica_id, v.log_end_offset))
            .collect();
        // Voter 1 gets its matched index; voters missing from the
        // replication map fall back to the -1 sentinel.
        let expected: BTreeMap<i32, i64> = [
            (1, 50),
            (2, UNKNOWN_LOG_END_OFFSET),
            (3, UNKNOWN_LOG_END_OFFSET),
        ]
        .into_iter()
        .collect();
        assert!(by_id == expected);
    }

    #[test]
    fn unknown_topic_returns_invalid_topic_exception() {
        let req = req_for("__consumer_offsets", 0);
        let q = quorum_state(Some(1), 1, 0, &[1], &[]);
        let out = build_topic_responses(&req, &q);
        let pd = &out[0].partitions[0];
        let expected = PartitionData {
            partition_index: 0,
            error_code: codes::INVALID_TOPIC_EXCEPTION,
            // The message names the only supported topic.
            error_message: Some("DescribeQuorum supports only `__cluster_metadata`".to_string()),
            leader_id: -1,
            leader_epoch: -1,
            high_watermark: -1,
            current_voters: Vec::new(),
            observers: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(*pd == expected);
    }

    #[test]
    fn metadata_topic_partition_nonzero_returns_invalid_topic_exception() {
        // KRaft cluster-metadata topic has exactly one partition (id 0).
        let req = req_for(CLUSTER_METADATA_TOPIC, 7);
        let q = quorum_state(Some(1), 1, 0, &[1], &[]);
        let out = build_topic_responses(&req, &q);
        let pd = &out[0].partitions[0];
        assert!(
            pd.error_code == codes::INVALID_TOPIC_EXCEPTION,
            "partition != 0 is not the metadata partition; reject"
        );
        assert!(pd.partition_index == 7, "echo the requested index back");
    }

    #[test]
    fn unknown_leader_emits_minus_one() {
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let q = quorum_state(/*leader=*/ None, 0, 0, &[1, 2], &[]);
        let out = build_topic_responses(&req, &q);
        let pd = &out[0].partitions[0];
        assert!(pd.leader_id == -1, "leader unknown surfaces as -1 sentinel");
        // Voter list still populated even when leader is unknown.
        assert!(pd.current_voters.len() == 2);
    }

    #[test]
    fn empty_request_returns_no_topics() {
        let q = quorum_state(Some(1), 1, 0, &[1], &[]);
        let out = build_topic_responses(&[], &q);
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
        let q = quorum_state(Some(1), 1, 0, &[1], &[]);
        let out = build_topic_responses(&req, &q);
        let codes_by_topic: Vec<(&str, i16)> = out
            .iter()
            .map(|t| (t.topic_name.as_str(), t.partitions[0].error_code))
            .collect();
        assert!(
            codes_by_topic
                == vec![
                    (CLUSTER_METADATA_TOPIC, codes::NONE),
                    ("other", codes::INVALID_TOPIC_EXCEPTION),
                ]
        );
    }

    #[test]
    fn v2_replica_directory_id_and_nodes_come_from_voter_nodes() {
        use crabka_metadata::VoterEndpoint;
        use crabka_raft::Node;

        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let dir1 = uuid::Uuid::from_u128(1);
        let dir2 = uuid::Uuid::from_u128(2);
        let mut voter_nodes = BTreeMap::new();
        voter_nodes.insert(
            crabka_audit::NodeId(1u64),
            Node {
                directory_id: dir1,
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "10.0.0.1".into(),
                    port: 9093,
                }],
                kraft_version: crabka_metadata::KRaftVersionRange::default(),
            },
        );
        voter_nodes.insert(
            crabka_audit::NodeId(2u64),
            Node {
                directory_id: dir2,
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "10.0.0.2".into(),
                    port: 9094,
                }],
                kraft_version: crabka_metadata::KRaftVersionRange::default(),
            },
        );
        let q = QuorumState {
            current_term: 1,
            last_applied_index: 5,
            current_leader: Some(crabka_audit::NodeId(1)),
            voters: vec![crabka_audit::NodeId(1), crabka_audit::NodeId(2)],
            voter_nodes,
            per_voter_matched_index: BTreeMap::new(),
        };

        // Per-voter replica_directory_id is sourced from voter_nodes.
        let topics = build_topic_responses(&req, &q);
        let voters = &topics[0].partitions[0].current_voters;
        let dir_by_id: BTreeMap<i32, Uuid> = voters
            .iter()
            .map(|v| (v.replica_id, v.replica_directory_id))
            .collect();
        assert!(dir_by_id[&1] == Uuid(*dir1.as_bytes()));
        assert!(dir_by_id[&2] == Uuid(*dir2.as_bytes()));

        // Top-level v2 Nodes block names each voter with its listeners.
        let nodes = build_nodes(&q);
        assert!(nodes.len() == 2);
        let first_voter = nodes.iter().find(|n| n.node_id == 1).unwrap();
        let expected_listener = Listener {
            name: "CONTROLLER".to_string(),
            host: "10.0.0.1".to_string(),
            port: 9093,
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(first_voter.listeners == vec![expected_listener]);
    }

    #[test]
    fn unknown_voter_directory_id_falls_back_to_zero() {
        // Follower with an empty voter_nodes map (only the leader fully
        // knows membership endpoints) → each replica_directory_id is ZERO
        // and the Nodes block is empty.
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let q = quorum_state(Some(1), 1, 0, &[1, 2], &[]);
        let topics = build_topic_responses(&req, &q);
        for v in &topics[0].partitions[0].current_voters {
            assert!(v.replica_directory_id == Uuid::ZERO);
        }
        assert!(build_nodes(&q).is_empty());
    }

    #[test]
    fn leader_id_above_i32_max_falls_back_to_minus_one() {
        // A raft NodeId is u64; the wire replica/leader id is i32. A leader
        // node id beyond i32::MAX must surface as the -1 "unknown" sentinel
        // (via `try_from(..).unwrap_or(-1)`), never wrap into a positive id.
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let huge = u64::from(u32::MAX) + 1; // > i32::MAX, try_from fails
        let q = quorum_state(Some(huge), 1, 0, &[1], &[]);
        let out = build_topic_responses(&req, &q);
        assert!(
            out[0].partitions[0].leader_id == -1,
            "leader node id > i32::MAX must fall back to -1, not a positive id"
        );
    }

    #[test]
    fn voter_replica_id_above_i32_max_falls_back_to_minus_one() {
        // Same guard on the per-voter replica_id: a voter node id beyond
        // i32::MAX surfaces as -1, not a wrapped positive value.
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let huge = u64::from(u32::MAX) + 1; // > i32::MAX
        let q = quorum_state(Some(1), 1, 0, &[huge], &[]);
        let out = build_topic_responses(&req, &q);
        let voters = &out[0].partitions[0].current_voters;
        assert!(voters.len() == 1);
        assert!(
            voters[0].replica_id == -1,
            "voter node id > i32::MAX must fall back to -1, not a positive id"
        );
    }

    #[test]
    fn current_term_above_i32_max_saturates() {
        // Defensive: openraft's term is u64; KRaft wire is i32. A term
        // beyond i32::MAX (huge cluster history) saturates so we don't
        // wrap silently into a negative epoch.
        let req = req_for(CLUSTER_METADATA_TOPIC, 0);
        let q = quorum_state(Some(1), u64::MAX, 0, &[1], &[]);
        let out = build_topic_responses(&req, &q);
        assert!(out[0].partitions[0].leader_epoch == i32::MAX);
    }
}
