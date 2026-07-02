//! `ElectLeaders` (`api_key` 43, KIP-460).
//!
//! Operator-triggered leader election. PREFERRED type moves leadership
//! back to `replicas[0]` after operator intervention; UNCLEAN type
//! elects outside the ISR when every ISR member is dead.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On Deny the
//! whole request returns `CLUSTER_AUTHORIZATION_FAILED (31)` on every
//! per-partition row.

use std::collections::HashMap;

use bytes::Bytes;
use crabka_metadata::{MetadataRecord, ResourceType};
use crabka_protocol::Encode;
use crabka_protocol::owned::elect_leaders_request::ElectLeadersRequest;
use crabka_protocol::owned::elect_leaders_response::{
    ElectLeadersResponse, PartitionResult, ReplicaElectionResult,
};

use tokio::sync::oneshot;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::config_keys::{RecoveryStrategy, resolve_recovery_strategy};
use crate::leader_election::{ElectError, ElectionType, select_new_leader_for_partition};
use crate::unclean_recovery::{RecoveryJob, RecoveryOutcome};

const WIRE_ELECTION_PREFERRED: i8 = 0;
const WIRE_ELECTION_UNCLEAN: i8 = 1;

/// Operator-triggered offset-aware recovery is bounded so the admin RPC
/// can't hang on a stalled replica. Slightly above the URM's Balanced
/// deadline (30s) would let the manager finish; we use 25s to fail the
/// client request before the inter-broker poll's own cap and surface a
/// retriable error.
const OPERATOR_RECOVERY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(25);

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_elect_leaders",
    level = "info",
    skip_all,
    fields(api = "ElectLeaders"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: ElectLeadersRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // Authorize Cluster Alter — whole-request gate.
    let image = broker.controller.current_image();
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        return encode_whole_request_error(
            &req,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "elect-leaders denied",
            api_version,
        );
    }

    // Decode election_type discriminant.
    let election = match req.election_type {
        WIRE_ELECTION_PREFERRED => ElectionType::Preferred,
        WIRE_ELECTION_UNCLEAN => ElectionType::Unclean,
        _ => {
            return encode_whole_request_error(
                &req,
                codes::INVALID_REQUEST,
                "unknown election_type",
                api_version,
            );
        }
    };

    // Resolve target partition set:
    //   topic_partitions = None      → every partition in the image
    //   Some([{topic, []}])          → every partition of that topic
    //   Some([{topic, [p, q, ...]}]) → exact set
    let targets: Vec<(String, Vec<i32>)> = match &req.topic_partitions {
        None => image
            .topics()
            .map(|t| {
                (
                    t.name.clone(),
                    image
                        .partitions_of(&t.name)
                        .map(|p| p.partition)
                        .collect::<Vec<_>>(),
                )
            })
            .collect(),
        Some(list) => list
            .iter()
            .map(|tp| {
                let partitions = if tp.partitions.is_empty() {
                    image
                        .partitions_of(&tp.topic)
                        .map(|p| p.partition)
                        .collect()
                } else {
                    tp.partitions.clone()
                };
                (tp.topic.clone(), partitions)
            })
            .collect(),
    };

    // Run the algorithm per target; accumulate new records to submit
    // and per-partition results to ship back.
    let liveness = broker.liveness.clone();
    let mut by_topic: HashMap<String, Vec<PartitionResult>> = HashMap::new();
    let mut to_submit: Vec<MetadataRecord> = Vec::new();
    for (topic, partitions) in &targets {
        let mut rows = Vec::with_capacity(partitions.len());
        for &p in partitions {
            // KIP-966: an UNCLEAN election on a topic that opted into an
            // offset-aware recovery strategy is routed through the Unclean
            // Recovery Manager, which polls surviving replicas for their log
            // state before electing. The URM owns `submit_change` for these,
            // so we must NOT push a record into `to_submit` here — we just
            // await the outcome and translate it to a per-partition row.
            let use_offset_aware = matches!(election, ElectionType::Unclean)
                && !matches!(
                    resolve_recovery_strategy(&image, topic),
                    RecoveryStrategy::None
                );
            if use_offset_aware {
                let strategy = resolve_recovery_strategy(&image, topic);
                let (tx, rx) = oneshot::channel();
                broker
                    .unclean_recovery
                    .enqueue(RecoveryJob {
                        topic: topic.clone(),
                        partition: p,
                        strategy,
                        reply: Some(tx),
                    })
                    .await;
                let row = match tokio::time::timeout(OPERATOR_RECOVERY_DEADLINE, rx).await {
                    Ok(Ok(RecoveryOutcome::Elected(_))) => PartitionResult {
                        partition_id: p,
                        error_code: 0,
                        error_message: None,
                        ..Default::default()
                    },
                    Ok(Ok(RecoveryOutcome::NoEligibleReplica)) => PartitionResult {
                        partition_id: p,
                        error_code: codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
                        error_message: Some("no eligible replica responded".into()),
                        ..Default::default()
                    },
                    Ok(Ok(RecoveryOutcome::NotNeeded)) => PartitionResult {
                        partition_id: p,
                        error_code: codes::ELECTION_NOT_NEEDED,
                        error_message: Some("partition already has a leader".into()),
                        ..Default::default()
                    },
                    // Stale / InProgress, dropped reply channel, or the
                    // operator deadline elapsed: surface a retriable error.
                    _ => PartitionResult {
                        partition_id: p,
                        error_code: codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
                        error_message: Some("unclean recovery in progress".into()),
                        ..Default::default()
                    },
                };
                rows.push(row);
                continue;
            }

            let result =
                select_new_leader_for_partition(&image, &liveness, topic, p, election).await;
            match result {
                Ok(new_pr) => {
                    to_submit.push(MetadataRecord::V1Partition(new_pr));
                    rows.push(PartitionResult {
                        partition_id: p,
                        error_code: 0,
                        error_message: None,
                        ..Default::default()
                    });
                }
                Err(err) => {
                    let (code, msg) = elect_error_to_wire(err);
                    rows.push(PartitionResult {
                        partition_id: p,
                        error_code: code,
                        error_message: Some(msg.into()),
                        ..Default::default()
                    });
                }
            }
        }
        by_topic.insert(topic.clone(), rows);
    }

    // Submit accumulated records. On failure, mark every queued OK row
    // with COORDINATOR_NOT_AVAILABLE.
    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "elect-leaders submit failed");
        for rows in by_topic.values_mut() {
            for r in rows.iter_mut() {
                if r.error_code == 0 {
                    r.error_code = codes::COORDINATOR_NOT_AVAILABLE;
                    r.error_message = Some(format!("submit failed: {e}"));
                }
            }
        }
    }

    // Build response.
    let replica_election_results: Vec<ReplicaElectionResult> = by_topic
        .into_iter()
        .map(|(topic, partition_result)| ReplicaElectionResult {
            topic,
            partition_result,
            ..Default::default()
        })
        .collect();

    let resp = ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: 0,
        replica_election_results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn elect_error_to_wire(err: ElectError) -> (i16, &'static str) {
    match err {
        ElectError::UnknownTopicOrPartition => (
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            "unknown topic or partition",
        ),
        ElectError::PreferredAlreadyLeader => (
            codes::ELECTION_NOT_NEEDED,
            "preferred replica is already leader",
        ),
        ElectError::ElectionNotNeeded => (
            codes::ELECTION_NOT_NEEDED,
            "isr still has a live member; unclean election not needed",
        ),
        ElectError::PreferredNotInIsr => (
            codes::PREFERRED_LEADER_NOT_AVAILABLE,
            "preferred replica not in ISR",
        ),
        ElectError::PreferredNotAlive => (
            codes::PREFERRED_LEADER_NOT_AVAILABLE,
            "preferred replica not alive",
        ),
        ElectError::NoEligibleReplica => {
            (codes::ELIGIBLE_LEADERS_NOT_AVAILABLE, "no alive replica")
        }
    }
}

fn encode_whole_request_error(
    req: &ElectLeadersRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // Build a response where every requested (topic, partition) row
    // carries the whole-request error code. Top-level error_code = 0
    // since the per-row codes carry the failure (matches Kafka).
    let results: Vec<ReplicaElectionResult> = match &req.topic_partitions {
        None => vec![],
        Some(list) => list
            .iter()
            .map(|tp| ReplicaElectionResult {
                topic: tp.topic.clone(),
                partition_result: tp
                    .partitions
                    .iter()
                    .map(|&p| PartitionResult {
                        partition_id: p,
                        error_code: code,
                        error_message: Some(msg.into()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
    };
    let resp = ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: 0,
        replica_election_results: results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| crate::error::BrokerError::Replication(format!("encode ElectLeaders: {e}")))?;
    Ok(Bytes::from(body))
}
