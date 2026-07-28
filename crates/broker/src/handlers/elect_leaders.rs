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
use crabka_metadata::MetadataRecord;
use crabka_protocol::{
    Encode,
    owned::{
        elect_leaders_request::ElectLeadersRequest,
        elect_leaders_response::{ElectLeadersResponse, PartitionResult, ReplicaElectionResult},
    },
};
use crabka_units::convert::TimeExt as _;
use tokio::sync::oneshot;

use crate::{
    broker::Broker,
    codes,
    config_keys::{RecoveryStrategy, resolve_recovery_strategy},
    handlers::cluster_alter_denied,
    leader_election::{ElectError, ElectionType, select_new_leader_for_partition},
    unclean_recovery::{RecoveryJob, RecoveryOutcome},
};

const WIRE_ELECTION_PREFERRED: i8 = 0;
const WIRE_ELECTION_UNCLEAN: i8 = 1;

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
    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
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
    let targets = resolve_targets(&image, &req);

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
                rows.push(run_offset_aware_recovery(broker, topic, p, strategy).await);
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

fn resolve_targets(
    image: &crabka_metadata::MetadataImage,
    request: &ElectLeadersRequest,
) -> Vec<(String, Vec<i32>)> {
    request.topic_partitions.as_ref().map_or_else(
        || {
            image
                .topics()
                .map(|topic| {
                    let partitions = image
                        .partitions_of(&topic.name)
                        .map(|partition| partition.partition)
                        .collect();
                    (topic.name.clone(), partitions)
                })
                .collect()
        },
        |topics| {
            topics
                .iter()
                .map(|topic| {
                    let partitions = if topic.partitions.is_empty() {
                        image
                            .partitions_of(&topic.topic)
                            .map(|partition| partition.partition)
                            .collect()
                    } else {
                        topic.partitions.clone()
                    };
                    (topic.topic.clone(), partitions)
                })
                .collect()
        },
    )
}

async fn run_offset_aware_recovery(
    broker: &Broker,
    topic: &str,
    partition: i32,
    strategy: RecoveryStrategy,
) -> PartitionResult {
    let (tx, rx) = oneshot::channel();
    broker
        .unclean_recovery
        .enqueue(RecoveryJob {
            topic: topic.to_string(),
            partition,
            strategy,
            reply: Some(tx),
        })
        .await;
    let (error_code, error_message) =
        match tokio::time::timeout(broker.config.operator_recovery_deadline.to_std(), rx).await {
            Ok(Ok(RecoveryOutcome::Elected(_))) => (codes::NONE, None),
            Ok(Ok(RecoveryOutcome::NoEligibleReplica)) => (
                codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
                Some("no eligible replica responded".into()),
            ),
            Ok(Ok(RecoveryOutcome::NotNeeded)) => (
                codes::ELECTION_NOT_NEEDED,
                Some("partition already has a leader".into()),
            ),
            _ => (
                codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
                Some("unclean recovery in progress".into()),
            ),
        };
    PartitionResult {
        partition_id: partition,
        error_code,
        error_message,
        ..Default::default()
    }
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
    crate::handlers::encode_response_with_context(resp, api_version, "encode ElectLeaders")
}
