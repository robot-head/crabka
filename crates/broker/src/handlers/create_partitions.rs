//! `CreatePartitions` (`api_key=37`). `kafka-topics --alter --partitions
//! N`. Round-robin replica placement matches the slice-7 `CreateTopics`
//! path. Operator-supplied `assignments` are ignored in this slice
//! (round-robin only); honoring them is deferred.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, MetadataRecord, PartitionRecord, TopicRecord};
use crabka_protocol::owned::create_partitions_request::CreatePartitionsRequest;
use crabka_protocol::owned::create_partitions_response::{
    CreatePartitionsResponse, CreatePartitionsTopicResult,
};
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;

use crate::authorizer::{AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::create_topics::round_robin_replicas;
use crate::replicator_supervisor::materialize_partition;

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = CreatePartitionsRequest::decode(&mut cur, version)?;

    let controller = &broker.controller;
    let node_id = broker.config.node_id;
    let partitions_map = broker.partitions.clone();
    let log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();

    let image = controller.current_image();

    // KIP-599: count partition mutations before running handler logic so that
    // even invalid/rejected requests consume quota (bad-faith clients can't
    // escape throttling by sending malformed RPCs).
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let mutation_count: u64 = req
        .topics
        .iter()
        .map(|t| {
            let current: i32 =
                i32::try_from(image.partitions_of(&t.name).count()).unwrap_or(i32::MAX);
            (t.count - current).max(0) as u64
        })
        .sum();

    // ── slice-13 ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Alter`. Topics that come
    // back `Deny` short-circuit the partition-change loop and emit
    // TOPIC_AUTHORIZATION_FAILED on that topic row.
    let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
    let acl_results = authorize_topics(
        broker.config.authorizer.as_ref(),
        &image,
        ctx.principal,
        ctx.peer,
        AclOperation::Alter,
        topic_names.iter().copied(),
    );
    let denied_topics: std::collections::HashSet<String> = acl_results
        .iter()
        .filter_map(|(name, r)| {
            if *r == AuthorizationResult::Deny {
                Some((*name).to_string())
            } else {
                None
            }
        })
        .collect();

    let mut results: Vec<CreatePartitionsTopicResult> = Vec::with_capacity(req.topics.len());

    for t in req.topics {
        let mut out = CreatePartitionsTopicResult {
            name: t.name.clone(),
            error_code: codes::NONE,
            error_message: None,
            ..Default::default()
        };

        // Per-topic ACL check.
        if denied_topics.contains(&t.name) {
            out.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
            results.push(out);
            continue;
        }

        let Some(topic_rec) = image.topic(&t.name).cloned() else {
            out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
            out.error_message = Some(format!("unknown topic `{}`", t.name));
            results.push(out);
            continue;
        };

        let existing = topic_rec.partitions;
        if t.count <= existing {
            out.error_code = codes::INVALID_PARTITIONS;
            out.error_message = Some(format!(
                "topic `{}` already has {} partitions; cannot decrease to {}",
                t.name, existing, t.count
            ));
            results.push(out);
            continue;
        }

        let mut sorted_brokers: Vec<crabka_raft::NodeId> =
            image.brokers().map(|b| b.node_id).collect();
        if sorted_brokers.is_empty() {
            sorted_brokers.push(node_id);
        }
        sorted_brokers.sort_unstable();
        let rf = topic_rec.replication_factor;
        let new_count = t.count;
        let new_partition_indices: Vec<i32> = (existing..new_count).collect();
        let assignments = round_robin_replicas(&sorted_brokers, new_count, rf);
        if assignments.is_empty() {
            out.error_code = codes::INVALID_REPLICATION_FACTOR;
            out.error_message = Some(format!(
                "replication_factor={rf} > broker_count={}",
                sorted_brokers.len()
            ));
            results.push(out);
            continue;
        }

        if req.validate_only {
            results.push(out);
            continue;
        }

        // Build batch: one updated V1Topic (new partition count) +
        // one V1Partition per new index.
        let mut records: Vec<MetadataRecord> = Vec::with_capacity(new_partition_indices.len() + 1);
        records.push(MetadataRecord::V1Topic(TopicRecord {
            name: t.name.clone(),
            topic_id: topic_rec.topic_id,
            partitions: new_count,
            replication_factor: rf,
        }));
        for p in &new_partition_indices {
            let p_usize = usize::try_from(*p).unwrap_or(0);
            let replicas = assignments[p_usize].clone();
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                topic: t.name.clone(),
                partition: *p,
                leader: replicas[0],
                replicas: replicas.clone(),
                isr: replicas,
                leader_epoch: 0,
                adding_replicas: vec![],
                removing_replicas: vec![],
            }));
        }

        match controller.submit_change(records).await {
            Ok(()) => {
                // Materialize new partitions on local disk where self in replicas.
                for p in &new_partition_indices {
                    let p_usize = usize::try_from(*p).unwrap_or(0);
                    let replicas = &assignments[p_usize];
                    if !replicas.contains(&node_id) {
                        continue;
                    }
                    if let Err(e) = materialize_partition(
                        &partitions_map,
                        &t.name,
                        *p,
                        &log_dirs,
                        &log_config,
                        &log_dir_status,
                    ) {
                        tracing::error!(
                            topic = %t.name, partition = *p, error = %e,
                            "CreatePartitions: materialize after quorum commit failed"
                        );
                    } else if let Some(part) =
                        partitions_map.get(&(t.name.clone(), *p)).map(|e| e.clone())
                    {
                        let leader = replicas[0];
                        part.install_leader_change(leader, 0).await;
                        if leader == node_id {
                            part.install_isr(replicas, leader).await;
                        }
                    }
                }
            }
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                out.error_code = codes::NOT_CONTROLLER;
            }
            Err(e) => {
                tracing::error!(topic = %t.name, error = %e,
                    "CreatePartitions submit_change failed");
                out.error_code = codes::UNKNOWN_SERVER_ERROR;
            }
        }

        results.push(out);
    }

    // KIP-599: apply controller_mutation_rate throttle after response assembly,
    // before encoding. Sets throttle_time_ms and sleeps so the client waits.
    let delay = crate::quota::consume_controller_mutation_quota(
        &image,
        &broker.quota_buckets,
        ctx.principal.name.as_str(),
        ctx.client_id,
        mutation_count,
    );
    let resp = CreatePartitionsResponse {
        results,
        throttle_time_ms: i32::try_from(delay.as_millis()).unwrap_or(i32::MAX),
        ..Default::default()
    };
    if delay > std::time::Duration::ZERO {
        tokio::time::sleep(delay).await;
    }
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
