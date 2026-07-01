//! `DeleteTopics` (`api_key=20`). Routes through `Controller::submit_change`
//! so every topic deletion is recorded in the metadata quorum before the
//! partition dirs and in-memory state are torn down.

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, DeleteTopicRecord, MetadataRecord};
use crabka_protocol::owned::delete_topics_request::DeleteTopicsRequest;
use crabka_protocol::owned::delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;

use crate::authorizer::{AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::log_dir;

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_delete_topics",
    level = "info",
    skip_all,
    fields(api = "DeleteTopics", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = &broker.controller;
    let partitions = broker.partitions.clone();
    let log_dirs = broker.config.all_log_dirs();

    let mut cur: &[u8] = req_bytes;
    let req = DeleteTopicsRequest::decode(&mut cur, version)?;

    // v0-5: `topic_names: Vec<String>` (topic_id not present).
    // v6+:  `topics: Vec<DeleteTopicState>` with optional name + topic_id.
    //
    // Collect (name, requested_by_id, topic_id_bytes) tuples. If the client
    // sent only a topic_id (name is None/empty), resolve the name from the
    // current image and mark the entry as id-based so that a miss returns
    // UNKNOWN_TOPIC_ID (KIP-516) rather than UNKNOWN_TOPIC_OR_PARTITION.
    let image = controller.current_image();
    // (resolved_name, requested_by_id, requested_topic_id)
    let mut name_list: Vec<(Option<String>, bool, WireUuid)> = Vec::new();
    if req.topic_names.is_empty() {
        for state in &req.topics {
            let id = state.topic_id;
            let requested_by_id = state
                .name
                .as_ref()
                .is_none_or(std::string::String::is_empty)
                && id != WireUuid::ZERO;
            if requested_by_id {
                // id-only path: look up by topic_id in the image index.
                let uuid = uuid::Uuid::from_bytes(id.0);
                let found = image.topic_by_id(&uuid).map(|t| t.name.clone());
                name_list.push((found, true, id));
            } else if let Some(ref n) = state.name {
                name_list.push((Some(n.clone()), false, id));
            } else {
                name_list.push((None, false, id));
            }
        }
    } else {
        for n in &req.topic_names {
            name_list.push((Some(n.clone()), false, WireUuid::ZERO));
        }
    }

    // KIP-599: count partition mutations before running the delete logic.
    // Nonexistent topics (name_opt = None) contribute 0 partitions.
    let mutation_count: u64 = name_list
        .iter()
        .map(|(name_opt, _, _)| {
            name_opt
                .as_deref()
                .map_or(0, |name| image.partitions_of(name).count() as u64)
        })
        .sum();

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Delete`. Topics that come
    // back `Deny` short-circuit the delete loop and emit
    // TOPIC_AUTHORIZATION_FAILED on that topic row.
    let known_names: Vec<&str> = name_list
        .iter()
        .filter_map(|(opt, _, _)| opt.as_deref())
        .collect();
    let acl_results = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Delete,
        known_names.iter().copied(),
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

    let mut results: Vec<DeletableTopicResult> = Vec::with_capacity(name_list.len());

    for (name_opt, requested_by_id, req_topic_id) in name_list {
        let Some(name) = name_opt else {
            // topic not found in image — choose error code by how it was requested.
            let error_code = if requested_by_id {
                codes::UNKNOWN_TOPIC_ID
            } else {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            };
            results.push(DeletableTopicResult {
                topic_id: req_topic_id,
                error_code,
                ..Default::default()
            });
            continue;
        };

        // Per-topic ACL check.
        if denied_topics.contains(&name) {
            results.push(DeletableTopicResult {
                name: Some(name),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }

        // Snapshot the (topic_id, partition_id) of every tiered
        // partition BEFORE the controller commits the delete and we tear
        // down in-memory state. After teardown the `Partition` is gone
        // and we lose the `remote.storage.enable` flag plus the topic_id;
        // the snapshot is the sole record that drives the remote-tier
        // partition-delete cascade.
        let tiered_to_cascade: Vec<crabka_remote_storage::TopicIdPartition> =
            if broker.remote_reader.is_some() {
                let topic_id = image.topic(&name).map(|t| t.topic_id);
                topic_id
                    .map(|tid| {
                        partitions
                            .partitions_of(&name)
                            .into_iter()
                            .filter(|&idx| {
                                partitions.get(&name, idx).is_some_and(|p| {
                                    p.log.lock().is_ok_and(|log| {
                                        log.config_snapshot().remote_storage_enable
                                    })
                                })
                            })
                            .map(|idx| {
                                crabka_remote_storage::TopicIdPartition::new(tid, name.clone(), idx)
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

        let res = controller
            .submit_change(vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: name.clone(),
            })])
            .await;

        let error_code = match res {
            Ok(()) => {
                // Committed to quorum — tear down in-memory state and dirs.
                for idx in partitions.partitions_of(&name) {
                    partitions.remove(&name, idx);
                    // JBOD: the partition may live in any log dir; resolve
                    // its actual location (existing-location wins).
                    let dir = log_dir::place_partition_dir(&log_dirs, &name, idx);
                    let _ = std::fs::remove_dir_all(dir);
                }
                // Now that the local tear-down is done, fire off
                // detached tasks that walk each tiered partition's remote
                // segments through `DeletePartitionMarked` →
                // `DeletePartitionStarted` → per-segment lifecycle →
                // `DeletePartitionFinished`. The response returns
                // immediately; failures inside the cascade log at WARN.
                if let Some(reader) = broker.remote_reader.as_ref() {
                    let broker_id = broker.config.broker_id;
                    for tp in tiered_to_cascade {
                        let rsm = reader.rsm.clone();
                        let rlmm = reader.rlmm.clone();
                        tokio::spawn(crate::remote_log_manager::cascade_remote_partition_delete(
                            tp, broker_id, rsm, rlmm,
                        ));
                    }
                }
                codes::NONE
            }
            Err(RaftError::Metadata(crabka_metadata::MetadataError::UnknownTopic(_))) => {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            }
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => codes::NOT_CONTROLLER,
            Err(e) => {
                tracing::error!(topic = %name, error = %e, "DeleteTopics submit_change failed");
                codes::UNKNOWN_SERVER_ERROR
            }
        };

        results.push(DeletableTopicResult {
            name: Some(name),
            error_code,
            ..Default::default()
        });
    }

    // Audit: emit one AdminOperation record for the successfully-deleted topics.
    let deleted: Vec<crabka_audit::AuditResource> = results
        .iter()
        .filter(|t| t.error_code == 0)
        .filter_map(|t| {
            t.name.as_deref().map(|n| crabka_audit::AuditResource {
                resource_type: "Topic".to_string(),
                name: n.to_string(),
            })
        })
        .collect();
    if !deleted.is_empty() {
        crate::handlers::audit_admin(
            broker,
            ctx,
            "DeleteTopics",
            crabka_audit::AuditOutcome::Success,
            deleted,
        );
    }

    // KIP-599: apply controller_mutation_rate throttle after response assembly.
    let delay = crate::quota::consume_controller_mutation_quota(
        &image,
        &broker.quota_buckets,
        ctx.principal.name.as_str(),
        ctx.client_id,
        mutation_count,
    );
    let throttle_time_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
    if delay > std::time::Duration::ZERO {
        tokio::time::sleep(delay).await;
    }

    let resp = DeleteTopicsResponse {
        responses: results,
        throttle_time_ms,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
