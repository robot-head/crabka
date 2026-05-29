//! `DeleteTopics` (`api_key=20`). Routes through `Controller::submit_change`
//! so every topic deletion is recorded in the metadata quorum before the
//! partition dirs and in-memory state are torn down.

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, DeleteTopicRecord, MetadataRecord};
use crabka_protocol::owned::delete_topics_request::DeleteTopicsRequest;
use crabka_protocol::owned::delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse};
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;

use crate::authorizer::{AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::log_dir;

#[allow(clippy::too_many_lines)]
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
    // Collect (name, topic_id_bytes) pairs. If the client sent only a
    // topic_id (name is None), resolve the name from the current image.
    let image = controller.current_image();
    let mut name_list: Vec<Option<String>> = Vec::new();
    if req.topic_names.is_empty() {
        for state in &req.topics {
            if let Some(ref n) = state.name {
                name_list.push(Some(n.clone()));
            } else {
                // name is absent — resolve by topic_id from the image.
                let found = image
                    .topics()
                    .find(|t| t.topic_id.into_bytes() == state.topic_id.0)
                    .map(|t| t.name.clone());
                name_list.push(found);
            }
        }
    } else {
        for n in &req.topic_names {
            name_list.push(Some(n.clone()));
        }
    }

    // KIP-599: count partition mutations before running the delete logic.
    // Nonexistent topics (name_opt = None) contribute 0 partitions.
    let mutation_count: u64 = name_list
        .iter()
        .map(|name_opt| {
            name_opt
                .as_deref()
                .map_or(0, |name| image.partitions_of(name).count() as u64)
        })
        .sum();

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Delete`. Topics that come
    // back `Deny` short-circuit the delete loop and emit
    // TOPIC_AUTHORIZATION_FAILED on that topic row.
    let known_names: Vec<&str> = name_list.iter().filter_map(|opt| opt.as_deref()).collect();
    let acl_results = authorize_topics(
        broker.config.authorizer.as_ref(),
        &image,
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

    for name_opt in name_list {
        let Some(name) = name_opt else {
            // topic_id not found in image — unknown topic.
            results.push(DeletableTopicResult {
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
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
