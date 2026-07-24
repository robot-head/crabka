//! `DeleteTopics` (`api_key=20`). Routes through `Controller::submit_change`
//! so every topic deletion is recorded in the metadata quorum before the
//! partition dirs and in-memory state are torn down.

use std::time::Duration;

use bytes::Bytes;
use crabka_metadata::{AclOperation, DeleteTopicRecord, MetadataRecord};
use crabka_protocol::{
    Decode,
    owned::{
        delete_topics_request::DeleteTopicsRequest,
        delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
    },
    primitives::uuid::Uuid as WireUuid,
};
use crabka_raft::RaftError;

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
    log_dir,
};

fn requested_by_topic_id(name: Option<&String>, id: WireUuid) -> bool {
    name.is_none_or(std::string::String::is_empty) && id != WireUuid::ZERO
}

fn delete_topic_result(
    name: Option<String>,
    topic_id: WireUuid,
    error_code: i16,
) -> DeletableTopicResult {
    DeletableTopicResult {
        name,
        topic_id,
        error_code,
        ..Default::default()
    }
}

fn delete_topics_response(
    responses: Vec<DeletableTopicResult>,
    throttle_time_ms: i32,
) -> DeleteTopicsResponse {
    DeleteTopicsResponse {
        responses,
        throttle_time_ms,
        ..Default::default()
    }
}

fn deleted_topic_resources(results: &[DeletableTopicResult]) -> Vec<crabka_audit::AuditResource> {
    results
        .iter()
        .filter(|t| t.error_code == codes::NONE)
        .filter_map(|t| {
            t.name.as_deref().map(|n| crabka_audit::AuditResource {
                resource_type: "Topic".to_string(),
                name: n.to_string(),
            })
        })
        .collect()
}

fn audit_deleted_topics(
    audit_log: &crabka_audit::AuditLog,
    ctx: &crate::handlers::RequestContext<'_>,
    deleted: Vec<crabka_audit::AuditResource>,
) {
    if !deleted.is_empty() {
        crate::handlers::audit_admin(
            audit_log,
            ctx,
            "DeleteTopics",
            crabka_audit::AuditOutcome::Success,
            deleted,
        );
    }
}

fn should_wait_for_quota_delay(delay: Duration) -> bool {
    delay > Duration::ZERO
}

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
    let name_list = resolve_topic_names(&req, &image);

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
    let denied_topics = denied_topic_names(
        broker.config.authorizer.as_ref(),
        &image,
        ctx.principal,
        ctx.peer,
        &name_list,
    );

    let mut results: Vec<DeletableTopicResult> = Vec::with_capacity(name_list.len());

    for (name_opt, requested_by_id, req_topic_id) in name_list {
        let Some(name) = name_opt else {
            // topic not found in image — choose error code by how it was requested.
            let error_code = if requested_by_id {
                codes::UNKNOWN_TOPIC_ID
            } else {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            };
            results.push(delete_topic_result(None, req_topic_id, error_code));
            continue;
        };

        // Per-topic ACL check.
        if denied_topics.contains(&name) {
            results.push(delete_topic_result(
                Some(name),
                WireUuid::ZERO,
                codes::TOPIC_AUTHORIZATION_FAILED,
            ));
            continue;
        }

        // Snapshot every local partition before committing the metadata
        // deletion. The metadata image watcher can remove registry entries as
        // soon as the commit becomes visible; enumerating afterward races that
        // watcher and can leave the on-disk log directory behind. A later
        // create of the same topic name would then reopen the deleted topic's
        // WAL, including stale transactional visibility state.
        let local_partitions = partitions.partitions_of(&name);

        // Snapshot the (topic_id, partition_id) of every tiered
        // partition BEFORE the controller commits the delete and we tear
        // down in-memory state. After teardown the `Partition` is gone
        // and we lose the `remote.storage.enable` flag plus the topic_id;
        // the snapshot is the sole record that drives the remote-tier
        // partition-delete cascade.
        let tiered_to_cascade =
            tiered_partitions(broker, &partitions, &image, &name, &local_partitions);

        let res = controller
            .submit_change(vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: name.clone(),
            })])
            .await;

        let error_code = match res {
            Ok(_) => {
                // Committed to quorum — tear down in-memory state and dirs.
                for idx in local_partitions {
                    partitions.remove(&name, idx);
                    // JBOD: the partition may live in any log dir; resolve
                    // its actual location (existing-location wins).
                    let dir = log_dir::place_partition_dir(&log_dirs, &name, idx.get());
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

        results.push(delete_topic_result(Some(name), WireUuid::ZERO, error_code));
    }

    // Audit: emit one AdminOperation record for the successfully-deleted topics.
    audit_deleted_topics(
        broker.audit_log.as_ref(),
        ctx,
        deleted_topic_resources(&results),
    );

    // KIP-599: apply controller_mutation_rate throttle after response assembly.
    let delay = crate::quota::consume_controller_mutation_quota(
        &image,
        &broker.quota_buckets,
        ctx.principal.name.as_str(),
        ctx.client_id,
        mutation_count,
        broker.config.quota_throttle_max,
    );
    let throttle_time_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
    if should_wait_for_quota_delay(delay) {
        tokio::time::sleep(delay).await;
    }

    let resp = delete_topics_response(results, throttle_time_ms);
    crate::handlers::encode_response(&resp, version)
}

type TopicNameRequest = (Option<String>, bool, WireUuid);

fn tiered_partitions(
    broker: &Broker,
    partitions: &crate::partition_registry::PartitionRegistry,
    image: &crabka_metadata::MetadataImage,
    topic_name: &str,
    local_partitions: &[crabka_ids::PartitionIndex],
) -> Vec<crabka_remote_storage::TopicIdPartition> {
    if broker.remote_reader.is_none() {
        return Vec::new();
    }
    let Some(topic_id) = image.topic(topic_name).map(|topic| topic.topic_id) else {
        return Vec::new();
    };
    local_partitions
        .iter()
        .copied()
        .filter(|&index| {
            partitions.get(topic_name, index).is_some_and(|partition| {
                partition
                    .log
                    .lock()
                    .is_ok_and(|log| log.config_snapshot().remote_storage_enable)
            })
        })
        .map(|index| {
            crabka_remote_storage::TopicIdPartition::new(
                topic_id,
                topic_name.to_string(),
                index.get(),
            )
        })
        .collect()
}

fn resolve_topic_names(
    request: &DeleteTopicsRequest,
    image: &crabka_metadata::MetadataImage,
) -> Vec<TopicNameRequest> {
    if !request.topic_names.is_empty() {
        return request
            .topic_names
            .iter()
            .map(|name| (Some(name.clone()), false, WireUuid::ZERO))
            .collect();
    }
    request
        .topics
        .iter()
        .map(|state| {
            let requested_by_id = requested_by_topic_id(state.name.as_ref(), state.topic_id);
            let name = if requested_by_id {
                image
                    .topic_by_id(&uuid::Uuid::from_bytes(state.topic_id.0))
                    .map(|topic| topic.name.clone())
            } else {
                state.name.clone()
            };
            (name, requested_by_id, state.topic_id)
        })
        .collect()
}

fn denied_topic_names(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    principal: &crabka_security::Principal,
    peer: &std::net::SocketAddr,
    requests: &[TopicNameRequest],
) -> std::collections::HashSet<String> {
    let known_names = requests.iter().filter_map(|(name, _, _)| name.as_deref());
    authorize_topics(
        authorizer,
        image,
        principal,
        peer,
        AclOperation::Delete,
        known_names,
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Deny)
    .map(|(name, _)| name.to_string())
    .collect()
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::{assert, check};
    use crabka_protocol::owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest};
    use crabka_security::Principal;

    use super::*;
    use crate::test_support::{DenyAll, peer, principal};

    const VERSION: i16 = 6;

    crate::test_support::wire_helpers!(
        DeleteTopicsRequest,
        DeleteTopicsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    fn named_state(name: &str) -> DeleteTopicState {
        DeleteTopicState {
            name: Some(name.into()),
            ..Default::default()
        }
    }

    fn id_state(id: WireUuid) -> DeleteTopicState {
        DeleteTopicState {
            name: None,
            topic_id: id,
            ..Default::default()
        }
    }

    fn request(topics: Vec<DeleteTopicState>) -> DeleteTopicsRequest {
        DeleteTopicsRequest {
            topics,
            timeout_ms: 5_000,
            ..Default::default()
        }
    }

    async fn drive(
        broker: &Broker,
        req: &DeleteTopicsRequest,
        principal: &Principal,
        peer: &SocketAddr,
    ) -> DeleteTopicsResponse {
        let ctx = test_context(principal, peer);
        let req_bytes = encode_request(req);
        let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        decode_response(&bytes)
    }

    #[test]
    fn requested_by_topic_id_requires_empty_name_and_nonzero_id() {
        let id = WireUuid([7; 16]);
        let empty = String::new();
        let named = String::from("orders");

        check!(requested_by_topic_id(None, id));
        check!(requested_by_topic_id(Some(&empty), id));
        check!(!requested_by_topic_id(Some(&named), id));
        check!(!requested_by_topic_id(None, WireUuid::ZERO));
    }

    #[test]
    fn response_helpers_preserve_topic_identity_error_and_throttle_fields() {
        let id = WireUuid([9; 16]);
        let unknown_id = delete_topic_result(None, id, codes::UNKNOWN_TOPIC_ID);
        let expected_unknown = DeletableTopicResult {
            name: None,
            topic_id: id,
            error_code: codes::UNKNOWN_TOPIC_ID,
            error_message: None,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(unknown_id == expected_unknown);

        let denied = delete_topic_result(
            Some("secret".into()),
            WireUuid::ZERO,
            codes::TOPIC_AUTHORIZATION_FAILED,
        );
        let expected_denied = DeletableTopicResult {
            name: Some("secret".into()),
            topic_id: WireUuid::ZERO,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: None,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(denied == expected_denied);

        let resp = delete_topics_response(vec![denied], 123);
        let expected_resp = DeleteTopicsResponse {
            throttle_time_ms: 123,
            responses: vec![expected_denied],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected_resp);
    }

    #[test]
    fn deleted_topic_resources_include_only_successful_named_topics() {
        let results = vec![
            delete_topic_result(Some("ok".into()), WireUuid::ZERO, codes::NONE),
            delete_topic_result(
                Some("denied".into()),
                WireUuid::ZERO,
                codes::TOPIC_AUTHORIZATION_FAILED,
            ),
            delete_topic_result(None, WireUuid([1; 16]), codes::NONE),
        ];

        let resources = deleted_topic_resources(&results);

        let expected = vec![crabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "ok".into(),
        }];
        assert!(resources == expected);
    }

    #[test]
    fn audit_deleted_topics_skips_empty_and_emits_non_empty_admin_event() {
        let (log, mut rx) = crabka_audit::AuditLog::new(8);
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        audit_deleted_topics(log.as_ref(), &ctx, Vec::new());
        assert!(
            rx.try_recv().is_err(),
            "empty audit resource list is a no-op"
        );

        audit_deleted_topics(
            log.as_ref(),
            &ctx,
            vec![crabka_audit::AuditResource {
                resource_type: "Topic".into(),
                name: "orders".into(),
            }],
        );

        let event = rx.try_recv().expect("admin audit event");
        let crabka_audit::AuditEvent::AdminOperation {
            outcome,
            principal,
            operation,
            resources,
            ..
        } = event
        else {
            panic!("expected AdminOperation");
        };
        let expected_resources = vec![crabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "orders".into(),
        }];
        check!(outcome == crabka_audit::AuditOutcome::Success);
        check!(principal.name.as_str() == "admin");
        check!(operation.as_str() == "DeleteTopics");
        check!(resources == expected_resources);
    }

    #[test]
    fn should_wait_for_quota_delay_only_waits_for_positive_delay() {
        assert!(!should_wait_for_quota_delay(Duration::ZERO));
        assert!(should_wait_for_quota_delay(Duration::from_millis(1)));
    }

    #[tokio::test]
    async fn handle_denied_topic_returns_authorization_failure() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let req = request(vec![named_state("secret")]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = DeleteTopicsResponse {
            throttle_time_ms: 0,
            responses: vec![DeletableTopicResult {
                name: Some("secret".into()),
                topic_id: WireUuid::ZERO,
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: None,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_unknown_name_and_id_preserve_error_rows() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let bogus_id = WireUuid([8; 16]);
        let req = request(vec![named_state("missing"), id_state(bogus_id)]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = DeleteTopicsResponse {
            throttle_time_ms: 0,
            responses: vec![
                DeletableTopicResult {
                    name: Some("missing".into()),
                    topic_id: WireUuid::ZERO,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: None,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
                },
                DeletableTopicResult {
                    name: None,
                    topic_id: bogus_id,
                    error_code: codes::UNKNOWN_TOPIC_ID,
                    error_message: None,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
