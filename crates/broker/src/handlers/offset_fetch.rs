//! `OffsetFetch` (`api_key=9`). Reads from `Group.committed_offsets`.
//!
//! For v0-v7 the request carries the legacy single-group fields:
//! `group_id` + `topics: Option<Vec<OffsetFetchRequestTopic>>`. v8+ moved
//! to a per-group array; for the MVP we ignore the `groups` array and only
//! serve the legacy single-group shape.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::offset_fetch_request::OffsetFetchRequest;
use crabka_protocol::owned::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

#[allow(clippy::too_many_lines)] // ACL preamble (group + per-topic) + fetch-all vs named-topic branches; splitting hurts readability
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = OffsetFetchRequest::decode(&mut cur, version)?;

    // ── slice-13 ACL preamble ────────────────────────────────────────────
    // Step 1: `Describe` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
    {
        let image = broker.controller.current_image();
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: req.group_id.as_str(),
            operation: AclOperation::Describe,
        };
        if broker.config.authorizer.authorize(&image, &acl_req) == AuthorizationResult::Deny {
            let resp = OffsetFetchResponse {
                topics: Vec::new(),
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                throttle_time_ms: 0,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }
    }

    let handle = broker.group_manager.get_or_create(&req.group_id);
    let g = handle.state.lock().await;

    // A `None` `topics` field (v ≥ 2) is the "fetch all" sentinel:
    // return every committed offset stored for this group.
    let topics_out: Vec<OffsetFetchResponseTopic> = if req.topics.is_none() {
        // Aggregate all committed offsets grouped by topic name.
        let mut by_topic: std::collections::HashMap<String, Vec<OffsetFetchResponsePartition>> =
            std::collections::HashMap::new();
        for ((topic, pid), entry) in &g.committed_offsets {
            by_topic
                .entry(topic.clone())
                .or_default()
                .push(OffsetFetchResponsePartition {
                    partition_index: *pid,
                    committed_offset: entry.offset,
                    committed_leader_epoch: entry.leader_epoch,
                    metadata: Some(entry.metadata.clone()),
                    error_code: codes::NONE,
                    ..Default::default()
                });
        }

        // ── slice-13 ACL preamble ─────────────────────────────────────
        // Step 2 (fetch-all): `Read` on each discovered topic. On Deny →
        // per-topic `error_code = TOPIC_AUTHORIZATION_FAILED (29)`.
        let discovered_topics: Vec<String> = by_topic.keys().cloned().collect();
        let topic_decisions = {
            let image = broker.controller.current_image();
            authorize_topics(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
                AclOperation::Read,
                discovered_topics.iter().map(String::as_str),
            )
        };

        by_topic
            .into_iter()
            .map(|(name, partitions)| {
                let denied = topic_decisions
                    .get(name.as_str())
                    .copied()
                    .unwrap_or(AuthorizationResult::Deny)
                    == AuthorizationResult::Deny;
                if denied {
                    // Return the topic with TOPIC_AUTHORIZATION_FAILED on each partition.
                    OffsetFetchResponseTopic {
                        name,
                        partitions: partitions
                            .into_iter()
                            .map(|p| OffsetFetchResponsePartition {
                                partition_index: p.partition_index,
                                committed_offset: -1,
                                committed_leader_epoch: -1,
                                metadata: None,
                                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }
                } else {
                    OffsetFetchResponseTopic {
                        name,
                        partitions,
                        ..Default::default()
                    }
                }
            })
            .collect()
    } else {
        let req_topics = req.topics.as_deref().unwrap_or(&[]);

        // ── slice-13 ACL preamble ─────────────────────────────────────
        // Step 2 (named topics): `Read` on each requested topic. On Deny →
        // per-topic `error_code = TOPIC_AUTHORIZATION_FAILED (29)`.
        let topic_decisions = {
            let image = broker.controller.current_image();
            authorize_topics(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
                AclOperation::Read,
                req_topics.iter().map(|t| t.name.as_str()),
            )
        };

        req_topics
            .iter()
            .map(|t| {
                let denied = topic_decisions
                    .get(t.name.as_str())
                    .copied()
                    .unwrap_or(AuthorizationResult::Deny)
                    == AuthorizationResult::Deny;
                if denied {
                    // Return all partitions with TOPIC_AUTHORIZATION_FAILED.
                    let partitions = t
                        .partition_indexes
                        .iter()
                        .map(|&pid| OffsetFetchResponsePartition {
                            partition_index: pid,
                            committed_offset: -1,
                            committed_leader_epoch: -1,
                            metadata: None,
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                            ..Default::default()
                        })
                        .collect();
                    OffsetFetchResponseTopic {
                        name: t.name.clone(),
                        partitions,
                        ..Default::default()
                    }
                } else {
                    let partitions = t
                        .partition_indexes
                        .iter()
                        .map(
                            |&pid| match g.committed_offsets.get(&(t.name.clone(), pid)) {
                                Some(entry) => OffsetFetchResponsePartition {
                                    partition_index: pid,
                                    committed_offset: entry.offset,
                                    committed_leader_epoch: entry.leader_epoch,
                                    metadata: Some(entry.metadata.clone()),
                                    error_code: codes::NONE,
                                    ..Default::default()
                                },
                                None => OffsetFetchResponsePartition {
                                    partition_index: pid,
                                    committed_offset: -1,
                                    committed_leader_epoch: -1,
                                    metadata: None,
                                    error_code: codes::NONE,
                                    ..Default::default()
                                },
                            },
                        )
                        .collect();
                    OffsetFetchResponseTopic {
                        name: t.name.clone(),
                        partitions,
                        ..Default::default()
                    }
                }
            })
            .collect()
    };

    let resp = OffsetFetchResponse {
        topics: topics_out,
        error_code: codes::NONE,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
