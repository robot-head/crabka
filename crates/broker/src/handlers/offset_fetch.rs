//! `OffsetFetch` (`api_key=9`). Reads from `Group.committed_offsets`.
//!
//! For v0-v7 the request carries the legacy single-group fields:
//! `group_id` + `topics: Option<Vec<OffsetFetchRequestTopic>>`. v8+ (KIP-516)
//! moves to a per-group `groups[]` array and, at v10, keys topics by
//! `topic_id`; that path is handled in `handle_groups`. Internal offset
//! storage stays name-keyed, so topic ids are resolved to names at the wire
//! boundary and echoed back on the response.

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        offset_fetch_request::OffsetFetchRequest,
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
            OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::unified::actor::{GroupActorMessage, GroupKindTag},
    error::BrokerError,
};

#[allow(clippy::too_many_lines)]
// ACL preamble (group + per-topic) + fetch-all vs named-topic branches; splitting hurts readability
#[tracing::instrument(
    name = "handle_offset_fetch",
    level = "info",
    skip_all,
    fields(api = "OffsetFetch", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = OffsetFetchRequest::decode(&mut cur, version)?;

    // ── KIP-516 (v8+): per-group `groups[]` request/response shape ──
    // v8 moved from a single (group_id, topics) pair to an array of
    // groups, and v10 keys topics by `topic_id`. Internal offset storage
    // stays name-keyed, so resolve id→name at the boundary and echo the
    // id back. The legacy v0–v7 single-group path is preserved below.
    if version >= 8 {
        return handle_groups(broker, version, &req, ctx).await;
    }

    // ── ACL preamble ────────────────────────────────────────────
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
        if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
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

    // Fetch the group's committed offsets from its actor (a classic actor is
    // created for an unknown id; offsets are protocol-agnostic, so an existing
    // actor of either kind serves `FetchCommitted` the same way).
    let committed = {
        let h = broker
            .group_coordinator
            .find(&req.group_id)
            .unwrap_or_else(|| {
                broker
                    .group_coordinator
                    .get_or_create_group(&req.group_id, GroupKindTag::Classic)
            });
        let (tx, rx) = oneshot::channel();
        if h.tx
            .send(GroupActorMessage::FetchCommitted { reply: tx })
            .await
            .is_ok()
        {
            rx.await.unwrap_or_default()
        } else {
            std::collections::HashMap::new()
        }
    };

    // A `None` `topics` field (v ≥ 2) is the "fetch all" sentinel:
    // return every committed offset stored for this group.
    let topics_out: Vec<OffsetFetchResponseTopic> = if req.topics.is_none() {
        // Aggregate all committed offsets grouped by topic name.
        let mut by_topic: std::collections::HashMap<String, Vec<OffsetFetchResponsePartition>> =
            std::collections::HashMap::new();
        for ((topic, pid), entry) in &committed {
            by_topic
                .entry(topic.clone())
                .or_default()
                .push(OffsetFetchResponsePartition {
                    partition_index: *pid,
                    committed_offset: entry.offset.0,
                    committed_leader_epoch: entry.leader_epoch,
                    metadata: Some(entry.metadata.clone()),
                    error_code: codes::NONE,
                    ..Default::default()
                });
        }

        // ── ACL preamble ─────────────────────────────────────
        // Step 2 (fetch-all): `Read` on each discovered topic. On Deny →
        // per-topic `error_code = TOPIC_AUTHORIZATION_FAILED (29)`.
        let discovered_topics: Vec<String> = by_topic.keys().cloned().collect();
        let topic_decisions = {
            let image = broker.controller.current_image();
            authorize_topics(
                broker.config.authorizer.as_ref(),
                &*image,
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

        // ── ACL preamble ─────────────────────────────────────
        // Step 2 (named topics): `Read` on each requested topic. On Deny →
        // per-topic `error_code = TOPIC_AUTHORIZATION_FAILED (29)`.
        let topic_decisions = {
            let image = broker.controller.current_image();
            authorize_topics(
                broker.config.authorizer.as_ref(),
                &*image,
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
                        .map(|&pid| match committed.get(&(t.name.clone(), pid)) {
                            Some(entry) => OffsetFetchResponsePartition {
                                partition_index: pid,
                                committed_offset: entry.offset.0,
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
                        })
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

/// v8+ per-group fetch. Processes `req.groups` into `resp.groups`, leaving
/// `resp.topics` empty (it is only encoded for v < 8). Offset storage is
/// name-keyed, so at v10 we resolve each requested `topic_id` → name and
/// echo the id back; unknown ids return `UNKNOWN_TOPIC_ID` per partition.
#[allow(clippy::too_many_lines)] // per-group loop: ACL + id→name resolve + named/fetch-all branches
async fn handle_groups(
    broker: &Broker,
    version: i16,
    req: &OffsetFetchRequest,
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut groups_out: Vec<OffsetFetchResponseGroup> = Vec::with_capacity(req.groups.len());

    for grp in &req.groups {
        // ── ACL: `Describe` on `Group(group_id)` ────────────────
        {
            let image = broker.controller.current_image();
            let acl_req = AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::Group,
                resource_name: grp.group_id.as_str(),
                operation: AclOperation::Describe,
            };
            if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
                groups_out.push(OffsetFetchResponseGroup {
                    group_id: grp.group_id.clone(),
                    topics: Vec::new(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    ..Default::default()
                });
                continue;
            }
        }

        // Fetch the group's committed offsets from its actor (a classic actor
        // is created for an unknown id; offsets are protocol-agnostic, so an
        // existing actor of either kind serves `FetchCommitted` the same way).
        let committed = {
            let h = broker
                .group_coordinator
                .find(&grp.group_id)
                .unwrap_or_else(|| {
                    broker
                        .group_coordinator
                        .get_or_create_group(&grp.group_id, GroupKindTag::Classic)
                });
            let (tx, rx) = oneshot::channel();
            if h.tx
                .send(GroupActorMessage::FetchCommitted { reply: tx })
                .await
                .is_ok()
            {
                rx.await.unwrap_or_default()
            } else {
                std::collections::HashMap::new()
            }
        };
        let image = broker.controller.current_image();

        // Named/id'd topics: resolve id→name (v10) and read each requested
        // partition from the name-keyed store. `None` topics → fetch-all.
        let topics_out: Vec<OffsetFetchResponseTopics> =
            if let Some(req_topics) = grp.topics.as_deref() {
                // Resolve each requested topic to a name first (id→name at
                // v10); an unknown id is flagged so it short-circuits to
                // UNKNOWN_TOPIC_ID without an ACL lookup.
                let resolved: Vec<(&_, Option<String>)> = req_topics
                    .iter()
                    .map(|t| {
                        let name = if t.topic_id == WireUuid::ZERO {
                            Some(t.name.clone())
                        } else {
                            image
                                .topic_name_by_id(&uuid::Uuid::from_bytes(t.topic_id.0))
                                .map(str::to_string)
                        };
                        (t, name)
                    })
                    .collect();

                // ── ACL: `Read` on each resolved topic. On Deny → per-partition
                // TOPIC_AUTHORIZATION_FAILED (mirrors the v0–v7 path). The
                // names are collected into an owned Vec so the decisions map
                // doesn't borrow `resolved` (which is consumed below).
                let auth_names: Vec<String> =
                    resolved.iter().filter_map(|(_, n)| n.clone()).collect();
                let decisions = authorize_topics(
                    broker.config.authorizer.as_ref(),
                    &*image,
                    ctx.principal,
                    ctx.peer,
                    AclOperation::Read,
                    auth_names.iter().map(String::as_str),
                );

                resolved
                    .into_iter()
                    .map(|(t, name)| {
                        let Some(name) = name else {
                            // Unknown id → UNKNOWN_TOPIC_ID per partition.
                            return OffsetFetchResponseTopics {
                                name: String::new(),
                                topic_id: t.topic_id,
                                partitions: t
                                    .partition_indexes
                                    .iter()
                                    .map(|&pid| OffsetFetchResponsePartitions {
                                        partition_index: pid,
                                        committed_offset: -1,
                                        committed_leader_epoch: -1,
                                        metadata: None,
                                        error_code: codes::UNKNOWN_TOPIC_ID,
                                        ..Default::default()
                                    })
                                    .collect(),
                                ..Default::default()
                            };
                        };

                        let denied = decisions
                            .get(name.as_str())
                            .copied()
                            .unwrap_or(AuthorizationResult::Deny)
                            == AuthorizationResult::Deny;

                        let partitions = t
                            .partition_indexes
                            .iter()
                            .map(|&pid| {
                                if denied {
                                    return OffsetFetchResponsePartitions {
                                        partition_index: pid,
                                        committed_offset: -1,
                                        committed_leader_epoch: -1,
                                        metadata: None,
                                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                                        ..Default::default()
                                    };
                                }
                                match committed.get(&(name.clone(), pid)) {
                                    Some(entry) => OffsetFetchResponsePartitions {
                                        partition_index: pid,
                                        committed_offset: entry.offset.0,
                                        committed_leader_epoch: entry.leader_epoch,
                                        metadata: Some(entry.metadata.clone()),
                                        error_code: codes::NONE,
                                        ..Default::default()
                                    },
                                    None => OffsetFetchResponsePartitions {
                                        partition_index: pid,
                                        committed_offset: -1,
                                        committed_leader_epoch: -1,
                                        metadata: None,
                                        error_code: codes::NONE,
                                        ..Default::default()
                                    },
                                }
                            })
                            .collect();

                        OffsetFetchResponseTopics {
                            name,
                            topic_id: t.topic_id,
                            partitions,
                            ..Default::default()
                        }
                    })
                    .collect()
            } else {
                // fetch-all: every committed offset for the group, grouped by
                // topic name. Echo each topic's id (required at v10, where the
                // name is dropped from the wire) and authorize Read per topic.
                let mut by_topic: std::collections::HashMap<
                    String,
                    Vec<OffsetFetchResponsePartitions>,
                > = std::collections::HashMap::new();
                for ((topic, pid), entry) in &committed {
                    by_topic.entry(topic.clone()).or_default().push(
                        OffsetFetchResponsePartitions {
                            partition_index: *pid,
                            committed_offset: entry.offset.0,
                            committed_leader_epoch: entry.leader_epoch,
                            metadata: Some(entry.metadata.clone()),
                            error_code: codes::NONE,
                            ..Default::default()
                        },
                    );
                }

                let discovered: Vec<String> = by_topic.keys().cloned().collect();
                let decisions = authorize_topics(
                    broker.config.authorizer.as_ref(),
                    &*image,
                    ctx.principal,
                    ctx.peer,
                    AclOperation::Read,
                    discovered.iter().map(String::as_str),
                );

                by_topic
                    .into_iter()
                    .map(|(name, partitions)| {
                        let topic_id = image
                            .topic(&name)
                            .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()));
                        let denied = decisions
                            .get(name.as_str())
                            .copied()
                            .unwrap_or(AuthorizationResult::Deny)
                            == AuthorizationResult::Deny;
                        if denied {
                            OffsetFetchResponseTopics {
                                name,
                                topic_id,
                                partitions: partitions
                                    .into_iter()
                                    .map(|p| OffsetFetchResponsePartitions {
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
                            OffsetFetchResponseTopics {
                                name,
                                topic_id,
                                partitions,
                                ..Default::default()
                            }
                        }
                    })
                    .collect()
            };

        groups_out.push(OffsetFetchResponseGroup {
            group_id: grp.group_id.clone(),
            topics: topics_out,
            error_code: codes::NONE,
            ..Default::default()
        });
    }

    let resp = OffsetFetchResponse {
        topics: Vec::new(),
        error_code: codes::NONE,
        throttle_time_ms: 0,
        groups: groups_out,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
