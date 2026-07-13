//! `OffsetFetch` (`api_key=9`). Reads from `Group.committed_offsets`.
//!
//! For v0-v7 the request carries the legacy single-group fields:
//! `group_id` + `topics: Option<Vec<OffsetFetchRequestTopic>>`. v8+ (KIP-516)
//! moves to a per-group `groups[]` array and, at v10, keys topics by
//! `topic_id`; that path is handled in `handle_groups`. Internal offset
//! storage stays name-keyed, so topic ids are resolved to names at the wire
//! boundary and echoed back on the response.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
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
        if !group_authorized(broker, ctx, &req.group_id) {
            let resp = OffsetFetchResponse {
                topics: Vec::new(),
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                throttle_time_ms: 0,
                ..Default::default()
            };
            return crate::handlers::encode_response(&resp, version);
        }
    }

    // Fetch the group's committed offsets from its actor (a classic actor is
    // created for an unknown id; offsets are protocol-agnostic, so an existing
    // actor of either kind serves `FetchCommitted` the same way).
    let committed = fetch_committed(broker, &req.group_id).await;

    // A `None` `topics` field (v ≥ 2) is the "fetch all" sentinel:
    // return every committed offset stored for this group.
    let topics_out: Vec<OffsetFetchResponseTopic> = if req.topics.is_none() {
        legacy_fetch_all(broker, ctx, &committed)
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
    crate::handlers::encode_response(&resp, version)
}

fn legacy_fetch_all(
    broker: &Broker,
    context: &crate::handlers::RequestContext<'_>,
    committed: &std::collections::HashMap<
        (String, i32),
        crate::coordinator::unified::classic_state::OffsetEntry,
    >,
) -> Vec<OffsetFetchResponseTopic> {
    let mut by_topic: std::collections::HashMap<String, Vec<OffsetFetchResponsePartition>> =
        std::collections::HashMap::new();
    for ((topic, partition), entry) in committed {
        by_topic
            .entry(topic.clone())
            .or_default()
            .push(OffsetFetchResponsePartition {
                partition_index: *partition,
                committed_offset: entry.offset.0,
                committed_leader_epoch: entry.leader_epoch,
                metadata: Some(entry.metadata.clone()),
                error_code: codes::NONE,
                ..Default::default()
            });
    }
    let names: Vec<_> = by_topic.keys().cloned().collect();
    let image = broker.controller.current_image();
    let decisions = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        context.principal,
        context.peer,
        AclOperation::Read,
        names.iter().map(String::as_str),
    );
    by_topic
        .into_iter()
        .map(|(name, mut partitions)| {
            if decisions.get(name.as_str()).copied() != Some(AuthorizationResult::Allow) {
                for partition in &mut partitions {
                    partition.committed_offset = -1;
                    partition.committed_leader_epoch = -1;
                    partition.metadata = None;
                    partition.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
                }
            }
            OffsetFetchResponseTopic {
                name,
                partitions,
                ..Default::default()
            }
        })
        .collect()
}

fn group_authorized(
    broker: &Broker,
    context: &crate::handlers::RequestContext<'_>,
    group_id: &str,
) -> bool {
    broker.config.authorizer.authorize(
        &*broker.controller.current_image(),
        &AuthorizationRequest {
            principal: context.principal,
            host: context.peer,
            resource_type: ResourceType::Group,
            resource_name: group_id,
            operation: AclOperation::Describe,
        },
    ) == AuthorizationResult::Allow
}

async fn fetch_committed(
    broker: &Broker,
    group_id: &str,
) -> std::collections::HashMap<(String, i32), crate::coordinator::unified::classic_state::OffsetEntry>
{
    let handle = broker.group_coordinator.find(group_id).unwrap_or_else(|| {
        broker
            .group_coordinator
            .get_or_create_group(group_id, GroupKindTag::Classic)
    });
    let (reply, response) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::FetchCommitted { reply })
        .await
        .is_err()
    {
        return std::collections::HashMap::new();
    }
    response.await.unwrap_or_default()
}

/// v8+ per-group fetch. Processes `req.groups` into `resp.groups`, leaving
/// `resp.topics` empty (it is only encoded for v < 8). Offset storage is
/// name-keyed, so at v10 we resolve each requested `topic_id` → name and
/// echo the id back; unknown ids return `UNKNOWN_TOPIC_ID` per partition.
// per-group loop: ACL + id→name resolve + named/fetch-all branches
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
            if !group_authorized(broker, ctx, &grp.group_id) {
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
        let committed = fetch_committed(broker, &grp.group_id).await;
        let image = broker.controller.current_image();

        // Named/id'd topics: resolve id→name (v10) and read each requested
        // partition from the name-keyed store. `None` topics → fetch-all.
        let topics_out: Vec<OffsetFetchResponseTopics> =
            if let Some(req_topics) = grp.topics.as_deref() {
                group_named_topics(broker, ctx, &image, req_topics, &committed)
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
    crate::handlers::encode_response(&resp, version)
}

fn group_named_topics(
    broker: &Broker,
    context: &crate::handlers::RequestContext<'_>,
    image: &crabka_metadata::MetadataImage,
    requested: &[crabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopics],
    committed: &std::collections::HashMap<
        (String, i32),
        crate::coordinator::unified::classic_state::OffsetEntry,
    >,
) -> Vec<OffsetFetchResponseTopics> {
    let resolved: Vec<_> = requested
        .iter()
        .map(|topic| {
            let name = if topic.topic_id == WireUuid::ZERO {
                Some(topic.name.clone())
            } else {
                image
                    .topic_name_by_id(&uuid::Uuid::from_bytes(topic.topic_id.0))
                    .map(str::to_string)
            };
            (topic, name)
        })
        .collect();
    let names: Vec<_> = resolved
        .iter()
        .filter_map(|(_, name)| name.clone())
        .collect();
    let decisions = authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        AclOperation::Read,
        names.iter().map(String::as_str),
    );
    resolved
        .into_iter()
        .map(|(topic, name)| {
            let error = match name.as_deref() {
                None => codes::UNKNOWN_TOPIC_ID,
                Some(name) if decisions.get(name).copied() != Some(AuthorizationResult::Allow) => {
                    codes::TOPIC_AUTHORIZATION_FAILED
                }
                Some(_) => codes::NONE,
            };
            let partitions = topic
                .partition_indexes
                .iter()
                .map(|partition| {
                    let entry = if error == codes::NONE {
                        name.as_ref()
                            .and_then(|name| committed.get(&(name.clone(), *partition)))
                    } else {
                        None
                    };
                    OffsetFetchResponsePartitions {
                        partition_index: *partition,
                        committed_offset: entry.map_or(-1, |value| value.offset.0),
                        committed_leader_epoch: entry.map_or(-1, |value| value.leader_epoch),
                        metadata: entry.map(|value| value.metadata.clone()),
                        error_code: error,
                        ..Default::default()
                    }
                })
                .collect();
            OffsetFetchResponseTopics {
                name: name.unwrap_or_default(),
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_log::Offset;

    use super::*;
    use crate::{
        coordinator::unified::classic_state::OffsetEntry,
        test_support::{peer, principal, start_broker_with_authorizer_no_audit as start_broker},
    };

    // Seed a committed offset for (group, topic, partition) directly on the
    // group actor via UpdateCommitted.
    async fn seed_committed_offset(
        broker: &Broker,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) {
        let h = broker
            .group_coordinator
            .get_or_create_group(group, GroupKindTag::Classic);
        let (tx, rx) = oneshot::channel();
        h.tx.send(GroupActorMessage::UpdateCommitted {
            entries: vec![(
                (topic.to_string(), partition),
                OffsetEntry {
                    offset: Offset(offset),
                    leader_epoch: 5,
                    metadata: String::new(),
                    commit_timestamp_ms: 0,
                },
            )],
            reply: tx,
        })
        .await
        .expect("send UpdateCommitted");
        rx.await.expect("UpdateCommitted ack");
    }

    // A named-topic OffsetFetch (v0–v7 path) returns the group's committed
    // offset for the requested partition. A non-zero committed offset pins
    // the committed_offset field against the struct-field-deletion mutant,
    // which would default it to 0.
    #[tokio::test]
    async fn named_topic_fetch_returns_committed_offset() {
        const VERSION: i16 = 7; // legacy single-group path (< 8)
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        seed_committed_offset(&broker, "grp", "orders", 0, 42).await;

        let p = principal("admin");
        let peer = peer();
        let ctx = crate::test_support::request_context(&p, &peer, "consumer");
        let req = OffsetFetchRequest {
            group_id: "grp".into(),
            topics: Some(vec![
                crabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopic {
                    name: "orders".into(),
                    partition_indexes: vec![0],
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        let req_bytes = crate::test_support::encode_request(&req, VERSION);

        let bytes = handle(&broker, VERSION, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp: OffsetFetchResponse = crate::test_support::decode_response(&bytes, VERSION);

        let topic = resp
            .topics
            .iter()
            .find(|t| t.name == "orders")
            .expect("orders topic row");
        let part = topic
            .partitions
            .iter()
            .find(|p| p.partition_index == 0)
            .expect("partition 0 row");
        assert!(
            part.committed_offset == 42,
            "committed_offset must echo the seeded value (42), got {}",
            part.committed_offset
        );
        broker_handle.shutdown().await;
    }
}
