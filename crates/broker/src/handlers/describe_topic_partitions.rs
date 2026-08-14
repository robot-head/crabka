//! `DescribeTopicPartitions` (`api_key=75`, KIP-966) lists topics and their
//! partitions in pages.
//!
//! The JVM admin client uses this API for `kafka-topics --describe` against
//! Kafka 3.7+ brokers. It replaces the Metadata fan-out that the older admin
//! client used for the same job.
//!
//! ## Request shape
//!
//! - `topics`: if empty, the broker returns all topics in alphabetical order.
//!   If not empty, the broker returns exactly those topics, in request order.
//! - `response_partition_limit`: the maximum number of partition rows in the
//!   response. Default 2000.
//! - `cursor`: an optional resume point `(topic_name, partition_index)`. If
//!   set, the response starts at that topic's partition and the broker skips
//!   all earlier topics.
//!
//! ## ACL semantics
//!
//! The broker checks `Describe` on `Topic(name)` for each topic. For a *named*
//! request, a Deny gives a topic row with
//! `error_code = TOPIC_AUTHORIZATION_FAILED (29)`. For a *fetch-all* request, a
//! Deny makes the broker omit the topic. This matches `Metadata` fetch-all, so
//! the broker does not leak topic names to unauthorized clients.
//!
//! ## KIP-430 integration
//!
//! Every Allow row carries `topic_authorized_operations`. The v0 schema always
//! encodes this field. Metadata has an opt-in flag for it, but this API does
//! not.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        describe_topic_partitions_request::DescribeTopicPartitionsRequest,
        describe_topic_partitions_response::{
            Cursor as ResponseCursor, DescribeTopicPartitionsResponse,
            DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{authorized_operations::authorized_operations_bits, is_internal_topic},
};

// Read-only handler — never suspends. The `async fn` shape matches the
// other inline-intercept handlers (DescribeCluster, DescribeGroups) so
// dispatch.rs can call it through one `await`.
// ACL preamble + pagination + cursor logic
#[tracing::instrument(
    name = "handle_describe_topic_partitions",
    level = "info",
    skip_all,
    fields(api = "DescribeTopicPartitions", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DescribeTopicPartitionsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // ── 1. Resolve the topic-name iteration order ──────────────────────
    // Named request: return every requested name, in request order, even
    // if some don't exist (those rows carry UNKNOWN_TOPIC_OR_PARTITION).
    // Fetch-all (empty `topics`): walk every topic from the image,
    // alphabetical for deterministic pagination.
    let (named, ordered_names, cursor_partition) = resolve_names(&image, &req);

    // ── 3. Batch-authorize Describe on all candidate topics. ───────────
    let acl_by_name = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Describe,
        ordered_names.iter().map(String::as_str),
    );

    // ── 4. Walk topics, building rows under the partition-limit budget. ─
    let partition_limit = req.response_partition_limit.max(0);
    let mut emitted_partitions: i32 = 0;
    let mut topics_out: Vec<DescribeTopicPartitionsResponseTopic> =
        Vec::with_capacity(ordered_names.len());
    let mut next_cursor: Option<ResponseCursor> = None;

    // Apply the request cursor's partition_index only to the first topic
    // we process (the resume topic); every subsequent topic starts at
    // partition 0.
    let mut first_topic_partition_offset = cursor_partition;

    for name in &ordered_names {
        let allowed = acl_by_name
            .get(name.as_str())
            .copied()
            .unwrap_or(AuthorizationResult::Deny)
            == AuthorizationResult::Allow;
        if !allowed {
            if named {
                topics_out.push(error_topic(name, codes::TOPIC_AUTHORIZATION_FAILED));
            }
            // Fetch-all Deny: silently omit so the broker doesn't leak
            // topic existence to unauthorized clients.
            first_topic_partition_offset = 0;
            continue;
        }

        let topic = image.topic(name);
        let Some(t) = topic else {
            topics_out.push(error_topic(name, codes::UNKNOWN_TOPIC_OR_PARTITION));
            first_topic_partition_offset = 0;
            continue;
        };

        // `partitions_of` yields ascending partition-index order — the
        // order the cursor pagination below depends on.
        let mut sorted_parts: Vec<_> = image.partitions_of(name).collect();

        // Skip partitions before the cursor's `partition_index` on the
        // resume-topic only. `cursor_partition = 0` is a no-op skip.
        if first_topic_partition_offset > 0 {
            sorted_parts.retain(|p| p.partition >= first_topic_partition_offset);
        }
        // Reset the cursor offset; future topics in this response start
        // from partition 0.
        first_topic_partition_offset = 0;

        let mut row_partitions: Vec<DescribeTopicPartitionsResponsePartition> =
            Vec::with_capacity(sorted_parts.len());
        let mut truncated = false;
        let mut next_partition_index: i32 = 0;
        for p in &sorted_parts {
            if emitted_partitions >= partition_limit {
                truncated = true;
                next_partition_index = p.partition;
                break;
            }
            row_partitions.push(partition_response(p));
            emitted_partitions += 1;
        }

        // KIP-430: the v0 schema always encodes the bitfield, no opt-in
        // flag exists for this API. Always populate via the shared helper.
        let topic_authorized_operations = authorized_operations_bits(
            broker.config.authorizer.as_ref(),
            &image,
            ctx.principal,
            ctx.peer,
            ResourceType::Topic,
            name.as_str(),
        );

        topics_out.push(DescribeTopicPartitionsResponseTopic {
            error_code: codes::NONE,
            name: Some(name.clone()),
            topic_id: WireUuid(t.topic_id.into_bytes()),
            is_internal: is_internal_topic(name),
            partitions: row_partitions,
            topic_authorized_operations,
            ..Default::default()
        });

        if truncated {
            next_cursor = Some(ResponseCursor {
                topic_name: name.clone(),
                partition_index: next_partition_index,
                ..Default::default()
            });
            break;
        }
    }

    let resp = DescribeTopicPartitionsResponse {
        throttle_time_ms: 0,
        topics: topics_out,
        next_cursor,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

fn partition_response(
    partition: &crabka_metadata::PartitionRecord,
) -> DescribeTopicPartitionsResponsePartition {
    DescribeTopicPartitionsResponsePartition {
        error_code: codes::NONE,
        partition_index: partition.partition,
        leader_id: i32::try_from(partition.leader.0).unwrap_or(i32::MAX),
        leader_epoch: partition.leader_epoch.0,
        replica_nodes: partition
            .replicas
            .iter()
            .map(|&replica| i32::try_from(replica.0).unwrap_or(i32::MAX))
            .collect(),
        isr_nodes: partition
            .isr
            .iter()
            .map(|&replica| i32::try_from(replica.0).unwrap_or(i32::MAX))
            .collect(),
        // Kafka clients assume these nullable lists are present.
        eligible_leader_replicas: Some(Vec::new()),
        last_known_elr: Some(Vec::new()),
        offline_replicas: Vec::new(),
        ..Default::default()
    }
}

fn error_topic(name: &str, error_code: i16) -> DescribeTopicPartitionsResponseTopic {
    DescribeTopicPartitionsResponseTopic {
        error_code,
        name: Some(name.to_string()),
        topic_id: WireUuid::ZERO,
        is_internal: false,
        partitions: Vec::new(),
        topic_authorized_operations: i32::MIN,
        ..Default::default()
    }
}

fn resolve_names(
    image: &crabka_metadata::MetadataImage,
    req: &DescribeTopicPartitionsRequest,
) -> (bool, Vec<String>, i32) {
    let named = !req.topics.is_empty();
    let mut ordered_names: Vec<String> = if named {
        req.topics.iter().map(|topic| topic.name.clone()).collect()
    } else {
        let mut all_topics: Vec<_> = image.topics().map(|topic| topic.name.clone()).collect();
        all_topics.sort();
        all_topics
    };
    let cursor_partition = req.cursor.as_ref().map_or(0, |cursor| {
        ordered_names.retain(|candidate| candidate.as_str() >= cursor.topic_name.as_str());
        cursor.partition_index
    });
    (named, ordered_names, cursor_partition)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord};

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{peer, principal},
    };

    const VERSION: i16 = crabka_protocol::owned::describe_topic_partitions_response::MAX_VERSION;

    crate::test_support::wire_helpers!(
        DescribeTopicPartitionsRequest,
        DescribeTopicPartitionsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    async fn seed_topic_with_epoch(handle: &BrokerHandle, leader_epoch: i32) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "orders".into(),
                    topic_id: uuid::Uuid::from_u128(1),
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "orders".into(),
                    partition: 0,
                    leader: NodeId(1),
                    replicas: vec![NodeId(1)],
                    isr: vec![NodeId(1)],
                    leader_epoch: crabka_metadata::LeaderEpoch(leader_epoch),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![uuid::Uuid::nil()],
                    partition_epoch: 3,
                }),
            ])
            .await
            .expect("seed topic + partition");
    }

    /// The response partition echoes the `leader_epoch` of the metadata image
    /// exactly (KIP-320). A non-zero epoch pins the field against the
    /// struct-field-deletion mutant, which would set it to 0.
    #[tokio::test]
    async fn response_partition_carries_leader_epoch() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic_with_epoch(&broker_handle, 9).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let req = encode_request(&DescribeTopicPartitionsRequest {
            topics: vec![
                crabka_protocol::owned::describe_topic_partitions_request::TopicRequest {
                    name: "orders".into(),
                    ..Default::default()
                },
            ],
            response_partition_limit: 2000,
            ..Default::default()
        });

        let bytes = handle(&broker, VERSION, 123, &req, &ctx).expect("handle");
        let resp = decode_response(&bytes);

        let topic = resp
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some("orders"))
            .expect("orders topic row");
        let part = topic
            .partitions
            .iter()
            .find(|p| p.partition_index == 0)
            .expect("partition 0 row");
        assert!(
            part.leader_epoch == 9,
            "response must echo the image leader_epoch (9), got {}",
            part.leader_epoch
        );
        broker_handle.shutdown().await;
    }

    #[test]
    fn is_internal_topic_matches_known_internal_names() {
        for (name, want) in [
            ("__consumer_offsets", true),
            ("__transaction_state", true),
            ("__remote_log_metadata", true),
            ("foo", false),
            ("_foo", false),
            ("__user_topic", false),
            // No accidental prefix matching.
            ("__consumer_offsets-2", false),
        ] {
            assert!(is_internal_topic(name) == want, "{name}");
        }
    }
}
