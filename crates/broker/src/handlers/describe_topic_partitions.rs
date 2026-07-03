//! `DescribeTopicPartitions` (`api_key=75`, KIP-966). Paginated topic-+-partition
//! listing the JVM admin client uses for `kafka-topics --describe`
//! against Kafka 3.7+ brokers. Replaces the Metadata-fan-out the older
//! admin client used for the same job.
//!
//! ## Request shape
//!
//! - `topics`: empty → return all topics (alphabetical). Non-empty →
//!   return exactly those, in request order.
//! - `response_partition_limit`: cap on partition rows in the response.
//!   Default 2000.
//! - `cursor`: optional resume point `(topic_name, partition_index)`.
//!   When set, the response starts at that topic's partition, skipping
//!   earlier topics entirely.
//!
//! ## ACL semantics
//!
//! Per-topic `Describe` on `Topic(name)`. For *named* requests, Deny →
//! per-topic row with `error_code = TOPIC_AUTHORIZATION_FAILED (29)`.
//! For *fetch-all* requests, Deny → silently omit (matches
//! `Metadata`-fetch-all so the broker doesn't leak topic names to
//! unauthorized clients).
//!
//! ## KIP-430 integration
//!
//! Every Allow row carries `topic_authorized_operations` — the v0 schema
//! always encodes this field (no opt-in flag, unlike Metadata).

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
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
    handlers::authorized_operations::authorized_operations_bits,
};

/// Crabka's three internal topics. JVM clients display these with the
/// `is_internal` flag set so `kafka-topics --list` and friends don't
/// surface them by default.
fn is_internal_topic(name: &str) -> bool {
    matches!(
        name,
        "__consumer_offsets" | "__transaction_state" | "__remote_log_metadata"
    )
}

// Read-only handler — never suspends. The `async fn` shape matches the
// other inline-intercept handlers (DescribeCluster, DescribeGroups) so
// dispatch.rs can call it through one `await`.
#[allow(clippy::unused_async)]
#[allow(clippy::too_many_lines)] // ACL preamble + pagination + cursor logic
#[tracing::instrument(
    name = "handle_describe_topic_partitions",
    level = "info",
    skip_all,
    fields(api = "DescribeTopicPartitions", version, req_bytes = req_bytes.len()),
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
    let req = DescribeTopicPartitionsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // ── 1. Resolve the topic-name iteration order ──────────────────────
    // Named request: return every requested name, in request order, even
    // if some don't exist (those rows carry UNKNOWN_TOPIC_OR_PARTITION).
    // Fetch-all (empty `topics`): walk every topic from the image,
    // alphabetical for deterministic pagination.
    let named = !req.topics.is_empty();
    let mut ordered_names: Vec<String> = if named {
        req.topics.iter().map(|t| t.name.clone()).collect()
    } else {
        let mut all: Vec<String> = image.topics().map(|t| t.name.clone()).collect();
        all.sort();
        all
    };

    // ── 2. Apply request Cursor: skip topics before `cursor.topic_name`. ─
    let cursor_partition: i32 = match &req.cursor {
        Some(c) => {
            // Drop every name strictly before the cursor's topic. If the
            // cursor's topic isn't in the list, we keep walking from
            // wherever the binary partition lands — matches the JVM's
            // "lower bound" behavior on out-of-set cursors.
            let drop_until = c.topic_name.as_str();
            ordered_names.retain(|n| n.as_str() >= drop_until);
            c.partition_index
        }
        None => 0,
    };

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
                topics_out.push(DescribeTopicPartitionsResponseTopic {
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    name: Some(name.clone()),
                    topic_id: WireUuid::ZERO,
                    is_internal: false,
                    partitions: Vec::new(),
                    topic_authorized_operations: i32::MIN,
                    ..Default::default()
                });
            }
            // Fetch-all Deny: silently omit so the broker doesn't leak
            // topic existence to unauthorized clients.
            first_topic_partition_offset = 0;
            continue;
        }

        let topic = image.topic(name);
        let Some(t) = topic else {
            topics_out.push(DescribeTopicPartitionsResponseTopic {
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                name: Some(name.clone()),
                topic_id: WireUuid::ZERO,
                is_internal: false,
                partitions: Vec::new(),
                topic_authorized_operations: i32::MIN,
                ..Default::default()
            });
            first_topic_partition_offset = 0;
            continue;
        };

        let mut sorted_parts: Vec<_> = image.partitions_of(name).collect();
        sorted_parts.sort_by_key(|p| p.partition);

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
            row_partitions.push(DescribeTopicPartitionsResponsePartition {
                error_code: codes::NONE,
                partition_index: p.partition,
                leader_id: i32::try_from(p.leader.0).unwrap_or(i32::MAX),
                leader_epoch: p.leader_epoch.0,
                replica_nodes: p
                    .replicas
                    .iter()
                    .map(|&r| i32::try_from(r.0).unwrap_or(i32::MAX))
                    .collect(),
                isr_nodes: p
                    .isr
                    .iter()
                    .map(|&r| i32::try_from(r.0).unwrap_or(i32::MAX))
                    .collect(),
                // ELR / last-known-ELR: the schema marks both
                // `nullableVersions: 0+` and `default: null`, but the
                // Kafka 3.8 admin client's
                // `DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo`
                // calls `.stream()` on the decoded list without a null
                // guard — i.e. a null value NPEs the client. Real
                // Kafka brokers emit empty lists rather than null;
                // mirror that to stay compatible. A null value produces
                // "Cannot invoke java.util.List.stream() because
                // the return value of …eligibleLeaderReplicas() is null".
                eligible_leader_replicas: Some(Vec::new()),
                last_known_elr: Some(Vec::new()),
                offline_replicas: Vec::new(),
                ..Default::default()
            });
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
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

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
