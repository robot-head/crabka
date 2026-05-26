//! `OffsetDelete` (`api_key=47`, KIP-496). Deletes committed offsets for
//! specific (topic, partition) tuples within a consumer group. Used by
//! `kafka-consumer-groups --delete-offsets`.
//!
//! Authorization (KIP-496):
//!   - whole-response `Delete` on `Group(group_id)`
//!   - per-topic `Read` on `Topic(name)` → per-partition
//!     `TOPIC_AUTHORIZATION_FAILED` on Deny
//!
//! Semantics:
//!   - missing group → whole-response `GROUP_ID_NOT_FOUND`
//!   - missing topic / partition out of range → per-partition
//!     `UNKNOWN_TOPIC_OR_PARTITION`
//!   - group has live members AND any member's consumer-protocol
//!     subscription contains the topic → per-partition
//!     `GROUP_SUBSCRIBED_TO_TOPIC` (86)
//!   - otherwise: append a tombstone (key = `OffsetCommitKey`, value =
//!     null) to `__consumer_offsets-0`, remove the entry from
//!     `Group.committed_offsets`, per-partition `NONE`.

use std::collections::HashSet;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::consumer_protocol_subscription::ConsumerProtocolSubscription;
use crabka_protocol::owned::offset_delete_request::OffsetDeleteRequest;
use crabka_protocol::owned::offset_delete_response::{
    OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
};
use crabka_protocol::records::{Record, RecordBatch};
use crabka_protocol::{Decode, Encode};
use tokio::sync::oneshot;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::bootstrap::{OFFSETS_PARTITION, OFFSETS_TOPIC};
use crate::coordinator::group::GroupState;
use crate::coordinator::persistence::OffsetCommitValue;
use crate::error::BrokerError;
use crate::partition::{ProduceJob, WriterMessage};

#[allow(clippy::too_many_lines)] // ACL preamble + subscription guard + tombstone pipeline; splitting hurts readability
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = OffsetDeleteRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // Group `Delete` ACL — whole-response on Deny.
    let acl_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: req.group_id.as_str(),
        operation: AclOperation::Delete,
    };
    if broker.config.authorizer.authorize(&image, &acl_req) == AuthorizationResult::Deny {
        return encode(
            version,
            &whole_error(&req, codes::GROUP_AUTHORIZATION_FAILED),
        );
    }

    // Group must exist.
    let Some(group_handle) = broker.group_manager.find(&req.group_id) else {
        return encode(version, &whole_error(&req, codes::GROUP_ID_NOT_FOUND));
    };

    // Per-topic `Read` ACL — per-partition `TOPIC_AUTHORIZATION_FAILED` on Deny.
    let topic_decisions = {
        let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
        authorize_topics(
            broker.config.authorizer.as_ref(),
            &image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            topic_names,
        )
    };

    // Snapshot live subscriptions. KIP-496 only blocks deletion when a
    // *consumer-protocol* group with live members still subscribes to
    // the topic; Empty/Dead groups and non-`"consumer"` protocol_type
    // groups skip the guard.
    let subscribed_topics: HashSet<String> = {
        let g = group_handle.state.lock().await;
        if matches!(g.state, GroupState::Empty | GroupState::Dead)
            || g.protocol_type.as_deref() != Some("consumer")
        {
            HashSet::new()
        } else {
            g.members
                .values()
                .flat_map(|m| decode_subscribed_topics(&m.protocol_metadata))
                .collect()
        }
    };

    // Build per-topic/per-partition result rows and queue the tombstone
    // batch for the rows that should actually delete.
    let now_ms = now_ms();
    let mut tombstones = RecordBatch {
        max_timestamp: now_ms,
        ..RecordBatch::default()
    };
    let mut delta: i32 = 0;
    let mut topics_out: Vec<OffsetDeleteResponseTopic> = Vec::with_capacity(req.topics.len());
    let mut to_remove: Vec<(String, i32)> = Vec::new();

    for topic in &req.topics {
        let denied = topic_decisions
            .get(topic.name.as_str())
            .copied()
            .unwrap_or(AuthorizationResult::Deny)
            == AuthorizationResult::Deny;
        let topic_record = image.topic(&topic.name);
        let mut partitions_out: Vec<OffsetDeleteResponsePartition> =
            Vec::with_capacity(topic.partitions.len());

        for part in &topic.partitions {
            let code = if denied {
                codes::TOPIC_AUTHORIZATION_FAILED
            } else if subscribed_topics.contains(&topic.name) {
                codes::GROUP_SUBSCRIBED_TO_TOPIC
            } else {
                match topic_record {
                    Some(tr)
                        if part.partition_index >= 0 && part.partition_index < tr.partitions =>
                    {
                        tombstones.records.push(Record {
                            offset_delta: delta,
                            timestamp_delta: 0,
                            key: Some(OffsetCommitValue::encode_key(
                                &req.group_id,
                                &topic.name,
                                part.partition_index,
                            )),
                            value: None, // null value = tombstone
                            ..Default::default()
                        });
                        delta += 1;
                        to_remove.push((topic.name.clone(), part.partition_index));
                        codes::NONE
                    }
                    _ => codes::UNKNOWN_TOPIC_OR_PARTITION,
                }
            };
            partitions_out.push(OffsetDeleteResponsePartition {
                partition_index: part.partition_index,
                error_code: code,
                ..Default::default()
            });
        }

        topics_out.push(OffsetDeleteResponseTopic {
            name: topic.name.clone(),
            partitions: partitions_out,
            ..Default::default()
        });
    }

    if !tombstones.records.is_empty() {
        tombstones.last_offset_delta = (delta - 1).max(0);
        if let Err(code) = append_tombstones(broker, tombstones).await {
            return encode(version, &rewrite_success_as(topics_out, code));
        }
        let mut g = group_handle.state.lock().await;
        for key in &to_remove {
            g.committed_offsets.remove(key);
        }
    }

    let resp = OffsetDeleteResponse {
        error_code: codes::NONE,
        throttle_time_ms: 0,
        topics: topics_out,
        ..Default::default()
    };
    encode(version, &resp)
}

fn whole_error(req: &OffsetDeleteRequest, code: i16) -> OffsetDeleteResponse {
    OffsetDeleteResponse {
        error_code: code,
        throttle_time_ms: 0,
        topics: req
            .topics
            .iter()
            .map(|t| OffsetDeleteResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| OffsetDeleteResponsePartition {
                        partition_index: p.partition_index,
                        error_code: code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn rewrite_success_as(topics: Vec<OffsetDeleteResponseTopic>, code: i16) -> OffsetDeleteResponse {
    let topics = topics
        .into_iter()
        .map(|mut t| {
            for p in &mut t.partitions {
                if p.error_code == codes::NONE {
                    p.error_code = code;
                }
            }
            t
        })
        .collect();
    OffsetDeleteResponse {
        error_code: codes::NONE,
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    }
}

async fn append_tombstones(broker: &Broker, batch: RecordBatch) -> Result<(), i16> {
    let Some(part_handle) = broker
        .partitions
        .get(&(OFFSETS_TOPIC.to_string(), OFFSETS_PARTITION))
        .map(|e| e.value().clone())
    else {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    };
    let (ack_tx, ack_rx) = oneshot::channel();
    if part_handle
        .writer_tx
        .send(WriterMessage::Produce(ProduceJob { batch, ack: ack_tx }))
        .await
        .is_err()
    {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    }
    match ack_rx.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "OffsetDelete writer returned error");
            Err(codes::from_broker_error(&e))
        }
        Err(e) => {
            tracing::error!(error = %e, "OffsetDelete writer ack dropped");
            Err(codes::UNKNOWN_SERVER_ERROR)
        }
    }
}

/// Decode the `topics` list from a member's `protocol_metadata` blob.
/// The blob carries a leading `i16` version (the "consumer" protocol's
/// version negotiation, separate from the `ConsumerProtocolSubscription`
/// schema's per-field version gates) followed by the schema body. Returns
/// an empty list on any decode error — best-effort, since a malformed
/// subscription would otherwise silently let stale offsets be deleted.
fn decode_subscribed_topics(metadata: &[u8]) -> Vec<String> {
    use bytes::Buf;
    if metadata.len() < 2 {
        return Vec::new();
    }
    let mut cur = metadata;
    let version = cur.get_i16();
    if !(0..=3).contains(&version) {
        return Vec::new();
    }
    ConsumerProtocolSubscription::decode(&mut cur, version)
        .map(|s| s.topics)
        .unwrap_or_default()
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

fn encode(version: i16, resp: &OffsetDeleteResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use crabka_protocol::owned::consumer_protocol_subscription::ConsumerProtocolSubscription;

    fn encode_subscription(topics: &[&str]) -> Vec<u8> {
        let sub = ConsumerProtocolSubscription {
            topics: topics.iter().map(|s| (*s).to_string()).collect(),
            ..Default::default()
        };
        let mut out = bytes::BytesMut::new();
        out.put_i16(0); // protocol version negotiation prefix
        sub.encode(&mut out, 0).unwrap();
        out.to_vec()
    }

    #[test]
    fn decode_subscription_extracts_topic_names() {
        let bytes = encode_subscription(&["foo", "bar"]);
        let got = decode_subscribed_topics(&bytes);
        assert_eq!(got, vec!["foo".to_string(), "bar".to_string()]);
    }

    #[test]
    fn decode_subscription_empty_input_is_empty() {
        assert!(decode_subscribed_topics(&[]).is_empty());
    }

    #[test]
    fn decode_subscription_short_input_is_empty() {
        assert!(decode_subscribed_topics(&[0u8]).is_empty());
    }

    #[test]
    fn decode_subscription_rejects_out_of_range_version() {
        // Version 99 is not a known ConsumerProtocolSubscription version.
        let bytes = vec![0u8, 99u8];
        assert!(decode_subscribed_topics(&bytes).is_empty());
    }

    #[test]
    fn decode_subscription_malformed_body_returns_empty() {
        // Valid version prefix, but truncated body → decode fails.
        let bytes = vec![0u8, 0u8];
        assert!(decode_subscribed_topics(&bytes).is_empty());
    }
}
