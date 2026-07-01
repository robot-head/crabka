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
use crate::coordinator::persistence::OffsetCommitValue;
use crate::coordinator::unified::actor::GroupActorMessage;
use crate::coordinator::unified::classic_state::GroupState;
use crate::error::BrokerError;
use crate::partition::{ProduceData, ProduceJob, WriterMessage};

#[allow(clippy::too_many_lines)] // ACL preamble + subscription guard + tombstone pipeline; splitting hurts readability
#[tracing::instrument(
    name = "handle_offset_delete",
    level = "info",
    skip_all,
    fields(api = "OffsetDelete", version, req_bytes = req_bytes.len()),
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
    if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
        return encode(
            version,
            &whole_error(&req, codes::GROUP_AUTHORIZATION_FAILED),
        );
    }

    // Group must exist.
    let Some(group_handle) = broker.group_coordinator.find(&req.group_id) else {
        return encode(version, &whole_error(&req, codes::GROUP_ID_NOT_FOUND));
    };

    // Per-topic `Read` ACL — per-partition `TOPIC_AUTHORIZATION_FAILED` on Deny.
    let topic_decisions = {
        let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
        authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            topic_names,
        )
    };

    // Snapshot live subscriptions. KIP-496 only blocks deletion when a
    // *consumer-protocol* classic group with live members still subscribes to
    // the topic; Empty/Dead groups, non-`"consumer"` protocol_type groups, and
    // next-gen consumer groups skip the guard.
    // `ClassicInspect` dispatches on the actor's LIVE `group.kind`: it replies
    // ONLY for a classic-kind group and drops the sender for a consumer-kind
    // group, so the `&& let Ok(view) = rx.await` guard yields the empty set for
    // a consumer group (including an UPGRADED one) without consulting the stale
    // spawn-time `handle.kind`. A KIP-848 downgrade leaves the group classic, so
    // its `ClassicInspect` view is the one that matters.
    let subscribed_topics: HashSet<String> = {
        let (tx, rx) = oneshot::channel();
        if group_handle
            .tx
            .send(GroupActorMessage::ClassicInspect { reply: tx })
            .await
            .is_ok()
            && let Ok(view) = rx.await
            && !matches!(view.state, GroupState::Empty | GroupState::Dead)
            && view.protocol_type.as_deref() == Some("consumer")
        {
            view.members
                .iter()
                .flat_map(|m| decode_subscribed_topics(&m.protocol_metadata))
                .collect()
        } else {
            HashSet::new()
        }
    };

    // Build per-topic/per-partition result rows and queue the tombstone
    // batch for the rows that should actually delete.
    let topic_partition_counts: std::collections::HashMap<&str, i32> = req
        .topics
        .iter()
        .filter_map(|t| {
            image
                .topic(&t.name)
                .map(|tr| (t.name.as_str(), tr.partitions))
        })
        .collect();
    let (topics_out, tombstone_records, to_remove) = build_response_rows(
        &req.group_id,
        &req.topics,
        &topic_decisions,
        &subscribed_topics,
        &topic_partition_counts,
    );

    if !tombstone_records.is_empty() {
        let last_offset_delta =
            i32::try_from(tombstone_records.len().saturating_sub(1)).unwrap_or(i32::MAX);
        let tombstones = RecordBatch {
            max_timestamp: now_ms(),
            last_offset_delta,
            records: tombstone_records,
            ..RecordBatch::default()
        };
        if let Err(code) = append_tombstones(broker, tombstones).await {
            return encode(version, &rewrite_success_as(topics_out, code));
        }
        let (tx, rx) = oneshot::channel();
        if group_handle
            .tx
            .send(GroupActorMessage::RemoveCommitted {
                keys: to_remove,
                reply: tx,
            })
            .await
            .is_ok()
        {
            let _ = rx.await;
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

/// Pure helper: build the per-topic / per-partition response rows for a
/// decoded `OffsetDeleteRequest`, plus the tombstone records that need to
/// be appended to `__consumer_offsets-0` and the `(topic, partition)`
/// keys to remove from in-memory `Group.committed_offsets` once the
/// append succeeds.
///
/// Branching matches KIP-496:
///   - `topic_decisions[name] == Deny` → per-partition
///     `TOPIC_AUTHORIZATION_FAILED` for every partition in the topic.
///   - `subscribed_topics.contains(name)` →
///     `GROUP_SUBSCRIBED_TO_TOPIC` for every partition in the topic.
///   - `topic_partition_counts[name]` absent or `partition_index` out of
///     range → `UNKNOWN_TOPIC_OR_PARTITION`.
///   - otherwise → tombstone queued, `NONE` returned.
fn build_response_rows(
    group_id: &str,
    topics: &[crabka_protocol::owned::offset_delete_request::OffsetDeleteRequestTopic],
    topic_decisions: &std::collections::HashMap<&str, AuthorizationResult>,
    subscribed_topics: &HashSet<String>,
    topic_partition_counts: &std::collections::HashMap<&str, i32>,
) -> (
    Vec<OffsetDeleteResponseTopic>,
    Vec<Record>,
    Vec<(String, i32)>,
) {
    let mut topics_out: Vec<OffsetDeleteResponseTopic> = Vec::with_capacity(topics.len());
    let mut tombstones: Vec<Record> = Vec::new();
    let mut to_remove: Vec<(String, i32)> = Vec::new();
    let mut delta: i32 = 0;

    for topic in topics {
        let denied = topic_decisions
            .get(topic.name.as_str())
            .copied()
            .unwrap_or(AuthorizationResult::Deny)
            == AuthorizationResult::Deny;
        let mut partitions_out: Vec<OffsetDeleteResponsePartition> =
            Vec::with_capacity(topic.partitions.len());

        for part in &topic.partitions {
            let code = if denied {
                codes::TOPIC_AUTHORIZATION_FAILED
            } else if subscribed_topics.contains(&topic.name) {
                codes::GROUP_SUBSCRIBED_TO_TOPIC
            } else {
                match topic_partition_counts.get(topic.name.as_str()) {
                    Some(n) if part.partition_index >= 0 && part.partition_index < *n => {
                        tombstones.push(Record {
                            offset_delta: delta,
                            timestamp_delta: 0,
                            key: Some(OffsetCommitValue::encode_key(
                                group_id,
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

    (topics_out, tombstones, to_remove)
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
    let Some(part_handle) = broker.partitions.get(OFFSETS_TOPIC, OFFSETS_PARTITION) else {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    };
    let (ack_tx, ack_rx) = oneshot::channel();
    if part_handle
        .writer_tx
        .send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(batch),
            ack: ack_tx,
        }))
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
    use assert2::assert;
    use bytes::BufMut;
    use crabka_protocol::owned::consumer_protocol_subscription::ConsumerProtocolSubscription;
    use crabka_protocol::owned::offset_delete_request::{
        OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    };
    use std::collections::HashMap;

    // ── decode_subscribed_topics ─────────────────────────────────────

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
        assert!(got == vec!["foo".to_string(), "bar".to_string()]);
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

    // ── whole_error ──────────────────────────────────────────────────

    fn req_with_topics(topics: &[(&str, &[i32])]) -> OffsetDeleteRequest {
        OffsetDeleteRequest {
            group_id: "g".to_string(),
            topics: topics
                .iter()
                .map(|(n, ps)| OffsetDeleteRequestTopic {
                    name: (*n).to_string(),
                    partitions: ps
                        .iter()
                        .map(|p| OffsetDeleteRequestPartition {
                            partition_index: *p,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn whole_error_stamps_top_level_and_each_partition() {
        let req = req_with_topics(&[("t1", &[0, 1]), ("t2", &[5])]);
        let resp = whole_error(&req, codes::GROUP_AUTHORIZATION_FAILED);
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(resp.topics.len() == 2);
        assert!(resp.topics[0].partitions.len() == 2);
        for t in &resp.topics {
            for p in &t.partitions {
                assert!(p.error_code == codes::GROUP_AUTHORIZATION_FAILED);
            }
        }
    }

    #[test]
    fn whole_error_with_empty_request_returns_empty_topics_list() {
        let req = req_with_topics(&[]);
        let resp = whole_error(&req, codes::GROUP_ID_NOT_FOUND);
        assert!(resp.error_code == codes::GROUP_ID_NOT_FOUND);
        assert!(resp.topics.is_empty());
    }

    // ── rewrite_success_as ───────────────────────────────────────────

    fn resp_topics(rows: &[(&str, &[(i32, i16)])]) -> Vec<OffsetDeleteResponseTopic> {
        rows.iter()
            .map(|(n, ps)| OffsetDeleteResponseTopic {
                name: (*n).to_string(),
                partitions: ps
                    .iter()
                    .map(|(idx, code)| OffsetDeleteResponsePartition {
                        partition_index: *idx,
                        error_code: *code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn rewrite_success_as_overwrites_none_rows_only() {
        let rows = resp_topics(&[(
            "t",
            &[
                (0, codes::NONE),
                (1, codes::TOPIC_AUTHORIZATION_FAILED),
                (2, codes::NONE),
            ],
        )]);
        let resp = rewrite_success_as(rows, codes::UNKNOWN_SERVER_ERROR);
        assert!(resp.error_code == codes::NONE);
        assert!(resp.topics[0].partitions[0].error_code == codes::UNKNOWN_SERVER_ERROR);
        assert!(
            resp.topics[0].partitions[1].error_code == codes::TOPIC_AUTHORIZATION_FAILED,
            "denied row stays denied"
        );
        assert!(resp.topics[0].partitions[2].error_code == codes::UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn rewrite_success_as_noop_when_no_none_rows() {
        let rows = resp_topics(&[("t", &[(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)])]);
        let resp = rewrite_success_as(rows, codes::UNKNOWN_SERVER_ERROR);
        assert!(resp.topics[0].partitions[0].error_code == codes::GROUP_SUBSCRIBED_TO_TOPIC);
    }

    // ── build_response_rows ──────────────────────────────────────────

    #[test]
    fn build_rows_denied_topic_returns_topic_authorization_failed_per_partition() {
        let req = req_with_topics(&[("t1", &[0, 1])]);
        let mut decisions = HashMap::new();
        decisions.insert("t1", AuthorizationResult::Deny);
        let subscribed: HashSet<String> = HashSet::new();
        let mut counts = HashMap::new();
        counts.insert("t1", 4);

        let (out, tombs, to_remove) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        assert!(tombs.is_empty(), "no tombstones when denied");
        assert!(to_remove.is_empty());
        for p in &out[0].partitions {
            assert!(p.error_code == codes::TOPIC_AUTHORIZATION_FAILED);
        }
    }

    #[test]
    fn build_rows_subscribed_topic_returns_group_subscribed_per_partition() {
        let req = req_with_topics(&[("t1", &[0])]);
        let mut decisions = HashMap::new();
        decisions.insert("t1", AuthorizationResult::Allow);
        let mut subscribed: HashSet<String> = HashSet::new();
        subscribed.insert("t1".to_string());
        let mut counts = HashMap::new();
        counts.insert("t1", 4);

        let (out, tombs, _) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        assert!(tombs.is_empty(), "subscribed topic → no tombstone");
        assert!(out[0].partitions[0].error_code == codes::GROUP_SUBSCRIBED_TO_TOPIC);
    }

    #[test]
    fn build_rows_missing_topic_returns_unknown_topic_or_partition() {
        let req = req_with_topics(&[("ghost", &[0])]);
        let mut decisions = HashMap::new();
        decisions.insert("ghost", AuthorizationResult::Allow);
        let subscribed: HashSet<String> = HashSet::new();
        // counts is empty: topic doesn't exist in image.
        let counts: HashMap<&str, i32> = HashMap::new();

        let (out, tombs, _) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        assert!(tombs.is_empty());
        assert!(out[0].partitions[0].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn build_rows_partition_out_of_range_returns_unknown_topic_or_partition() {
        let req = req_with_topics(&[("t1", &[0, 99, -1])]);
        let mut decisions = HashMap::new();
        decisions.insert("t1", AuthorizationResult::Allow);
        let subscribed: HashSet<String> = HashSet::new();
        let mut counts = HashMap::new();
        counts.insert("t1", 2);

        let (out, tombs, to_remove) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        // p=0 succeeds; p=99 and p=-1 each fail with UNKNOWN_TOPIC_OR_PARTITION.
        assert!(out[0].partitions[0].error_code == codes::NONE);
        assert!(out[0].partitions[1].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
        assert!(out[0].partitions[2].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
        assert!(tombs.len() == 1, "only p=0 queued");
        assert!(to_remove == vec![("t1".to_string(), 0)]);
    }

    #[test]
    fn build_rows_happy_path_queues_tombstone_with_increasing_deltas() {
        let req = req_with_topics(&[("t1", &[0, 2]), ("t2", &[7])]);
        let mut decisions = HashMap::new();
        decisions.insert("t1", AuthorizationResult::Allow);
        decisions.insert("t2", AuthorizationResult::Allow);
        let subscribed: HashSet<String> = HashSet::new();
        let mut counts = HashMap::new();
        counts.insert("t1", 4);
        counts.insert("t2", 8);

        let (out, tombs, to_remove) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        assert!(tombs.len() == 3, "3 partitions × 1 tombstone each");
        // Offset deltas increase monotonically across the whole batch.
        assert!(tombs[0].offset_delta == 0);
        assert!(tombs[1].offset_delta == 1);
        assert!(tombs[2].offset_delta == 2);
        // Tombstones carry null values.
        assert!(tombs[0].value.is_none());
        assert!(tombs[1].value.is_none());
        assert!(tombs[2].value.is_none());
        for t in &out {
            for p in &t.partitions {
                assert!(p.error_code == codes::NONE);
            }
        }
        assert!(
            to_remove
                == vec![
                    ("t1".to_string(), 0),
                    ("t1".to_string(), 2),
                    ("t2".to_string(), 7),
                ]
        );
    }

    #[test]
    fn build_rows_missing_topic_in_decisions_treats_as_deny() {
        // ACL decisions map didn't include the topic → treat as Deny
        // (defensive default). Per-partition TOPIC_AUTHORIZATION_FAILED.
        let req = req_with_topics(&[("t1", &[0])]);
        let decisions: HashMap<&str, AuthorizationResult> = HashMap::new();
        let subscribed: HashSet<String> = HashSet::new();
        let mut counts = HashMap::new();
        counts.insert("t1", 4);

        let (out, tombs, _) =
            build_response_rows("g", &req.topics, &decisions, &subscribed, &counts);
        assert!(tombs.is_empty());
        assert!(out[0].partitions[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED);
    }
}
