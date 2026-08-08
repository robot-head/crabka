//! `ShareAcknowledge` (`api_key` 79), from KIP-932.
//!
//! This is the acknowledge-only counterpart of `ShareFetch`. It acknowledges
//! records that a member acquired earlier, and acquires no new ones.
//!
//! For every requested partition that this broker leads, the handler applies
//! each acknowledgement batch to the `(group, topic, partition)`
//! [`AcquisitionState`] machine, and persists the result. Accept advances the
//! SPSO, Release offers the records again, and Reject and Gap archive them.
//!
//! A partition that this broker does not lead gets `NOT_LEADER_OR_FOLLOWER`.
//! An acknowledge that targets records the member does not currently hold
//! fails that partition row with `INVALID_RECORD_STATE`.
//!
//! `network::dispatch` intercepts this request inline, so the handler receives
//! the per-connection principal and the peer `SocketAddr` for the per-topic
//! `Read` ACL gate.

use std::time::Instant;

use bytes::Bytes;
use crabka_log::Offset;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        share_acknowledge_request::ShareAcknowledgeRequest,
        share_acknowledge_response::{
            LeaderIdAndEpoch, PartitionData, ShareAcknowledgeResponse,
            ShareAcknowledgeTopicResponse,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::share_fetch::apply_one_ack,
};

#[tracing::instrument(
    name = "handle_share_acknowledge",
    level = "info",
    skip_all,
    fields(api = "ShareAcknowledge", version, req_bytes = req_bytes.len()),
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
    let req = ShareAcknowledgeRequest::decode(&mut cur, version)?;

    let cfg = broker.config.share_group.clone();
    let lock_timeout_ms = i32::try_from(cfg.record_lock_duration.as_millis()).unwrap_or(i32::MAX);

    if !cfg.enable {
        return encode_error_response(version, codes::UNSUPPORTED_VERSION, lock_timeout_ms);
    }

    let group = req.group_id.clone().unwrap_or_default();
    let member = req.member_id.clone().unwrap_or_default();

    if let Err(code) =
        broker
            .share_partition_leaders
            .validate_session(&group, &member, req.share_session_epoch)
    {
        return encode_error_response(version, code, lock_timeout_ms);
    }

    let now = Instant::now();
    let responses = process_topics(broker, &req, ctx, &cfg, &group, &member, now).await;

    let resp = ShareAcknowledgeResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        acquisition_lock_timeout_ms: lock_timeout_ms,
        responses,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

async fn process_topics(
    broker: &Broker,
    req: &ShareAcknowledgeRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    cfg: &crate::coordinator::unified::share::config::ShareGroupConfig,
    group: &str,
    member: &str,
    now: Instant,
) -> Vec<ShareAcknowledgeTopicResponse> {
    let mgr = &broker.share_partition_leaders;
    let image = broker.controller.current_image();
    let mut responses = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        let topic_name = mgr.topic_name_for(topic_id);

        let denied = match topic_name.as_deref() {
            Some(name) => {
                broker.config.authorizer.authorize(
                    &*image,
                    &AuthorizationRequest {
                        principal: ctx.principal,
                        host: ctx.peer,
                        resource_type: ResourceType::Topic,
                        resource_name: name,
                        operation: AclOperation::Read,
                    },
                ) == AuthorizationResult::Deny
            }
            None => true,
        };

        let mut parts: Vec<PartitionData> = Vec::with_capacity(topic.partitions.len());
        for ap in &topic.partitions {
            let mut out = PartitionData {
                partition_index: ap.partition_index,
                ..Default::default()
            };

            if denied {
                out.error_code = if topic_name.is_some() {
                    codes::TOPIC_AUTHORIZATION_FAILED
                } else {
                    codes::UNKNOWN_TOPIC_OR_PARTITION
                };
                parts.push(out);
                continue;
            }

            if !mgr.topic_leader_is_self(topic_id, ap.partition_index) {
                let (leader_id, leader_epoch) = mgr.current_leader_of(topic_id, ap.partition_index);
                out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                out.current_leader = LeaderIdAndEpoch {
                    leader_id,
                    leader_epoch,
                    ..Default::default()
                };
                parts.push(out);
                continue;
            }

            let cell = mgr.get_or_load(group, topic_id, ap.partition_index).await;
            let mut st = cell.lock().await;
            let mut err = codes::NONE;
            for batch in &ap.acknowledgement_batches {
                // A renew-ack RENEWs each batch's lock instead of acknowledging.
                let res = if req.is_renew_ack {
                    st.renew(
                        member,
                        Offset(batch.first_offset),
                        Offset(batch.last_offset),
                        now,
                        cfg.record_lock_duration,
                    )
                } else {
                    apply_one_ack(
                        &mut st,
                        member,
                        batch.first_offset,
                        batch.last_offset,
                        &batch.acknowledge_types,
                        now,
                    )
                };
                if let Err(code) = res {
                    err = code;
                }
            }
            out.error_code = err;
            mgr.persist_if_dirty(group, topic_id, ap.partition_index, &mut st)
                .await;
            parts.push(out);
        }

        responses.push(ShareAcknowledgeTopicResponse {
            topic_id: topic.topic_id,
            partitions: parts,
            ..Default::default()
        });
    }
    responses
}

/// Encodes a `ShareAcknowledgeResponse` that carries a top-level error and no
/// per-partition row. The error is a feature-gate or session failure.
fn encode_error_response(
    version: i16,
    error_code: i16,
    lock_timeout_ms: i32,
) -> Result<Bytes, BrokerError> {
    let resp = ShareAcknowledgeResponse {
        throttle_time_ms: 0,
        error_code,
        error_message: None,
        acquisition_lock_timeout_ms: lock_timeout_ms,
        responses: Vec::new(),
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use assert2::assert;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            share_acknowledge_request::{AcknowledgePartition, AcknowledgeTopic},
            share_acknowledge_response,
        },
        primitives::uuid::Uuid as ProtoUuid,
    };
    use crabka_security::Principal;

    use super::*;

    crate::test_support::wire_helpers!(
        ShareAcknowledgeRequest,
        ShareAcknowledgeResponse,
        version = share_acknowledge_response::MAX_VERSION,
        client_id = "client-a"
    );

    fn request(topic_id: ProtoUuid, partitions: &[i32]) -> ShareAcknowledgeRequest {
        ShareAcknowledgeRequest {
            group_id: Some("g1".into()),
            member_id: Some("member-1".into()),
            share_session_epoch: 0,
            topics: vec![AcknowledgeTopic {
                topic_id,
                partitions: partitions
                    .iter()
                    .map(|partition_index| AcknowledgePartition {
                        partition_index: *partition_index,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    async fn start_broker(share_enabled: bool) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.share_group.enable = share_enabled;
        })
        .await
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    #[test]
    fn encode_error_response_preserves_top_level_fields() {
        let resp = encode_error_response(
            share_acknowledge_response::MAX_VERSION,
            codes::UNSUPPORTED_VERSION,
            12_345,
        )
        .expect("encode");
        let resp = decode_response(&resp);

        let expected = ShareAcknowledgeResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            acquisition_lock_timeout_ms: 12_345,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn handle_disabled_feature_returns_top_level_unsupported_version() {
        let version = share_acknowledge_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(false).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request(ProtoUuid([7; 16]), &[0]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = ShareAcknowledgeResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            acquisition_lock_timeout_ms: 30_000,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_unknown_topic_preserves_topic_and_partition_rows() {
        let version = share_acknowledge_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let topic_id = ProtoUuid([8; 16]);
        let req_bytes = encode_request(&request(topic_id, &[3, 5]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = ShareAcknowledgeResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            acquisition_lock_timeout_ms: 30_000,
            responses: vec![ShareAcknowledgeTopicResponse {
                topic_id,
                partitions: vec![
                    PartitionData {
                        partition_index: 3,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        error_message: None,
                        current_leader: LeaderIdAndEpoch {
                            leader_id: 0,
                            leader_epoch: 0,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        },
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                    PartitionData {
                        partition_index: 5,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        error_message: None,
                        current_leader: LeaderIdAndEpoch {
                            leader_id: 0,
                            leader_epoch: 0,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        },
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                ],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            node_endpoints: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
