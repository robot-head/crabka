//! `ShareAcknowledge` (`api_key` 79) — KIP-932.
//!
//! The ack-only counterpart of `ShareFetch`: it acknowledges previously
//! acquired records without acquiring new ones. For every requested partition
//! this broker leads, each acknowledgement batch is applied to the
//! `(group, topic, partition)` [`AcquisitionState`] machine (Accept advances
//! the SPSO, Release re-offers, Reject/Gap archives), and the result is
//! persisted. Partitions this broker doesn't lead get `NOT_LEADER_OR_FOLLOWER`;
//! an acknowledge that targets records not currently held by the member fails
//! the partition row with `INVALID_RECORD_STATE`.
//!
//! Intercepted inline in `network::dispatch` so the handler receives the
//! per-connection principal + peer `SocketAddr` for the per-topic `Read` ACL
//! gate.

use std::time::Instant;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::share_acknowledge_request::ShareAcknowledgeRequest;
use crabka_protocol::owned::share_acknowledge_response::{
    LeaderIdAndEpoch, PartitionData, ShareAcknowledgeResponse, ShareAcknowledgeTopicResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::share_fetch::apply_one_ack;

#[allow(clippy::too_many_lines)]
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

    let mgr = broker.share_partition_leaders.clone();
    let image = broker.controller.current_image();
    let now = Instant::now();

    let mut responses: Vec<ShareAcknowledgeTopicResponse> = Vec::with_capacity(req.topics.len());
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

            let cell = mgr.get_or_load(&group, topic_id, ap.partition_index).await;
            let mut st = cell.lock().await;
            let mut err = codes::NONE;
            for batch in &ap.acknowledgement_batches {
                // A renew-ack RENEWs each batch's lock instead of acknowledging.
                let res = if req.is_renew_ack {
                    st.renew(
                        &member,
                        batch.first_offset,
                        batch.last_offset,
                        now,
                        cfg.record_lock_duration,
                    )
                } else {
                    apply_one_ack(
                        &mut st,
                        &member,
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
            mgr.persist_if_dirty(&group, topic_id, ap.partition_index, &mut st)
                .await;
            parts.push(out);
        }

        responses.push(ShareAcknowledgeTopicResponse {
            topic_id: topic.topic_id,
            partitions: parts,
            ..Default::default()
        });
    }

    let resp = ShareAcknowledgeResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        acquisition_lock_timeout_ms: lock_timeout_ms,
        responses,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Encode a top-level-error `ShareAcknowledgeResponse` (feature-gate or session
/// failure) with no per-partition rows.
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
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::UnknownTaggedFields;
    use crabka_protocol::owned::share_acknowledge_request::{
        AcknowledgePartition, AcknowledgeTopic,
    };
    use crabka_protocol::owned::share_acknowledge_response;
    use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;

    fn encode_request(req: &ShareAcknowledgeRequest) -> Bytes {
        let version = share_acknowledge_response::MAX_VERSION;
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes) -> ShareAcknowledgeResponse {
        let version = share_acknowledge_response::MAX_VERSION;
        let mut cur: &[u8] = bytes.as_ref();
        let resp = ShareAcknowledgeResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

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

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "client-a",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    async fn start_broker(share_enabled: bool) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.share_group.enable = share_enabled;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    fn principal() -> Principal {
        Principal {
            name: "alice".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
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
