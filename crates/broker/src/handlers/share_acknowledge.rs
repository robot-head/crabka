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
