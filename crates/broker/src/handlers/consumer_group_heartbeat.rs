//! `ConsumerGroupHeartbeat` (`api_key` 68) — KIP-848 next-gen consumer
//! group protocol. Routes the request to the per-group actor in
//! `GroupCoordinator`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::actor::GroupActorMessage;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let coordinator = broker.group_coordinator.clone();
    let image = broker.controller.current_image();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ConsumerGroupHeartbeatRequest::decode(&mut cur, version)?;

        // KIP-848 / KIP-584: the next-gen protocol is gated on a finalized
        // group.version >= 1. Below that — including UNFINALIZED, which means
        // disabled — reject so the client falls back to the classic protocol.
        if !crate::features::feature_enabled(
            &image,
            crabka_metadata::group_version::GROUP_VERSION_FEATURE,
            1,
        ) {
            return encode(version, &error(codes::UNSUPPORTED_VERSION));
        }

        if !coordinator.config.next_gen_enabled() {
            return encode(version, &error(codes::GROUP_ID_NOT_FOUND));
        }

        // The actor's kind is the per-group type lock: a classic group rejects
        // a next-gen heartbeat (and `get_or_create_consumer` returns `None`),
        // mirroring the old `group_type == Classic` rejection.
        let Some(handle) = coordinator.get_or_create_consumer(&req.group_id) else {
            return encode(version, &error(codes::GROUP_ID_NOT_FOUND));
        };
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: req,
                client_host: String::new(),
                reply: tx,
            })
            .await
            .is_err()
        {
            return encode(version, &error(codes::COORDINATOR_LOAD_IN_PROGRESS));
        }
        let resp = rx
            .await
            .unwrap_or_else(|_| error(codes::UNKNOWN_SERVER_ERROR));
        encode(version, &resp)
    })
}

fn error(code: i16) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code: code,
        ..Default::default()
    }
}

fn encode(version: i16, resp: &ConsumerGroupHeartbeatResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
