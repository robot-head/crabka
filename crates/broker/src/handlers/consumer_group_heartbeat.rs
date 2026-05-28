//! `ConsumerGroupHeartbeat` (api_key 68) — KIP-848 next-gen consumer
//! group protocol. Routes the request to the per-group actor in
//! `NextGenCoordinator`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::next_gen::GroupType;
use crate::coordinator::next_gen::group_actor::GroupActorMessage;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ConsumerGroupHeartbeatRequest::decode(&mut cur, version)?;

        let ng = match group_manager.next_gen() {
            Some(c) if c.config.next_gen_enabled() => c.clone(),
            _ => return encode(version, &error(codes::GROUP_ID_NOT_FOUND)),
        };

        if matches!(ng.group_type(&req.group_id), Some(GroupType::Classic)) {
            return encode(version, &error(codes::GROUP_ID_NOT_FOUND));
        }

        ng.mark_next_gen(&req.group_id);

        let handle = ng.get_or_create(&req.group_id);
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
        let resp = rx.await.unwrap_or_else(|_| error(codes::UNKNOWN_SERVER_ERROR));
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
