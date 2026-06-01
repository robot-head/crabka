//! `Heartbeat` (`api_key=12`). Validates `(generation, member)` and
//! refreshes the member's `last_heartbeat` clock inside the group's actor.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::heartbeat_response::HeartbeatResponse;
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
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = HeartbeatRequest::decode(&mut cur, version)?;

        let error_code = match coordinator.find(&req.group_id) {
            None => codes::UNKNOWN_MEMBER_ID,
            Some(handle) => {
                let (tx, rx) = oneshot::channel();
                if handle
                    .tx
                    .send(GroupActorMessage::ClassicHeartbeat { req, reply: tx })
                    .await
                    .is_err()
                {
                    codes::UNKNOWN_MEMBER_ID
                } else {
                    rx.await.unwrap_or(codes::UNKNOWN_MEMBER_ID)
                }
            }
        };

        let resp = HeartbeatResponse {
            error_code,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
