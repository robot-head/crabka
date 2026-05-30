//! `LeaveGroup` (`api_key=13`). Removes one or more members inside the group's
//! actor and (if the group is still `Stable` with survivors) reopens a
//! rebalance.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use crabka_protocol::owned::leave_group_request::LeaveGroupRequest;
use crabka_protocol::owned::leave_group_response::LeaveGroupResponse;
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
        let req = LeaveGroupRequest::decode(&mut cur, version)?;

        let members = match coordinator.find(&req.group_id) {
            // No such group; respond OK but no member responses.
            None => Vec::new(),
            Some(handle) => {
                let (tx, rx) = oneshot::channel();
                if handle
                    .tx
                    .send(GroupActorMessage::ClassicLeave {
                        req,
                        version,
                        reply: tx,
                    })
                    .await
                    .is_err()
                {
                    Vec::new()
                } else {
                    rx.await.unwrap_or_default()
                }
            }
        };

        let resp = LeaveGroupResponse {
            error_code: codes::NONE,
            throttle_time_ms: 0,
            members,
            ..Default::default()
        };
        encode(version, &resp)
    })
}

fn encode(version: i16, resp: &LeaveGroupResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
