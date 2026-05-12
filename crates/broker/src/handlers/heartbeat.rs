//! `Heartbeat` (`api_key=12`). Validates `(generation, member)` and
//! refreshes the member's `last_heartbeat` clock.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::heartbeat_response::HeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::GroupState;
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
        let req = HeartbeatRequest::decode(&mut cur, version)?;

        let error_code = match group_manager.find(&req.group_id) {
            None => codes::UNKNOWN_MEMBER_ID,
            Some(handle) => {
                let mut g = handle.state.lock().await;
                // Check preconditions in order so callers see the most
                // informative code first. `let-else` can't chain like this,
                // so split membership / generation / state into a flat
                // sequence of guards before mutating.
                if !g.members.contains_key(&req.member_id) {
                    codes::UNKNOWN_MEMBER_ID
                } else if g.generation_id != req.generation_id {
                    codes::ILLEGAL_GENERATION
                } else if !matches!(g.state, GroupState::Stable) {
                    codes::REBALANCE_IN_PROGRESS
                } else {
                    g.members
                        .get_mut(&req.member_id)
                        .expect("contains_key checked above")
                        .last_heartbeat = std::time::Instant::now();
                    codes::NONE
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
