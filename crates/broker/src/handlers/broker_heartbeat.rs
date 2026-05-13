//! `BrokerHeartbeat` (`api_key=63`). KIP-500 controller-side heartbeat handler.
//!
//! Only the openraft leader handles heartbeats. Non-leaders return
//! `NOT_CONTROLLER` so the broker client can redirect.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use crabka_protocol::owned::broker_heartbeat_response::BrokerHeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let liveness = broker.liveness.clone();
    // Check leadership: this broker is the controller leader iff the
    // watch channel reports a leader id equal to our own node_id.
    let is_leader = broker
        .controller
        .watch_leader()
        .borrow()
        .is_some_and(|n| n == broker.config.node_id);
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = BrokerHeartbeatRequest::decode(&mut cur, version)?;

        // Only the openraft leader handles heartbeats. NOT_CONTROLLER
        // tells the broker client to redirect.
        if !is_leader {
            let resp = BrokerHeartbeatResponse {
                throttle_time_ms: 0,
                error_code: codes::NOT_CONTROLLER,
                is_caught_up: false,
                is_fenced: true,
                should_shut_down: false,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        // Record the heartbeat. If it's a revival, the liveness ticker
        // will pick up the transition next cycle and the heartbeat-side
        // wakeup is a no-op for slice 10b — slice 11's controlled-shutdown
        // path will add explicit on-revival handling.
        let _transition = liveness
            .record_heartbeat(u64::try_from(req.broker_id).unwrap_or(0))
            .await;

        let resp = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            is_caught_up: true,
            is_fenced: false,
            should_shut_down: false,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
