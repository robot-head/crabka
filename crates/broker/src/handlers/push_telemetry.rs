//! `PushTelemetry` (`api_key=72`, KIP-714). Clients call this to deliver
//! OTel-encoded metrics to the broker.
//!
//! Crabka's [`get_telemetry_subscriptions`](super::get_telemetry_subscriptions)
//! handler advertises an empty `requested_metrics` set, so well-behaved
//! JVM clients never send `PushTelemetry` in the first place. A
//! defensive no-op handler is still wired here because:
//!
//! 1. A client running ahead of its periodic re-fetch may still ship a
//!    push that races our "no subscription" answer. Returning success
//!    silently is the friendliest outcome — the client moves on.
//! 2. KIP-714 specifies that the client back off and re-query the
//!    subscription on certain error codes; emitting one would cost
//!    the client a stall for no benefit (we'll always say "no
//!    subscription" anyway).
//!
//! The metrics payload (`req.metrics`) is dropped on the floor. No
//! decoding is attempted — it's an `OTel` `MetricsData` protobuf and
//! ingesting it would require a metrics pipeline the broker doesn't
//! have.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::push_telemetry_request::PushTelemetryRequest;
use crabka_protocol::owned::push_telemetry_response::PushTelemetryResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    _broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        // Decoded but unused — we silently drop the metrics payload.
        // Decoding still runs so a malformed request surfaces a
        // protocol error rather than a silent ack.
        let _req = PushTelemetryRequest::decode(&mut cur, version)?;

        let resp = PushTelemetryResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
