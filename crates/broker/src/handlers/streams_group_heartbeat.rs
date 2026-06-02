//! `StreamsGroupHeartbeat` (`api_key` 88) — KIP-1071 streams rebalance
//! protocol. Routes the request to the per-group streams actor in
//! `GroupCoordinator`.
//!
//! Mirrors the KIP-932 share-group heartbeat handler
//! ([`super::share_group_heartbeat`]): decode, gate, `mark_streams` +
//! `get_or_create_streams`, send a `Heartbeat` actor message, await the
//! oneshot, encode. Gated on BOTH the finalized `streams.version >= 1` feature
//! (KIP-1071 early access) AND the `streams_group.enable` config kill-switch.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use crabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest;
use crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::streams::actor::StreamsGroupActorMessage;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let streams_enabled = broker.config.streams_group.enable;
    let image = broker.controller.current_image();
    let ng = broker.group_coordinator.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = StreamsGroupHeartbeatRequest::decode(&mut cur, version)?;

        // KIP-1071: the streams protocol is gated on a finalized
        // streams.version >= 1 (early access, default-disabled) AND the
        // `streams_group.enable` config kill-switch. Either off → reject so the
        // client knows the broker does not serve this protocol.
        if !crate::features::feature_enabled(&image, crate::features::STREAMS_VERSION, 1)
            || !streams_enabled
        {
            return encode(version, &error(codes::UNSUPPORTED_VERSION));
        }

        ng.mark_streams(&req.group_id);
        let handle = ng.get_or_create_streams(&req.group_id);
        let (tx, rx) = oneshot::channel();
        // TODO: the plain 4-arg handler has no request-header / peer access, so
        // we pass empty client_id/host (matching the share-group handler). A
        // future inline-intercept upgrade in `network::dispatch` can thread the
        // real client_id + peer SocketAddr through for member metadata.
        if handle
            .tx
            .send(StreamsGroupActorMessage::Heartbeat {
                request: req,
                client_id: String::new(),
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

/// Response returned when the streams protocol is disabled on this broker
/// (feature unfinalized or config kill-switch off).
fn disabled_response() -> StreamsGroupHeartbeatResponse {
    error(codes::UNSUPPORTED_VERSION)
}

fn error(code: i16) -> StreamsGroupHeartbeatResponse {
    StreamsGroupHeartbeatResponse {
        error_code: code,
        ..Default::default()
    }
}

fn encode(version: i16, resp: &StreamsGroupHeartbeatResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn disabled_feature_yields_unsupported_version() {
        let resp = disabled_response();
        assert!(resp.error_code == codes::UNSUPPORTED_VERSION);
    }
}
