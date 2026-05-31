//! `ShareGroupHeartbeat` (`api_key` 76) — KIP-932 share-group membership.
//! Routes the request to the per-group share actor in `GroupCoordinator`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use tokio::sync::oneshot;

use crabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest;
use crabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::share::actor::ShareGroupActorMessage;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let share_enabled = broker.config.share_group.enable;
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ShareGroupHeartbeatRequest::decode(&mut cur, version)?;

        if !share_enabled {
            return encode(version, &error(codes::UNSUPPORTED_VERSION));
        }

        let Some(ng) = group_manager.next_gen().cloned() else {
            return encode(version, &error(codes::GROUP_ID_NOT_FOUND));
        };

        ng.mark_share(&req.group_id);
        let handle = ng.get_or_create_share(&req.group_id);
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(ShareGroupActorMessage::Heartbeat {
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

/// Response returned when share groups are disabled on this broker.
fn disabled_response() -> ShareGroupHeartbeatResponse {
    error(codes::UNSUPPORTED_VERSION)
}

fn error(code: i16) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        error_code: code,
        ..Default::default()
    }
}

fn encode(version: i16, resp: &ShareGroupHeartbeatResponse) -> Result<Bytes, BrokerError> {
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
