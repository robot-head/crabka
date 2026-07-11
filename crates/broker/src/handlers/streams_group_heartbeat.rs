//! `StreamsGroupHeartbeat` (`api_key` 88) — KIP-1071 streams rebalance
//! protocol. Routes the request to the per-group streams actor in
//! `GroupCoordinator`.
//!
//! Mirrors the KIP-932 share-group heartbeat handler
//! ([`super::share_group_heartbeat`]): decode, gate, `mark_streams` +
//! `get_or_create_streams`, send a `Heartbeat` actor message, await the
//! oneshot, encode. Gated on BOTH the finalized `streams.version >= 1` feature
//! (KIP-1071 early access) AND the `streams_group.enable` config kill-switch.

use bytes::Bytes;
use crabka_protocol::{
    Decode,
    owned::{
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker, codes, coordinator::unified::streams::actor::StreamsGroupActorMessage,
    error::BrokerError, handlers::group_read_denied, time_util::now_ms,
};

#[tracing::instrument(
    name = "handle_streams_group_heartbeat",
    level = "info",
    skip_all,
    fields(api = "StreamsGroupHeartbeat", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let streams_enabled = broker.config.streams_group.enable;
    let image = broker.controller.current_image();
    let ng = broker.group_coordinator.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = StreamsGroupHeartbeatRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // `Read` on `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`. Topology/topic ACLs
        // are not evaluated by this handler.
        if group_read_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            &req.group_id,
        ) {
            return crate::handlers::encode_response(
                &error(codes::GROUP_AUTHORIZATION_FAILED),
                version,
            );
        }

        // KIP-1071: the streams protocol is gated on a finalized
        // streams.version >= 1 (early access, default-disabled) AND the
        // `streams_group.enable` config kill-switch. Either off → reject so the
        // client knows the broker does not serve this protocol.
        if !crate::features::feature_enabled(&image, crate::features::STREAMS_VERSION, 1)
            || !streams_enabled
        {
            return crate::handlers::encode_response(&error(codes::UNSUPPORTED_VERSION), version);
        }

        // KIP-1071 cold upgrade: a StreamsGroupHeartbeat for a drained classic group
        // converts it in place; a classic group with live members is rejected (online
        // streams migration is unsupported). Non-classic group_ids pass through.
        match ng
            .try_convert_classic_to_streams(&req.group_id, now_ms())
            .await
        {
            Ok(
                crate::coordinator::unified::streams::migration::ConvertOutcome::RejectLiveMembers,
            ) => {
                return crate::handlers::encode_response(
                    &error(codes::GROUP_ID_NOT_FOUND),
                    version,
                );
            }
            Ok(_) => {} // NotClassic | Converted → serve normally below
            Err(e) => return Err(e),
        }

        ng.mark_streams(&req.group_id);
        let handle = ng.get_or_create_streams(&req.group_id);
        let (tx, rx) = oneshot::channel();
        // The actor message shape carries client_id/client_host, but this
        // handler does not use them for routing, so pass empty values.
        if handle
            .tx
            .send(StreamsGroupActorMessage::Heartbeat {
                request: Box::new(req),
                client_id: String::new(),
                client_host: String::new(),
                reply: tx,
            })
            .await
            .is_err()
        {
            return crate::handlers::encode_response(
                &error(codes::COORDINATOR_LOAD_IN_PROGRESS),
                version,
            );
        }
        let resp = rx
            .await
            .unwrap_or_else(|_| error(codes::UNKNOWN_SERVER_ERROR));
        crate::handlers::encode_response(&resp, version)
    }
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

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use crabka_metadata::{FeatureLevelRecord, MetadataRecord};
    use crabka_protocol::owned::streams_group_heartbeat_response;
    use crabka_security::Principal;

    fn request(group_id: &str) -> StreamsGroupHeartbeatRequest {
        StreamsGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: String::new(),
            member_epoch: 0,
            ..Default::default()
        }
    }

    fn encode_request(req: &StreamsGroupHeartbeatRequest) -> Bytes {
        crate::test_support::encode_request(req, streams_group_heartbeat_response::MAX_VERSION)
    }

    fn decode_response(bytes: &Bytes) -> StreamsGroupHeartbeatResponse {
        crate::test_support::decode_response(bytes, streams_group_heartbeat_response::MAX_VERSION)
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    fn context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "streams-client")
    }

    async fn start_broker(
        streams_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.streams_group.enable = streams_enabled;
        })
        .await
    }

    async fn finalize_streams_version(broker: &Broker) {
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crate::features::STREAMS_VERSION.into(),
                level: 1,
            })])
            .await
            .expect("submit streams.version");

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if broker
                    .controller
                    .current_image()
                    .finalized_feature(crate::features::STREAMS_VERSION)
                    == Some(1)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("streams.version visible");
    }

    #[tokio::test]
    async fn handle_unfinalized_feature_returns_unsupported_version_with_read_allowed() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req_bytes = encode_request(&request("streams-app-disabled-feature"));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_disabled_config_returns_unsupported_version_when_feature_finalized() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(false).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req_bytes = encode_request(&request("streams-app-disabled-config"));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");
        broker_handle.shutdown().await;
    }

    use super::*;

    #[test]
    fn disabled_feature_yields_unsupported_version() {
        let resp = disabled_response();
        assert!(resp.error_code == codes::UNSUPPORTED_VERSION);
    }

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        use crabka_protocol::owned::streams_group_heartbeat_response::{
            self, StreamsGroupHeartbeatResponse,
        };

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "streams-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        let bytes = crate::handlers::encode_response(
            &error(codes::GROUP_AUTHORIZATION_FAILED),
            streams_group_heartbeat_response::MAX_VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = StreamsGroupHeartbeatResponse::decode(
            &mut cur,
            streams_group_heartbeat_response::MAX_VERSION,
        )
        .unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }
}
