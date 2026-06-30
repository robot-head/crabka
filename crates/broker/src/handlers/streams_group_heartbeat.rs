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
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest;
use crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::streams::actor::StreamsGroupActorMessage;
use crate::error::BrokerError;
use crate::time_util::now_ms;

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
            ctx.principal,
            ctx.peer,
            &req.group_id,
        ) {
            return encode(version, &error(codes::GROUP_AUTHORIZATION_FAILED));
        }

        // KIP-1071: the streams protocol is gated on a finalized
        // streams.version >= 1 (early access, default-disabled) AND the
        // `streams_group.enable` config kill-switch. Either off → reject so the
        // client knows the broker does not serve this protocol.
        if !crate::features::feature_enabled(&image, crate::features::STREAMS_VERSION, 1)
            || !streams_enabled
        {
            return encode(version, &error(codes::UNSUPPORTED_VERSION));
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
                return encode(version, &error(codes::GROUP_ID_NOT_FOUND));
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
    }
}

/// `Read` on `Group(group_id)` gate. Returns `true` when denied.
fn group_read_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    principal: &crabka_security::Principal,
    host: &std::net::SocketAddr,
    group_id: &str,
) -> bool {
    authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: ResourceType::Group,
            resource_name: group_id,
            operation: AclOperation::Read,
        },
    ) == AuthorizationResult::Deny
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
    use crabka_metadata::{FeatureLevelRecord, MetadataRecord};
    use crabka_protocol::owned::streams_group_heartbeat_response;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    fn request(group_id: &str) -> StreamsGroupHeartbeatRequest {
        StreamsGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: String::new(),
            member_epoch: 0,
            ..Default::default()
        }
    }

    fn encode_request(req: &StreamsGroupHeartbeatRequest) -> Bytes {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes) -> StreamsGroupHeartbeatResponse {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            StreamsGroupHeartbeatResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn principal() -> Principal {
        Principal {
            name: "alice".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "streams-client",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    async fn start_broker(
        streams_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        cfg.streams_group.enable = streams_enabled;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
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

        assert!(group_read_denied(
            &authorizer,
            &image,
            &principal,
            &peer,
            "g"
        ));

        let bytes = encode(
            streams_group_heartbeat_response::MAX_VERSION,
            &error(codes::GROUP_AUTHORIZATION_FAILED),
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
