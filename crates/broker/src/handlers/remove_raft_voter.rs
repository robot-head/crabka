//! `RemoveRaftVoter` (`api_key=81`, KIP-853). Admin RPC that drops a voter
//! from the controller-raft voter set (refusing to remove the last voter).
//!
//! ## ACL
//!
//! `Alter` on `Cluster("kafka-cluster")`. Deny → whole-response
//! `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! Outcome → error code mapping is shared with [`super::add_raft_voter`].

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::remove_raft_voter_request::RemoveRaftVoterRequest;
use crabka_protocol::owned::remove_raft_voter_response::RemoveRaftVoterResponse;
use crabka_protocol::{Decode, Encode};
use crabka_raft::reconfig::RemoveVoter;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::add_raft_voter::outcome_to_code;

#[tracing::instrument(
    name = "handle_remove_raft_voter",
    level = "info",
    skip_all,
    fields(api = "RemoveRaftVoter", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = RemoveRaftVoterRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Alter,
        },
    );
    if allow == AuthorizationResult::Deny {
        return encode_resp(
            version,
            &RemoveRaftVoterResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("remove-raft-voter denied".into()),
                ..Default::default()
            },
        );
    }

    let Ok(id) = u64::try_from(req.voter_id) else {
        return encode_resp(
            version,
            &RemoveRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some(format!(
                    "voter_id must be non-negative, got {}",
                    req.voter_id
                )),
                ..Default::default()
            },
        );
    };

    let (error_code, error_message) = outcome_to_code(
        broker
            .controller
            .remove_voter(RemoveVoter {
                id,
                directory_id: uuid::Uuid::from_bytes(req.voter_directory_id.0),
            })
            .await,
    );

    encode_resp(
        version,
        &RemoveRaftVoterResponse {
            error_code,
            error_message,
            ..Default::default()
        },
    )
}

fn encode_resp(version: i16, resp: &RemoveRaftVoterResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::{AuthorizationRequest, Authorizer};
    use crate::config::BrokerConfig;

    #[derive(Debug)]
    struct DenyAll;

    impl Authorizer for DenyAll {
        fn authorize(
            &self,
            _source: &dyn crabka_authz::AclSource,
            _req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            AuthorizationResult::Deny
        }
    }

    fn request(voter_id: i32) -> RemoveRaftVoterRequest {
        RemoveRaftVoterRequest {
            cluster_id: Some("cluster".into()),
            voter_id,
            voter_directory_id: ProtoUuid([3; 16]),
            ..Default::default()
        }
    }

    fn encode_request(version: i16, req: &RemoveRaftVoterRequest) -> Bytes {
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(version: i16, bytes: &Bytes) -> RemoveRaftVoterResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = RemoveRaftVoterResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "admin-client",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.authorizer = authorizer;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    /// Decode→encode round-trip at min and max versions.
    #[test]
    fn response_round_trips_at_min_and_max_versions() {
        use crabka_protocol::owned::remove_raft_voter_response::{self, RemoveRaftVoterResponse};
        for version in [
            remove_raft_voter_response::MIN_VERSION,
            remove_raft_voter_response::MAX_VERSION,
        ] {
            let resp = RemoveRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("cannot remove the last voter".into()),
                ..Default::default()
            };
            let bytes = encode_resp(version, &resp).expect("encode");
            let mut cur: &[u8] = &bytes;
            let decoded = RemoveRaftVoterResponse::decode(&mut cur, version).expect("decode");
            assert!(
                (
                    decoded.error_code,
                    decoded.error_message.as_deref(),
                    cur.is_empty(),
                ) == (
                    codes::INVALID_REQUEST,
                    Some("cannot remove the last voter"),
                    true,
                ),
                "all bytes consumed at v{version}"
            );
        }
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_without_calling_reconfig() {
        let version = crabka_protocol::owned::remove_raft_voter_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(version, &request(2));

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(version, &resp);

        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
        assert!(resp.error_message.as_deref() == Some("remove-raft-voter denied"));
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_negative_voter_id_before_reconfig() {
        let version = crabka_protocol::owned::remove_raft_voter_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(version, &request(-7));

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(version, &resp);

        assert!(resp.error_code == codes::INVALID_REQUEST);
        assert!(
            resp.error_message.as_deref().is_some_and(|m| {
                m.contains("voter_id must be non-negative") && m.contains("-7")
            })
        );
        broker_handle.shutdown().await;
    }
}
