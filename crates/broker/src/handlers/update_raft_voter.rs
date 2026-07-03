//! `UpdateRaftVoter` (`api_key=82`, KIP-853). Admin RPC that rewrites an
//! existing voter's listeners / supported `kraft.version` range.
//!
//! ## ACL
//!
//! `Alter` on `Cluster("kafka-cluster")`. Deny → whole-response
//! `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! Outcome → error code mapping is shared with [`super::add_raft_voter`].
//! `UpdateVoter` never surfaces `VoterNotCaughtUp`; an unknown voter id
//! comes back as `ReconfigRejected → INVALID_REQUEST`.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType, Voter, VoterEndpoint};
use crabka_protocol::owned::update_raft_voter_request::UpdateRaftVoterRequest;
use crabka_protocol::owned::update_raft_voter_response::UpdateRaftVoterResponse;
use crabka_protocol::{Decode, Encode};
use crabka_raft::reconfig::UpdateVoter;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::add_raft_voter::outcome_to_code;

#[tracing::instrument(
    name = "handle_update_raft_voter",
    level = "info",
    skip_all,
    fields(api = "UpdateRaftVoter", version, req_bytes = req_bytes.len()),
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
    let req = UpdateRaftVoterRequest::decode(&mut cur, version)?;

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
            &UpdateRaftVoterResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            },
        );
    }

    let Ok(id) = u64::try_from(req.voter_id) else {
        return encode_resp(
            version,
            &UpdateRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                ..Default::default()
            },
        );
    };

    let voter = Voter {
        id,
        directory_id: uuid::Uuid::from_bytes(req.voter_directory_id.0),
        endpoints: req
            .listeners
            .into_iter()
            .map(|l| VoterEndpoint {
                name: l.name,
                host: l.host,
                port: l.port,
            })
            .collect(),
        kraft_version: crabka_metadata::KRaftVersionRange::default(),
    };

    let (error_code, _msg) =
        outcome_to_code(broker.controller.update_voter(UpdateVoter { voter }).await);

    encode_resp(
        version,
        &UpdateRaftVoterResponse {
            error_code,
            ..Default::default()
        },
    )
}

fn encode_resp(version: i16, resp: &UpdateRaftVoterResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::owned::update_raft_voter_request::{KRaftVersionFeature, Listener};
    use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::test_support::DenyAll;

    fn request(voter_id: i32) -> UpdateRaftVoterRequest {
        UpdateRaftVoterRequest {
            cluster_id: Some("cluster".into()),
            current_leader_epoch: 1,
            voter_id,
            voter_directory_id: ProtoUuid([4; 16]),
            listeners: vec![Listener {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9093,
                ..Default::default()
            }],
            k_raft_version_feature: KRaftVersionFeature {
                min_supported_version: 1,
                max_supported_version: 1,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        UpdateRaftVoterRequest,
        UpdateRaftVoterResponse,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer as start_broker;

    /// Decode→encode round-trip at min and max versions.
    #[test]
    fn response_round_trips_at_min_and_max_versions() {
        use crabka_protocol::owned::update_raft_voter_response::{self, UpdateRaftVoterResponse};
        for version in [
            update_raft_voter_response::MIN_VERSION,
            update_raft_voter_response::MAX_VERSION,
        ] {
            let resp = UpdateRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                ..Default::default()
            };
            let bytes = encode_resp(version, &resp).expect("encode");
            let mut cur: &[u8] = &bytes;
            let decoded = UpdateRaftVoterResponse::decode(&mut cur, version).expect("decode");
            assert!(decoded.error_code == codes::INVALID_REQUEST);
            assert!(cur.is_empty(), "all bytes consumed at v{version}");
        }
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_without_calling_reconfig() {
        let version = 0;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request(2), version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_negative_voter_id_before_reconfig() {
        let version = 0;
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
        let req_bytes = encode_request(&request(-7), version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(resp.error_code == codes::INVALID_REQUEST);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_reports_reconfig_error_from_controller() {
        let version = 0;
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
        let req_bytes = encode_request(&request(2), version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(resp.error_code == codes::UNKNOWN_SERVER_ERROR);
        broker_handle.shutdown().await;
    }
}
