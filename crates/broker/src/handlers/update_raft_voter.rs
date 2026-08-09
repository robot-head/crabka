//! `UpdateRaftVoter` (`api_key=82`, KIP-853). Admin RPC that rewrites an
//! existing voter's listeners and its supported `kraft.version` range.
//!
//! ## ACL
//!
//! `Alter` on `Cluster("kafka-cluster")`. Deny → whole-response
//! `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! The outcome → error code mapping is shared with
//! [`super::add_raft_voter`]. `UpdateVoter` never returns
//! `VoterNotCaughtUp`. An unknown voter id comes back as
//! `ReconfigRejected → INVALID_REQUEST`.

use bytes::Bytes;
use crabka_metadata::{Voter, VoterEndpoint};
use crabka_protocol::{
    Decode,
    owned::{
        update_raft_voter_request::UpdateRaftVoterRequest,
        update_raft_voter_response::UpdateRaftVoterResponse,
    },
};
use crabka_raft::reconfig::UpdateVoter;

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{add_raft_voter::outcome_to_code, cluster_alter_denied},
};

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

    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return encode_resp(
            version,
            &UpdateRaftVoterResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            },
        );
    }

    let cluster_id = image.cluster_id().to_string();
    let quorum = broker.controller.quorum_state();
    if req.cluster_id.as_deref() != Some(cluster_id.as_str())
        || req.voter_directory_id == crabka_protocol::primitives::uuid::Uuid::ZERO
        || i64::from(req.current_leader_epoch)
            != i64::try_from(quorum.current_term).unwrap_or(i64::MAX)
        || req.listeners.is_empty()
        || req.listeners.iter().any(|listener| {
            listener.name.is_empty() || listener.host.is_empty() || listener.port == 0
        })
    {
        return encode_resp(
            version,
            &UpdateRaftVoterResponse {
                error_code: codes::INVALID_UPDATE,
                ..Default::default()
            },
        );
    }

    let Ok(min_version) = u16::try_from(req.k_raft_version_feature.min_supported_version) else {
        return encode_resp(
            version,
            &UpdateRaftVoterResponse {
                error_code: codes::INVALID_UPDATE,
                ..Default::default()
            },
        );
    };
    let Ok(max_version) = u16::try_from(req.k_raft_version_feature.max_supported_version) else {
        return encode_resp(
            version,
            &UpdateRaftVoterResponse {
                error_code: codes::INVALID_UPDATE,
                ..Default::default()
            },
        );
    };
    if min_version > max_version {
        return encode_resp(
            version,
            &UpdateRaftVoterResponse {
                error_code: codes::INVALID_UPDATE,
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
        id: crabka_raft::NodeId(id),
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
        kraft_version: crabka_metadata::KRaftVersionRange {
            min: min_version,
            max: max_version,
        },
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
    crate::handlers::encode_response(resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use crabka_protocol::{
        owned::update_raft_voter_request::{KRaftVersionFeature, Listener},
        primitives::uuid::Uuid as ProtoUuid,
    };
    use crabka_security::{AuthMethod, Principal};

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

    use super::*;
    use crate::test_support::start_broker_with_authorizer as start_broker;

    /// Decode and encode round-trip at the min and max versions.
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
        let mut request = request(-7);
        request.cluster_id = Some(broker.controller.current_image().cluster_id().to_string());
        request.current_leader_epoch =
            i32::try_from(broker.controller.quorum_state().current_term).unwrap_or(i32::MAX);
        let req_bytes = encode_request(&request, version);

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
        let mut request = request(2);
        request.cluster_id = Some(broker.controller.current_image().cluster_id().to_string());
        request.current_leader_epoch =
            i32::try_from(broker.controller.quorum_state().current_term).unwrap_or(i32::MAX);
        let req_bytes = encode_request(&request, version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(resp.error_code == codes::VOTER_NOT_FOUND);
        broker_handle.shutdown().await;
    }
}
