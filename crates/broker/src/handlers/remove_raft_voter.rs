//! `RemoveRaftVoter` (`api_key=81`, KIP-853). Admin RPC that drops a voter
//! from the controller-raft voter set (refusing to remove the last voter).
//!
//! ## ACL
//!
//! `Alter` on `Cluster("kafka-cluster")`. Deny → whole-response
//! `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! Outcome → error code mapping is shared with [`super::add_raft_voter`].

use bytes::Bytes;
use crabka_protocol::{
    Decode,
    owned::{
        remove_raft_voter_request::RemoveRaftVoterRequest,
        remove_raft_voter_response::RemoveRaftVoterResponse,
    },
};
use crabka_raft::reconfig::RemoveVoter;

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{add_raft_voter::outcome_to_code, cluster_alter_denied},
};

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

    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
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
                id: crabka_raft::NodeId(id),
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
    crate::handlers::encode_response(resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
    use crabka_security::{AuthMethod, Principal};

    use crate::test_support::DenyAll;

    fn request(voter_id: i32) -> RemoveRaftVoterRequest {
        RemoveRaftVoterRequest {
            cluster_id: Some("cluster".into()),
            voter_id,
            voter_directory_id: ProtoUuid([3; 16]),
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        RemoveRaftVoterRequest,
        RemoveRaftVoterResponse,
        client_id = "admin-client"
    );

    use super::*;
    use crate::test_support::start_broker_with_authorizer as start_broker;

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
        let req_bytes = encode_request(&request(2), version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

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
        let req_bytes = encode_request(&request(-7), version);

        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);

        assert!(resp.error_code == codes::INVALID_REQUEST);
        assert!(
            resp.error_message.as_deref().is_some_and(|m| {
                m.contains("voter_id must be non-negative") && m.contains("-7")
            })
        );
        broker_handle.shutdown().await;
    }
}
