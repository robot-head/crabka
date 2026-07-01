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
            resource_name: "kafka-cluster",
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
            assert!(decoded.error_code == codes::INVALID_REQUEST);
            assert!(cur.is_empty(), "all bytes consumed at v{version}");
        }
    }
}
