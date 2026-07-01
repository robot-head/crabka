//! `AddRaftVoter` (`api_key=80`, KIP-853). Admin RPC that promotes a
//! caught-up observer into the controller-raft voter set.
//!
//! ## ACL
//!
//! `Alter` on `Cluster("kafka-cluster")`. Deny → whole-response
//! `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! ## Outcome → error code (mirrors KIP-853 / JVM `KafkaApis`)
//!
//! - `Committed` → `NONE (0)`
//! - `NotLeader` → `NOT_LEADER_OR_FOLLOWER (6)` (client retries on the leader)
//! - `VoterNotCaughtUp` → `INVALID_REQUEST (42)`
//! - `ReconfigInProgress` → `REQUEST_TIMED_OUT (7)` (another reconfig holds
//!   the serialization lock; the client should retry)
//! - `ReconfigRejected` → `INVALID_REQUEST (42)`
//! - any other raft error → `UNKNOWN_SERVER_ERROR (-1)`

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType, Voter, VoterEndpoint};
use crabka_protocol::owned::add_raft_voter_request::AddRaftVoterRequest;
use crabka_protocol::owned::add_raft_voter_response::AddRaftVoterResponse;
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;
use crabka_raft::reconfig::{AddVoter, ReconfigOutcome};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_add_raft_voter",
    level = "info",
    skip_all,
    fields(api = "AddRaftVoter", version, req_bytes = req_bytes.len()),
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
    let req = AddRaftVoterRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // Cluster:Alter gate — KIP-853 reconfiguration is a cluster-wide
    // mutation, same gate as UnregisterBroker.
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
            &AddRaftVoterResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("add-raft-voter denied".into()),
                ..Default::default()
            },
        );
    }

    // Voter ids are non-negative; the wire field is signed.
    let Ok(id) = u64::try_from(req.voter_id) else {
        return encode_resp(
            version,
            &AddRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some(format!(
                    "voter_id must be non-negative, got {}",
                    req.voter_id
                )),
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

    let (error_code, error_message) =
        outcome_to_code(broker.controller.add_voter(AddVoter { voter }).await);

    encode_resp(
        version,
        &AddRaftVoterResponse {
            error_code,
            error_message,
            ..Default::default()
        },
    )
}

/// Map a coordinator outcome / raft error onto a Kafka error code +
/// optional message. Shared by the Add/Remove/Update handlers (Update can
/// never surface `VoterNotCaughtUp`, but the arm is harmless there).
pub(crate) fn outcome_to_code(
    outcome: Result<ReconfigOutcome, RaftError>,
) -> (i16, Option<String>) {
    match outcome {
        Ok(ReconfigOutcome::Committed) => (codes::NONE, None),
        Ok(ReconfigOutcome::NotLeader { leader }) => (
            codes::NOT_LEADER_OR_FOLLOWER,
            Some(match leader {
                Some(id) => format!("not the raft leader; current leader is {id}"),
                None => "not the raft leader; leader currently unknown".into(),
            }),
        ),
        Err(RaftError::VoterNotCaughtUp { id, lag }) => (
            codes::INVALID_REQUEST,
            Some(format!("voter {id} not caught up (lag {lag})")),
        ),
        Err(RaftError::ReconfigInProgress) => (
            codes::REQUEST_TIMED_OUT,
            Some("another reconfiguration is in progress".into()),
        ),
        Err(RaftError::ReconfigRejected(why)) => (codes::INVALID_REQUEST, Some(why)),
        Err(e) => (codes::UNKNOWN_SERVER_ERROR, Some(e.to_string())),
    }
}

fn encode_resp(version: i16, resp: &AddRaftVoterResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn committed_maps_to_none() {
        let (code, msg) = outcome_to_code(Ok(ReconfigOutcome::Committed));
        assert!(code == codes::NONE);
        assert!(msg.is_none());
    }

    #[test]
    fn not_leader_maps_to_not_leader_or_follower() {
        let (code, msg) = outcome_to_code(Ok(ReconfigOutcome::NotLeader { leader: Some(3) }));
        assert!(code == codes::NOT_LEADER_OR_FOLLOWER);
        assert!(msg.unwrap().contains('3'));
    }

    #[test]
    fn not_caught_up_maps_to_invalid_request() {
        let (code, _) = outcome_to_code(Err(RaftError::VoterNotCaughtUp { id: 7, lag: 99 }));
        assert!(code == codes::INVALID_REQUEST);
    }

    #[test]
    fn in_progress_maps_to_request_timed_out() {
        let (code, _) = outcome_to_code(Err(RaftError::ReconfigInProgress));
        assert!(code == codes::REQUEST_TIMED_OUT);
    }

    #[test]
    fn rejected_maps_to_invalid_request_with_reason() {
        let (code, msg) = outcome_to_code(Err(RaftError::ReconfigRejected("nope".into())));
        assert!(code == codes::INVALID_REQUEST);
        assert!(msg.as_deref() == Some("nope"));
    }

    /// Decode→encode round-trip at min and max versions. Guards against
    /// the response failing to encode at either end of the version range
    /// the schema declares.
    #[test]
    fn response_round_trips_at_min_and_max_versions() {
        use crabka_protocol::owned::add_raft_voter_response::{self, AddRaftVoterResponse};
        for version in [
            add_raft_voter_response::MIN_VERSION,
            add_raft_voter_response::MAX_VERSION,
        ] {
            let resp = AddRaftVoterResponse {
                error_code: codes::NOT_LEADER_OR_FOLLOWER,
                error_message: Some("not the raft leader".into()),
                ..Default::default()
            };
            let bytes = encode_resp(version, &resp).expect("encode");
            let mut cur: &[u8] = &bytes;
            let decoded = AddRaftVoterResponse::decode(&mut cur, version).expect("decode");
            assert!(decoded.error_code == codes::NOT_LEADER_OR_FOLLOWER);
            assert!(cur.is_empty(), "all bytes consumed at v{version}");
        }
    }
}
