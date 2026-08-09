//! KIP-853 controller auto-join.
//!
//! A broker started in [`crate::BootstrapMode::Join`] with
//! `auto_join = true` is NOT yet a member of the controller raft group: its
//! Raft log is empty and it waits as an observer. This module
//! drives the joiner side of the dance — it discovers the leader via the
//! configured `bootstrap_servers` and sends the **Kafka `AddRaftVoter` wire
//! RPC** (`api_key` 80) carrying its own voter identity. The leader-side
//! handler (`crate::handlers::add_raft_voter`) waits for the observer to catch
//! up and appends the authoritative `VotersRecord`. Once the joiner sees its
//! exact node and directory identity in the committed voter set it stops.
//!
//! The joiner advertises its **real bound** controller endpoint (not the
//! configured `controller_listen_addr`, which may carry port 0 for an
//! OS-assigned port) so the leader's `add_learner` can dial it back.
//!
//! This is purely a client-side driver: it does NOT touch the reconfiguration
//! Raft state directly. All lockstep safety lives in the leader's single-owner
//! Raft engine.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        add_raft_voter_request::{self, AddRaftVoterRequest, Listener},
        add_raft_voter_response::AddRaftVoterResponse,
        remove_raft_voter_request::{self, RemoveRaftVoterRequest},
        remove_raft_voter_response::RemoveRaftVoterResponse,
        update_raft_voter_request::{
            self, KRaftVersionFeature, Listener as UpdateListener, UpdateRaftVoterRequest,
        },
        update_raft_voter_response::UpdateRaftVoterResponse,
    },
};
use crabka_units::{Time, convert::TimeExt as _};

use crate::codes;

/// Everything the auto-join driver needs, pulled out of `BrokerConfig` +
/// `Broker` so the loop can be spawned *before* the full `Broker` Arc exists.
/// A `Join` broker's `Broker::start` blocks waiting for a leader, and that
/// leader only appears once this loop has driven the leader-side `add_learner`
/// + promotion — so the two must run concurrently.
#[derive(Clone)]
pub(crate) struct AutoJoinParams {
    pub auto_join: bool,
    pub retry_backoff: Time,
    pub voter_request_timeout: Time,
    pub node_id: crabka_raft::NodeId,
    pub directory_id: uuid::Uuid,
    pub cluster_id: Option<uuid::Uuid>,
    pub bootstrap_servers: Vec<String>,
    /// Protocol of the bootstrap server's controller listener.
    pub listener_protocol: crabka_security::ListenerProtocol,
    pub inter_broker_server_name: String,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
}

/// Advertise this controller at startup and after each leader change. The
/// leader accepts this at both `kraft.version` levels; level zero keeps the
/// data in memory for upgrade preflight, while level one persists it.
pub(crate) async fn run_voter_updates(params: AutoJoinParams) {
    if params.bootstrap_servers.is_empty() {
        tracing::debug!(
            node_id = params.node_id.0,
            "no bootstrap server is available for UpdateVoter"
        );
        return;
    }
    let Ok(voter_id) = i32::try_from(params.node_id.0) else {
        tracing::error!(
            node_id = params.node_id.0,
            "node_id exceeds i32; cannot update voter"
        );
        return;
    };
    let listener = controller_listener(params.controller.controller_bound_addr());
    let mut last_updated = None;
    let mut next_server = 0usize;
    loop {
        let quorum = params.controller.quorum_state();
        let leader = quorum.current_leader;
        let epoch = i32::try_from(quorum.current_term).unwrap_or(i32::MAX);
        if leader.is_some() && last_updated != Some((leader, epoch)) {
            let target = select_bootstrap_server(&params.bootstrap_servers, next_server);
            next_server = next_server.wrapping_add(1);
            let request = UpdateRaftVoterRequest {
                cluster_id: params.cluster_id.map(|id| id.to_string()),
                current_leader_epoch: epoch,
                voter_id,
                voter_directory_id: crabka_protocol::primitives::uuid::Uuid(
                    *params.directory_id.as_bytes(),
                ),
                listeners: vec![UpdateListener {
                    name: listener.name.clone(),
                    host: listener.host.clone(),
                    port: listener.port,
                    ..Default::default()
                }],
                k_raft_version_feature: KRaftVersionFeature {
                    min_supported_version: 0,
                    max_supported_version: 1,
                    ..Default::default()
                },
                ..Default::default()
            };
            match send_update_voter(
                &params.inter_broker_client,
                params.listener_protocol,
                &params.inter_broker_server_name,
                target,
                &request,
            )
            .await
            {
                Ok(response) if response.error_code == codes::NONE => {
                    last_updated = Some((leader, epoch));
                }
                Ok(response) => tracing::debug!(
                    node_id = params.node_id.0,
                    server = %target,
                    error_code = response.error_code,
                    "UpdateVoter was not acknowledged; retrying"
                ),
                Err(error) => tracing::debug!(
                    node_id = params.node_id.0,
                    server = %target,
                    %error,
                    "UpdateVoter failed; retrying"
                ),
            }
        }
        tokio::time::sleep(params.retry_backoff.to_std()).await;
    }
}

/// Drive the auto-join loop. Returns immediately (without touching the
/// network) when `auto_join` is disabled. Otherwise loops until this broker
/// appears in the committed voter set, rotating across `bootstrap_servers`.
/// Intended to be spawned as a detached background task during `Broker::start`.
pub(crate) async fn run(params: AutoJoinParams) {
    if !params.auto_join {
        return;
    }

    let self_id = params.node_id;
    let bootstrap_servers = params.bootstrap_servers;
    if bootstrap_servers.is_empty() {
        tracing::warn!(
            node_id = self_id.0,
            "auto_join enabled but bootstrap_servers is empty; cannot discover a leader"
        );
        return;
    }

    // Self's voter identity, advertising the REAL bound controller endpoint
    // (resolved port, not the possibly-zero configured port) so the leader's
    // add_learner can dial us back.
    let bound = params.controller.controller_bound_addr();
    let Ok(voter_id) = i32::try_from(self_id.0) else {
        tracing::error!(node_id = self_id.0, "node_id exceeds i32; cannot auto-join");
        return;
    };
    let directory_id = crabka_protocol::primitives::uuid::Uuid(*params.directory_id.as_bytes());
    let listener = controller_listener(bound);

    let protocol = params.listener_protocol;
    let server_name = params.inter_broker_server_name;
    let retry_backoff = params.retry_backoff;
    let Ok(voter_request_timeout_ms) = i32::try_from(params.voter_request_timeout.millis_i64())
    else {
        tracing::error!(
            timeout = ?params.voter_request_timeout,
            "auto-join voter request timeout exceeds Kafka wire limit"
        );
        return;
    };
    let client = params.inter_broker_client;
    let controller = params.controller;
    let cluster_id = params.cluster_id;

    let mut next_server = 0usize;
    loop {
        // Terminate as soon as the committed voter set includes us.
        if let Some(existing) = controller.current_image().voters().get(self_id)
            && existing.directory_id == params.directory_id
        {
            tracing::info!(node_id = self_id.0, "auto-join complete; node is a voter");
            return;
        }

        let target = select_bootstrap_server(&bootstrap_servers, next_server);
        next_server = next_server.wrapping_add(1);

        if let Some(existing) = controller.current_image().voters().get(self_id)
            && existing.directory_id != params.directory_id
        {
            let req = RemoveRaftVoterRequest {
                cluster_id: cluster_id.map(|id| id.to_string()),
                voter_id,
                voter_directory_id: crabka_protocol::primitives::uuid::Uuid(
                    *existing.directory_id.as_bytes(),
                ),
                ..Default::default()
            };
            if let Err(error) =
                send_remove_raft_voter(&client, protocol, &server_name, target, &req).await
            {
                tracing::debug!(node_id = self_id.0, server = %target, %error, "auto-join: stale voter removal failed");
            }
            tokio::time::sleep(retry_backoff.to_std()).await;
            continue;
        }

        let req = build_add_raft_voter_request(
            cluster_id,
            voter_id,
            directory_id,
            listener.clone(),
            voter_request_timeout_ms,
        );

        match send_add_raft_voter(&client, protocol, &server_name, target, &req).await {
            Ok(resp) => {
                let _: JoinOutcome = log_join_outcome(self_id, target, &resp);
            }
            Err(e) => {
                tracing::debug!(
                    node_id = self_id.0,
                    server = %target,
                    error = %e,
                    "auto-join: dial/RPC failed; trying next bootstrap server"
                );
            }
        }

        tokio::time::sleep(retry_backoff.to_std()).await;
    }
}

async fn send_remove_raft_voter(
    client: &crate::network::client::InterBrokerClient,
    protocol: crabka_security::ListenerProtocol,
    server_name: &str,
    target: &str,
    req: &RemoveRaftVoterRequest,
) -> Result<RemoveRaftVoterResponse, String> {
    let version = remove_raft_voter_request::MAX_VERSION;
    let mut body = BytesMut::with_capacity(req.encoded_len(version));
    req.encode(&mut body, version)
        .map_err(|error| format!("RemoveRaftVoter encode: {error}"))?;
    let (host, port) = split_bootstrap_server(target)?;
    let connection = client
        .connect_as_connection(
            host,
            port,
            protocol,
            server_name,
            auto_join_connection_options(),
        )
        .await
        .map_err(|error| format!("dial {target}: {error}"))?;
    let response = connection
        .raw_request(
            remove_raft_voter_request::API_KEY,
            version,
            Bytes::from(body),
        )
        .await
        .map_err(|error| format!("RemoveRaftVoter raw_request: {error}"));
    connection.close();
    let response = response?;
    let mut cursor: &[u8] = &response;
    RemoveRaftVoterResponse::decode(&mut cursor, version)
        .map_err(|error| format!("RemoveRaftVoter decode: {error}"))
}

async fn send_update_voter(
    client: &crate::network::client::InterBrokerClient,
    protocol: crabka_security::ListenerProtocol,
    server_name: &str,
    target: &str,
    request: &UpdateRaftVoterRequest,
) -> Result<UpdateRaftVoterResponse, String> {
    let version = update_raft_voter_request::MAX_VERSION;
    let mut body = BytesMut::with_capacity(request.encoded_len(version));
    request
        .encode(&mut body, version)
        .map_err(|error| format!("UpdateVoter encode: {error}"))?;
    let (host, port) = split_bootstrap_server(target)?;
    let connection = client
        .connect_as_connection(
            host,
            port,
            protocol,
            server_name,
            auto_join_connection_options(),
        )
        .await
        .map_err(|error| format!("dial {target}: {error}"))?;
    let response = connection
        .raw_request(
            update_raft_voter_request::API_KEY,
            version,
            Bytes::from(body),
        )
        .await
        .map_err(|error| format!("UpdateVoter raw_request: {error}"));
    connection.close();
    let response = response?;
    UpdateRaftVoterResponse::decode(&mut response.as_ref(), version)
        .map_err(|error| format!("UpdateVoter decode: {error}"))
}

fn controller_listener(bound: std::net::SocketAddr) -> Listener {
    let host = if bound.ip().is_unspecified() {
        std::env::var("HOSTNAME").unwrap_or_else(|_| "127.0.0.1".to_string())
    } else {
        bound.ip().to_string()
    };
    Listener {
        name: "CONTROLLER".to_string(),
        host,
        port: bound.port(),
        ..Default::default()
    }
}

fn select_bootstrap_server(bootstrap_servers: &[String], attempt: usize) -> &str {
    &bootstrap_servers[attempt % bootstrap_servers.len()]
}

fn build_add_raft_voter_request(
    cluster_id: Option<uuid::Uuid>,
    voter_id: i32,
    directory_id: crabka_protocol::primitives::uuid::Uuid,
    listener: Listener,
    timeout_ms: i32,
) -> AddRaftVoterRequest {
    AddRaftVoterRequest {
        cluster_id: cluster_id.map(|u| u.to_string()),
        timeout_ms,
        voter_id,
        voter_directory_id: directory_id,
        listeners: vec![listener],
        ack_when_committed: false,
        unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinOutcome {
    Accepted,
    NotLeader,
    TimedOut,
    NotCaughtUp,
    Unexpected(i16),
}

/// Log the leader's `AddRaftVoter` reply at the appropriate level. None of the
/// outcomes terminate the loop — the `voters().contains` check at the top of
/// `run` is the sole exit — so this is purely diagnostic.
fn log_join_outcome(
    self_id: crabka_raft::NodeId,
    target: &str,
    resp: &AddRaftVoterResponse,
) -> JoinOutcome {
    match resp.error_code {
        codes::NONE => {
            tracing::info!(
                node_id = self_id.0,
                leader = %target,
                "auto-join accepted by leader"
            );
            // The committed V1Voters record may not be visible in our local
            // image yet (we're still catching up); the next loop iteration's
            // `voters().contains` check confirms before exiting.
            JoinOutcome::Accepted
        }
        codes::NOT_LEADER_OR_FOLLOWER => {
            // Not the leader. The error message may name the current leader,
            // but it isn't a routable address — fall back to rotating across
            // the configured bootstrap servers.
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join target is not the leader; trying next bootstrap server"
            );
            JoinOutcome::NotLeader
        }
        codes::REQUEST_TIMED_OUT => {
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                "auto-join: reconfiguration in progress on leader; retrying"
            );
            JoinOutcome::TimedOut
        }
        codes::INVALID_REQUEST => {
            // Observer not yet caught up within the lag bound. Keep replicating
            // (openraft is doing that in the background) and retry shortly.
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join: not yet caught up; retrying"
            );
            JoinOutcome::NotCaughtUp
        }
        other => {
            tracing::warn!(
                node_id = self_id.0,
                server = %target,
                error_code = other,
                msg = ?resp.error_message,
                "auto-join: unexpected error_code; retrying"
            );
            JoinOutcome::Unexpected(other)
        }
    }
}

/// Dial `target`'s controller listener (terminating TLS / SASL as the
/// protocol demands) and send a single `AddRaftVoter` request, returning the
/// decoded response. A fresh connection per attempt mirrors
/// `Controller::forward_submit_to`.
async fn send_add_raft_voter(
    client: &crate::network::client::InterBrokerClient,
    protocol: crabka_security::ListenerProtocol,
    server_name: &str,
    target: &str,
    req: &AddRaftVoterRequest,
) -> Result<AddRaftVoterResponse, String> {
    let version = add_raft_voter_request::MAX_VERSION;

    let mut body = BytesMut::with_capacity(req.encoded_len(version));
    req.encode(&mut body, version)
        .map_err(|e| format!("AddRaftVoter encode: {e}"))?;

    let (host, port) = split_bootstrap_server(target)?;
    let opts = auto_join_connection_options();
    let conn = client
        .connect_as_connection(host, port, protocol, server_name, opts)
        .await
        .map_err(|e| format!("dial {target}: {e}"))?;

    let resp_body = conn
        .raw_request(add_raft_voter_request::API_KEY, version, Bytes::from(body))
        .await
        .map_err(|e| format!("AddRaftVoter raw_request: {e}"));
    conn.close();
    let resp_body = resp_body?;

    let mut cur: &[u8] = &resp_body;
    AddRaftVoterResponse::decode(&mut cur, version).map_err(|e| format!("AddRaftVoter decode: {e}"))
}

fn split_bootstrap_server(target: &str) -> Result<(&str, u16), String> {
    let (host, port) = target
        .rsplit_once(':')
        .ok_or_else(|| format!("bootstrap server {target:?} must use <host>:<port>"))?;
    let port = port
        .parse::<u16>()
        .map_err(|error| format!("invalid bootstrap server port in {target:?}: {error}"))?;
    Ok((host.trim_matches(['[', ']']), port))
}

fn auto_join_connection_options() -> crabka_client_core::ConnectionOptions {
    crabka_client_core::ConnectionOptions {
        client_id: "crabka-auto-join".to_string(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use assert2::assert;
    use crabka_metadata::{
        KRaftVersionRange, MetadataImage, MetadataRecord, Voter, VoterEndpoint, VoterSet,
        VotersRecord,
    };
    use crabka_raft::{
        AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter,
        SnapshotRange, UpdateVoter,
    };
    use crabka_units::{millis, secs};
    use tokio::sync::watch;

    use super::*;

    struct MockSource {
        image: Arc<MetadataImage>,
        current_image_calls: AtomicUsize,
        controller_bound_addr_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for MockSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.current_image_calls.fetch_add(1, Ordering::Relaxed);
            self.image.clone()
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            let (_tx, rx) = watch::channel(self.image.clone());
            rx
        }

        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            let (_tx, rx) = watch::channel(None);
            rx
        }

        fn quorum_state(&self) -> QuorumState {
            panic!("not used by auto_join tests")
        }

        async fn submit_change(
            &self,
            _records: Vec<crabka_metadata::MetadataRecord>,
        ) -> Result<crabka_raft::SubmitChangeResult, RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            panic!("not used by auto_join tests")
        }

        fn controller_bound_addr(&self) -> SocketAddr {
            self.controller_bound_addr_calls
                .fetch_add(1, Ordering::Relaxed);
            "127.0.0.1:19093".parse().expect("bound controller addr")
        }

        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            panic!("not used by auto_join tests")
        }

        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("not used by auto_join tests")
        }

        async fn cancel(&self) {}
    }

    fn image_with_voter(node_id: NodeId) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: VoterSet::from_voters([Voter {
                id: node_id,
                directory_id: uuid::Uuid::from_u128(node_id.0.into()),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 19093,
                }],
                kraft_version: KRaftVersionRange::default(),
            }]),
        }));
        image
    }

    #[test]
    fn controller_listener_uses_bound_controller_endpoint() {
        let listener = controller_listener("192.0.2.10:19093".parse().unwrap());
        assert_eq!(listener.name, "CONTROLLER");
        assert_eq!(listener.host, "192.0.2.10");
        assert_eq!(listener.port, 19093);
    }

    #[test]
    fn select_bootstrap_server_wraps_attempts() {
        let servers: Vec<String> = ["127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"]
            .into_iter()
            .map(str::to_owned)
            .collect();

        assert_eq!(select_bootstrap_server(&servers, 0), servers[0].as_str());
        assert_eq!(select_bootstrap_server(&servers, 2), servers[2].as_str());
        assert_eq!(select_bootstrap_server(&servers, 3), servers[0].as_str());
        assert_eq!(select_bootstrap_server(&servers, 5), servers[2].as_str());
    }

    #[test]
    fn build_add_raft_voter_request_carries_joiner_identity() {
        let cluster_id = uuid::Uuid::from_u128(0xCAFE);
        let dir = uuid::Uuid::from_u128(0xD1E);
        let listener = controller_listener("127.0.0.1:19093".parse().unwrap());
        let req = build_add_raft_voter_request(
            Some(cluster_id),
            7,
            crabka_protocol::primitives::uuid::Uuid(*dir.as_bytes()),
            listener,
            1_234,
        );

        let cluster_id_string = cluster_id.to_string();
        assert!(matches!(
            (
                req.cluster_id.as_deref(),
                req.timeout_ms,
                req.voter_id,
                req.voter_directory_id.0,
                req.listeners.len(),
                req.listeners[0].name.as_str(),
                req.listeners[0].host.as_str(),
                req.listeners[0].port,
            ),
            (Some(id), 1_234, 7, directory_id, 1, "CONTROLLER", "127.0.0.1", 19093)
                if id == cluster_id_string && directory_id == *dir.as_bytes()
        ));
        assert!(!req.ack_when_committed);
    }

    #[test]
    fn build_add_raft_voter_request_encodes_ack_when_committed() {
        let listener = controller_listener("127.0.0.1:19093".parse().unwrap());
        let req = build_add_raft_voter_request(
            None,
            7,
            crabka_protocol::primitives::uuid::Uuid(*uuid::Uuid::from_u128(7).as_bytes()),
            listener,
            30_000,
        );
        let version = add_raft_voter_request::MAX_VERSION;
        let mut bytes = BytesMut::new();

        req.encode(&mut bytes, version).expect("encode request");
        let decoded =
            AddRaftVoterRequest::decode(&mut bytes.freeze(), version).expect("decode request");

        assert!(!decoded.ack_when_committed);
    }

    #[test]
    fn auto_join_connection_options_uses_joiner_client_id() {
        let opts = auto_join_connection_options();

        assert_eq!(opts.client_id, "crabka-auto-join");
    }

    #[test]
    fn log_join_outcome_classifies_response_codes() {
        let target = "127.0.0.1:9092";
        let response = |error_code| AddRaftVoterResponse {
            error_code,
            ..Default::default()
        };

        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(codes::NONE)),
            JoinOutcome::Accepted
        );
        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(codes::NOT_LEADER_OR_FOLLOWER)),
            JoinOutcome::NotLeader
        );
        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(codes::REQUEST_TIMED_OUT)),
            JoinOutcome::TimedOut
        );
        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(codes::INVALID_REQUEST)),
            JoinOutcome::NotCaughtUp
        );
        assert_eq!(
            log_join_outcome(NodeId(1), target, &response(1234)),
            JoinOutcome::Unexpected(1234)
        );
    }

    #[tokio::test]
    async fn send_add_raft_voter_errors_when_target_is_unreachable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let target = listener.local_addr().expect("local addr");
        drop(listener);
        let target = target.to_string();

        let client = crate::network::client::InterBrokerClient::new(None, None);
        let req = AddRaftVoterRequest::default();
        let err = send_add_raft_voter(
            &client,
            crabka_security::ListenerProtocol::Plaintext,
            "broker.internal",
            &target,
            &req,
        )
        .await
        .expect_err("closed port must not produce a successful default response");
        assert!(err.contains("dial"), "unexpected error: {err}");
    }

    /// `run` returns immediately when `auto_join` is disabled — no panic, no
    /// network dial. Build params with a real controller + inter-broker client
    /// but `auto_join = false`, and a deliberately bogus bootstrap server. If
    /// `run` honoured the flag it never dials; if it regressed and dialed, the
    /// loop would spin against the unreachable address and the timeout would
    /// fire (failing the test).
    #[tokio::test]
    async fn run_returns_immediately_when_auto_join_disabled() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let config = crate::BrokerConfig::for_tests(tempdir.path().to_path_buf());
        let handle = crate::Broker::start(config).await.expect("broker start");
        let broker = handle.broker_arc_for_test();

        let params = AutoJoinParams {
            auto_join: false,
            retry_backoff: millis(7),
            voter_request_timeout: secs(30),
            node_id: crabka_raft::NodeId(999),
            directory_id: uuid::Uuid::from_u128(1),
            cluster_id: None,
            // Unroutable: would hang the loop if `run` ignored auto_join=false.
            bootstrap_servers: vec!["127.0.0.1:1".to_string()],
            listener_protocol: crabka_security::ListenerProtocol::Plaintext,
            inter_broker_server_name: "broker.internal".to_string(),
            controller: broker.controller_for_test(),
            inter_broker_client: broker.inter_broker_client_for_test(),
        };

        tokio::time::timeout(Duration::from_secs(2), run(params))
            .await
            .expect("run() returned immediately for auto_join=false");

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn run_with_auto_join_true_checks_current_voter_set_before_returning() {
        let source = Arc::new(MockSource {
            image: Arc::new(image_with_voter(NodeId(7))),
            current_image_calls: AtomicUsize::new(0),
            controller_bound_addr_calls: AtomicUsize::new(0),
        });
        let params = AutoJoinParams {
            auto_join: true,
            retry_backoff: millis(7),
            voter_request_timeout: secs(30),
            node_id: crabka_raft::NodeId(7),
            directory_id: uuid::Uuid::from_u128(7),
            cluster_id: None,
            bootstrap_servers: vec!["127.0.0.1:1".to_string()],
            listener_protocol: crabka_security::ListenerProtocol::Plaintext,
            inter_broker_server_name: "broker.internal".to_string(),
            controller: source.clone(),
            inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(
                None, None,
            )),
        };

        tokio::time::timeout(Duration::from_secs(2), run(params))
            .await
            .expect("already-voter auto join returns without dialing");

        assert_eq!(
            source.controller_bound_addr_calls.load(Ordering::Relaxed),
            1
        );
        assert_eq!(source.current_image_calls.load(Ordering::Relaxed), 1);
    }
}
