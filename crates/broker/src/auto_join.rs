//! KIP-853 controller auto-join.
//!
//! A broker started in [`crate::BootstrapMode::Join`] with `auto_join = true`
//! is NOT yet a member of the controller raft group. Its raft log is empty and
//! it waits in openraft's `Learner` state. This module drives the joiner side.
//! It discovers the leader through the configured `bootstrap_servers` and sends
//! the **Kafka `AddRaftVoter` wire RPC**, `api_key` 80, with its own voter
//! identity. The leader-side handler `crate::handlers::add_raft_voter` runs
//! `add_learner`, which dials this joiner's controller listener to replicate the
//! log. The handler then waits for the observer to catch up, promotes it with
//! `change_membership`, and submits the authoritative `V1Voters` record. The
//! joiner stops once it sees itself in the committed voter set.
//!
//! The joiner advertises its **real bound** controller endpoint, so the leader's
//! `add_learner` can dial it back. It does not advertise the configured
//! `controller_listen_addr`, which can carry port 0 for an OS-assigned port.
//!
//! This module is only a client-side driver. It does NOT touch the
//! reconfiguration coordinator or openraft membership directly. All the lockstep
//! safety lives on the leader.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        add_raft_voter_request::{self, AddRaftVoterRequest, Listener},
        add_raft_voter_response::AddRaftVoterResponse,
    },
};
use crabka_units::{Time, convert::TimeExt as _};

use crate::codes;

/// Everything the auto-join driver needs, taken from `BrokerConfig` and
/// `Broker`.
///
/// This struct lets the caller spawn the loop *before* the full `Broker` Arc
/// exists. A `Join` broker's `Broker::start` blocks and waits for a leader. That
/// leader appears only after this loop drives the leader-side `add_learner` and
/// the promotion, so the two must run at the same time.
pub(crate) struct AutoJoinParams {
    pub auto_join: bool,
    pub retry_backoff: Time,
    pub voter_request_timeout: Time,
    pub node_id: crabka_raft::NodeId,
    pub directory_id: uuid::Uuid,
    pub cluster_id: Option<uuid::Uuid>,
    pub bootstrap_servers: Vec<std::net::SocketAddr>,
    /// Protocol of the bootstrap server's data-plane listener, that is, the
    /// inter-broker listener protocol. `AddRaftVoter` is served there.
    pub listener_protocol: crabka_security::ListenerProtocol,
    pub inter_broker_server_name: String,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
}

/// Drive the auto-join loop.
///
/// The function returns immediately, and does not touch the network, when
/// `auto_join` is disabled. If not, it loops until this broker appears in the
/// committed voter set, and it rotates across `bootstrap_servers`. The caller
/// spawns it as a detached background task during `Broker::start`.
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
        if controller.current_image().voters().contains(self_id) {
            tracing::info!(node_id = self_id.0, "auto-join complete; node is a voter");
            return;
        }

        let target = select_bootstrap_server(&bootstrap_servers, next_server);
        next_server = next_server.wrapping_add(1);

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

fn controller_listener(bound: std::net::SocketAddr) -> Listener {
    Listener {
        name: "CONTROLLER".to_string(),
        host: bound.ip().to_string(),
        port: bound.port(),
        ..Default::default()
    }
}

fn select_bootstrap_server(
    bootstrap_servers: &[std::net::SocketAddr],
    attempt: usize,
) -> std::net::SocketAddr {
    bootstrap_servers[attempt % bootstrap_servers.len()]
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
        ack_when_committed: true,
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

/// Log the leader's `AddRaftVoter` reply at the correct level.
///
/// No outcome ends the loop. The `voters().contains` check at the top of `run`
/// is the only exit, so this function is diagnostic only.
fn log_join_outcome(
    self_id: crabka_raft::NodeId,
    target: std::net::SocketAddr,
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

/// Dial `target`'s controller listener and send one `AddRaftVoter` request.
///
/// The function terminates TLS and SASL as the protocol demands, and returns the
/// decoded response. It opens a new connection for each attempt, which mirrors
/// `Controller::forward_submit_to`.
async fn send_add_raft_voter(
    client: &crate::network::client::InterBrokerClient,
    protocol: crabka_security::ListenerProtocol,
    server_name: &str,
    target: std::net::SocketAddr,
    req: &AddRaftVoterRequest,
) -> Result<AddRaftVoterResponse, String> {
    let version = add_raft_voter_request::MAX_VERSION;

    let mut body = BytesMut::with_capacity(req.encoded_len(version));
    req.encode(&mut body, version)
        .map_err(|e| format!("AddRaftVoter encode: {e}"))?;

    let opts = auto_join_connection_options();
    let conn = client
        .connect_as_connection(
            &target.ip().to_string(),
            target.port(),
            protocol,
            server_name,
            opts,
        )
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
        let servers: Vec<std::net::SocketAddr> =
            ["127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"]
                .into_iter()
                .map(|s| s.parse().unwrap())
                .collect();

        assert_eq!(select_bootstrap_server(&servers, 0), servers[0]);
        assert_eq!(select_bootstrap_server(&servers, 2), servers[2]);
        assert_eq!(select_bootstrap_server(&servers, 3), servers[0]);
        assert_eq!(select_bootstrap_server(&servers, 5), servers[2]);
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
        assert!(req.ack_when_committed);
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

        assert!(decoded.ack_when_committed);
    }

    #[test]
    fn auto_join_connection_options_uses_joiner_client_id() {
        let opts = auto_join_connection_options();

        assert_eq!(opts.client_id, "crabka-auto-join");
    }

    #[test]
    fn log_join_outcome_classifies_response_codes() {
        let target = "127.0.0.1:9092".parse().unwrap();
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

        let client = crate::network::client::InterBrokerClient::new(None, None);
        let req = AddRaftVoterRequest::default();
        let err = send_add_raft_voter(
            &client,
            crabka_security::ListenerProtocol::Plaintext,
            "broker.internal",
            target,
            &req,
        )
        .await
        .expect_err("closed port must not produce a successful default response");
        assert!(err.contains("dial"), "unexpected error: {err}");
    }

    /// `run` returns immediately when `auto_join` is disabled, with no panic
    /// and no network dial.
    ///
    /// The test builds params with a real controller and inter-broker client,
    /// `auto_join = false`, and a deliberately bogus bootstrap server. If `run`
    /// obeys the flag it never dials. If it regressed and dialed, the loop would
    /// spin against the unreachable address and the timeout would fire, which
    /// fails the test.
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
            bootstrap_servers: vec!["127.0.0.1:1".parse().unwrap()],
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
            bootstrap_servers: vec!["127.0.0.1:1".parse().unwrap()],
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
