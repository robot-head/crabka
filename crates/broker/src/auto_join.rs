//! KIP-853 controller auto-join.
//!
//! A broker started in [`crate::BootstrapMode::Join`] with
//! `auto_join = true` is NOT yet a member of the controller raft group: its
//! raft log is empty and it waits in openraft's `Learner` state. This module
//! drives the joiner side of the dance — it discovers the leader via the
//! configured `bootstrap_servers` and sends the **Kafka `AddRaftVoter` wire
//! RPC** (`api_key` 80) carrying its own voter identity. The leader-side
//! handler (`crate::handlers::add_raft_voter`) runs `add_learner` (dialing
//! this joiner's controller listener to replicate the log), waits for the
//! observer to catch up, promotes it via `change_membership`, and submits the
//! authoritative `V1Voters` record. Once the joiner sees itself in the
//! committed voter set it stops.
//!
//! The joiner advertises its **real bound** controller endpoint (not the
//! configured `controller_listen_addr`, which may carry port 0 for an
//! OS-assigned port) so the leader's `add_learner` can dial it back.
//!
//! This is purely a client-side driver: it does NOT touch the reconfiguration
//! coordinator or openraft membership directly. All the lockstep safety lives
//! on the leader.

use std::{sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        add_raft_voter_request::{self, AddRaftVoterRequest, Listener},
        add_raft_voter_response::AddRaftVoterResponse,
    },
};

use crate::codes;

/// Backoff between join attempts so a failed dial / not-yet-caught-up reply
/// doesn't hot-spin the loop.
const RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Everything the auto-join driver needs, pulled out of `BrokerConfig` +
/// `Broker` so the loop can be spawned *before* the full `Broker` Arc exists.
/// A `Join` broker's `Broker::start` blocks waiting for a leader, and that
/// leader only appears once this loop has driven the leader-side `add_learner`
/// + promotion — so the two must run concurrently.
pub(crate) struct AutoJoinParams {
    pub auto_join: bool,
    pub node_id: crabka_raft::NodeId,
    pub directory_id: uuid::Uuid,
    pub cluster_id: Option<uuid::Uuid>,
    pub bootstrap_servers: Vec<std::net::SocketAddr>,
    /// Protocol of the bootstrap server's data-plane listener (the
    /// inter-broker listener protocol) — `AddRaftVoter` is served there.
    pub listener_protocol: crabka_security::ListenerProtocol,
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
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

        let req =
            build_add_raft_voter_request(cluster_id, voter_id, directory_id, listener.clone());

        match send_add_raft_voter(&client, protocol, target, &req).await {
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

        tokio::time::sleep(RETRY_BACKOFF).await;
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
) -> AddRaftVoterRequest {
    AddRaftVoterRequest {
        cluster_id: cluster_id.map(|u| u.to_string()),
        timeout_ms: 30_000,
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

/// Log the leader's `AddRaftVoter` reply at the appropriate level. None of the
/// outcomes terminate the loop — the `voters().contains` check at the top of
/// `run` is the sole exit — so this is purely diagnostic.
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

/// Dial `target`'s controller listener (terminating TLS / SASL as the
/// protocol demands) and send a single `AddRaftVoter` request, returning the
/// decoded response. A fresh connection per attempt mirrors
/// `Controller::forward_submit_to`.
async fn send_add_raft_voter(
    client: &crate::network::client::InterBrokerClient,
    protocol: crabka_security::ListenerProtocol,
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
            "localhost",
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
    let default = crabka_client_core::ConnectionOptions::default();
    crabka_client_core::ConnectionOptions {
        client_id: "crabka-auto-join".to_string(),
        connect_timeout: default.connect_timeout,
        request_timeout: default.request_timeout,
        security: default.security,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use crabka_metadata::{
        KRaftVersionRange, MetadataImage, MetadataRecord, Voter, VoterEndpoint, VoterSet,
        VotersRecord,
    };
    use crabka_raft::{
        AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter,
        SnapshotRange, UpdateVoter,
    };
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
        ) -> Result<(), RaftError> {
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
        assert2::assert!(
            listener
                == Listener {
                    name: "CONTROLLER".to_string(),
                    host: "192.0.2.10".to_string(),
                    port: 19093,
                    ..Default::default()
                }
        );
    }

    #[test]
    fn select_bootstrap_server_wraps_attempts() {
        let servers: Vec<std::net::SocketAddr> =
            ["127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"]
                .into_iter()
                .map(|s| s.parse().unwrap())
                .collect();

        for (_case, attempt, expected_index) in [
            ("first server", 0, 0),
            ("last server", 2, 2),
            ("wrap to first", 3, 0),
            ("wrap to last", 5, 2),
        ] {
            assert2::assert!(select_bootstrap_server(&servers, attempt) == servers[expected_index]);
        }
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
        );

        assert2::assert!(
            req == AddRaftVoterRequest {
                cluster_id: Some(cluster_id.to_string()),
                timeout_ms: 30_000,
                voter_id: 7,
                voter_directory_id: crabka_protocol::primitives::uuid::Uuid(*dir.as_bytes()),
                listeners: vec![Listener {
                    name: "CONTROLLER".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 19093,
                    ..Default::default()
                }],
                ack_when_committed: true,
                ..Default::default()
            }
        );
    }

    #[test]
    fn build_add_raft_voter_request_encodes_ack_when_committed() {
        let listener = controller_listener("127.0.0.1:19093".parse().unwrap());
        let req = build_add_raft_voter_request(
            None,
            7,
            crabka_protocol::primitives::uuid::Uuid(*uuid::Uuid::from_u128(7).as_bytes()),
            listener,
        );
        let version = add_raft_voter_request::MAX_VERSION;
        let mut bytes = BytesMut::new();

        req.encode(&mut bytes, version).expect("encode request");
        let decoded =
            AddRaftVoterRequest::decode(&mut bytes.freeze(), version).expect("decode request");

        assert2::assert!(decoded.ack_when_committed);
    }

    #[test]
    fn auto_join_connection_options_uses_joiner_client_id() {
        let opts = auto_join_connection_options();

        assert2::assert!(opts.client_id == "crabka-auto-join");
    }

    #[test]
    fn log_join_outcome_classifies_response_codes() {
        let target = "127.0.0.1:9092".parse().unwrap();
        let response = |error_code| AddRaftVoterResponse {
            error_code,
            ..Default::default()
        };

        for (_case, error_code, expected) in [
            ("accepted", codes::NONE, JoinOutcome::Accepted),
            (
                "not leader",
                codes::NOT_LEADER_OR_FOLLOWER,
                JoinOutcome::NotLeader,
            ),
            ("timed out", codes::REQUEST_TIMED_OUT, JoinOutcome::TimedOut),
            (
                "not caught up",
                codes::INVALID_REQUEST,
                JoinOutcome::NotCaughtUp,
            ),
            ("unexpected", 1234, JoinOutcome::Unexpected(1234)),
        ] {
            assert2::assert!(
                log_join_outcome(NodeId(1), target, &response(error_code)) == expected
            );
        }
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
            target,
            &req,
        )
        .await
        .expect_err("closed port must not produce a successful default response");
        assert2::assert!(err.contains("dial"));
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
            node_id: crabka_raft::NodeId(999),
            directory_id: uuid::Uuid::from_u128(1),
            cluster_id: None,
            // Unroutable: would hang the loop if `run` ignored auto_join=false.
            bootstrap_servers: vec!["127.0.0.1:1".parse().unwrap()],
            listener_protocol: crabka_security::ListenerProtocol::Plaintext,
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
            node_id: crabka_raft::NodeId(7),
            directory_id: uuid::Uuid::from_u128(7),
            cluster_id: None,
            bootstrap_servers: vec!["127.0.0.1:1".parse().unwrap()],
            listener_protocol: crabka_security::ListenerProtocol::Plaintext,
            controller: source.clone(),
            inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(
                None, None,
            )),
        };

        tokio::time::timeout(Duration::from_secs(2), run(params))
            .await
            .expect("already-voter auto join returns without dialing");

        assert2::assert!(source.controller_bound_addr_calls.load(Ordering::Relaxed) == 1);
        assert2::assert!(source.current_image_calls.load(Ordering::Relaxed) == 1);
    }
}
