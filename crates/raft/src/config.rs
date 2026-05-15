//! Construction-time config for `Controller::start`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::network::OutboundDialer;
use crate::types::NodeId;

/// Bootstrap orchestration for a freshly-formatted controller node.
///
/// Openraft 0.9 lacks pre-vote (KIP-595's equivalent), so simultaneous
/// `raft.initialize(full_voter_set)` on multiple brokers can split-vote
/// indefinitely on cold boot. This enum lets the operator (or test harness)
/// pick a deterministic boot order:
///
/// 1. One broker boots with `Bootstrap` — it initializes as the sole voter
///    in a singleton cluster and self-elects on the first election timeout.
/// 2. Remaining brokers boot with `Join` — they don't initialize, so they
///    don't race to elect. The bootstrap broker brings them in via
///    [`crate::ControllerHandle::add_learner`] +
///    [`crate::ControllerHandle::change_membership`].
/// 3. After the initial format, restarted brokers use `Rejoin` — their
///    on-disk raft log already carries the membership and openraft replays
///    it during `Raft::new`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapMode {
    /// Cold-boot the first voter of a fresh cluster. `Controller::start`
    /// calls `raft.initialize({(self.node_id, self.controller_listen_addr)})`,
    /// producing a singleton-voter cluster that elects this broker as
    /// leader on its first timeout.
    Bootstrap,

    /// Cold-boot a subsequent voter. `Controller::start` skips `initialize`
    /// and the raft engine sits in Learner state waiting for the bootstrap
    /// broker to add it via `add_learner` + `change_membership`.
    Join,

    /// Restart a previously-formatted broker. The on-disk raft log encodes
    /// the cluster's current membership; `Controller::start` skips
    /// `initialize` and openraft replays existing state during `Raft::new`.
    Rejoin,
}

#[derive(Clone)]
pub struct ControllerConfig {
    pub node_id: NodeId,
    pub voters: Vec<(NodeId, SocketAddr)>,
    pub controller_listen_addr: SocketAddr,
    pub log_dir: PathBuf,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub client_id: String,
    pub bootstrap_mode: BootstrapMode,
    /// Optional outbound dialer. `None` means: open a plain TCP socket
    /// to peers (legacy PLAINTEXT-only path). The broker injects an
    /// `InterBrokerClient`-backed dialer here when inter-broker TLS or
    /// SASL is configured.
    pub dialer: Option<Arc<dyn OutboundDialer>>,
    /// Optional inbound handshake hook. `None` keeps the legacy
    /// PLAINTEXT path. The broker injects a `BrokerRaftHandshake`
    /// implementation here when the controller listener should
    /// terminate TLS and/or SASL before raft frames start flowing.
    pub handshake: Option<Arc<dyn crate::RaftListenerHandshake>>,
}

impl std::fmt::Debug for ControllerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControllerConfig")
            .field("node_id", &self.node_id)
            .field("voters", &self.voters)
            .field("controller_listen_addr", &self.controller_listen_addr)
            .field("log_dir", &self.log_dir)
            .field("election_timeout", &self.election_timeout)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("client_id", &self.client_id)
            .field("bootstrap_mode", &self.bootstrap_mode)
            .field("dialer", &self.dialer.is_some())
            .field("handshake", &self.handshake.is_some())
            .finish()
    }
}

impl ControllerConfig {
    #[must_use]
    pub fn for_tests(node_id: NodeId, log_dir: PathBuf) -> Self {
        Self {
            node_id,
            voters: vec![(node_id, "127.0.0.1:0".parse().expect("static"))],
            controller_listen_addr: "127.0.0.1:0".parse().expect("static"),
            log_dir,
            election_timeout: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(200),
            client_id: "crabka-controller-test".into(),
            bootstrap_mode: BootstrapMode::Bootstrap,
            dialer: None,
            handshake: None,
        }
    }
}
