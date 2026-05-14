//! Construction-time config for `Controller::start`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

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

#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub node_id: NodeId,
    pub voters: Vec<(NodeId, SocketAddr)>,
    pub controller_listen_addr: SocketAddr,
    pub log_dir: PathBuf,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub client_id: String,
    pub bootstrap_mode: BootstrapMode,
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
        }
    }
}
