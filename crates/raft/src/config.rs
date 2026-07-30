//! Construction-time config for `Controller::start`.

use std::{future::Future, net::SocketAddr, path::PathBuf, pin::Pin, sync::Arc};

use bytes::Bytes;
use crabka_kraft_core::snapshot_fetch::METADATA_SNAPSHOT_FETCH_HARD_MAX;
use crabka_units::{
    fmt::Human as _,
    prelude::{ByteSize, Time, hours, mebibytes, millis, secs},
};
use uuid::Uuid;

use crate::{error::RaftError, network::OutboundDialer, types::NodeId};

/// `metadata.log.max.record.bytes.between.snapshots` default: 20 MiB.
const DEFAULT_MAX_BYTES_BETWEEN_SNAPSHOTS: ByteSize = mebibytes(20);

/// `metadata.log.max.snapshot.interval.ms` default: one hour.
const DEFAULT_MAX_SNAPSHOT_INTERVAL: Time = hours(1);

/// Election timeout used by [`ControllerConfig::for_tests`].
const TEST_ELECTION_TIMEOUT: Time = secs(1);

/// Leader heartbeat cadence used by [`ControllerConfig::for_tests`].
const TEST_HEARTBEAT_INTERVAL: Time = millis(200);

/// Optional router for KIP-595 traffic addressed to non-metadata quorum shards.
pub type ShardRouteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<Bytes>, RaftError>> + Send + 'a>>;

/// Classifies and serves shard-addressed KIP-595 requests before metadata dispatch.
pub trait RaftShardRouter: Send + Sync {
    fn route(&self, api_key: i16, body: Bytes) -> ShardRouteFuture<'_>;
}

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
///    on-disk raft log already carries the membership and the engine replays
///    it during `Raft::new`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapMode {
    /// Cold-boot the first voter of a fresh cluster. This node holds the
    /// initial `VotersRecord`; `Controller::start` calls `raft.initialize`
    /// with [`ControllerConfig::initial_voters`], producing the seed
    /// membership that elects this broker as leader on its first timeout.
    Bootstrap,

    /// Cold-boot a subsequent voter with an empty start. `Controller::start`
    /// skips `initialize`; the engine sits in Learner state and discovers
    /// the leader via [`ControllerConfig::bootstrap_servers`], then
    /// auto-joins (issuing `AddVoter` for itself once caught up) when
    /// [`ControllerConfig::auto_join`] is set.
    Join,

    /// Restart a previously-formatted broker. The on-disk raft log encodes
    /// the cluster's current membership; `Controller::start` skips
    /// recovers existing state from the on-disk log + checkpoint at startup.
    Rejoin,
}

#[derive(Clone)]
pub struct ControllerConfig {
    /// Capacity used by outbound controller client connections.
    pub client_dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity,
    /// Maximum frame size used by outbound controller client connections.
    pub client_frame_max: crabka_client_core::ClientFrameMax,
    pub node_id: NodeId,
    /// Endpoints used only to discover the leader at cold start (KIP-853 dynamic).
    pub bootstrap_servers: Vec<SocketAddr>,
    /// This replica's stable directory id (generated at format time).
    pub directory_id: Uuid,
    /// Issue `AddVoter` for self once caught up as an observer.
    pub auto_join: bool,
    /// Max allowed lag (in log entries) for an observer to be promotable.
    pub observer_lag_bound: u64,
    /// Initial voter set for the bootstrapping node only; empty for joiners.
    pub initial_voters: crabka_metadata::VoterSet,
    pub controller_listen_addr: SocketAddr,
    pub log_dir: PathBuf,
    pub election_timeout: Time,
    pub heartbeat_interval: Time,
    pub client_id: String,
    pub bootstrap_mode: BootstrapMode,
    /// Cluster UUID applied to the `MetadataImage` on first construction.
    /// `None` falls back to `Uuid::nil()` (legacy single-node default).
    /// The operator sets this to the `KafkaCluster` UID so every broker
    /// in the same cluster shares one identifier across restarts.
    pub cluster_id: Option<Uuid>,
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
    /// Optional KIP-595 shard router. Metadata traffic returns `None`; diskless
    /// WAL shards return an encoded response body and bypass metadata dispatch.
    pub shard_router: Option<Arc<dyn RaftShardRouter>>,
    /// `metadata.log.max.record.bytes.between.snapshots` (default 20 MiB).
    pub max_bytes_between_snapshots: ByteSize,
    /// `metadata.log.max.snapshot.interval.ms` (default 1 h; 0 = disabled).
    pub max_snapshot_interval: Time,
    /// Snapshot once committed offset advances this many records past the last
    /// snapshot, then prune the log below it. `0` disables snapshotting.
    pub snapshot_interval_records: u64,
    /// Maximum metadata snapshot size this follower will fetch. Deployments may
    /// lower the default 1 GiB security ceiling but cannot raise it.
    pub metadata_snapshot_fetch_max: ByteSize,
}

impl std::fmt::Debug for ControllerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControllerConfig")
            .field(
                "client_dispatch_queue_capacity",
                &self.client_dispatch_queue_capacity,
            )
            .field("client_frame_max", &self.client_frame_max)
            .field("node_id", &self.node_id.0)
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("directory_id", &self.directory_id)
            .field("auto_join", &self.auto_join)
            .field("observer_lag_bound", &self.observer_lag_bound)
            .field("initial_voters", &self.initial_voters)
            .field("controller_listen_addr", &self.controller_listen_addr)
            .field("log_dir", &self.log_dir)
            // Quantities render in the operator form (`1s`, `20MiB`) rather than
            // `uom`'s dimension-annotated `Debug`, which is unreadable in a log.
            .field(
                "election_timeout",
                &self.election_timeout.human().to_string(),
            )
            .field(
                "heartbeat_interval",
                &self.heartbeat_interval.human().to_string(),
            )
            .field("client_id", &self.client_id)
            .field("bootstrap_mode", &self.bootstrap_mode)
            .field("cluster_id", &self.cluster_id)
            .field("dialer", &self.dialer.is_some())
            .field("handshake", &self.handshake.is_some())
            .field("shard_router", &self.shard_router.is_some())
            .field(
                "max_bytes_between_snapshots",
                &self.max_bytes_between_snapshots.human().to_string(),
            )
            .field(
                "max_snapshot_interval",
                &self.max_snapshot_interval.human().to_string(),
            )
            .field("snapshot_interval_records", &self.snapshot_interval_records)
            .field(
                "metadata_snapshot_fetch_max",
                &self.metadata_snapshot_fetch_max.human().to_string(),
            )
            .finish()
    }
}

impl ControllerConfig {
    /// # Panics
    /// Panics only if the static loopback test address is invalid.
    #[must_use]
    pub fn for_tests(node_id: NodeId, log_dir: PathBuf) -> Self {
        let listen: SocketAddr = "127.0.0.1:0".parse().expect("static");
        let directory_id = Uuid::from_u128(u128::from(node_id.0));
        Self {
            client_dispatch_queue_capacity:
                crabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: crabka_client_core::ClientFrameMax::default(),
            node_id,
            bootstrap_servers: vec![],
            directory_id,
            auto_join: false,
            observer_lag_bound: 1000,
            initial_voters: crabka_metadata::VoterSet::from_voters([crabka_metadata::Voter {
                id: node_id,
                directory_id,
                endpoints: vec![crabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: listen.ip().to_string(),
                    port: listen.port(),
                }],
                kraft_version: crabka_metadata::KRaftVersionRange::default(),
            }]),
            controller_listen_addr: listen,
            log_dir,
            election_timeout: TEST_ELECTION_TIMEOUT,
            heartbeat_interval: TEST_HEARTBEAT_INTERVAL,
            client_id: "crabka-controller-test".into(),
            bootstrap_mode: BootstrapMode::Bootstrap,
            cluster_id: None,
            dialer: None,
            handshake: None,
            shard_router: None,
            max_bytes_between_snapshots: DEFAULT_MAX_BYTES_BETWEEN_SNAPSHOTS,
            max_snapshot_interval: DEFAULT_MAX_SNAPSHOT_INTERVAL,
            snapshot_interval_records: 0,
            metadata_snapshot_fetch_max: METADATA_SNAPSHOT_FETCH_HARD_MAX,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_units::prelude::{ByteSizeExt as _, TimeExt as _};

    use super::*;

    #[test]
    fn for_tests_uses_expected_snapshot_defaults() {
        let cfg = ControllerConfig::for_tests(NodeId(7), PathBuf::from("/tmp/raft-test"));

        check!(
            (
                cfg.max_bytes_between_snapshots,
                cfg.max_snapshot_interval,
                cfg.snapshot_interval_records,
                cfg.metadata_snapshot_fetch_max,
            ) == (mebibytes(20), hours(1), 0, METADATA_SNAPSHOT_FETCH_HARD_MAX,)
        );
        // The quantities must carry the magnitudes the Kafka configs name, not
        // just compare equal to the constants they were built from.
        check!(cfg.max_bytes_between_snapshots.bytes_u64() == 20 * 1024 * 1024);
        check!(cfg.max_snapshot_interval.millis_i64() == 3_600_000);
        check!(cfg.election_timeout.millis_i64() == 1_000);
        check!(cfg.heartbeat_interval.millis_i64() == 200);
    }

    #[test]
    fn debug_reports_configuration_fields_and_optional_hooks() {
        let cfg = ControllerConfig::for_tests(NodeId(7), PathBuf::from("/tmp/raft-test"));
        let rendered = format!("{cfg:?}");

        for needle in [
            "ControllerConfig",
            "node_id: 7",
            "client_id: \"crabka-controller-test\"",
            "dialer: false",
            "handshake: false",
            // Quantities render in the operator form, so 20 MiB reads as `20MiB`
            // rather than as a bare byte count.
            "max_bytes_between_snapshots: \"20MiB\"",
            "election_timeout: \"1s\"",
            "max_snapshot_interval: \"1h\"",
            "metadata_snapshot_fetch_max: \"1GiB\"",
        ] {
            assert2::assert!(rendered.contains(needle));
        }
    }
}
