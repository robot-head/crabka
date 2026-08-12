//! Construction-time config for `Controller::start`.

use std::{fmt, future::Future, net::SocketAddr, path::PathBuf, pin::Pin, str::FromStr, sync::Arc};

use bytes::Bytes;
use crabka_kraft_core::snapshot_fetch::METADATA_SNAPSHOT_FETCH_HARD_MAX;
use crabka_units::{
    fmt::Human as _,
    prelude::{ByteSize, ByteSizeExt as _, Time, hours, mebibytes, millis, secs},
};
use refined_type::rule::{GreaterI32, GreaterU32, GreaterUsize};
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

pub const DEFAULT_CONTROLLER_FETCH_MISS_LIMIT: u32 = 3;
pub const DEFAULT_METADATA_RAFT_COMMAND_QUEUE_CAPACITY: usize = 256;
pub const DEFAULT_METADATA_RAFT_FETCH_MAX: ByteSize = mebibytes(8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControllerFetchMissLimit(u32);

impl ControllerFetchMissLimit {
    /// Validate the consecutive fetch-miss limit.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u32) -> Result<Self, String> {
        GreaterU32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("controller fetch miss limit: {error}"))
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Default for ControllerFetchMissLimit {
    fn default() -> Self {
        Self::new(DEFAULT_CONTROLLER_FETCH_MISS_LIMIT)
            .expect("default controller fetch miss limit is positive")
    }
}

impl FromStr for ControllerFetchMissLimit {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

impl fmt::Display for ControllerFetchMissLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataRaftCommandQueueCapacity(usize);

impl MetadataRaftCommandQueueCapacity {
    /// Validate the metadata Raft command queue capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("metadata raft command queue capacity: {error}"))
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl Default for MetadataRaftCommandQueueCapacity {
    fn default() -> Self {
        Self::new(DEFAULT_METADATA_RAFT_COMMAND_QUEUE_CAPACITY)
            .expect("default metadata raft command queue capacity is positive")
    }
}

impl FromStr for MetadataRaftCommandQueueCapacity {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

impl fmt::Display for MetadataRaftCommandQueueCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataRaftFetchMax(i32);

impl MetadataRaftFetchMax {
    /// Validate the protocol byte count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("metadata raft fetch max: {error}"))
    }

    #[must_use]
    pub const fn bytes(self) -> i32 {
        self.0
    }

    #[must_use]
    pub fn size(self) -> ByteSize {
        ByteSize::from_bytes_i64(i64::from(self.0))
    }
}

impl TryFrom<ByteSize> for MetadataRaftFetchMax {
    type Error = String;

    fn try_from(value: ByteSize) -> Result<Self, Self::Error> {
        let bytes = value.bytes_f64();
        if !bytes.is_finite()
            || bytes.fract() != 0.0
            || !(1.0..=f64::from(i32::MAX)).contains(&bytes)
        {
            return Err(
                "metadata raft fetch max must be a positive whole-byte value that fits i32"
                    .to_owned(),
            );
        }
        Self::new(value.bytes_i32())
    }
}

impl Default for MetadataRaftFetchMax {
    fn default() -> Self {
        Self::try_from(DEFAULT_METADATA_RAFT_FETCH_MAX)
            .expect("default metadata raft fetch max is protocol-safe")
    }
}

impl FromStr for MetadataRaftFetchMax {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        crabka_units::parse::byte_size(value)
            .map_err(|error| error.to_string())?
            .try_into()
    }
}

impl fmt::Display for MetadataRaftFetchMax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.size().human().fmt(formatter)
    }
}

/// Optional router for KIP-595 traffic addressed to non-metadata quorum shards.
pub type ShardRouteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<Bytes>, RaftError>> + Send + 'a>>;

/// Classifies and serves shard-addressed KIP-595 requests before metadata dispatch.
pub trait RaftShardRouter: Send + Sync {
    fn route(&self, api_key: i16, body: Bytes) -> ShardRouteFuture<'_>;
}

/// One Kafka API version range served by a controller-listener Admin router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControllerApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub flexible_min: i16,
}

/// Authenticated request handed from the controller listener to the broker's
/// existing Admin handler registry.
#[derive(Clone, Debug)]
pub struct ControllerAdminRequest {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub body: Bytes,
    pub peer: SocketAddr,
    pub principal: Option<crabka_security::Principal>,
    pub authenticated_via_token: bool,
}

/// Encoded Kafka response body plus its response-header shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerAdminResponse {
    pub body: Bytes,
    pub flexible: bool,
}

pub type ControllerAdminRouteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ControllerAdminResponse>, RaftError>> + Send + 'a>>;

/// Optional KIP-919 Admin RPC surface attached by the broker crate.
pub trait ControllerAdminRouter: Send + Sync {
    fn api_versions(&self) -> &[ControllerApiVersion];
    fn route(&self, request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_>;
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
    pub bootstrap_servers: Vec<String>,
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
    /// Explicit heartbeat cadence. `None` preserves the derived
    /// `election_timeout / 3` behavior.
    pub heartbeat_interval: Option<Time>,
    pub controller_fetch_miss_limit: ControllerFetchMissLimit,
    pub metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
    pub metadata_raft_fetch_max: MetadataRaftFetchMax,
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
    /// Optional KIP-919 Admin router. The broker injects its existing handler
    /// registry here after construction, keeping controller and broker
    /// semantics on one implementation.
    pub admin_router: Option<Arc<dyn ControllerAdminRouter>>,
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
                &self
                    .heartbeat_interval
                    .map(|value| value.human().to_string()),
            )
            .field(
                "controller_fetch_miss_limit",
                &self.controller_fetch_miss_limit,
            )
            .field(
                "metadata_raft_command_queue_capacity",
                &self.metadata_raft_command_queue_capacity,
            )
            .field("metadata_raft_fetch_max", &self.metadata_raft_fetch_max)
            .field("client_id", &self.client_id)
            .field("bootstrap_mode", &self.bootstrap_mode)
            .field("cluster_id", &self.cluster_id)
            .field("dialer", &self.dialer.is_some())
            .field("handshake", &self.handshake.is_some())
            .field("shard_router", &self.shard_router.is_some())
            .field("admin_router", &self.admin_router.is_some())
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
            heartbeat_interval: Some(TEST_HEARTBEAT_INTERVAL),
            controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
            metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(),
            metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
            client_id: "crabka-controller-test".into(),
            bootstrap_mode: BootstrapMode::Bootstrap,
            cluster_id: None,
            dialer: None,
            handshake: None,
            shard_router: None,
            admin_router: None,
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
        check!(
            cfg.heartbeat_interval
                .expect("test heartbeat is explicit")
                .millis_i64()
                == 200
        );
    }

    #[test]
    fn raft_runtime_policy_defaults_and_validation() {
        check!(ControllerFetchMissLimit::default().get() == 3);
        check!(ControllerFetchMissLimit::new(0).is_err());
        check!(
            "7".parse::<ControllerFetchMissLimit>()
                .expect("positive miss limit")
                .get()
                == 7
        );

        check!(MetadataRaftCommandQueueCapacity::default().get() == 256);
        check!(MetadataRaftCommandQueueCapacity::new(0).is_err());
        check!(
            "512"
                .parse::<MetadataRaftCommandQueueCapacity>()
                .expect("positive command queue capacity")
                .get()
                == 512
        );

        check!(MetadataRaftFetchMax::default().size() == mebibytes(8));
        check!(MetadataRaftFetchMax::try_from(ByteSize::from_bytes_i64(0)).is_err());
        check!(
            "4MiB"
                .parse::<MetadataRaftFetchMax>()
                .expect("positive whole-byte fetch maximum")
                .bytes()
                == 4 * 1024 * 1024
        );
        check!(MetadataRaftFetchMax::try_from(ByteSize::from_bytes_f64(1.5)).is_err());
        check!(
            MetadataRaftFetchMax::try_from(ByteSize::from_bytes_i64(i64::from(i32::MAX) + 1))
                .is_err()
        );
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
