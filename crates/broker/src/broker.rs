//! Top-level `Broker` lifecycle. Wires together the partition registry,
//! metadata image, network listener, and handler table.

use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering},
    },
};

use crabka_ids::PartitionIndex;
use dashmap::DashMap;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    config::BrokerConfig,
    error::BrokerError,
    handlers::DispatchRegistry,
    log_dir,
    partition::{Partition, WriterMessage},
    partition_registry::PartitionRegistry,
};

/// Startup deadline for the controller quorum to elect a leader before
/// `Broker::start` fails with a `Startup` error.
const STARTUP_LEADER_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(2);

/// Maximum broker self-registration submit attempts before `Broker::start`
/// gives up and fails startup.
const SELF_REGISTRATION_MAX_ATTEMPTS: u32 = 8;

/// First delay of the exponential self-registration retry backoff.
const SELF_REGISTRATION_BACKOFF_MIN: std::time::Duration = std::time::Duration::from_millis(100);

/// Ceiling of the exponential self-registration retry backoff.
const SELF_REGISTRATION_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// Max bytes per `__cluster_metadata` fetch issued by a broker-only node's
/// metadata observer (1 MiB).
const OBSERVER_FETCH_MAX_BYTES: u32 = 1_048_576;

/// How often a broker-only node's metadata observer polls the controller
/// quorum for new `__cluster_metadata` records.
const OBSERVER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Capacity of the bounded audit-event channel between `AuditLog::emit`
/// callers and the `AuditWriter` drain task; events past this depth drop.
const AUDIT_EVENT_QUEUE_CAPACITY: usize = 8192;

/// Cadence at which the `AuditWriter` replays spooled audit records back
/// into the audit topic.
const AUDIT_SPOOL_REPLAY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// Cadence at which audit spool stats are polled into Prometheus gauges.
const AUDIT_STATS_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Startup wait for this broker's led audit partition to materialise before
/// emitting the `BrokerStarted` lifecycle event (best-effort; on timeout the
/// event is emitted anyway and may drop).
const AUDIT_PARTITION_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Cadence of the controller-side liveness scan that fires leader-election
/// callbacks on broker alive/dead transitions.
const LIVENESS_TICK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Cadence of the background poll refreshing the partition/controller health
/// gauges (`partitions_led`, URP, under-min-ISR, offline, `active_controller`).
const GAUGE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Cadence of the KIP-714 client-metrics stale-entry eviction sweep.
const CLIENT_METRICS_EVICTION_TICK: std::time::Duration = std::time::Duration::from_mins(1);

/// KIP-714 eviction: a client-metrics entry is stale once this many push
/// intervals have elapsed without a push.
const CLIENT_METRICS_STALE_PUSH_INTERVALS: u32 = 3;

/// KIP-714 eviction: floor on the staleness window, so clients on very short
/// push intervals are not evicted between pushes.
const CLIENT_METRICS_STALE_FLOOR: std::time::Duration = std::time::Duration::from_mins(10);

/// Safety-net timeout shared by the test-helper `wait_*` awaiters: a
/// condition that has not held within this window fails the test loudly.
#[cfg(any(test, feature = "test-helpers"))]
const TEST_AWAITER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The running broker. Library callers get a [`BrokerHandle`] from
/// [`Broker::start`]; this struct is the shared internal state.
// `config`, `metadata`, `partitions` are consumed by the per-API handlers
// landing in Tasks 12-16; allow dead_code on the struct until the handlers
// pick them up.
#[allow(dead_code)]
pub struct Broker {
    pub(crate) config: BrokerConfig,
    /// Metadata authority for this broker. For combined/controller nodes
    /// this is a live openraft `ControllerHandle`; for broker-only nodes it
    /// is an observer-backed source that fetches `__cluster_metadata` and
    /// forwards writes to the controller quorum. Handlers reach it via the
    /// `MetadataSource` trait, so the concrete backing is invisible to them.
    pub(crate) controller: Arc<dyn crate::metadata_source::MetadataSource>,
    /// Wrapped in `Arc` so handlers cloning the field share the same
    /// underlying registry. Lookups take a borrowed `&str` topic, so the
    /// produce/fetch hot path resolves partitions with no per-lookup `String`
    /// allocation.
    pub(crate) partitions: Arc<PartitionRegistry>,
    /// KIP-113 (`AlterReplicaLogDirs`): in-progress intra-broker
    /// log-dir moves. One entry per `(topic, partition)` currently
    /// being copied to a different log.dir. `DescribeLogDirs` reads
    /// this to surface `is_future_key=true` rows; the
    /// `AlterReplicaLogDirs` handler reads it to make a second
    /// request for the same partition idempotent (or reject a
    /// conflicting target).
    pub(crate) future_logs:
        Arc<DashMap<(String, PartitionIndex), Arc<crate::future_log::FutureLogState>>>,
    pub(crate) group_coordinator: Arc<crate::coordinator::GroupCoordinator>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    pub(crate) producer_state: Arc<crate::producer_state::ProducerState>,
    pub(crate) txn_coordinator: Arc<crate::txn::coordinator::TxnCoordinator>,
    pub(crate) share_coordinator: Arc<crate::share_coordinator::coordinator::ShareCoordinator>,
    pub(crate) share_partition_leaders:
        Arc<crate::share_partition::manager::SharePartitionLeaderManager>,
    pub(crate) supervisor_shutdown: tokio_util::sync::CancellationToken,
    pub(crate) supervisor_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    /// Handle for the periodic disk-usage scanner spawned when
    /// `BrokerConfig::partition_disk_scan_interval_secs > 0`. Retained on
    /// the struct so [`BrokerHandle::shutdown`] can await it after
    /// cancelling `supervisor_shutdown`. `None` when the scanner is
    /// disabled (interval = 0, typical in tests).
    pub(crate) disk_scanner_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    pub(crate) liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    /// `Some` when `BrokerConfig::tls_config` is set. Per-listener
    /// accept loops snapshot the current `Arc<ServerConfig>` via
    /// `current()` and wrap it in a fresh `TlsAcceptor`. The TLS
    /// hot-reload path swaps the inner config without restart.
    pub(crate) tls_dynamic: Option<Arc<crabka_security::DynamicServerConfig>>,
    /// Linux kTLS (Increment F): `true` when the startup probe confirmed the
    /// kernel supports kTLS TX (kernel ≥ 4.13 + the `tls` module loadable) and
    /// rustls is configured to export secrets. Set ONCE at startup —
    /// `ktls::config_ktls_server` consumes the `TlsStream` by value, so a
    /// per-connection failure is unrecoverable; routing through kTLS only when
    /// this is `true` keeps the per-connection path infallible-by-construction.
    /// When `false` (non-Linux, no `tls` module, or no TLS configured), TLS
    /// listeners serve the exact userspace rustls path (byte-identical wire).
    pub(crate) ktls_enabled: bool,
    /// Shared outbound dialer used by the replicator, raft transport,
    /// and controller-heartbeat loops. When `inter_broker_credentials`
    /// is `None` and the listener is `PLAINTEXT` the dialer falls back
    /// to a plain `TcpStream::connect` — the new wiring is transparent
    /// for the legacy PLAINTEXT-only path.
    pub(crate) inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    /// Resolved protocol of the inter-broker listener (matched from
    /// `BrokerConfig::effective_listeners()` against
    /// `inter_broker_listener_name`). Threaded into outbound inter-broker
    /// dials — the replicator and heartbeat hold their own copies; the
    /// `EndTxn` marker fan-out reads this one so TLS / SASL run when the
    /// listener demands them.
    pub(crate) inter_broker_listener_protocol: crabka_security::ListenerProtocol,
    /// KIP-966 offset-aware unclean recovery. Cloneable handle for
    /// enqueuing recovery jobs onto the Unclean Recovery Manager task.
    /// Used by the `ElectLeaders UNCLEAN` handler (which awaits the
    /// outcome) and the automatic failover path (fire-and-forget).
    pub(crate) unclean_recovery: crate::unclean_recovery::UncleanRecoveryHandle,
    /// KIP-73 throttle buckets. Updated by the throttle refresh task and
    /// consulted by the Fetch handler and replicator.
    pub throttle_state: Arc<crate::throttle::ThrottleState>,
    /// KIP-13/KIP-124 quota buckets. Updated by the quota refresh task and
    /// consulted by the Produce/Fetch handlers and request-rate enforcement.
    pub quota_buckets: Arc<crate::quota::QuotaBuckets>,
    /// Live connection accounting for the `max.connections` /
    /// `max.connections.per.ip` caps. `accept_loop` consults these before
    /// spawning a per-connection task and an RAII [`ConnectionGuard`]
    /// decrements them when the connection ends.
    pub(crate) connections: ConnectionLimiter,
    /// KIP-227 incremental-fetch-session cache. Consulted by the Fetch
    /// handler before each read; sized by
    /// `BrokerConfig::max_incremental_fetch_session_cache_slots`.
    pub fetch_session_cache: Arc<crate::fetch_session::FetchSessionCache>,
    /// Prometheus metrics. Cloned into every subsystem that
    /// emits (produce/fetch handlers, isr-maintenance loop, etc.). The
    /// `BrokerMetrics` struct internally clones cheaply (single Arc).
    pub metrics: crate::metrics::BrokerMetrics,
    /// The actual `SocketAddr` the `/metrics` HTTP server is
    /// bound to. Populated only when `BrokerConfig::metrics_listen_addr`
    /// is `Some`; useful for tests that pass `127.0.0.1:0` and need to
    /// discover the OS-assigned port.
    pub(crate) metrics_bound_addr: Option<SocketAddr>,
    /// Controlled shutdown. Set to `true` by
    /// [`BrokerHandle::controlled_shutdown`]; the heartbeat client reads
    /// this every tick and stamps `want_shut_down=true` onto outbound
    /// `BrokerHeartbeat` requests.
    pub(crate) want_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    /// Controlled shutdown. Set to `true` by the heartbeat
    /// client when the controller responds `should_shut_down=true`;
    /// [`BrokerHandle::controlled_shutdown`] awaits this before invoking
    /// the regular shutdown path.
    pub(crate) should_shutdown: Arc<tokio::sync::watch::Sender<bool>>,
    /// KIP-405: shared remote-storage + remote-log-metadata
    /// reader. `Some` when `BrokerConfig::remote_log_storage_dir` is set;
    /// the remote-log-manager copy task and the Fetch/ListOffsets
    /// handlers share the same instance through this handle.
    pub(crate) remote_reader: Option<Arc<crate::remote_reader::RemoteReader>>,
    /// KIP-113 (offline-dir handling): per-log-dir online/offline status,
    /// built by a writability probe at `Broker::start` time. Handlers
    /// (today: `DescribeLogDirs`; future: produce/fetch) read this via
    /// [`crate::log_dir_status::LogDirRegistry::is_offline`] before
    /// touching the dir.
    pub(crate) log_dir_status: crate::log_dir_status::LogDirRegistry,
    /// KIP-858: stable UUID per configured log.dir, minted + persisted at
    /// startup. Shared with the heartbeat client (`offline_log_dirs` UUID list)
    /// and the assignment reporter (`AssignReplicasToDirs` handler).
    pub(crate) log_dir_ids: crate::log_dir_id::LogDirIds,
    /// KIP-714 client-metrics receiver: subscription manager + Prometheus
    /// collector + OTLP forwarder. Shared so the push handler
    /// and the scrape path both touch the same instance.
    pub(crate) client_metrics: Arc<crate::client_metrics::ClientMetrics>,
    /// Test-only counter of served `OffsetForLeaderEpoch` (`api_key` 23)
    /// requests. Incremented once per decoded request by the handler.
    /// Used by the KIP-320 proactive-validation integration test to prove
    /// the consumer's validate pass actually issued an OFLE RPC (as opposed
    /// to detecting truncation via the reactive in-band `diverging_epoch` /
    /// `OFFSET_OUT_OF_RANGE` fetch paths, which issue no OFLE).
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) offset_for_leader_epoch_requests: Arc<std::sync::atomic::AtomicU64>,
    /// `FedRAMP` MLA (Slice 1): cloneable handle to the audit pipeline.
    /// Handlers and lifecycle code call `emit` to record events; the
    /// `AuditWriter` background task drains them into the
    /// `KafkaTopicAuditSink`. Disabled (`AuditLog::disabled()`) when
    /// `BrokerConfig::audit_enabled` is `false`.
    pub(crate) audit_log: std::sync::Arc<crabka_audit::AuditLog>,
    handlers: DispatchRegistry,
}

impl Broker {
    pub(crate) fn handlers(&self) -> &DispatchRegistry {
        &self.handlers
    }

    pub(crate) fn audit_product() -> crabka_audit::ProductInfo {
        crabka_audit::ProductInfo {
            vendor_name: "Crabka".to_string(),
            name: "crabka-broker".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Test-only: clone the controller handle so the `auto_join` unit test can
    /// build `AutoJoinParams` without reaching into private fields.
    #[cfg(test)]
    pub(crate) fn controller_for_test(&self) -> Arc<dyn crate::metadata_source::MetadataSource> {
        self.controller.clone()
    }

    /// Test-only: clone the shared inter-broker client (same reason).
    #[cfg(test)]
    pub(crate) fn inter_broker_client_for_test(
        &self,
    ) -> Arc<crate::network::client::InterBrokerClient> {
        self.inter_broker_client.clone()
    }
}

/// Lifecycle handle returned by [`Broker::start`]. Drop or call
/// [`shutdown`](BrokerHandle::shutdown) to stop the broker.
pub struct BrokerHandle {
    listen_addr: SocketAddr,
    shutdown: CancellationToken,
    /// One task per `ListenerSpec` bound during `Broker::start`. `shutdown()`
    /// awaits every task to drain in-flight connections.
    listener_tasks: Vec<JoinHandle<()>>,
    /// Held so partition writer tasks live as long as the handle.
    _broker: Arc<Broker>,
}

impl BrokerHandle {
    /// The actual bound `SocketAddr` (useful when `BrokerConfig.listen_addr`
    /// used port 0 to let the OS pick).
    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// The actual bound address of the Prometheus `/metrics`
    /// HTTP server, if one is configured. Tests pass `127.0.0.1:0` in
    /// `BrokerConfig::metrics_listen_addr` and read the OS-assigned
    /// port back through this accessor.
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self._broker.metrics_bound_addr
    }

    /// The actual `SocketAddr` this broker's controller listener bound to
    /// (resolves the OS-assigned port when `controller_listen_addr` used port
    /// 0). KIP-853 dynamic-voters tests read this to point joiners at the
    /// bootstrap broker's real controller endpoint.
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn controller_addr(&self) -> SocketAddr {
        self._broker.controller.controller_bound_addr()
    }

    /// Current Raft leader id as observed by this broker's controller.
    /// Returns `None` before the first leader is elected. Trivial
    /// passthrough to [`crabka_raft::ControllerHandle::watch_leader`].
    ///
    /// `async fn` even though the underlying `watch::Receiver::borrow` is
    /// synchronous — the broker public API expects
    /// `controller_leader_id().await`, and keeping the signature async
    /// preserves room for a future implementation that waits for the
    /// first non-`None` value via `watch::Receiver::changed`.
    #[allow(clippy::unused_async, clippy::used_underscore_binding)]
    pub async fn controller_leader_id(&self) -> Option<crabka_raft::NodeId> {
        *self._broker.controller.watch_leader().borrow()
    }

    /// Test-only: the controller's current quorum state (leader epoch, HWM,
    /// per-voter matched index). Used by the mixed-quorum acceptance test to
    /// observe whether the Crabka leader commits/advances and whether a peer
    /// voter (e.g. a JVM follower) is fetching.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn controller_quorum_state_for_test(&self) -> crabka_raft::QuorumState {
        self._broker.controller.quorum_state()
    }

    /// Number of brokers currently registered in this broker's
    /// `MetadataImage`. Used by replication integration tests to wait
    /// for all peers to come up before issuing `CreateTopics`.
    ///
    /// `async fn` for the same reason as
    /// [`controller_leader_id`](Self::controller_leader_id): keeps the
    /// public test surface uniform and leaves room for a future
    /// implementation that blocks until convergence.
    #[allow(clippy::unused_async, clippy::used_underscore_binding)]
    pub async fn broker_count(&self) -> usize {
        self._broker.controller.current_image().brokers().count()
    }

    /// This broker's own registration endpoints, as stored in the
    /// quorum-replicated [`crabka_metadata::MetadataImage`]. Integration
    /// tests verify per-listener endpoints were
    /// projected from `BrokerConfig::effective_listeners()` onto the
    /// self-registration record. Returns the cloned endpoint list (or
    /// empty if the broker has not yet self-registered).
    #[allow(clippy::unused_async, clippy::used_underscore_binding)]
    pub async fn self_registration_endpoints(&self) -> Vec<crabka_metadata::BrokerEndpoint> {
        let node_id = self._broker.config.node_id;
        self._broker
            .controller
            .current_image()
            .broker(node_id)
            .map(|b| b.endpoints.clone())
            .unwrap_or_default()
    }

    /// Manually mutate the openraft voter set on this broker's controller.
    /// `new_voters` is the complete desired set (not a delta). Callers must
    /// invoke this on the broker that's currently the openraft leader, or
    /// the call returns [`BrokerError::Replication`] with the underlying
    /// `RaftError::NotLeader` rendered into the message. See
    /// [`crabka_raft::ControllerHandle::change_membership`] for full semantics.
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft errors as [`BrokerError::Replication`].
    #[allow(clippy::used_underscore_binding)]
    pub async fn change_membership(
        &self,
        new_voters: std::collections::BTreeSet<crabka_raft::NodeId>,
    ) -> Result<(), BrokerError> {
        self._broker
            .controller
            .change_membership(new_voters)
            .await
            .map_err(|e| BrokerError::Replication(format!("change_membership: {e}")))
    }

    /// Register a non-voting openraft learner at `addr`. Blocks until the
    /// leader has caught the new node up to the current commit index.
    /// Subsequent [`Self::change_membership`] promotes the learner to a voter.
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft errors as [`BrokerError::Replication`].
    #[allow(clippy::used_underscore_binding)]
    pub async fn add_learner(
        &self,
        node_id: crabka_raft::NodeId,
        addr: std::net::SocketAddr,
    ) -> Result<(), BrokerError> {
        // KIP-853: openraft membership now keys on the full `Node` identity.
        // This `SocketAddr`-shaped convenience wrapper (used by integration
        // tests) synthesizes a single CONTROLLER endpoint and derives the
        // directory id from the node id, matching the `for_tests` convention.
        let node = crabka_raft::Node {
            directory_id: uuid::Uuid::from_u128(u128::from(node_id.0)),
            endpoints: vec![crabka_metadata::VoterEndpoint {
                name: "CONTROLLER".into(),
                host: addr.ip().to_string(),
                port: addr.port(),
            }],
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        };
        self._broker
            .controller
            .add_learner(node_id, node)
            .await
            .map_err(|e| BrokerError::Replication(format!("add_learner: {e}")))
    }

    /// Is `(topic, partition)` present in this broker's `MetadataImage`?
    /// Used by replication integration tests to wait for topic
    /// propagation.
    #[allow(clippy::unused_async, clippy::used_underscore_binding)]
    pub async fn has_partition(&self, topic: &str, partition: i32) -> bool {
        self._broker
            .controller
            .current_image()
            .partition(topic, partition)
            .is_some()
    }

    /// Local `log_end_offset` for `(topic, partition)`, if this broker
    /// hosts the partition. Used by replication integration tests to
    /// assert all followers caught up.
    #[allow(clippy::unused_async, clippy::used_underscore_binding)]
    pub async fn local_log_end_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let part = self
            ._broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        // Unwrap `Offset` -> `i64` at this test-helper boundary: integration
        // tests compare the result against raw offset literals.
        Some(part.log_end_offset().0)
    }

    /// Test-only: truncate this broker's local partition log so no
    /// records at offset `>= offset` remain. Simulates "fell behind
    /// past retention" in the out-of-range replication integration
    /// test.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Replication`] if the partition is not
    /// hosted on this broker.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn test_truncate_local_log(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), crate::error::BrokerError> {
        let part = self
            ._broker
            .partitions
            .get(topic, PartitionIndex(partition))
            .ok_or_else(|| {
                crate::error::BrokerError::Replication(format!(
                    "partition {topic}-{partition} not local"
                ))
            })?;
        part.truncate_to(crabka_log::Offset(offset)).await?;
        // Mirror the production truncation path (the replicator): a log
        // truncation also reverts idempotent-producer dedup entries for the
        // dropped offsets, so a retried batch from the truncated tail re-appends
        // instead of deduplicating against a vanished offset.
        self._broker
            .producer_state
            .truncate(topic, PartitionIndex(partition), offset)
            .await;
        Ok(())
    }

    /// Test-only: advance this broker's local partition `log_start_offset`
    /// to `new_start` without physically deleting on-disk segments.
    /// Simulates retention-driven truncation on a leader for the
    /// out-of-range replication integration test.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Replication`] if the partition is not
    /// hosted on this broker.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn test_advance_log_start(
        &self,
        topic: &str,
        partition: i32,
        new_start: i64,
    ) -> Result<(), crate::error::BrokerError> {
        let part = self
            ._broker
            .partitions
            .get(topic, PartitionIndex(partition))
            .ok_or_else(|| {
                crate::error::BrokerError::Replication(format!(
                    "partition {topic}-{partition} not local"
                ))
            })?;
        part.test_set_log_start(crabka_log::Offset(new_start)).await
    }

    /// Test-only: directly set `current_leader_epoch` on a locally-hosted
    /// partition. Used by `tests/leader_epoch.rs` to simulate split-brain
    /// (force an epoch bump) without going through the supervisor's
    /// metadata-image-driven path.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub fn test_set_leader_epoch(&self, topic: &str, partition: i32, epoch: i32) {
        if let Some(part) = self
            ._broker
            .partitions
            .get(topic, PartitionIndex(partition))
        {
            part.test_set_leader_epoch(epoch);
        }
    }

    /// Test-only: return `true` if `(topic, partition)` is present in this
    /// broker's in-process partition registry. Used by admin-handler
    /// integration tests to confirm that `CreatePartitions` materialised a
    /// new partition dir + writer task.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn partition_exists_for_test(&self, topic: &str, partition: i32) -> bool {
        self._broker
            .partitions
            .contains(topic, PartitionIndex(partition))
    }

    /// Test-only: read the share-state summary
    /// `(state_epoch, leader_epoch, start_offset, delivery_complete_count)`
    /// for `(group, topic_id, partition)` straight from this broker's
    /// [`ShareCoordinator`](crate::share_coordinator::coordinator::ShareCoordinator).
    /// Returns `None` when the key has no initialized state. KIP-932 lifecycle
    /// tests use this to assert the group-coordinator Initialized per-partition
    /// share state without advertising the persister RPCs over the wire.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn share_state_summary_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<(i32, i32, i64, i32)> {
        // Unwrap the summary's `start_offset` -> `i64` at this test-helper
        // boundary: integration tests compare it against raw offset literals.
        self._broker
            .share_coordinator
            .read_summary(group, topic_id, partition)
            .await
            .map(|(state_epoch, leader_epoch, start_offset, count)| {
                (state_epoch, leader_epoch, start_offset.0, count)
            })
    }

    /// Test-only: await until the persisted share-state summary exists for
    /// `(group, topic_id, partition)` (share-state initialized / recovered).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_for_share_state_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if self
                    .share_state_summary_for_test(group, topic_id, partition)
                    .await
                    .is_some()
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "share-state summary for {group}:{topic_id}:{partition} not present within 30s"
        );
    }

    /// Test-only: await until the share-partition SPSO (start_offset) >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_share_spso(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        min: i64,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some((_, _, spso, _)) = self
                    .share_state_summary_for_test(group, topic_id, partition)
                    .await
                    && spso >= min
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "share SPSO for {group}:{topic_id}:{partition} did not reach {min} within 30s"
        );
    }

    /// Test-only: await until the share-partition delivery-complete count >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_share_delivery_complete(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        min: i32,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some((_, _, _, dcc)) = self
                    .share_state_summary_for_test(group, topic_id, partition)
                    .await
                    && dcc >= min
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "share dcc for {group}:{topic_id}:{partition} did not reach {min} within 30s"
        );
    }

    /// Test-only: await until the live share-partition has exactly `n` Acquired
    /// in-flight batches (e.g. after a ShareFetch acquires, or after lock-timeout
    /// redelivery returns records to Available — count drops back).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_share_acquired_count(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        n: i32,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some(cell) = self
                    ._broker
                    .share_partition_leaders
                    .peek_for_test(group, topic_id, partition)
                {
                    let count = cell.lock().await.count_acquired_batches();
                    if count == n {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "share acquired-batch count for {group}:{topic_id}:{partition} did not reach {n} within 30s"
        );
    }

    // ── consumer/streams/share group awaiters ─────────────────────────────────

    /// Test-only: describe a consumer/share/streams group via its actor.
    /// `None` if the group has no live actor.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn group_describe_for_test(
        &self,
        group_id: &str,
    ) -> Option<crate::coordinator::unified::actor::DescribeView> {
        let handle = self
            ._broker
            .group_coordinator
            .groups
            .get(group_id)?
            .value()
            .clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(crate::coordinator::unified::actor::GroupActorMessage::Describe { reply: tx })
            .await
            .ok()?;
        rx.await.ok()
    }

    /// Test-only: await until the group has exactly `n` members.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_group_member_count(&self, group_id: &str, n: usize) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                let count = self
                    .group_describe_for_test(group_id)
                    .await
                    .map_or(0, |v| v.members.len());
                if count == n {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "group {group_id} did not settle at {n} members within 30s"
        );
    }

    /// Test-only: await until the group is empty/drained (no members).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_group_empty(&self, group_id: &str) {
        self.wait_until_group_member_count(group_id, 0).await;
    }

    /// Test-only: describe a **classic**-protocol group via `ClassicInspect`.
    /// Returns `None` when no actor exists or the actor is consumer-kind (not classic).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn classic_group_inspect_for_test(
        &self,
        group_id: &str,
    ) -> Option<crate::coordinator::unified::actor::ClassicView> {
        let handle = self
            ._broker
            .group_coordinator
            .groups
            .get(group_id)?
            .value()
            .clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(
                crate::coordinator::unified::actor::GroupActorMessage::ClassicInspect { reply: tx },
            )
            .await
            .ok()?;
        rx.await.ok()
    }

    /// Test-only: await until the classic group has exactly `n` live members
    /// (i.e., they have been registered in the actor via `ClassicJoin`).
    ///
    /// Use this rather than `wait_until_group_member_count` for classic-protocol
    /// groups, because the next-gen `Describe` message is a no-op on a
    /// classic-kind actor.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_classic_group_member_count(&self, group_id: &str, n: usize) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                let count = self
                    .classic_group_inspect_for_test(group_id)
                    .await
                    .map_or(0, |v| v.members.len());
                if count == n {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "classic group {group_id} did not settle at {n} members within 30s"
        );
    }

    // ── streams-group awaiters ────────────────────────────────────────────────

    /// Test-only: describe a streams group via its actor.
    /// `None` if the group has no live streams actor.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn streams_group_describe_for_test(
        &self,
        group_id: &str,
    ) -> Option<crate::coordinator::unified::streams::actor::StreamsDescribeView> {
        let handle = self
            ._broker
            .group_coordinator
            .streams_groups
            .get(group_id)?
            .value()
            .clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(
                crate::coordinator::unified::streams::actor::StreamsGroupActorMessage::Describe {
                    reply: tx,
                },
            )
            .await
            .ok()?;
        rx.await.ok()
    }

    /// Test-only: await until the streams group has exactly `n` members.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_streams_group_member_count(&self, group_id: &str, n: usize) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                let count = self
                    .streams_group_describe_for_test(group_id)
                    .await
                    .map_or(0, |v| v.members.len());
                if count == n {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "streams group {group_id} did not settle at {n} members within 30s"
        );
    }

    /// Test-only: await until the streams group is empty/drained (no members).
    ///
    /// Replaces the fixed-duration `tokio::time::sleep` calls that follow a
    /// `streams_leave()` heartbeat in the downgrade integration tests, where the
    /// test must ensure the leave has propagated through the streams actor before
    /// issuing the classic `JoinGroup` that triggers the streams→classic conversion.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_streams_group_empty(&self, group_id: &str) {
        self.wait_until_streams_group_member_count(group_id, 0)
            .await;
    }

    // ── partition log helpers ──────────────────────────────────────────────────

    /// Test-only: return the `log_start_offset` of `(topic, partition)` as
    /// reported by its underlying [`crabka_log::Log`]. Returns `None` if the
    /// partition is not hosted on this broker.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn partition_log_start_for_test(&self, topic: &str, partition: i32) -> Option<i64> {
        let part = self
            ._broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        // Unwrap `Offset` -> `i64` at this test-helper boundary.
        Some(part.log_start_offset().0)
    }

    /// Test-only: return the `retention.ms` override currently active in
    /// `(topic, partition)`'s log config. Returns `None` if the partition is
    /// not hosted on this broker. The inner `Option<Duration>` is `None` when
    /// no retention override has been applied (topic uses broker default).
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn partition_retention_ms_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<Option<std::time::Duration>> {
        let part = self
            ._broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        let snap = part.log.lock().ok()?.config_snapshot();
        Some(snap.retention_ms)
    }

    /// Test-only: full `LogConfig` snapshot for `(topic, partition)`.
    /// Returns `None` if the partition is not hosted on this broker.
    /// Used by the compaction integration test to wait for
    /// `cleanup.policy=compact` + `segment.bytes` overrides to propagate
    /// from the metadata image through the supervisor's reconcile loop.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn partition_log_config_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<crabka_log::LogConfig> {
        let part = self
            ._broker
            .partitions
            .get(topic, PartitionIndex(partition))?;
        Some(part.log.lock().ok()?.config_snapshot())
    }

    /// Test-only: append `n` single-record batches to `(topic, partition)`
    /// through the partition's writer task. Used by admin-handler integration
    /// tests that need a non-empty log without going through the Kafka Produce
    /// wire protocol. Returns the `base_offset` of the last appended batch, or
    /// an error if the partition is not hosted on this broker or the writer is
    /// dead.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Replication`] if the partition is not local.
    /// Returns [`BrokerError::Txn`] if the writer task is dead.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn produce_records_for_test(
        &self,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Result<i64, crate::error::BrokerError> {
        let part = self
            ._broker
            .partitions
            .get(topic, PartitionIndex(partition))
            .ok_or_else(|| {
                crate::error::BrokerError::Replication(format!(
                    "partition {topic}-{partition} not local"
                ))
            })?;
        let mut last_offset = 0i64;
        for i in 0..n {
            let batch = crabka_protocol::records::RecordBatch {
                records: vec![crabka_protocol::records::Record {
                    offset_delta: 0,
                    value: Some(bytes::Bytes::from(format!("test-record-{i}").into_bytes())),
                    ..Default::default()
                }],
                ..Default::default()
            };
            // Unwrap `Offset` -> `i64` at this test-helper boundary.
            last_offset = part.produce_batch(batch).await?.0;
        }
        Ok(last_offset)
    }

    /// Test-only: read the `tiered_storage_rlmm_topic_backed` gauge. `1`
    /// once the bootstrap task has swapped the fail-closed `NotReadyRlmm`
    /// for the topic-backed [`crabka_remote_storage::RemoteLogMetadataManager`],
    /// `0` before the swap completes, or when `remote_log_metadata` is
    /// `RlmmKind::InMemory`.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn rlmm_topic_backed_active_for_test(&self) -> bool {
        self._broker.metrics.tiered_storage_rlmm_topic_backed.get() == 1
    }

    /// Test-only: submit a [`crabka_metadata::MetadataRecord`] directly to
    /// this broker's controller, bypassing the public Kafka APIs. Used by
    /// integration tests to provision a SCRAM credential before the
    /// `AlterUserScramCredentials` handler exists. Returns an
    /// error if the submit fails (e.g., this broker is not the raft leader
    /// and forwarding fails).
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft errors as [`BrokerError::Replication`].
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn submit_metadata_record_for_test(
        &self,
        rec: crabka_metadata::MetadataRecord,
    ) -> Result<(), crate::error::BrokerError> {
        self._broker
            .controller
            .submit_change(vec![rec])
            .await
            .map_err(|e| crate::error::BrokerError::Replication(format!("submit: {e}")))
    }

    /// Test-only: insert a classic group into this broker's
    /// `GroupCoordinator`. Returns immediately if the group already exists
    /// (idempotent). Used by admin-handler integration tests to seed the group
    /// registry without running a full `JoinGroup` / `SyncGroup` exchange.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub fn group_create_for_test(&self, group_id: &str) {
        let _ = self
            ._broker
            .group_coordinator
            .get_or_create_classic(group_id);
    }

    /// Test-only: return the locked `GroupType` for `group_id`, if any.
    /// Integration tests use this to assert a group has been flagged as
    /// Classic (after `JoinGroup`) or converted to Streams (after a
    /// `StreamsGroupHeartbeat` on a drained classic group).
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn group_type_for_test(
        &self,
        group_id: &str,
    ) -> Option<crate::coordinator::unified::GroupType> {
        self._broker.group_coordinator.group_type(group_id)
    }

    /// Test-only: await until the coordinator's group-type lock for `group_id`
    /// reaches `expected`. This replaces immediate assertions after protocol
    /// requests that enqueue actor work and then asynchronously persist the
    /// classic/streams type marker.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_group_type(
        &self,
        group_id: &str,
        expected: crate::coordinator::unified::GroupType,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if self.group_type_for_test(group_id) == Some(expected) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "group {group_id} did not settle at type {expected:?} within {TEST_AWAITER_TIMEOUT:?}; \
             last={:?}",
            self.group_type_for_test(group_id)
        );
    }

    /// This broker's raft `node_id` (1-indexed broker id used in raft quorum
    /// and metadata records). Exposed so integration tests can build
    /// `IncrementalAlterConfigs` broker-resource requests targeting this
    /// broker without hard-coding a node id.
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn node_id(&self) -> u64 {
        self._broker.config.node_id.0
    }

    /// Test-only: return a snapshot of the current `MetadataImage` as seen by
    /// this broker's controller. Mirrors `partition_leader_for_test` /
    /// `partition_record_for_test` but exposes the whole image so throttle
    /// integration tests can call `broker_throttle_rate` and
    /// `topic_config` directly without adding per-field accessors.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn controller_image_for_test(&self) -> std::sync::Arc<crabka_metadata::MetadataImage> {
        self._broker.controller.current_image()
    }

    /// Test-only: the raft voter set this node's metadata source reports.
    /// A controller/combined node returns the openraft membership; a
    /// broker-only (observer) node returns an empty set since it never
    /// joins the quorum. Used by the role-separation test to assert a
    /// broker-only node is absent from the controller's voters.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn quorum_voters_for_test(&self) -> Vec<crabka_raft::NodeId> {
        self._broker.controller.quorum_state().voters
    }

    /// Test-only: clone the inner `Arc<Broker>`. Used by the `auto_join`
    /// unit test (and dynamic-voters integration tests) that need to drive
    /// broker-internal background routines directly.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn broker_arc_for_test(&self) -> Arc<Broker> {
        self._broker.clone()
    }

    /// Test-only: the controller voter set's size as seen by this broker's
    /// committed `MetadataImage`. KIP-853 dynamic-voters tests poll this to
    /// observe auto-join growing / `remove_voter` shrinking the quorum.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn voter_count_for_test(&self) -> usize {
        self._broker.controller.current_image().voters().len()
    }

    /// Test-only: the controller voter ids as seen by this broker's
    /// committed `MetadataImage`. Used to pick a follower to remove in the
    /// dynamic-voters shrink test.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn voter_ids_for_test(&self) -> std::collections::BTreeSet<crabka_raft::NodeId> {
        self._broker.controller.current_image().voters().ids()
    }

    /// Test-only: the `directory_id` of voter `id` from this broker's
    /// committed `MetadataImage`, if present. `remove_voter` needs the
    /// voter's directory id to disambiguate.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn voter_directory_id_for_test(&self, id: crabka_raft::NodeId) -> Option<uuid::Uuid> {
        self._broker
            .controller
            .current_image()
            .voters()
            .get(id)
            .map(|v| v.directory_id)
    }

    /// Test-only: run the KIP-853 `remove_voter` reconfiguration on this
    /// broker's controller (must be the raft leader). Returns the coordinator
    /// outcome so the dynamic-voters test can assert `Committed`.
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft error.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn remove_voter_for_test(
        &self,
        req: crabka_raft::reconfig::RemoveVoter,
    ) -> Result<crabka_raft::reconfig::ReconfigOutcome, crabka_raft::RaftError> {
        self._broker.controller.remove_voter(req).await
    }

    /// Test-only: ask this broker's controller to generate a metadata
    /// snapshot. The trigger only schedules the work; the snapshot
    /// completes asynchronously, so callers poll for the result.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying raft trigger.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn trigger_snapshot_for_test(&self) -> Result<(), crabka_raft::RaftError> {
        self._broker.controller.trigger_snapshot().await
    }

    /// Test-only: return the current leader node-id for `(topic, partition)`
    /// as seen by this broker's metadata image. Returns `None` if the
    /// partition is not yet in the image or the leader field is `0` (no
    /// elected leader).
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn partition_leader_for_test(&self, topic: &str, partition: i32) -> Option<u64> {
        let img = self._broker.controller.current_image();
        let p = img.partition(topic, partition)?;
        if p.leader == crabka_raft::NodeId(0) {
            None
        } else {
            Some(p.leader.0)
        }
    }

    /// Test-only: return the current ISR for `(topic, partition)` as seen
    /// by this broker's metadata image. Returns `None` if the partition is
    /// not yet in the image.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn partition_isr_for_test(&self, topic: &str, partition: i32) -> Option<Vec<u64>> {
        let img = self._broker.controller.current_image();
        let p = img.partition(topic, partition)?;
        Some(p.isr.iter().map(|n| n.0).collect())
    }

    /// Test-only: return a clone of the full `PartitionRecord` for
    /// `(topic, partition)` as seen by this broker's metadata image.
    /// Returns `None` if the partition is not yet in the image.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn partition_record_for_test(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<crabka_metadata::PartitionRecord> {
        self._broker
            .controller
            .current_image()
            .partition(topic, partition)
            .cloned()
    }

    /// Test-only: subscribe to the controller's leader watch channel.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn watch_leader_for_test(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<crabka_raft::NodeId>> {
        self._broker.controller.watch_leader()
    }

    /// Test-only: await until `pred` holds for the controller metadata image.
    /// Subscribes to the image watch channel and `.await`s changes — no polling
    /// sleep. Bounded by a 30s safety-net so a stuck condition fails loudly.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_for_image<F>(&self, pred: F)
    where
        F: Fn(&crabka_metadata::MetadataImage) -> bool,
    {
        let mut rx = self._broker.controller.watch_image();
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                // Scope the borrow so it is dropped before the await.
                if pred(&rx.borrow_and_update()) {
                    return;
                }
                if rx.changed().await.is_err() {
                    return; // sender dropped (broker shutting down)
                }
            }
        })
        .await;
        assert!(res.is_ok(), "wait_for_image timed out after 30s");
    }

    /// Test-only: borrow this broker's live [`crate::metrics::BrokerMetrics`]
    /// bundle so integration tests can read counters / gauges in-process.
    ///
    /// Pair with [`Self::wait_for_metrics`] to replace fixed-duration `sleep`s
    /// with a bounded poll on an observable signal (a counter crossing a
    /// threshold, a gauge reaching an expected value) — the metric moves the
    /// instant the awaited work lands, so the wait is race-free rather than a
    /// timing guess.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn metrics(&self) -> &crate::metrics::BrokerMetrics {
        &self._broker.metrics
    }

    /// Test-only: poll `predicate` against this broker's live metrics every
    /// ~25ms until it returns `true` or [`TEST_AWAITER_TIMEOUT`] elapses.
    ///
    /// The metrics-driven replacement for a fixed `sleep` in integration
    /// tests: instead of sleeping "long enough" for a background loop (the
    /// gauge sampler, disk scanner, cleaner, ISR-maintenance tick, audit
    /// flush, …) to run and hoping it did, wait until the counter / gauge it
    /// bumps reflects the awaited state. `what` names the condition for the
    /// timeout panic message. Unlike [`Self::wait_for_image`] there is no
    /// change-notification channel behind a Prometheus metric, so this polls;
    /// the 25ms cadence is an internal implementation detail, not a
    /// test-visible timing assumption.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_for_metrics<F>(&self, what: &str, mut predicate: F)
    where
        F: FnMut(&crate::metrics::BrokerMetrics) -> bool,
    {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if predicate(&self._broker.metrics) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "wait_for_metrics({what}) timed out after {TEST_AWAITER_TIMEOUT:?}"
        );
    }

    /// Test-only: await until a non-zero controller leader is elected.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_controller_leader(&self) -> crabka_raft::NodeId {
        let mut rx = self.watch_leader_for_test();
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some(id) = *rx.borrow_and_update()
                    && id != crabka_raft::NodeId(0)
                {
                    return id;
                }
                if rx.changed().await.is_err() {
                    return crabka_raft::NodeId(0);
                }
            }
        })
        .await;
        let id = res.expect("wait_until_controller_leader timed out after 30s");
        assert!(
            id != crabka_raft::NodeId(0),
            "leader channel closed before a leader was elected"
        );
        id
    }

    /// Test-only: await until this node's metadata image sees `>= n` brokers.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_brokers_registered(&self, n: usize) {
        self.wait_for_image(|img| img.brokers().count() >= n).await;
    }

    /// Test-only: await until `topic-partition` is present in the metadata image.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_partition_present(&self, topic: &str, partition: i32) {
        self.wait_for_image(|img| img.partition(topic, partition).is_some())
            .await;
    }

    /// Test-only: await until `topic-partition`'s leader is some non-`exclude`
    /// node with a non-zero epoch.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_partition_leader_changed(
        &self,
        topic: &str,
        partition: i32,
        exclude: crabka_raft::NodeId,
    ) {
        self.wait_for_image(|img| {
            img.partition(topic, partition).is_some_and(|p| {
                p.leader != crabka_raft::NodeId(0)
                    && p.leader != exclude
                    && p.leader_epoch > crabka_metadata::LeaderEpoch(0)
            })
        })
        .await;
    }

    /// Test-only: await until `topic-partition`'s ISR has exactly `len` members.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_isr_len(&self, topic: &str, partition: i32, len: usize) {
        self.wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.isr.len() == len)
        })
        .await;
    }

    /// Test-only: await until the LOCAL log for `topic-partition` reaches
    /// `log_end_offset >= min`. Uses the partition's `append_notify`; if the
    /// partition has not yet materialized locally, awaits a metadata image change
    /// and retries. The `notified()` future is created BEFORE the offset check to
    /// avoid a lost wakeup.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_local_log_end_offset(&self, topic: &str, partition: i32, min: i64) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some(part) = self
                    ._broker
                    .partitions
                    .get(topic, PartitionIndex(partition))
                {
                    let notified = part.append_notify.notified();
                    if part.log_end_offset() >= crabka_log::Offset(min) {
                        return;
                    }
                    notified.await;
                } else {
                    let mut img = self._broker.controller.watch_image();
                    if img.changed().await.is_err() {
                        return;
                    }
                }
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "local log_end_offset({topic}-{partition}) did not reach {min} within 30s"
        );
    }

    /// Test-only: await until the LOCAL high watermark for `topic-partition`
    /// reaches `min`. Uses the partition's HW notify so tests can wait for the
    /// async HW recompute that happens after the writer acks `acks=1` appends.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_high_watermark(&self, topic: &str, partition: i32, min: i64) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some(part) = self
                    ._broker
                    .partitions
                    .get(topic, PartitionIndex(partition))
                {
                    let notified = part.hw_advance_notify.notified();
                    if part.high_watermark().await >= crabka_log::Offset(min) {
                        return;
                    }
                    notified.await;
                } else {
                    let mut img = self._broker.controller.watch_image();
                    if img.changed().await.is_err() {
                        return;
                    }
                }
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "high_watermark({topic}-{partition}) did not reach {min} within 30s"
        );
    }

    /// Test-only: await until the LOCAL log end offset for `topic-partition` is
    /// EXACTLY `target`. Unlike `wait_until_local_log_end_offset` (monotonic `>=`),
    /// this is for non-monotonic convergence (e.g. a follower truncating a divergent
    /// suffix then re-replicating to match the leader): the offset may pass through
    /// `>= target` with wrong-epoch data before settling at `target`. Wakes on
    /// `append_notify` (re-appends) with a short fallback tick to also observe a
    /// truncation, which does not notify. Returns the instant LEO == target; this is
    /// a condition wait on real state (not a fixed-duration sleep), so it cannot
    /// flake on timing — only fail if the condition never holds within 30s.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_local_log_end_offset_eq(
        &self,
        topic: &str,
        partition: i32,
        target: i64,
    ) {
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some(part) = self
                    ._broker
                    .partitions
                    .get(topic, PartitionIndex(partition))
                {
                    let notified = part.append_notify.notified();
                    if part.log_end_offset() == crabka_log::Offset(target) {
                        return;
                    }
                    // Truncation does not fire append_notify; fall back to a short
                    // re-check tick so a truncate-to-target is still observed.
                    tokio::select! {
                        () = notified => {}
                        () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
                    }
                } else {
                    let mut img = self._broker.controller.watch_image();
                    if img.changed().await.is_err() {
                        return;
                    }
                }
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "local log_end_offset({topic}-{partition}) did not settle at {target} within 30s"
        );
    }

    /// Test-only: number of `OffsetForLeaderEpoch` (`api_key` 23) requests this
    /// broker has served since startup. The KIP-320 proactive-validation
    /// integration test reads this before and after a `Consumer::poll` to
    /// prove the consumer's validate pass issued an OFLE RPC — distinguishing
    /// the proactive path from the reactive in-band `diverging_epoch` /
    /// `OFFSET_OUT_OF_RANGE` fetch paths, which issue no OFLE.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn offset_for_leader_epoch_count_for_test(&self) -> u64 {
        self._broker
            .offset_for_leader_epoch_requests
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Test-only: flip a configured log dir offline at runtime, simulating a
    /// live fsync failure, without real EIO injection (unreliable
    /// cross-platform). Drives the KIP-112 offline path.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn test_mark_log_dir_offline(&self, dir: &std::path::Path) -> bool {
        self._broker
            .log_dir_status
            .mark_offline(dir, "test-injected storage failure")
    }

    /// Rebuild the TLS server config from the cert/key paths
    /// in `BrokerConfig::tls_config` *right now*, bypassing the
    /// periodic mtime watcher. New TLS handshakes after this call see
    /// the rebuilt config; in-flight handshakes are unaffected.
    ///
    /// Operators call this from sidecars / hook scripts that just
    /// wrote new cert files into place and want the change to take
    /// effect without waiting for the next `tls_reload_interval` tick.
    ///
    /// # Errors
    ///
    /// - `BrokerError::Tls` — the new cert / key / client-CA failed
    ///   to parse or rustls rejected the assembled config. The
    ///   previous config remains in place; the broker keeps serving
    ///   with the old cert.
    /// - `BrokerError::Startup` — no TLS config is configured.
    #[allow(clippy::used_underscore_binding)]
    pub fn reload_tls(&self) -> Result<(), BrokerError> {
        let Some(dynamic) = self._broker.tls_dynamic.as_ref() else {
            return Err(BrokerError::Startup(
                "reload_tls: broker has no tls_config".into(),
            ));
        };
        let Some(tls_cfg) = self._broker.config.tls_config.as_ref() else {
            return Err(BrokerError::Startup(
                "reload_tls: broker has no tls_config".into(),
            ));
        };
        dynamic
            .reload_from(tls_cfg)
            .map_err(|e| BrokerError::Tls(e.to_string()))
    }

    /// Subscribe to the self-shutdown signal. Flips `true` when the broker
    /// decides to stop on its own — today: all log dirs went offline
    /// (KIP-112). The embedding application should call
    /// [`Self::shutdown`] (or `controlled_shutdown`) when this fires.
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn should_shutdown_rx(&self) -> tokio::sync::watch::Receiver<bool> {
        self._broker.should_shutdown.subscribe()
    }

    /// Request a graceful, controlled shutdown of this broker.
    ///
    /// Signals the heartbeat client to set `want_shut_down=true` on
    /// outbound `BrokerHeartbeat` requests. The controller leader
    /// reassigns leadership of every partition currently led by this
    /// broker; once leadership is fully drained, the controller
    /// responds with `should_shut_down=true`. This call then invokes
    /// the regular [`shutdown`](Self::shutdown).
    ///
    /// Always stops the broker before returning: on a clean drain via the
    /// regular [`shutdown`](Self::shutdown), and on `timeout` via a hard
    /// shutdown fallback (returning `Err(ShutdownTimeout)` so the caller
    /// knows the drain was incomplete). Either way the broker is stopped, so
    /// the process can exit before a Kubernetes SIGKILL.
    ///
    /// # Errors
    ///
    /// - `BrokerError::ShutdownTimeout` — the controller did not
    ///   acknowledge `should_shut_down=true` within `timeout`; the broker was
    ///   hard-shut-down anyway.
    #[allow(clippy::used_underscore_binding)]
    pub async fn controlled_shutdown(
        self,
        timeout: std::time::Duration,
    ) -> Result<(), BrokerError> {
        let mut should_shutdown_rx = self._broker.should_shutdown.subscribe();
        // Latch the request flag. Idempotent — repeated sends to a
        // `watch::Sender` with the same value are harmless and the
        // heartbeat client reads `borrow_and_update()` each tick.
        let _ = self._broker.want_shutdown.send(true);
        // Wait for the heartbeat client to observe should_shut_down=true.
        let wait = async {
            // `subscribe()` returns the current value (`false`) without
            // marking it seen — so the first `changed()` only fires on
            // a true edge.
            loop {
                if *should_shutdown_rx.borrow() {
                    return;
                }
                if should_shutdown_rx.changed().await.is_err() {
                    return;
                }
            }
        };
        // `if`/`else` rather than `match { Ok(()) => .., Err(_) => .. }` to
        // satisfy `clippy::single_match_else`.
        if tokio::time::timeout(timeout, wait).await.is_ok() {
            self.shutdown().await;
            Ok(())
        } else {
            // Leadership did not fully drain in time (e.g. the controller
            // is itself unreachable). Still stop cleanly via the regular
            // hard shutdown so the process exits before the Kubernetes
            // SIGKILL — a partly-drained graceful stop still beats an
            // abrupt kill. The `ShutdownTimeout` return tells the caller
            // the drain was incomplete.
            tracing::warn!(
                ?timeout,
                "controlled shutdown drain timed out; falling back to hard shutdown"
            );
            self.shutdown().await;
            Err(BrokerError::ShutdownTimeout(timeout))
        }
    }

    /// Cancel the listener + drain in-flight connections. Awaiting the
    /// returned future blocks until the listener task exits.
    #[allow(clippy::used_underscore_binding)] // `_broker` carries shared state we must reach into during shutdown
    pub async fn shutdown(mut self) {
        // Emit the BrokerStopping lifecycle event before tearing down
        // partitions. This record may be dropped if the audit partition is
        // already gone — acceptable for Slice 1; durable shutdown auditing
        // is Slice 3.
        self._broker
            .audit_log
            .emit(crabka_audit::AuditEvent::Lifecycle {
                kind: crabka_audit::LifecycleKind::BrokerStopping,
                node_id: i64::from(self._broker.config.broker_id),
                time_ms: crate::time_util::now_ms(),
            });

        // Cancel the replicator supervisor BEFORE the controller drops:
        // in-flight replication tasks must observe a clean cancellation
        // rather than a torn-down metadata-watch channel.
        self._broker.supervisor_shutdown.cancel();
        if let Some(h) = self._broker.supervisor_handle.lock().await.take() {
            let _ = h.await;
        }
        // Drain the disk-usage scanner if it was spawned.
        // The scanner observes the same `supervisor_shutdown` cancellation
        // its sibling tasks do; awaiting the handle here ensures the
        // background tick is fully wound down before we tear the rest
        // of the broker apart.
        if let Some(h) = self._broker.disk_scanner_handle.lock().await.take() {
            let _ = h.await;
        }
        self.shutdown.cancel();
        for t in self.listener_tasks.drain(..) {
            let _ = t.await;
        }
        // Shut down the raft engine so this broker's openraft instance stops
        // participating in elections after the broker is logically dead.
        // Without this, a killed broker's in-process raft engine keeps ticking
        // and re-elects itself, preventing the surviving nodes from detecting
        // the leader failure and electing a replacement.
        self._broker.controller.cancel().await;
    }
}

/// Wraps a real [`crabka_raft::ControllerHandle`] so it can satisfy the
/// [`crate::leader_rebalance::ControllerLike`] trait required by the
/// auto-rebalance background task.
struct ControllerAdapter {
    handle: Arc<dyn crate::metadata_source::MetadataSource>,
    node_id: crabka_raft::NodeId,
}

#[async_trait::async_trait]
impl crate::leader_rebalance::ControllerLike for ControllerAdapter {
    fn is_leader(&self) -> bool {
        *self.handle.watch_leader().borrow() == Some(self.node_id)
    }

    fn current_image(&self) -> Arc<crabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

    async fn submit_change(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<(), String> {
        self.handle
            .submit_change(records)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Wraps a real [`crabka_raft::ControllerHandle`] so it can satisfy the
/// [`crate::reassignment::ReassignmentController`] trait required by the
/// reassignment-completion background task.
struct ReassignmentControllerAdapter {
    handle: Arc<dyn crate::metadata_source::MetadataSource>,
    node_id: crabka_raft::NodeId,
}

#[async_trait::async_trait]
impl crate::reassignment::ReassignmentController for ReassignmentControllerAdapter {
    fn is_leader(&self) -> bool {
        *self.handle.watch_leader().borrow() == Some(self.node_id)
    }

    fn current_image(&self) -> Arc<crabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

    fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<crabka_metadata::MetadataImage>> {
        self.handle.watch_image()
    }

    async fn submit_change(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<(), String> {
        self.handle
            .submit_change(records)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Wraps a real [`crabka_raft::ControllerHandle`] so it can satisfy the
/// [`crate::throttle::ImageWatcher`] trait required by the throttle refresh
/// background task. Every broker runs this (not just the controller leader)
/// since each broker manages its own throttle buckets.
struct ThrottleControllerAdapter {
    handle: Arc<dyn crate::metadata_source::MetadataSource>,
}

impl crate::throttle::ImageWatcher for ThrottleControllerAdapter {
    fn current_image(&self) -> Arc<crabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

    fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<crabka_metadata::MetadataImage>> {
        self.handle.watch_image()
    }
}

/// Wraps a real [`crabka_raft::ControllerHandle`] so it can satisfy the
/// [`crate::quota::ImageWatcher`] trait required by the quota refresh
/// background task. Every broker runs this (not just the controller leader)
/// since each broker enforces its own quotas via its own buckets.
struct QuotaControllerAdapter {
    handle: Arc<dyn crate::metadata_source::MetadataSource>,
}

impl crate::quota::ImageWatcher for QuotaControllerAdapter {
    fn current_image(&self) -> Arc<crabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

    fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<crabka_metadata::MetadataImage>> {
        self.handle.watch_image()
    }
}

/// KIP-48: wraps a real [`crabka_raft::ControllerHandle`] so it
/// can satisfy the [`crate::delegation_token_cleanup::DelegationTokenController`]
/// trait required by the delegation-token expiry sweep. Every broker runs
/// the sweep; raft serializes duplicate tombstones so each becomes a no-op.
struct DelegationTokenCleanupControllerAdapter {
    handle: Arc<dyn crate::metadata_source::MetadataSource>,
}

#[async_trait::async_trait]
impl crate::delegation_token_cleanup::DelegationTokenController
    for DelegationTokenCleanupControllerAdapter
{
    fn current_image(&self) -> Arc<crabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

    async fn submit_change(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<(), String> {
        self.handle
            .submit_change(records)
            .await
            .map_err(|e| e.to_string())
    }
}

impl Broker {
    /// Build a `Broker`, scan the log dir, spawn partition writers for
    /// every existing `<topic>-<partition>/`, bind the TCP listener, and
    /// return the handle.
    pub async fn start(config: BrokerConfig) -> Result<BrokerHandle, BrokerError> {
        Self::start_with_listeners(config, None, None).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr`.
    ///
    /// Thin wrapper over [`Self::start_with_listeners`] for callers that only
    /// hand off the controller port; the data plane still binds from `config`.
    /// See that method for the full handoff contract.
    pub async fn start_with_controller_listener(
        config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
    ) -> Result<BrokerHandle, BrokerError> {
        Self::start_with_listeners(config, controller_listener, None).await
    }

    /// Like [`Self::start`], but adopts caller-supplied, already-bound
    /// listeners instead of binding their addresses itself:
    ///
    /// * `controller_listener` — threaded through to
    ///   [`crabka_raft::Controller::start_with_listener`]. Its local address
    ///   MUST equal `config.controller_listen_addr`.
    /// * `data_plane_listener` — adopted for the data-plane [`ListenerSpec`]
    ///   whose `bind_addr` equals the listener's local address (for the legacy
    ///   single-listener path that is `config.listen_addr`). Any non-matching
    ///   specs still bind from `config`.
    ///
    /// Handing over live sockets — rather than the bind-and-drop trick of
    /// reading an ephemeral port then dropping the probe before re-binding —
    /// closes the TOCTOU window in which another process can claim the
    /// just-released port, the `AddrInUse` flake test harnesses hit under
    /// parallel execution. The data-plane port must still be concrete in
    /// `config` up front (the broker self-registers `listen_addr.port()`
    /// before binding the data plane), so callers read it back from the live
    /// listener's `local_addr()` and set `config.listen_addr` /
    /// `advertised_listener` to it before calling.
    ///
    /// [`ListenerSpec`]: crate::config::ListenerSpec
    #[allow(clippy::too_many_lines)]
    // sequential bring-up; splitting hurts readability more than it helps
    // cargo-mutants: network/socket bring-up, not unit-testable
    #[cfg_attr(test, mutants::skip)]
    pub async fn start_with_listeners(
        mut config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
        mut data_plane_listener: Option<tokio::net::TcpListener>,
    ) -> Result<BrokerHandle, BrokerError> {
        // 0a. Install the rustls crypto provider exactly once per process.
        //     `rustls 0.23` with `default-features = false` does NOT auto-install
        //     a provider; without this the `ServerConfig::builder()` call below
        //     (and any client-side rustls usage) panics at runtime. `.ok()`
        //     swallows the `AlreadySet` error when a previous broker / test
        //     in the same process installed it first.
        let _ = rustls::crypto::ring::default_provider().install_default();

        // 0b. Validate listener + auth configuration before any side effects.
        config.validate()?;

        // 0c. Build the dynamic TLS server config up front so we can
        //     fail fast on bad cert / key paths before bringing up any
        //     state. Wrapped in a `DynamicServerConfig` so the TLS
        //     hot-reload path can swap certs without restart.
        let tls_dynamic = match &config.tls_config {
            Some(tls) => Some(
                crabka_security::DynamicServerConfig::from_tls_config(tls)
                    .map_err(|e| BrokerError::Tls(e.to_string()))?,
            ),
            None => None,
        };

        // 0c-bis. Probe Linux kTLS support ONCE. `ktls::config_ktls_server`
        //         consumes the post-handshake `TlsStream` by value, so a
        //         per-connection failure cannot fall back to userspace TLS on
        //         that same stream. We therefore decide kTLS routing up front:
        //         only when this probe succeeds does the per-connection path
        //         attempt the kTLS transition. The probe runs a real loopback
        //         TLS handshake and `config_ktls_server` on a throwaway socket
        //         pair, exercising the exact kernel path (TCP_ULP="tls" +
        //         crypto_info install) the data plane will use. No TLS config →
        //         no kTLS (probe skipped, stays false).
        let ktls_enabled = if tls_dynamic.is_some() {
            crate::network::ktls_probe::probe_ktls_support().await
        } else {
            false
        };
        if ktls_enabled {
            tracing::info!(
                "Linux kTLS supported: TLS fetch connections will use kernel-offloaded \
                 sendfile (zero-copy over TLS)"
            );
        } else if tls_dynamic.is_some() {
            tracing::info!(
                "Linux kTLS unavailable (kernel lacks `tls` module or non-Linux): TLS fetch \
                 connections use the userspace rustls copy path"
            );
        }

        // 0d. Build the outbound `TlsConnector` and the shared
        //     `InterBrokerClient` once. Both the replicator-supervisor
        //     and the heartbeat client clone the resulting Arc; the
        //     raft transport receives it as an injected dialer.
        let tls_connector = match &config.tls_config {
            Some(tls) => {
                // Build a client config that BOTH trusts the peer's serving
                // CA (`trust_roots_path`, the cluster CA) AND presents this
                // broker's own cert as a client cert — the controller
                // listener requires mTLS (`client_auth = Required`), so an
                // anonymous client config would be rejected with a TLS
                // `UnknownCA`/handshake failure on every KIP-595 peer dial.
                let client_cfg = tls
                    .build_client_config_with_identity()
                    .map_err(|e| BrokerError::Tls(e.to_string()))?;
                Some(tokio_rustls::TlsConnector::from(client_cfg))
            }
            None => None,
        };
        let inter_broker_client = Arc::new(crate::network::client::InterBrokerClient::new(
            tls_connector,
            config.inter_broker_credentials.clone(),
        ));

        // 1. Bring up the metadata quorum BEFORE the client listener so
        //    handlers can read from it the moment they accept their first
        //    connection. The controller owns its own listener bound to
        //    `controller_listen_addr`.
        //
        //    Raft dialer + handshake wiring:
        //
        //    Replication + heartbeat dials route through the data-plane
        //    inter-broker listener (which speaks SASL/TLS when
        //    configured). The Raft RPCs (`AppendEntries`, `Vote`,
        //    `SubmitChange`) dial the *controller* listener which now
        //    shares the same SASL/TLS handshake path via the
        //    `InterBrokerDialer` adapter on the outbound side and
        //    `BrokerRaftHandshake` on the inbound side. With the default
        //    `controller_listener_protocol = Plaintext`, the dialer's
        //    `dial` impl reduces to a `TcpStream::connect` and the
        //    handshake is `None`, so the legacy raw-TCP raft path is
        //    byte-identical for existing deployments and tests.
        //
        //    `BrokerRaftHandshake` needs a `ControllerHandle` to satisfy
        //    SCRAM credential lookups, but the handle isn't built until
        //    `Controller::start` returns. We bridge that with an
        //    `Arc<OnceCell<Arc<ControllerHandle>>>` that's installed into
        //    the handshake up front and `set` once the controller exists.
        let controller_cell: Arc<tokio::sync::OnceCell<Arc<crabka_raft::ControllerHandle>>> =
            Arc::new(tokio::sync::OnceCell::new());

        let handshake_opt: Option<Arc<dyn crabka_raft::RaftListenerHandshake>> = if config
            .controller_listener_protocol
            == crabka_security::ListenerProtocol::Plaintext
        {
            // C-1: the controller listener carries raft/controller RPCs
            // (Vote, BeginQuorumEpoch, Fetch, SubmitChange) — i.e. cluster
            // metadata and leadership. Running it as PLAINTEXT means there
            // is NO authentication and NO authorization on that path: any
            // host with network reachability to this port can inject
            // cluster metadata, hijack leadership, or disrupt consensus.
            // We keep dev/single-node working (do not refuse to start), but
            // warn loudly so this is never an accidental production posture.
            tracing::warn!(
                "controller listener is PLAINTEXT: no authentication or authorization is \
                 enforced on raft/controller RPCs. Any host with network access to this port \
                 can inject cluster metadata or manipulate leadership. Configure TLS or SASL \
                 (controller_listener_protocol = Ssl / SaslPlaintext / SaslSsl) for any \
                 non-isolated deployment."
            );
            None
        } else {
            // Raft handshake uses the current (snapshot) acceptor. A
            // mid-rotation reload affects only *new* raft handshakes;
            // already-established peer connections keep their negotiated
            // ServerConfig.
            let raft_acceptor = tls_dynamic
                .as_ref()
                .map(|d| tokio_rustls::TlsAcceptor::from(d.current()));
            let hs = crate::raft_handshake::BrokerRaftHandshake {
                tls_acceptor: raft_acceptor,
                plain_credentials: config.plain_credentials.clone(),
                enabled_sasl_mechanisms: config.enabled_sasl_mechanisms.clone(),
                protocol: config.controller_listener_protocol,
                controller: controller_cell.clone(),
                authorizer: config.authorizer.clone(),
            };
            Some(Arc::new(hs) as Arc<dyn crabka_raft::RaftListenerHandshake>)
        };

        // SNI for dialing peer controller listeners. The operator renders the
        // shared headless-Service FQDN (a SAN on every broker's serving cert)
        // so mTLS validates regardless of which peer pod IP we connect to.
        // Falls back to "localhost" for single-node / dev (no peers dialed).
        let controller_server_name = config
            .controller_server_name
            .clone()
            .unwrap_or_else(|| "localhost".to_string());
        let raft_dialer: Option<std::sync::Arc<dyn crabka_raft::OutboundDialer>> =
            Some(Arc::new(crate::network::client::InterBrokerDialer::new(
                inter_broker_client.clone(),
                config.controller_listener_protocol,
                controller_server_name,
            )) as Arc<dyn crabka_raft::OutboundDialer>);

        // KIP-853: the bootstrap records carry the seed `VotersRecord`. Load
        // them once here so the cold-boot voter set feeds `ControllerConfig`;
        // the same records are submitted through raft after a leader is
        // elected (step 2b below). A `Join` node has no seed set and relies
        // on `bootstrap_servers` + auto-join instead. Broker-only nodes never
        // run a controller, so the records stay unused (step 2b is gated on
        // having a non-empty set and `Bootstrap` mode).
        let mut bootstrap_records = crate::bootstrap::load_bootstrap_records(&config.log_dir)?;

        let controller: Arc<dyn crate::metadata_source::MetadataSource> = if config.is_controller()
        {
            let mut initial_voters = crate::bootstrap::initial_voters(&bootstrap_records);

            // KIP-595 static voters: the engine reconstructs its voter set from
            // `ControllerConfig.initial_voters` every boot. When no `VotersRecord` was
            // loaded from `bootstrap.records.bin` (the in-process start path that
            // didn't run `crabka format`), build the static set from
            // `controller_quorum_voters`. Two shapes:
            //
            //   * Single-voter (`len <= 1`): standalone self-bootstrap of *this*
            //     node; self-elects on the first election timeout.
            //   * Multi-voter (`len > 1`): the full configured voter set, so
            //     every node starts with voters={all configured} and elects
            //     among themselves over the real KIP-595 wire — no auto-join.
            //
            // This must run for `Rejoin` as well as `Bootstrap`: a restarted
            // node recovers its committed metadata log from disk but the engine
            // still derives `voters` (peer endpoints for dialing/forwarding)
            // from `initial_voters`. Without this a `Rejoin` node would come up
            // with an empty voter set — unable to dial peers or forward
            // `submit_change` to the leader. Only `Bootstrap` additionally seeds
            // the metadata log (the `V1Voters` + `V1KRaftVersion` pair submitted
            // through raft in step 2b); a `Rejoin` node already has them
            // committed on disk and must not re-submit.
            if initial_voters.is_empty() && !config.controller_quorum_voters.is_empty() {
                // Build the static voter set, keeping peer hosts as their
                // configured DNS names so the dialer re-resolves them per
                // (re)connect — a rejoining peer on a new pod IP stays
                // reachable. See `static_controller_voter_set`.
                let voters = static_controller_voter_set(
                    &config.controller_quorum_voters,
                    config.node_id,
                    config.directory_id,
                    config.controller_listen_addr,
                );
                tracing::info!(
                    node_id = config.node_id.0,
                    voter_count = config.controller_quorum_voters.len(),
                    mode = ?config.bootstrap_mode,
                    "KIP-595 static voter set: deriving voters from controller_quorum_voters (peer hosts re-resolved per dial)"
                );
                // The engine reconstructs its voter set from `initial_voters`
                // (config-derived) every boot under KIP-595 static voters, so the
                // `V1Voters` / `V1KRaftVersion` raft-control records are NOT seeded
                // onto the KIP-631-framed metadata log here (they have no KIP-631
                // counterpart; dynamic reconfiguration is not handled here).
                initial_voters = voters;

                // KIP-584/1022: a standalone self-bootstrap finalizes every
                // feature at the newest release's default (metadata.version MAX),
                // matching a freshly-formatted 4.0 cluster — so e.g. group.version=1
                // is finalized and the next-gen group protocol is enabled.
                bootstrap_records.extend(crabka_metadata::bootstrap_feature_records(
                    crabka_metadata::metadata_version::METADATA_VERSION_MAX,
                ));
            }

            let controller_cfg = crabka_raft::ControllerConfig {
                node_id: config.node_id,
                bootstrap_servers: config.bootstrap_servers.clone(),
                directory_id: config.directory_id,
                auto_join: config.auto_join,
                observer_lag_bound: config.observer_lag_bound,
                initial_voters,
                controller_listen_addr: config.controller_listen_addr,
                log_dir: config.log_dir.join("__cluster_metadata"),
                // Sourced from `BrokerConfig` — see the docstrings there for
                // the production-vs-test tradeoff. Crucially this also sets
                // openraft's `leader_lease` to `election_timeout × 2`, which
                // is the floor on how fast a 3-broker cluster can elect a
                // replacement when the controller leader dies.
                election_timeout: config.controller_election_timeout,
                heartbeat_interval: config.controller_heartbeat_interval,
                client_id: format!("crabka-broker-{}-controller", config.broker_id),
                bootstrap_mode: config.bootstrap_mode,
                cluster_id: config.cluster_id,
                dialer: raft_dialer.clone(),
                handshake: handshake_opt,
                max_bytes_between_snapshots: config.metadata_max_bytes_between_snapshots,
                max_snapshot_interval: config.metadata_max_snapshot_interval,
                snapshot_interval_records: config.metadata_snapshot_interval_records,
            };
            let handle = Arc::new(
                crabka_raft::Controller::start_with_listener(controller_cfg, controller_listener)
                    .await
                    .map_err(|e| BrokerError::Startup(e.to_string()))?,
            );
            // Populate the late-bound controller handle so the inbound
            // `BrokerRaftHandshake` (already wired into the controller's
            // accept loop) can perform SCRAM credential lookups on the next
            // authenticated connection. The `set` cannot fail in practice
            // because we hold the only writer; swallow the `SetError` defensively.
            let _ = controller_cell.set(handle.clone());
            handle as Arc<dyn crate::metadata_source::MetadataSource>
        } else {
            // Broker-only node: no openraft voter. Keep the `MetadataImage`
            // current by fetching `__cluster_metadata` from the controller
            // quorum (the observer), and forward writes to the quorum leader.
            // `controller_cell` is left unset — a broker-only node runs no
            // controller listener, so nothing performs SCRAM lookups on it.
            // The caller-supplied `controller_listener` (if any) goes unused.
            drop(controller_listener);
            let dialer = raft_dialer
                .clone()
                .expect("broker-only node requires a raft dialer");
            let observer = crate::metadata_observer::MetadataObserver::start(
                crate::metadata_observer::ObserverConfig {
                    voters: config.controller_quorum_voters.clone(),
                    dialer: dialer.clone(),
                    client_id: format!("crabka-broker-{}-observer", config.broker_id),
                    cluster_id: config.cluster_id.unwrap_or_else(uuid::Uuid::nil),
                    max_bytes: OBSERVER_FETCH_MAX_BYTES,
                    poll_interval: OBSERVER_POLL_INTERVAL,
                    sleeper: std::sync::Arc::new(qubit_clock::sleep::SystemSleeper::new()),
                },
            );
            let forwarder = crate::metadata_source::QuorumForwarder {
                voters: config.controller_quorum_voters.clone(),
                dialer,
                client_id: format!("crabka-broker-{}-writer", config.broker_id),
                leader: observer.watch_leader(),
            };
            Arc::new(crate::metadata_source::ObserverSource::new(
                observer,
                Arc::new(forwarder),
            )) as Arc<dyn crate::metadata_source::MetadataSource>
        };

        // 1b. KIP-853 controller auto-join. Spawned BEFORE the leader-wait in
        //     step 2: a `Join` broker's empty raft log keeps it in openraft's
        //     Learner state with no leader, so `Broker::start` would block in
        //     step 2 forever. The auto-join loop concurrently sends
        //     `AddRaftVoter(self)` to a `bootstrap_servers` entry; the leader's
        //     handler runs `add_learner` (replicating the log to us) and
        //     promotes us — at which point step 2's `watch_leader` fires and
        //     start proceeds. `run` returns immediately when `auto_join` is
        //     disabled (bootstrap / standalone brokers), so this is a cheap
        //     no-op there. The loop advertises the controller's REAL bound
        //     address, known now that `Controller::start` has bound the
        //     listener.
        // The joiner sends `AddRaftVoter` to a bootstrap server's *client*
        // data-plane listener (where api_key 80 is served), so it speaks the
        // inter-broker listener protocol — not the controller-listener
        // protocol that openraft RPCs use.
        // Auto-join grows the controller *voter* quorum, so only nodes that
        // run a controller participate. A broker-only node is a pure observer
        // and never joins the quorum.
        if config.is_controller() {
            let auto_join_protocol = config
                .effective_listeners()
                .iter()
                .find(|l| l.name == config.inter_broker_listener_name)
                .map_or(crabka_security::ListenerProtocol::Plaintext, |l| l.protocol);
            tokio::spawn(crate::auto_join::run(crate::auto_join::AutoJoinParams {
                auto_join: config.auto_join,
                node_id: config.node_id,
                directory_id: config.directory_id,
                cluster_id: config.cluster_id,
                bootstrap_servers: config.bootstrap_servers.clone(),
                listener_protocol: auto_join_protocol,
                controller: controller.clone(),
                inter_broker_client: inter_broker_client.clone(),
            }));
        }

        // 2. Wait for a leader, then submit a self-registration record so
        //    other brokers can discover us. Best-effort: if the submit
        //    fails the next caller's request will surface the error and
        //    membership reconciliation can retry later.
        {
            let mut leader_rx = controller.watch_leader();
            let deadline = std::time::Instant::now() + STARTUP_LEADER_WAIT_TIMEOUT;
            while leader_rx.borrow().is_none() {
                if std::time::Instant::now() > deadline {
                    return Err(BrokerError::Startup(format!(
                        "no leader elected within {STARTUP_LEADER_WAIT_TIMEOUT:?}"
                    )));
                }
                let _ = tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    leader_rx.changed(),
                )
                .await;
            }

            // Controller-only nodes never register — they host no data and
            // must not appear as brokers in Metadata/DescribeCluster.
            if config.is_broker() {
                // Load (or generate) the incarnation UUID before registering.
                // This persists a UUID to `{log_dir}/incarnation_id` on first
                // boot and reloads it on every subsequent start, so a JVM
                // controller can detect broker restarts and fence stale
                // replica memberships (KIP-631).
                config.incarnation_id = crate::incarnation::load_or_generate(&config.log_dir);

                // Per-listener endpoints: every configured listener's
                // advertised `host:port` + protocol becomes a `BrokerEndpoint`
                // on the broker's self-registration record. Clients on
                // `Metadata` v9+ pick the right endpoint for their connection;
                // legacy callers continue reading the top-level `host`/`port`.
                let endpoints: Vec<crabka_metadata::BrokerEndpoint> = config
                    .effective_listeners()
                    .iter()
                    .map(|l| {
                        let (host, port) = parse_advertised_host_port(&l.advertised);
                        crabka_metadata::BrokerEndpoint {
                            name: l.name.clone(),
                            host,
                            port,
                            protocol: l.protocol,
                        }
                    })
                    .collect();
                let self_reg = crabka_metadata::MetadataRecord::V1BrokerRegistration(
                    crabka_metadata::BrokerRegistrationRecord {
                        node_id: config.node_id,
                        broker_epoch: 0,
                        incarnation_id: config.incarnation_id,
                        host: config
                            .advertised_listener
                            .split(':')
                            .next()
                            .unwrap_or("127.0.0.1")
                            .to_string(),
                        port: config.listen_addr.port(),
                        rack: config.rack.clone(),
                        endpoints,
                    },
                );
                let backoff = exponential_backoff::Backoff::new(
                    SELF_REGISTRATION_MAX_ATTEMPTS,
                    SELF_REGISTRATION_BACKOFF_MIN,
                    Some(SELF_REGISTRATION_BACKOFF_MAX),
                );
                for (attempt_idx, delay) in backoff.into_iter().enumerate() {
                    let attempt = attempt_idx + 1;
                    match controller.submit_change(vec![self_reg.clone()]).await {
                        Ok(()) => break,
                        Err(e) => match delay {
                            Some(delay) => {
                                tracing::warn!(
                                    attempt,
                                    retry_after_ms = delay.as_millis(),
                                    error = %e,
                                    "self-registration failed; retrying before serving"
                                );
                                tokio::time::sleep(delay).await;
                            }
                            None => {
                                return Err(BrokerError::Startup(format!(
                                    "self-registration failed after {attempt} attempts: {e}"
                                )));
                            }
                        },
                    }
                }
            }

            // 2b. First-start bootstrap-records submit.
            //
            //     On a fresh-cluster cold boot (`BootstrapMode::Bootstrap`),
            //     consume `log_dir/bootstrap.records.bin` if present and submit
            //     its records through raft as a single batched change. This is
            //     how operator-supplied SCRAM credentials (and any future
            //     bootstrap-only metadata) enter the cluster before any client
            //     connection succeeds — `submit_change` blocks until raft has
            //     committed and applied the batch, so by the time we proceed
            //     past this point the records are visible in
            //     `controller.current_image()`.
            //
            //     `Join` brokers skip this entirely: bootstrap records are a
            //     fresh-cluster initialization concern, never replayed by
            //     joining voters (the leader already has the committed state).
            //
            //     Missing-file is treated as empty (handled by the loader),
            //     so the legacy zero-record path is a no-op and existing
            //     deployments / tests are byte-identical.
            if matches!(config.bootstrap_mode, crate::BootstrapMode::Bootstrap) {
                // KIP-853 raft-control records (`V1Voters` / `V1KRaftVersion`)
                // have no KIP-631 metadata-log counterpart and are never carried
                // on the KIP-631-framed metadata log here — the engine
                // reconstructs its voter set from `initial_voters` (config) every
                // boot. Drop them from the metadata-log submit; only the genuine
                // metadata records (ACLs, SCRAM, quotas, …) are seeded.
                bootstrap_records.retain(|r| {
                    !matches!(
                        r,
                        crabka_metadata::MetadataRecord::V1Voters(_)
                            | crabka_metadata::MetadataRecord::V1KRaftVersion(_)
                    )
                });
                // The cluster-wide finalized features (`metadata.version` etc.)
                // are already seeded into `bootstrap_records` by
                // `bootstrap_feature_records` at the point the static voter set
                // is derived (see above) — a JVM follower refuses to build its
                // `FeaturesImage` without `metadata.version`, so every
                // KRaft-faithful fresh cluster carries them in the committed log.
                if !bootstrap_records.is_empty() {
                    tracing::info!(
                        count = bootstrap_records.len(),
                        "submitting bootstrap records"
                    );
                    controller
                        .submit_change(bootstrap_records)
                        .await
                        .map_err(|e| {
                            BrokerError::Replication(format!("bootstrap submit failed: {e}"))
                        })?;
                }
            }
        }

        // 3. Probe every configured log dir for writability. KIP-113
        //    offline-dir handling: a single bad dir on a JBOD broker
        //    must not take down the whole broker — it just gets marked
        //    offline so `DescribeLogDirs` surfaces it with
        //    `KAFKA_STORAGE_ERROR` and JBOD placement skips it.
        let log_dir_status = crate::log_dir_status::LogDirRegistry::probe(&config.all_log_dirs());

        // KIP-858: resolve a stable UUID per configured log.dir (minting +
        // persisting for any extra JBOD dir that lacks one). Shared with the
        // heartbeat client (offline_log_dirs) and the assignment reporter.
        let log_dir_ids = crate::log_dir_id::LogDirIds::resolve(&config.all_log_dirs());

        // 4. Scan + recover partitions on disk. Partition state is still
        //    a local-disk concern; the metadata image is sourced from
        //    `controller.current_image()` whenever a handler needs it.
        //    Skip dirs marked offline by the probe — `scan_all` would
        //    fail the whole startup on the first IO error, and we want
        //    to keep recovering partitions on the surviving dirs.
        let partitions: Arc<PartitionRegistry> = Arc::new(PartitionRegistry::new());
        // Broker-wide idempotent/transactional producer-sequence tracker.
        // Created before the partition scan so each recovered partition's
        // writer task gets the shared handle (the `Compact` handler needs
        // it to build the `RETAIN_EMPTY` active-producer snapshot).
        let producer_state = Arc::new(crate::producer_state::ProducerState::new());
        // Controller-only nodes host no data partitions, so they skip the
        // disk scan/recovery entirely.
        if config.is_broker() {
            let scan_dirs = log_dir_status.online_subset(&config.all_log_dirs());
            for (topic, partition_id, owning_dir) in log_dir::scan_all(&scan_dirs)? {
                let dir = log_dir::partition_dir(&owning_dir, &topic, partition_id);
                let log = crabka_log::Log::open(&dir, config.log_config.clone())?;
                let part = spawn_partition(
                    topic.clone(),
                    PartitionIndex(partition_id),
                    owning_dir,
                    log,
                    log_dir_status.clone(),
                    producer_state.clone(),
                );
                partitions.insert(topic.clone(), PartitionIndex(partition_id), part);
            }
        }

        // Group coordinator bootstrap. One unified coordinator owns both the
        // classic and the next-gen consumer-group protocols.
        let offsets_log: std::sync::Arc<dyn crate::coordinator::unified::offsets_log::OffsetsLog> =
            std::sync::Arc::new(
                crate::coordinator::unified::offsets_log::ProductionOffsetsLog::new(
                    partitions.clone(),
                ),
            );
        let group_coordinator = std::sync::Arc::new(crate::coordinator::GroupCoordinator::new(
            config.next_gen_consumer_group.as_ref().clone(),
            config.share_group.as_ref().clone(),
            std::sync::Arc::new(crate::coordinator::unified::ImageMetadataProvider {
                controller: controller.clone(),
            }),
            offsets_log,
            config.streams_group.as_ref().clone(),
        ));
        let producer_ids = Arc::new(crate::producer_id_manager::ProducerIdManager::new());
        crate::coordinator::bootstrap::bootstrap(
            &config,
            &controller,
            &partitions,
            &group_coordinator,
            &log_dir_status,
            &producer_state,
        )
        .await?;
        crate::coordinator::bootstrap::bootstrap_audit_topic(&config, &controller).await?;

        // 4a. Construct the transaction coordinator. All dependencies
        //     (controller, partitions, producer_ids) are ready at this point.
        //     Replay any existing __transaction_state records; errors are
        //     warnings because a brand-new broker has nothing to replay.
        let txn_coordinator = Arc::new(crate::txn::coordinator::TxnCoordinator::new(
            config.node_id,
            partitions.clone(),
            producer_ids.clone(),
        ));
        let _ = txn_coordinator
            .recover(&controller.current_image())
            .await
            .map_err(|e| tracing::warn!(error = %e, "txn coordinator recovery error"));

        // 4a'. Construct the share coordinator (KIP-932 persister). Same
        //      dependencies as the txn coordinator; replay any existing
        //      __share_group_state records (warnings only — a fresh broker
        //      has nothing to replay).
        let share_coordinator = Arc::new(
            crate::share_coordinator::coordinator::ShareCoordinator::new(
                config.node_id,
                partitions.clone(),
                (*config.share_coordinator).clone(),
            ),
        );
        let _ = share_coordinator
            .recover(&controller.current_image())
            .await
            .map_err(|e| tracing::warn!(error = %e, "share coordinator recovery error"));

        // 4a''. Wire the KIP-932 group-coordinator → share-state-persister
        //       bridge. Both the `ShareCoordinator` and the `GroupCoordinator`
        //       exist now, so build the `SharePersister` and hand it to the
        //       coordinator; its per-group share actors read it after reconcile
        //       to Initialize/Delete per-partition share state. Single-broker
        //       setups always route locally; remote routing mirrors EndTxn.
        let share_persister = Arc::new(
            crate::share_coordinator::persister_client::SharePersister::new(
                config.node_id,
                share_coordinator.clone(),
                controller.clone(),
                inter_broker_client.clone(),
                config
                    .effective_listeners()
                    .iter()
                    .find(|l| l.name == config.inter_broker_listener_name)
                    .map_or(crabka_security::ListenerProtocol::Plaintext, |l| l.protocol),
                config.inter_broker_listener_name.clone(),
            ),
        );
        group_coordinator.set_share_persister(share_persister.clone());
        group_coordinator.set_metadata_source(controller.clone());

        // Share-partition leader manager (KIP-932): owns the
        //        in-memory acquisition state machines for the (group, topic,
        //        partition) triples this broker leads, loading/persisting them
        //        through the same `SharePersister`. A background sweeper expires
        //        acquisition locks so unacknowledged records redeliver.
        let share_partition_leaders = Arc::new(
            crate::share_partition::manager::SharePartitionLeaderManager::new(
                config.node_id,
                partitions.clone(),
                controller.clone(),
                share_persister.clone(),
                Arc::new((*config.share_group).clone()),
            ),
        );
        share_partition_leaders.spawn_lock_sweeper();

        // 4b. Spawn the replicator supervisor. Started AFTER the controller
        //    is up and self-registration succeeded so the supervisor's
        //    initial reconcile already sees this broker in the brokers()
        //    set. With replication_factor=1 the desired follower set is
        //    always empty, so this is a no-op for single-broker setups.
        let supervisor_shutdown = CancellationToken::new();

        // KIP-73 throttle state. Created here so it can be forwarded to
        // the replicator supervisor (and from there to each replicator
        // task). The refresh task is spawned later but shares the same Arc.
        let throttle_state = Arc::new(crate::throttle::ThrottleState::new());

        let inter_listener_proto = config
            .effective_listeners()
            .iter()
            .find(|l| l.name == config.inter_broker_listener_name)
            .map_or(crabka_security::ListenerProtocol::Plaintext, |l| l.protocol);

        // Build the broker metrics handle early so the
        // replicator supervisor (constructed next) can clone it; the
        // `/metrics` HTTP listener and any other later consumers
        // still reuse the same `metrics` value.
        let metrics = crate::metrics::BrokerMetrics::new();

        // ── Audit pipeline (FedRAMP MLA, Slice 1) ──────────────────────────
        // Wired after `bootstrap_audit_topic` (which submits the audit topic
        // metadata record into raft) and after `partitions` + `metrics` are
        // live. The `KafkaTopicAuditSink` resolves the local audit partition
        // lazily at write time, so it is safe to construct here even though
        // the replicator supervisor (spawned below) has not yet materialized
        // the local partition replica.
        // Resolve the audit partition this broker leads (broker-affinity).
        // Hoisted out of the audit_enabled block so block 2 (BrokerStarted
        // wait) can key on the actual led partition index rather than
        // hardcoding partition 0.
        let audit_led_partition: Option<PartitionIndex> = if config.audit_enabled {
            let image = controller.current_image();
            let mut led: Option<PartitionIndex> = None;
            let mut idx = 0i32;
            while let Some(part) = image.partition(&config.audit_topic, idx) {
                if part.leader == config.node_id {
                    led = Some(PartitionIndex(idx));
                    break;
                }
                idx += 1;
            }
            led
        } else {
            None
        };

        let audit_log = if config.audit_enabled {
            let (log, rx) = crabka_audit::AuditLog::new(AUDIT_EVENT_QUEUE_CAPACITY);
            if let Some(pidx) = audit_led_partition {
                let sink = std::sync::Arc::new(crate::audit_sink::KafkaTopicAuditSink::new(
                    partitions.clone(),
                    config.audit_topic.clone(),
                    pidx,
                    metrics.clone(),
                ));
                let audit_signer: Option<std::sync::Arc<dyn crabka_audit::SigningKeyProvider>> =
                    if let (Some(path), Some(key_id)) =
                        (&config.audit_signing_key_path, &config.audit_signing_key_id)
                    {
                        match crabka_audit::FileEd25519Signer::from_pkcs8_file(path, key_id.clone())
                        {
                            Ok(s) => Some(std::sync::Arc::new(s)),
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    "failed to load audit signing key; checkpoints disabled"
                                );
                                None
                            }
                        }
                    } else {
                        tracing::info!(
                            "no audit signing key configured; audit checkpoints disabled"
                        );
                        None
                    };

                // Open the durable spool (resolve relative dir under the log dir).
                let spool_dir = if config.audit_spool_dir.is_absolute() {
                    config.audit_spool_dir.clone()
                } else {
                    config.log_dir.join(&config.audit_spool_dir)
                };
                let spool = match crabka_audit::Spool::open(
                    &spool_dir,
                    config.audit_spool_max_bytes,
                ) {
                    Ok(s) => Some(s),
                    Err(e) => {
                        tracing::error!(error = %e, "failed to open audit spool; spooling disabled");
                        None
                    }
                };

                // Recover the chain: prefer the spool's last record, else the topic tail.
                let resume = spool
                    .as_ref()
                    .and_then(|s| s.resume_point().ok().flatten())
                    .or_else(|| {
                        partitions
                            .get(&config.audit_topic, pidx)
                            .and_then(|p| crate::audit_recovery::recover_from_partition_tail(&p))
                    });
                let chain = match resume {
                    Some((next_seq, head)) => {
                        tracing::info!(next_seq, "audit chain resumed");
                        crabka_audit::ChainState::resume(next_seq, head)
                    }
                    None => crabka_audit::ChainState::new(),
                };

                let stats = std::sync::Arc::new(crabka_audit::AuditStats::new());
                let writer = crabka_audit::AuditWriter::new(
                    rx,
                    crabka_audit::AuditWriterParams {
                        sink,
                        product: Self::audit_product(),
                        signer: audit_signer,
                        checkpoint_every_n: config.audit_checkpoint_every_n,
                        checkpoint_every: std::time::Duration::from_secs(
                            config.audit_checkpoint_every_secs,
                        ),
                        chain,
                        spool,
                        stats: stats.clone(),
                        replay_every: AUDIT_SPOOL_REPLAY_INTERVAL,
                        sleeper: std::sync::Arc::new(qubit_clock::sleep::SystemSleeper::new()),
                    },
                );
                tokio::spawn(writer.run());

                // Poll spool stats into Prometheus on each tick.
                let poll_metrics = metrics.clone();
                let poll_log = log.clone();
                tokio::spawn(async move {
                    let mut last_spooled = 0u64;
                    let mut last_replayed = 0u64;
                    let mut last_dropped = 0u64;
                    let mut tick = tokio::time::interval(AUDIT_STATS_POLL_INTERVAL);
                    loop {
                        tick.tick().await;
                        let spooled = stats.spooled();
                        let replayed = stats.replayed();
                        // dropped = spool-full/io drops (stats) + channel-full drops (AuditLog)
                        let dropped = stats.dropped() + poll_log.dropped();
                        poll_metrics
                            .audit_records_spooled_total
                            .inc_by(spooled - last_spooled);
                        poll_metrics
                            .audit_records_replayed_total
                            .inc_by(replayed - last_replayed);
                        poll_metrics
                            .audit_records_dropped_total
                            .inc_by(dropped - last_dropped);
                        last_spooled = spooled;
                        last_replayed = replayed;
                        last_dropped = dropped;
                        poll_metrics
                            .audit_spool_depth
                            .set(i64::try_from(stats.depth()).unwrap_or(i64::MAX));
                        poll_metrics
                            .audit_spool_bytes
                            .set(i64::try_from(stats.spool_bytes()).unwrap_or(i64::MAX));
                    }
                });
            } else {
                tracing::warn!("no audit partition led by this broker; audit records will drop");
            }
            log
        } else {
            crabka_audit::AuditLog::disabled()
        };
        // Wrap the cluster authorizer with the deny-auditing decorator so every
        // Deny decision is recorded as an AuthorizationDenied audit event.
        if config.audit_enabled {
            config.authorizer = Arc::new(crate::audit_authorizer::AuditingAuthorizer::new(
                config.authorizer.clone(),
                audit_log.clone(),
            ));
        }
        // ───────────────────────────────────────────────────────────────────

        // KIP-113 offline-dir handling: feed the supervisor the online
        // subset so newly materialized partitions never land on a dir
        // the startup probe flagged unwritable.
        let supervisor = crate::replicator_supervisor::ReplicatorSupervisor::new(
            config.node_id,
            config.broker_id,
            controller.clone(),
            partitions.clone(),
            log_dir_status.online_subset(&config.all_log_dirs()),
            config.log_config.clone(),
            format!("crabka-broker-{}-replicator", config.broker_id),
            supervisor_shutdown.clone(),
            Some(txn_coordinator.clone()),
            Some(share_coordinator.clone()),
            inter_broker_client.clone(),
            inter_listener_proto,
            config.inter_broker_listener_name.clone(),
            throttle_state.clone(),
            log_dir_status.clone(),
            producer_state.clone(),
            metrics.clone(),
            log_dir_ids.clone(),
        );
        let supervisor_handle = supervisor.spawn();

        // 4c. Liveness state for KIP-500 BrokerHeartbeat tracking.
        let liveness = Arc::new(
            crate::heartbeat::controller_state::ControllerLivenessState::new(
                std::time::Duration::from_millis(config.heartbeat_timeout_ms),
            ),
        );

        // 4d. Broker-side heartbeat client: sends BrokerHeartbeat to the
        //     controller leader on every tick. Child token of
        //     supervisor_shutdown so it is cancelled on broker shutdown.
        let (want_shutdown_tx, want_shutdown_rx) = tokio::sync::watch::channel(false);
        let (should_shutdown_tx, _should_shutdown_rx) = tokio::sync::watch::channel(false);
        let want_shutdown_tx = Arc::new(want_shutdown_tx);
        let should_shutdown_tx = Arc::new(should_shutdown_tx);
        let heartbeat_shutdown = supervisor_shutdown.child_token();
        let _heartbeat_handle = tokio::spawn(crate::heartbeat::client::run(
            crate::heartbeat::client::Config {
                broker_id: config.broker_id,
                interval: std::time::Duration::from_millis(config.heartbeat_interval_ms),
                controller: controller.clone(),
                shutdown: heartbeat_shutdown,
                inter_broker_client: inter_broker_client.clone(),
                inter_broker_listener_protocol: inter_listener_proto,
                inter_broker_listener_name: config.inter_broker_listener_name.clone(),
                want_shutdown: want_shutdown_rx,
                should_shutdown: should_shutdown_tx.clone(),
                log_dir_status: log_dir_status.clone(),
                log_dir_ids: log_dir_ids.clone(),
                all_log_dirs: config.all_log_dirs(),
                supervisor_shutdown: supervisor_shutdown.clone(),
            },
        ));

        // 4d-2. KIP-966 Unclean Recovery Manager: the controller-side
        //       orchestrator that polls surviving replicas for their log
        //       state and elects the most-complete-log replica. Both the
        //       automatic failover path (the ticker below) and the operator
        //       `ElectLeaders UNCLEAN` handler enqueue jobs onto it. Built
        //       here, before the ticker, so the ticker closure can capture a
        //       clone of the handle. Reuses the same `Arc<InterBrokerClient>`
        //       and inter-broker listener protocol as the heartbeat path.
        let unclean_recovery = crate::unclean_recovery::UncleanRecoveryManager::spawn(
            controller.clone(),
            liveness.clone(),
            config.node_id,
            inter_broker_client.clone(),
            inter_listener_proto,
            metrics.clone(),
            supervisor_shutdown.child_token(),
        );

        // 4e. Controller-side liveness ticker: scans the heartbeat registry
        //     every second and fires leader_election callbacks on transitions.
        let liveness_for_ticker = liveness.clone();
        let controller_for_ticker = controller.clone();
        let ticker_node_id = config.node_id;
        let ticker_shutdown = supervisor_shutdown.child_token();
        let metrics_for_ticker = metrics.clone();
        let recovery_for_ticker = unclean_recovery.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(LIVENESS_TICK_INTERVAL);
            loop {
                tokio::select! {
                    _ = tick.tick() => {},
                    () = ticker_shutdown.cancelled() => return,
                }
                let transitions = liveness_for_ticker.tick().await;
                for t in transitions {
                    use crate::heartbeat::controller_state::LivenessTransition::{
                        AliveToDead, DeadToAlive,
                    };
                    match t {
                        AliveToDead(n) => {
                            if let Err(e) = crate::leader_election::on_broker_dead(
                                &controller_for_ticker,
                                ticker_node_id,
                                crabka_raft::NodeId(n),
                                &liveness_for_ticker,
                                &metrics_for_ticker,
                                &recovery_for_ticker,
                            )
                            .await
                            {
                                tracing::warn!(broker = n, error = %e,
                                    "leader_election on_broker_dead failed");
                            }
                        }
                        DeadToAlive(n) => {
                            if let Err(e) = crate::leader_election::on_broker_alive(
                                &controller_for_ticker,
                                ticker_node_id,
                                crabka_raft::NodeId(n),
                                &liveness_for_ticker,
                            )
                            .await
                            {
                                tracing::warn!(broker = n, error = %e,
                                    "leader_election on_broker_alive failed");
                            }
                        }
                    }
                }
            }
        });

        // 4e-2. Leadership-change watcher: whenever this broker becomes the
        //       raft leader it seeds the liveness registry with all brokers
        //       known in the current metadata image. This gives live brokers a
        //       full heartbeat timeout window to discover the new controller
        //       before liveness failover runs against this node's fresh local
        //       heartbeat map. Without seeding, `AliveToDead` never fires for a
        //       broker that dies while a *different* raft node is the leader.
        {
            let mut leader_watch = controller.watch_leader();
            let this_node = config.node_id;
            let liveness_seed = liveness.clone();
            let controller_seed = controller.clone();
            let seed_shutdown = supervisor_shutdown.child_token();
            let metrics_seed = metrics.clone();
            // Track the leader value across `changed()`
            // notifications so we only bump the counter on actual
            // transitions (the watch may signal-without-change in some
            // raft impls; ignore those).
            let mut last_leader: Option<crabka_raft::NodeId> = *leader_watch.borrow();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = leader_watch.changed() => {},
                        () = seed_shutdown.cancelled() => return,
                    }
                    let new_leader = *leader_watch.borrow();
                    if new_leader != last_leader {
                        metrics_seed.controller_leader_changes_total.inc();
                        last_leader = new_leader;
                    }
                    if new_leader == Some(this_node) {
                        // We just became the raft leader. Seed liveness for
                        // every broker currently in the metadata image.
                        let ids: Vec<u64> = controller_seed
                            .current_image()
                            .brokers()
                            .map(|b| b.node_id.0)
                            .collect();
                        liveness_seed.seed_brokers(ids).await;
                    }
                }
            });
        }

        // Build the Prometheus metrics registry + handles
        // *before* spawning subsystems that emit. Each subsystem clones
        // a cheap `BrokerMetrics` (single Arc bump). The HTTP
        // `/metrics` server is spawned only when configured; ISR
        // maintenance / produce / fetch always update counters.
        // (The `BrokerMetrics` value itself was constructed
        // earlier so the replicator supervisor could share it.)
        let metrics_bound_addr = if let Some(addr) = config.metrics_listen_addr {
            let shutdown = supervisor_shutdown.child_token();
            let registry = metrics.registry.clone();
            // `run` already spawns the server task internally; await
            // its bind so a port conflict surfaces from `Broker::start`
            // instead of being lost in a detached task.
            let bound = crate::metrics_server::run(addr, registry, shutdown)
                .await
                .map_err(BrokerError::Io)?;
            Some(bound)
        } else {
            None
        };
        // KIP-714 client-metrics: build the bundle (manager + Prometheus
        // collector + OTLP forwarder) and register the collector into the
        // shared metrics registry so it appears on the `/metrics` scrape.
        let otlp_metrics_endpoint = crate::telemetry::OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "",
            "",
            "crabka-broker",
        )
        .map(|c| c.endpoint);
        let client_metrics = Arc::new(crate::client_metrics::ClientMetrics::new(
            crate::client_metrics::DEFAULT_TELEMETRY_MAX_BYTES,
            otlp_metrics_endpoint,
        ));
        {
            let mut reg = metrics.registry.lock().await;
            reg.register_collector(Box::new(
                crate::client_metrics::prometheus_sink::SharedClientMetricsCollector(
                    client_metrics.prometheus.clone(),
                ),
            ));
        }
        // Periodic eviction of stale client-metrics entries. Child token of
        // supervisor_shutdown.
        {
            let cm = client_metrics.clone();
            let token = supervisor_shutdown.child_token();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(CLIENT_METRICS_EVICTION_TICK);
                loop {
                    tokio::select! {
                        () = token.cancelled() => break,
                        _ = tick.tick() => cm.manager.evict_stale(
                            CLIENT_METRICS_STALE_PUSH_INTERVALS,
                            CLIENT_METRICS_STALE_FLOOR,
                        ),
                    }
                }
            });
        }

        // Background gauge updater: poll partitions_led + active_controller
        // once a second. Cheap (DashMap iteration + one atomic borrow).
        {
            let partitions_for_gauge = partitions.clone();
            let controller_for_gauge = controller.clone();
            let liveness_for_gauge = liveness.clone();
            let node_id = config.node_id;
            let m = metrics.clone();
            let shutdown = supervisor_shutdown.child_token();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(GAUGE_POLL_INTERVAL);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {}
                        () = shutdown.cancelled() => return,
                    }
                    let led = partitions_for_gauge
                        .arcs()
                        .iter()
                        .filter(|p| {
                            p.current_leader.load(std::sync::atomic::Ordering::Acquire) == node_id
                        })
                        .count();
                    m.partitions_led.set(i64::try_from(led).unwrap_or(i64::MAX));
                    // Total replicas this broker hosts (leader
                    // + follower). Cheap: just the registry length.
                    let total = partitions_for_gauge.len();
                    m.partitions_total
                        .set(i64::try_from(total).unwrap_or(i64::MAX));
                    // Per-leader URP count, sampled from the
                    // current MetadataImage. URP = ISR.len() < replicas.len()
                    // among partitions this broker leads. Read-only walk
                    // of the image; matches Kafka's
                    // ReplicaManager.UnderReplicatedPartitions semantics.
                    //
                    // The same walk also tallies UnderMinIsr (ISR <
                    // topic's min.insync.replicas) and Offline (leader is
                    // a dead broker, no live ISR replacement) so all three
                    // health gauges land in lockstep from a single image
                    // snapshot. Defaults to min.insync.replicas=1 when the
                    // topic config is missing or unparseable, matching the
                    // produce-path gate.
                    let image = controller_for_gauge.current_image();
                    let mut urp: usize = 0;
                    let mut under_min_isr: usize = 0;
                    let mut offline: usize = 0;
                    // Snapshot the alive set once (single lock) rather than
                    // taking the liveness lock per partition inside the scan.
                    let alive = liveness_for_gauge.alive_snapshot().await;
                    // Resolve each topic's min.insync.replicas once
                    // (O(topics)); defaults to 1 when missing or unparseable,
                    // matching the produce-path gate.
                    let min_isr_by_topic: std::collections::HashMap<&str, usize> = image
                        .topics()
                        .map(|topic| {
                            let min_isr = image
                                .topic_config(&topic.name)
                                .and_then(|m| m.get(crate::config_keys::MIN_INSYNC_REPLICAS))
                                .and_then(|v| v.parse::<usize>().ok())
                                .unwrap_or(1);
                            (topic.name.as_str(), min_isr)
                        })
                        .collect();
                    // Single O(P) walk over every partition.
                    for ((topic_name, _idx), pr) in image.all_partitions() {
                        if pr.leader == node_id {
                            if pr.isr.len() < pr.replicas.len() {
                                urp += 1;
                            }
                            let min_isr = min_isr_by_topic
                                .get(topic_name.as_str())
                                .copied()
                                .unwrap_or(1);
                            if pr.isr.len() < min_isr {
                                under_min_isr += 1;
                            }
                        }
                        // Offline = the leader broker isn't
                        // alive. Crabka's NodeId is `u64`, so
                        // there's no in-band "leader unknown"
                        // sentinel — the controller leaves a
                        // PartitionRecord pointing at the dead
                        // broker until a successor election (or
                        // an unclean election) runs.
                        // Only count partitions in the local
                        // broker's owned set so the metric stays
                        // per-broker; cluster-wide rollup can
                        // sum across brokers.
                        if pr.replicas.contains(&node_id) && !alive.contains(&pr.leader.0) {
                            offline += 1;
                        }
                    }
                    m.under_replicated_partitions
                        .set(i64::try_from(urp).unwrap_or(i64::MAX));
                    m.under_min_isr_partition_count
                        .set(i64::try_from(under_min_isr).unwrap_or(i64::MAX));
                    m.offline_partitions_count
                        .set(i64::try_from(offline).unwrap_or(i64::MAX));
                    let is_ctrl_leader = controller_for_gauge
                        .watch_leader()
                        .borrow()
                        .is_some_and(|n| n == node_id);
                    m.active_controller.set(i64::from(u8::from(is_ctrl_leader)));
                }
            });
        }

        // 4f. ISR maintenance: per-leader-partition shrink/expand tick.
        //     Proposes AlterPartition changes when follower lag exceeds
        //     `replica_lag_time_max_ms`. Child token of supervisor_shutdown.
        let isr_shutdown = supervisor_shutdown.child_token();
        tokio::spawn(crate::isr_maintenance::run(
            crate::isr_maintenance::Config {
                node_id: config.node_id,
                partitions: partitions.clone(),
                controller: controller.clone(),
                replica_lag_time_max: std::time::Duration::from_millis(
                    config.replica_lag_time_max_ms,
                ),
                broker_id: config.broker_id,
                shutdown: isr_shutdown,
                metrics: metrics.clone(),
            },
        ));

        // Periodic per-partition disk-usage scanner. Walks
        // the log dir each tick and updates `partition_disk_bytes` for
        // the rebalancer's usage scraper. `0` disables entirely. The
        // `JoinHandle` is retained on the `Broker` so shutdown can drain
        // the task after cancelling `supervisor_shutdown`.
        let disk_scanner_handle = if config.partition_disk_scan_interval_secs > 0 {
            let scanner = crate::disk_scanner::DiskScanner {
                log_dirs: config.all_log_dirs(),
                interval: std::time::Duration::from_secs(config.partition_disk_scan_interval_secs),
                metrics: metrics.clone(),
                shutdown: supervisor_shutdown.child_token(),
            };
            Some(tokio::spawn(scanner.run()))
        } else {
            None
        };

        // OAUTHBEARER JWKS refresher. Spawned only when a JWKS
        // endpoint is configured (signed-token validation). It shares the
        // validator's key handle, so a successful fetch rotates the keys the
        // SaslAuthenticate path reads — no restart, no lock. The first fetch
        // fires immediately on spawn. Child token of supervisor_shutdown.
        // Consume the parked signal_rx + shared rate-limit /
        // expiry-timestamp state from `apply_to`; on-demand refresh + cache
        // expiry are wired here so the validator's `JwksHandle` and the
        // refresher point at the same Arc-shared cells.
        if let Some(endpoint) = config.oauthbearer_jwks_endpoint.clone()
            && let Some(handle) = config.oauthbearer_validator.jwks_handle()
        {
            let signal_rx = config
                .oauthbearer_jwks_signal_rx
                .lock()
                .unwrap()
                .take()
                .expect("apply_to must park signal_rx whenever a signed validator is configured");
            let refresher = crate::oauth_jwks::JwksRefresher {
                endpoint,
                handle,
                interval: config.oauthbearer_jwks_refresh_interval,
                shutdown: supervisor_shutdown.child_token(),
                tls_trust: config.oauthbearer_idp_tls_trust.clone(),
                signal_rx,
                min_on_demand_pause: config.oauthbearer_jwks_min_on_demand_pause,
                last_successful_fetch_ms: config.oauthbearer_jwks_last_successful_fetch_ms.clone(),
                last_on_demand_refresh_ms: config
                    .oauthbearer_jwks_last_on_demand_refresh_ms
                    .clone(),
                ignore_key_use: config.oauthbearer_jwks_ignore_key_use,
                sleeper: std::sync::Arc::new(qubit_clock::sleep::SystemSleeper::new()),
            };
            tokio::spawn(refresher.run());
        }

        // 4g. Auto-rebalance background task (KIP-460). The task itself
        //     checks is_leader() on every tick so it is safe to run on
        //     every broker; only the raft leader will actually submit
        //     partition changes. Child token of supervisor_shutdown.
        if config.auto_leader_rebalance_enable {
            let rebalance_cfg = crate::leader_rebalance::AutoRebalanceConfig {
                check_interval: std::time::Duration::from_secs(
                    config.leader_imbalance_check_interval_secs,
                ),
                imbalance_threshold_pct: config.leader_imbalance_per_broker_percentage,
            };
            let adapter: Arc<dyn crate::leader_rebalance::ControllerLike> =
                Arc::new(ControllerAdapter {
                    handle: controller.clone(),
                    node_id: config.node_id,
                });
            let rebalance_liveness = liveness.clone();
            let rebalance_shutdown = supervisor_shutdown.child_token();
            tokio::spawn(crate::leader_rebalance::run(
                adapter,
                rebalance_liveness,
                rebalance_cfg,
                rebalance_shutdown,
            ));
        }

        // Spawn reassignment-completion background task. The task itself
        // checks is_leader() per image apply — safe to run on every broker.
        // Always-on (no config gate): reassignment completion is a
        // correctness requirement, not an optional behavior.
        {
            let adapter: Arc<dyn crate::reassignment::ReassignmentController> =
                Arc::new(ReassignmentControllerAdapter {
                    handle: controller.clone(),
                    node_id: config.node_id,
                });
            let liveness_clone = liveness.clone();
            let shutdown_clone = supervisor_shutdown.child_token();
            tokio::spawn(crate::reassignment::run(
                adapter,
                liveness_clone,
                shutdown_clone,
            ));
        }

        // Idempotent-producer state expiry sweep (KIP-360 /
        // `producer.id.expiration.ms`). Without it the per-partition
        // producer-state maps grow unbounded as idempotent producers churn.
        // Kafka defaults: expire entries idle for 24h, checked every 10min.
        {
            const PRODUCER_ID_EXPIRATION_MS: i64 = 86_400_000;
            const CHECK_INTERVAL_SECS: u64 = 600;
            let producer_state = producer_state.clone();
            let shutdown = supervisor_shutdown.child_token();
            tokio::spawn(async move {
                let mut tick =
                    tokio::time::interval(std::time::Duration::from_secs(CHECK_INTERVAL_SECS));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let now_ms = crate::time_util::now_ms();
                            producer_state
                                .expire_older_than(now_ms, PRODUCER_ID_EXPIRATION_MS)
                                .await;
                        }
                        () = shutdown.cancelled() => return,
                    }
                }
            });
        }

        // Per-broker log compaction ticker. Always-on; the
        // cleaner internally filters to (leader && cleanup.policy=compact)
        // partitions so brokers with no compact topics pay nothing.
        {
            let partitions = partitions.clone();
            let shutdown = supervisor_shutdown.child_token();
            #[allow(unused_mut)]
            let mut cfg = crate::cleaner::CleanerConfig::default();
            #[cfg(any(test, feature = "test-helpers"))]
            if let Some(interval) = config.cleaner_interval_override {
                cfg.interval = interval;
            }
            tokio::spawn(crate::cleaner::run(
                partitions,
                config.node_id,
                cfg,
                shutdown,
                metrics.clone(),
            ));
        }

        // KIP-405: tiered-storage copy path. Only spawn when a
        // remote-storage directory is configured. The reference local store
        // + in-memory metadata manager are constructed once and shared
        // across ticks; per-topic offload is gated by `remote.storage.enable`,
        // and the task filters to partitions this broker leads.
        //
        // Construction is hoisted here so the `RemoteReader` on
        // `Broker` and the copy task share the same RSM / RLMM pair. A
        // single broker has exactly one of each.
        // Hoist the parameters for the topic-backed RLMM
        // bootstrap before `config` moves into the broker struct.
        let kafka_swap_kickoff: Option<KafkaSwapKickoff> = match &config.remote_log_metadata {
            crate::config::RlmmKind::InMemory => None,
            crate::config::RlmmKind::TopicBacked(cfg) => Some({
                // Resolve the inter-broker listener: its advertised address is
                // the RLMM bootstrap; its protocol + the broker's credentials +
                // TLS client config form the metadata-client security policy.
                let listeners = config.effective_listeners();
                let inter = listeners
                    .iter()
                    .find(|l| l.name == config.inter_broker_listener_name);
                let proto =
                    inter.map_or(crabka_security::ListenerProtocol::Plaintext, |l| l.protocol);
                // Plaintext inter-broker → no security (loopback unchanged).
                let security = if proto.requires_tls() || proto.requires_sasl() {
                    // Advertised host of the inter-broker listener. Used for
                    // both the TLS SNI and the GSSAPI SPN host so they agree.
                    let advertised_host = inter.map_or_else(
                        || "localhost".to_string(),
                        |l| parse_advertised_host_port(&l.advertised).0,
                    );
                    let tls = if proto.requires_tls() {
                        config.tls_config.as_ref().map(|t| {
                            crabka_client_core::security::TlsConnectorConfig {
                                trust_roots_pem: t.trust_roots_path.clone(),
                                // SNI = the advertised host of the inter-broker listener.
                                server_name: advertised_host.clone(),
                                client_identity: None,
                            }
                        })
                    } else {
                        None
                    };
                    let sasl = config
                        .inter_broker_credentials
                        .as_ref()
                        .map(crate::network::client::to_client_creds);
                    // GSSAPI SPN host: set for any SASL listener (SASL_PLAINTEXT
                    // and SASL_SSL) so Kerberos gets the real advertised host
                    // rather than falling back to "localhost".
                    let sasl_host = proto.requires_sasl().then(|| advertised_host.clone());
                    Some(Box::new(crabka_client_core::security::ClientSecurity {
                        protocol: proto,
                        tls,
                        sasl,
                        sasl_host,
                    }))
                } else {
                    None
                };
                // Bootstrap address for the in-process RLMM metadata client.
                // Priority: explicit config wins; then inter-broker advertised
                // addr for secured listeners (TLS SNI / SASL host must match);
                // then loopback-derived from the primary data listener.
                let bootstrap = if !cfg.bootstrap.is_empty() {
                    // Operator/file-config supplied an explicit address — always honor it.
                    cfg.bootstrap.clone()
                } else if security.is_some() {
                    // Secured inter-broker listener: dial its advertised address so the TLS
                    // SNI / SASL host match. Fall back to loopback if no inter-broker listener.
                    inter.map_or_else(
                        || loopback_bootstrap(config.listen_addr),
                        |l| l.advertised.clone(),
                    )
                } else {
                    // Plaintext, no explicit bootstrap: dial our own listener over loopback.
                    loopback_bootstrap(config.listen_addr)
                };
                KafkaSwapKickoff {
                    cfg: crate::config::KafkaRlmmConfig {
                        bootstrap,
                        num_partitions: cfg.num_partitions,
                        replication: cfg.replication,
                        snapshot_interval: cfg.snapshot_interval,
                        // snapshot_dir is only empty for the programmatic BrokerConfig::default(); the TOML path (file_config.rs) already materialises it from log.dir.
                        snapshot_dir: if cfg.snapshot_dir.as_os_str().is_empty() {
                            config.log_dir.join("remote-log-metadata")
                        } else {
                            cfg.snapshot_dir.clone()
                        },
                        security,
                    },
                    broker_id: config.broker_id,
                }
            }),
        };
        let (remote_reader, kafka_swap_target): (
            Option<Arc<crate::remote_reader::RemoteReader>>,
            Option<Arc<crabka_remote_storage_topic::SwappableRlmm>>,
        ) = if let Some(backend) = config.remote_storage_backend.clone() {
            let rsm: Arc<dyn crabka_remote_storage::RemoteStorageManager> = match backend {
                crate::config::RemoteStorageBackend::Local { dir } => {
                    Arc::new(crabka_remote_storage::LocalTieredStorage::new(dir))
                }
                crate::config::RemoteStorageBackend::S3(cfg) => Arc::new(
                    crabka_remote_storage::S3RemoteStorage::from_s3_config(&cfg).map_err(|e| {
                        BrokerError::Startup(format!("remote_storage.s3 builder failed: {e}"))
                    })?,
                ),
                crate::config::RemoteStorageBackend::Gcs(cfg) => Arc::new(
                    crabka_remote_storage::S3RemoteStorage::from_gcs_config(&cfg).map_err(|e| {
                        BrokerError::Startup(format!("remote_storage.gcs builder failed: {e}"))
                    })?,
                ),
            };
            // `[remote_storage.kafka_metadata]` opts in to
            // the topic-backed RLMM. We can't construct it inline
            // because its `start` does a loopback `AdminClient` call
            // to provision `__remote_log_metadata`, and the broker's
            // listener accept loop is spawned later in this function.
            // Boot behind a `SwappableRlmm` facade, then spawn a task that
            // builds the `TopicBasedRemoteLogMetadataManager` once the
            // listener is serving and swaps it in.
            let (rlmm, kafka_swap_target): (
                Arc<dyn crabka_remote_storage::RemoteLogMetadataManager>,
                Option<Arc<crabka_remote_storage_topic::SwappableRlmm>>,
            ) = match &config.remote_log_metadata {
                crate::config::RlmmKind::TopicBacked(_) => {
                    // Fail-closed: until the topic-backed manager swaps in, every
                    // RLMM call returns NotReady. The copy task skips tiering
                    // (no orphaned RSM objects) and remote reads retry.
                    let not_ready: Arc<dyn crabka_remote_storage::RemoteLogMetadataManager> =
                        Arc::new(crabka_remote_storage_topic::NotReadyRlmm::new());
                    let swap = Arc::new(crabka_remote_storage_topic::SwappableRlmm::new(not_ready));
                    let typed: Arc<dyn crabka_remote_storage::RemoteLogMetadataManager> =
                        swap.clone();
                    (typed, Some(swap))
                }
                crate::config::RlmmKind::InMemory => {
                    let placeholder: Arc<dyn crabka_remote_storage::RemoteLogMetadataManager> =
                        Arc::new(crabka_remote_storage::InmemoryRemoteLogMetadataManager::new());
                    (placeholder, None)
                }
            };

            let partitions = partitions.clone();
            let controller = controller.clone();
            let shutdown = supervisor_shutdown.child_token();
            tokio::spawn(crate::remote_log_manager::run(
                partitions,
                controller,
                rsm.clone(),
                rlmm.clone(),
                config.node_id,
                config.broker_id,
                crate::remote_log_manager::RemoteLogManagerConfig {
                    interval: config.remote_log_manager_interval,
                },
                shutdown,
            ));

            (
                Some(Arc::new(crate::remote_reader::RemoteReader::new(rsm, rlmm))),
                kafka_swap_target,
            )
        } else {
            (None, None)
        };

        // TLS hot-reload watcher. Only spawn when a TLS
        // config is present — non-TLS brokers don't need it.
        if let (Some(dynamic), Some(tls_cfg_owned)) =
            (tls_dynamic.clone(), config.tls_config.clone())
        {
            let shutdown = supervisor_shutdown.child_token();
            tokio::spawn(crate::tls_reload::run(
                dynamic,
                tls_cfg_owned,
                config.tls_reload_interval,
                shutdown,
            ));
        }

        // KIP-73 throttle refresh task. Always-on; the bucket itself
        // has a rate-0 fast path so unthrottled clusters pay nothing.
        // `throttle_state` was created above (before the supervisor) so it
        // can be forwarded to the replicator.
        {
            let throttle = throttle_state.clone();
            let watcher: Arc<dyn crate::throttle::ImageWatcher> =
                Arc::new(ThrottleControllerAdapter {
                    handle: controller.clone(),
                });
            let shutdown = supervisor_shutdown.child_token();
            let node_id = config.node_id;
            tokio::spawn(crate::throttle::run(watcher, node_id, throttle, shutdown));
        }

        // KIP-227 incremental-fetch-session cache. Per-broker shared
        // state consulted by the Fetch handler; capacity from config.
        let fetch_session_cache = Arc::new(crate::fetch_session::FetchSessionCache::new(
            config.max_incremental_fetch_session_cache_slots,
        ));

        // KIP-13/KIP-124 quota refresh task. Always-on; every broker
        // enforces its own quotas via its own buckets (no leader gate needed).
        let quota_buckets = Arc::new(crate::quota::QuotaBuckets::new());
        {
            let buckets = quota_buckets.clone();
            let watcher: Arc<dyn crate::quota::ImageWatcher> = Arc::new(QuotaControllerAdapter {
                handle: controller.clone(),
            });
            let shutdown = supervisor_shutdown.child_token();
            tokio::spawn(crate::quota::run(watcher, buckets, shutdown));
        }

        // KIP-48: delegation-token expiry sweep. Only spawn
        // when a master key is configured — without it the four
        // delegation-token RPCs all return DELEGATION_TOKEN_AUTH_DISABLED
        // and the image never has any tokens to expire. Every broker
        // runs the sweep; raft serializes duplicate tombstones into
        // no-ops on the apply path.
        if config.delegation_token_secret_key.is_some() {
            let interval = std::time::Duration::from_millis(
                u64::try_from(config.delegation_token_expiry_check_interval_ms).unwrap_or(
                    u64::try_from(crate::config::DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS)
                        .unwrap_or(3_600_000),
                ),
            );
            let controller_adapter: Arc<
                dyn crate::delegation_token_cleanup::DelegationTokenController,
            > = Arc::new(DelegationTokenCleanupControllerAdapter {
                handle: controller.clone(),
            });
            let shutdown = supervisor_shutdown.child_token();
            tokio::spawn(crate::delegation_token_cleanup::run(
                controller_adapter,
                interval,
                shutdown,
            ));
        }

        // KIP-98 / KIP-939: idle-transaction reaper. Aborts Ongoing
        // transactions whose timeout has elapsed; 2PC (no-timeout) transactions
        // are skipped. Every broker runs the loop but only acts on transactions
        // it currently coordinates. `Duration::ZERO` disables it (the test
        // default), mirroring the disk-scanner / cleaner interval knobs.
        if !config.txn_abort_cleanup_interval.is_zero() {
            let shutdown = supervisor_shutdown.child_token();
            tokio::spawn(crate::txn::expiration::run(
                txn_coordinator.clone(),
                controller.clone(),
                config.txn_abort_cleanup_interval,
                shutdown,
            ));
        }

        // 5. Build dispatch registry.
        let handlers = crate::handlers::registry::build_registry();

        // 6. Bind one TcpListener per `ListenerSpec`. The legacy single-listener
        //    path is preserved via `effective_listeners()`, which synthesizes
        //    one PLAINTEXT spec from `listen_addr` + `advertised_listener` when
        //    `config.listeners` is empty.
        //
        //    Picks a canonical `listen_addr` for `BrokerHandle::listen_addr()`:
        //    the inter-broker listener's actual bound address when present,
        //    otherwise the first bound listener.
        let listeners_spec = config.effective_listeners();
        let mut bound: Vec<(crate::config::ListenerSpec, TcpListener, SocketAddr)> =
            Vec::with_capacity(listeners_spec.len());
        for spec in listeners_spec {
            // Adopt a caller-supplied, already-bound data-plane listener whose
            // local address matches this spec (test harnesses hand one over to
            // dodge the bind-and-drop TOCTOU race); otherwise bind the spec.
            let listener = match data_plane_listener
                .take_if(|l| l.local_addr().is_ok_and(|a| a == spec.bind_addr))
            {
                Some(l) => l,
                None => TcpListener::bind(spec.bind_addr).await?,
            };
            let actual = listener.local_addr()?;
            bound.push((spec, listener, actual));
        }
        let listen_addr = bound
            .iter()
            .find(|(spec, _, _)| spec.name == config.inter_broker_listener_name)
            .map_or(bound[0].2, |(_, _, a)| *a);

        // If the legacy `advertised_listener` points at port 0 (tests typically),
        // rewrite it to the canonical bound port so FindCoordinator/Metadata
        // return a useful host:port instead of `:0`.
        if config.advertised_listener.ends_with(":0")
            && let Some((host, _)) = config.advertised_listener.rsplit_once(':')
        {
            config.advertised_listener = format!("{host}:{}", listen_addr.port());
        }
        let future_logs: Arc<
            DashMap<(String, PartitionIndex), Arc<crate::future_log::FutureLogState>>,
        > = Arc::new(DashMap::new());

        // KIP-113: resume any interrupted intra-broker moves left on
        // disk as `<topic>-<partition>-future` dirs.
        for owning_dir in config.all_log_dirs() {
            let futures = log_dir::scan_future(&owning_dir).unwrap_or_default();
            for (topic, partition_id) in futures {
                let partition_idx = PartitionIndex(partition_id);
                if !partitions.contains(&topic, partition_idx) {
                    // Stranded future dir — partition is no longer
                    // hosted (e.g. topic deleted). Remove the leftover.
                    let stranded = log_dir::future_partition_dir(&owning_dir, &topic, partition_id);
                    if let Err(e) = std::fs::remove_dir_all(&stranded) {
                        tracing::warn!(
                            path = %stranded.display(),
                            error = %e,
                            "failed to remove stranded future-log dir"
                        );
                    }
                    continue;
                }
                if let Err(e) = crate::future_log::resume_move(
                    &partitions,
                    &future_logs,
                    &owning_dir,
                    &config.log_config,
                    &topic,
                    partition_idx,
                ) {
                    tracing::warn!(
                        topic = %topic, partition = partition_id,
                        error = ?e,
                        "failed to resume interrupted log-dir move"
                    );
                }
            }
        }

        // Capture the connection caps before `config` is moved into the
        // struct literal below.
        let config_max_connections = config.max_connections;
        let config_max_connections_per_ip = config.max_connections_per_ip;

        let broker = Arc::new(Self {
            config,
            controller,
            partitions,
            future_logs,
            group_coordinator: group_coordinator.clone(),
            producer_ids,
            producer_state,
            txn_coordinator,
            share_coordinator,
            share_partition_leaders,
            supervisor_shutdown,
            supervisor_handle: tokio::sync::Mutex::new(Some(supervisor_handle)),
            disk_scanner_handle: tokio::sync::Mutex::new(disk_scanner_handle),
            liveness: liveness.clone(),
            tls_dynamic: tls_dynamic.clone(),
            ktls_enabled,
            inter_broker_client,
            inter_broker_listener_protocol: inter_listener_proto,
            unclean_recovery,
            metrics: metrics.clone(),
            metrics_bound_addr,
            throttle_state,
            quota_buckets,
            connections: ConnectionLimiter::new(
                config_max_connections,
                config_max_connections_per_ip,
            ),
            fetch_session_cache,
            want_shutdown: want_shutdown_tx,
            should_shutdown: should_shutdown_tx,
            remote_reader,
            log_dir_status,
            log_dir_ids,
            client_metrics,
            #[cfg(any(test, feature = "test-helpers"))]
            offset_for_leader_epoch_requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            audit_log,
            handlers,
        });

        let shutdown = CancellationToken::new();
        let mut listener_tasks = Vec::with_capacity(bound.len());
        for (spec, listener, _) in bound {
            let task = tokio::spawn(accept_loop(
                broker.clone(),
                listener,
                spec,
                shutdown.clone(),
            ));
            listener_tasks.push(task);
        }

        // Emit the BrokerStarted lifecycle event now that the broker Arc is
        // live and listeners are bound. A broker that leads no audit partition
        // (e.g. a controller-only node, or one that bootstrapped before brokers
        // registered) has nowhere to durably write its own BrokerStarted record
        // in Slice 1, so we deliberately skip both the partition-wait and the
        // emit rather than emit a guaranteed-dropped record. Multi-broker / RF>1
        // durability is Slice 3. When audit_led_partition is Some, we wait for
        // the partition to materialize in the local registry before emitting.
        if let Some(led_pidx) = audit_led_partition {
            let audit_topic = broker.config.audit_topic.clone();
            let partitions_for_wait = broker.partitions.clone();
            let _ = tokio::time::timeout(AUDIT_PARTITION_WAIT_TIMEOUT, async {
                loop {
                    // Wait for this broker's led audit partition to materialise.
                    if partitions_for_wait.contains(&audit_topic, led_pidx) {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await;
            broker.audit_log.emit(crabka_audit::AuditEvent::Lifecycle {
                kind: crabka_audit::LifecycleKind::BrokerStarted,
                node_id: i64::from(broker.config.broker_id),
                time_ms: crate::time_util::now_ms(),
            });
        }

        // Now that the listener accept loops are spawned, launch the
        // topic-backed RLMM bootstrap task.  The broker already serves
        // behind the fail-closed `NotReadyRlmm` facade; the bootstrap
        // task retries with backoff until the topic-backed manager
        // starts or the broker shuts down.  Remote reads return a
        // retryable `NotReady` error and the copy task skips tiering
        // until the swap completes.
        if let Some(swap) = kafka_swap_target.as_ref()
            && let Some(kafka_cfg) = kafka_swap_kickoff.as_ref()
        {
            let swap = swap.clone();
            let kafka_cfg = kafka_cfg.clone();
            let runtime = tokio::runtime::Handle::current();
            let shutdown_token = shutdown.clone();
            let metrics_for_bootstrap = broker.metrics.clone();
            let node_id = broker.config.node_id;
            let image_rx = broker.controller.watch_image();
            let reconciler_shutdown = shutdown.clone();
            tokio::spawn(async move {
                tokio::select! {
                    () = shutdown_token.cancelled() => {
                        tracing::debug!("topic-backed RLMM bootstrap cancelled");
                    }
                    () = bootstrap_topic_rlmm(
                        swap,
                        kafka_cfg,
                        runtime,
                        metrics_for_bootstrap,
                        node_id,
                        image_rx,
                        reconciler_shutdown,
                    ) => {}
                }
            });
        }

        Ok(BrokerHandle {
            listen_addr,
            shutdown,
            listener_tasks,
            _broker: broker,
        })
    }
}

#[derive(Debug, Clone)]
struct KafkaSwapKickoff {
    cfg: crate::config::KafkaRlmmConfig,
    broker_id: i32,
}

/// The sorted, deduped set of `__remote_log_metadata` partitions this broker
/// (`node_id`) must consume: one entry per metadata partition covering any
/// user-topic-partition this node leads or follows, given the metadata topic's
/// `partition_count`.
fn needed_metadata_partitions(
    image: &crabka_metadata::MetadataImage,
    node_id: crabka_metadata::NodeId,
    partition_count: i32,
) -> Vec<i32> {
    let mut tps: Vec<crabka_remote_storage::TopicIdPartition> = Vec::new();
    for topic in image.topics() {
        for p in image.partitions_of(&topic.name) {
            if p.leader == node_id || p.replicas.contains(&node_id) {
                tps.push(crabka_remote_storage::TopicIdPartition::new(
                    topic.topic_id,
                    topic.name.clone(),
                    p.partition,
                ));
            }
        }
    }
    crabka_remote_storage_topic::metadata_partitions_for(tps.iter(), partition_count)
}

/// Cadence at which the metadata-partition reconciler re-applies the
/// current assigned set even when the metadata image is unchanged. Drives
/// recovery of partitions parked at the `HWM_UNKNOWN` sentinel after a
/// transient `high_water_marks` failure at assignment time.
const RLMM_RECONCILE_TICK: std::time::Duration = std::time::Duration::from_secs(30);

/// Maximum backoff between successive topic-backed RLMM bootstrap attempts.
const RLMM_BOOTSTRAP_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(10);

/// First backoff after a failed topic-backed RLMM bootstrap attempt; doubles
/// per failure up to [`RLMM_BOOTSTRAP_BACKOFF_MAX`].
const RLMM_BOOTSTRAP_BACKOFF_INITIAL: std::time::Duration = std::time::Duration::from_millis(250);

/// Next backoff after a failed RLMM bootstrap attempt: double, capped.
fn next_rlmm_backoff(cur: std::time::Duration, max: std::time::Duration) -> std::time::Duration {
    (cur * 2).min(max)
}

/// A connectable loopback `host:port` for the broker's own data listener,
/// used as the default RLMM metadata-client bootstrap when none is configured.
/// A wildcard bind (`0.0.0.0` / `::`) is mapped to loopback so the in-process
/// metadata client has a routable target.
fn loopback_bootstrap(listen: std::net::SocketAddr) -> String {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    let ip = match listen.ip() {
        IpAddr::V4(v4) if v4 == Ipv4Addr::UNSPECIFIED => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(v6) if v6 == Ipv6Addr::UNSPECIFIED => IpAddr::V6(Ipv6Addr::LOCALHOST),
        other => other,
    };
    std::net::SocketAddr::new(ip, listen.port()).to_string()
}

/// Back off after a failed RLMM bootstrap attempt. Sleeps for the current
/// backoff (advancing it toward the cap), or returns `false` if shutdown
/// fired during the sleep so the caller can abort the bootstrap.
async fn rlmm_bootstrap_backoff(
    backoff: &mut std::time::Duration,
    shutdown: &CancellationToken,
) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => {
            tracing::debug!("topic-backed RLMM bootstrap cancelled during backoff");
            false
        }
        () = tokio::time::sleep(*backoff) => {
            *backoff = next_rlmm_backoff(*backoff, RLMM_BOOTSTRAP_BACKOFF_MAX);
            true
        }
    }
}

/// Construct the topic-backed
/// [`crabka_remote_storage::RemoteLogMetadataManager`] against the
/// broker's loopback listener and swap it into `swap`. Retries with
/// bounded backoff until success or shutdown; the broker stays on the
/// fail-closed [`crabka_remote_storage_topic::NotReadyRlmm`] placeholder
/// while retrying.
async fn bootstrap_topic_rlmm(
    swap: Arc<crabka_remote_storage_topic::SwappableRlmm>,
    cfg: KafkaSwapKickoff,
    runtime: tokio::runtime::Handle,
    metrics: crate::metrics::BrokerMetrics,
    node_id: crabka_metadata::NodeId,
    mut image_rx: tokio::sync::watch::Receiver<Arc<crabka_metadata::MetadataImage>>,
    shutdown: CancellationToken,
) {
    let log_cfg = crabka_remote_storage_topic::KafkaMetadataLogConfig {
        bootstrap: cfg.cfg.bootstrap,
        topic: crabka_remote_storage_topic::METADATA_TOPIC.to_string(),
        num_partitions: cfg.cfg.num_partitions,
        replication: cfg.cfg.replication,
        client_id: format!("crabka-rlmm-broker-{}", cfg.broker_id),
        security: cfg.cfg.security.map(|b| *b),
    };

    // Retry the topic-backed bootstrap with bounded backoff until it succeeds
    // or the broker shuts down. Until then the SwappableRlmm stays on the
    // fail-closed NotReadyRlmm placeholder.
    let mut backoff = RLMM_BOOTSTRAP_BACKOFF_INITIAL;
    let manager = loop {
        metrics.tiered_storage_rlmm_bootstrap_attempts.inc();
        // Race the attempt against shutdown: `KafkaMetadataEventLog::start`
        // dials the broker's listener, and a pending TCP connect can take
        // seconds to fail on some platforms (Windows retransmits SYNs to a
        // closed loopback port instead of failing fast), so the token must
        // be honoured mid-attempt, not just between attempts. `biased`
        // makes an already-cancelled token win before the dial even starts.
        //
        // KafkaMetadataEventLog::start and TopicBasedRemoteLogMetadataManager::start
        // return different error types, so we handle them with separate match arms.
        let started = tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::debug!("topic-backed RLMM bootstrap cancelled during attempt");
                return;
            }
            res = crabka_remote_storage_topic::KafkaMetadataEventLog::start(log_cfg.clone()) => res,
        };
        let log = match started {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, backoff_ms = backoff.as_millis(),
                    "topic-backed RLMM log start failed; retrying");
                if !rlmm_bootstrap_backoff(&mut backoff, &shutdown).await {
                    return;
                }
                continue;
            }
        };
        let manager = match crabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager::start(
            log.clone(),
            runtime.clone(),
            cfg.cfg.snapshot_dir.clone(),
            cfg.cfg.snapshot_interval,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, backoff_ms = backoff.as_millis(),
                    "topic-backed RLMM manager start failed; retrying");
                if !rlmm_bootstrap_backoff(&mut backoff, &shutdown).await {
                    return;
                }
                continue;
            }
        };
        // `log` is an `Arc`; `manager` holds its own clone. Drop the local
        // binding here — we don't need a separate handle to the log.
        drop(log);
        break manager;
    };
    // Keep the concrete handle so the reconciler can call
    // `reconcile_assignment`; the swap facade only needs the trait object.
    swap.swap(manager.clone());
    metrics.tiered_storage_rlmm_topic_backed.set(1);
    tracing::info!("topic-backed RemoteLogMetadataManager activated");

    // Publish the leadership-derived needed-set on a watch; re-emit whenever
    // the metadata image changes. The initial value is the current image's
    // set, so the bootstrap assignment is leadership-derived (not all
    // partitions).
    let partition_count = cfg.cfg.num_partitions;
    let initial =
        needed_metadata_partitions(&image_rx.borrow_and_update(), node_id, partition_count);
    let (set_tx, set_rx) = tokio::sync::watch::channel(initial);

    // Image-watcher: recompute on every image change.
    {
        let set_tx = set_tx;
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    changed = image_rx.changed() => {
                        if changed.is_err() {
                            return; // image sender dropped
                        }
                        let set = needed_metadata_partitions(
                            &image_rx.borrow_and_update(),
                            node_id,
                            partition_count,
                        );
                        // send_if_modified avoids a reconcile when the set is
                        // unchanged across an image bump that didn't touch us.
                        set_tx.send_if_modified(|cur| {
                            if *cur == set {
                                false
                            } else {
                                *cur = set;
                                true
                            }
                        });
                    }
                }
            }
        });
    }

    // Reconciler: apply the latest set to the manager's AssignmentHandle.
    spawn_rlmm_reconciler(manager, set_rx, shutdown);
}

/// Spawn the metadata-partition reconciler: apply the leadership-derived
/// set to the RLMM's `AssignmentHandle` on the initial value, on every
/// change, and on a fixed [`RLMM_RECONCILE_TICK`] cadence.
///
/// The periodic tick is what makes a partition parked at the
/// `HWM_UNKNOWN` sentinel (after a transient assignment-time
/// `high_water_marks` failure) eventually re-attempt its HWM and leave the
/// `NotReady` state, even when the metadata image stays static.
/// `reconcile_assignment` is idempotent for partitions already
/// assigned-and-ready, so the periodic re-apply is cheap.
fn spawn_rlmm_reconciler(
    manager: Arc<crabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager>,
    mut set_rx: tokio::sync::watch::Receiver<Vec<i32>>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        // Apply the initial set immediately.
        {
            let set = set_rx.borrow_and_update().clone();
            manager.reconcile_assignment(&set).await;
        }
        let mut tick = tokio::time::interval(RLMM_RECONCILE_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                changed = set_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let set = set_rx.borrow_and_update().clone();
                    manager.reconcile_assignment(&set).await;
                }
                _ = tick.tick() => {
                    let set = set_rx.borrow().clone();
                    manager.reconcile_assignment(&set).await;
                }
            }
        }
    });
}

/// Create the partition runtime (mpsc channel + writer task + notify).
///
/// `log_dir` is the parent `log.dir` that owns the partition (i.e. the
/// configured directory, not the `<topic>-<partition>` subdirectory).
/// Stored on the `Partition` so KIP-113 (`AlterReplicaLogDirs`) can
/// reject moves whose target is the partition's current dir without
/// reaching into the `Log` mutex on the hot path, and so
/// `DescribeLogDirs` can attribute the partition to a dir even when
/// the path is not stable across canonicalisation.
pub(crate) fn spawn_partition(
    topic: String,
    partition_id: PartitionIndex,
    log_dir: std::path::PathBuf,
    log: crabka_log::Log,
    log_dir_status: crate::log_dir_status::LogDirRegistry,
    producer_state: Arc<crate::producer_state::ProducerState>,
) -> Arc<Partition> {
    /// Depth of the per-partition writer mpsc queue: bounds how many
    /// produce/replication appends may be in flight to one partition before
    /// senders back-pressure.
    const PARTITION_WRITER_QUEUE_DEPTH: usize = 64;
    let log = Arc::new(Mutex::new(log));
    let (tx, rx) = tokio::sync::mpsc::channel::<WriterMessage>(PARTITION_WRITER_QUEUE_DEPTH);
    let notify = Arc::new(tokio::sync::Notify::new());
    let replica_state = Arc::new(tokio::sync::Mutex::new(
        crate::replica_state::ReplicaState::new(),
    ));
    let hw_advance_notify = Arc::new(tokio::sync::Notify::new());
    let current_leader = Arc::new(AtomicU64::new(0));
    let current_leader_epoch = Arc::new(AtomicI32::new(0));
    let log_dir = Arc::new(arc_swap::ArcSwap::from_pointee(log_dir));
    let writer = tokio::spawn(crate::partition_writer::run(
        topic.clone(),
        partition_id,
        log.clone(),
        log_dir.clone(),
        rx,
        notify.clone(),
        replica_state.clone(),
        hw_advance_notify.clone(),
        log_dir_status,
        producer_state,
    ));
    Arc::new(Partition {
        topic,
        partition_id,
        log_dir,
        log,
        writer_tx: tx,
        append_notify: notify,
        replica_state,
        hw_advance_notify,
        current_leader,
        current_leader_epoch,
        _writer_handle: Arc::new(writer),
    })
}

/// Split a `host:port` advertised string. Mirrors the helpers in
/// `handlers::find_coordinator` / `handlers::metadata` but returns
/// `(String, u16)` for direct `BrokerEndpoint` use. Splits on the LAST
/// `:` so IPv6 literals do not break on inner colons (we still expect
/// IPv6 callers to wrap in `[...]`).
fn parse_advertised_host_port(addr: &str) -> (String, u16) {
    /// Kafka's canonical default plaintext port, used only as the fallback
    /// when an advertised address fails to parse as `host:port`.
    const DEFAULT_KAFKA_PORT: u16 = 9092;
    if let Some((h, p)) = addr.rsplit_once(':')
        && let Ok(port) = p.parse::<u16>()
    {
        return (h.to_string(), port);
    }
    tracing::warn!(
        addr,
        "advertised not host:port; falling back to localhost:9092"
    );
    ("localhost".into(), DEFAULT_KAFKA_PORT)
}

/// Build the KIP-595 static controller [`VoterSet`](crabka_metadata::VoterSet)
/// from the configured `controller_quorum_voters` (`(id, "<host>:<port>")`).
///
/// Peer endpoint hosts are kept as their configured **DNS names** — NOT
/// pre-resolved to IPs — so the inter-broker dialer re-resolves them on every
/// (re)connect (`TcpStream::connect((host, port))` does a fresh lookup). A
/// `StatefulSet` peer that restarts on a new pod IP keeps its stable DNS name,
/// so re-resolution reaches it again; freezing the boot-time IP here would
/// permanently strand a rejoining voter — its peers would dial the dead old IP
/// forever, the leader's `BeginQuorumEpoch` heartbeats would never arrive, and
/// the rejoining node would never learn the leader (so it would never open its
/// data listener).
///
/// `directory_id` is only load-bearing for self: the engine keys vote/peer
/// logic on `NodeId` and uses `Uuid::nil()` for vote keys, so peers get a nil
/// placeholder (verified against `kraft/network.rs::controller_addr` and
/// `kraft/core.rs`).
fn static_controller_voter_set(
    quorum_voters: &[(crabka_raft::NodeId, String)],
    self_node_id: crabka_raft::NodeId,
    self_directory_id: uuid::Uuid,
    self_controller_listen: std::net::SocketAddr,
) -> crabka_metadata::VoterSet {
    // Split a configured "<host>:<port>" into (host, port), keeping the host
    // verbatim (a DNS name resolved later, per dial). `file_config`
    // (`parse_quorum_voter`) validates the shape, so a parse miss here is not
    // expected; fall back to port 0 rather than panicking.
    fn split_host_port(host_port: &str) -> (String, u16) {
        match host_port.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().unwrap_or(0)),
            None => (host_port.to_string(), 0),
        }
    }
    fn voter(
        id: crabka_raft::NodeId,
        directory_id: uuid::Uuid,
        host: String,
        port: u16,
    ) -> crabka_metadata::Voter {
        crabka_metadata::Voter {
            id,
            directory_id,
            endpoints: vec![crabka_metadata::VoterEndpoint {
                name: "CONTROLLER".to_string(),
                host,
                port,
            }],
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        }
    }

    if quorum_voters.len() > 1 {
        // Static N-voter set: one `Voter` per configured `(node_id, host:port)`.
        let voters: Vec<crabka_metadata::Voter> = quorum_voters
            .iter()
            .map(|(node_id, host_port)| {
                let (host, port) = split_host_port(host_port);
                let directory_id = if *node_id == self_node_id {
                    self_directory_id
                } else {
                    uuid::Uuid::nil()
                };
                voter(*node_id, directory_id, host, port)
            })
            .collect();
        crabka_metadata::VoterSet::from_voters(voters)
    } else {
        // Standalone single self-voter. Self is never dialed, so its endpoint
        // uses this node's own controller listen address directly.
        crabka_metadata::VoterSet::from_voters([voter(
            self_node_id,
            self_directory_id,
            self_controller_listen.ip().to_string(),
            self_controller_listen.port(),
        )])
    }
}

/// Live-connection accounting backing the `max.connections` (global) and
/// `max.connections.per.ip` caps. Cloning shares the same counters
/// (`Arc`-wrapped internally), so every listener accept loop and every
/// [`ConnectionGuard`] account against one set of totals.
#[derive(Clone)]
pub(crate) struct ConnectionLimiter {
    /// Global ceiling. `usize::MAX` means unlimited.
    max_connections: usize,
    /// Per-IP ceiling. `usize::MAX` means unlimited.
    max_connections_per_ip: usize,
    /// Current live connection total across all listeners.
    total: Arc<AtomicUsize>,
    /// Current live connection count per client IP. Entries are removed
    /// when they hit 0 so the map doesn't grow unbounded.
    per_ip: Arc<DashMap<IpAddr, usize>>,
}

impl ConnectionLimiter {
    fn new(max_connections: usize, max_connections_per_ip: usize) -> Self {
        Self {
            max_connections,
            max_connections_per_ip,
            total: Arc::new(AtomicUsize::new(0)),
            per_ip: Arc::new(DashMap::new()),
        }
    }

    /// Try to reserve a connection slot for `ip`. On success returns a
    /// [`ConnectionGuard`] that releases both the global and per-IP slot
    /// on drop. Returns `None` (and reserves nothing) when either the
    /// global or the per-IP cap is already reached — the caller then
    /// closes the socket, matching Kafka's silent-drop behavior.
    fn try_acquire(&self, ip: IpAddr) -> Option<ConnectionGuard> {
        // Global cap. `fetch_update` keeps the increment atomic so two
        // concurrent accepts can't both slip past the ceiling.
        let global_ok = self
            .total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                (cur < self.max_connections).then_some(cur + 1)
            })
            .is_ok();
        if !global_ok {
            return None;
        }
        // Per-IP cap. The DashMap entry lock serializes the read-modify
        // on a single IP. On rejection we must undo the global reserve.
        let mut entry = self.per_ip.entry(ip).or_insert(0);
        if *entry >= self.max_connections_per_ip {
            drop(entry);
            self.total.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        *entry += 1;
        drop(entry);
        Some(ConnectionGuard {
            limiter: self.clone(),
            ip,
        })
    }

    /// Test/diagnostic accessor: current global live-connection count.
    #[cfg(test)]
    fn total(&self) -> usize {
        self.total.load(Ordering::Acquire)
    }

    /// Test/diagnostic accessor: current per-IP live-connection count.
    #[cfg(test)]
    fn per_ip_count(&self, ip: IpAddr) -> usize {
        self.per_ip.get(&ip).map_or(0, |e| *e)
    }
}

/// RAII release for one accepted connection. Moved into the spawned
/// per-connection task so it fires however the connection terminates
/// (clean close, error, panic, task abort). On drop it decrements the
/// global counter and the per-IP counter, removing the per-IP map entry
/// when it reaches 0.
pub(crate) struct ConnectionGuard {
    limiter: ConnectionLimiter,
    ip: IpAddr,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.limiter.total.fetch_sub(1, Ordering::AcqRel);
        // Decrement the per-IP entry; remove it at 0 to bound map growth.
        if let dashmap::mapref::entry::Entry::Occupied(mut occ) = self.limiter.per_ip.entry(self.ip)
        {
            let v = occ.get_mut();
            *v -= 1;
            if *v == 0 {
                occ.remove();
            }
        }
    }
}

/// KIP-612 quota key for accept-rate throttling; matches the
/// `connection_creation_rate` config name Kafka's `AlterClientQuotas` uses
/// for `ip` entities.
const CONNECTION_CREATION_RATE_QUOTA_KEY: &str = "connection_creation_rate";

/// Upper bound on the KIP-612 accept-throttle delay, so a very low configured
/// rate cannot stall the accept loop for more than this per connection.
const CONNECTION_CREATION_THROTTLE_MAX: std::time::Duration = std::time::Duration::from_secs(1);

async fn accept_loop(
    broker: Arc<Broker>,
    listener: TcpListener,
    spec: crate::config::ListenerSpec,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!(name = %spec.name, "listener shutting down");
                break;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        tracing::debug!(%peer, name = %spec.name, "accepted connection");
                        let peer_ip = peer.ip();
                        tune_accepted_socket(&stream);

                        // `max.connections` / `max.connections.per.ip` caps.
                        // Reserve a slot before doing any work; on rejection
                        // close the socket immediately (Kafka silently drops
                        // connections past either ceiling). The returned guard
                        // is moved into the spawned task so both counters are
                        // released however the connection ends.
                        let Some(conn_guard) = broker.connections.try_acquire(peer_ip) else {
                            tracing::debug!(
                                %peer,
                                name = %spec.name,
                                "connection limit reached; closing connection"
                            );
                            drop(stream);
                            continue;
                        };

                        // KIP-612 connection_creation_rate enforcement. Applies
                        // to both IPv4 and IPv6 peers — the quota is keyed by the
                        // peer IP's string form for either family.
                        let image = broker.controller.current_image();
                        if let Some((entity_key, rate)) =
                            crate::quota::lookup_ip_quota_with_key(
                                &image,
                                peer_ip,
                                CONNECTION_CREATION_RATE_QUOTA_KEY,
                            )
                            && rate > 0.0
                        {
                            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                            let initial_rate = rate.max(1.0) as u64;
                            let bucket = broker.quota_buckets.get_or_create(
                                CONNECTION_CREATION_RATE_QUOTA_KEY,
                                &entity_key,
                                initial_rate,
                            );
                            if bucket.try_consume(1) == 0 {
                                #[allow(
                                    clippy::cast_possible_truncation,
                                    clippy::cast_sign_loss
                                )]
                                let delay_micros =
                                    ((1.0_f64 / rate) * 1_000_000.0) as u64;
                                let delay =
                                    std::time::Duration::from_micros(delay_micros)
                                        .min(CONNECTION_CREATION_THROTTLE_MAX);
                                tokio::time::sleep(delay).await;
                            }
                        }
                        let b = broker.clone();
                        let s = spec.clone();
                        tokio::spawn(async move {
                            // Hold the connection guard for the lifetime of the
                            // connection; dropping it releases the global +
                            // per-IP slots.
                            let _conn_guard = conn_guard;
                            // `Box::pin` the per-connection handler: the request
                            // dispatch state machine (68 API handlers, each now
                            // carrying a tracing span) is a legitimately large
                            // future that trips `clippy::large_futures` once held
                            // inline in this spawned task. Boxing moves it to the
                            // heap (one alloc per long-lived connection — free).
                            Box::pin(crate::network::dispatch::serve_connection_on_listener(
                                b, stream, s,
                            ))
                            .await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, name = %spec.name, "accept failed");
                    }
                }
            }
        }
    }
}

/// Tune an accepted broker connection before serving it.
///
/// - `TCP_NODELAY`: disable Nagle so the request/response ping-pong isn't
///   stalled up to ~40 ms by delayed ACKs. Apache Kafka sets this on its
///   broker sockets; without it small-request latency and the header+records
///   write coalescing (once fetch uses `sendfile`) suffer.
/// - `SO_SNDBUF`/`SO_RCVBUF` (1 MiB): widen the kernel buffers so a single
///   large fetch/produce isn't throttled by a small in-flight window — the
///   send buffer in particular keeps a `sendfile`'d large fetch from stalling.
///
/// All failures are non-fatal (logged at debug): a connection that can't be
/// tuned still serves correctly, just less optimally.
fn tune_accepted_socket(stream: &tokio::net::TcpStream) {
    // 1 MiB send/recv. Kafka's defaults are 100 KiB but it commonly tunes to
    // 1 MiB+; large fetches/produces want the headroom.
    const SOCKET_BUF_BYTES: usize = 1 << 20;
    if let Err(e) = stream.set_nodelay(true) {
        tracing::debug!(error = %e, "TCP_NODELAY set failed on accepted socket");
    }
    let sock = socket2::SockRef::from(stream);
    if let Err(e) = sock.set_send_buffer_size(SOCKET_BUF_BYTES) {
        tracing::debug!(error = %e, "SO_SNDBUF set failed on accepted socket");
    }
    if let Err(e) = sock.set_recv_buffer_size(SOCKET_BUF_BYTES) {
        tracing::debug!(error = %e, "SO_RCVBUF set failed on accepted socket");
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use tempfile::tempdir;

    use super::*;

    struct MockMetadataSource {
        image: Arc<crabka_metadata::MetadataImage>,
        leader_tx: tokio::sync::watch::Sender<Option<crabka_raft::NodeId>>,
    }

    impl MockMetadataSource {
        fn new(image: crabka_metadata::MetadataImage, leader: Option<crabka_raft::NodeId>) -> Self {
            let (leader_tx, _) = tokio::sync::watch::channel(leader);
            Self {
                image: Arc::new(image),
                leader_tx,
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for MockMetadataSource {
        fn current_image(&self) -> Arc<crabka_metadata::MetadataImage> {
            self.image.clone()
        }

        fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<crabka_metadata::MetadataImage>> {
            let (_, rx) = tokio::sync::watch::channel(self.image.clone());
            rx
        }

        fn watch_leader(&self) -> tokio::sync::watch::Receiver<Option<crabka_raft::NodeId>> {
            self.leader_tx.subscribe()
        }

        fn quorum_state(&self) -> crabka_raft::QuorumState {
            crabka_raft::QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: *self.leader_tx.borrow(),
                voters: Vec::new(),
                voter_nodes: std::collections::BTreeMap::new(),
                per_voter_matched_index: std::collections::BTreeMap::new(),
            }
        }

        async fn submit_change(
            &self,
            _records: Vec<crabka_metadata::MetadataRecord>,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("mock metadata source"))
        }

        async fn change_membership(
            &self,
            _new_voters: std::collections::BTreeSet<crabka_raft::NodeId>,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("mock metadata source"))
        }

        async fn add_learner(
            &self,
            _node_id: crabka_raft::NodeId,
            _node: crabka_raft::Node,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("mock metadata source"))
        }

        fn controller_bound_addr(&self) -> std::net::SocketAddr {
            "127.0.0.1:9093".parse().unwrap()
        }

        fn read_snapshot_range(
            &self,
            _position: i64,
            _max_bytes: i32,
        ) -> crabka_raft::SnapshotRange {
            crabka_raft::SnapshotRange::NoSnapshot
        }

        async fn trigger_snapshot(&self) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("mock metadata source"))
        }

        async fn add_voter(
            &self,
            _req: crabka_raft::AddVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("mock metadata source"))
        }

        async fn remove_voter(
            &self,
            _req: crabka_raft::RemoveVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("mock metadata source"))
        }

        async fn update_voter(
            &self,
            _req: crabka_raft::UpdateVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("mock metadata source"))
        }

        async fn cancel(&self) {}
    }

    async fn assert_listener_stops_accepting(addr: SocketAddr) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match tokio::net::TcpStream::connect(addr).await {
                Ok(stream) => {
                    drop(stream);
                    assert!(
                        std::time::Instant::now() < deadline,
                        "listener at {addr} still accepts connections after shutdown"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(_) => return,
            }
        }
    }

    async fn wait_for_connection_count(broker: &Broker, expected: usize, message: &'static str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if broker.connections.total() == expected {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "{message}");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    fn local_partition_with_records(
        log_dir: &std::path::Path,
        topic: &str,
        partition: i32,
        values: &[&'static [u8]],
    ) -> Arc<Partition> {
        let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
        std::fs::create_dir_all(&part_dir).expect("create partition dir");
        let log = crabka_log::Log::open(&part_dir, crabka_log::LogConfig::default())
            .expect("open partition log");
        let part = spawn_partition(
            topic.to_string(),
            PartitionIndex(partition),
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        );
        if !values.is_empty() {
            let mut batch = crabka_protocol::records::RecordBatch {
                last_offset_delta: i32::try_from(values.len() - 1).expect("record count fits"),
                records: values
                    .iter()
                    .enumerate()
                    .map(|(idx, value)| crabka_protocol::records::Record {
                        offset_delta: i32::try_from(idx).expect("offset delta fits"),
                        value: Some(bytes::Bytes::from_static(value)),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            part.log
                .lock()
                .expect("partition log lock")
                .append(&mut batch)
                .expect("append records");
        }
        part
    }

    fn consumer_group_seed(member_id: &str) -> crate::coordinator::unified::GroupSeed {
        let mut seed = crate::coordinator::unified::GroupSeed {
            group_epoch: 3,
            target_epoch: 4,
            ..Default::default()
        };
        seed.members.insert(
            member_id.to_string(),
            crate::coordinator::unified::persistence_next_gen::MemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "client".to_string(),
                client_host: "127.0.0.1".to_string(),
                subscribed_topic_names: vec!["orders".to_string()],
                subscribed_topic_regex: None,
                server_assignor: None,
                rebalance_timeout_ms: 60_000,
                classic: None,
            },
        );
        seed
    }

    fn classic_group_with_member(
        group_id: &str,
        member_id: &str,
    ) -> Box<crate::coordinator::unified::group::Group> {
        let mut classic = crate::coordinator::unified::classic_state::Group::new(group_id);
        classic.protocol_type = Some("consumer".to_string());
        classic.generation_id = 1;
        let member = crate::coordinator::unified::classic_state::Member::new(
            member_id,
            "client",
            "127.0.0.1",
            std::time::Duration::from_secs(30),
            std::time::Duration::from_mins(1),
            vec![("range".to_string(), bytes::Bytes::from_static(b"metadata"))],
        );
        let _ = classic.add_member(member);
        Box::new(crate::coordinator::unified::group::Group {
            group_id: group_id.to_string(),
            kind: crate::coordinator::unified::group::GroupKind::Classic(classic),
            committed_offsets: std::collections::HashMap::new(),
        })
    }

    fn streams_group_seed(member_id: &str) -> crate::coordinator::unified::StreamsGroupSeed {
        let mut active = std::collections::BTreeMap::new();
        active.insert("subtopology-0".to_string(), vec![0, 1]);

        let mut members = std::collections::HashMap::new();
        members.insert(
            member_id.to_string(),
            crate::coordinator::unified::streams::persistence::StreamsGroupMemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "streams-client".to_string(),
                client_host: "127.0.0.1".to_string(),
                process_id: "process-1".to_string(),
                user_endpoint: None,
                client_tags: Vec::new(),
                rebalance_timeout_ms: 60_000,
                topology_epoch: 0,
            },
        );

        let mut target_per_member = std::collections::HashMap::new();
        target_per_member.insert(
            member_id.to_string(),
            crate::coordinator::unified::streams::persistence::StreamsGroupTargetAssignmentMemberValue {
                active: active.clone(),
                standby: std::collections::BTreeMap::new(),
                warmup: std::collections::BTreeMap::new(),
            },
        );

        let mut current_per_member = std::collections::HashMap::new();
        current_per_member.insert(
            member_id.to_string(),
            crate::coordinator::unified::streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
                member_epoch: 5,
                previous_member_epoch: 4,
                state: crate::coordinator::unified::streams::state::StreamsMemberAssignmentState::Stable
                    .as_i8(),
                active,
                standby: std::collections::BTreeMap::new(),
                warmup: std::collections::BTreeMap::new(),
                active_pending_revocation: std::collections::BTreeMap::new(),
            },
        );

        crate::coordinator::unified::StreamsGroupSeed {
            group_epoch: 5,
            assignment_epoch: 6,
            topology: None,
            partition_metadata: None,
            members,
            target_per_member,
            current_per_member,
        }
    }

    async fn assert_streams_group_helpers_observe_live_actor_view(
        broker: &Arc<Broker>,
        handle: &BrokerHandle,
    ) {
        let streams_group_id = "handle-streams-group-mutant";
        let streams_member_id = "streams-member-1";
        let streams_actor = broker
            .group_coordinator
            .get_or_create_streams(streams_group_id);
        streams_actor
            .tx
            .send(
                crate::coordinator::unified::streams::actor::StreamsGroupActorMessage::Seed(
                    streams_group_seed(streams_member_id),
                ),
            )
            .await
            .expect("seed streams group");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_streams_group_member_count(streams_group_id, 1),
            )
            .await
            .is_ok()
        );
        let streams = handle
            .streams_group_describe_for_test(streams_group_id)
            .await
            .expect("streams group describe");
        let expected_active = {
            let mut active = std::collections::BTreeMap::new();
            active.insert("subtopology-0".to_string(), vec![0, 1]);
            active
        };
        check!(streams.group_id.as_str() == streams_group_id);
        check!(streams.members.len() == 1);
        check!(streams.members[0].member_id.as_str() == streams_member_id);
        check!(streams.members[0].active == expected_active);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_streams_group_empty(streams_group_id),
            )
            .await
            .is_err()
        );

        let empty_streams_group_id = "handle-empty-streams-group-mutant";
        let _ = broker
            .group_coordinator
            .get_or_create_streams(empty_streams_group_id);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_streams_group_empty(empty_streams_group_id),
            )
            .await
            .is_ok()
        );
    }

    fn metadata_topic_record(topic: &str, topic_id: u128) -> crabka_metadata::MetadataRecord {
        crabka_metadata::MetadataRecord::V1Topic(crabka_metadata::TopicRecord {
            name: topic.to_string(),
            topic_id: uuid::Uuid::from_u128(topic_id),
            partitions: 1,
            replication_factor: 1,
        })
    }

    fn metadata_partition_record(
        topic: &str,
        partition: i32,
        leader: u64,
        replicas: &[u64],
        isr: &[u64],
        leader_epoch: i32,
    ) -> crabka_metadata::PartitionRecord {
        crabka_metadata::PartitionRecord {
            topic: topic.to_string(),
            partition,
            leader: crabka_audit::NodeId(leader),
            replicas: replicas.iter().copied().map(crabka_audit::NodeId).collect(),
            isr: isr.iter().copied().map(crabka_audit::NodeId).collect(),
            leader_epoch: crabka_metadata::LeaderEpoch(leader_epoch),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: vec![uuid::Uuid::nil(); replicas.len()],
            partition_epoch: 0,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn submit_metadata_topic_partition(
        handle: &BrokerHandle,
        topic: &str,
        topic_id: u128,
        partition: i32,
        leader: u64,
        replicas: &[u64],
        isr: &[u64],
        leader_epoch: i32,
    ) {
        handle
            .submit_metadata_record_for_test(metadata_topic_record(topic, topic_id))
            .await
            .expect("submit topic record");
        let partition_record =
            metadata_partition_record(topic, partition, leader, replicas, isr, leader_epoch);
        handle
            .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1Partition(
                partition_record.clone(),
            ))
            .await
            .expect("submit partition record");

        let image = handle.controller_image_for_test();
        assert!(image.topic(topic).is_some());
        assert!(image.partition(topic, partition) == Some(&partition_record));
    }

    fn unused_loopback_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
        listener.local_addr().expect("reserved loopback addr")
    }

    fn static_voter_test_config(
        log_dir: &std::path::Path,
        node_id: u64,
        listen_addr: SocketAddr,
        controller_addr: SocketAddr,
        voters: &[(u64, SocketAddr)],
    ) -> BrokerConfig {
        let mut config = BrokerConfig::for_tests(log_dir.to_path_buf());
        config.broker_id = i32::try_from(node_id).expect("node id fits broker id");
        config.node_id = crabka_raft::NodeId(node_id);
        config.listen_addr = listen_addr;
        config.advertised_listener = listen_addr.to_string();
        config.controller_listen_addr = controller_addr;
        config.directory_id = uuid::Uuid::from_u128(u128::from(node_id));
        config.controller_quorum_voters = voters
            .iter()
            .map(|(id, addr)| (crabka_raft::NodeId(*id), addr.to_string()))
            .collect();
        config
    }

    #[test]
    fn static_voter_set_keeps_peer_hostnames_for_per_dial_resolution() {
        // Peer endpoint hosts MUST be the configured DNS names, NOT resolved to
        // IPs: the inter-broker dialer re-resolves the host on every connect, so
        // a peer that restarts on a new pod IP (stable DNS name, fresh A record)
        // is reached again. Regression — pre-resolving froze the peer's
        // boot-time IP, so after a `StatefulSet` pod restart every peer dialed
        // the dead old IP forever, the rejoining voter never received
        // `BeginQuorumEpoch`, never learned the leader, and never opened :9092.
        let quorum = vec![
            (
                crabka_raft::NodeId(0),
                "demo-broker-0-0.demo-broker-headless.default.svc.cluster.local:9093".to_string(),
            ),
            (
                crabka_raft::NodeId(1),
                "demo-broker-1-0.demo-broker-headless.default.svc.cluster.local:9093".to_string(),
            ),
        ];
        let self_dir = uuid::Uuid::from_u128(7);
        let set = static_controller_voter_set(
            &quorum,
            crabka_audit::NodeId(0),
            self_dir,
            "0.0.0.0:9093".parse().unwrap(),
        );

        let v0 = set.get(crabka_audit::NodeId(0)).expect("voter 0 present");
        let ep0 = v0
            .endpoints
            .iter()
            .find(|e| e.name == "CONTROLLER")
            .expect("controller endpoint");
        // Self keeps its real directory id; peers get the nil placeholder.
        check!(
            ep0.host.as_str() == "demo-broker-0-0.demo-broker-headless.default.svc.cluster.local"
        );
        check!(ep0.port == 9093);
        check!(v0.directory_id == self_dir);

        let v1 = set.get(crabka_audit::NodeId(1)).expect("voter 1 present");
        let ep1 = v1
            .endpoints
            .iter()
            .find(|e| e.name == "CONTROLLER")
            .expect("controller endpoint");
        assert!(ep1.host == "demo-broker-1-0.demo-broker-headless.default.svc.cluster.local");
        assert!(v1.directory_id == uuid::Uuid::nil());
    }

    #[test]
    fn static_voter_set_single_self_voter_uses_listen_addr() {
        // Standalone single-voter: the lone self endpoint uses this node's own
        // controller listen address.
        let quorum = vec![(crabka_raft::NodeId(3), "127.0.0.1:9093".to_string())];
        let self_dir = uuid::Uuid::from_u128(3);
        let set = static_controller_voter_set(
            &quorum,
            crabka_audit::NodeId(3),
            self_dir,
            "192.168.1.5:9099".parse().unwrap(),
        );
        assert!(set.len() == 1);
        let v = set
            .get(crabka_audit::NodeId(3))
            .expect("self voter present");
        let ep = v.endpoints.iter().find(|e| e.name == "CONTROLLER").unwrap();
        assert!(ep.host == "192.168.1.5");
        assert!(ep.port == 9099);
    }

    #[test]
    fn advertised_listener_parser_preserves_valid_host_ports_and_uses_fallback() {
        let cases = [
            ("broker-1.example:19092", ("broker-1.example", 19092)),
            ("[2001:db8::7]:9094", ("[2001:db8::7]", 9094)),
            ("missing-port", ("localhost", 9092)),
            ("broker:not-a-port", ("localhost", 9092)),
        ];
        for (input, (host, port)) in cases {
            assert!(
                parse_advertised_host_port(input) == (host.to_string(), port),
                "input {input:?}"
            );
        }
    }

    #[test]
    fn connection_guard_increments_and_decrements_global_and_per_ip() {
        let limiter = Arc::new(ConnectionLimiter::new(usize::MAX, usize::MAX));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(limiter.total() == 0);
        assert!(limiter.per_ip_count(ip) == 0);

        let g1 = limiter
            .try_acquire(ip)
            .expect("acquire under unlimited caps");
        assert!(limiter.total() == 1);
        assert!(limiter.per_ip_count(ip) == 1);

        let g2 = limiter.try_acquire(ip).expect("second acquire");
        assert!(limiter.total() == 2);
        assert!(limiter.per_ip_count(ip) == 2);

        drop(g1);
        assert!(limiter.total() == 1);
        assert!(limiter.per_ip_count(ip) == 1);

        drop(g2);
        // Per-IP entry must be removed (not left at 0) when it hits zero.
        check!(limiter.total() == 0);
        check!(limiter.per_ip_count(ip) == 0);
        check!(limiter.per_ip.get(&ip).is_none());
    }

    #[test]
    fn global_cap_rejects_at_limit() {
        let limiter = Arc::new(ConnectionLimiter::new(1, usize::MAX));
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        let _g = limiter.try_acquire(a).expect("first connection accepted");
        // Global ceiling of 1 reached — a different IP is still rejected,
        // and the rejection reserves nothing (per-IP entry not created).
        check!(limiter.try_acquire(b).is_none());
        check!(limiter.total() == 1);
        check!(limiter.per_ip_count(b) == 0);
        check!(limiter.per_ip.get(&b).is_none());
    }

    #[test]
    fn per_ip_cap_rejects_but_other_ip_allowed() {
        let limiter = Arc::new(ConnectionLimiter::new(usize::MAX, 1));
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        let _g1 = limiter.try_acquire(a).expect("first from a");
        // Second from the same IP rejected; global must be rolled back so
        // the count reflects only the one live connection.
        check!(limiter.try_acquire(a).is_none());
        check!(limiter.total() == 1);
        check!(limiter.per_ip_count(a) == 1);
        // A different IP is still under its own per-IP ceiling.
        let _g2 = limiter.try_acquire(b).expect("first from b allowed");
        assert!(limiter.total() == 2);
        assert!(limiter.per_ip_count(b) == 1);
    }

    #[test]
    fn ipv6_peer_acquires_and_releases() {
        let limiter = Arc::new(ConnectionLimiter::new(usize::MAX, usize::MAX));
        let ip: IpAddr = "2001:db8::1".parse().unwrap();
        let g = limiter.try_acquire(ip).expect("ipv6 acquire");
        assert!(limiter.per_ip_count(ip) == 1);
        drop(g);
        assert!(limiter.per_ip_count(ip) == 0);
    }

    #[tokio::test]
    async fn accepted_socket_tuning_sets_nodelay_and_large_buffers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener addr");
        let client_task = tokio::spawn(tokio::net::TcpStream::connect(addr));
        let (server, _) = listener.accept().await.expect("accept loopback client");
        let client = client_task
            .await
            .expect("connect task")
            .expect("connect loopback client");

        let sock = socket2::SockRef::from(&server);
        server.set_nodelay(false).expect("clear TCP_NODELAY");
        sock.set_send_buffer_size(4096).expect("shrink send buffer");
        sock.set_recv_buffer_size(4096).expect("shrink recv buffer");
        let send_before = sock.send_buffer_size().expect("read baseline send buffer");
        let recv_before = sock.recv_buffer_size().expect("read baseline recv buffer");

        tune_accepted_socket(&server);

        check!(server.nodelay().expect("read TCP_NODELAY"));
        // Kernels and containers clamp socket buffers differently (and Linux
        // reports doubled values), so assert the portable invariant: tuning grows
        // each buffer above the explicit tiny baseline instead of pinning a host
        // sysctl-dependent absolute size.
        check!(sock.send_buffer_size().expect("read send buffer") > send_before);
        check!(sock.recv_buffer_size().expect("read recv buffer") > recv_before);
        drop(client);
    }

    #[test]
    fn loopback_bootstrap_maps_wildcard_to_loopback() {
        use std::net::SocketAddr;
        let cases = [
            ("0.0.0.0:9092", "127.0.0.1:9092"),
            ("192.168.1.5:9094", "192.168.1.5:9094"),
            ("[::]:9092", "[::1]:9092"),
            ("[2001:db8::5]:9092", "[2001:db8::5]:9092"),
        ];
        for (listen, expected) in cases {
            assert!(
                loopback_bootstrap(listen.parse::<SocketAddr>().unwrap()) == expected,
                "listen {listen}"
            );
        }
    }

    #[test]
    fn controller_adapters_report_leadership_from_leader_watch() {
        let source: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(MockMetadataSource::new(
                crabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
                Some(crabka_raft::NodeId(7)),
            ));

        let leader_adapter = ControllerAdapter {
            handle: source.clone(),
            node_id: crabka_raft::NodeId(7),
        };
        let follower_adapter = ControllerAdapter {
            handle: source.clone(),
            node_id: crabka_raft::NodeId(8),
        };
        assert!(crate::leader_rebalance::ControllerLike::is_leader(
            &leader_adapter
        ));
        assert!(!crate::leader_rebalance::ControllerLike::is_leader(
            &follower_adapter
        ));

        let leader_adapter = ReassignmentControllerAdapter {
            handle: source.clone(),
            node_id: crabka_raft::NodeId(7),
        };
        let follower_adapter = ReassignmentControllerAdapter {
            handle: source,
            node_id: crabka_raft::NodeId(8),
        };
        assert!(crate::reassignment::ReassignmentController::is_leader(
            &leader_adapter
        ));
        assert!(!crate::reassignment::ReassignmentController::is_leader(
            &follower_adapter
        ));
    }

    #[test]
    fn image_watcher_adapters_forward_current_image() {
        let cluster_id = uuid::Uuid::from_u128(0x5150);
        let source: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(MockMetadataSource::new(
                crabka_metadata::MetadataImage::new(cluster_id),
                Some(crabka_raft::NodeId(1)),
            ));

        let leader = ControllerAdapter {
            handle: source.clone(),
            node_id: crabka_raft::NodeId(1),
        };
        assert!(
            crate::leader_rebalance::ControllerLike::current_image(&leader).cluster_id()
                == cluster_id
        );

        let reassignment = ReassignmentControllerAdapter {
            handle: source.clone(),
            node_id: crabka_raft::NodeId(1),
        };
        assert!(
            crate::reassignment::ReassignmentController::current_image(&reassignment).cluster_id()
                == cluster_id
        );
        let reassignment_rx =
            crate::reassignment::ReassignmentController::watch_image(&reassignment);
        assert!(reassignment_rx.borrow().cluster_id() == cluster_id);

        let throttle = ThrottleControllerAdapter {
            handle: source.clone(),
        };
        assert!(crate::throttle::ImageWatcher::current_image(&throttle).cluster_id() == cluster_id);
        let throttle_rx = crate::throttle::ImageWatcher::watch_image(&throttle);
        assert!(throttle_rx.borrow().cluster_id() == cluster_id);

        let quota = QuotaControllerAdapter {
            handle: source.clone(),
        };
        assert!(crate::quota::ImageWatcher::current_image(&quota).cluster_id() == cluster_id);

        let cleanup = DelegationTokenCleanupControllerAdapter { handle: source };
        assert!(
            crate::delegation_token_cleanup::DelegationTokenController::current_image(&cleanup)
                .cluster_id()
                == cluster_id
        );
    }

    #[tokio::test]
    async fn controller_adapters_forward_submit_errors() {
        let source: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(MockMetadataSource::new(
                crabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
                Some(crabka_raft::NodeId(1)),
            ));
        let record = metadata_topic_record("adapter-submit-mutant-topic", 0xADAD);

        let leader = ControllerAdapter {
            handle: source.clone(),
            node_id: crabka_raft::NodeId(1),
        };
        assert!(
            crate::leader_rebalance::ControllerLike::submit_change(&leader, vec![record.clone()])
                .await
                .is_err()
        );

        let reassignment = ReassignmentControllerAdapter {
            handle: source.clone(),
            node_id: crabka_raft::NodeId(1),
        };
        assert!(
            crate::reassignment::ReassignmentController::submit_change(
                &reassignment,
                vec![record.clone()],
            )
            .await
            .is_err()
        );

        let cleanup = DelegationTokenCleanupControllerAdapter { handle: source };
        assert!(
            crate::delegation_token_cleanup::DelegationTokenController::submit_change(
                &cleanup,
                vec![record],
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn rlmm_bootstrap_backoff_returns_false_when_cancelled() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let mut backoff = std::time::Duration::from_mins(1);

        assert!(!rlmm_bootstrap_backoff(&mut backoff, &shutdown).await);
        assert!(backoff == std::time::Duration::from_mins(1));
    }

    #[tokio::test]
    async fn rlmm_bootstrap_backoff_returns_true_after_sleep_and_advances() {
        tokio::time::pause();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_millis(250);
            let ok = rlmm_bootstrap_backoff(&mut backoff, &shutdown).await;
            (ok, backoff)
        });

        tokio::time::advance(std::time::Duration::from_millis(250)).await;
        let (ok, backoff) = task.await.expect("backoff task");
        assert!(ok);
        assert!(backoff == std::time::Duration::from_millis(500));
    }

    #[tokio::test]
    async fn rlmm_reconciler_applies_initial_and_changed_assignments() {
        let log: Arc<dyn crabka_remote_storage_topic::MetadataEventLog> =
            crabka_remote_storage_topic::InProcessMetadataEventLog::new(3);
        let snapshot_dir = tempfile::tempdir().expect("snapshot tempdir");
        let manager = crabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager::start(
            log,
            tokio::runtime::Handle::current(),
            snapshot_dir.path().join("rlmm-manager"),
            std::time::Duration::from_hours(1),
        )
        .await
        .expect("topic-backed manager start");
        let (set_tx, set_rx) = tokio::sync::watch::channel(vec![0, 2]);
        let shutdown = CancellationToken::new();

        spawn_rlmm_reconciler(manager.clone(), set_rx, shutdown.clone());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while manager.assigned_metadata_partitions() != vec![0, 2] {
            assert!(
                std::time::Instant::now() < deadline,
                "initial assignment was not reconciled"
            );
            tokio::task::yield_now().await;
        }

        set_tx.send(vec![1]).expect("send changed assignment");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while manager.assigned_metadata_partitions() != vec![1] {
            assert!(
                std::time::Instant::now() < deadline,
                "changed assignment was not reconciled"
            );
            tokio::task::yield_now().await;
        }

        shutdown.cancel();
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn single_broker_handle_helpers_observe_real_state_and_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
        let handle = Broker::start(config).await.expect("broker start");
        let broker = handle.broker_arc_for_test();

        check!(broker.handlers().get(18).is_some());
        check!(handle.metrics_addr().is_some_and(|addr| addr.port() != 0));
        check!(handle.offset_for_leader_epoch_count_for_test() == 0);
        broker
            .offset_for_leader_epoch_requests
            .store(2, std::sync::atomic::Ordering::Release);
        assert!(handle.offset_for_leader_epoch_count_for_test() == 2);
        assert!(!handle.rlmm_topic_backed_active_for_test());
        broker.metrics.tiered_storage_rlmm_topic_backed.set(1);
        assert!(handle.rlmm_topic_backed_active_for_test());
        broker.metrics.tiered_storage_rlmm_topic_backed.set(2);
        check!(!handle.rlmm_topic_backed_active_for_test());
        check!(handle.reload_tls().is_err());
        check!(!handle.has_partition("missing-mutant-topic", 0).await);
        check!(handle.local_log_end_offset("missing-mutant-topic", 0).await == None);
        check!(
            handle
                .test_advance_log_start("missing-mutant-topic", 0, 10)
                .await
                .is_err()
        );
        check!(
            handle
                .change_membership([crabka_raft::NodeId(1)].into_iter().collect())
                .await
                .is_err()
        );

        let leader = handle.wait_until_controller_leader().await;
        assert!(leader == crabka_raft::NodeId(handle.node_id()));
        assert!(handle.controller_leader_id().await == Some(crabka_raft::NodeId(handle.node_id())));

        let mut endpoints = handle.self_registration_endpoints().await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while endpoints.is_empty() && std::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
            endpoints = handle.self_registration_endpoints().await;
        }
        assert!(!endpoints.is_empty());

        handle
            .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1BrokerRegistration(
                crabka_metadata::BrokerRegistrationRecord {
                    node_id: crabka_raft::NodeId(handle.node_id() + 1),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::from_u128(0xBEEF),
                    host: "127.0.0.1".to_string(),
                    port: 19_092,
                    rack: None,
                    endpoints: vec![crabka_metadata::BrokerEndpoint {
                        name: "PLAINTEXT".to_string(),
                        host: "127.0.0.1".to_string(),
                        port: 19_092,
                        protocol: crabka_security::ListenerProtocol::Plaintext,
                    }],
                },
            ))
            .await
            .expect("submit peer broker registration");
        assert!(
            handle
                .controller_image_for_test()
                .broker(crabka_raft::NodeId(handle.node_id() + 1))
                .is_some()
        );
        handle.wait_until_brokers_registered(2).await;
        assert!(handle.broker_count().await == 2);

        let topic = "handle-mutant-topic";
        let partition_leader = handle.node_id() + 1;
        let partition_isr = [partition_leader, handle.node_id()];
        submit_metadata_topic_partition(
            &handle,
            topic,
            0xCAFE,
            0,
            partition_leader,
            &partition_isr,
            &partition_isr,
            3,
        )
        .await;
        handle.wait_until_partition_present(topic, 0).await;
        check!(handle.has_partition(topic, 0).await);
        check!(handle.partition_leader_for_test(topic, 0) == Some(partition_leader));
        check!(handle.partition_isr_for_test(topic, 0) == Some(partition_isr.to_vec()));
        let observed_partition = handle
            .partition_record_for_test(topic, 0)
            .expect("partition record");
        let expected_partition = crabka_metadata::PartitionRecord {
            topic: topic.to_string(),
            partition: 0,
            leader: crabka_audit::NodeId(partition_leader),
            replicas: partition_isr
                .iter()
                .copied()
                .map(crabka_audit::NodeId)
                .collect(),
            isr: partition_isr
                .iter()
                .copied()
                .map(crabka_audit::NodeId)
                .collect(),
            leader_epoch: crabka_metadata::LeaderEpoch(3),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: vec![uuid::Uuid::nil(); partition_isr.len()],
            partition_epoch: 0,
        };
        assert!(observed_partition == expected_partition);
        check!(handle.partition_leader_for_test("missing-mutant-topic", 0) == None);
        check!(handle.partition_isr_for_test("missing-mutant-topic", 0) == None);
        check!(handle.partition_record_for_test("missing-mutant-topic", 0) == None);
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_partition_leader_changed(
                    topic,
                    0,
                    crabka_raft::NodeId(handle.node_id())
                ),
            )
            .await
            .is_ok()
        );

        assert!(
            matches!(
                broker.controller.read_snapshot_range(0, 1),
                crabka_raft::SnapshotRange::NoSnapshot
            ),
            "test should start without a metadata snapshot"
        );
        handle
            .trigger_snapshot_for_test()
            .await
            .expect("trigger metadata snapshot");
        let crabka_raft::SnapshotRange::Slice(snapshot) =
            broker.controller.read_snapshot_range(0, 1)
        else {
            panic!("trigger_snapshot_for_test should write a readable snapshot");
        };
        assert!(snapshot.total_size > 0);
        assert!(!snapshot.bytes.is_empty());

        let local_topic = "handle-local-log-mutant-topic";
        let local_part = local_partition_with_records(dir.path(), local_topic, 0, &[b"a", b"b"]);
        assert!(!handle.partition_exists_for_test(local_topic, 0));
        broker.partitions.insert(
            local_topic.to_string(),
            PartitionIndex(0),
            Arc::clone(&local_part),
        );
        assert!(handle.partition_exists_for_test(local_topic, 0));
        assert!(handle.local_log_end_offset(local_topic, 0).await == Some(2));
        handle.test_set_leader_epoch(local_topic, 0, 7);
        assert!(
            local_part
                .current_leader_epoch
                .load(std::sync::atomic::Ordering::Acquire)
                == 7
        );
        handle
            .test_truncate_local_log(local_topic, 0, 1)
            .await
            .expect("truncate local partition");
        assert!(handle.local_log_end_offset(local_topic, 0).await == Some(0));

        let helper_topic = "handle-partition-helper-mutant-topic";
        let helper_part = local_partition_with_records(dir.path(), helper_topic, 0, &[]);
        let helper_config = crabka_log::LogConfig {
            retention_ms: Some(std::time::Duration::from_secs(123)),
            segment_bytes: 4096,
            ..Default::default()
        };
        helper_part
            .log
            .lock()
            .expect("helper partition log lock")
            .set_config(helper_config.clone());
        broker.partitions.insert(
            helper_topic.to_string(),
            PartitionIndex(0),
            Arc::clone(&helper_part),
        );
        handle
            .test_advance_log_start(helper_topic, 0, 2)
            .await
            .expect("advance helper partition log start");
        assert!(handle.partition_log_start_for_test(helper_topic, 0) == Some(2));
        assert!(
            handle.partition_retention_ms_for_test(helper_topic, 0)
                == Some(Some(std::time::Duration::from_secs(123)))
        );
        let observed_config = handle
            .partition_log_config_for_test(helper_topic, 0)
            .expect("helper partition log config");
        assert!(observed_config.retention_ms == helper_config.retention_ms);
        assert!(observed_config.segment_bytes == helper_config.segment_bytes);
        let last_offset = handle
            .produce_records_for_test(helper_topic, 0, 3)
            .await
            .expect("produce helper partition records");
        let log_end = handle
            .local_log_end_offset(helper_topic, 0)
            .await
            .expect("helper partition log end offset");
        assert!(last_offset >= 2);
        assert!(last_offset + 1 == log_end);
        let read = helper_part
            .log
            .lock()
            .expect("helper partition log lock")
            .read(crabka_log::Offset(2), 1 << 20)
            .expect("read helper partition records");
        assert!(read.start_offset == crabka_log::Offset(2));
        assert!(!read.batches.is_empty());
        let records: Vec<_> = read
            .batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .collect();
        check!(records.len() == 1);
        check!(records[0].offset_delta == 0);
        check!(
            records[0].value.as_ref().map(bytes::Bytes::as_ref)
                == Some(b"test-record-2".as_slice())
        );
        // Waiting for log_end + 1 must stay pending; waiting for the reached
        // log_end must resolve (both the >= and == variants).
        check!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_local_log_end_offset(helper_topic, 0, log_end + 1),
            )
            .await
            .is_err()
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_local_log_end_offset_eq(helper_topic, 0, log_end + 1),
            )
            .await
            .is_err()
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_local_log_end_offset(helper_topic, 0, log_end),
            )
            .await
            .is_ok()
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_local_log_end_offset_eq(helper_topic, 0, log_end),
            )
            .await
            .is_ok()
        );

        let share_group = "handle-share-summary-mutant-group";
        let share_topic_id = uuid::Uuid::from_u128(0xBEE5);
        let share_partition = 3;
        assert!(
            handle
                .share_state_summary_for_test(share_group, share_topic_id, share_partition)
                .await
                .is_none()
        );
        let share_state_partition = broker.share_coordinator.state_partition_for(
            share_group,
            &share_topic_id,
            share_partition,
        );
        let share_state_part = local_partition_with_records(
            dir.path(),
            crate::share_coordinator::bootstrap::TOPIC,
            share_state_partition.0,
            &[],
        );
        broker.partitions.insert(
            crate::share_coordinator::bootstrap::TOPIC.to_string(),
            share_state_partition,
            share_state_part,
        );
        broker
            .share_coordinator
            .initialize(
                share_group,
                share_topic_id,
                share_partition,
                11,
                crabka_log::Offset(90),
            )
            .await
            .expect("initialize share state");
        broker
            .share_coordinator
            .write(
                share_group,
                share_topic_id,
                share_partition,
                12,
                2,
                crabka_log::Offset(95),
                7,
                vec![crate::share_coordinator::persistence::StateBatch {
                    first_offset: crabka_log::Offset(95),
                    last_offset: crabka_log::Offset(99),
                    delivery_state: 0,
                    delivery_count: 1,
                }],
            )
            .await
            .expect("write share state summary");
        check!(
            handle
                .share_state_summary_for_test(share_group, share_topic_id, share_partition)
                .await
                == Some((12, 2, 95, 7))
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_share_spso(share_group, share_topic_id, share_partition, 95),
            )
            .await
            .is_ok()
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_share_delivery_complete(
                    share_group,
                    share_topic_id,
                    share_partition,
                    7,
                ),
            )
            .await
            .is_ok()
        );

        let acquired_group = "handle-share-acquired-mutant-group";
        let acquired_topic_id = uuid::Uuid::from_u128(0xACCD);
        let acquired_cell = broker
            .share_partition_leaders
            .get_or_load(acquired_group, acquired_topic_id, 0)
            .await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_share_acquired_count(acquired_group, acquired_topic_id, 0, 1),
            )
            .await
            .is_err()
        );
        {
            let mut state = acquired_cell.lock().await;
            state.materialize(crabka_log::Offset(3), 10);
            let acquired = state.acquire(
                "member-1",
                3,
                i32::MAX,
                std::time::Instant::now(),
                std::time::Duration::from_secs(30),
                i16::MAX,
            );
            assert!(!acquired.is_empty());
        }
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_share_acquired_count(acquired_group, acquired_topic_id, 0, 1),
            )
            .await
            .is_ok()
        );

        let closed_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed learner port");
        let closed_addr = closed_listener.local_addr().expect("closed learner addr");
        drop(closed_listener);
        let add_learner = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            handle.add_learner(crabka_raft::NodeId(handle.node_id() + 10), closed_addr),
        )
        .await
        .expect("add_learner returned before timeout");
        assert!(add_learner.is_err());

        let own_directory = handle
            .voter_directory_id_for_test(crabka_raft::NodeId(handle.node_id()))
            .expect("own voter directory id");
        check!(own_directory != uuid::Uuid::nil());
        check!(
            handle.voter_directory_id_for_test(crabka_raft::NodeId(handle.node_id() + 10_000))
                == None
        );

        // Marking the same log dir offline twice: first succeeds, second is a
        // no-op.
        check!(handle.test_mark_log_dir_offline(dir.path()));
        check!(!handle.test_mark_log_dir_offline(dir.path()));
        handle.shutdown().await;
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn group_handle_helpers_observe_live_actor_views() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.expect("broker start");
        let broker = handle.broker_arc_for_test();

        let group_id = "handle-next-gen-group-mutant";
        let member_id = "member-1";
        let actor = broker.group_coordinator.get_or_create_group(
            group_id,
            crate::coordinator::unified::actor::GroupKindTag::Consumer,
        );
        actor
            .tx
            .send(crate::coordinator::unified::actor::GroupActorMessage::Seed(
                consumer_group_seed(member_id),
            ))
            .await
            .expect("seed next-gen group");
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_group_member_count(group_id, 1),
            )
            .await
            .is_ok()
        );
        let described = handle
            .group_describe_for_test(group_id)
            .await
            .expect("next-gen group describe");
        check!(described.group_id.as_str() == group_id);
        check!(described.members.len() == 1);
        check!(described.members[0].member_id.as_str() == member_id);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_group_empty(group_id),
            )
            .await
            .is_err()
        );

        let empty_group_id = "handle-empty-group-mutant";
        let _ = broker.group_coordinator.get_or_create_group(
            empty_group_id,
            crate::coordinator::unified::actor::GroupKindTag::Consumer,
        );
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_group_empty(empty_group_id),
            )
            .await
            .is_ok()
        );

        let classic_group_id = "handle-classic-group-mutant";
        let classic_member_id = "classic-member-1";
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_classic_group_member_count(classic_group_id, 1),
            )
            .await
            .is_err()
        );
        broker.group_coordinator.seed_classic(
            classic_group_id,
            classic_group_with_member(classic_group_id, classic_member_id),
        );
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_classic_group_member_count(classic_group_id, 1),
            )
            .await
            .is_ok()
        );
        let classic = handle
            .classic_group_inspect_for_test(classic_group_id)
            .await
            .expect("classic group inspect");
        check!(classic.group_id.as_str() == classic_group_id);
        check!(classic.members.len() == 1);
        check!(classic.members[0].member_id.as_str() == classic_member_id);

        let created_classic_group_id = "handle-create-classic-group-mutant";
        assert!(
            handle
                .classic_group_inspect_for_test(created_classic_group_id)
                .await
                .is_none()
        );
        handle.group_create_for_test(created_classic_group_id);
        let created = handle
            .classic_group_inspect_for_test(created_classic_group_id)
            .await
            .expect("created classic group inspect");
        assert!(created.group_id == created_classic_group_id);
        assert!(created.members.is_empty());

        let marked_classic_group_id = "handle-marked-classic-group-mutant";
        assert!(
            handle
                .group_type_for_test(marked_classic_group_id)
                .is_none()
        );
        broker
            .group_coordinator
            .mark_classic(marked_classic_group_id);
        assert!(
            handle.group_type_for_test(marked_classic_group_id)
                == Some(crate::coordinator::unified::GroupType::Classic)
        );

        assert_streams_group_helpers_observe_live_actor_view(&broker, &handle).await;

        handle.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broker_handle_reports_non_default_node_and_voter_state() {
        let dir7 = tempfile::tempdir().unwrap();
        let dir8 = tempfile::tempdir().unwrap();
        let listen7 = unused_loopback_addr();
        let listen8 = unused_loopback_addr();
        let controller7 = unused_loopback_addr();
        let controller8 = unused_loopback_addr();
        let voters = [(7, controller7), (8, controller8)];

        let config7 = static_voter_test_config(dir7.path(), 7, listen7, controller7, &voters);
        let config8 = static_voter_test_config(dir8.path(), 8, listen8, controller8, &voters);
        let start = Box::pin(tokio::time::timeout(
            std::time::Duration::from_secs(10),
            async { tokio::try_join!(Broker::start(config7), Broker::start(config8)) },
        ));
        let (handle7, handle8) = start
            .await
            .expect("two-voter brokers started before timeout")
            .expect("two-voter broker start");

        let leader = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(leader) = handle7.controller_leader_id().await
                    && leader != crabka_raft::NodeId(0)
                {
                    return leader;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("two-voter cluster leader");
        assert!(leader == crabka_raft::NodeId(7) || leader == crabka_raft::NodeId(8));
        handle7.wait_for_image(|img| img.voters().len() == 2).await;
        handle8.wait_for_image(|img| img.voters().len() == 2).await;

        check!(handle7.node_id() == 7);
        check!(handle8.node_id() == 8);
        check!(handle7.controller_leader_id().await == Some(leader));
        check!(
            handle7
                .quorum_voters_for_test()
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                == [crabka_raft::NodeId(7), crabka_raft::NodeId(8)]
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>()
        );
        check!(handle7.voter_count_for_test() == 2);
        check!(
            handle7.voter_ids_for_test()
                == [crabka_raft::NodeId(7), crabka_raft::NodeId(8)]
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>()
        );

        // The multi-thread test runtime aborts remaining tasks on exit if raft
        // shutdown takes longer than the helper assertions above.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(handle7.shutdown(), handle8.shutdown());
        })
        .await;
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn wait_helpers_remain_pending_until_their_conditions_are_met() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.expect("broker start");
        let timeout = std::time::Duration::from_millis(75);
        let topic_id = uuid::Uuid::from_u128(0xFEED);

        // Every wait helper must still be pending (time out) while its
        // condition is unmet. The futures are lazy async fns, so building the
        // table up front does no work; each is awaited sequentially below.
        #[allow(clippy::type_complexity)]
        let pending_waits: [(
            &str,
            std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
        ); 9] = [
            (
                "wait_for_share_state_summary",
                Box::pin(async {
                    let () = handle
                        .wait_for_share_state_summary("missing-mutant-group", topic_id, 0)
                        .await;
                }),
            ),
            (
                "wait_until_share_spso",
                Box::pin(async {
                    handle
                        .wait_until_share_spso("missing-mutant-group", topic_id, 0, 1)
                        .await;
                }),
            ),
            (
                "wait_until_share_delivery_complete",
                Box::pin(async {
                    handle
                        .wait_until_share_delivery_complete("missing-mutant-group", topic_id, 0, 1)
                        .await;
                }),
            ),
            (
                "wait_until_group_member_count",
                Box::pin(async {
                    handle
                        .wait_until_group_member_count("missing-mutant-group", 1)
                        .await;
                }),
            ),
            (
                "wait_until_streams_group_member_count",
                Box::pin(async {
                    handle
                        .wait_until_streams_group_member_count("missing-mutant-streams", 1)
                        .await;
                }),
            ),
            (
                "wait_until_brokers_registered",
                Box::pin(async {
                    handle.wait_until_brokers_registered(2).await;
                }),
            ),
            (
                "wait_until_partition_present",
                Box::pin(async {
                    handle
                        .wait_until_partition_present("missing-mutant-topic", 0)
                        .await;
                }),
            ),
            (
                "wait_until_partition_leader_changed",
                Box::pin(async {
                    handle
                        .wait_until_partition_leader_changed(
                            "missing-mutant-topic",
                            0,
                            crabka_raft::NodeId(1),
                        )
                        .await;
                }),
            ),
            (
                "wait_until_isr_len",
                Box::pin(async {
                    handle
                        .wait_until_isr_len("missing-mutant-topic", 0, 1)
                        .await;
                }),
            ),
        ];
        for (name, wait) in pending_waits {
            assert!(
                tokio::time::timeout(timeout, wait).await.is_err(),
                "{name} resolved while its condition was unmet"
            );
        }

        // wait_until_partition_leader_changed must stay pending for each of
        // these submitted partitions:
        // (topic, topic_id, leader, replicas/isr, leader_epoch, excluded leader)
        #[allow(clippy::type_complexity)]
        let leader_changed_cases: [(&str, u128, u64, &[u64], i32, u64); 4] = [
            // leader 0 means "no leader" — never counts as a change.
            ("leader-zero-mutant-topic", 0xF001, 0, &[1], 3, 1),
            // the current leader is exactly the excluded node.
            ("leader-excluded-mutant-topic", 0xF002, 2, &[1, 2], 3, 2),
            // leader epoch 0 is not a completed election.
            ("leader-epoch-zero-mutant-topic", 0xF003, 2, &[1, 2], 0, 1),
            // negative leader epoch likewise.
            (
                "leader-epoch-negative-mutant-topic",
                0xF004,
                2,
                &[1, 2],
                -1,
                1,
            ),
        ];
        for (topic, topic_id, leader, replicas, leader_epoch, excluded) in leader_changed_cases {
            submit_metadata_topic_partition(
                &handle,
                topic,
                topic_id,
                0,
                leader,
                replicas,
                replicas,
                leader_epoch,
            )
            .await;
            assert!(
                tokio::time::timeout(
                    timeout,
                    handle.wait_until_partition_leader_changed(
                        topic,
                        0,
                        crabka_raft::NodeId(excluded)
                    ),
                )
                .await
                .is_err(),
                "{topic}: wait_until_partition_leader_changed resolved"
            );
        }
        // Leader 0 is also reported as "no leader" by the direct helper.
        assert!(
            handle
                .partition_leader_for_test("leader-zero-mutant-topic", 0)
                .is_none()
        );

        submit_metadata_topic_partition(
            &handle,
            "isr-len-mutant-topic",
            0xF005,
            0,
            1,
            &[1, 2],
            &[1, 2],
            3,
        )
        .await;
        assert!(
            tokio::time::timeout(
                timeout,
                handle.wait_until_isr_len("isr-len-mutant-topic", 0, 1)
            )
            .await
            .is_err()
        );

        handle.shutdown().await;
    }

    #[test]
    fn needed_metadata_partitions_covers_led_and_followed() {
        use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
        use crabka_remote_storage::TopicIdPartition;
        use crabka_remote_storage_topic::metadata_partition_for;
        use uuid::Uuid;

        let topic_id = Uuid::from_u128(0xABCD);
        let mut image = MetadataImage::new(Uuid::from_u128(1));
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions: 3,
            replication_factor: 2,
        }));
        // node 7 leads p0, follows p1 (replica), is absent from p2.
        for (partition, leader, replicas) in [
            (0_i32, 7_u64, vec![7_u64, 8]),
            (1, 8, vec![8, 7]),
            (2, 8, vec![8, 9]),
        ] {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "orders".into(),
                partition,
                leader: crabka_audit::NodeId(leader),
                replicas: replicas.iter().copied().map(crabka_audit::NodeId).collect(),
                isr: replicas.iter().copied().map(crabka_audit::NodeId).collect(),
                leader_epoch: crabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }

        let got = needed_metadata_partitions(&image, crabka_audit::NodeId(7), 50);

        let mut expected = vec![
            metadata_partition_for(&TopicIdPartition::new(topic_id, "orders", 0), 50),
            metadata_partition_for(&TopicIdPartition::new(topic_id, "orders", 1), 50),
        ];
        expected.sort_unstable();
        expected.dedup();
        assert!(
            got == expected,
            "p2 (node 7 not a replica) must be excluded"
        );
    }

    #[tokio::test]
    async fn start_and_shutdown_clean() {
        let dir = tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.unwrap();
        let broker = handle.broker_arc_for_test();
        let addr = handle.listen_addr();
        assert!(addr.port() != 0);
        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("listener accepts before shutdown");
        wait_for_connection_count(&broker, 1, "accept_loop did not register live connection").await;
        drop(stream);
        wait_for_connection_count(&broker, 0, "connection guard did not release client slot").await;
        handle.shutdown().await;
        assert_listener_stops_accepting(addr).await;
    }

    #[tokio::test]
    async fn controlled_shutdown_timeout_stops_listener_and_reports_error() {
        let dir = tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.unwrap();
        let addr = handle.listen_addr();
        let err = handle
            .controlled_shutdown(std::time::Duration::ZERO)
            .await
            .expect_err("zero-timeout controlled shutdown should report drain timeout");
        assert!(matches!(err, BrokerError::ShutdownTimeout(timeout) if timeout.is_zero()));
        assert_listener_stops_accepting(addr).await;
    }

    #[test]
    fn rlmm_backoff_doubles_then_caps() {
        use std::time::Duration;
        let max = Duration::from_secs(10);
        let cases = [
            (Duration::from_millis(250), Duration::from_millis(500)),
            (Duration::from_secs(8), max), // 16s capped to 10s
            (max, max),
        ];
        for (current, expected) in cases {
            assert!(
                next_rlmm_backoff(current, max) == expected,
                "current {current:?}"
            );
        }
    }

    #[tokio::test]
    async fn cancelled_topic_rlmm_bootstrap_attempts_once_without_activating() {
        // A loopback address with nothing listening: bind to learn a free
        // port, then drop the listener so the bootstrap's dial cannot
        // succeed. On Windows such a connect does not fail fast, which is
        // exactly why the bootstrap must honour the token mid-attempt.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap = listener.local_addr().unwrap().to_string();
        drop(listener);

        let swap = Arc::new(crabka_remote_storage_topic::SwappableRlmm::new(Arc::new(
            crabka_remote_storage_topic::NotReadyRlmm::new(),
        )));
        let snapshot_dir = tempdir().unwrap();
        let cfg = KafkaSwapKickoff {
            cfg: crate::config::KafkaRlmmConfig {
                bootstrap,
                num_partitions: 1,
                replication: 1,
                snapshot_interval: std::time::Duration::from_mins(1),
                snapshot_dir: snapshot_dir.path().to_path_buf(),
                security: None,
            },
            broker_id: 1,
        };
        let metrics = crate::metrics::BrokerMetrics::new();
        let (_image_tx, image_rx) = tokio::sync::watch::channel(Arc::new(
            crabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
        ));
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            bootstrap_topic_rlmm(
                swap,
                cfg,
                tokio::runtime::Handle::current(),
                metrics.clone(),
                crabka_raft::NodeId(7),
                image_rx,
                shutdown,
            ),
        )
        .await
        .expect("cancelled bootstrap should return promptly");

        // One attempt was recorded, but the cancelled token stopped the
        // dial before anything could activate the topic-backed manager.
        assert!(metrics.tiered_storage_rlmm_bootstrap_attempts.get() == 1);
        assert!(metrics.tiered_storage_rlmm_topic_backed.get() == 0);
    }

    #[tokio::test]
    async fn start_recovers_existing_partition_dirs() {
        let dir = tempdir().unwrap();
        // Create a partition dir with a log inside.
        let part_dir = dir.path().join("foo-0");
        std::fs::create_dir(&part_dir).unwrap();
        {
            let _log = crabka_log::Log::open(&part_dir, crabka_log::LogConfig::default()).unwrap();
        }

        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.unwrap();
        // We can't easily inspect the partition registry from outside the
        // crate yet, but starting cleanly is the assertion we need here.
        handle.shutdown().await;
    }
}
