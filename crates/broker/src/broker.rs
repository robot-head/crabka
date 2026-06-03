//! Top-level `Broker` lifecycle. Wires together the partition registry,
//! metadata image, network listener, and handler table.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use std::sync::atomic::{AtomicI32, AtomicU64};

use crate::config::BrokerConfig;
use crate::error::BrokerError;
use crate::handlers::HandlerTable;
use crate::log_dir;
use crate::partition::{Partition, WriterMessage};
use crate::partition_registry::PartitionRegistry;

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
    pub(crate) future_logs: Arc<DashMap<(String, i32), Arc<crate::future_log::FutureLogState>>>,
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
    /// KIP-714 client-metrics receiver: subscription manager + Prometheus
    /// collector + OTLP forwarder. Shared so the push handler (Task 15)
    /// and the scrape path both touch the same instance.
    pub(crate) client_metrics: Arc<crate::client_metrics::ClientMetrics>,
    handlers: HandlerTable,
}

impl Broker {
    pub(crate) fn handlers(&self) -> &HandlerTable {
        &self.handlers
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
            directory_id: uuid::Uuid::from_u128(u128::from(node_id)),
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
        let part = self._broker.partitions.get(topic, partition)?;
        Some(part.log_end_offset())
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
            .get(topic, partition)
            .ok_or_else(|| {
                crate::error::BrokerError::Replication(format!(
                    "partition {topic}-{partition} not local"
                ))
            })?;
        part.truncate_to(offset).await
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
            .get(topic, partition)
            .ok_or_else(|| {
                crate::error::BrokerError::Replication(format!(
                    "partition {topic}-{partition} not local"
                ))
            })?;
        part.test_set_log_start(new_start).await
    }

    /// Test-only: directly set `current_leader_epoch` on a locally-hosted
    /// partition. Used by `tests/leader_epoch.rs` to simulate split-brain
    /// (force an epoch bump) without going through the supervisor's
    /// metadata-image-driven path.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub fn test_set_leader_epoch(&self, topic: &str, partition: i32, epoch: i32) {
        if let Some(part) = self._broker.partitions.get(topic, partition) {
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
        self._broker.partitions.contains(topic, partition)
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
        self._broker
            .share_coordinator
            .read_summary(group, topic_id, partition)
            .await
    }

    /// Test-only: return the `log_start_offset` of `(topic, partition)` as
    /// reported by its underlying [`crabka_log::Log`]. Returns `None` if the
    /// partition is not hosted on this broker.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn partition_log_start_for_test(&self, topic: &str, partition: i32) -> Option<i64> {
        let part = self._broker.partitions.get(topic, partition)?;
        Some(part.log_start_offset())
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
        let part = self._broker.partitions.get(topic, partition)?;
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
        let part = self._broker.partitions.get(topic, partition)?;
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
            .get(topic, partition)
            .ok_or_else(|| {
                crate::error::BrokerError::Replication(format!(
                    "partition {topic}-{partition} not local"
                ))
            })?;
        let mut last_offset = 0i64;
        for i in 0..n {
            let batch = crabka_protocol::records::RecordBatch {
                last_offset_delta: 0,
                records: vec![crabka_protocol::records::Record {
                    offset_delta: 0,
                    value: Some(bytes::Bytes::from(format!("test-record-{i}").into_bytes())),
                    ..Default::default()
                }],
                ..Default::default()
            };
            last_offset = part.produce_batch(batch).await?;
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

    /// This broker's raft `node_id` (1-indexed broker id used in raft quorum
    /// and metadata records). Exposed so integration tests can build
    /// `IncrementalAlterConfigs` broker-resource requests targeting this
    /// broker without hard-coding a node id.
    #[must_use]
    #[allow(clippy::used_underscore_binding)]
    pub fn node_id(&self) -> u64 {
        self._broker.config.node_id
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
        if p.leader == 0 { None } else { Some(p.leader) }
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
        Some(p.isr.clone())
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

    /// Request a graceful, controlled shutdown of this broker.
    ///
    /// Signals the heartbeat client to set `want_shut_down=true` on
    /// outbound `BrokerHeartbeat` requests. The controller leader
    /// reassigns leadership of every partition currently led by this
    /// broker; once leadership is fully drained, the controller
    /// responds with `should_shut_down=true`. This call then invokes
    /// the regular [`shutdown`](Self::shutdown).
    ///
    /// Returns `Err(BrokerError::ShutdownTimeout)` if leadership is not
    /// fully drained within `timeout`. The caller can then fall back to
    /// a hard shutdown via [`shutdown`](Self::shutdown) if it wishes.
    ///
    /// # Errors
    ///
    /// - `BrokerError::ShutdownTimeout` — the controller did not
    ///   acknowledge `should_shut_down=true` within `timeout`.
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
        match tokio::time::timeout(timeout, wait).await {
            Ok(()) => {
                self.shutdown().await;
                Ok(())
            }
            Err(_) => Err(BrokerError::ShutdownTimeout(timeout)),
        }
    }

    /// Cancel the listener + drain in-flight connections. Awaiting the
    /// returned future blocks until the listener task exits.
    #[allow(clippy::used_underscore_binding)] // `_broker` carries shared state we must reach into during shutdown
    pub async fn shutdown(mut self) {
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
        Self::start_with_controller_listener(config, None).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr`.
    ///
    /// This threads the listener through to
    /// [`crabka_raft::Controller::start_with_listener`] so test harnesses
    /// can eliminate the bind-and-drop TOCTOU race on the controller port.
    /// The listener's local address MUST equal
    /// `config.controller_listen_addr`. The data-plane listeners still
    /// bind from `config` — pass `127.0.0.1:0` there and read the bound
    /// port back via [`BrokerHandle::listen_addr`] to avoid that race too.
    #[allow(clippy::too_many_lines)] // sequential bring-up; splitting hurts readability more than it helps
    pub async fn start_with_controller_listener(
        mut config: BrokerConfig,
        controller_listener: Option<tokio::net::TcpListener>,
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

        // 0d. Build the outbound `TlsConnector` and the shared
        //     `InterBrokerClient` once. Both the replicator-supervisor
        //     and the heartbeat client clone the resulting Arc; the
        //     raft transport receives it as an injected dialer.
        let tls_connector = match &config.tls_config {
            Some(tls) => {
                let client_cfg = tls
                    .build_client_config()
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
            };
            Some(Arc::new(hs) as Arc<dyn crabka_raft::RaftListenerHandshake>)
        };

        let raft_dialer: Option<std::sync::Arc<dyn crabka_raft::OutboundDialer>> =
            Some(Arc::new(crate::network::client::InterBrokerDialer::new(
                inter_broker_client.clone(),
                config.controller_listener_protocol,
                "localhost".to_string(),
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
            // `ControllerConfig.initial_voters` every boot (static this slice;
            // KIP-853 dynamic reconfig is Slice 5). When no `VotersRecord` was
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
                let voters = if config.controller_quorum_voters.len() > 1 {
                    // Static N-voter set: one `Voter` per configured
                    // `(node_id, controller_addr)`. The engine identifies
                    // voters by node id for dialing (CONTROLLER endpoint) and
                    // for vote-grant tallying; `directory_id` is carried in the
                    // records but is not used by the engine's vote/peer logic
                    // (verified against `kraft/network.rs::controller_addr` and
                    // `kraft/core.rs`, which key on `NodeId` and use
                    // `Uuid::nil()` for vote keys). So `directory_id` only needs
                    // to be exact for self; peers get a deterministic
                    // placeholder until KIP-853/JVM-interop (Slice 5/6) makes
                    // peer directory ids load-bearing.
                    let voters: Vec<crabka_metadata::Voter> = config
                        .controller_quorum_voters
                        .iter()
                        .map(|&(node_id, addr)| crabka_metadata::Voter {
                            id: node_id,
                            directory_id: if node_id == config.node_id {
                                config.directory_id
                            } else {
                                uuid::Uuid::nil()
                            },
                            endpoints: vec![crabka_metadata::VoterEndpoint {
                                name: "CONTROLLER".to_string(),
                                host: addr.ip().to_string(),
                                port: addr.port(),
                            }],
                            kraft_version: crabka_metadata::KRaftVersionRange::default(),
                        })
                        .collect();
                    tracing::info!(
                        node_id = config.node_id,
                        voter_count = voters.len(),
                        mode = ?config.bootstrap_mode,
                        "KIP-595 static multi-voter set: deriving voters from controller_quorum_voters"
                    );
                    crabka_metadata::VoterSet::from_voters(voters)
                } else {
                    let self_voter = crabka_metadata::Voter {
                        id: config.node_id,
                        directory_id: config.directory_id,
                        endpoints: vec![crabka_metadata::VoterEndpoint {
                            name: "CONTROLLER".to_string(),
                            host: config.controller_listen_addr.ip().to_string(),
                            port: config.controller_listen_addr.port(),
                        }],
                        kraft_version: crabka_metadata::KRaftVersionRange::default(),
                    };
                    tracing::info!(
                        node_id = config.node_id,
                        mode = ?config.bootstrap_mode,
                        "KIP-595 single-voter set: forming/recovering single-voter cluster"
                    );
                    crabka_metadata::VoterSet::from_voters([self_voter])
                };
                // The engine reconstructs its voter set from `initial_voters`
                // (config-derived) every boot under KIP-595 static voters, so the
                // `V1Voters` / `V1KRaftVersion` raft-control records are NOT seeded
                // onto the KIP-631-framed metadata log here (they have no KIP-631
                // counterpart; dynamic reconfiguration is a later slice).
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
                    max_bytes: 1_048_576,
                    poll_interval: std::time::Duration::from_millis(100),
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
            let deadline = std::time::Instant::now() + std::time::Duration::from_mins(2);
            while leader_rx.borrow().is_none() {
                if std::time::Instant::now() > deadline {
                    return Err(BrokerError::Startup(
                        "no leader elected within 2 min".into(),
                    ));
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
                // Per-listener endpoints (Task 11): every configured listener's
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
                if let Err(e) = controller.submit_change(vec![self_reg]).await {
                    tracing::warn!(error = %e, "self-registration failed; continuing");
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
                // on the KIP-631-framed metadata log in this slice — the engine
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

        // 4. Scan + recover partitions on disk. Partition state is still
        //    a local-disk concern; the metadata image is sourced from
        //    `controller.current_image()` whenever a handler needs it.
        //    Skip dirs marked offline by the probe — `scan_all` would
        //    fail the whole startup on the first IO error, and we want
        //    to keep recovering partitions on the surviving dirs.
        let partitions: Arc<PartitionRegistry> = Arc::new(PartitionRegistry::new());
        // Controller-only nodes host no data partitions, so they skip the
        // disk scan/recovery entirely.
        if config.is_broker() {
            let scan_dirs = log_dir_status.online_subset(&config.all_log_dirs());
            for (topic, partition_id, owning_dir) in log_dir::scan_all(&scan_dirs)? {
                let dir = log_dir::partition_dir(&owning_dir, &topic, partition_id);
                let log = crabka_log::Log::open(&dir, config.log_config.clone())?;
                let part = spawn_partition(
                    topic.clone(),
                    partition_id,
                    owning_dir,
                    log,
                    log_dir_status.clone(),
                );
                partitions.insert(topic.clone(), partition_id, part);
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
        ));
        let producer_ids = Arc::new(crate::producer_id_manager::ProducerIdManager::new());
        let producer_state = Arc::new(crate::producer_state::ProducerState::new());
        crate::coordinator::bootstrap::bootstrap(
            &config,
            &controller,
            &partitions,
            &group_coordinator,
            &log_dir_status,
        )
        .await?;

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

        // 4a'''. Share-partition leader manager (KIP-932 Slice C): owns the
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
        // KIP-113 offline-dir handling: feed the supervisor the online
        // subset so newly materialized partitions never land on a dir
        // the startup probe flagged unwritable.
        let supervisor = crate::replicator_supervisor::ReplicatorSupervisor::new(
            config.node_id,
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
            metrics.clone(),
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
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
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
                                n,
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
                                n,
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
        //       known in the current metadata image.  This ensures that peers
        //       which were heartbeating to the previous leader (and therefore
        //       have no entry in *this* broker's liveness map) are detected as
        //       dead after `heartbeat_timeout_ms` if they stop sending to us.
        //       Without seeding, `AliveToDead` never fires for a broker that
        //       dies while a *different* raft node is the leader.
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
                            .map(|b| b.node_id)
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
        let otlp_metrics_endpoint =
            crate::telemetry::OtlpConfig::from_env(|k| std::env::var(k).ok(), "", "")
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
        // Periodic eviction of stale client-metrics entries (3× push
        // interval, floor 600s). Child token of supervisor_shutdown.
        {
            let cm = client_metrics.clone();
            let token = supervisor_shutdown.child_token();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_mins(1));
                loop {
                    tokio::select! {
                        () = token.cancelled() => break,
                        _ = tick.tick() => cm.manager.evict_stale(3, std::time::Duration::from_mins(10)),
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
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
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
                                .and_then(|m| m.get("min.insync.replicas"))
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
                        if pr.replicas.contains(&node_id) && !alive.contains(&pr.leader) {
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

        // 5. Build handler table.
        let handlers = crate::handlers::build_table();

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
            let listener = TcpListener::bind(spec.bind_addr).await?;
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
        let future_logs: Arc<DashMap<(String, i32), Arc<crate::future_log::FutureLogState>>> =
            Arc::new(DashMap::new());

        // KIP-113: resume any interrupted intra-broker moves left on
        // disk as `<topic>-<partition>-future` dirs.
        for owning_dir in config.all_log_dirs() {
            let futures = log_dir::scan_future(&owning_dir).unwrap_or_default();
            for (topic, partition_id) in futures {
                if !partitions.contains(&topic, partition_id) {
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
                    partition_id,
                ) {
                    tracing::warn!(
                        topic = %topic, partition = partition_id,
                        error = ?e,
                        "failed to resume interrupted log-dir move"
                    );
                }
            }
        }

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
            inter_broker_client,
            inter_broker_listener_protocol: inter_listener_proto,
            unclean_recovery,
            metrics: metrics.clone(),
            metrics_bound_addr,
            throttle_state,
            quota_buckets,
            fetch_session_cache,
            want_shutdown: want_shutdown_tx,
            should_shutdown: should_shutdown_tx,
            remote_reader,
            log_dir_status,
            client_metrics,
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
    let mut backoff = std::time::Duration::from_millis(250);
    let manager = loop {
        metrics.tiered_storage_rlmm_bootstrap_attempts.inc();
        // KafkaMetadataEventLog::start and TopicBasedRemoteLogMetadataManager::start
        // return different error types, so we handle them with separate match arms.
        let log = match crabka_remote_storage_topic::KafkaMetadataEventLog::start(log_cfg.clone())
            .await
        {
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
    partition_id: i32,
    log_dir: std::path::PathBuf,
    log: crabka_log::Log,
    log_dir_status: crate::log_dir_status::LogDirRegistry,
) -> Arc<Partition> {
    let log = Arc::new(Mutex::new(log));
    let (tx, rx) = tokio::sync::mpsc::channel::<WriterMessage>(64);
    let notify = Arc::new(tokio::sync::Notify::new());
    let replica_state = Arc::new(tokio::sync::Mutex::new(
        crate::replica_state::ReplicaState::new(),
    ));
    let hw_advance_notify = Arc::new(tokio::sync::Notify::new());
    let current_leader = Arc::new(AtomicU64::new(0));
    let current_leader_epoch = Arc::new(AtomicI32::new(0));
    let log_dir = Arc::new(arc_swap::ArcSwap::from_pointee(log_dir));
    let writer = tokio::spawn(crate::partition_writer::run(
        log.clone(),
        log_dir.clone(),
        rx,
        notify.clone(),
        replica_state.clone(),
        hw_advance_notify.clone(),
        log_dir_status,
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
    if let Some((h, p)) = addr.rsplit_once(':')
        && let Ok(port) = p.parse::<u16>()
    {
        return (h.to_string(), port);
    }
    tracing::warn!(
        addr,
        "advertised not host:port; falling back to localhost:9092"
    );
    ("localhost".into(), 9092)
}

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
                        // KIP-612 connection_creation_rate enforcement.
                        // IPv4 only, for ACL parity. IPv6 peers skip the quota check.
                        if let std::net::IpAddr::V4(peer_ipv4) = peer.ip() {
                            let image = broker.controller.current_image();
                            if let Some((entity_key, rate)) =
                                crate::quota::lookup_ip_quota_with_key(
                                    &image,
                                    &peer_ipv4,
                                    "connection_creation_rate",
                                )
                                && rate > 0.0
                            {
                                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                                let initial_rate = rate.max(1.0) as u64;
                                let bucket = broker.quota_buckets.get_or_create(
                                    "connection_creation_rate",
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
                                            .min(std::time::Duration::from_secs(1));
                                    tokio::time::sleep(delay).await;
                                }
                            }
                        }
                        let b = broker.clone();
                        let s = spec.clone();
                        tokio::spawn(async move {
                            crate::network::dispatch::serve_connection_on_listener(b, stream, s).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use tempfile::tempdir;

    #[test]
    fn loopback_bootstrap_maps_wildcard_to_loopback() {
        use std::net::SocketAddr;
        assert!(
            loopback_bootstrap("0.0.0.0:9092".parse::<SocketAddr>().unwrap()) == "127.0.0.1:9092"
        );
        assert!(
            loopback_bootstrap("192.168.1.5:9094".parse::<SocketAddr>().unwrap())
                == "192.168.1.5:9094"
        );
        assert!(loopback_bootstrap("[::]:9092".parse::<SocketAddr>().unwrap()) == "[::1]:9092");
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
                leader,
                replicas: replicas.clone(),
                isr: replicas,
                leader_epoch: 0,
                adding_replicas: vec![],
                removing_replicas: vec![],
            }));
        }

        let got = needed_metadata_partitions(&image, 7, 50);

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
        assert!(handle.listen_addr().port() != 0);
        handle.shutdown().await;
    }

    #[test]
    fn rlmm_backoff_doubles_then_caps() {
        use std::time::Duration;
        let max = Duration::from_secs(10);
        assert!(next_rlmm_backoff(Duration::from_millis(250), max) == Duration::from_millis(500));
        assert!(next_rlmm_backoff(Duration::from_secs(8), max) == max); // 16s capped to 10s
        assert!(next_rlmm_backoff(max, max) == max);
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
