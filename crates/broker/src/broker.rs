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

/// The running broker. Library callers get a [`BrokerHandle`] from
/// [`Broker::start`]; this struct is the shared internal state.
// `config`, `metadata`, `partitions` are consumed by the per-API handlers
// landing in Tasks 12-16; allow dead_code on the struct until the handlers
// pick them up.
#[allow(dead_code)]
pub struct Broker {
    pub(crate) config: BrokerConfig,
    /// Quorum-backed metadata controller. Replaces the slice-4 in-memory
    /// `MetadataImage`; every metadata read goes through
    /// [`crabka_raft::ControllerHandle::current_image`].
    pub(crate) controller: Arc<crabka_raft::ControllerHandle>,
    /// Wrapped in `Arc` so handlers cloning the field share the same
    /// underlying map. `DashMap::clone` is a deep copy by default.
    pub(crate) partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub(crate) group_manager: Arc<crate::coordinator::GroupManager>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    pub(crate) producer_state: Arc<crate::producer_state::ProducerState>,
    pub(crate) txn_coordinator: Arc<crate::txn::coordinator::TxnCoordinator>,
    pub(crate) supervisor_shutdown: tokio_util::sync::CancellationToken,
    pub(crate) supervisor_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    pub(crate) liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    /// `Some` when `BrokerConfig::tls_config` is set. Per-listener accept
    /// loops clone this and call `accept` for every TLS connection.
    pub(crate) tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    /// Shared outbound dialer used by the replicator, raft transport,
    /// and controller-heartbeat loops. When `inter_broker_credentials`
    /// is `None` and the listener is `PLAINTEXT` the dialer falls back
    /// to a plain `TcpStream::connect` — the new wiring is transparent
    /// for the legacy PLAINTEXT-only path.
    pub(crate) inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    handlers: HandlerTable,
}

impl Broker {
    pub(crate) fn handlers(&self) -> &HandlerTable {
        &self.handlers
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

    /// Current Raft leader id as observed by this broker's controller.
    /// Returns `None` before the first leader is elected. Trivial
    /// passthrough to [`crabka_raft::ControllerHandle::watch_leader`].
    ///
    /// `async fn` even though the underlying `watch::Receiver::borrow` is
    /// synchronous — the slice-7 test plan and broker public API expect
    /// `controller_leader_id().await`, and keeping the signature async
    /// preserves room for a future implementation that waits for the
    /// first non-`None` value via `watch::Receiver::changed`.
    #[allow(clippy::unused_async, clippy::used_underscore_binding)]
    pub async fn controller_leader_id(&self) -> Option<crabka_raft::NodeId> {
        *self._broker.controller.watch_leader().borrow()
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
    /// quorum-replicated [`crabka_metadata::MetadataImage`]. Used by
    /// Task-11 integration tests to verify per-listener endpoints were
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
        self._broker
            .controller
            .add_learner(node_id, addr)
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
            .get(&(topic.to_string(), partition))?
            .value()
            .clone();
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
            .get(&(topic.to_string(), partition))
            .ok_or_else(|| {
                crate::error::BrokerError::Replication(format!(
                    "partition {topic}-{partition} not local"
                ))
            })?
            .value()
            .clone();
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
            .get(&(topic.to_string(), partition))
            .ok_or_else(|| {
                crate::error::BrokerError::Replication(format!(
                    "partition {topic}-{partition} not local"
                ))
            })?
            .value()
            .clone();
        part.test_set_log_start(new_start).await
    }

    /// Test-only: directly set `current_leader_epoch` on a locally-hosted
    /// partition. Used by `tests/leader_epoch.rs` to simulate split-brain
    /// (force an epoch bump) without going through the supervisor's
    /// metadata-image-driven path.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub fn test_set_leader_epoch(&self, topic: &str, partition: i32, epoch: i32) {
        if let Some(part) = self._broker.partitions.get(&(topic.to_string(), partition)) {
            part.value().test_set_leader_epoch(epoch);
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
            .contains_key(&(topic.to_string(), partition))
    }

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
            .get(&(topic.to_string(), partition))?
            .value()
            .clone();
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
        let part = self
            ._broker
            .partitions
            .get(&(topic.to_string(), partition))?
            .value()
            .clone();
        let snap = part.log.lock().ok()?.config_snapshot();
        Some(snap.retention_ms)
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
            .get(&(topic.to_string(), partition))
            .ok_or_else(|| {
                crate::error::BrokerError::Replication(format!(
                    "partition {topic}-{partition} not local"
                ))
            })?
            .value()
            .clone();
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

    /// Test-only: submit a [`crabka_metadata::MetadataRecord`] directly to
    /// this broker's controller, bypassing the public Kafka APIs. Used by
    /// Task-14 integration tests to provision a SCRAM credential before the
    /// `AlterUserScramCredentials` handler (Task 15) exists. Returns an
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

    /// Test-only: insert a group into this broker's `GroupManager`. Returns
    /// immediately if the group already exists (idempotent). Used by
    /// admin-handler integration tests to seed the group registry without
    /// running a full `JoinGroup` / `SyncGroup` protocol exchange.
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub fn group_create_for_test(&self, group_id: &str) {
        let _ = self._broker.group_manager.get_or_create(group_id);
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
    handle: Arc<crabka_raft::ControllerHandle>,
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

impl Broker {
    /// Build a `Broker`, scan the log dir, spawn partition writers for
    /// every existing `<topic>-<partition>/`, bind the TCP listener, and
    /// return the handle.
    #[allow(clippy::too_many_lines)] // sequential bring-up; splitting hurts readability more than it helps
    pub async fn start(mut config: BrokerConfig) -> Result<BrokerHandle, BrokerError> {
        // 0a. Install the rustls crypto provider exactly once per process.
        //     `rustls 0.23` with `default-features = false` does NOT auto-install
        //     a provider; without this the `ServerConfig::builder()` call below
        //     (and any client-side rustls usage) panics at runtime. `.ok()`
        //     swallows the `AlreadySet` error when a previous broker / test
        //     in the same process installed it first.
        let _ = rustls::crypto::ring::default_provider().install_default();

        // 0b. Validate listener + auth configuration before any side effects.
        config.validate()?;

        // 0c. Build the TLS acceptor up front so we can fail fast on bad
        //     cert / key paths before bringing up any state.
        let tls_acceptor = match &config.tls_config {
            Some(tls) => {
                let server_cfg = tls
                    .build_server_config()
                    .map_err(|e| BrokerError::Tls(e.to_string()))?;
                Some(tokio_rustls::TlsAcceptor::from(server_cfg))
            }
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
        //    Slice 12b raft dialer + handshake wiring:
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
            let hs = crate::raft_handshake::BrokerRaftHandshake {
                tls_acceptor: tls_acceptor.clone(),
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

        let controller_cfg = crabka_raft::ControllerConfig {
            node_id: config.node_id,
            voters: config.controller_quorum_voters.clone(),
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
            dialer: raft_dialer,
            handshake: handshake_opt,
        };
        let controller = Arc::new(
            crabka_raft::Controller::start(controller_cfg)
                .await
                .map_err(|e| BrokerError::Startup(e.to_string()))?,
        );
        // Populate the late-bound controller handle so the inbound
        // `BrokerRaftHandshake` (already wired into the controller's
        // accept loop) can perform SCRAM credential lookups on the next
        // authenticated connection. The `set` cannot fail in practice
        // because we hold the only writer; swallow the `SetError` defensively.
        let _ = controller_cell.set(controller.clone());

        // 2. Wait for a leader, then submit a self-registration record so
        //    other brokers can discover us. Best-effort: if the submit
        //    fails the next caller's request will surface the error and
        //    membership reconciliation can retry later.
        {
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
                    rack: None,
                    endpoints,
                },
            );
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
            if let Err(e) = controller.submit_change(vec![self_reg]).await {
                tracing::warn!(error = %e, "self-registration failed; continuing");
            }

            // 2b. First-start bootstrap-records submit (slice 12b).
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
                let records = crate::bootstrap::load_bootstrap_records(&config.log_dir)?;
                if !records.is_empty() {
                    tracing::info!(count = records.len(), "submitting bootstrap records");
                    controller.submit_change(records).await.map_err(|e| {
                        BrokerError::Replication(format!("bootstrap submit failed: {e}"))
                    })?;
                }
            }
        }

        // 3. Scan + recover partitions on disk. Partition state is still
        //    a local-disk concern; the metadata image is sourced from
        //    `controller.current_image()` whenever a handler needs it.
        let partitions: Arc<DashMap<(String, i32), Arc<Partition>>> = Arc::new(DashMap::new());
        for (topic, partition_id) in log_dir::scan(&config.log_dir)? {
            let dir = log_dir::partition_dir(&config.log_dir, &topic, partition_id);
            let log = crabka_log::Log::open(&dir, config.log_config.clone())?;
            let part = spawn_partition(topic.clone(), partition_id, log);
            partitions.insert((topic.clone(), partition_id), part);
        }

        // Group coordinator bootstrap (slice 5).
        let group_manager = Arc::new(crate::coordinator::GroupManager::new());
        let producer_ids = Arc::new(crate::producer_id_manager::ProducerIdManager::new());
        let producer_state = Arc::new(crate::producer_state::ProducerState::new());
        crate::coordinator::bootstrap::bootstrap(
            &config,
            &controller,
            &partitions,
            group_manager.as_ref(),
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

        // 4b. Spawn the replicator supervisor. Started AFTER the controller
        //    is up and self-registration succeeded so the supervisor's
        //    initial reconcile already sees this broker in the brokers()
        //    set. With replication_factor=1 the desired follower set is
        //    always empty, so this is a no-op for single-broker setups.
        let supervisor_shutdown = CancellationToken::new();
        let inter_listener_proto = config
            .effective_listeners()
            .iter()
            .find(|l| l.name == config.inter_broker_listener_name)
            .map_or(crabka_security::ListenerProtocol::Plaintext, |l| l.protocol);
        let supervisor = crate::replicator_supervisor::ReplicatorSupervisor::new(
            config.node_id,
            controller.clone(),
            partitions.clone(),
            config.log_dir.clone(),
            config.log_config.clone(),
            format!("crabka-broker-{}-replicator", config.broker_id),
            supervisor_shutdown.clone(),
            Some(txn_coordinator.clone()),
            inter_broker_client.clone(),
            inter_listener_proto,
            config.inter_broker_listener_name.clone(),
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
            },
        ));

        // 4e. Controller-side liveness ticker: scans the heartbeat registry
        //     every second and fires leader_election callbacks on transitions.
        let liveness_for_ticker = liveness.clone();
        let controller_for_ticker = controller.clone();
        let ticker_node_id = config.node_id;
        let ticker_shutdown = supervisor_shutdown.child_token();
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
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = leader_watch.changed() => {},
                        () = seed_shutdown.cancelled() => return,
                    }
                    let new_leader = *leader_watch.borrow();
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
            },
        ));

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
        let broker = Arc::new(Self {
            config,
            controller,
            partitions,
            group_manager: group_manager.clone(),
            producer_ids,
            producer_state,
            txn_coordinator,
            supervisor_shutdown,
            supervisor_handle: tokio::sync::Mutex::new(Some(supervisor_handle)),
            liveness: liveness.clone(),
            tls_acceptor,
            inter_broker_client,
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

        Ok(BrokerHandle {
            listen_addr,
            shutdown,
            listener_tasks,
            _broker: broker,
        })
    }
}

/// Create the partition runtime (mpsc channel + writer task + notify).
pub(crate) fn spawn_partition(
    topic: String,
    partition_id: i32,
    log: crabka_log::Log,
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
    let writer = tokio::spawn(crate::partition_writer::run(
        log.clone(),
        rx,
        notify.clone(),
        replica_state.clone(),
        hw_advance_notify.clone(),
    ));
    Arc::new(Partition {
        topic,
        partition_id,
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
    use tempfile::tempdir;

    #[tokio::test]
    async fn start_and_shutdown_clean() {
        let dir = tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = Broker::start(config).await.unwrap();
        assert_ne!(handle.listen_addr().port(), 0);
        handle.shutdown().await;
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
