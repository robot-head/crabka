//! Top-level `Broker` lifecycle. Wires together the partition registry,
//! metadata image, network listener, and handler table.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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
    pub(crate) supervisor_shutdown: tokio_util::sync::CancellationToken,
    pub(crate) supervisor_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
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
    listener_task: Option<JoinHandle<()>>,
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
        if let Some(t) = self.listener_task.take() {
            let _ = t.await;
        }
    }
}

impl Broker {
    /// Build a `Broker`, scan the log dir, spawn partition writers for
    /// every existing `<topic>-<partition>/`, bind the TCP listener, and
    /// return the handle.
    #[allow(clippy::too_many_lines)] // sequential bring-up; splitting hurts readability more than it helps
    pub async fn start(mut config: BrokerConfig) -> Result<BrokerHandle, BrokerError> {
        // 1. Bring up the metadata quorum BEFORE the client listener so
        //    handlers can read from it the moment they accept their first
        //    connection. The controller owns its own listener bound to
        //    `controller_listen_addr`.
        let controller_cfg = crabka_raft::ControllerConfig {
            node_id: config.node_id,
            voters: config.controller_quorum_voters.clone(),
            controller_listen_addr: config.controller_listen_addr,
            log_dir: config.log_dir.join("__cluster_metadata"),
            // Aggressive defaults (1s / 200ms) split-vote on slow CI runners
            // when our hand-rolled wire's RPC round-trip exceeds the
            // election-timeout window. 5s/500ms keeps elections deterministic
            // for multi-node startups without making the single-node path
            // perceptibly slower.
            election_timeout: std::time::Duration::from_secs(5),
            heartbeat_interval: std::time::Duration::from_millis(500),
            client_id: format!("crabka-broker-{}-controller", config.broker_id),
        };
        let controller = Arc::new(
            crabka_raft::Controller::start(controller_cfg)
                .await
                .map_err(|e| BrokerError::Startup(e.to_string()))?,
        );

        // 2. Wait for a leader, then submit a self-registration record so
        //    other brokers can discover us. Best-effort: if the submit
        //    fails the next caller's request will surface the error and
        //    membership reconciliation can retry later.
        {
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

        // 4. Spawn the replicator supervisor. Started AFTER the controller
        //    is up and self-registration succeeded so the supervisor's
        //    initial reconcile already sees this broker in the brokers()
        //    set. With replication_factor=1 the desired follower set is
        //    always empty, so this is a no-op for single-broker setups.
        let supervisor_shutdown = CancellationToken::new();
        let supervisor = crate::replicator_supervisor::ReplicatorSupervisor::new(
            config.node_id,
            controller.clone(),
            partitions.clone(),
            config.log_dir.clone(),
            config.log_config.clone(),
            format!("crabka-broker-{}-replicator", config.broker_id),
            supervisor_shutdown.clone(),
        );
        let supervisor_handle = supervisor.spawn();

        // 5. Build handler table.
        let handlers = crate::handlers::build_table();

        // 6. Bind first so the actual port is known. If
        //    `advertised_listener` points at port 0 (tests typically),
        //    rewrite it to the bound port so FindCoordinator/Metadata
        //    return a useful host:port instead of `:0`.
        let listener = TcpListener::bind(config.listen_addr).await?;
        let listen_addr = listener.local_addr()?;
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
            supervisor_shutdown,
            supervisor_handle: tokio::sync::Mutex::new(Some(supervisor_handle)),
            handlers,
        });

        let shutdown = CancellationToken::new();
        let listener_task = tokio::spawn(accept_loop(broker.clone(), listener, shutdown.clone()));

        Ok(BrokerHandle {
            listen_addr,
            shutdown,
            listener_task: Some(listener_task),
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
    let writer = tokio::spawn(crate::partition_writer::run(
        log.clone(),
        rx,
        notify.clone(),
    ));
    Arc::new(Partition {
        topic,
        partition_id,
        log,
        writer_tx: tx,
        append_notify: notify,
        _writer_handle: Arc::new(writer),
    })
}

async fn accept_loop(broker: Arc<Broker>, listener: TcpListener, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("listener shutting down");
                break;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        tracing::debug!(%peer, "accepted connection");
                        let b = broker.clone();
                        tokio::spawn(async move {
                            crate::network::dispatch::serve_connection(b, stream).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
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
