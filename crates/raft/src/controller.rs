//! `Controller` is the public entry point. Owns the openraft node, the
//! state machine watcher, the controller listener task, and the
//! `submit_change` leader-aware forwarding logic.
//!
//! Slice-7 scope: a single static voter set is read from
//! `ControllerConfig`. Membership changes (adding / removing voters) and
//! snapshot replay are deferred to a later slice — `Controller::start`
//! refuses to call `Raft::initialize` if the log already has entries, so
//! restarting a node re-joins the existing quorum rather than re-seeding
//! it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crabka_metadata::{MetadataImage, MetadataRecord};

use crate::config::ControllerConfig;
use crate::error::RaftError;
use crate::log_store::RaftLogStore;
use crate::network::CrabkaRaftNetworkFactory;
use crate::server;
use crate::state_machine::CrabkaStateMachine;
use crate::types::{AppData, Node, NodeId, Raft};

/// Handle returned by [`Controller::start`]. Carries the live openraft
/// node, the state machine, a leader-id watcher, and the background task
/// join handles. Drop is NOT a clean shutdown — call [`Self::shutdown`]
/// to drain the listener + leader-pump tasks before the runtime is torn
/// down.
pub struct ControllerHandle {
    raft: Arc<Raft>,
    state_machine: Arc<CrabkaStateMachine>,
    leader: watch::Receiver<Option<NodeId>>,
    shutdown: CancellationToken,
    listener_task: Mutex<Option<JoinHandle<()>>>,
    leader_pump_task: Mutex<Option<JoinHandle<()>>>,
}

impl ControllerHandle {
    /// Current metadata snapshot (cheap; `Arc` clone).
    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.state_machine.current_image()
    }

    /// Subscribe to leader-id changes. The receiver's initial value is
    /// `None` until the first metrics tick arrives from openraft.
    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader.clone()
    }

    /// Submit a batch of metadata records.
    ///
    /// Returns `Ok(())` once the records are committed AND applied on
    /// this node. Pre-validates against the current image so we don't
    /// spam Raft with records the local state already rejects.
    ///
    /// On `ForwardToLeader`, retries up to three times with a 100ms
    /// backoff. After the third failure, surfaces the last reported
    /// leader id as `RaftError::NotLeader`. Any other openraft error is
    /// returned as `RaftError::Openraft` (debug-formatted; callers
    /// should treat it as a transient cluster fault).
    pub async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        // Pre-validation: cheap, local, and avoids waking openraft when
        // the change is doomed (e.g., duplicate topic).
        let image = self.state_machine.current_image();
        for r in &records {
            image.validate(r)?;
        }
        let data = AppData { records };

        let mut last_known_leader: Option<NodeId> = None;
        for attempt in 1u32..=3 {
            match self.raft.client_write(data.clone()).await {
                Ok(_) => return Ok(()),
                Err(openraft::error::RaftError::APIError(
                    openraft::error::ClientWriteError::ForwardToLeader(f),
                )) => {
                    last_known_leader = f.leader_id;
                    if attempt == 3 {
                        return Err(RaftError::NotLeader {
                            current_leader: last_known_leader,
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => return Err(RaftError::Openraft(format!("{e:?}"))),
            }
        }
        // Unreachable: the loop above always returns inside attempt == 3.
        Err(RaftError::NotLeader {
            current_leader: last_known_leader,
        })
    }

    /// Drain all background tasks and shut down the inner openraft node.
    /// Idempotent in practice — `CancellationToken::cancel` is, and the
    /// task join handles are taken under the mutex.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        // Stop the openraft engine first so the listener stops getting
        // fresh work; then drain the spawned tasks.
        let _ = self.raft.shutdown().await;
        if let Some(h) = self.listener_task.lock().await.take() {
            let _ = h.await;
        }
        if let Some(h) = self.leader_pump_task.lock().await.take() {
            let _ = h.await;
        }
    }
}

/// Zero-sized factory for [`ControllerHandle`]s. Kept as a unit struct
/// (rather than a free function) so callers and rustdoc can hang
/// trait-style documentation off a stable type name.
pub struct Controller;

impl Controller {
    /// Start an openraft node, open the controller listener, and begin
    /// participating in the quorum.
    ///
    /// On a fresh log (no prior entries), this node attempts to
    /// `Raft::initialize` with the static voter set from
    /// `ControllerConfig::voters`. In a multi-node cluster every node
    /// races to initialize; the losers see
    /// `InitializeError::NotAllowed` (or equivalent) and we log + ignore
    /// it, since the cluster is already seeded.
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, RaftError> {
        // 1. Log + state machine. The cluster UUID is `nil` for slice 7;
        //    a later slice will derive it from the first record applied
        //    so cross-node images compare equal.
        let log_store = Arc::new(RaftLogStore::open(config.log_dir.clone()).await?);
        let state_machine = Arc::new(CrabkaStateMachine::new(Uuid::nil()));

        // 2. openraft engine config. Times are millis; we widen the
        //    election window to `[t, 2t]` to keep the standard openraft
        //    jitter behavior.
        let election_min: u64 = u64::try_from(config.election_timeout.as_millis()).unwrap_or(1_000);
        let election_max = election_min.saturating_mul(2);
        let heartbeat: u64 = u64::try_from(config.heartbeat_interval.as_millis()).unwrap_or(200);
        let raft_config = openraft::Config {
            cluster_name: "crabka-metadata".to_string(),
            election_timeout_min: election_min,
            election_timeout_max: election_max,
            heartbeat_interval: heartbeat,
            install_snapshot_timeout: 5_000,
            ..Default::default()
        };

        // 3. Network factory. Sees each peer addr through the voter map
        //    surfaced to openraft via `Node` (the `addr` string lives in
        //    `BasicNode`).
        let network = CrabkaRaftNetworkFactory::new(config.client_id.clone());

        // 4. Spawn openraft. `Raft::new` consumes the log store and state
        //    machine; we keep our own `Arc` clones so the controller
        //    handle can read the image and probe the log on shutdown.
        let raft = Arc::new(
            openraft::Raft::new(
                config.node_id,
                Arc::new(raft_config),
                network,
                log_store.clone(),
                state_machine.clone(),
            )
            .await
            .map_err(|e| RaftError::Openraft(format!("{e:?}")))?,
        );

        // 5. First-boot bootstrap. Only attempt `initialize` when the log
        //    is empty — restarting a node that has already participated
        //    must NOT re-seed the cluster.
        if log_store.last_log_id().await.is_none() {
            let members: BTreeMap<NodeId, Node> = config
                .voters
                .iter()
                .map(|(id, addr)| {
                    (
                        *id,
                        openraft::BasicNode {
                            addr: addr.to_string(),
                        },
                    )
                })
                .collect();
            if let Err(e) = raft.initialize(members).await {
                // Every node tries to initialize; only one wins, and the
                // rest report some flavor of `NotAllowed` / "already
                // initialized". That's not a startup failure.
                warn!(error = ?e, "raft initialize returned error (likely already-initialized); continuing");
            }
        }

        // 6. Controller listener. Bind first so we surface a clear error
        //    if the port is taken, then hand it off to the accept loop.
        let listener = tokio::net::TcpListener::bind(config.controller_listen_addr)
            .await
            .map_err(|e| RaftError::Storage(crabka_log::LogError::Io(e)))?;
        let actual_addr = listener
            .local_addr()
            .map_err(|e| RaftError::Storage(crabka_log::LogError::Io(e)))?;
        let shutdown = CancellationToken::new();
        let listener_task = tokio::spawn(server::run(listener, raft.clone(), shutdown.clone()));
        info!(
            node_id = config.node_id,
            addr = %actual_addr,
            "controller started"
        );

        // 7. Leader-watch pump. Republish openraft's `current_leader`
        //    metric into a public `watch::Receiver<Option<NodeId>>` so
        //    callers don't need to depend on openraft directly.
        let (leader_tx, leader_rx) = watch::channel::<Option<NodeId>>(None);
        let mut metrics_rx = raft.metrics();
        let shutdown_for_pump = shutdown.clone();
        let leader_pump_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = shutdown_for_pump.cancelled() => break,
                    res = metrics_rx.changed() => {
                        if res.is_err() {
                            // Metrics sender dropped — engine is gone.
                            break;
                        }
                        let leader = metrics_rx.borrow().current_leader;
                        // `send` errors only when there are no receivers; that's fine.
                        let _ = leader_tx.send(leader);
                    }
                }
            }
        });

        Ok(ControllerHandle {
            raft,
            state_machine,
            leader: leader_rx,
            shutdown,
            listener_task: Mutex::new(Some(listener_task)),
            leader_pump_task: Mutex::new(Some(leader_pump_task)),
        })
    }
}
