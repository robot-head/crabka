//! `Controller` is the public entry point. Owns the openraft node, the
//! state machine watcher, the controller listener task, and the
//! `submit_change` leader-aware forwarding logic.
//!
//! Cluster formation is driven by `BootstrapMode`: one broker boots as
//! the singleton voter (`Bootstrap`), remaining fresh brokers skip
//! `initialize` (`Join`), and restarted brokers replay their on-disk log
//! (`Rejoin`). Snapshot replay is deferred to a later slice.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

use crabka_metadata::{MetadataImage, MetadataRecord};

use crate::config::{BootstrapMode, ControllerConfig};
use crate::error::RaftError;
use crate::log_store::RaftLogStore;
use crate::network::{CrabkaRaftNetworkFactory, OutboundDialer, PlaintextDialer};
use crate::server;
use crate::state_machine::CrabkaStateMachine;
use crate::types::{AppData, Node, NodeId, Raft};

/// Crabka-native view of the openraft node's current quorum state.
/// Surfaced by [`ControllerHandle::quorum_state`] for the broker's
/// `DescribeQuorum` admin handler so callers don't have to depend on
/// openraft types directly.
#[derive(Debug, Clone)]
pub struct QuorumState {
    /// Raft term — used as the `KRaft` `leader_epoch` on the wire.
    pub current_term: u64,
    /// Index of the last log entry applied to this node's state machine.
    /// Used as `KRaft` `high_watermark` on the wire. `0` until the first
    /// commit (the same value openraft treats as "no log applied yet").
    pub last_applied_index: u64,
    /// Current cluster leader. `None` mid-election.
    pub current_leader: Option<NodeId>,
    /// Voter ids in the current membership config.
    pub voters: Vec<NodeId>,
    /// Per-voter `matched` log index from openraft's `replication` map.
    /// Populated ONLY on the leader — openraft only knows peers'
    /// progress when this node is acknowledging their `AppendEntries`
    /// replies. On followers (and during elections) the map is empty
    /// and callers should fall back to the JVM `-1` ("Unknown")
    /// sentinel for each voter.
    pub per_voter_matched_index: BTreeMap<NodeId, u64>,
}

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
    /// Static voter map cloned from `ControllerConfig::voters`. Used by
    /// `submit_change` to forward writes to the current leader when the
    /// local node is a follower — the local openraft instance returns
    /// `ForwardToLeader` and we dial the leader's controller listener
    /// directly via the slice-7 `API_KEY_SUBMIT_CHANGE` RPC.
    voters: Vec<(NodeId, SocketAddr)>,
    client_id: String,
    /// Outbound dialer cloned from the factory at construction time.
    /// `forward_submit_to` uses it to reach the leader's controller
    /// listener with the same TLS / SASL handshake that openraft's
    /// `AppendEntries` / `Vote` RPCs ride on top of (slice 12). For the
    /// legacy PLAINTEXT path the broker doesn't inject a dialer, and
    /// `Controller::start` substitutes `PlaintextDialer` — equivalent
    /// to a bare `Connection::connect`.
    dialer: Arc<dyn OutboundDialer>,
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

    /// Subscribe to metadata-image changes. The receiver holds the
    /// current image immediately; callers use
    /// `rx.borrow()` + `rx.changed().await` to track updates.
    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.state_machine.watch_image()
    }

    /// Snapshot the openraft node's current quorum state in
    /// Crabka-native terms. Used by the broker's `DescribeQuorum`
    /// (`api_key=55`, KIP-595) handler to fill `leader_epoch`,
    /// `high_watermark`, and per-voter `log_end_offset` for
    /// `kafka-metadata-quorum --describe`. Cheap — a clone of
    /// openraft's `RaftMetrics` watch value.
    #[must_use]
    pub fn quorum_state(&self) -> QuorumState {
        let m = self.raft.metrics().borrow().clone();
        let voters: Vec<NodeId> = m.membership_config.membership().voter_ids().collect();
        // openraft populates `replication` only on the current leader;
        // on a follower the map is empty and per-voter `log_end_offset`
        // stays at the `Unknown` sentinel for each peer.
        let per_voter_matched_index: BTreeMap<NodeId, u64> = m
            .replication
            .as_ref()
            .map(|repl| {
                repl.iter()
                    .map(|(nid, log_id_opt)| (*nid, log_id_opt.as_ref().map_or(0, |lid| lid.index)))
                    .collect()
            })
            .unwrap_or_default();
        QuorumState {
            current_term: m.current_term,
            last_applied_index: m.last_applied.as_ref().map_or(0, |lid| lid.index),
            current_leader: m.current_leader,
            voters,
            per_voter_matched_index,
        }
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
        // the change is doomed (e.g., duplicate topic). Validate against a
        // local clone that we apply records into as we go — otherwise a
        // batch containing `V1Topic(t)` followed by `V1Partition{topic: t}`
        // would reject the partition because the topic isn't yet in the
        // committed image.
        let mut scratch: MetadataImage = (*self.state_machine.current_image()).clone();
        for r in &records {
            scratch.validate(r)?;
            scratch.apply(r);
        }
        let data = AppData { records };

        let mut last_known_leader: Option<NodeId> = None;
        for attempt in 1u32..=3 {
            match self.raft.client_write(data.clone()).await {
                Ok(resp) => {
                    // The leader's state machine re-validates each record
                    // at apply time; the first rejection is the canonical
                    // error for the caller. `MetadataError` doesn't
                    // derive `serde`, so we carry the rendered string
                    // through `AppDataResponse` and reconstruct here.
                    // The "topic '<name>' already exists" prefix is the
                    // only signal we need for slice 7's
                    // `TopicExists`-vs-`InvalidRecord` discrimination.
                    if let Some(msg) = resp.data.rejected.into_iter().next() {
                        let err = if let Some(rest) = msg.strip_prefix("topic '")
                            && let Some(name) = rest.strip_suffix("' already exists")
                        {
                            crabka_metadata::MetadataError::TopicExists(name.to_string())
                        } else {
                            crabka_metadata::MetadataError::InvalidRecord("validation failed")
                        };
                        return Err(RaftError::Metadata(err));
                    }
                    return Ok(());
                }
                Err(openraft::error::RaftError::APIError(
                    openraft::error::ClientWriteError::ForwardToLeader(f),
                )) => {
                    last_known_leader = f.leader_id;
                    // If openraft tells us who the leader is and it isn't
                    // us, forward the change directly to the leader's
                    // controller listener via the slice-7
                    // `API_KEY_SUBMIT_CHANGE` RPC. Otherwise (transient
                    // `leader_id: None` during election) fall through to
                    // the retry loop.
                    if let Some(leader) = last_known_leader
                        && let Some(addr) = self.voter_addr(leader)
                    {
                        return self.forward_submit_to(leader, addr, &data.records).await;
                    }
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

    /// Mutate the openraft voter set. `new_voters` is the **complete** desired set
    /// (not a delta). Any voter in the current set but not in `new_voters` is
    /// removed entirely (`retain=false`). Any voter in `new_voters` that isn't
    /// already in the cluster must have been registered via [`Self::add_learner`]
    /// first — openraft refuses to promote unknown ids.
    ///
    /// Two-phase joint config: openraft commits a joint membership log entry
    /// (old ∪ new), then a uniform log entry (new only). If the leader crashes
    /// between the two, the cluster is left in joint config and a future call
    /// completes the transition.
    ///
    /// # Errors
    ///
    /// - `RaftError::NotLeader` if this node isn't the openraft leader.
    /// - `RaftError::ChangeRejected` if openraft rejects (e.g., the new voter set
    ///   would leave the cluster without quorum, or a promoted node isn't a learner).
    /// - `RaftError::Shutdown` if the raft engine has been shut down.
    pub async fn change_membership(
        &self,
        new_voters: std::collections::BTreeSet<NodeId>,
    ) -> Result<(), RaftError> {
        use openraft::error::ClientWriteError;
        use openraft::error::RaftError as ORE;
        match self.raft.change_membership(new_voters, false).await {
            Ok(_) => Ok(()),
            Err(ORE::APIError(ClientWriteError::ForwardToLeader(f))) => Err(RaftError::NotLeader {
                current_leader: f.leader_id,
            }),
            Err(ORE::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(RaftError::ChangeRejected(format!("{e:?}")))
            }
            Err(e) => Err(RaftError::Openraft(format!("{e:?}"))),
        }
    }

    /// Register a non-voting raft learner at `addr` with id `node_id`. Blocks
    /// until the leader has replicated up to its current commit index to the
    /// new node (so a subsequent [`Self::change_membership`] promotion won't
    /// stall waiting for catch-up). Pair with [`Self::change_membership`] to
    /// turn a learner into a voter:
    ///
    /// ```ignore
    /// controller.add_learner(4, "127.0.0.1:9094".parse().unwrap()).await?;
    /// controller.change_membership([1, 2, 3, 4].into_iter().collect()).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// - `RaftError::NotLeader` if this node isn't the openraft leader.
    /// - `RaftError::ChangeRejected` if openraft rejects (e.g., the learner
    ///   never catches up within openraft's internal deadline).
    /// - `RaftError::Shutdown` if the raft engine has been shut down.
    pub async fn add_learner(&self, node_id: NodeId, addr: SocketAddr) -> Result<(), RaftError> {
        use openraft::error::ClientWriteError;
        use openraft::error::RaftError as ORE;
        let node = openraft::BasicNode {
            addr: addr.to_string(),
        };
        match self.raft.add_learner(node_id, node, true).await {
            Ok(_) => Ok(()),
            Err(ORE::APIError(ClientWriteError::ForwardToLeader(f))) => Err(RaftError::NotLeader {
                current_leader: f.leader_id,
            }),
            Err(ORE::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(RaftError::ChangeRejected(format!("{e:?}")))
            }
            Err(e) => Err(RaftError::Openraft(format!("{e:?}"))),
        }
    }

    fn voter_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.voters
            .iter()
            .find_map(|(id, addr)| (*id == node_id).then_some(*addr))
    }

    /// Open a one-shot authenticated connection to the leader's
    /// controller listener, send a bincode-encoded `Vec<MetadataRecord>`
    /// as `API_KEY_SUBMIT_CHANGE`, and translate the response back into a
    /// `RaftError`.
    ///
    /// Routes through [`OutboundDialer::dial`] so the same TLS / SASL
    /// handshake openraft's `AppendEntries` / `Vote` RPCs ride on top
    /// of (slice 12) applies here too. For the legacy PLAINTEXT path,
    /// `Controller::start` substitutes `PlaintextDialer`, which is
    /// byte-equivalent to a bare `Connection::connect`.
    ///
    /// A fresh connection per call mirrors the pre-slice-12b raw
    /// `TcpStream::connect` behaviour — `submit_change` forwarding is
    /// rare (only on follower-side writes) and reusing the openraft
    /// network factory's cache from here would complicate ownership for
    /// negligible gain.
    async fn forward_submit_to(
        &self,
        leader: NodeId,
        addr: SocketAddr,
        records: &[crabka_metadata::MetadataRecord],
    ) -> Result<(), RaftError> {
        let body_bytes = <serde_wincode::SerdeCompat<Vec<crabka_metadata::MetadataRecord>> as wincode::Serialize>::serialize(
            &records.to_vec(),
        )
        .map_err(crate::error::RaftError::from)?;
        let payload = crate::wire::CrabkaSubmitChangeRequest {
            records: bytes::Bytes::from(body_bytes),
        };
        let mut body = Vec::with_capacity(payload.records.len() + 4);
        payload.encode_v0(&mut body)?;

        // Dial through the injected dialer so SASL / TLS terminates
        // before the first raft frame leaves this host. The dialer
        // returns a `Connection` that has already negotiated
        // `ApiVersions` and (when applicable) completed SASL auth.
        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            ..crabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(leader, &addr.to_string(), opts)
            .await
            .map_err(RaftError::Network)?;

        // `raw_request` builds the v2 RequestHeader, writes the frame,
        // reads the response, and strips the v1 ResponseHeader's
        // leading tagged-fields byte. The server in `server.rs` returns
        // a v1 ResponseHeader for every Crabka-private api key, so the
        // returned bytes are the bare `CrabkaSubmitChangeResponse` body.
        let resp_body = conn
            .raw_request(
                crate::wire::API_KEY_SUBMIT_CHANGE,
                0,
                bytes::Bytes::from(body),
            )
            .await
            .map_err(RaftError::Network)?;
        conn.close();

        let mut cur: &[u8] = &resp_body;
        let resp = crate::wire::CrabkaSubmitChangeResponse::decode_v0(&mut cur)?;
        match resp.error_code {
            0 => Ok(()),
            // `error_code = 2` => leader rejected at apply-time. Slice 7
            // collapses the typed `MetadataError` into a generic
            // `TopicExists` here since the wire only carries an error
            // code; the topic name is what the caller had in hand.
            2 => Err(RaftError::Metadata(
                crabka_metadata::MetadataError::TopicExists(String::new()),
            )),
            // `error_code = 1` (not leader) and `3` (other) collapse to
            // `NotLeader` — the test of record (CreateTopics) maps that
            // to `NOT_CONTROLLER`, which the client treats as retryable.
            _ => Err(RaftError::NotLeader {
                current_leader: (resp.leader_hint >= 0)
                    .then(|| u64::try_from(resp.leader_hint).unwrap_or(leader)),
            }),
        }
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

    /// Stop the openraft engine and cancel the controller listener without
    /// consuming `self`. Used by `BrokerHandle::shutdown` where the
    /// controller is behind an `Arc` and cannot be moved out.
    ///
    /// Idempotent — calling multiple times is safe (cancellation token and
    /// `raft.shutdown()` are both idempotent).
    ///
    /// Awaits the listener task so that the OS port is guaranteed to be
    /// released before this method returns. This is important for tests
    /// that immediately rebind the same port after `BrokerHandle::shutdown`.
    pub async fn cancel(&self) {
        self.shutdown.cancel();
        let _ = self.raft.shutdown().await;
        // Drain the listener task so its `TcpListener` is dropped (and the
        // port is released by the OS) before we return.
        if let Some(h) = self.listener_task.lock().await.take() {
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
    /// Cluster formation is governed by [`ControllerConfig::bootstrap_mode`]:
    /// `Bootstrap` initializes a singleton-voter cluster on an empty log;
    /// `Join` skips initialize and waits for an external `add_learner`;
    /// `Rejoin` skips initialize and relies on the on-disk raft log.
    /// Mismatches between mode and log state return `RaftError::Startup`.
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, RaftError> {
        Self::start_with_listener(config, None).await
    }

    /// Like [`Self::start`], but adopts a caller-supplied, already-bound
    /// controller listener instead of binding `controller_listen_addr`
    /// itself.
    ///
    /// Test harnesses use this to defeat the bind-and-drop TOCTOU race:
    /// the test binds an ephemeral port, hands the live `TcpListener`
    /// here (never dropping it), so no other process can claim the port
    /// in the gap between probe and bind. The supplied listener's local
    /// address MUST equal `config.controller_listen_addr` — the bootstrap
    /// membership record and the voter map are built from the config
    /// value, so a mismatch would advertise an unreachable dial address.
    #[allow(clippy::too_many_lines)]
    pub async fn start_with_listener(
        config: ControllerConfig,
        prebound: Option<tokio::net::TcpListener>,
    ) -> Result<ControllerHandle, RaftError> {
        // 1. Log + state machine. The cluster UUID is injected from the
        //    operator (via `BrokerConfig::cluster_id`) so every broker in
        //    the same `KafkaCluster` reports a matching `MetadataImage`
        //    cluster id. Falls back to `Uuid::nil()` for legacy
        //    single-node tests that don't set it.
        let log_store = Arc::new(RaftLogStore::open(config.log_dir.clone()).await?);
        let state_machine = Arc::new(CrabkaStateMachine::new(
            config.cluster_id.unwrap_or_else(Uuid::nil),
        ));

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
        // Use the injected dialer if the broker provided one (slice-12
        // inter-broker TLS / SASL), otherwise fall back to plain
        // `TcpStream::connect`. This keeps every existing PLAINTEXT-only
        // test path identical to slice 11.
        let dialer: Arc<dyn OutboundDialer> = config
            .dialer
            .clone()
            .unwrap_or_else(|| Arc::new(PlaintextDialer));
        let network = CrabkaRaftNetworkFactory::new(config.client_id.clone(), dialer.clone());

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

        // 5. First-boot orchestration. The bootstrap_mode tells us which
        //    role this broker plays in cluster formation. Misuse is fatal —
        //    a Bootstrap on top of an existing log would re-seed the
        //    cluster, and a Rejoin on an empty log would never converge.
        let log_is_empty = log_store.last_log_id().await.is_none();
        match (config.bootstrap_mode, log_is_empty) {
            (BootstrapMode::Bootstrap, true) => {
                // Singleton-voter init. We become leader on our first
                // election timeout, no contention, no split-vote.
                let self_node = openraft::BasicNode {
                    addr: config.controller_listen_addr.to_string(),
                };
                let members: BTreeMap<NodeId, Node> =
                    [(config.node_id, self_node)].into_iter().collect();
                raft.initialize(members)
                    .await
                    .map_err(|e| RaftError::Openraft(format!("bootstrap initialize: {e:?}")))?;
            }
            (BootstrapMode::Join, true) | (BootstrapMode::Rejoin, false) => {
                // Join: don't initialize — engine waits in Learner state
                //   until the bootstrap broker's add_learner arrives.
                // Rejoin: existing log carries membership; openraft
                //   replayed it during Raft::new. Nothing to do.
            }
            (BootstrapMode::Bootstrap, false) => {
                return Err(RaftError::Startup(
                    "Bootstrap mode requires empty raft log; existing log indicates an already-initialized broker — use Rejoin".into(),
                ));
            }
            (BootstrapMode::Rejoin, true) => {
                return Err(RaftError::Startup(
                    "Rejoin mode requires non-empty raft log; this broker has no on-disk state — use Bootstrap or Join".into(),
                ));
            }
            (BootstrapMode::Join, false) => {
                return Err(RaftError::Startup(
                    "Join mode requires empty raft log; this broker has on-disk state — use Rejoin"
                        .into(),
                ));
            }
        }

        // 6. Controller listener. Adopt the caller-supplied listener when
        //    present (test harness handoff); otherwise bind here so we
        //    surface a clear error if the port is taken, then hand it off
        //    to the accept loop.
        let listener = match prebound {
            Some(l) => l,
            None => tokio::net::TcpListener::bind(config.controller_listen_addr)
                .await
                .map_err(|e| RaftError::Storage(crabka_log::LogError::Io(e)))?,
        };
        let actual_addr = listener
            .local_addr()
            .map_err(|e| RaftError::Storage(crabka_log::LogError::Io(e)))?;
        let shutdown = CancellationToken::new();
        let listener_task = tokio::spawn(server::run(
            listener,
            raft.clone(),
            shutdown.clone(),
            config.handshake.clone(),
        ));
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
            voters: config.voters.clone(),
            client_id: config.client_id.clone(),
            dialer,
        })
    }
}

#[cfg(test)]
mod bootstrap_mode_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn bootstrap_on_non_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        // First boot: bootstrap fresh.
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("first bootstrap ok");
        ctrl.shutdown().await;

        // Second boot: log is non-empty, Bootstrap must error.
        let cfg2 = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        match Controller::start(cfg2).await {
            Err(err) => assert!(
                matches!(err, RaftError::Startup(_)),
                "Bootstrap on existing log must return Startup; got: {err:?}"
            ),
            Ok(ctrl) => {
                ctrl.shutdown().await;
                panic!("Bootstrap on existing log must error but succeeded");
            }
        }
    }

    #[tokio::test]
    async fn rejoin_on_empty_log_errors() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Rejoin,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        match Controller::start(cfg).await {
            Err(err) => assert!(
                matches!(err, RaftError::Startup(_)),
                "Rejoin on empty log must return Startup; got: {err:?}"
            ),
            Ok(ctrl) => {
                ctrl.shutdown().await;
                panic!("Rejoin on empty log must error but succeeded");
            }
        }
    }

    #[tokio::test]
    async fn join_on_empty_log_starts_in_learner_state() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Join,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg)
            .await
            .expect("Join on empty log starts ok");
        // Without an external add_learner the watch_leader stays None.
        assert!(ctrl.watch_leader().borrow().is_none());
        ctrl.shutdown().await;
    }
}
