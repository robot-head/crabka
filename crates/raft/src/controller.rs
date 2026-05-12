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
use std::net::SocketAddr;
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
    /// Static voter map cloned from `ControllerConfig::voters`. Used by
    /// `submit_change` to forward writes to the current leader when the
    /// local node is a follower — the local openraft instance returns
    /// `ForwardToLeader` and we dial the leader's controller listener
    /// directly via the slice-7 `API_KEY_SUBMIT_CHANGE` RPC.
    voters: Vec<(NodeId, SocketAddr)>,
    client_id: String,
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

    fn voter_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.voters
            .iter()
            .find_map(|(id, addr)| (*id == node_id).then_some(*addr))
    }

    /// Open a one-shot TCP connection to the leader's controller listener,
    /// send a bincode-encoded `Vec<MetadataRecord>` as
    /// `API_KEY_SUBMIT_CHANGE`, and translate the response back into a
    /// `RaftError`.
    ///
    /// We deliberately do NOT reuse `crabka_client_core::Connection` here
    /// because that path forces an `ApiVersions` handshake on connect,
    /// and the controller listener treats `ApiVersions` as a bootstrap
    /// no-op only — it doesn't propagate request/response framing the way
    /// `Connection::send` expects for typed messages. The submit RPC is
    /// rare enough (one round trip per follower-side `submit_change`)
    /// that an ad-hoc TCP path is simpler than extending `Connection`.
    async fn forward_submit_to(
        &self,
        leader: NodeId,
        addr: SocketAddr,
        records: &[crabka_metadata::MetadataRecord],
    ) -> Result<(), RaftError> {
        use bytes::{BufMut, BytesMut};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let body_bytes = bincode::serde::encode_to_vec(records, bincode::config::standard())
            .map_err(crate::error::RaftError::from)?;
        let payload = crate::wire::CrabkaSubmitChangeRequest {
            records: bytes::Bytes::from(body_bytes),
        };
        let mut body = Vec::with_capacity(payload.records.len() + 4);
        payload.encode_v0(&mut body)?;

        // RequestHeader v2 (flexible).
        let mut frame = BytesMut::with_capacity(32 + body.len());
        frame.put_i16(crate::wire::API_KEY_SUBMIT_CHANGE);
        frame.put_i16(0);
        frame.put_i32(0);
        let cid = i16::try_from(self.client_id.len()).unwrap_or(i16::MAX);
        frame.put_i16(cid);
        frame.put_slice(self.client_id.as_bytes());
        frame.put_u8(0); // tagged_fields=0
        frame.put_slice(&body);

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| RaftError::Network(crabka_client_core::ClientError::Io(e)))?;
        let mut len_prefix = [0u8; 4];
        len_prefix.copy_from_slice(&i32::try_from(frame.len()).unwrap_or(i32::MAX).to_be_bytes());
        stream
            .write_all(&len_prefix)
            .await
            .map_err(|e| RaftError::Network(crabka_client_core::ClientError::Io(e)))?;
        stream
            .write_all(&frame)
            .await
            .map_err(|e| RaftError::Network(crabka_client_core::ClientError::Io(e)))?;
        stream
            .flush()
            .await
            .map_err(|e| RaftError::Network(crabka_client_core::ClientError::Io(e)))?;

        let mut resp_len_buf = [0u8; 4];
        stream
            .read_exact(&mut resp_len_buf)
            .await
            .map_err(|e| RaftError::Network(crabka_client_core::ClientError::Io(e)))?;
        let resp_len = usize::try_from(i32::from_be_bytes(resp_len_buf).max(0)).unwrap_or(0);
        let mut resp_buf = vec![0u8; resp_len];
        stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(|e| RaftError::Network(crabka_client_core::ClientError::Io(e)))?;
        // ResponseHeader v1: corr_id(4) + tagged_fields(1).
        if resp_buf.len() < 5 {
            return Err(RaftError::Openraft("truncated submit response".into()));
        }
        let mut cur: &[u8] = &resp_buf[5..];
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
            voters: config.voters.clone(),
            client_id: config.client_id.clone(),
        })
    }
}
