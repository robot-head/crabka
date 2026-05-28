//! KIP-966 offset-aware unclean recovery: pure selection helpers + the
//! controller-side Unclean Recovery Manager (URM) task. The URM polls
//! surviving replicas for their log-end-offset and last-written leader
//! epoch (`GetReplicaLogInfo`, `api_key` 93) and elects the most complete
//! log. See docs/superpowers/specs/2026-05-28-crabka-unclean-recovery-kip966-design.md.

#![allow(dead_code)]

use crabka_raft::NodeId;

/// One replica's reported log state, gathered from a `GetReplicaLogInfo`
/// response. Decoupled from the generated wire type so the selection
/// logic is unit-testable without building protocol structs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaLogInfo {
    pub broker_id: NodeId,
    pub last_written_leader_epoch: i32,
    pub log_end_offset: i64,
    pub current_leader_epoch: i32,
}

/// Pick the replica with the most complete log: highest
/// `last_written_leader_epoch`, then highest `log_end_offset`, then
/// lowest `broker_id` for determinism. Returns `None` for an empty input.
pub(crate) fn select_best_replica(responses: &[ReplicaLogInfo]) -> Option<NodeId> {
    responses
        .iter()
        .max_by(|a, b| {
            a.last_written_leader_epoch
                .cmp(&b.last_written_leader_epoch)
                .then(a.log_end_offset.cmp(&b.log_end_offset))
                .then(b.broker_id.cmp(&a.broker_id)) // lower broker_id wins ties
        })
        .map(|r| r.broker_id)
}

/// True if any responder reports a `current_leader_epoch` strictly
/// greater than the controller's known `leader_epoch` for the partition,
/// meaning a newer leader already exists and this recovery is stale.
pub(crate) fn has_newer_leader(responses: &[ReplicaLogInfo], known_leader_epoch: i32) -> bool {
    responses
        .iter()
        .any(|r| r.current_leader_epoch > known_leader_epoch)
}

// ---------------------------------------------------------------------------
// Unclean Recovery Manager (URM): the controller-side orchestrator.
// ---------------------------------------------------------------------------

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_raft::ControllerHandle;
use futures_util::FutureExt as _;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;

use crate::config_keys::RecoveryStrategy;
use crate::heartbeat::controller_state::ControllerLivenessState;
use crate::network::client::InterBrokerClient;

/// Aggressive recovery takes whatever responses arrive within this short
/// window — it does not wait for slow/unreachable replicas.
const AGGRESSIVE_DEADLINE: Duration = Duration::from_secs(2);
/// Balanced recovery waits longer, hoping to gather every replica's log
/// state before electing, but still caps the wait so a single hung replica
/// can't stall recovery forever.
const BALANCED_DEADLINE: Duration = Duration::from_secs(30);

/// A request to (possibly) run unclean recovery for one partition. Enqueued
/// by the failover path and the `ElectLeaders` handler; serviced by the URM.
pub(crate) struct RecoveryJob {
    pub topic: String,
    pub partition: i32,
    pub strategy: RecoveryStrategy,
    /// Optional reply channel. `ElectLeaders` (admin-triggered) wants the
    /// outcome; the background failover path fires-and-forgets.
    pub reply: Option<oneshot::Sender<RecoveryOutcome>>,
}

/// Result of attempting unclean recovery for a single partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryOutcome {
    /// A new leader was elected (and the change submitted). Carries the id.
    Elected(NodeId),
    /// No surviving replica could serve as a leader.
    NoEligibleReplica,
    /// Recovery turned out to be unnecessary (leader alive, or we are not
    /// the controller leader, or the partition is gone).
    NotNeeded,
    /// A newer leader already exists; this recovery is stale and was aborted.
    Stale,
    /// Another recovery for the same `(topic, partition)` is already running.
    InProgress,
}

/// Cloneable handle for enqueuing [`RecoveryJob`]s onto the URM task.
#[derive(Clone)]
pub(crate) struct UncleanRecoveryHandle {
    tx: mpsc::Sender<RecoveryJob>,
}

impl UncleanRecoveryHandle {
    /// Enqueue a recovery job. Logs (but does not panic) if the manager has
    /// shut down.
    pub(crate) async fn enqueue(&self, job: RecoveryJob) {
        if self.tx.send(job).await.is_err() {
            warn!("unclean recovery manager is gone; job dropped");
        }
    }
}

/// The controller-side Unclean Recovery Manager. Receives [`RecoveryJob`]s,
/// dedups in-flight work per partition, queries surviving replicas for their
/// log state, and elects the most-complete-log replica via `submit_change`.
pub(crate) struct UncleanRecoveryManager {
    controller: Arc<ControllerHandle>,
    liveness: Arc<ControllerLivenessState>,
    node_id: NodeId,
    inter_broker_client: Arc<InterBrokerClient>,
    listener_protocol: crabka_security::ListenerProtocol,
    metrics: crate::metrics::BrokerMetrics,
    in_flight: Arc<Mutex<HashSet<(String, i32)>>>,
}

impl UncleanRecoveryManager {
    /// Spawn the URM dispatch loop and return a cloneable handle for
    /// enqueuing jobs. The loop exits when `shutdown` fires or the last
    /// handle is dropped.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        controller: Arc<ControllerHandle>,
        liveness: Arc<ControllerLivenessState>,
        node_id: NodeId,
        inter_broker_client: Arc<InterBrokerClient>,
        listener_protocol: crabka_security::ListenerProtocol,
        metrics: crate::metrics::BrokerMetrics,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> UncleanRecoveryHandle {
        let (tx, mut rx) = mpsc::channel::<RecoveryJob>(256);
        let mgr = Arc::new(Self {
            controller,
            liveness,
            node_id,
            inter_broker_client,
            listener_protocol,
            metrics,
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        });
        tokio::spawn(async move {
            loop {
                let job = tokio::select! {
                    () = shutdown.cancelled() => return,
                    j = rx.recv() => match j { Some(j) => j, None => return },
                };
                let mgr = mgr.clone();
                tokio::spawn(async move {
                    mgr.recover_one(job).await;
                });
            }
        });
        UncleanRecoveryHandle { tx }
    }

    /// Per-job entry point: dedup against in-flight recoveries for the same
    /// partition, run the recovery, then release the in-flight slot and
    /// reply (if a reply channel was supplied).
    async fn recover_one(self: Arc<Self>, job: RecoveryJob) {
        let key = (job.topic.clone(), job.partition);
        {
            let mut set = self.in_flight.lock().await;
            if !set.insert(key.clone()) {
                if let Some(r) = job.reply {
                    let _ = r.send(RecoveryOutcome::InProgress);
                }
                return;
            }
        }
        let outcome = self.run_recovery(&job).await;
        self.in_flight.lock().await.remove(&key);
        if let Some(r) = job.reply {
            let _ = r.send(outcome);
        }
    }

    /// Core recovery routine. Confirms we are the controller leader and the
    /// partition still needs recovery, queries surviving replicas, and (if a
    /// winner emerges and no newer leader has appeared) submits the leader
    /// change.
    async fn run_recovery(&self, job: &RecoveryJob) -> RecoveryOutcome {
        let is_leader = self
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == self.node_id);
        if !is_leader {
            return RecoveryOutcome::NotNeeded;
        }

        let image = self.controller.current_image();
        let Some(pr) = image.partition(&job.topic, job.partition) else {
            return RecoveryOutcome::NotNeeded;
        };
        // If the current leader is alive, there's nothing to recover.
        if self.liveness.is_alive(pr.leader).await {
            return RecoveryOutcome::NotNeeded;
        }
        let known_epoch = pr.leader_epoch;
        let topic_id = image
            .topic(&job.topic)
            .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()));

        // Gather the surviving (alive) replicas to query.
        let mut alive: Vec<NodeId> = Vec::new();
        for &r in &pr.replicas {
            if self.liveness.is_alive(r).await {
                alive.push(r);
            }
        }
        if alive.is_empty() {
            return RecoveryOutcome::NoEligibleReplica;
        }

        let mut futs = Vec::with_capacity(alive.len());
        for r in alive {
            let Some(reg) = image.broker(r) else { continue };
            let (host, port) = (reg.host.clone(), reg.port);
            let client = self.inter_broker_client.clone();
            let proto = self.listener_protocol;
            let topic = job.topic.clone();
            let partition = job.partition;
            let my_id = i32::try_from(self.node_id).unwrap_or(-1);
            futs.push(
                async move {
                    query_replica(
                        &client, proto, &host, port, my_id, topic_id, &topic, partition, r,
                    )
                    .await
                }
                .boxed(),
            );
        }

        let deadline = match job.strategy {
            RecoveryStrategy::Aggressive | RecoveryStrategy::None => AGGRESSIVE_DEADLINE,
            RecoveryStrategy::Balanced => BALANCED_DEADLINE,
        };
        let collected: Vec<ReplicaLogInfo> = gather_responses(futs, job.strategy, deadline).await;

        if has_newer_leader(&collected, known_epoch) {
            return RecoveryOutcome::Stale;
        }
        let Some(winner) = select_best_replica(&collected) else {
            return RecoveryOutcome::NoEligibleReplica;
        };

        // Re-read the image and re-check before committing: the leader may
        // have come back, or the partition may have been deleted, while we
        // were polling replicas.
        let image = self.controller.current_image();
        let Some(pr) = image.partition(&job.topic, job.partition) else {
            return RecoveryOutcome::NotNeeded;
        };
        if self.liveness.is_alive(pr.leader).await {
            return RecoveryOutcome::NotNeeded;
        }

        let new_pr = PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: winner,
            replicas: pr.replicas.clone(),
            isr: vec![winner],
            leader_epoch: pr.leader_epoch + 1,
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
        };
        warn!(
            topic = %job.topic,
            partition = job.partition,
            leader = winner,
            "unclean recovery: elected most-complete-log replica (possible data loss)"
        );
        if let Err(e) = self
            .controller
            .submit_change(vec![MetadataRecord::V1Partition(new_pr)])
            .await
        {
            warn!(error = %e, "unclean recovery submit_change failed");
            return RecoveryOutcome::NoEligibleReplica;
        }
        self.metrics.record_unclean_leader_election();
        RecoveryOutcome::Elected(winner)
    }
}

/// Query one replica for its log-end-offset and leader-epoch state via
/// `GetReplicaLogInfo` (`api_key` 93). Returns `None` on any connect / send /
/// decode error, or if the replica reports an error for this partition.
#[allow(clippy::too_many_arguments)]
async fn query_replica(
    client: &InterBrokerClient,
    proto: crabka_security::ListenerProtocol,
    host: &str,
    port: u16,
    my_broker_id: i32,
    topic_id: WireUuid,
    _topic: &str,
    partition: i32,
    replica: NodeId,
) -> Option<ReplicaLogInfo> {
    use crabka_protocol::owned::get_replica_log_info_request::{
        GetReplicaLogInfoRequest, TopicPartitions,
    };
    let opts = crabka_client_core::ConnectionOptions {
        client_id: "crabka-unclean-recovery".to_string(),
        ..crabka_client_core::ConnectionOptions::default()
    };
    let conn = client
        .connect_as_connection(host, port, proto, "localhost", opts)
        .await
        .ok()?;
    let req = GetReplicaLogInfoRequest {
        broker_id: my_broker_id,
        topic_partitions: vec![TopicPartitions {
            topic_id,
            partitions: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = conn.send(req).await.ok()?;
    for t in &resp.topic_partition_log_info_list {
        for pli in &t.partition_log_info {
            if pli.partition == partition && pli.error_code == 0 {
                return Some(ReplicaLogInfo {
                    broker_id: replica,
                    last_written_leader_epoch: pli.last_written_leader_epoch,
                    log_end_offset: pli.log_end_offset,
                    current_leader_epoch: pli.current_leader_epoch,
                });
            }
        }
    }
    None
}

/// Drive the per-replica query futures concurrently. Returns when all futures
/// resolve OR `deadline` elapses, whichever is first. On timeout, returns
/// whatever responses arrived so far (never silently discards partial data).
async fn gather_responses<F>(
    futs: Vec<F>,
    _strategy: RecoveryStrategy,
    deadline: Duration,
) -> Vec<ReplicaLogInfo>
where
    F: std::future::Future<Output = Option<ReplicaLogInfo>> + Send + 'static,
{
    use futures_util::stream::{FuturesUnordered, StreamExt};
    let total = futs.len();
    let mut stream: FuturesUnordered<_> = futs.into_iter().collect();
    let mut out: Vec<ReplicaLogInfo> = Vec::with_capacity(total);
    let sleep = tokio::time::sleep(deadline);
    tokio::pin!(sleep);
    loop {
        if out.len() == total {
            break;
        }
        tokio::select! {
            () = &mut sleep => break,
            item = stream.next() => match item {
                Some(Some(info)) => out.push(info),
                Some(None) => {}
                None => break,
            },
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ri(broker_id: NodeId, epoch: i32, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id,
            last_written_leader_epoch: epoch,
            log_end_offset: leo,
            current_leader_epoch: epoch,
        }
    }

    #[test]
    fn picks_highest_epoch_then_offset() {
        // Broker 3 has a higher epoch even though broker 2 has a longer log.
        let r = [ri(2, 4, 100), ri(3, 5, 10)];
        assert_eq!(select_best_replica(&r), Some(3));
    }

    #[test]
    fn ties_on_epoch_break_by_offset() {
        let r = [ri(2, 5, 90), ri(3, 5, 120)];
        assert_eq!(select_best_replica(&r), Some(3));
    }

    #[test]
    fn ties_on_epoch_and_offset_break_by_lowest_broker_id() {
        let r = [ri(3, 5, 100), ri(1, 5, 100), ri(2, 5, 100)];
        assert_eq!(select_best_replica(&r), Some(1));
    }

    #[test]
    fn empty_input_returns_none() {
        assert_eq!(select_best_replica(&[]), None);
    }

    #[test]
    fn newer_leader_detected() {
        let r = [ReplicaLogInfo {
            broker_id: 2,
            last_written_leader_epoch: 5,
            log_end_offset: 10,
            current_leader_epoch: 7,
        }];
        assert!(has_newer_leader(&r, 6));
        assert!(!has_newer_leader(&r, 7));
    }
}

#[cfg(test)]
mod urm_tests {
    use super::*;
    use std::time::Duration;

    fn info(id: NodeId, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id: id,
            last_written_leader_epoch: 1,
            log_end_offset: leo,
            current_leader_epoch: 1,
        }
    }

    #[tokio::test]
    async fn balanced_waits_for_all_then_picks_best() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(
            vec![f1.boxed(), f2.boxed()],
            RecoveryStrategy::Balanced,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(got.len(), 2);
        assert_eq!(select_best_replica(&got), Some(2));
    }

    #[tokio::test]
    async fn balanced_returns_partial_on_timeout() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(
            vec![f1.boxed(), f2.boxed()],
            RecoveryStrategy::Balanced,
            Duration::from_millis(50),
        )
        .await;
        assert_eq!(got.len(), 1, "must return what arrived before the cap");
        assert_eq!(got[0].broker_id, 1);
    }

    #[tokio::test]
    async fn aggressive_takes_early_responders() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(
            vec![f1.boxed(), f2.boxed()],
            RecoveryStrategy::Aggressive,
            Duration::from_millis(50),
        )
        .await;
        assert_eq!(got, vec![info(1, 50)]);
    }
}
