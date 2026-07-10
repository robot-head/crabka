//! KIP-966 offset-aware unclean recovery: pure selection helpers + the
//! controller-side Unclean Recovery Manager (URM) task. The URM polls
//! surviving replicas for their log-end-offset and last-written leader
//! epoch (`GetReplicaLogInfo`, `api_key` 93) and elects the most complete
//! log.

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

use std::{collections::HashSet, sync::Arc, time::Duration};

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use futures_util::FutureExt as _;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;

use crate::{
    config_keys::RecoveryStrategy, heartbeat::controller_state::ControllerLivenessState,
    network::client::InterBrokerClient,
};

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
    #[cfg(test)]
    pub(crate) fn for_tests(tx: mpsc::Sender<RecoveryJob>) -> Self {
        Self { tx }
    }

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
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
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
        controller: Arc<dyn crate::metadata_source::MetadataSource>,
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
    // cargo-mutants: per-job orchestration over live recovery state and reply channels;
    // recovery policy decisions are exercised through focused tests.
    #[cfg_attr(test, mutants::skip)]
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
        if self.liveness.is_alive(pr.leader.0).await {
            return RecoveryOutcome::NotNeeded;
        }
        let known_epoch = pr.leader_epoch;
        let topic_id = image
            .topic(&job.topic)
            .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()));

        // Gather the surviving (alive) replicas to query.
        let mut alive: Vec<NodeId> = Vec::new();
        for &r in &pr.replicas {
            if self.liveness.is_alive(r.0).await {
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
            let partition = job.partition;
            let my_id = i32::try_from(self.node_id.0).unwrap_or(-1);
            futs.push(
                async move {
                    query_replica(&client, proto, &host, port, my_id, topic_id, partition, r).await
                }
                .boxed(),
            );
        }

        let deadline = match job.strategy {
            RecoveryStrategy::Aggressive | RecoveryStrategy::None => AGGRESSIVE_DEADLINE,
            RecoveryStrategy::Balanced => BALANCED_DEADLINE,
        };
        let collected: Vec<ReplicaLogInfo> = gather_responses(futs, deadline).await;

        if has_newer_leader(&collected, known_epoch.0) {
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
        if self.liveness.is_alive(pr.leader.0).await {
            return RecoveryOutcome::NotNeeded;
        }

        self.commit_elected_leader(job, pr, winner).await
    }

    /// Build and submit the `PartitionRecord` electing `winner` as the new
    /// leader (bumping the epoch and shrinking ISR to just the winner).
    async fn commit_elected_leader(
        &self,
        job: &RecoveryJob,
        pr: &PartitionRecord,
        winner: NodeId,
    ) -> RecoveryOutcome {
        let new_pr = PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: winner,
            replicas: pr.replicas.clone(),
            isr: vec![winner],
            leader_epoch: pr.leader_epoch.next(),
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
            directories: pr.directories.clone(),
            partition_epoch: pr.partition_epoch + 1,
        };
        warn!(
            topic = %job.topic,
            partition = job.partition,
            leader = winner.0,
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
async fn gather_responses<F>(futs: Vec<F>, deadline: Duration) -> Vec<ReplicaLogInfo>
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
    use assert2::assert;

    use super::*;

    fn ri(broker_id: u64, epoch: i32, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id: NodeId(broker_id),
            last_written_leader_epoch: epoch,
            log_end_offset: leo,
            current_leader_epoch: epoch,
        }
    }

    #[test]
    fn picks_highest_epoch_then_offset() {
        // Broker 3 has a higher epoch even though broker 2 has a longer log.
        let r = [ri(2, 4, 100), ri(3, 5, 10)];
        assert!(select_best_replica(&r) == Some(NodeId(3)));
    }

    #[test]
    fn ties_on_epoch_break_by_offset() {
        let r = [ri(2, 5, 90), ri(3, 5, 120)];
        assert!(select_best_replica(&r) == Some(NodeId(3)));
    }

    #[test]
    fn ties_on_epoch_and_offset_break_by_lowest_broker_id() {
        let r = [ri(3, 5, 100), ri(1, 5, 100), ri(2, 5, 100)];
        assert!(select_best_replica(&r) == Some(NodeId(1)));
    }

    #[test]
    fn empty_input_returns_none() {
        assert!(select_best_replica(&[]) == None);
    }

    #[test]
    fn newer_leader_detected() {
        let r = [ReplicaLogInfo {
            broker_id: NodeId(2),
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
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    fn info(id: u64, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id: NodeId(id),
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
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], Duration::from_secs(5)).await;
        assert!(got.len() == 2);
        assert!(select_best_replica(&got) == Some(NodeId(2)));
    }

    #[tokio::test]
    async fn balanced_returns_partial_on_timeout() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], Duration::from_millis(50)).await;
        assert!(
            got.iter().map(|info| info.broker_id).collect::<Vec<_>>()
                == vec![crabka_audit::NodeId(1)]
        );
    }

    #[tokio::test]
    async fn aggressive_takes_early_responders() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], Duration::from_millis(50)).await;
        assert!(got == vec![info(1, 50)]);
    }
}

#[cfg(test)]
mod run_recovery_tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        net::SocketAddr,
    };

    use assert2::assert;
    use crabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    };
    use crabka_raft::{
        AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
        UpdateVoter,
    };
    use tokio::sync::watch;
    use uuid::Uuid;

    use super::*;
    use crate::{
        heartbeat::controller_state::ControllerLivenessState, metadata_source::MetadataSource,
    };

    /// Minimal `MetadataSource` for driving `run_recovery`'s control flow. Only
    /// `watch_leader`, `current_image`, and `submit_change` are exercised; the
    /// rest are never reached on these paths.
    struct MockSource {
        leader_rx: watch::Receiver<Option<NodeId>>,
        _leader_tx: watch::Sender<Option<NodeId>>,
        image: Arc<MetadataImage>,
    }

    impl MockSource {
        fn new(leader: Option<u64>, image: MetadataImage) -> Self {
            let (tx, rx) = watch::channel(leader.map(NodeId));
            Self {
                leader_rx: rx,
                _leader_tx: tx,
                image: Arc::new(image),
            }
        }
    }

    #[async_trait::async_trait]
    impl MetadataSource for MockSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }
        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            let (_tx, rx) = watch::channel(self.image.clone());
            rx
        }
        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            self.leader_rx.clone()
        }
        fn quorum_state(&self) -> QuorumState {
            QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: *self.leader_rx.borrow(),
                voters: Vec::new(),
                voter_nodes: BTreeMap::new(),
                per_voter_matched_index: BTreeMap::new(),
            }
        }
        async fn submit_change(&self, _records: Vec<MetadataRecord>) -> Result<(), RaftError> {
            Ok(())
        }
        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            panic!("MockSource::change_membership must not be called by unclean recovery tests")
        }
        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            panic!("MockSource::add_learner must not be called by unclean recovery tests")
        }
        fn controller_bound_addr(&self) -> SocketAddr {
            SocketAddr::from(([0, 0, 0, 0], 0))
        }
        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            SnapshotRange::NoSnapshot
        }
        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            Ok(())
        }
        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("MockSource::add_voter must not be called by unclean recovery tests")
        }
        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("MockSource::remove_voter must not be called by unclean recovery tests")
        }
        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("MockSource::update_voter must not be called by unclean recovery tests")
        }
        async fn cancel(&self) {
            // Nothing to cancel in the in-memory test double.
        }
    }

    const NODE: u64 = 10;

    fn image_with_partition(leader: u64, replicas: &[u64]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).unwrap(),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: replicas.iter().copied().map(NodeId).collect(),
            leader_epoch: crabka_metadata::LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        img
    }

    fn register_broker(img: &mut MetadataImage, node_id: u64, host: &str, port: u16) {
        img.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(node_id),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: host.into(),
                port,
                rack: None,
                endpoints: vec![],
            },
        ));
    }

    async fn liveness_with_alive(alive: &[u64]) -> Arc<ControllerLivenessState> {
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for &n in alive {
            l.record_heartbeat(n).await;
        }
        Arc::new(l)
    }

    fn manager(
        source: MockSource,
        liveness: Arc<ControllerLivenessState>,
    ) -> UncleanRecoveryManager {
        UncleanRecoveryManager {
            controller: Arc::new(source),
            liveness,
            node_id: NodeId(NODE),
            inter_broker_client: Arc::new(InterBrokerClient::new(None, None)),
            listener_protocol: crabka_security::ListenerProtocol::Plaintext,
            metrics: crate::metrics::BrokerMetrics::new(),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn job() -> RecoveryJob {
        RecoveryJob {
            topic: "t".into(),
            partition: 0,
            strategy: RecoveryStrategy::None,
            reply: None,
        }
    }

    #[tokio::test]
    async fn not_controller_leader_is_not_needed() {
        let mgr = manager(
            MockSource::new(Some(99), image_with_partition(1, &[1, 2])),
            liveness_with_alive(&[]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NotNeeded);
    }

    #[tokio::test]
    async fn missing_partition_is_not_needed() {
        let mgr = manager(
            MockSource::new(Some(NODE), MetadataImage::new(Uuid::nil())),
            liveness_with_alive(&[]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NotNeeded);
    }

    #[tokio::test]
    async fn live_leader_is_not_needed() {
        let mgr = manager(
            MockSource::new(Some(NODE), image_with_partition(1, &[1, 2])),
            liveness_with_alive(&[1]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NotNeeded);
    }

    #[tokio::test]
    async fn dead_leader_no_alive_replicas_is_no_eligible() {
        // Leader 1 is dead and no replica is alive: nothing to query.
        let mgr = manager(
            MockSource::new(Some(NODE), image_with_partition(1, &[1, 2])),
            liveness_with_alive(&[]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NoEligibleReplica);
    }

    #[tokio::test]
    async fn dead_leader_all_queries_fail_is_no_eligible() {
        // Replica 2 is alive but its endpoint refuses connections, so the
        // query returns no log info and no winner can be selected.
        let mut img = image_with_partition(1, &[1, 2]);
        register_broker(&mut img, 2, "127.0.0.1", 1);
        let mgr = manager(
            MockSource::new(Some(NODE), img),
            liveness_with_alive(&[2]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NoEligibleReplica);
    }

    #[tokio::test]
    async fn recover_one_dedups_in_flight_job() {
        let mgr = Arc::new(manager(
            MockSource::new(Some(NODE), image_with_partition(1, &[1, 2])),
            liveness_with_alive(&[]).await,
        ));
        // Pre-mark this partition as already recovering.
        mgr.in_flight.lock().await.insert(("t".to_string(), 0));
        let (tx, rx) = oneshot::channel();
        let j = RecoveryJob {
            topic: "t".into(),
            partition: 0,
            strategy: RecoveryStrategy::None,
            reply: Some(tx),
        };
        mgr.clone().recover_one(j).await;
        assert!(rx.await.unwrap() == RecoveryOutcome::InProgress);
    }
}
