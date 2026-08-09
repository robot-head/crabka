//! KIP-966 offset-aware unclean recovery.
//!
//! This module holds the pure selection helpers and the controller-side
//! Unclean Recovery Manager (URM) task. The URM polls surviving replicas for
//! their log-end-offset and last-written leader epoch with `GetReplicaLogInfo`
//! (`api_key` 93), and elects the most complete log.

use crabka_raft::NodeId;
use crabka_units::{Time, convert::TimeExt as _};

/// One replica's reported log state, from a `GetReplicaLogInfo` response.
///
/// This type is separate from the generated wire type, so a unit test can
/// drive the selection logic without building protocol structs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaLogInfo {
    pub broker_id: NodeId,
    pub last_written_leader_epoch: i32,
    pub log_end_offset: i64,
    pub current_leader_epoch: i32,
}

/// Picks the replica with the most complete log. It ranks by the highest
/// `last_written_leader_epoch`, then the highest `log_end_offset`, then the
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

/// Returns true if any responder reports a `current_leader_epoch` strictly
/// greater than the controller's known `leader_epoch` for the partition. A
/// newer leader then already exists, and this recovery is stale.
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

#[derive(Debug, Clone)]
pub(crate) struct RecoveryPolicy {
    pub aggressive_deadline: Time,
    pub balanced_deadline: Time,
    pub queue_capacity: usize,
    pub listener_protocol: crabka_security::ListenerProtocol,
    pub inter_broker_server_name: String,
}

impl RecoveryPolicy {
    fn deadline(&self, strategy: RecoveryStrategy) -> Time {
        match strategy {
            RecoveryStrategy::Aggressive | RecoveryStrategy::None => self.aggressive_deadline,
            RecoveryStrategy::Balanced => self.balanced_deadline,
        }
    }
}

/// A request to run unclean recovery for one partition, if it is needed. The
/// failover path and the `ElectLeaders` handler enqueue it, and the URM
/// services it.
pub(crate) struct RecoveryJob {
    pub topic: String,
    pub partition: i32,
    pub strategy: RecoveryStrategy,
    /// Optional reply channel. The admin-triggered `ElectLeaders` path wants
    /// the outcome. The background failover path sends the job and does not
    /// wait for a reply.
    pub reply: Option<oneshot::Sender<RecoveryOutcome>>,
}

/// Result of attempting unclean recovery for a single partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryOutcome {
    /// The URM elected a new leader and submitted the change. This variant
    /// carries the id.
    Elected(NodeId),
    /// No surviving replica could serve as a leader.
    NoEligibleReplica,
    /// Recovery was unnecessary. The leader is alive, or this node is not the
    /// controller leader, or the partition is gone.
    NotNeeded,
    /// A newer leader already exists, so this recovery is stale and the URM
    /// aborted it.
    Stale,
    /// Another recovery for the same `(topic, partition)` is already running.
    InProgress,
}

/// Cloneable handle that enqueues [`RecoveryJob`] values onto the URM task.
#[derive(Clone)]
pub(crate) struct UncleanRecoveryHandle {
    tx: mpsc::Sender<RecoveryJob>,
}

impl UncleanRecoveryHandle {
    #[cfg(test)]
    pub(crate) fn for_tests(tx: mpsc::Sender<RecoveryJob>) -> Self {
        Self { tx }
    }

    /// Enqueues a recovery job. It logs a message, and does not panic, if the
    /// manager has shut down.
    pub(crate) async fn enqueue(&self, job: RecoveryJob) {
        if self.tx.send(job).await.is_err() {
            warn!("unclean recovery manager is gone; job dropped");
        }
    }
}

/// The controller-side Unclean Recovery Manager.
///
/// It receives [`RecoveryJob`] values, dedups the in-flight work for each
/// partition, queries surviving replicas for their log state, and elects the
/// replica with the most complete log through `submit_change`.
pub(crate) struct UncleanRecoveryManager {
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: Arc<ControllerLivenessState>,
    node_id: NodeId,
    inter_broker_client: Arc<InterBrokerClient>,
    listener_protocol: crabka_security::ListenerProtocol,
    metrics: crate::metrics::BrokerMetrics,
    policy: RecoveryPolicy,
    in_flight: Arc<Mutex<HashSet<(String, i32)>>>,
}

impl UncleanRecoveryManager {
    /// Spawns the URM dispatch loop and returns a cloneable handle that
    /// enqueues jobs. The loop exits when `shutdown` fires or when the last
    /// handle drops.
    pub(crate) fn spawn(
        controller: Arc<dyn crate::metadata_source::MetadataSource>,
        liveness: Arc<ControllerLivenessState>,
        node_id: NodeId,
        inter_broker_client: Arc<InterBrokerClient>,
        metrics: crate::metrics::BrokerMetrics,
        policy: RecoveryPolicy,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> UncleanRecoveryHandle {
        let (tx, mut rx) = mpsc::channel::<RecoveryJob>(policy.queue_capacity);
        let mgr = Arc::new(Self {
            controller,
            liveness,
            node_id,
            inter_broker_client,
            listener_protocol: policy.listener_protocol,
            metrics,
            policy,
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

    /// Per-job entry point. It dedups against the in-flight recoveries for
    /// the same partition, runs the recovery, then releases the in-flight slot
    /// and replies if the caller supplied a reply channel.
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

    /// Core recovery routine.
    ///
    /// It confirms that this node is the controller leader and that the
    /// partition still needs recovery, then queries the surviving replicas.
    /// If a winner emerges and no newer leader has appeared, it submits the
    /// leader change.
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
            let server_name = self.policy.inter_broker_server_name.clone();
            let my_id = i32::try_from(self.node_id.0).unwrap_or(-1);
            futs.push(
                async move {
                    query_replica(
                        &client,
                        ReplicaQuery {
                            proto,
                            host,
                            port,
                            my_broker_id: my_id,
                            topic_id,
                            partition,
                            replica: r,
                            server_name,
                        },
                    )
                    .await
                }
                .boxed(),
            );
        }

        let deadline = self.policy.deadline(job.strategy);
        let collected: Vec<ReplicaLogInfo> = gather_responses(futs, deadline.to_std()).await;

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

    /// Builds and submits the `PartitionRecord` that elects `winner` as the
    /// new leader. The record bumps the epoch and shrinks the ISR to the
    /// winner alone.
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

/// Queries one replica for its log-end-offset and leader-epoch state with
/// `GetReplicaLogInfo` (`api_key` 93). Returns `None` on any connect, send, or
/// decode error, and also if the replica reports an error for this
/// partition.
struct ReplicaQuery {
    proto: crabka_security::ListenerProtocol,
    host: String,
    port: u16,
    my_broker_id: i32,
    topic_id: WireUuid,
    partition: i32,
    replica: NodeId,
    server_name: String,
}

async fn query_replica(client: &InterBrokerClient, query: ReplicaQuery) -> Option<ReplicaLogInfo> {
    use crabka_protocol::owned::get_replica_log_info_request::{
        GetReplicaLogInfoRequest, TopicPartitions,
    };
    let opts = crabka_client_core::ConnectionOptions {
        client_id: "crabka-unclean-recovery".to_string(),
        ..crabka_client_core::ConnectionOptions::default()
    };
    let conn = client
        .connect_as_connection(
            &query.host,
            query.port,
            query.proto,
            &query.server_name,
            opts,
        )
        .await
        .ok()?;
    let req = GetReplicaLogInfoRequest {
        broker_id: query.my_broker_id,
        topic_partitions: vec![TopicPartitions {
            topic_id: query.topic_id,
            partitions: vec![query.partition],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = conn.send(req).await.ok()?;
    for t in &resp.topic_partition_log_info_list {
        for pli in &t.partition_log_info {
            if pli.partition == query.partition && pli.error_code == 0 {
                return Some(ReplicaLogInfo {
                    broker_id: query.replica,
                    last_written_leader_epoch: pli.last_written_leader_epoch,
                    log_end_offset: pli.log_end_offset,
                    current_leader_epoch: pli.current_leader_epoch,
                });
            }
        }
    }
    None
}

/// Drives the per-replica query futures concurrently.
///
/// It returns when all futures resolve OR when `deadline` passes, whichever
/// comes first. On a timeout it returns the responses that arrived so far, and
/// never silently discards partial data.
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
    use crabka_units::millis;

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
    fn recovery_policy_selects_configured_deadlines() {
        let policy = RecoveryPolicy {
            aggressive_deadline: millis(7),
            balanced_deadline: millis(19),
            queue_capacity: 3,
            listener_protocol: crabka_security::ListenerProtocol::Ssl,
            inter_broker_server_name: "broker.internal".to_string(),
        };

        assert!(policy.deadline(RecoveryStrategy::Aggressive) == millis(7));
        assert!(policy.deadline(RecoveryStrategy::Balanced) == millis(19));
        assert!(policy.queue_capacity == 3);
        assert!(policy.listener_protocol == crabka_security::ListenerProtocol::Ssl);
        assert!(policy.inter_broker_server_name == "broker.internal");
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
        assert!(got.len() == 1, "must return what arrived before the cap");
        assert!(got[0].broker_id == crabka_audit::NodeId(1));
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
    use std::{collections::BTreeSet, net::SocketAddr};

    use assert2::assert;
    use crabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    };
    use crabka_raft::{
        AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
        UpdateVoter,
    };
    use crabka_units::secs;
    use tokio::sync::watch;
    use uuid::Uuid;

    use super::*;
    use crate::{
        heartbeat::controller_state::ControllerLivenessState, metadata_source::MetadataSource,
    };

    /// Minimal `MetadataSource` that drives the control flow of
    /// `run_recovery`. These paths exercise only `watch_leader`,
    /// `current_image`, and `submit_change`, and never reach the rest.
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
            unimplemented!()
        }
        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            self.leader_rx.clone()
        }
        fn quorum_state(&self) -> QuorumState {
            unimplemented!()
        }
        async fn submit_change(
            &self,
            _records: Vec<MetadataRecord>,
        ) -> Result<crabka_raft::SubmitChangeResult, RaftError> {
            Ok(crabka_raft::SubmitChangeResult::default())
        }
        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            unimplemented!()
        }
        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            unimplemented!()
        }
        fn controller_bound_addr(&self) -> SocketAddr {
            unimplemented!()
        }
        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            unimplemented!()
        }
        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            unimplemented!()
        }
        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!()
        }
        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!()
        }
        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!()
        }
        async fn cancel(&self) {
            unimplemented!()
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
        let l = ControllerLivenessState::new(crabka_units::secs(10));
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
            policy: RecoveryPolicy {
                aggressive_deadline: secs(2),
                balanced_deadline: secs(30),
                queue_capacity: 256,
                listener_protocol: crabka_security::ListenerProtocol::Plaintext,
                inter_broker_server_name: "localhost".to_string(),
            },
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
