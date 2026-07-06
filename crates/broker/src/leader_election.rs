//! Controller-side leader-election scan. Called by the liveness ticker
//! when a broker transitions `alive → dead`. Scans every partition where
//! the dead broker is leader, picks the first alive ISR replica as new
//! leader, bumps `leader_epoch`, and emits the new `PartitionRecord`s
//! through openraft.
//!
//! KIP-841: when ISR becomes empty and the topic's
//! `unclean.leader.election.enable` is `true`, the scan falls through to
//! an out-of-ISR pick (first alive replica, singleton-ISR) — accepting
//! possible data loss in exchange for availability. Default `false`
//! preserves Kafka's safe-by-default behavior (partition unavailable
//! until a former ISR member returns).

#![allow(dead_code)]

use std::sync::Arc;

use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
use crabka_raft::NodeId;
use tracing::warn;

use crate::{
    config_keys::{RecoveryStrategy, UNCLEAN_LEADER_ELECTION_ENABLE, resolve_recovery_strategy},
    error::BrokerError,
    heartbeat::controller_state::ControllerLivenessState,
};

/// Output of a failover scan: immediate metadata changes plus partitions
/// that need asynchronous offset-aware recovery via the URM.
pub(crate) struct FailoverPlan {
    pub changes: Vec<MetadataRecord>,
    pub recoveries: Vec<(String, i32, RecoveryStrategy)>,
}

/// The pure per-partition failover decision shared by the dead-broker scan
/// (`compute_failover_changes`) and the offline-log-dir scan
/// (`compute_offline_dir_failover_changes`). No I/O: the callers handle
/// partition filtering, the alive snapshot, record construction, metrics, and
/// recovery enqueue. Extracted so the failover policy is independently
/// unit-testable and model-checkable, and so the two scans share one copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FailoverDecision {
    /// Elect `leader` with `isr`; the caller bumps `leader_epoch + 1` and, when
    /// `unclean`, records the unclean-election metric.
    Elect {
        leader: NodeId,
        isr: Vec<NodeId>,
        unclean: bool,
    },
    /// Defer to the offset-aware Unclean Recovery Manager (KIP-966).
    Recover(RecoveryStrategy),
    /// Dead broker was a non-leader ISR member: shrink ISR (leader/epoch kept).
    ShrinkIsr { isr: Vec<NodeId> },
    /// Leader is dead, ISR empty, and no unclean path is permitted/available.
    Unavailable,
    /// Nothing to do for this partition.
    NoChange,
}

/// Decide the failover action for one partition. `alive` is the controller's
/// snapshot of live brokers; `strategy` / `unclean_enabled` are the topic's
/// resolved recovery policy.
pub(crate) fn failover_one(
    pr: &PartitionRecord,
    dead: NodeId,
    alive: &std::collections::HashSet<NodeId>,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
) -> FailoverDecision {
    // The ISR after dropping the dead broker AND any other non-alive member.
    let alive_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| **n != dead && alive.contains(n))
        .copied()
        .collect();
    if pr.leader == dead {
        if let Some(&new_leader) = alive_isr.first() {
            // Clean: the new leader was in the ISR, so it holds every committed
            // record. No data loss.
            FailoverDecision::Elect {
                leader: new_leader,
                isr: alive_isr,
                unclean: false,
            }
        } else {
            match strategy {
                RecoveryStrategy::Balanced | RecoveryStrategy::Aggressive => {
                    FailoverDecision::Recover(strategy)
                }
                RecoveryStrategy::None if unclean_enabled => {
                    // KIP-841: ISR is dead but the operator opted into possible
                    // data loss. Elect the first alive replica, singleton ISR.
                    match pr
                        .replicas
                        .iter()
                        .find(|n| **n != dead && alive.contains(n))
                    {
                        Some(&new_leader) => FailoverDecision::Elect {
                            leader: new_leader,
                            isr: vec![new_leader],
                            unclean: true,
                        },
                        None => FailoverDecision::Unavailable,
                    }
                }
                RecoveryStrategy::None => FailoverDecision::Unavailable,
            }
        }
    } else if alive_isr.len() < pr.isr.len() {
        FailoverDecision::ShrinkIsr { isr: alive_isr }
    } else {
        FailoverDecision::NoChange
    }
}

/// Read `unclean.leader.election.enable` for a topic, defaulting to
/// `false` when the key is unset or unparseable. Centralized so the
/// failover path and any future caller stay consistent.
fn unclean_election_enabled(image: &MetadataImage, topic: &str) -> bool {
    image
        .topic_config(topic)
        .and_then(|m| m.get(UNCLEAN_LEADER_ELECTION_ENABLE))
        .is_some_and(|v| v == "true")
}

/// Compute the failover `MetadataRecord` changes for `dead` against
/// `image`. Pure — no I/O beyond `liveness.is_alive` lookups. Extracted
/// so the failover policy (including the KIP-841 unclean toggle) is
/// unit-testable without spinning up a controller.
pub(crate) async fn compute_failover_changes(
    image: &MetadataImage,
    dead: NodeId,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();
    // Snapshot the alive set once (single lock) rather than taking the
    // liveness lock per ISR/replica entry inside the scan below.
    let alive: std::collections::HashSet<NodeId> = liveness
        .alive_snapshot()
        .await
        .into_iter()
        .map(NodeId)
        .collect();
    // Single O(P) walk over every partition in the image.
    for (_, pr) in image.all_partitions() {
        if !pr.replicas.contains(&dead) && !pr.isr.contains(&dead) {
            continue;
        }
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = unclean_election_enabled(image, &pr.topic);
        match failover_one(pr, dead, &alive, strategy, unclean_enabled) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader = leader.0,
                        "unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                    );
                    // KIP-841: account this election so operators can alert on a
                    // non-zero rate of unclean failovers in their cluster.
                    metrics.record_unclean_leader_election();
                }
                // One source of truth for the bumped epoch: used by both the
                // log line and the emitted record, so the failover tests that
                // assert the incremented `leader_epoch` also pin the logged
                // value (no un-killable log-only arithmetic).
                let new_leader_epoch = pr.leader_epoch.next();
                tracing::info!(
                    topic = %pr.topic,
                    partition = pr.partition,
                    dead = dead.0,
                    old_leader = pr.leader.0,
                    new_leader = leader.0,
                    old_isr = ?pr.isr,
                    new_isr = ?isr,
                    new_leader_epoch = new_leader_epoch.0,
                    unclean,
                    "failover: re-electing partition leader (triggered by dead broker)"
                );
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: new_leader_epoch,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::ShrinkIsr { isr } => {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::Recover(strategy) => {
                // KIP-966: defer to the offset-aware Unclean Recovery Manager —
                // it polls surviving replicas and elects the most complete log.
                recoveries.push((pr.topic.clone(), pr.partition, strategy));
            }
            FailoverDecision::Unavailable => {
                warn!(
                    topic = %pr.topic, partition = pr.partition,
                    "leader dead, no live ISR replica; partition unavailable"
                );
            }
            FailoverDecision::NoChange => {}
        }
    }
    FailoverPlan {
        changes,
        recoveries,
    }
}

/// Compute failover changes for partitions whose replica on `broker` lives
/// on a now-offline log directory (`offline_uuids`). KIP-112: a broker stays
/// alive after a disk failure, so the dead-broker scan never fires — this
/// scan does, driven by the broker's `offline_log_dirs` heartbeat.
///
/// For each affected partition:
/// - if `broker` is the leader, elect a new leader from the alive ISR minus
///   `broker` (same clean / KIP-966 / KIP-841 policy as
///   [`compute_failover_changes`]), drop `broker` from ISR, bump epoch;
/// - if `broker` is a non-leader ISR member, drop it from ISR (no epoch bump).
///
/// Pure; idempotent (after the change `broker` is neither leader nor in ISR,
/// so a repeat yields an empty plan).
#[allow(clippy::too_many_lines)]
pub(crate) async fn compute_offline_dir_failover_changes(
    image: &MetadataImage,
    broker: NodeId,
    offline_uuids: &std::collections::HashSet<uuid::Uuid>,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();
    let alive: std::collections::HashSet<NodeId> = liveness
        .alive_snapshot()
        .await
        .into_iter()
        .map(NodeId)
        .collect();
    for (_, pr) in image.all_partitions() {
        let Some(slot) = pr.replicas.iter().position(|n| *n == broker) else {
            continue;
        };
        let on_offline = pr
            .directories
            .get(slot)
            .is_some_and(|d| offline_uuids.contains(d));
        if !on_offline {
            continue;
        }
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = unclean_election_enabled(image, &pr.topic);
        match failover_one(pr, broker, &alive, strategy, unclean_enabled) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader = leader.0,
                        "offline-dir unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                    );
                    metrics.record_unclean_leader_election();
                }
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch.next(),
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::ShrinkIsr { isr } => {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::Recover(strategy) => {
                recoveries.push((pr.topic.clone(), pr.partition, strategy));
            }
            FailoverDecision::Unavailable => {
                warn!(
                    topic = %pr.topic, partition = pr.partition,
                    "offline dir on leader, no live ISR replica; partition unavailable"
                );
            }
            FailoverDecision::NoChange => {}
        }
    }
    FailoverPlan {
        changes,
        recoveries,
    }
}

/// Called when the liveness ticker observes `AliveToDead(dead)`. Scans
/// every partition where `dead` is leader OR in the ISR; proposes
/// updated `PartitionRecord`s.
///
/// No-op unless `controller` is currently the openraft leader (only
/// the leader can `submit_change`).
#[tracing::instrument(
    name = "leader_election_on_broker_dead",
    level = "info",
    skip_all,
    fields(node_id, dead),
    err
)]
pub(crate) async fn on_broker_dead(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    node_id: NodeId,
    dead: NodeId,
    liveness: &Arc<ControllerLivenessState>,
    metrics: &crate::metrics::BrokerMetrics,
    recovery: &crate::unclean_recovery::UncleanRecoveryHandle,
) -> Result<(), BrokerError> {
    let is_controller_leader = controller
        .watch_leader()
        .borrow()
        .is_some_and(|n| n == node_id);
    if !is_controller_leader {
        return Ok(());
    }

    let image = controller.current_image();
    let plan = compute_failover_changes(&image, dead, liveness, metrics).await;
    if !plan.changes.is_empty() {
        controller
            .submit_change(plan.changes)
            .await
            .map_err(|e| BrokerError::Replication(format!("submit_change: {e}")))?;
    }
    // KIP-966: partitions whose topic opted into an offset-aware recovery
    // strategy are handed to the Unclean Recovery Manager, which polls
    // surviving replicas for their log state before electing. Fire and
    // forget — the failover path does not await the outcome.
    for (topic, partition, strategy) in plan.recoveries {
        recovery
            .enqueue(crate::unclean_recovery::RecoveryJob {
                topic,
                partition,
                strategy,
                reply: None,
            })
            .await;
    }
    Ok(())
}

/// Called when the liveness ticker observes `DeadToAlive(alive)`. This
/// is a no-op — ISR expand happens organically via
/// `isr_maintenance` once the rejoined broker's replicator catches up.
/// The hook is here for future enhancements (e.g. auto-rebalance).
#[allow(clippy::unused_async)]
pub(crate) async fn on_broker_alive(
    _controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    _node_id: NodeId,
    _alive: NodeId,
    _liveness: &Arc<ControllerLivenessState>,
) -> Result<(), BrokerError> {
    Ok(())
}

/// Operator-triggered election type per KIP-460.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectionType {
    /// Move leadership back to the first replica in `replicas[]` if it's
    /// alive and in the ISR. Safe — no data loss possible.
    Preferred,
    /// Allow election outside the ISR when every ISR member is dead.
    /// Operator has accepted the possible-data-loss risk.
    Unclean,
}

/// Reasons `select_new_leader_for_partition` may refuse to elect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectError {
    UnknownTopicOrPartition,
    PreferredAlreadyLeader,
    ElectionNotNeeded,
    PreferredNotInIsr,
    PreferredNotAlive,
    NoEligibleReplica,
}

/// Pick a replacement leader for a partition currently led by a broker
/// that asked to shut down. Returns the new `PartitionRecord` ready to
/// submit, or `ElectError::ElectionNotNeeded` when `shutting_down` is
/// not actually this partition's current leader, or
/// `ElectError::NoEligibleReplica` when no other ISR member is alive.
///
/// Differs from `select_new_leader_for_partition(Preferred)`:
/// - Trigger is "current leader wants to drain", not "preferred replica
///   isn't leader". So we pick any alive ISR member that isn't the
///   shutting-down broker, not strictly the preferred one.
/// - ISR is left unchanged. The shutting-down broker stays in ISR
///   until it actually goes offline; the heartbeat loop is what flips
///   it dead.
pub(crate) async fn select_replacement_leader_for_shutdown(
    image: &crabka_metadata::MetadataImage,
    liveness: &ControllerLivenessState,
    topic: &str,
    partition: i32,
    shutting_down: NodeId,
) -> Result<crabka_metadata::PartitionRecord, ElectError> {
    let pr = image
        .partition(topic, partition)
        .ok_or(ElectError::UnknownTopicOrPartition)?;
    if pr.leader != shutting_down {
        return Err(ElectError::ElectionNotNeeded);
    }
    let mut new_leader: Option<NodeId> = None;
    for &n in &pr.isr {
        if n == shutting_down {
            continue;
        }
        if liveness.is_alive(n.0).await {
            new_leader = Some(n);
            break;
        }
    }
    let Some(new_leader) = new_leader else {
        return Err(ElectError::NoEligibleReplica);
    };
    Ok(crabka_metadata::PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader: new_leader,
        replicas: pr.replicas.clone(),
        isr: pr.isr.clone(),
        leader_epoch: pr.leader_epoch.next(),
        adding_replicas: pr.adding_replicas.clone(),
        removing_replicas: pr.removing_replicas.clone(),
        directories: pr.directories.clone(),
        partition_epoch: pr.partition_epoch + 1,
    })
}

/// Operator-triggered single-partition election. Returns the new
/// `PartitionRecord` ready to submit, or an `ElectError`.
///
/// Pure: no I/O, no panics. Caller is responsible for submitting the
/// returned record via the controller.
pub(crate) async fn select_new_leader_for_partition(
    image: &crabka_metadata::MetadataImage,
    liveness: &ControllerLivenessState,
    topic: &str,
    partition: i32,
    election: ElectionType,
) -> Result<PartitionRecord, ElectError> {
    let pr = image
        .partition(topic, partition)
        .ok_or(ElectError::UnknownTopicOrPartition)?;
    match election {
        ElectionType::Preferred => {
            let preferred = *pr
                .replicas
                .first()
                .ok_or(ElectError::UnknownTopicOrPartition)?;
            if pr.leader == preferred {
                return Err(ElectError::PreferredAlreadyLeader);
            }
            if !pr.isr.contains(&preferred) {
                return Err(ElectError::PreferredNotInIsr);
            }
            if !liveness.is_alive(preferred.0).await {
                return Err(ElectError::PreferredNotAlive);
            }
            Ok(PartitionRecord {
                topic: pr.topic.clone(),
                partition: pr.partition,
                leader: preferred,
                replicas: pr.replicas.clone(),
                isr: pr.isr.clone(),
                leader_epoch: pr.leader_epoch.next(),
                adding_replicas: pr.adding_replicas.clone(),
                removing_replicas: pr.removing_replicas.clone(),
                directories: pr.directories.clone(),
                partition_epoch: pr.partition_epoch + 1,
            })
        }
        ElectionType::Unclean => {
            // Bail if any ISR member is alive — UNCLEAN is meant for
            // catastrophic ISR loss, not routine rebalances.
            for &n in &pr.isr {
                if liveness.is_alive(n.0).await {
                    return Err(ElectError::ElectionNotNeeded);
                }
            }
            // Find the first alive replica, in or out of ISR.
            for &n in &pr.replicas {
                if liveness.is_alive(n.0).await {
                    return Ok(PartitionRecord {
                        topic: pr.topic.clone(),
                        partition: pr.partition,
                        leader: n,
                        replicas: pr.replicas.clone(),
                        isr: vec![n],
                        leader_epoch: pr.leader_epoch.next(),
                        adding_replicas: pr.adding_replicas.clone(),
                        removing_replicas: pr.removing_replicas.clone(),
                        directories: pr.directories.clone(),
                        partition_epoch: pr.partition_epoch + 1,
                    });
                }
            }
            Err(ElectError::NoEligibleReplica)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};

    use assert2::{assert, check};
    use crabka_metadata::{
        LeaderEpoch, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    };
    use crabka_raft::{
        AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter,
        SnapshotRange, UpdateVoter,
    };
    use tokio::sync::{Mutex, watch};
    use uuid::Uuid;

    use super::{
        ControllerLivenessState, ElectError, ElectionType, on_broker_dead,
        select_new_leader_for_partition, select_replacement_leader_for_shutdown,
    };

    fn img_with_partition(
        topic: &str,
        partition: i32,
        leader: u64,
        replicas: &[u64],
        isr: &[u64],
    ) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: topic.into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).unwrap(),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.into(),
            partition,
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: isr.iter().copied().map(NodeId).collect(),
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        img
    }

    async fn liveness_with_alive(alive: &[u64]) -> Arc<ControllerLivenessState> {
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for &n in alive {
            l.record_heartbeat(n).await;
        }
        Arc::new(l)
    }

    struct TestMetadataSource {
        image: Arc<MetadataImage>,
        leader_tx: watch::Sender<Option<NodeId>>,
        submitted: Mutex<Vec<Vec<MetadataRecord>>>,
    }

    impl TestMetadataSource {
        fn new(image: MetadataImage, leader: Option<NodeId>) -> Self {
            let (leader_tx, _) = watch::channel(leader);
            Self {
                image: Arc::new(image),
                leader_tx,
                submitted: Mutex::new(Vec::new()),
            }
        }

        async fn submitted_batches(&self) -> Vec<Vec<MetadataRecord>> {
            self.submitted.lock().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for TestMetadataSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            let (_, rx) = watch::channel(self.image.clone());
            rx
        }

        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            self.leader_tx.subscribe()
        }

        fn quorum_state(&self) -> QuorumState {
            QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: *self.leader_tx.borrow(),
                voters: Vec::new(),
                voter_nodes: std::collections::BTreeMap::new(),
                per_voter_matched_index: std::collections::BTreeMap::new(),
            }
        }

        async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
            self.submitted.lock().await.push(records);
            Ok(())
        }

        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            unimplemented!("unused in leader_election tests")
        }

        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            unimplemented!("unused in leader_election tests")
        }

        fn controller_bound_addr(&self) -> SocketAddr {
            unimplemented!("unused in leader_election tests")
        }

        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            unimplemented!("unused in leader_election tests")
        }

        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            unimplemented!("unused in leader_election tests")
        }

        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!("unused in leader_election tests")
        }

        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!("unused in leader_election tests")
        }

        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!("unused in leader_election tests")
        }

        async fn cancel(&self) {}
    }

    fn recovery_handle_for_tests() -> crate::unclean_recovery::UncleanRecoveryHandle {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        crate::unclean_recovery::UncleanRecoveryHandle::for_tests(tx)
    }

    #[tokio::test]
    async fn preferred_happy_path() {
        let img = img_with_partition("foo", 0, /*leader*/ 2, &[1, 2, 3], &[1, 2, 3]);
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let new_pr = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
            .await
            .expect("should elect");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        };
        assert!(new_pr == expected);
    }

    #[tokio::test]
    async fn preferred_election_error_cases() {
        // Replicas are always [1, 2, 3]; the preferred leader is replica 1.
        // (current_leader, isr, alive, expected)
        let cases: [(u64, &[u64], &[u64], ElectError); 3] = [
            // Preferred replica 1 is already the leader.
            (
                1,
                &[1, 2, 3],
                &[1, 2, 3],
                ElectError::PreferredAlreadyLeader,
            ),
            // Preferred replica 1 is not in the ISR.
            (2, &[2, 3], &[1, 2, 3], ElectError::PreferredNotInIsr),
            // Preferred replica 1 is in the ISR but dead.
            (2, &[1, 2, 3], &[2, 3], ElectError::PreferredNotAlive),
        ];
        for (leader, isr, alive, expected) in cases {
            let img = img_with_partition("foo", 0, leader, &[1, 2, 3], isr);
            let l = liveness_with_alive(alive).await;
            let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
                .await
                .unwrap_err();
            assert!(
                err == expected,
                "leader {leader}, isr {isr:?}, alive {alive:?}"
            );
        }
    }

    #[tokio::test]
    async fn unclean_happy_path() {
        // ISR is just {1}, broker 1 is dead, brokers 2/3 are alive.
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1]);
        let l = liveness_with_alive(&[2, 3]).await;
        let new_pr = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Unclean)
            .await
            .expect("unclean should elect");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(2),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(2)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        };
        assert!(new_pr == expected);
    }

    #[tokio::test]
    async fn unclean_no_alive_replicas() {
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1]);
        let l = liveness_with_alive(&[]).await; // everyone dead
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Unclean)
            .await
            .unwrap_err();
        assert!(err == ElectError::NoEligibleReplica);
    }

    #[tokio::test]
    async fn unclean_isr_member_alive_returns_election_not_needed() {
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2]);
        let l = liveness_with_alive(&[1, 2]).await; // ISR has live member
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Unclean)
            .await
            .unwrap_err();
        assert!(err == ElectError::ElectionNotNeeded);
    }

    #[tokio::test]
    async fn shutdown_replacement_picks_alive_isr_member() {
        // Broker 1 is leader and wants to shut down. ISR is {1,2,3}, all alive.
        let img = img_with_partition("foo", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let new_pr = select_replacement_leader_for_shutdown(
            &img,
            &l,
            "foo",
            0,
            /*shutting_down*/ NodeId(1),
        )
        .await
        .expect("should pick replacement");
        // ISR untouched — shutting-down broker stays in ISR until dead.
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(2),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        };
        assert!(new_pr == expected);
    }

    #[tokio::test]
    async fn shutdown_replacement_skips_dead_isr_members() {
        // Broker 1 (leader) wants to drain. ISR {1,2,3} but 2 is dead.
        // Replacement should be 3.
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2, 3]);
        let l = liveness_with_alive(&[1, 3]).await;
        let new_pr = select_replacement_leader_for_shutdown(&img, &l, "foo", 0, NodeId(1))
            .await
            .expect("should pick replacement");
        assert!(new_pr.leader == 3);
        assert!(new_pr.leader_epoch == 6);
    }

    #[tokio::test]
    async fn shutdown_replacement_error_cases() {
        // Replicas are always [1, 2, 3]; leader is always broker 1.
        // (isr, alive, shutting_down, expected)
        let cases: [(&[u64], &[u64], u64, ElectError); 3] = [
            // Broker 5 wants to shut down, but leader is 1. No-op.
            (&[1, 2, 3], &[1, 2, 3, 5], 5, ElectError::ElectionNotNeeded),
            // Broker 1 wants to drain. ISR is {1} only (singleton). No
            // other broker eligible.
            (&[1], &[1, 2, 3], 1, ElectError::NoEligibleReplica),
            // Broker 1 wants to drain. ISR {1,2} but 2 is dead; 3 is alive
            // but not in ISR.
            (&[1, 2], &[1, 3], 1, ElectError::NoEligibleReplica),
        ];
        for (isr, alive, shutting_down, expected) in cases {
            let img = img_with_partition("foo", 0, 1, &[1, 2, 3], isr);
            let l = liveness_with_alive(alive).await;
            let err =
                select_replacement_leader_for_shutdown(&img, &l, "foo", 0, NodeId(shutting_down))
                    .await
                    .unwrap_err();
            assert!(
                err == expected,
                "isr {isr:?}, alive {alive:?}, shutting_down {shutting_down}"
            );
        }
    }

    #[tokio::test]
    async fn shutdown_replacement_unknown_partition() {
        let img = MetadataImage::new(Uuid::nil());
        let l = liveness_with_alive(&[1]).await;
        let err = select_replacement_leader_for_shutdown(&img, &l, "ghost", 0, NodeId(1))
            .await
            .unwrap_err();
        assert!(err == ElectError::UnknownTopicOrPartition);
    }

    #[tokio::test]
    async fn unknown_topic_returns_error() {
        let img = MetadataImage::new(Uuid::nil());
        let l = liveness_with_alive(&[]).await;
        let err = select_new_leader_for_partition(&img, &l, "ghost", 0, ElectionType::Preferred)
            .await
            .unwrap_err();
        assert!(err == ElectError::UnknownTopicOrPartition);
    }

    // ── KIP-841: automatic-failover + unclean.leader.election.enable ────────

    use std::collections::BTreeMap;

    use crabka_metadata::TopicConfigRecord;

    use super::compute_failover_changes;
    use crate::config_keys::{
        RecoveryStrategy, UNCLEAN_LEADER_ELECTION_ENABLE, UNCLEAN_RECOVERY_STRATEGY,
    };

    /// Apply a `V1TopicConfig` override on top of an existing image —
    /// matches the runtime path where `AlterConfigs` writes the record.
    fn set_topic_config(img: &mut MetadataImage, topic: &str, key: &str, value: &str) {
        let mut overrides = BTreeMap::new();
        overrides.insert(key.into(), value.into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: topic.into(),
            overrides,
        }));
    }

    /// Extract the single-element `PartitionRecord` from a one-entry change
    /// list. Panics if the list is empty or carries a non-partition record.
    fn one_partition_change(changes: &[MetadataRecord]) -> &PartitionRecord {
        assert!(
            changes.len() == 1,
            "expected exactly one change, got {changes:?}"
        );
        match &changes[0] {
            MetadataRecord::V1Partition(pr) => pr,
            other => panic!("expected V1Partition, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn failover_picks_alive_isr_member_when_available() {
        // Leader 1 dies, ISR {1, 2, 3}, both 2 and 3 alive — pick 2.
        let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(1),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(plan.recoveries.is_empty());
        let pr = one_partition_change(&plan.changes);
        // leader_epoch and partition_epoch must both bump on election.
        let expected = PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(2),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        };
        assert!(*pr == expected);
    }

    #[tokio::test]
    async fn failover_processes_dead_replica_even_when_not_in_isr() {
        // Synthetic but valid during ISR churn: dead broker is the current
        // leader/replica, while the ISR already contains only surviving peers.
        let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[2, 3]);
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }

        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(1),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;

        let pr = one_partition_change(&plan.changes);
        assert!(pr.leader == 2);
        assert!(pr.isr == vec![NodeId(2), NodeId(3)]);
        assert!(pr.leader_epoch == 6, "leader_epoch must bump on election");
    }

    #[tokio::test]
    async fn failover_ignores_partition_when_dead_broker_is_unrelated() {
        // Broker 9 is neither a replica nor an ISR member. Even if some other
        // ISR member is dead, this scan must not rewrite the partition.
        let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [1u64, 3] {
            l.record_heartbeat(n).await;
        }

        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(9),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;

        assert!(plan.changes.is_empty());
        assert!(plan.recoveries.is_empty());
    }

    #[tokio::test]
    async fn failover_leaves_partition_unavailable_when_unclean_disabled() {
        // ISR is just {1}, broker 1 dies, brokers 2/3 alive. With
        // `unclean.leader.election.enable=false` (the default) the
        // controller must not elect — partition stays unavailable.
        let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(1),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(
            plan.changes.is_empty(),
            "default-off must not emit any change, got {:?}",
            plan.changes,
        );
        assert!(plan.recoveries.is_empty());
    }

    #[tokio::test]
    async fn failover_elects_unclean_when_topic_opts_in() {
        // Same setup, but `unclean.leader.election.enable=true` on the
        // topic. Controller must elect the first alive out-of-ISR replica
        // (broker 2) as leader with singleton ISR.
        let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
        set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let metrics = crate::metrics::BrokerMetrics::new();
        let plan = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;
        assert!(plan.recoveries.is_empty());
        let pr = one_partition_change(&plan.changes);
        // Must elect the first alive replica (broker 2) with a singleton
        // ISR (KIP-841) and a bumped leader_epoch.
        let expected = PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(2),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(2)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        };
        assert!(*pr == expected);
        // Each unclean election bumps the counter exactly once.
        assert!(metrics.unclean_leader_elections_total.get() == 1);
    }

    #[tokio::test]
    async fn failover_clean_does_not_bump_unclean_counter() {
        // Clean failover (ISR non-empty with an alive member) must not
        // bump the unclean-election counter — the metric is reserved
        // for the KIP-841 data-loss footgun path.
        let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let metrics = crate::metrics::BrokerMetrics::new();
        let _ = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;
        assert!(metrics.unclean_leader_elections_total.get() == 0);
    }

    #[tokio::test]
    async fn failover_unclean_skips_when_no_alive_replica() {
        // Unclean opt-in but ALL replicas dead — no election possible.
        let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
        set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        // No heartbeats — nobody alive.
        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(1),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(
            plan.changes.is_empty(),
            "no alive replica → no election, got {:?}",
            plan.changes,
        );
        assert!(plan.recoveries.is_empty());
    }

    #[tokio::test]
    async fn failover_unclean_false_string_keeps_default_safe_behavior() {
        // Explicit `false` must behave the same as unset.
        let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
        set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "false");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(1),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(
            plan.changes.is_empty(),
            "explicit `false` keeps safe default"
        );
        assert!(plan.recoveries.is_empty());
    }

    #[tokio::test]
    async fn failover_unclean_does_not_pick_dead_broker_itself() {
        // Edge case: `dead` is in `replicas`. The unclean fallback must
        // skip it — otherwise we'd re-elect the dead broker.
        let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
        set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        // Only broker 3 alive — broker 2 also dead.
        l.record_heartbeat(3).await;
        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(1),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(plan.recoveries.is_empty());
        let pr = one_partition_change(&plan.changes);
        assert!(pr.leader == 3);
        assert!(pr.isr == vec![NodeId(3)]);
    }

    #[tokio::test]
    async fn failover_unclean_does_not_apply_when_isr_still_has_alive_member() {
        // Leader 1 dies. ISR {1, 2} but 2 is alive — clean path picks
        // broker 2 even if unclean is enabled. (The unclean branch only
        // fires when alive_isr is empty.)
        let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2]);
        set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(1),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(plan.recoveries.is_empty());
        let pr = one_partition_change(&plan.changes);
        assert!(pr.leader == 2);
        assert!(
            pr.isr == vec![NodeId(2)],
            "clean ISR-only election keeps the surviving ISR member, not a singleton-of-some-other-replica"
        );
    }

    #[tokio::test]
    async fn failover_shrinks_isr_for_partitions_where_dead_is_non_leader() {
        // Broker 2 dies; partition's leader is 1 (still alive). The
        // dead member must be dropped from ISR without bumping the
        // leader_epoch (the leader isn't changing).
        let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [1u64, 3] {
            l.record_heartbeat(n).await;
        }
        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(2),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(plan.recoveries.is_empty());
        let pr = one_partition_change(&plan.changes);
        // Leader unchanged; a non-leader-change must NOT bump leader_epoch
        // (stays 5) but does bump partition_epoch.
        let expected = PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        };
        assert!(*pr == expected);
    }

    #[tokio::test]
    async fn on_broker_dead_submits_failover_when_this_controller_is_leader() {
        let img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let source = Arc::new(TestMetadataSource::new(img, Some(NodeId(7))));
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = source.clone();
        let liveness = liveness_with_alive(&[2, 3]).await;
        let recovery = recovery_handle_for_tests();

        on_broker_dead(
            &controller,
            NodeId(7),
            NodeId(1),
            &liveness,
            &crate::metrics::BrokerMetrics::new(),
            &recovery,
        )
        .await
        .expect("broker dead handling should submit");

        let batches = source.submitted_batches().await;
        assert!(batches.len() == 1);
        let pr = one_partition_change(&batches[0]);
        assert!(pr.leader == 2);
        assert!(pr.partition_epoch == 1);
    }

    // ── KIP-112: compute_offline_dir_failover_changes ───────────────────────

    fn img_with_dirs(
        topic: &str,
        leader: u64,
        replicas: &[u64],
        isr: &[u64],
        dirs: &[uuid::Uuid],
    ) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: topic.into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).unwrap(),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.into(),
            partition: 0,
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: isr.iter().copied().map(NodeId).collect(),
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: dirs.to_vec(),
            partition_epoch: 0,
        }));
        img
    }

    #[tokio::test]
    async fn offline_dir_elects_alive_isr_member_when_leader_dir_failed() {
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2, 3], &[bad, good, good]);
        let l = ControllerLivenessState::new(std::time::Duration::from_secs(10));
        for n in [1u64, 2, 3] {
            l.record_heartbeat(n).await;
        }
        let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = super::compute_offline_dir_failover_changes(
            &img,
            NodeId(1),
            &offline,
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        let MetadataRecord::V1Partition(pr) = &plan.changes[0] else {
            panic!()
        };
        let expected = PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(2),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![bad, good, good],
            partition_epoch: 1,
        };
        assert!(*pr == expected);
    }

    #[tokio::test]
    async fn offline_dir_leaves_healthy_dir_partition_untouched() {
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2, 3], &[good, good, good]);
        let l = ControllerLivenessState::new(std::time::Duration::from_secs(10));
        for n in [1u64, 2, 3] {
            l.record_heartbeat(n).await;
        }
        let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = super::compute_offline_dir_failover_changes(
            &img,
            NodeId(1),
            &offline,
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(plan.changes.is_empty());
    }

    #[tokio::test]
    async fn offline_dir_shrinks_isr_for_non_leader_replica() {
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2, 3], &[good, bad, good]);
        let l = ControllerLivenessState::new(std::time::Duration::from_secs(10));
        for n in [1u64, 2, 3] {
            l.record_heartbeat(n).await;
        }
        let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = super::compute_offline_dir_failover_changes(
            &img,
            NodeId(2),
            &offline,
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        let MetadataRecord::V1Partition(pr) = &plan.changes[0] else {
            panic!()
        };
        let expected = PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![good, bad, good],
            partition_epoch: 1,
        };
        assert!(*pr == expected);
    }

    #[tokio::test]
    async fn offline_dir_idempotent_after_failover() {
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        // After failover: broker 1's dir is bad but broker 1 is no longer
        // leader (broker 2 is), and broker 1 is not in ISR {2,3} either.
        let img = img_with_dirs("t", 2, &[1, 2, 3], &[2, 3], &[bad, good, good]);
        let l = ControllerLivenessState::new(std::time::Duration::from_secs(10));
        for n in [1u64, 2, 3] {
            l.record_heartbeat(n).await;
        }
        let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = super::compute_offline_dir_failover_changes(
            &img,
            NodeId(1),
            &offline,
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(plan.changes.is_empty());
    }

    // ── KIP-112: compute_offline_dir_failover_changes empty-ISR branches ──────

    use super::compute_offline_dir_failover_changes;

    #[tokio::test]
    async fn offline_dir_empty_isr_balanced_strategy_defers_to_urm() {
        // Broker 1 is leader, its replica is on the bad dir, and the only other
        // ISR member (broker 2) is NOT alive — alive_isr is empty.
        // Topic sets unclean.recovery.strategy=Balanced.
        // Expect: recoveries gets the entry, changes is empty.
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        let mut img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
        set_topic_config(&mut img, "t", UNCLEAN_RECOVERY_STRATEGY, "Balanced");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        // Only broker 3 alive but it's NOT in the ISR — alive_isr = empty.
        l.record_heartbeat(3).await;
        let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = compute_offline_dir_failover_changes(
            &img,
            NodeId(1),
            &offline,
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(
            plan.changes.is_empty(),
            "Balanced strategy must not make an immediate change; got {:?}",
            plan.changes
        );
        assert!(
            plan.recoveries == vec![("t".to_string(), 0, RecoveryStrategy::Balanced)],
            "Balanced strategy must enqueue a recovery job; got {:?}",
            plan.recoveries
        );
    }

    #[tokio::test]
    async fn offline_dir_empty_isr_aggressive_strategy_defers_to_urm() {
        // Same as above but with Aggressive strategy.
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        let mut img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
        set_topic_config(&mut img, "t", UNCLEAN_RECOVERY_STRATEGY, "Aggressive");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        // broker 2 is not alive, broker 3 is alive but not in ISR.
        l.record_heartbeat(3).await;
        let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = compute_offline_dir_failover_changes(
            &img,
            NodeId(1),
            &offline,
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(plan.changes.is_empty());
        assert!(
            plan.recoveries == vec![("t".to_string(), 0, RecoveryStrategy::Aggressive)],
            "Aggressive strategy must enqueue a recovery job; got {:?}",
            plan.recoveries
        );
    }

    #[tokio::test]
    async fn offline_dir_empty_isr_unclean_enabled_elects_out_of_isr_replica() {
        // Broker 1 is leader on bad dir, broker 2 (the only ISR peer) is dead,
        // broker 3 is alive and out-of-ISR.
        // unclean.leader.election.enable=true → elect broker 3, singleton ISR,
        // bump unclean_leader_elections_total.
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        let mut img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
        set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        // broker 3 alive, broker 2 dead (no heartbeat).
        l.record_heartbeat(3).await;
        let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
        let metrics = crate::metrics::BrokerMetrics::new();
        let plan =
            compute_offline_dir_failover_changes(&img, NodeId(1), &offline, &l, &metrics).await;
        assert!(plan.recoveries.is_empty());
        let pr = one_partition_change(&plan.changes);
        // Must elect broker 3 (only alive out-of-ISR) with a singleton
        // ISR (unclean election) and a bumped leader_epoch.
        let expected = PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(3),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(3)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![bad, good, good],
            partition_epoch: 1,
        };
        assert!(*pr == expected);
        assert!(
            metrics.unclean_leader_elections_total.get() == 1,
            "unclean counter must be bumped exactly once"
        );
    }

    #[tokio::test]
    async fn offline_dir_empty_isr_no_unclean_leaves_partition_unavailable() {
        // Broker 1 is leader on bad dir, broker 2 dead, broker 3 alive but
        // not in ISR.  No recovery strategy, no unclean flag → no change.
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        l.record_heartbeat(3).await; // only 3 alive, but not in ISR
        let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = compute_offline_dir_failover_changes(
            &img,
            NodeId(1),
            &offline,
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(
            plan.changes.is_empty(),
            "default-off must not emit any change; got {:?}",
            plan.changes
        );
        assert!(plan.recoveries.is_empty());
    }

    #[tokio::test]
    async fn offline_dir_empty_isr_unclean_enabled_no_alive_replica_stays_unavailable() {
        // Broker 1 is leader on bad dir, ALL brokers are dead.
        // unclean enabled but no alive replica → no change.
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        let mut img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2], &[bad, good, good]);
        set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        // No heartbeats — nobody alive.
        let offline: std::collections::HashSet<uuid::Uuid> = [bad].into_iter().collect();
        let metrics = crate::metrics::BrokerMetrics::new();
        let plan =
            compute_offline_dir_failover_changes(&img, NodeId(1), &offline, &l, &metrics).await;
        check!(
            plan.changes.is_empty(),
            "no alive replica → no election; got {:?}",
            plan.changes
        );
        check!(plan.recoveries.is_empty());
        check!(
            metrics.unclean_leader_elections_total.get() == 0,
            "no election means no counter bump"
        );
    }

    // ── KIP-966: offset-aware recovery strategies defer to the URM ──────────

    #[tokio::test]
    async fn failover_balanced_strategy_requests_recovery_not_immediate_change() {
        // Leader 1 dies, ISR shrinks to empty after dropping it; the topic
        // opted into `unclean.recovery.strategy=Balanced`, so the failover
        // scan must NOT make a blind immediate change — it hands the
        // partition to the URM via `recoveries`.
        let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
        set_topic_config(&mut img, "t", UNCLEAN_RECOVERY_STRATEGY, "Balanced");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(1),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(
            plan.changes.is_empty(),
            "Balanced strategy must defer to the URM, not elect immediately, got {:?}",
            plan.changes,
        );
        assert!(plan.recoveries == vec![("t".to_string(), 0, RecoveryStrategy::Balanced)]);
    }

    #[tokio::test]
    async fn failover_strategy_none_still_uses_legacy_enable_flag() {
        // No recovery strategy set (defaults to None), but the legacy
        // `unclean.leader.election.enable=true` flag is on. The scan keeps
        // the KIP-841 behavior: blind pick of the first alive replica.
        let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
        set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let plan = compute_failover_changes(
            &img,
            /*dead=*/ NodeId(1),
            &l,
            &crate::metrics::BrokerMetrics::new(),
        )
        .await;
        assert!(
            plan.recoveries.is_empty(),
            "strategy None must not enqueue an offset-aware recovery",
        );
        let pr = one_partition_change(&plan.changes);
        assert!(pr.leader == 2, "legacy path picks first alive replica");
        assert!(pr.isr == vec![NodeId(2)]);
    }
}

#[cfg(test)]
#[path = "leader_failover_model.rs"]
mod leader_failover_model;
