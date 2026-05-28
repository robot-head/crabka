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
use crabka_raft::{ControllerHandle, NodeId};
use tracing::warn;

use crate::config_keys::UNCLEAN_LEADER_ELECTION_ENABLE;
use crate::error::BrokerError;
use crate::heartbeat::controller_state::ControllerLivenessState;

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
) -> Vec<MetadataRecord> {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    for topic in image.topics() {
        for pr in image.partitions_of(&topic.name) {
            if !pr.replicas.contains(&dead) && !pr.isr.contains(&dead) {
                continue;
            }
            // Compute the new ISR after dropping the dead broker AND any
            // other replicas that aren't alive.
            let mut alive_isr: Vec<NodeId> = Vec::with_capacity(pr.isr.len());
            for n in &pr.isr {
                if *n != dead && liveness.is_alive(*n).await {
                    alive_isr.push(*n);
                }
            }
            let needs_election = pr.leader == dead;
            if needs_election {
                if let Some(&new_leader) = alive_isr.first() {
                    changes.push(MetadataRecord::V1Partition(PartitionRecord {
                        topic: pr.topic.clone(),
                        partition: pr.partition,
                        leader: new_leader,
                        replicas: pr.replicas.clone(),
                        isr: alive_isr,
                        leader_epoch: pr.leader_epoch + 1,
                        adding_replicas: pr.adding_replicas.clone(),
                        removing_replicas: pr.removing_replicas.clone(),
                    }));
                } else if unclean_election_enabled(image, &pr.topic) {
                    // KIP-841: ISR is dead but operator has opted into
                    // possible data loss. Pick the first alive replica
                    // (in or out of ISR) and elect with singleton-ISR.
                    let mut elected: Option<NodeId> = None;
                    for &n in &pr.replicas {
                        if n != dead && liveness.is_alive(n).await {
                            elected = Some(n);
                            break;
                        }
                    }
                    if let Some(new_leader) = elected {
                        warn!(
                            topic = %pr.topic, partition = pr.partition, leader = new_leader,
                            "unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                        );
                        changes.push(MetadataRecord::V1Partition(PartitionRecord {
                            topic: pr.topic.clone(),
                            partition: pr.partition,
                            leader: new_leader,
                            replicas: pr.replicas.clone(),
                            isr: vec![new_leader],
                            leader_epoch: pr.leader_epoch + 1,
                            adding_replicas: pr.adding_replicas.clone(),
                            removing_replicas: pr.removing_replicas.clone(),
                        }));
                    } else {
                        warn!(
                            topic = %pr.topic, partition = pr.partition,
                            "unclean leader election enabled but no alive replica; partition unavailable"
                        );
                    }
                } else {
                    warn!(
                        topic = %pr.topic, partition = pr.partition,
                        "no live ISR replica; partition unavailable (unclean.leader.election.enable=false)"
                    );
                }
            } else if alive_isr.len() < pr.isr.len() {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr: alive_isr,
                    leader_epoch: pr.leader_epoch,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                }));
            }
        }
    }
    changes
}

/// Called when the liveness ticker observes `AliveToDead(dead)`. Scans
/// every partition where `dead` is leader OR in the ISR; proposes
/// updated `PartitionRecord`s.
///
/// No-op unless `controller` is currently the openraft leader (only
/// the leader can `submit_change`).
pub(crate) async fn on_broker_dead(
    controller: &Arc<ControllerHandle>,
    node_id: NodeId,
    dead: NodeId,
    liveness: &Arc<ControllerLivenessState>,
) -> Result<(), BrokerError> {
    let is_controller_leader = controller
        .watch_leader()
        .borrow()
        .is_some_and(|n| n == node_id);
    if !is_controller_leader {
        return Ok(());
    }

    let image = controller.current_image();
    let changes = compute_failover_changes(&image, dead, liveness).await;
    if !changes.is_empty() {
        controller
            .submit_change(changes)
            .await
            .map_err(|e| BrokerError::Replication(format!("submit_change: {e}")))?;
    }
    Ok(())
}

/// Called when the liveness ticker observes `DeadToAlive(alive)`. For
/// slice-10b this is a no-op — ISR expand happens organically via
/// `isr_maintenance` once the rejoined broker's replicator catches up.
/// The hook is here for future enhancements (e.g. auto-rebalance).
#[allow(clippy::unused_async)]
pub(crate) async fn on_broker_alive(
    _controller: &Arc<ControllerHandle>,
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
        if liveness.is_alive(n).await {
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
        leader_epoch: pr.leader_epoch + 1,
        adding_replicas: pr.adding_replicas.clone(),
        removing_replicas: pr.removing_replicas.clone(),
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
            if !liveness.is_alive(preferred).await {
                return Err(ElectError::PreferredNotAlive);
            }
            Ok(PartitionRecord {
                topic: pr.topic.clone(),
                partition: pr.partition,
                leader: preferred,
                replicas: pr.replicas.clone(),
                isr: pr.isr.clone(),
                leader_epoch: pr.leader_epoch + 1,
                adding_replicas: pr.adding_replicas.clone(),
                removing_replicas: pr.removing_replicas.clone(),
            })
        }
        ElectionType::Unclean => {
            // Bail if any ISR member is alive — UNCLEAN is meant for
            // catastrophic ISR loss, not routine rebalances.
            for &n in &pr.isr {
                if liveness.is_alive(n).await {
                    return Err(ElectError::ElectionNotNeeded);
                }
            }
            // Find the first alive replica, in or out of ISR.
            for &n in &pr.replicas {
                if liveness.is_alive(n).await {
                    return Ok(PartitionRecord {
                        topic: pr.topic.clone(),
                        partition: pr.partition,
                        leader: n,
                        replicas: pr.replicas.clone(),
                        isr: vec![n],
                        leader_epoch: pr.leader_epoch + 1,
                        adding_replicas: pr.adding_replicas.clone(),
                        removing_replicas: pr.removing_replicas.clone(),
                    });
                }
            }
            Err(ElectError::NoEligibleReplica)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
    use uuid::Uuid;

    use super::{
        ControllerLivenessState, ElectError, ElectionType, select_new_leader_for_partition,
        select_replacement_leader_for_shutdown,
    };
    use crabka_raft::NodeId;

    fn img_with_partition(
        topic: &str,
        partition: i32,
        leader: NodeId,
        replicas: &[NodeId],
        isr: &[NodeId],
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
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            leader_epoch: 5,
            adding_replicas: vec![],
            removing_replicas: vec![],
        }));
        img
    }

    async fn liveness_with_alive(alive: &[NodeId]) -> Arc<ControllerLivenessState> {
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for &n in alive {
            l.record_heartbeat(n).await;
        }
        Arc::new(l)
    }

    #[tokio::test]
    async fn preferred_happy_path() {
        let img = img_with_partition("foo", 0, /*leader*/ 2, &[1, 2, 3], &[1, 2, 3]);
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let new_pr = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
            .await
            .expect("should elect");
        assert_eq!(new_pr.leader, 1);
        assert_eq!(new_pr.isr, vec![1, 2, 3]);
        assert_eq!(new_pr.leader_epoch, 6);
    }

    #[tokio::test]
    async fn preferred_already_leader() {
        let img = img_with_partition("foo", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::PreferredAlreadyLeader);
    }

    #[tokio::test]
    async fn preferred_not_in_isr() {
        let img = img_with_partition("foo", 0, 2, &[1, 2, 3], &[2, 3]);
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::PreferredNotInIsr);
    }

    #[tokio::test]
    async fn preferred_not_alive() {
        let img = img_with_partition("foo", 0, 2, &[1, 2, 3], &[1, 2, 3]);
        let l = liveness_with_alive(&[2, 3]).await; // 1 dead
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::PreferredNotAlive);
    }

    #[tokio::test]
    async fn unclean_happy_path() {
        // ISR is just {1}, broker 1 is dead, brokers 2/3 are alive.
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1]);
        let l = liveness_with_alive(&[2, 3]).await;
        let new_pr = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Unclean)
            .await
            .expect("unclean should elect");
        assert_eq!(new_pr.leader, 2);
        assert_eq!(new_pr.isr, vec![2]);
        assert_eq!(new_pr.leader_epoch, 6);
    }

    #[tokio::test]
    async fn unclean_no_alive_replicas() {
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1]);
        let l = liveness_with_alive(&[]).await; // everyone dead
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Unclean)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::NoEligibleReplica);
    }

    #[tokio::test]
    async fn unclean_isr_member_alive_returns_election_not_needed() {
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2]);
        let l = liveness_with_alive(&[1, 2]).await; // ISR has live member
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Unclean)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::ElectionNotNeeded);
    }

    #[tokio::test]
    async fn shutdown_replacement_picks_alive_isr_member() {
        // Broker 1 is leader and wants to shut down. ISR is {1,2,3}, all alive.
        let img = img_with_partition("foo", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]);
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let new_pr =
            select_replacement_leader_for_shutdown(&img, &l, "foo", 0, /*shutting_down*/ 1)
                .await
                .expect("should pick replacement");
        assert_eq!(new_pr.leader, 2);
        // ISR untouched — shutting-down broker stays in ISR until dead.
        assert_eq!(new_pr.isr, vec![1, 2, 3]);
        assert_eq!(new_pr.leader_epoch, 6);
    }

    #[tokio::test]
    async fn shutdown_replacement_skips_dead_isr_members() {
        // Broker 1 (leader) wants to drain. ISR {1,2,3} but 2 is dead.
        // Replacement should be 3.
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2, 3]);
        let l = liveness_with_alive(&[1, 3]).await;
        let new_pr = select_replacement_leader_for_shutdown(&img, &l, "foo", 0, 1)
            .await
            .expect("should pick replacement");
        assert_eq!(new_pr.leader, 3);
        assert_eq!(new_pr.leader_epoch, 6);
    }

    #[tokio::test]
    async fn shutdown_replacement_election_not_needed_when_not_leader() {
        // Broker 5 wants to shut down, but leader is 1. No-op.
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2, 3]);
        let l = liveness_with_alive(&[1, 2, 3, 5]).await;
        let err = select_replacement_leader_for_shutdown(&img, &l, "foo", 0, 5)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::ElectionNotNeeded);
    }

    #[tokio::test]
    async fn shutdown_replacement_no_other_isr_alive() {
        // Broker 1 wants to drain. ISR is {1} only (singleton). No
        // other broker eligible.
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1]);
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let err = select_replacement_leader_for_shutdown(&img, &l, "foo", 0, 1)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::NoEligibleReplica);
    }

    #[tokio::test]
    async fn shutdown_replacement_other_isr_member_dead_falls_to_no_eligible() {
        // Broker 1 wants to drain. ISR {1,2} but 2 is dead.
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2]);
        let l = liveness_with_alive(&[1, 3]).await; // 2 dead, 3 alive but not in ISR
        let err = select_replacement_leader_for_shutdown(&img, &l, "foo", 0, 1)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::NoEligibleReplica);
    }

    #[tokio::test]
    async fn shutdown_replacement_unknown_partition() {
        let img = MetadataImage::new(Uuid::nil());
        let l = liveness_with_alive(&[1]).await;
        let err = select_replacement_leader_for_shutdown(&img, &l, "ghost", 0, 1)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::UnknownTopicOrPartition);
    }

    #[tokio::test]
    async fn unknown_topic_returns_error() {
        let img = MetadataImage::new(Uuid::nil());
        let l = liveness_with_alive(&[]).await;
        let err = select_new_leader_for_partition(&img, &l, "ghost", 0, ElectionType::Preferred)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::UnknownTopicOrPartition);
    }

    // ── KIP-841: automatic-failover + unclean.leader.election.enable ────────

    use super::compute_failover_changes;
    use crate::config_keys::UNCLEAN_LEADER_ELECTION_ENABLE;
    use crabka_metadata::TopicConfigRecord;
    use std::collections::BTreeMap;

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
        assert_eq!(
            changes.len(),
            1,
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
        let changes = compute_failover_changes(&img, /*dead=*/ 1, &l).await;
        let pr = one_partition_change(&changes);
        assert_eq!(pr.leader, 2);
        assert_eq!(pr.isr, vec![2, 3]);
        assert_eq!(pr.leader_epoch, 6, "leader_epoch must bump on election");
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
        let changes = compute_failover_changes(&img, /*dead=*/ 1, &l).await;
        assert!(
            changes.is_empty(),
            "default-off must not emit any change, got {changes:?}",
        );
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
        let changes = compute_failover_changes(&img, /*dead=*/ 1, &l).await;
        let pr = one_partition_change(&changes);
        assert_eq!(pr.leader, 2, "must elect first alive replica (broker 2)");
        assert_eq!(
            pr.isr,
            vec![2],
            "unclean election installs singleton ISR (KIP-841)",
        );
        assert_eq!(pr.leader_epoch, 6);
    }

    #[tokio::test]
    async fn failover_unclean_skips_when_no_alive_replica() {
        // Unclean opt-in but ALL replicas dead — no election possible.
        let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
        set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        // No heartbeats — nobody alive.
        let changes = compute_failover_changes(&img, /*dead=*/ 1, &l).await;
        assert!(
            changes.is_empty(),
            "no alive replica → no election, got {changes:?}",
        );
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
        let changes = compute_failover_changes(&img, /*dead=*/ 1, &l).await;
        assert!(changes.is_empty(), "explicit `false` keeps safe default");
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
        let changes = compute_failover_changes(&img, /*dead=*/ 1, &l).await;
        let pr = one_partition_change(&changes);
        assert_eq!(pr.leader, 3);
        assert_eq!(pr.isr, vec![3]);
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
        let changes = compute_failover_changes(&img, /*dead=*/ 1, &l).await;
        let pr = one_partition_change(&changes);
        assert_eq!(pr.leader, 2);
        assert_eq!(
            pr.isr,
            vec![2],
            "clean ISR-only election keeps the surviving ISR member, not a singleton-of-some-other-replica",
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
        let changes = compute_failover_changes(&img, /*dead=*/ 2, &l).await;
        let pr = one_partition_change(&changes);
        assert_eq!(pr.leader, 1, "leader unchanged");
        assert_eq!(pr.isr, vec![1, 3]);
        assert_eq!(
            pr.leader_epoch, 5,
            "non-leader-change must NOT bump leader_epoch"
        );
    }
}
