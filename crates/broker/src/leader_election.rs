//! Controller-side leader-election scan. Called by the liveness ticker
//! when a broker transitions `alive → dead`. Scans every partition where
//! the dead broker is leader, picks the first alive ISR replica as new
//! leader, bumps `leader_epoch`, and emits the new `PartitionRecord`s
//! through openraft.

#![allow(dead_code)]

use std::sync::Arc;

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_raft::{ControllerHandle, NodeId};
use tracing::warn;

use crate::error::BrokerError;
use crate::heartbeat::controller_state::ControllerLivenessState;

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
                let Some(&new_leader) = alive_isr.first() else {
                    warn!(
                        topic = %pr.topic, partition = pr.partition,
                        "no live ISR replica; partition unavailable"
                    );
                    continue;
                };
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: new_leader,
                    replicas: pr.replicas.clone(),
                    isr: alive_isr,
                    leader_epoch: pr.leader_epoch + 1,
                }));
            } else if alive_isr.len() < pr.isr.len() {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr: alive_isr,
                    leader_epoch: pr.leader_epoch,
                }));
            }
        }
    }
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
    PreferredNotInIsr,
    PreferredNotAlive,
    NoEligibleReplica,
    NotControllerLeader,
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
            })
        }
        ElectionType::Unclean => {
            // Bail if any ISR member is alive — UNCLEAN is meant for
            // catastrophic ISR loss, not routine rebalances.
            for &n in &pr.isr {
                if liveness.is_alive(n).await {
                    return Err(ElectError::PreferredAlreadyLeader);
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
        assert_eq!(err, ElectError::PreferredAlreadyLeader);
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
}
