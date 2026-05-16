//! KIP-455 reassignment-completion background task.
//!
//! Runs on the controller leader. Watches the metadata image; when a
//! reassignment's `adding_replicas` are all in ISR, atomically
//! transitions to the target replica set. If the current leader is in
//! `removing_replicas`, hands off leadership first to a target replica
//! in ISR.

#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
use crabka_raft::NodeId;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::heartbeat::controller_state::ControllerLivenessState;

/// Minimal trait for the controller surface this task needs. Lets unit
/// tests inject a mock without spinning up real raft.
#[async_trait]
pub(crate) trait ReassignmentController: Send + Sync {
    fn is_leader(&self) -> bool;
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

/// Background task entry point. Driven by image-apply events.
pub(crate) async fn run(
    controller: Arc<dyn ReassignmentController>,
    liveness: Arc<ControllerLivenessState>,
    shutdown: CancellationToken,
) {
    let mut watcher = controller.watch_image();
    loop {
        tokio::select! {
            result = watcher.changed() => {
                if result.is_err() {
                    // Channel closed — controller dropped.
                    break;
                }
            },
            () = shutdown.cancelled() => {
                info!("reassignment task shutting down");
                return;
            }
        }
        if !controller.is_leader() {
            debug!("reassignment tick skipped: not controller leader");
            continue;
        }
        let image = controller.current_image();
        let updates = compute_reassignment_progress(&image, &liveness).await;
        if !updates.is_empty() {
            info!(
                count = updates.len(),
                "reassignment: submitting completion updates"
            );
            if let Err(e) = controller.submit_change(updates).await {
                warn!(error = %e, "reassignment: submit failed");
            }
        }
    }
}

/// Pure logic: scan every in-flight reassignment; produce completion
/// or leader-handoff records for those ready to advance.
pub(crate) async fn compute_reassignment_progress(
    image: &MetadataImage,
    liveness: &ControllerLivenessState,
) -> Vec<MetadataRecord> {
    let mut updates = Vec::new();
    for pr in image.reassignments_in_flight() {
        let target: Vec<NodeId> = pr
            .replicas
            .iter()
            .filter(|r| !pr.removing_replicas.contains(r))
            .copied()
            .collect();
        let adding_caught_up = pr.adding_replicas.iter().all(|n| pr.isr.contains(n));
        if !adding_caught_up {
            continue; // wait for replication
        }
        if pr.removing_replicas.contains(&pr.leader) {
            // Leader handoff phase. Find an eligible new leader in target ∩ isr that is alive.
            let mut new_leader: Option<NodeId> = None;
            for n in &target {
                if pr.isr.contains(n) && liveness.is_alive(*n).await {
                    new_leader = Some(*n);
                    break;
                }
            }
            if let Some(leader) = new_leader {
                updates.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader,
                    leader_epoch: pr.leader_epoch + 1,
                    replicas: pr.replicas.clone(),
                    isr: pr.isr.clone(),
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                }));
            }
            // Whether or not we found a leader, don't also try to complete this tick.
            continue;
        }
        // Completion phase.
        let new_isr: Vec<NodeId> = pr
            .isr
            .iter()
            .filter(|n| target.contains(n))
            .copied()
            .collect();
        updates.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: pr.leader,
            leader_epoch: pr.leader_epoch, // unchanged: leader stays, only replica set changes
            replicas: target,
            isr: new_isr,
            adding_replicas: vec![],
            removing_replicas: vec![],
        }));
    }
    updates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord};
    use std::time::Duration;
    use uuid::Uuid;

    fn img(
        replicas: &[NodeId],
        isr: &[NodeId],
        adding: &[NodeId],
        removing: &[NodeId],
        leader: NodeId,
    ) -> Arc<MetadataImage> {
        let mut img = MetadataImage::new(Uuid::nil());
        for n in 1..=6 {
            img.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: n,
                    host: String::new(),
                    port: 0,
                    rack: None,
                    endpoints: vec![],
                },
            ));
        }
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).expect("replication factor fits i16"),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            leader_epoch: 5,
            adding_replicas: adding.to_vec(),
            removing_replicas: removing.to_vec(),
        }));
        Arc::new(img)
    }

    async fn liveness(alive: &[NodeId]) -> ControllerLivenessState {
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in alive {
            l.record_heartbeat(*n).await;
        }
        l
    }

    fn first_partition(rec: &MetadataRecord) -> &PartitionRecord {
        match rec {
            MetadataRecord::V1Partition(p) => p,
            _ => panic!("expected V1Partition"),
        }
    }

    #[tokio::test]
    async fn complete_when_adding_in_isr_writes_target() {
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 1);
        let pr = first_partition(&updates[0]);
        assert_eq!(pr.replicas, vec![1, 3]);
        assert_eq!(pr.adding_replicas, Vec::<NodeId>::new());
        assert_eq!(pr.removing_replicas, Vec::<NodeId>::new());
        assert_eq!(pr.isr, vec![1, 3]);
        assert_eq!(pr.leader, 1); // unchanged
        assert_eq!(pr.leader_epoch, 5); // unchanged (leader didn't change)
    }

    #[tokio::test]
    async fn wait_when_adding_not_in_isr() {
        let img = img(&[1, 2, 3], &[1, 2], &[3], &[2], 1);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.is_empty(), "should wait; got {updates:?}");
    }

    #[tokio::test]
    async fn leader_handoff_when_leader_in_removing() {
        // leader=2, removing=[2]; new leader must come from target ∩ isr = {1,3} ∩ {1,2,3} = {1,3}.
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 2);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 1);
        let pr = first_partition(&updates[0]);
        assert!(pr.leader == 1 || pr.leader == 3, "leader was {}", pr.leader);
        assert_eq!(pr.leader_epoch, 6); // bumped
        // Replica set unchanged — completion happens next tick.
        assert_eq!(pr.adding_replicas, vec![3]);
        assert_eq!(pr.removing_replicas, vec![2]);
    }

    #[tokio::test]
    async fn leader_handoff_skipped_if_no_alive_target_replica() {
        // leader=2, removing=[2]; only target replicas {1,3} in isr but
        // none alive — wait.
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 2);
        let l = liveness(&[2]).await; // only 2 alive
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.is_empty());
    }

    #[tokio::test]
    async fn idle_partition_emits_no_update() {
        let img = img(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.is_empty());
    }

    #[tokio::test]
    async fn multiple_partitions_handled_independently() {
        let mut img_inner = MetadataImage::new(Uuid::nil());
        for n in 1..=6 {
            img_inner.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: n,
                    host: String::new(),
                    port: 0,
                    rack: None,
                    endpoints: vec![],
                },
            ));
        }
        for name in ["foo", "bar"] {
            img_inner.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 3,
            }));
            img_inner.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: name.into(),
                partition: 0,
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
                leader_epoch: 5,
                adding_replicas: vec![3],
                removing_replicas: vec![2],
            }));
        }
        let img = Arc::new(img_inner);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 2);
    }

    #[tokio::test]
    async fn target_includes_only_replicas_minus_removing() {
        // adding=[4,5], removing=[1,2], replicas=[1,2,3,4,5].
        // target = [3,4,5]. isr ⊇ adding required; isr=[1,2,3,4,5].
        let img = img(&[1, 2, 3, 4, 5], &[1, 2, 3, 4, 5], &[4, 5], &[1, 2], 3);
        let l = liveness(&[1, 2, 3, 4, 5]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 1);
        let pr = first_partition(&updates[0]);
        assert_eq!(pr.replicas, vec![3, 4, 5]);
        assert_eq!(pr.isr, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn isr_intersection_when_some_targets_not_in_isr() {
        // adding=[4], removing=[2]; isr=[1,2,3,4]; target=[1,3,4].
        // new_isr = isr ∩ target = [1,3,4].
        let img = img(&[1, 2, 3, 4], &[1, 2, 3, 4], &[4], &[2], 1);
        let l = liveness(&[1, 2, 3, 4]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 1);
        let pr = first_partition(&updates[0]);
        assert_eq!(pr.isr, vec![1, 3, 4]);
    }
}
