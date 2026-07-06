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

/// Remap a partition's `directories` vector onto a new `replicas` ordering
/// (KIP-455 reassignment changes replica membership/order). `directories`
/// is index-parallel to `replicas`; a verbatim clone after the replica set
/// changes would misalign the slots and break KIP-112 offline-dir failover.
/// Surviving replicas keep their dir UUID; newly-added replicas get
/// `Uuid::nil()` (UNASSIGNED) until they report via `AssignReplicasToDirs`.
pub(crate) fn remap_directories(
    old_replicas: &[NodeId],
    old_directories: &[uuid::Uuid],
    new_replicas: &[NodeId],
) -> Vec<uuid::Uuid> {
    let old: std::collections::HashMap<NodeId, uuid::Uuid> = old_replicas
        .iter()
        .copied()
        .zip(old_directories.iter().copied())
        .collect();
    new_replicas
        .iter()
        .map(|n| old.get(n).copied().unwrap_or_else(uuid::Uuid::nil))
        .collect()
}

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

/// The pure per-partition reassignment decision: given a partition's current
/// record and the alive set, return the next `PartitionRecord` (a leader
/// handoff or a completion), or `None` to wait. No I/O. Extracted from
/// `compute_reassignment_progress` so the policy is independently unit-testable
/// and model-checkable.
pub(crate) fn reassign_one(
    pr: &PartitionRecord,
    alive: &std::collections::HashSet<NodeId>,
) -> Option<PartitionRecord> {
    let target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|r| !pr.removing_replicas.contains(r))
        .copied()
        .collect();
    if !pr.adding_replicas.iter().all(|n| pr.isr.contains(n)) {
        return None; // wait for replication
    }
    if pr.removing_replicas.contains(&pr.leader) {
        // Leader-handoff phase: pick a new leader from target ∩ isr ∩ alive.
        let new_leader = *target
            .iter()
            .find(|n| pr.isr.contains(n) && alive.contains(n))?;
        return Some(PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: new_leader,
            leader_epoch: pr.leader_epoch.next(),
            replicas: pr.replicas.clone(),
            isr: pr.isr.clone(),
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
            directories: pr.directories.clone(),
            partition_epoch: pr.partition_epoch + 1,
        });
    }
    // Completion phase: switch to the target replica set.
    let new_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| target.contains(n))
        .copied()
        .collect();
    let new_directories = remap_directories(&pr.replicas, &pr.directories, &target);
    Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader: pr.leader,
        leader_epoch: pr.leader_epoch, // unchanged: leader stays, only replica set changes
        replicas: target,
        isr: new_isr,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
    })
}

/// Pure logic: scan every in-flight reassignment; produce completion
/// or leader-handoff records for those ready to advance.
pub(crate) async fn compute_reassignment_progress(
    image: &MetadataImage,
    liveness: &ControllerLivenessState,
) -> Vec<MetadataRecord> {
    let mut updates = Vec::new();
    // Snapshot the alive set once (single lock) instead of taking the
    // liveness lock per target replica in the leader-handoff branch.
    let alive: std::collections::HashSet<NodeId> = liveness
        .alive_snapshot()
        .await
        .into_iter()
        .map(NodeId)
        .collect();
    for pr in image.reassignments_in_flight() {
        if let Some(next) = reassign_one(pr, &alive) {
            updates.push(MetadataRecord::V1Partition(next));
        }
    }
    updates
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use assert2::{assert, check};
    use crabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord};
    use uuid::Uuid;

    use super::*;

    struct MockReassignmentController {
        is_leader: AtomicBool,
        current: Mutex<Arc<MetadataImage>>,
        image_tx: watch::Sender<Arc<MetadataImage>>,
        submitted: Mutex<Vec<Vec<MetadataRecord>>>,
    }

    impl MockReassignmentController {
        fn new(is_leader: bool, image: Arc<MetadataImage>) -> Self {
            let (image_tx, _) = watch::channel(image.clone());
            Self {
                is_leader: AtomicBool::new(is_leader),
                current: Mutex::new(image),
                image_tx,
                submitted: Mutex::new(Vec::new()),
            }
        }

        fn publish(&self, image: Arc<MetadataImage>) {
            *self.current.lock().expect("current image mutex poisoned") = image.clone();
            self.image_tx
                .send(image)
                .expect("run loop is watching image");
        }

        fn submitted_len(&self) -> usize {
            self.submitted
                .lock()
                .expect("submitted mutex poisoned")
                .len()
        }

        fn submissions(&self) -> Vec<Vec<MetadataRecord>> {
            self.submitted
                .lock()
                .expect("submitted mutex poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl ReassignmentController for MockReassignmentController {
        fn is_leader(&self) -> bool {
            self.is_leader.load(Ordering::SeqCst)
        }

        fn current_image(&self) -> Arc<MetadataImage> {
            self.current
                .lock()
                .expect("current image mutex poisoned")
                .clone()
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            self.image_tx.subscribe()
        }

        async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String> {
            self.submitted
                .lock()
                .expect("submitted mutex poisoned")
                .push(records);
            Ok(())
        }
    }

    async fn wait_for_submission_count(controller: &MockReassignmentController, count: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if controller.submitted_len() >= count {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reassignment run loop did not submit expected records");
    }

    fn img(
        replicas: &[u64],
        isr: &[u64],
        adding: &[u64],
        removing: &[u64],
        leader: u64,
    ) -> Arc<MetadataImage> {
        let mut img = MetadataImage::new(Uuid::nil());
        for n in 1..=6u64 {
            img.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: NodeId(n),
                    broker_epoch: 0,
                    incarnation_id: Uuid::nil(),
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
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: isr.iter().copied().map(NodeId).collect(),
            leader_epoch: crabka_metadata::LeaderEpoch(5),
            adding_replicas: adding.iter().copied().map(NodeId).collect(),
            removing_replicas: removing.iter().copied().map(NodeId).collect(),
            directories: vec![],
            partition_epoch: 0,
        }));
        Arc::new(img)
    }

    async fn liveness(alive: &[u64]) -> ControllerLivenessState {
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

    #[test]
    fn remap_directories_preserves_slot_alignment_on_replica_removal() {
        let da = uuid::Uuid::from_u128(0xA);
        let db = uuid::Uuid::from_u128(0xB);
        let dc = uuid::Uuid::from_u128(0xC);
        // replicas [1,2,3] dirs [dA,dB,dC]; reassignment removes broker 2.
        let new = remap_directories(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[da, db, dc],
            &[NodeId(1), NodeId(3)],
        );
        // broker 1 keeps dA at slot 0; broker 3 keeps dC at slot 1 (NOT dB).
        assert!(new == vec![da, dc]);
    }

    #[test]
    fn remap_directories_assigns_nil_to_new_replica() {
        let da = uuid::Uuid::from_u128(0xA);
        // replicas [1] dirs [dA]; add broker 2 (no dir yet).
        let new = remap_directories(&[NodeId(1)], &[da], &[NodeId(1), NodeId(2)]);
        assert!(new == vec![da, uuid::Uuid::nil()]);
    }

    /// Build an image with explicit directories, to test that
    /// `compute_reassignment_progress` keeps directories aligned after
    /// completion removes a replica from the set.
    fn img_with_dirs(
        replicas: &[u64],
        isr: &[u64],
        adding: &[u64],
        removing: &[u64],
        leader: u64,
        directories: &[Uuid],
    ) -> Arc<MetadataImage> {
        let mut image = MetadataImage::new(Uuid::nil());
        for n in 1..=6u64 {
            image.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: NodeId(n),
                    broker_epoch: 0,
                    incarnation_id: Uuid::nil(),
                    host: String::new(),
                    port: 0,
                    rack: None,
                    endpoints: vec![],
                },
            ));
        }
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).expect("replication factor fits i16"),
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: isr.iter().copied().map(NodeId).collect(),
            leader_epoch: crabka_metadata::LeaderEpoch(5),
            adding_replicas: adding.iter().copied().map(NodeId).collect(),
            removing_replicas: removing.iter().copied().map(NodeId).collect(),
            directories: directories.to_vec(),
            partition_epoch: 0,
        }));
        Arc::new(image)
    }

    #[tokio::test]
    async fn completion_preserves_directory_slot_alignment() {
        // replicas=[1,2,3], adding=[3], removing=[2], all in ISR.
        // directories=[dA, dB, dC] — slot 0→broker1, 1→broker2, 2→broker3.
        // After completion target=[1,3]; expected dirs=[dA, dC].
        let da = Uuid::from_u128(0xA);
        let db = Uuid::from_u128(0xB);
        let dc = Uuid::from_u128(0xC);
        let image = img_with_dirs(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1, &[da, db, dc]);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&image, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        // Slot 0 → broker 1 → dA; slot 1 → broker 3 → dC (NOT dB).
        check!(pr.replicas == vec![NodeId(1), NodeId(3)]);
        check!(pr.directories == vec![da, dc]);
        check!(pr.partition_epoch == 1);
    }

    #[tokio::test]
    async fn complete_when_adding_in_isr_writes_target() {
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        // leader and leader_epoch are unchanged (leader didn't change).
        check!(pr.replicas == vec![NodeId(1), NodeId(3)]);
        check!(pr.adding_replicas == Vec::<NodeId>::new());
        check!(pr.removing_replicas == Vec::<NodeId>::new());
        check!(pr.isr == vec![NodeId(1), NodeId(3)]);
        check!(pr.leader == 1);
        check!(pr.leader_epoch == crabka_metadata::LeaderEpoch(5));
        check!(pr.partition_epoch == 1);
    }

    #[tokio::test]
    async fn no_update_emitted_when_waiting_idle_or_no_alive_target() {
        // (case, replicas, isr, adding, removing, leader, alive) — every case
        // should wait / stay idle: compute_reassignment_progress emits nothing.
        let cases = [
            // Adding replica 3 not yet in ISR → wait.
            (
                "adding_not_in_isr",
                vec![1, 2, 3],
                vec![1, 2],
                vec![3],
                vec![2],
                1,
                vec![1, 2, 3],
            ),
            // leader=2, removing=[2]; only target replicas {1,3} in isr but
            // none alive (only 2 alive) — wait.
            (
                "leader_handoff_no_alive_target_replica",
                vec![1, 2, 3],
                vec![1, 2, 3],
                vec![3],
                vec![2],
                2,
                vec![2],
            ),
            // No reassignment in flight → idle partition emits no update.
            (
                "idle_partition",
                vec![1, 2, 3],
                vec![1, 2, 3],
                vec![],
                vec![],
                1,
                vec![1, 2, 3],
            ),
        ];
        for (case, replicas, isr, adding, removing, leader, alive) in cases {
            let img = img(&replicas, &isr, &adding, &removing, leader);
            let l = liveness(&alive).await;
            let updates = compute_reassignment_progress(&img, &l).await;
            assert!(
                updates.is_empty(),
                "case {case}: should wait; got {updates:?}"
            );
        }
    }

    #[tokio::test]
    async fn leader_handoff_when_leader_in_removing() {
        // leader=2, removing=[2]; new leader must come from target ∩ isr = {1,3} ∩ {1,2,3} = {1,3}.
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 2);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        assert!(
            pr.leader == 1 || pr.leader == 3,
            "leader was {}",
            pr.leader.0
        );
        // leader_epoch bumped; replica set unchanged — completion happens
        // next tick.
        check!(pr.leader_epoch == crabka_metadata::LeaderEpoch(6));
        check!(pr.partition_epoch == 1);
        check!(pr.adding_replicas == vec![NodeId(3)]);
        check!(pr.removing_replicas == vec![NodeId(2)]);
    }

    #[tokio::test]
    async fn run_submits_ready_reassignment_on_image_change() {
        let initial = img(&[1], &[1], &[], &[], 1);
        let controller = Arc::new(MockReassignmentController::new(true, initial));
        let l = Arc::new(liveness(&[1, 2, 3]).await);
        let shutdown = CancellationToken::new();
        let task_controller: Arc<dyn ReassignmentController> = controller.clone();
        let task = tokio::spawn(run(task_controller, l, shutdown.clone()));

        tokio::task::yield_now().await;
        controller.publish(img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1));
        wait_for_submission_count(&controller, 1).await;

        shutdown.cancel();
        task.await.expect("reassignment task panicked");
        let submissions = controller.submissions();
        assert!(submissions.len() == 1);
        assert!(submissions[0].len() == 1);
        let pr = first_partition(&submissions[0][0]);
        assert!(pr.replicas == vec![NodeId(1), NodeId(3)]);
        assert!(pr.partition_epoch == 1);
    }

    #[tokio::test]
    async fn run_skips_ready_reassignment_when_not_leader() {
        let initial = img(&[1], &[1], &[], &[], 1);
        let controller = Arc::new(MockReassignmentController::new(false, initial));
        let l = Arc::new(liveness(&[1, 2, 3]).await);
        let shutdown = CancellationToken::new();
        let task_controller: Arc<dyn ReassignmentController> = controller.clone();
        let task = tokio::spawn(run(task_controller, l, shutdown.clone()));

        tokio::task::yield_now().await;
        controller.publish(img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1));
        let observed = tokio::time::timeout(
            Duration::from_millis(100),
            wait_for_submission_count(&controller, 1),
        )
        .await;

        shutdown.cancel();
        task.await.expect("reassignment task panicked");
        assert!(observed.is_err(), "non-leader must not submit changes");
        assert!(controller.submitted_len() == 0);
    }

    #[tokio::test]
    async fn multiple_partitions_handled_independently() {
        let mut img_inner = MetadataImage::new(Uuid::nil());
        for n in 1..=6u64 {
            img_inner.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: NodeId(n),
                    broker_epoch: 0,
                    incarnation_id: Uuid::nil(),
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
                leader: NodeId(1),
                replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
                isr: vec![NodeId(1), NodeId(2), NodeId(3)],
                leader_epoch: crabka_metadata::LeaderEpoch(5),
                adding_replicas: vec![NodeId(3)],
                removing_replicas: vec![NodeId(2)],
                directories: vec![],
                partition_epoch: 0,
            }));
        }
        let img = Arc::new(img_inner);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 2);
    }

    #[tokio::test]
    async fn target_includes_only_replicas_minus_removing() {
        // adding=[4,5], removing=[1,2], replicas=[1,2,3,4,5].
        // target = [3,4,5]. isr ⊇ adding required; isr=[1,2,3,4,5].
        let img = img(&[1, 2, 3, 4, 5], &[1, 2, 3, 4, 5], &[4, 5], &[1, 2], 3);
        let l = liveness(&[1, 2, 3, 4, 5]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        assert!(pr.replicas == vec![NodeId(3), NodeId(4), NodeId(5)]);
        assert!(pr.isr == vec![NodeId(3), NodeId(4), NodeId(5)]);
    }

    #[tokio::test]
    async fn isr_intersection_when_some_targets_not_in_isr() {
        // adding=[4], removing=[2]; isr=[1,2,3,4]; target=[1,3,4].
        // new_isr = isr ∩ target = [1,3,4].
        let img = img(&[1, 2, 3, 4], &[1, 2, 3, 4], &[4], &[2], 1);
        let l = liveness(&[1, 2, 3, 4]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        assert!(pr.isr == vec![NodeId(1), NodeId(3), NodeId(4)]);
    }
}

#[cfg(test)]
#[path = "reassignment_model.rs"]
mod reassignment_model;
