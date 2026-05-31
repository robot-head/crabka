//! Unified group-coordinator subsystem (KIP-848 64d-B). Shared infra and
//! persistence for both the classic and next-gen group protocols.
//!
//! [`GroupCoordinator`] is the single owner of the next-gen consumer-group
//! machinery: it spawns per-group actors, tracks each group's locked type,
//! and replays persisted state during bootstrap.
pub mod actor;
pub mod assignor;
pub mod config;
pub(crate) mod consumer_state;
pub(crate) mod group;
pub mod offsets_log;
pub(crate) mod persistence;
pub mod persistence_next_gen;
pub mod reconciler;
pub mod share;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::oneshot;

use actor::{GroupActorHandle, GroupActorMessage, MetadataProvider};
use config::NextGenConfig;
use offsets_log::OffsetsLog;
use share::actor::{ShareGroupActorHandle, ShareGroupActorMessage};
use share::config::ShareGroupConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupType {
    Classic,
    NextGen,
    Share,
}

#[derive(Debug)]
pub struct GroupCoordinator {
    pub config: Arc<NextGenConfig>,
    pub share_config: Arc<ShareGroupConfig>,
    pub metadata: Arc<dyn MetadataProvider>,
    pub offsets_log: Arc<dyn OffsetsLog>,
    pub groups: Arc<DashMap<String, Arc<GroupActorHandle>>>,
    /// Per-`group_id` share-group actor handles (KIP-932).
    pub share_groups: Arc<DashMap<String, Arc<ShareGroupActorHandle>>>,
    /// First record persisted per `group_id` locks its type for life.
    pub group_types: Arc<DashMap<String, GroupType>>,
    /// Bootstrap-time accumulator; drained by `finalize_bootstrap`.
    pub seeds: Arc<DashMap<String, GroupSeed>>,
    /// Bootstrap-time share-group accumulator; drained by `finalize_bootstrap`.
    pub share_seeds: Arc<DashMap<String, ShareGroupSeed>>,
    /// Last-known-good state per group, populated alongside every
    /// successful actor write. Used to seed a fresh actor when the
    /// previous instance crashed after a log-write failure.
    pub seeds_cache: Arc<DashMap<String, GroupSeed>>,
    /// Last-known-good share-group state, the share-group analogue of
    /// `seeds_cache`.
    pub share_seeds_cache: Arc<DashMap<String, ShareGroupSeed>>,
    /// KIP-932 group-coordinator → share-state-persister bridge. Set once in
    /// `Broker::start` after both the `ShareCoordinator` and this coordinator
    /// exist. Per-group share actors read it (via [`Self::share_persister`]) to
    /// drive Initialize/Delete lifecycle calls after reconcile. `None` in the
    /// pure-coordinator unit tests, where the lifecycle hook is a no-op.
    pub(crate) share_persister:
        std::sync::OnceLock<Arc<crate::share_coordinator::persister_client::SharePersister>>,
}

impl GroupCoordinator {
    pub fn new(
        config: NextGenConfig,
        share_config: ShareGroupConfig,
        metadata: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            share_config: Arc::new(share_config),
            metadata,
            offsets_log,
            groups: Arc::new(DashMap::new()),
            share_groups: Arc::new(DashMap::new()),
            group_types: Arc::new(DashMap::new()),
            seeds: Arc::new(DashMap::new()),
            share_seeds: Arc::new(DashMap::new()),
            seeds_cache: Arc::new(DashMap::new()),
            share_seeds_cache: Arc::new(DashMap::new()),
            share_persister: std::sync::OnceLock::new(),
        }
    }

    /// Install the KIP-932 share-state persister bridge. Called once in
    /// `Broker::start`. A second call is silently ignored (the `OnceLock`
    /// keeps the first value), which keeps construction order-independent.
    pub(crate) fn set_share_persister(
        &self,
        persister: Arc<crate::share_coordinator::persister_client::SharePersister>,
    ) {
        let _ = self.share_persister.set(persister);
    }

    /// The installed share-state persister, if any. `None` in unit tests that
    /// construct a bare `GroupCoordinator`; the lifecycle hook then no-ops.
    #[must_use]
    pub(crate) fn share_persister(
        &self,
    ) -> Option<&Arc<crate::share_coordinator::persister_client::SharePersister>> {
        self.share_persister.get()
    }

    /// Replace the cached seed for `group_id` with `seed`. Called by the
    /// actor after every successful `OffsetsLog::append`.
    pub fn update_cache(&self, group_id: &str, seed: GroupSeed) {
        self.seeds_cache.insert(group_id.into(), seed);
    }

    /// Fetch the most recently cached seed for `group_id`, if any.
    #[must_use]
    pub fn cached_seed(&self, group_id: &str) -> Option<GroupSeed> {
        self.seeds_cache.get(group_id).map(|e| e.value().clone())
    }

    #[must_use]
    pub fn group_type(&self, group_id: &str) -> Option<GroupType> {
        self.group_types.get(group_id).map(|e| *e.value())
    }

    pub fn mark_classic(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Classic);
    }

    pub fn mark_next_gen(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::NextGen);
    }

    pub fn mark_share(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Share);
    }

    /// Replace the cached share-group seed for `group_id`. Called by the
    /// share actor after every successful `OffsetsLog::append`.
    pub fn update_share_cache(&self, group_id: &str, seed: ShareGroupSeed) {
        self.share_seeds_cache.insert(group_id.into(), seed);
    }

    /// Fetch the most recently cached share-group seed for `group_id`, if any.
    #[must_use]
    pub fn cached_share_seed(&self, group_id: &str) -> Option<ShareGroupSeed> {
        self.share_seeds_cache
            .get(group_id)
            .map(|e| e.value().clone())
    }

    #[must_use]
    pub fn get_or_create(self: &Arc<Self>, group_id: &str) -> Arc<GroupActorHandle> {
        if let Some(h) = self.groups.get(group_id) {
            // Dead-actor detection: if the mpsc sender is closed, the actor
            // has exited (typically after a log-write failure). Drop the
            // entry and fall through to spawn a fresh actor.
            if !h.value().tx.is_closed() {
                return h.value().clone();
            }
            drop(h);
            self.groups.remove(group_id);
        }
        let h = Arc::new(GroupActorHandle::spawn(
            group_id.into(),
            self.config.clone(),
            self.metadata.clone(),
            self.offsets_log.clone(),
            self.clone(),
        ));
        let inserted = self
            .groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone();
        if let Some(seed) = self.cached_seed(group_id) {
            let _ = inserted.tx.try_send(GroupActorMessage::Seed(seed));
        }
        inserted
    }

    #[must_use]
    pub fn find(&self, group_id: &str) -> Option<Arc<GroupActorHandle>> {
        self.groups.get(group_id).map(|e| e.value().clone())
    }

    #[must_use]
    pub fn get_or_create_share(self: &Arc<Self>, group_id: &str) -> Arc<ShareGroupActorHandle> {
        if let Some(h) = self.share_groups.get(group_id) {
            // Dead-actor detection: a closed mpsc sender means the actor exited
            // (typically after a log-write failure). Drop the entry and respawn.
            if !h.value().tx.is_closed() {
                return h.value().clone();
            }
            drop(h);
            self.share_groups.remove(group_id);
        }
        let h = Arc::new(ShareGroupActorHandle::spawn(
            group_id.into(),
            self.share_config.clone(),
            self.metadata.clone(),
            self.offsets_log.clone(),
            self.clone(),
        ));
        let inserted = self
            .share_groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone();
        if let Some(seed) = self.cached_share_seed(group_id) {
            let _ = inserted.tx.try_send(ShareGroupActorMessage::Seed(seed));
        }
        inserted
    }

    #[must_use]
    pub fn find_share(&self, group_id: &str) -> Option<Arc<ShareGroupActorHandle>> {
        self.share_groups.get(group_id).map(|e| e.value().clone())
    }

    pub async fn shutdown_all(&self) {
        let handles: Vec<Arc<GroupActorHandle>> =
            self.groups.iter().map(|e| e.value().clone()).collect();
        for h in handles {
            let (tx, rx) = oneshot::channel();
            if h.tx.send(GroupActorMessage::Shutdown(tx)).await.is_ok() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
            }
        }
        let share_handles: Vec<Arc<ShareGroupActorHandle>> = self
            .share_groups
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for h in share_handles {
            let (tx, rx) = oneshot::channel();
            if h.tx
                .send(ShareGroupActorMessage::Shutdown(tx))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
            }
        }
    }
}

impl GroupCoordinator {
    pub fn replay_group_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::GroupMetadataValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.group_epoch = v.epoch;
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.group_epoch = v.epoch;
    }
    pub fn replay_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::MemberMetadataValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.members.insert(member_id.into(), v.clone());
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.members.insert(member_id.into(), v);
    }
    pub fn replay_target_assignment_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::TargetAssignmentMetadataValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.target_epoch = v.assignment_epoch;
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.target_epoch = v.assignment_epoch;
    }
    pub fn replay_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::TargetAssignmentMemberValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.target_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.target_per_member.insert(member_id.into(), v);
    }
    pub fn replay_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::CurrentMemberAssignmentValue,
    ) {
        {
            let mut seed = self.seeds.entry(group_id.into()).or_default();
            seed.current_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.current_per_member.insert(member_id.into(), v);
    }

    /// Apply a tombstone for a next-gen key. Removes the corresponding
    /// entry from both `seeds` and `seeds_cache`. Used by bootstrap replay
    /// to honor records with `value = None`.
    pub fn replay_next_gen_tombstone(&self, key: &persistence_next_gen::NextGenKey) {
        use persistence_next_gen::NextGenKey as K;
        let group_id = match key {
            K::GroupMetadata { group_id }
            | K::MemberMetadata { group_id, .. }
            | K::TargetAssignmentMetadata { group_id }
            | K::TargetAssignmentMember { group_id, .. }
            | K::CurrentMemberAssignment { group_id, .. } => group_id.as_str(),
        };
        let scrub = |seed: &mut GroupSeed| match key {
            K::GroupMetadata { .. } => {
                seed.group_epoch = 0;
            }
            K::MemberMetadata { member_id, .. } => {
                seed.members.remove(member_id);
            }
            K::TargetAssignmentMetadata { .. } => {
                seed.target_epoch = 0;
            }
            K::TargetAssignmentMember { member_id, .. } => {
                seed.target_per_member.remove(member_id);
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                seed.current_per_member.remove(member_id);
            }
        };
        {
            if let Some(mut s) = self.seeds.get_mut(group_id) {
                scrub(s.value_mut());
            }
        }
        if let Some(mut s) = self.seeds_cache.get_mut(group_id) {
            scrub(s.value_mut());
        }
    }

    // ── KIP-932 share-group replay ───────────────────────────────────────

    pub fn replay_share_group_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.group_epoch = v.epoch;
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.group_epoch = v.epoch;
    }
    pub fn replay_share_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupMemberMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.members.insert(member_id.into(), v.clone());
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.members.insert(member_id.into(), v);
    }
    pub fn replay_share_target_assignment_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupTargetAssignmentMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.target_epoch = v.assignment_epoch;
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.target_epoch = v.assignment_epoch;
    }
    pub fn replay_share_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupTargetAssignmentMemberValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.target_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.target_per_member.insert(member_id.into(), v);
    }
    pub fn replay_share_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupCurrentMemberAssignmentValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.current_per_member.insert(member_id.into(), v.clone());
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.current_per_member.insert(member_id.into(), v);
    }

    /// Replay a KIP-932 `ShareGroupStatePartitionMetadata` (key v14) record,
    /// recording which `(topic_id, partition)` share-states the group has
    /// initialized so the lifecycle hook can skip re-initialization after a
    /// restart.
    pub fn replay_share_state_partition_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupStatePartitionMetadataValue,
    ) {
        {
            let mut seed = self.share_seeds.entry(group_id.into()).or_default();
            seed.state_partition_metadata = v.clone();
        }
        let mut cached = self.share_seeds_cache.entry(group_id.into()).or_default();
        cached.state_partition_metadata = v;
    }

    /// Apply a tombstone for a share-group key. Removes the corresponding
    /// entry from both `share_seeds` and `share_seeds_cache`.
    pub fn replay_share_tombstone(&self, key: &share::persistence::ShareGroupKey) {
        use share::persistence::ShareGroupKey as K;
        let group_id = match key {
            K::GroupMetadata { group_id }
            | K::MemberMetadata { group_id, .. }
            | K::TargetAssignmentMetadata { group_id }
            | K::TargetAssignmentMember { group_id, .. }
            | K::CurrentMemberAssignment { group_id, .. }
            | K::StatePartitionMetadata { group_id } => group_id.as_str(),
        };
        let scrub = |seed: &mut ShareGroupSeed| match key {
            K::GroupMetadata { .. } => seed.group_epoch = 0,
            K::MemberMetadata { member_id, .. } => {
                seed.members.remove(member_id);
            }
            K::TargetAssignmentMetadata { .. } => seed.target_epoch = 0,
            K::TargetAssignmentMember { member_id, .. } => {
                seed.target_per_member.remove(member_id);
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                seed.current_per_member.remove(member_id);
            }
            K::StatePartitionMetadata { .. } => {
                seed.state_partition_metadata =
                    share::persistence::ShareGroupStatePartitionMetadataValue::default();
            }
        };
        {
            if let Some(mut s) = self.share_seeds.get_mut(group_id) {
                scrub(s.value_mut());
            }
        }
        if let Some(mut s) = self.share_seeds_cache.get_mut(group_id) {
            scrub(s.value_mut());
        }
    }

    pub fn finalize_bootstrap(self: &Arc<Self>) {
        let group_ids: Vec<String> = self.seeds.iter().map(|e| e.key().clone()).collect();
        for gid in group_ids {
            if let Some((_, seed)) = self.seeds.remove(&gid) {
                let handle = self.get_or_create(&gid);
                let _ = handle.tx.try_send(actor::GroupActorMessage::Seed(seed));
            }
        }
        let share_ids: Vec<String> = self.share_seeds.iter().map(|e| e.key().clone()).collect();
        for gid in share_ids {
            if let Some((_, seed)) = self.share_seeds.remove(&gid) {
                let handle = self.get_or_create_share(&gid);
                let _ = handle.tx.try_send(ShareGroupActorMessage::Seed(seed));
            }
        }
    }
}

/// `MetadataProvider` backed by `crabka_raft::ControllerHandle::current_image()`.
pub struct ImageMetadataProvider {
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
}

impl std::fmt::Debug for ImageMetadataProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageMetadataProvider")
            .finish_non_exhaustive()
    }
}

impl MetadataProvider for ImageMetadataProvider {
    fn snapshot(&self) -> reconciler::ReconcileInput {
        use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
        let image = self.controller.current_image();
        let mut topic_id_by_name = std::collections::HashMap::new();
        let mut partitions_per_topic = std::collections::HashMap::new();
        let mut partition_racks: std::collections::HashMap<(ProtoUuid, i32), Vec<String>> =
            std::collections::HashMap::new();
        for topic in image.topics() {
            let proto_id = ProtoUuid(*topic.topic_id.as_bytes());
            topic_id_by_name.insert(topic.name.clone(), proto_id);
            partitions_per_topic.insert(proto_id, topic.partitions);
            // Collect the set of racks the partition's
            // replicas are on, so the rack-aware UniformAssignor can
            // prefer rack-collocated subscribers. Partitions whose
            // replicas have no rack info don't get an entry — the
            // assignor then falls back to its non-rack-aware path.
            for pr in image.partitions_of(&topic.name) {
                let mut racks: Vec<String> = pr
                    .replicas
                    .iter()
                    .filter_map(|&node_id| image.broker(node_id).and_then(|b| b.rack.clone()))
                    .collect();
                racks.sort();
                racks.dedup();
                if !racks.is_empty() {
                    partition_racks.insert((proto_id, pr.partition), racks);
                }
            }
        }
        reconciler::ReconcileInput {
            topic_id_by_name,
            partitions_per_topic,
            partition_racks,
        }
    }
}

/// Hydration seed passed from the bootstrap replayer into a freshly-spawned
/// [`actor::GroupActorHandle`]. All fields come directly from records
/// decoded out of `__consumer_offsets`.
#[derive(Debug, Default, Clone)]
pub struct GroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members: std::collections::HashMap<String, persistence_next_gen::MemberMetadataValue>,
    pub target_per_member:
        std::collections::HashMap<String, persistence_next_gen::TargetAssignmentMemberValue>,
    pub current_per_member:
        std::collections::HashMap<String, persistence_next_gen::CurrentMemberAssignmentValue>,
}

/// Hydration seed for a [`share::actor::ShareGroupActorHandle`]. All fields
/// come from share-group records decoded out of `__consumer_offsets`.
#[derive(Debug, Default, Clone)]
pub struct ShareGroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members:
        std::collections::HashMap<String, share::persistence::ShareGroupMemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<
        String,
        share::persistence::ShareGroupTargetAssignmentMemberValue,
    >,
    pub current_per_member: std::collections::HashMap<
        String,
        share::persistence::ShareGroupCurrentMemberAssignmentValue,
    >,
    /// KIP-932 `ShareGroupStatePartitionMetadata` (key v14): which
    /// `(topic_id, partition)` share-states this group has already
    /// initialized, plus topic ids whose share-state is being deleted.
    /// Lets the lifecycle hook skip re-initializing partitions on restart.
    pub state_partition_metadata: share::persistence::ShareGroupStatePartitionMetadataValue,
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn group_type_has_share_variant() {
        // KIP-932: a third locked group type alongside Classic and NextGen.
        let t = GroupType::Share;
        assert!(t == GroupType::Share);
        assert!(t != GroupType::Classic);
        assert!(t != GroupType::NextGen);
    }

    fn make_coord() -> Arc<GroupCoordinator> {
        use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;
        let metadata: Arc<dyn MetadataProvider> = Arc::new(ImageMetadatalessProvider);
        Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            ShareGroupConfig::default(),
            metadata,
            Arc::new(InMemoryOffsetsLog::default()),
        ))
    }

    #[derive(Debug)]
    struct ImageMetadatalessProvider;
    impl MetadataProvider for ImageMetadatalessProvider {
        fn snapshot(&self) -> reconciler::ReconcileInput {
            reconciler::ReconcileInput::default()
        }
    }

    #[test]
    fn mark_share_locks_group_type() {
        let coord = make_coord();
        coord.mark_share("sg");
        assert!(coord.group_type("sg") == Some(GroupType::Share));
        // First mark wins: a later mark_classic must not override.
        coord.mark_classic("sg");
        assert!(coord.group_type("sg") == Some(GroupType::Share));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_or_create_share_is_idempotent() {
        let coord = make_coord();
        let a = coord.get_or_create_share("sg");
        let b = coord.get_or_create_share("sg");
        assert!(Arc::ptr_eq(&a, &b));
        assert!(coord.find_share("sg").is_some());
    }
}
