//! Unified group-coordinator subsystem (KIP-848 64d-B). Shared infra and
//! persistence for both the classic and next-gen group protocols.
//!
//! [`GroupCoordinator`] is the single owner of the next-gen consumer-group
//! machinery: it spawns per-group actors, tracks each group's locked type,
//! and replays persisted state during bootstrap.
pub mod actor;
pub mod assignor;
pub(crate) mod classic_ops;
pub(crate) mod classic_state;
pub mod config;
pub(crate) mod consumer_state;
pub(crate) mod group;
pub mod offsets_log;
pub(crate) mod persistence;
pub mod persistence_next_gen;
pub mod reconciler;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::oneshot;

use actor::{GroupActorHandle, GroupActorMessage, GroupKindTag, MetadataProvider};
use config::NextGenConfig;
use group::Group;
use offsets_log::OffsetsLog;

use crate::coordinator::{DeleteGroupError, GroupSnapshot};

#[derive(Debug)]
pub struct GroupCoordinator {
    pub config: Arc<NextGenConfig>,
    pub metadata: Arc<dyn MetadataProvider>,
    pub offsets_log: Arc<dyn OffsetsLog>,
    pub groups: Arc<DashMap<String, Arc<GroupActorHandle>>>,
    /// Bootstrap-time accumulator for next-gen state; drained by
    /// `finalize_bootstrap`.
    pub seeds: Arc<DashMap<String, GroupSeed>>,
    /// Last-known-good next-gen state per group, populated alongside every
    /// successful actor write. Used to seed a fresh actor when the
    /// previous instance crashed after a log-write failure.
    pub seeds_cache: Arc<DashMap<String, GroupSeed>>,
}

impl GroupCoordinator {
    pub fn new(
        config: NextGenConfig,
        metadata: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            metadata,
            offsets_log,
            groups: Arc::new(DashMap::new()),
            seeds: Arc::new(DashMap::new()),
            seeds_cache: Arc::new(DashMap::new()),
        }
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

    /// Get the existing actor for `group_id`, or spawn one of `kind`.
    ///
    /// Returns `None` if a *live* actor of the **other** protocol already owns
    /// the id — this enforces the per-group type lock (formerly the
    /// `group_types` map + `mark_*`): the first protocol to create the actor
    /// wins, and the second is rejected by its handler, exactly as before.
    #[must_use]
    pub fn get_or_create(
        self: &Arc<Self>,
        group_id: &str,
        kind: GroupKindTag,
    ) -> Option<Arc<GroupActorHandle>> {
        if let Some(h) = self.groups.get(group_id) {
            // Dead-actor detection: if the mpsc sender is closed, the actor
            // has exited (typically after a log-write failure). Drop the
            // entry and fall through to spawn a fresh actor.
            if !h.value().tx.is_closed() {
                if h.value().kind != kind {
                    return None;
                }
                return Some(h.value().clone());
            }
            drop(h);
            self.groups.remove(group_id);
        }
        let h = Arc::new(GroupActorHandle::spawn(
            group_id.into(),
            kind,
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
        if inserted.kind != kind {
            // Lost a spawn race against the other protocol.
            return None;
        }
        // Re-hydrate a respawned consumer actor from its last-known-good state.
        if kind == GroupKindTag::Consumer
            && let Some(seed) = self.cached_seed(group_id)
        {
            let _ = inserted.tx.try_send(GroupActorMessage::Seed(seed));
        }
        Some(inserted)
    }

    /// Get-or-create a classic-protocol actor. `None` if a consumer group
    /// already owns the id.
    #[must_use]
    pub fn get_or_create_classic(
        self: &Arc<Self>,
        group_id: &str,
    ) -> Option<Arc<GroupActorHandle>> {
        self.get_or_create(group_id, GroupKindTag::Classic)
    }

    /// Get-or-create a next-gen consumer-protocol actor. `None` if a classic
    /// group already owns the id.
    #[must_use]
    pub fn get_or_create_consumer(
        self: &Arc<Self>,
        group_id: &str,
    ) -> Option<Arc<GroupActorHandle>> {
        self.get_or_create(group_id, GroupKindTag::Consumer)
    }

    #[must_use]
    pub fn find(&self, group_id: &str) -> Option<Arc<GroupActorHandle>> {
        self.groups.get(group_id).map(|e| e.value().clone())
    }

    /// Spawn a classic actor seeded with a fully-replayed `Group` (bootstrap).
    pub fn seed_classic(self: &Arc<Self>, group_id: &str, group: Box<Group>) {
        if let Some(handle) = self.get_or_create_classic(group_id) {
            let _ = handle.tx.try_send(GroupActorMessage::ClassicSeed(group));
        }
    }

    /// Snapshot every **classic** group. Consumer groups are intentionally not
    /// surfaced to the legacy admin APIs (preserved from the two-coordinator
    /// era). TODO(64d-C+): surface consumer groups in admin APIs.
    pub async fn list_groups(&self) -> Vec<GroupSnapshot> {
        let handles: Vec<Arc<GroupActorHandle>> = self
            .groups
            .iter()
            .filter(|e| e.value().kind == GroupKindTag::Classic)
            .map(|e| e.value().clone())
            .collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            let (tx, rx) = oneshot::channel();
            if h.tx
                .send(GroupActorMessage::ClassicInspect { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
            {
                out.push(view.snapshot());
            }
        }
        out
    }

    /// Snapshot a single **classic** group, or `None` if unknown / consumer.
    pub async fn describe_group(&self, group_id: &str) -> Option<GroupSnapshot> {
        let handle = self.find(group_id)?;
        if handle.kind != GroupKindTag::Classic {
            return None;
        }
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicInspect { reply: tx })
            .await
            .ok()?;
        rx.await.ok().map(|v| v.snapshot())
    }

    /// Drop a **classic** group from the registry. `NonEmpty` if it still has
    /// live members; `NotFound` if unknown / consumer.
    pub async fn delete_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
        let handle = self.find(group_id).ok_or(DeleteGroupError::NotFound)?;
        if handle.kind != GroupKindTag::Classic {
            return Err(DeleteGroupError::NotFound);
        }
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicInspect { reply: tx })
            .await
            .map_err(|_| DeleteGroupError::NotFound)?;
        let view = rx.await.map_err(|_| DeleteGroupError::NotFound)?;
        if !view.members.is_empty() {
            return Err(DeleteGroupError::NonEmpty);
        }
        self.groups.remove(group_id);
        Ok(())
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

    pub fn finalize_bootstrap(self: &Arc<Self>) {
        let group_ids: Vec<String> = self.seeds.iter().map(|e| e.key().clone()).collect();
        for gid in group_ids {
            if let Some((_, seed)) = self.seeds.remove(&gid)
                && let Some(handle) = self.get_or_create_consumer(&gid)
            {
                let _ = handle.tx.try_send(actor::GroupActorMessage::Seed(seed));
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
