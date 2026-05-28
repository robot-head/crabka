//! KIP-848 next-gen consumer group protocol coordinator.

pub mod assignor;
pub mod config;
pub mod group_actor;
pub mod group_state;
pub mod persistence;
pub mod reconciler;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::oneshot;

use config::NextGenConfig;
use group_actor::{GroupActorHandle, GroupActorMessage, MetadataProvider};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupType {
    Classic,
    NextGen,
}

#[derive(Debug)]
pub struct NextGenCoordinator {
    pub config: Arc<NextGenConfig>,
    pub metadata: Arc<dyn MetadataProvider>,
    pub groups: Arc<DashMap<String, Arc<GroupActorHandle>>>,
    /// First record persisted per `group_id` locks its type for life.
    pub group_types: Arc<DashMap<String, GroupType>>,
    /// Bootstrap-time accumulator; drained by `finalize_bootstrap`.
    pub seeds: Arc<DashMap<String, GroupSeed>>,
}

impl NextGenCoordinator {
    pub fn new(config: NextGenConfig, metadata: Arc<dyn MetadataProvider>) -> Self {
        Self {
            config: Arc::new(config),
            metadata,
            groups: Arc::new(DashMap::new()),
            group_types: Arc::new(DashMap::new()),
            seeds: Arc::new(DashMap::new()),
        }
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

    #[must_use]
    pub fn get_or_create(&self, group_id: &str) -> Arc<GroupActorHandle> {
        if let Some(h) = self.groups.get(group_id) {
            return h.value().clone();
        }
        let h = Arc::new(GroupActorHandle::spawn(
            group_id.into(),
            self.config.clone(),
            self.metadata.clone(),
        ));
        self.groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone()
    }

    #[must_use]
    pub fn find(&self, group_id: &str) -> Option<Arc<GroupActorHandle>> {
        self.groups.get(group_id).map(|e| e.value().clone())
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

impl NextGenCoordinator {
    pub fn replay_group_metadata(&self, group_id: &str, v: persistence::GroupMetadataValue) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.group_epoch = v.epoch;
    }
    pub fn replay_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence::MemberMetadataValue,
    ) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.members.insert(member_id.into(), v);
    }
    pub fn replay_target_assignment_metadata(
        &self,
        group_id: &str,
        v: persistence::TargetAssignmentMetadataValue,
    ) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.target_epoch = v.assignment_epoch;
    }
    pub fn replay_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence::TargetAssignmentMemberValue,
    ) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.target_per_member.insert(member_id.into(), v);
    }
    pub fn replay_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence::CurrentMemberAssignmentValue,
    ) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.current_per_member.insert(member_id.into(), v);
    }

    pub fn finalize_bootstrap(&self) {
        let group_ids: Vec<String> = self.seeds.iter().map(|e| e.key().clone()).collect();
        for gid in group_ids {
            if let Some((_, seed)) = self.seeds.remove(&gid) {
                let handle = self.get_or_create(&gid);
                let _ = handle
                    .tx
                    .try_send(group_actor::GroupActorMessage::Seed(seed));
            }
        }
    }
}

/// `MetadataProvider` backed by `crabka_raft::ControllerHandle::current_image()`.
pub struct ImageMetadataProvider {
    pub controller: Arc<crabka_raft::ControllerHandle>,
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
        for topic in image.topics() {
            let proto_id = ProtoUuid(*topic.topic_id.as_bytes());
            topic_id_by_name.insert(topic.name.clone(), proto_id);
            partitions_per_topic.insert(proto_id, topic.partitions);
        }
        reconciler::ReconcileInput {
            topic_id_by_name,
            partitions_per_topic,
        }
    }
}

/// Hydration seed passed from the bootstrap replayer into a freshly-spawned
/// [`group_actor::GroupActorHandle`]. All fields come directly from records
/// decoded out of `__consumer_offsets`.
#[derive(Debug, Default)]
pub struct GroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members: std::collections::HashMap<String, persistence::MemberMetadataValue>,
    pub target_per_member:
        std::collections::HashMap<String, persistence::TargetAssignmentMemberValue>,
    pub current_per_member:
        std::collections::HashMap<String, persistence::CurrentMemberAssignmentValue>,
}
