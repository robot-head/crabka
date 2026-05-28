//! KIP-848 next-gen consumer group protocol coordinator.

pub mod assignor;
pub mod config;
pub mod group_actor;
pub mod group_state;
pub mod persistence;
pub mod reconciler;

/// Hydration seed passed from the bootstrap replayer into a freshly-spawned
/// [`group_actor::GroupActorHandle`]. All fields come directly from records
/// decoded out of `__consumer_offsets`.
#[derive(Debug, Default)]
pub struct GroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members: std::collections::HashMap<String, persistence::MemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<String, persistence::TargetAssignmentMemberValue>,
    pub current_per_member: std::collections::HashMap<String, persistence::CurrentMemberAssignmentValue>,
}
