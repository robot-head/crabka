//! Server-side assignors (KIP-848). Each implementation maps a set of
//! members + subscriptions + topic metadata to per-member partition
//! assignments.

pub mod range;
pub mod uniform;

use std::collections::HashMap;

use crabka_protocol::primitives::uuid::Uuid;
pub use range::RangeAssignor;
pub use uniform::UniformAssignor;

#[derive(Debug, Clone)]
pub struct MemberSubscription {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub subscribed_topic_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Default)]
pub struct TopicMetadata {
    pub partitions_per_topic: HashMap<Uuid, i32>,
    /// Per-`(topic_id, partition_index)` set of racks on which the
    /// partition has at least one replica. The value is empty, or the key
    /// is missing, for partitions whose replicas have no rack info. The
    /// assignor then falls back to its non-rack-aware behavior. The
    /// coordinator's metadata snapshot fills this map.
    pub partition_racks: HashMap<(Uuid, i32), Vec<String>>,
}

pub type Assignment = HashMap<String, HashMap<Uuid, Vec<i32>>>;

pub trait Assignor: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;
    fn assign(&self, members: &[MemberSubscription], topics: &TopicMetadata) -> Assignment;
}
