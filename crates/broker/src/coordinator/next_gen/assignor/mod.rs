//! Server-side assignors (KIP-848). Each implementation maps a set of
//! members + subscriptions + topic metadata to per-member partition
//! assignments.

pub mod range;
pub mod uniform;

use std::collections::HashMap;

use crabka_protocol::primitives::uuid::Uuid;

#[derive(Debug, Clone)]
pub struct MemberSubscription {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub subscribed_topic_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Default)]
pub struct TopicMetadata {
    pub partitions_per_topic: HashMap<Uuid, i32>,
}

pub type Assignment = HashMap<String, HashMap<Uuid, Vec<i32>>>;

pub trait Assignor: Send + Sync {
    fn name(&self) -> &'static str;
    fn assign(&self, members: &[MemberSubscription], topics: &TopicMetadata) -> Assignment;
}

#[must_use]
pub fn select(name: &str) -> Option<Box<dyn Assignor>> {
    match name {
        "uniform" => Some(Box::new(uniform::UniformAssignor)),
        "range" => Some(Box::new(range::RangeAssignor)),
        _ => None,
    }
}
