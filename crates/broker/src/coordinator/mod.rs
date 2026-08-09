//! Group-coordinator subsystem.
//!
//! The unified [`GroupCoordinator`], `unified::GroupCoordinator`, owns one
//! tokio actor for each `group_id`. Each actor speaks either the classic
//! `JoinGroup`, `SyncGroup`, `Heartbeat`, and `LeaveGroup` protocol, or the
//! KIP-848 next-gen `ConsumerGroupHeartbeat` protocol. Both sit behind one
//! registry, one persistence path, and one actor model.

pub(crate) mod bootstrap;
pub(crate) mod leadership;
pub(crate) mod partitioner;

pub use bootstrap::AUDIT_TOPIC;
pub mod unified;
pub(crate) mod persistence {
    pub(crate) use crate::coordinator::unified::persistence::*;
}

pub(crate) use unified::GroupCoordinator;

/// Result of [`GroupCoordinator::delete_group`].
#[derive(Debug, PartialEq, Eq)]
pub enum DeleteGroupError {
    /// No classic group with this id exists.
    NotFound,
    /// The group still has at least one live member.
    NonEmpty,
    /// A durable side effect of the delete failed, for example the tombstone
    /// append.
    Internal,
}

/// Read-only projection of a classic `Group` for the `ListGroups` and
/// `DescribeGroups` handlers. It is cheap to build: a few `String` values and
/// a small struct.
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub group_id: String,
    pub state: crate::coordinator::unified::classic_state::GroupState,
    pub protocol_type: Option<String>,
    /// The selected protocol NAME, which maps to the `DescribeGroups` field
    /// `protocol_data`. It is `None` for an empty or dead group.
    pub protocol_name: Option<String>,
    pub generation_id: i32,
    pub members: Vec<MemberSnapshot>,
}

/// Read-only projection of a classic member.
#[derive(Debug, Clone)]
pub struct MemberSnapshot {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    /// Assignment bytes from the last `SyncGroup`. It is empty when the
    /// member has no assignment yet.
    pub assignment: Vec<u8>,
    /// `JoinGroup` protocol metadata bytes, which map to the
    /// `DescribeGroups` field `member_metadata`. It is empty when the member
    /// has not joined, and for a next-gen member.
    pub protocol_metadata: Vec<u8>,
}
