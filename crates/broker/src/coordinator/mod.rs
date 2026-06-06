//! Group-coordinator subsystem. The unified [`GroupCoordinator`]
//! (`unified::GroupCoordinator`) owns one tokio actor per `group_id`, each
//! speaking either the classic `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`
//! protocol or the KIP-848 next-gen `ConsumerGroupHeartbeat` protocol behind a
//! single registry, one persistence path, and one actor model.

pub(crate) mod bootstrap;
pub mod unified;
pub(crate) mod persistence {
    pub(crate) use crate::coordinator::unified::persistence::*;
}

pub(crate) use unified::GroupCoordinator;

/// Result of [`GroupCoordinator::delete_group`].
#[derive(Debug, PartialEq, Eq)]
pub enum DeleteGroupError {
    /// No (classic) group with this id exists.
    NotFound,
    /// Group still has at least one live member.
    NonEmpty,
}

/// Read-only projection of a classic `Group` for the `ListGroups` /
/// `DescribeGroups` handlers. Cheap to build (Strings + small struct).
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub group_id: String,
    pub state: crate::coordinator::unified::classic_state::GroupState,
    pub protocol_type: Option<String>,
    /// Selected protocol NAME, maps to `DescribeGroups` `protocol_data`;
    /// `None` for an empty/dead group.
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
    /// Assignment bytes from the last `SyncGroup`, or empty if not yet assigned.
    pub assignment: Vec<u8>,
    /// `JoinGroup` protocol metadata bytes → `DescribeGroups`
    /// `member_metadata`; empty if not joined / next-gen.
    pub protocol_metadata: Vec<u8>,
}
