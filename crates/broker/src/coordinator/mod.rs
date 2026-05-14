//! Group-coordinator subsystem. `GroupManager` owns the runtime registry
//! of `Group`s and exposes per-group locking, blocking-handler gates, and
//! a periodic expiration ticker.

#![allow(dead_code)] // consumers land in Tasks 6-12.

pub(crate) mod bootstrap;
pub(crate) mod group;
pub(crate) mod persistence;

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use group::Group;

/// Result of [`GroupManager::delete_group`].
#[derive(Debug, PartialEq, Eq)]
pub enum DeleteGroupError {
    /// No group with this id exists.
    NotFound,
    /// Group still has at least one live member.
    NonEmpty,
}

/// Read-only projection of a `Group` for the `ListGroups` / `DescribeGroups`
/// handlers. Cheap to build (Strings + small struct).
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub group_id: String,
    pub state: crate::coordinator::group::GroupState,
    pub protocol_type: Option<String>,
    pub generation_id: i32,
    pub members: Vec<MemberSnapshot>,
}

/// Read-only projection of a [`group::Member`].
#[derive(Debug, Clone)]
pub struct MemberSnapshot {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    /// Assignment bytes from the last `SyncGroup`, or empty if not yet assigned.
    pub assignment: Vec<u8>,
}

/// Runtime handles for one group: the locked `Group` plus per-stage
/// `Notify`s used by `join_group` and `sync_group` to park waiting members.
pub(crate) struct GroupHandle {
    pub state: Mutex<Group>,
    /// Wakes all parked `JoinGroup` handlers when the rebalance deadline fires
    /// or when every expected member has joined.
    pub join_complete: Notify,
    /// Wakes all parked `SyncGroup` handlers when the leader's `SyncGroup` arrives.
    pub sync_complete: Notify,
}

impl GroupHandle {
    fn new(group_id: impl Into<String>) -> Self {
        Self {
            state: Mutex::new(Group::new(group_id)),
            join_complete: Notify::new(),
            sync_complete: Notify::new(),
        }
    }
}

pub(crate) struct GroupManager {
    /// Cheap-to-clone shared map.
    pub(crate) groups: Arc<DashMap<String, Arc<GroupHandle>>>,
    /// Cancellation token for the expiration ticker.
    shutdown: CancellationToken,
    /// Held so the ticker is reaped when `GroupManager` drops.
    _ticker: JoinHandle<()>,
}

impl GroupManager {
    pub fn new() -> Self {
        let groups: Arc<DashMap<String, Arc<GroupHandle>>> = Arc::new(DashMap::new());
        let shutdown = CancellationToken::new();
        let ticker = tokio::spawn(expiration_ticker(groups.clone(), shutdown.clone()));
        Self {
            groups,
            shutdown,
            _ticker: ticker,
        }
    }

    pub fn get_or_create(&self, group_id: &str) -> Arc<GroupHandle> {
        if let Some(h) = self.groups.get(group_id) {
            return h.value().clone();
        }
        let new_handle = Arc::new(GroupHandle::new(group_id));
        self.groups
            .entry(group_id.to_string())
            .or_insert(new_handle)
            .value()
            .clone()
    }

    pub fn find(&self, group_id: &str) -> Option<Arc<GroupHandle>> {
        self.groups.get(group_id).map(|h| h.value().clone())
    }

    /// Cancel the ticker. Called from `Broker::shutdown` if it ever wants
    /// to drain explicitly; otherwise the ticker exits when `_ticker`
    /// drops.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Snapshot every known group. The returned `Vec` is in arbitrary
    /// order (matching Apache Kafka's `ListGroups`, which doesn't promise
    /// ordering either).
    pub async fn list_groups(&self) -> Vec<GroupSnapshot> {
        let handles: Vec<Arc<GroupHandle>> =
            self.groups.iter().map(|e| e.value().clone()).collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            let g = h.state.lock().await;
            out.push(snapshot(&g));
        }
        out
    }

    /// Snapshot a single group, or `None` if unknown.
    pub async fn describe_group(&self, group_id: &str) -> Option<GroupSnapshot> {
        let handle = self.find(group_id)?;
        let g = handle.state.lock().await;
        Some(snapshot(&g))
    }

    /// Drop a group from the in-memory registry. Returns
    /// [`DeleteGroupError::NonEmpty`] if the group still has live members,
    /// [`DeleteGroupError::NotFound`] if the group doesn't exist.
    pub async fn delete_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
        let handle = self.find(group_id).ok_or(DeleteGroupError::NotFound)?;
        {
            let g = handle.state.lock().await;
            if !g.members.is_empty() {
                return Err(DeleteGroupError::NonEmpty);
            }
        }
        self.groups.remove(group_id);
        Ok(())
    }
}

fn snapshot(g: &crate::coordinator::group::Group) -> GroupSnapshot {
    GroupSnapshot {
        group_id: g.group_id.clone(),
        state: g.state,
        protocol_type: g.protocol_type.clone(),
        generation_id: g.generation_id,
        members: g
            .members
            .values()
            .map(|m| MemberSnapshot {
                member_id: m.member_id.clone(),
                client_id: m.client_id.clone(),
                client_host: m.host.clone(),
                assignment: m
                    .assignment
                    .as_ref()
                    .map(|b| b.to_vec())
                    .unwrap_or_default(),
            })
            .collect(),
    }
}

impl std::fmt::Debug for GroupManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupManager")
            .field("group_count", &self.groups.len())
            .finish_non_exhaustive()
    }
}

/// Wake every group's expirations every second. On any drop, fire the
/// per-group `join_complete` notify so blocked `JoinGroup` handlers can
/// observe state changes (e.g. transition back to `PreparingRebalance`).
async fn expiration_ticker(
    groups: Arc<DashMap<String, Arc<GroupHandle>>>,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = interval.tick() => {
                let now = std::time::Instant::now();
                // Collect the handles first so we don't hold the DashMap
                // shard guard across the `.await` on the per-group mutex.
                let handles: Vec<(String, Arc<GroupHandle>)> = groups
                    .iter()
                    .map(|e| (e.key().clone(), e.value().clone()))
                    .collect();
                for (group_id, handle) in handles {
                    let dropped = {
                        let mut g = handle.state.lock().await;
                        g.expire_dead_members(now)
                    };
                    if !dropped.is_empty() {
                        tracing::info!(
                            group = %group_id,
                            dropped = ?dropped,
                            "expired members; waking joiners"
                        );
                        handle.join_complete.notify_waiters();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn get_or_create_is_idempotent() {
        let m = GroupManager::new();
        let a = m.get_or_create("g");
        let b = m.get_or_create("g");
        // Same Arc.
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[tokio::test]
    async fn list_groups_includes_known_groups() {
        let mgr = GroupManager::new();
        let _ = mgr.get_or_create("g1");
        let _ = mgr.get_or_create("g2");
        let listed = mgr.list_groups().await;
        let ids: std::collections::HashSet<String> =
            listed.iter().map(|s| s.group_id.clone()).collect();
        assert!(ids.contains("g1"));
        assert!(ids.contains("g2"));
    }

    #[tokio::test]
    async fn describe_group_returns_snapshot() {
        let mgr = GroupManager::new();
        let _ = mgr.get_or_create("g1");
        let snap = mgr.describe_group("g1").await.expect("known");
        assert_eq!(snap.group_id, "g1");
        assert!(snap.members.is_empty());
    }

    #[tokio::test]
    async fn delete_group_removes_empty_group() {
        let mgr = GroupManager::new();
        let _ = mgr.get_or_create("g1");
        mgr.delete_group("g1").await.expect("delete");
        assert!(mgr.describe_group("g1").await.is_none());
    }

    #[tokio::test]
    async fn delete_group_unknown_is_err() {
        let mgr = GroupManager::new();
        let err = mgr.delete_group("ghost").await.unwrap_err();
        assert_eq!(err, DeleteGroupError::NotFound);
    }
}
