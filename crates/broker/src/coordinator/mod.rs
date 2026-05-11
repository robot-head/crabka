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
}
