//! In-memory lock manager for concurrent writers. Exclusive/shared locks over
//! two key spaces — heap rows (`(table, rowid)`) and unique local index keys
//! (`LockKey::UniqueKey`) — transaction-scoped (released at COMMIT/ROLLBACK).
//! A blocked writer calls the integrated async `acquire`, which detects a
//! conflict and registers a per-waiter `Notify` ATOMICALLY under one guard; the
//! holder's `release_all` wakes each waiter with `notify_one` (which stores a
//! permit if the waiter has not yet awaited, so no wakeup is ever lost). A
//! wait-for graph (each waiting xid -> the xid it blocks on) is checked eagerly
//! for cycles before blocking, aborting the would-be waiter with a deadlock
//! error; both key spaces share the one graph, so a cycle spanning a row lock
//! and a unique-key lock is still detected. Purely in-memory: after a restart
//! no transactions are in flight, so no lock state must survive.
//!
//! The graph sees only this engine's waits, so a cycle whose edges span two
//! engines (each leg of a cross-range transaction waiting on the other's
//! participant) is invisible to it. Sessions that can be enlisted in such a
//! cycle pass a wait cap to `acquire`; the cap expiring aborts the waiter as a
//! presumed distributed deadlock, the detector of last resort where no shared
//! wait-for graph exists.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

/// Result of a non-blocking lock attempt.
pub enum Acquire {
    Acquired,
    /// Held by `holder` (one of the holders) — an xid to wait on.
    Conflict(u64),
}

/// Result of the eager cycle check.
pub enum CycleCheck {
    Ok,
    Deadlock,
}

/// Why a blocking acquire refused to keep waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// Blocking would close a wait-for cycle on this engine.
    Deadlock,
    /// The caller's wait cap expired before the lock was granted — treated as
    /// a distributed deadlock, since a cross-engine cycle never shows up in
    /// any single engine's wait-for graph.
    CapExpired,
}

/// Identity of a lockable resource. Row locks and unique-key locks live in the
/// same table (and the same wait-for graph), so deadlock cycles spanning both
/// kinds are detected.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockKey {
    /// A heap row: `(table, rowid)`.
    Row(crabka_pgcatalog::TableId, u64),
    /// A unique local index key: the encoded index-entry prefix
    /// (`secondary_index_entry_prefix(table, index, values)`), a deterministic
    /// identity for `(table, index, key values)`. Serializes the
    /// check-then-write unique probe per key instead of engine-wide.
    UniqueKey(Vec<u8>),
}

struct HeldLock {
    mode: LockMode,
    holders: HashSet<u64>,
}

struct Inner {
    locks: HashMap<LockKey, HeldLock>,
    waiters: HashMap<u64, Vec<Arc<Notify>>>, // holder xid -> waiters' notifiers
    // wait-for graph: each waiting xid -> the single holder xid it blocks on.
    // NOTE: this is single-successor (one out-edge per waiter), so the eager
    // cycle check is exact for exclusive locks. A deadlock cycle that runs only
    // through a non-chosen *shared* co-holder may not be flagged on the first
    // check — but it cannot hang permanently: every release re-wakes the waiter,
    // which re-checks against a possibly-different chosen holder until the cyclic
    // one is the edge. Shared-only deadlocks are out of SP6's tested scope.
    wait_for: HashMap<u64, u64>,
}

pub(crate) struct RowLockManager {
    inner: Mutex<Inner>,
}

impl RowLockManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                locks: HashMap::new(),
                waiters: HashMap::new(),
                wait_for: HashMap::new(),
            }),
        }
    }

    /// Non-blocking acquire. Idempotent if `my_xid` already holds compatibly; a
    /// sole shared holder may upgrade to exclusive. Thin wrapper that locks and
    /// delegates to [`try_acquire_locked`].
    #[cfg(test)]
    pub(crate) fn try_acquire(
        &self,
        table: crabka_pgcatalog::TableId,
        rowid: u64,
        mode: LockMode,
        my_xid: u64,
    ) -> Acquire {
        let mut g = self.inner.lock().expect("lockmgr");
        try_acquire_locked(&mut g, LockKey::Row(table, rowid), mode, my_xid)
    }

    /// Number of lock-table entries currently held (both key spaces). Lets
    /// tests assert released entries are REMOVED, not left empty.
    #[cfg(test)]
    pub(crate) fn held_entry_count(&self) -> usize {
        self.inner.lock().expect("lockmgr").locks.len()
    }

    /// Recovery re-acquisition (SP24 abort atomicity): grab `(table, rowid)`
    /// EXCLUSIVELY for an inherited in-doubt local xid `Li` whose `Prepared(Li -> g)`
    /// row version this leader inherited but whose in-memory lock was wiped by the
    /// failover. Always installs the lock under `my_xid` — overwriting any holder of
    /// the SAME row, because on the rising edge no live transaction holds this row
    /// (the lock table started empty) and the inherited marker is the sole claimant.
    /// Idempotent: re-acquiring an already-held lock is a no-op. The lock is freed by
    /// the rise sweep's `release_all(Li)` once `g` is driven terminal — so a
    /// concurrent re-staging writer BLOCKS here until the inherited row resolves,
    /// giving exactly one live version (the serialize-before-serve invariant the
    /// per-session `effective_global_xid` fence cannot enforce under apply lag).
    pub(crate) fn reacquire_exclusive(
        &self,
        table: crabka_pgcatalog::TableId,
        rowid: u64,
        my_xid: u64,
    ) {
        let mut g = self.inner.lock().expect("lockmgr");
        // Install an exclusive lock held solely by `my_xid`. We intentionally do NOT
        // go through `try_acquire_locked` (which would return Conflict against a
        // pre-existing holder): on a fresh leadership rise the lock table is empty,
        // so this only ever installs a NEW lock or no-ops on a re-scan of the same
        // `Li`. Keeping it unconditional makes recovery deterministic regardless of
        // sweep re-entry.
        let lock = g
            .locks
            .entry(LockKey::Row(table, rowid))
            .or_insert_with(|| HeldLock {
                mode: LockMode::Exclusive,
                holders: HashSet::new(),
            });
        lock.mode = LockMode::Exclusive;
        lock.holders.insert(my_xid);
    }

    /// Acquire `(table, rowid)` in `mode` for `my_xid`, blocking until granted
    /// or `wait_cap` (when given) expires. Returns the deadlock or cap error
    /// (caller maps both to 40P01). See [`Self::acquire_key`].
    pub async fn acquire(
        &self,
        table: crabka_pgcatalog::TableId,
        rowid: u64,
        mode: LockMode,
        my_xid: u64,
        wait_cap: Option<Duration>,
    ) -> Result<(), AcquireError> {
        self.acquire_key(LockKey::Row(table, rowid), mode, my_xid, wait_cap)
            .await
    }

    /// Acquire `key` in `mode` for `my_xid`, blocking until granted or
    /// `wait_cap` (when given) expires. Returns [`AcquireError::Deadlock`] if
    /// blocking would close a wait-for cycle and [`AcquireError::CapExpired`]
    /// when the cap runs out first (callers map both to 40P01).
    /// Conflict-detect and waiter-register happen ATOMICALLY under one guard,
    /// and the holder's `release_all` wakes us via a permit-backed
    /// `notify_one` — so there is no lost-wakeup window and no chance of
    /// registering on a holder that already released.
    pub async fn acquire_key(
        &self,
        key: LockKey,
        mode: LockMode,
        my_xid: u64,
        wait_cap: Option<Duration>,
    ) -> Result<(), AcquireError> {
        let deadline = wait_cap.map(|cap| tokio::time::Instant::now() + cap);
        loop {
            let (notify, holder) = {
                let mut g = self.inner.lock().expect("lockmgr");
                match try_acquire_locked(&mut g, key.clone(), mode, my_xid) {
                    Acquire::Acquired => {
                        g.wait_for.remove(&my_xid); // no longer waiting
                        return Ok(());
                    }
                    Acquire::Conflict(holder) => {
                        if matches!(
                            check_cycle(&g.wait_for, holder, my_xid),
                            CycleCheck::Deadlock
                        ) {
                            g.wait_for.remove(&my_xid);
                            return Err(AcquireError::Deadlock);
                        }
                        g.wait_for.insert(my_xid, holder);
                        let n = Arc::new(Notify::new());
                        g.waiters.entry(holder).or_default().push(Arc::clone(&n));
                        (n, holder)
                    }
                }
            };
            // Guard dropped. `notify_one()` stores a permit if it fires before
            // we await, so this cannot lose a wakeup; on wake we loop and
            // re-attempt the acquire.
            match deadline {
                None => notify.notified().await,
                Some(deadline) => {
                    if tokio::time::timeout_at(deadline, notify.notified())
                        .await
                        .is_err()
                    {
                        // Deregister the abandoned wait entirely: the edge, so
                        // it cannot feed false cycles, and this waiter's entry
                        // in the holder's wake list, so a long-running holder
                        // on a hot key does not accumulate one dead `Notify`
                        // per expired wait until it releases.
                        let mut g = self.inner.lock().expect("lockmgr");
                        g.wait_for.remove(&my_xid);
                        if let Some(queue) = g.waiters.get_mut(&holder) {
                            queue.retain(|waiter| !Arc::ptr_eq(waiter, &notify));
                            if queue.is_empty() {
                                g.waiters.remove(&holder);
                            }
                        }
                        return Err(AcquireError::CapExpired);
                    }
                }
            }
        }
    }

    /// Number of waiters currently registered against `holder` (test-only:
    /// lets the cap-expiry test assert the abandoned wait was deregistered).
    #[cfg(test)]
    pub(crate) fn waiter_queue_len(&self, holder: u64) -> usize {
        self.inner
            .lock()
            .expect("lockmgr")
            .waiters
            .get(&holder)
            .map_or(0, Vec::len)
    }

    /// Release ONE lock held by `my_xid`, waking every waiter blocked on
    /// `my_xid` and clearing its edge.
    ///
    /// O(1) in the lock-table size, unlike [`Self::release_all`]'s full-table
    /// walk. The vacuum sweep holds at most one row lock at a time and drops
    /// it after every candidate row; paying a full-table walk per row is
    /// quadratic whenever a concurrent bulk writer holds a large lock set. A
    /// waiter woken here that was actually blocked on a DIFFERENT key still
    /// held by `my_xid` (impossible for the single-lock sweep, but harmless
    /// in general) simply re-attempts its acquire and re-blocks.
    pub fn release_key(&self, key: &LockKey, my_xid: u64) {
        let to_wake = {
            let mut g = self.inner.lock().expect("lockmgr");
            if let Some(lock) = g.locks.get_mut(key) {
                lock.holders.remove(&my_xid);
                if lock.holders.is_empty() {
                    g.locks.remove(key);
                }
            }
            g.wait_for.remove(&my_xid);
            g.waiters.remove(&my_xid).unwrap_or_default()
        };
        // Permit-backed like `release_all`: `notify_one` stores a permit if
        // the waiter has not yet reached `.await`, so no wakeup is ever lost.
        for n in to_wake {
            n.notify_one();
        }
    }

    /// Release every lock held by `my_xid`, wake its waiters, clear its edge.
    pub fn release_all(&self, my_xid: u64) {
        let to_wake = {
            let mut g = self.inner.lock().expect("lockmgr");
            g.locks.retain(|_, lock| {
                lock.holders.remove(&my_xid);
                !lock.holders.is_empty()
            });
            g.wait_for.remove(&my_xid);
            g.waiters.remove(&my_xid).unwrap_or_default()
        };
        // Each per-waiter `Notify` has exactly one consumer; `notify_one` stores
        // a permit if the waiter has not yet reached `.await`, so no wakeup is
        // ever lost.
        for n in to_wake {
            n.notify_one();
        }
    }

    #[cfg(test)]
    pub(crate) fn wait_for_register_only(&self, waiter: u64, holder: u64) {
        self.inner
            .lock()
            .expect("lockmgr")
            .wait_for
            .insert(waiter, holder);
    }
    #[cfg(test)]
    pub(crate) fn check_cycle(&self, holder: u64, my_xid: u64) -> CycleCheck {
        check_cycle(
            &self.inner.lock().expect("lockmgr").wait_for,
            holder,
            my_xid,
        )
    }
}

/// Locked, non-blocking acquire over `&mut Inner`. Idempotent if `my_xid`
/// already holds compatibly; a sole shared holder may upgrade to exclusive.
fn try_acquire_locked(inner: &mut Inner, key: LockKey, mode: LockMode, my_xid: u64) -> Acquire {
    match inner.locks.get_mut(&key) {
        None => {
            let mut holders = HashSet::new();
            holders.insert(my_xid);
            inner.locks.insert(key, HeldLock { mode, holders });
            Acquire::Acquired
        }
        Some(lock) => {
            if lock.holders.contains(&my_xid) {
                if mode == LockMode::Exclusive && lock.mode == LockMode::Shared {
                    if lock.holders.len() == 1 {
                        lock.mode = LockMode::Exclusive;
                        Acquire::Acquired
                    } else {
                        let other = *lock
                            .holders
                            .iter()
                            .find(|&&h| h != my_xid)
                            .expect("other holder");
                        Acquire::Conflict(other)
                    }
                } else {
                    Acquire::Acquired
                }
            } else if mode == LockMode::Shared && lock.mode == LockMode::Shared {
                lock.holders.insert(my_xid);
                Acquire::Acquired
            } else {
                Acquire::Conflict(*lock.holders.iter().next().expect("a holder"))
            }
        }
    }
}

/// Would adding `my_xid -> holder` close a cycle? Walk the chain from `holder`;
/// if it reaches `my_xid`, the edge closes a cycle.
fn check_cycle(wait_for: &HashMap<u64, u64>, holder: u64, my_xid: u64) -> CycleCheck {
    let mut cur = holder;
    let mut seen = HashSet::new();
    loop {
        if cur == my_xid {
            return CycleCheck::Deadlock;
        }
        if !seen.insert(cur) {
            return CycleCheck::Ok; // pre-existing cycle not through my_xid
        }
        match wait_for.get(&cur) {
            Some(&next) => cur = next,
            None => return CycleCheck::Ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_conflicts_shared_coexists() {
        let m = RowLockManager::new();
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 10),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 11),
            Acquire::Conflict(10)
        ));
        assert!(matches!(
            m.try_acquire(1, 2, LockMode::Shared, 11),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 2, LockMode::Shared, 12),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 2, LockMode::Exclusive, 13),
            Acquire::Conflict(_)
        ));
    }

    #[test]
    fn release_all_frees_rows_and_is_reacquirable() {
        let m = RowLockManager::new();
        m.try_acquire(1, 1, LockMode::Exclusive, 10);
        m.try_acquire(1, 2, LockMode::Exclusive, 10);
        m.release_all(10);
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 11),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 2, LockMode::Exclusive, 11),
            Acquire::Acquired
        ));
    }

    #[tokio::test]
    async fn release_key_frees_only_that_entry_and_wakes_its_waiter() {
        use std::sync::Arc;

        use assert2::assert;
        let m = Arc::new(RowLockManager::new());
        m.try_acquire(1, 1, LockMode::Exclusive, 10);
        m.try_acquire(1, 2, LockMode::Exclusive, 10);
        let m2 = Arc::clone(&m);
        let waiter = tokio::spawn(async move {
            // blocks: row (1,1) is held exclusively by xid 10
            m2.acquire(1, 1, LockMode::Exclusive, 11, None)
                .await
                .expect("not a deadlock");
        });
        tokio::task::yield_now().await;
        m.release_key(&LockKey::Row(1, 1), 10);
        // bound the wait so a regression FAILS instead of hanging forever
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("waiter did not hang")
            .expect("waiter completes");
        // Only the released entry was freed: (1,2) is still held by xid 10,
        // and the released entry was REMOVED (the waiter now holds it anew).
        assert!(matches!(
            m.try_acquire(1, 2, LockMode::Exclusive, 11),
            Acquire::Conflict(10)
        ));
        assert!(m.held_entry_count() == 2);
    }

    #[test]
    fn reacquire_by_same_holder_is_idempotent() {
        let m = RowLockManager::new();
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 10),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 10),
            Acquire::Acquired
        ));
    }

    #[test]
    fn shared_holder_upgrades_to_exclusive_when_sole() {
        let m = RowLockManager::new();
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Shared, 10),
            Acquire::Acquired
        ));
        // sole shared holder upgrades to exclusive
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 10),
            Acquire::Acquired
        ));
        // now another exclusive conflicts
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 11),
            Acquire::Conflict(10)
        ));
    }

    #[tokio::test]
    async fn acquire_resumes_when_holder_releases() {
        use std::sync::Arc;
        let m = Arc::new(RowLockManager::new());
        m.try_acquire(1, 1, LockMode::Exclusive, 10);
        let m2 = Arc::clone(&m);
        let waiter = tokio::spawn(async move {
            // blocks: row is held exclusively by xid 10
            m2.acquire(1, 1, LockMode::Exclusive, 11, None)
                .await
                .expect("not a deadlock");
        });
        tokio::task::yield_now().await;
        m.release_all(10);
        // bound the wait so a regression FAILS instead of hanging forever
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("waiter did not hang")
            .expect("waiter completes");
    }

    #[tokio::test]
    async fn acquire_does_not_lose_wakeup_under_race() {
        // Stress: holder releases immediately, racing the waiter's registration.
        // The waiter must still wake. Run many iterations to shake out a
        // lost-wakeup.
        use std::sync::Arc;
        for _ in 0..50 {
            let m = Arc::new(RowLockManager::new());
            m.try_acquire(1, 1, LockMode::Exclusive, 10);
            let m2 = Arc::clone(&m);
            let waiter =
                tokio::spawn(async move { m2.acquire(1, 1, LockMode::Exclusive, 11, None).await });
            let m3 = Arc::clone(&m);
            let releaser = tokio::spawn(async move {
                m3.release_all(10);
            });
            releaser.await.expect("releaser");
            // must not hang: bound the wait so a lost wakeup fails the test
            // instead of hanging forever
            tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
                .await
                .expect("waiter did not hang")
                .expect("waiter task")
                .expect("not a deadlock");
        }
    }

    #[tokio::test]
    async fn acquire_succeeds_when_holder_released_before_wait() {
        // The holder-released-before-register bug: the row is freed BEFORE the
        // waiter ever calls `acquire`, so `acquire` must simply succeed (the
        // atomic try-acquire-or-register under one guard sees a free row).
        use std::sync::Arc;
        let m = Arc::new(RowLockManager::new());
        m.try_acquire(1, 1, LockMode::Exclusive, 10);
        m.release_all(10); // released before the waiter starts
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            m.acquire(1, 1, LockMode::Exclusive, 11, None),
        )
        .await
        .expect("did not hang")
        .expect("acquires the now-free row");
    }

    #[tokio::test]
    async fn acquire_returns_err_when_edge_closes_a_cycle() {
        // Deadlock path: pre-register 10 -> 11 (10 waits for 11). Now have 11
        // try to acquire a row held by 10: the edge 11 -> 10 closes the cycle
        // 10 -> 11 -> 10, so `acquire` must return Err(()) instead of blocking.
        use std::sync::Arc;
        let m = Arc::new(RowLockManager::new());
        m.wait_for_register_only(10, 11); // 10 waits for 11
        m.try_acquire(1, 1, LockMode::Exclusive, 10); // 10 holds row 1
        let res = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            m.acquire(1, 1, LockMode::Exclusive, 11, None), // 11 wants row 1 held by 10
        )
        .await
        .expect("did not hang");
        assert!(res.is_err(), "closing the cycle must abort with Err(())");
    }

    #[tokio::test]
    async fn unique_key_locks_conflict_per_key_and_release_removes_entries() {
        use std::sync::Arc;

        use assert2::assert;
        let m = Arc::new(RowLockManager::new());
        let k = LockKey::UniqueKey(vec![1, 2, 3]);
        m.acquire_key(k.clone(), LockMode::Exclusive, 10, None)
            .await
            .expect("free key");
        // A DIFFERENT key does not conflict.
        m.acquire_key(LockKey::UniqueKey(vec![9]), LockMode::Exclusive, 11, None)
            .await
            .expect("different key is free");
        // Re-acquiring my own key is idempotent.
        m.acquire_key(k.clone(), LockMode::Exclusive, 10, None)
            .await
            .expect("idempotent re-acquire");
        // The SAME key by another xid blocks until the holder releases.
        let m2 = Arc::clone(&m);
        let waiter =
            tokio::spawn(async move { m2.acquire_key(k, LockMode::Exclusive, 12, None).await });
        tokio::task::yield_now().await;
        m.release_all(10);
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("waiter did not hang")
            .expect("waiter join")
            .expect("not a deadlock");
        // Memory hygiene: releasing every holder REMOVES the entries (no
        // unbounded growth under key churn).
        m.release_all(11);
        m.release_all(12);
        assert!(m.held_entry_count() == 0);
    }

    #[tokio::test]
    async fn row_and_unique_key_locks_share_the_wait_graph() {
        use std::sync::Arc;

        use assert2::assert;
        let m = Arc::new(RowLockManager::new());
        let key = LockKey::UniqueKey(vec![7]);
        // xid 10 holds row (1,1); xid 11 holds unique key K.
        m.try_acquire(1, 1, LockMode::Exclusive, 10);
        m.acquire_key(key.clone(), LockMode::Exclusive, 11, None)
            .await
            .expect("free key");
        // 10 blocks on K (registers the edge 10 -> 11 in the shared graph).
        let m2 = Arc::clone(&m);
        let waiter =
            tokio::spawn(async move { m2.acquire_key(key, LockMode::Exclusive, 10, None).await });
        tokio::task::yield_now().await;
        // 11 now tries the row 10 holds: the edge 11 -> 10 closes a cycle that
        // SPANS a row lock and a unique-key lock — must be Err, not a hang.
        let res = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            m.acquire(1, 1, LockMode::Exclusive, 11, None),
        )
        .await
        .expect("did not hang");
        assert!(res.is_err());
        // Unblock the waiter so the test ends cleanly.
        m.release_all(11);
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("waiter did not hang")
            .expect("waiter join")
            .expect("not a deadlock");
    }

    #[tokio::test]
    async fn capped_wait_expires_as_presumed_distributed_deadlock() {
        use assert2::assert;
        let m = RowLockManager::new();
        m.try_acquire(1, 1, LockMode::Exclusive, 10);

        let result = m
            .acquire(
                1,
                1,
                LockMode::Exclusive,
                11,
                Some(Duration::from_millis(50)),
            )
            .await;

        assert!(result == Err(AcquireError::CapExpired));
        // The abandoned wait's edge is cleared: the holder later waiting on
        // the expired waiter must not read as a cycle through a ghost edge.
        assert!(matches!(m.check_cycle(11, 10), CycleCheck::Ok));
        // And its wake registration is gone: a long-running holder on a hot
        // key must not retain one dead Notify per expired wait.
        assert!(m.waiter_queue_len(10) == 0);
    }

    #[tokio::test]
    async fn capped_wait_granted_before_expiry_succeeds() {
        use std::sync::Arc;

        use assert2::assert;
        let m = Arc::new(RowLockManager::new());
        m.try_acquire(1, 1, LockMode::Exclusive, 10);
        let m2 = Arc::clone(&m);
        let waiter = tokio::spawn(async move {
            m2.acquire(1, 1, LockMode::Exclusive, 11, Some(Duration::from_secs(30)))
                .await
        });
        tokio::task::yield_now().await;

        m.release_all(10);

        let granted = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter did not hang")
            .expect("waiter join");
        assert!(granted == Ok(()));
    }

    #[test]
    fn cycle_check_detects_a_two_cycle() {
        let m = RowLockManager::new();
        m.wait_for_register_only(10, 11); // 10 waits for 11
        // now 11 -> 10 would close the cycle; ask "does my_xid=11 waiting for
        // holder=10 close a cycle?": walk from holder=10 -> 11 -> ==11 -> yes.
        assert!(matches!(m.check_cycle(10, 11), CycleCheck::Deadlock));
        // a non-closing edge is fine: my_xid=99 waiting for holder=11; walk from
        // 11 -> (no edge) -> Ok.
        assert!(matches!(m.check_cycle(11, 99), CycleCheck::Ok));
    }
}
