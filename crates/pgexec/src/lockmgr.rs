//! In-memory lock manager for concurrent writers. Exclusive/shared locks over
//! three key spaces — heap rows (`(table, rowid)`), unique local index keys
//! (`LockKey::UniqueKey`), and unique-index backfill relations — transaction-
//! scoped (released at COMMIT/ROLLBACK).
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
//! engines is invisible to it. That happens when each leg of a cross-range
//! transaction waits on the other's participant. A session that can join such a
//! cycle passes a wait cap to `acquire`. An expired cap aborts the waiter as a
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

impl LockMode {
    /// The mode's name as a span attribute. It is a fixed pair of strings, so
    /// `pg.lock.mode` stays a discriminator and not free text.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Exclusive => "exclusive",
        }
    }
}

/// `40P01`, `deadlock_detected`, which is what a caller maps both
/// [`AcquireError`] variants onto. This records it here so a lock span carries
/// the same discriminator the statement span will.
const DEADLOCK_SQLSTATE: &str = "40P01";

/// Build the span covering a *blocked* lock acquisition.
///
/// Only the conflict arm of [`RowLockManager::acquire_key`] creates this, and
/// deliberately so. An uncontended acquire happens once per row a write
/// touches, so a span each would bury the statement's trace under thousands of
/// zero-duration spans. What an operator wants from a lock is the wait, and a
/// span exists here exactly when there was one.
///
/// TRACE, because even contended acquires are frequent on a hot key.
fn wait_span(key: &LockKey, mode: LockMode, owner: LockOwner) -> tracing::Span {
    // A session lock has no transaction id; record the id it does have, so the
    // span still names who waited.
    let (my_xid, owner_kind) = match owner {
        LockOwner::Xid(xid) => (xid, "xid"),
        LockOwner::Session(id) => (id, "session"),
    };
    let span = tracing::trace_span!(
        target: crate::telemetry::EXEC_TARGET,
        "pg.lock.row",
        otel.kind = "internal",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        db.response.status_code = tracing::field::Empty,
        "error.type" = tracing::field::Empty,
        pg.table_id = tracing::field::Empty,
        pg.rowid = tracing::field::Empty,
        pg.lock.key_kind = tracing::field::Empty,
        pg.lock.mode = mode.as_str(),
        pg.lock.waited = true,
        pg.lock.holder_xid = tracing::field::Empty,
        pg.lock.outcome = tracing::field::Empty,
        pg.txn.xid = crate::telemetry::integer(my_xid),
        pg.lock.owner_kind = owner_kind,
    );
    match key {
        LockKey::Row(table, rowid) => {
            span.record("pg.lock.key_kind", "row");
            span.record("pg.table_id", crate::telemetry::integer(*table));
            span.record("pg.rowid", crate::telemetry::integer(*rowid));
        }
        // A unique-key lock names an index entry rather than a row, so it
        // carries neither a table id nor a rowid — the encoded key itself is
        // both high-cardinality and derived from user data, so it is not
        // recorded at all.
        LockKey::UniqueKey(_) => {
            span.record("pg.lock.key_kind", "unique_key");
        }
        // The relation-wide gate a unique-index build takes. It names only the
        // relation, so there is no rowid to record.
        LockKey::UniqueIndexRelation(table) => {
            span.record("pg.lock.key_kind", "unique_index_relation");
            span.record("pg.table_id", crate::telemetry::integer(*table));
        }
    }
    span
}

/// Result of a non-blocking lock attempt.
pub enum Acquire {
    Acquired,
    /// Held by `holder` (one of the holders).
    Conflict(LockOwner),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LockOwner {
    Xid(u64),
    Session(u64),
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
    /// The caller's wait cap expired before the grant. The manager treats this
    /// as a distributed deadlock, because a cross-engine cycle never shows up
    /// in any single engine's wait-for graph.
    CapExpired,
}

/// Identity of a lockable resource. Row locks and unique-key locks live in the
/// same table and the same wait-for graph, so the manager detects a deadlock
/// cycle that spans both kinds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockKey {
    /// A heap row: `(table, rowid)`.
    Row(crabka_pgcatalog::TableId, u64),
    /// A unique local index key: the encoded index-entry prefix
    /// (`secondary_index_entry_prefix(table, index, values)`), a deterministic
    /// identity for `(table, index, key values)`. This serializes the
    /// check-then-write unique probe per key, not engine-wide.
    UniqueKey(Vec<u8>),
    /// A relation whose ordinary writes run SHARED with unique-index backfill
    /// running EXCLUSIVE. Session ownership makes same-transaction upgrades
    /// atomic and keeps its waits in the ordinary row-lock deadlock graph.
    UniqueIndexRelation(crabka_pgcatalog::TableId),
}

struct HeldLock {
    mode: LockMode,
    holders: HashSet<LockOwner>,
}

struct Inner {
    locks: HashMap<LockKey, HeldLock>,
    waiters: HashMap<LockOwner, Vec<Arc<Notify>>>, // holder -> waiters' notifiers
    // wait-for graph: each waiting xid -> the single holder xid it blocks on.
    // NOTE: this is single-successor (one out-edge per waiter), so the eager
    // cycle check is exact for exclusive locks. A deadlock cycle that runs only
    // through a non-chosen *shared* co-holder may not be flagged on the first
    // check — but it cannot hang permanently: every release re-wakes the waiter,
    // which re-checks against a possibly-different chosen holder until the cyclic
    // one is the edge. Shared-only deadlocks are out of SP6's tested scope.
    wait_for: HashMap<LockOwner, LockOwner>,
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

    /// Non-blocking acquire. It is idempotent if `my_xid` already holds
    /// compatibly, and a sole shared holder may upgrade to exclusive. This is a
    /// thin wrapper that locks and delegates to [`try_acquire_locked`].
    ///
    /// This is how `NOWAIT` and `SKIP LOCKED` are served: both need to know
    /// whether the row is free *without* waiting, and differ only in what they do
    /// with a conflict.
    #[cfg(test)]
    pub(crate) fn try_acquire(
        &self,
        table: crabka_pgcatalog::TableId,
        rowid: u64,
        mode: LockMode,
        my_xid: u64,
    ) -> Acquire {
        self.try_acquire_as(table, rowid, mode, LockOwner::Xid(my_xid))
    }

    pub(crate) fn try_acquire_as(
        &self,
        table: crabka_pgcatalog::TableId,
        rowid: u64,
        mode: LockMode,
        owner: LockOwner,
    ) -> Acquire {
        let mut g = self.inner.lock().expect("lockmgr");
        try_acquire_locked(&mut g, LockKey::Row(table, rowid), mode, owner)
    }

    /// Number of lock-table entries currently held, over both key spaces. This
    /// lets tests assert that released entries are REMOVED, not left empty.
    #[cfg(test)]
    pub(crate) fn held_entry_count(&self) -> usize {
        self.inner.lock().expect("lockmgr").locks.len()
    }

    /// Recovery re-acquisition (SP24 abort atomicity): grab `(table, rowid)`
    /// EXCLUSIVELY for an inherited in-doubt local xid `Li`. This leader
    /// inherited that xid's `Prepared(Li -> g)` row version, but the failover
    /// wiped its in-memory lock.
    ///
    /// This always installs the lock under `my_xid` and overwrites any holder of
    /// the SAME row. On the rising edge no live transaction holds this row,
    /// because the lock table started empty, and the inherited marker is the
    /// sole claimant. It is idempotent: a re-acquire of an already-held lock is
    /// a no-op. The rise sweep's `release_all(Li)` frees the lock once `g` is
    /// driven terminal. So a concurrent re-staging writer BLOCKS here until the
    /// inherited row resolves, which gives exactly one live version.
    /// That is the serialize-before-serve invariant the per-session
    /// `effective_global_xid` fence cannot enforce under apply lag.
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
        lock.holders.insert(LockOwner::Xid(my_xid));
    }

    /// Acquire `(table, rowid)` in `mode` for `my_xid`. This blocks until the
    /// grant, or until `wait_cap` expires when the caller gives one. Returns the
    /// deadlock or cap error, and the caller maps both to 40P01. See
    /// [`Self::acquire_key`].
    pub async fn acquire(
        &self,
        table: crabka_pgcatalog::TableId,
        rowid: u64,
        mode: LockMode,
        my_xid: u64,
        wait_cap: Option<Duration>,
    ) -> Result<(), AcquireError> {
        self.acquire_as(table, rowid, mode, LockOwner::Xid(my_xid), wait_cap)
            .await
    }

    pub(crate) async fn acquire_as(
        &self,
        table: crabka_pgcatalog::TableId,
        rowid: u64,
        mode: LockMode,
        owner: LockOwner,
        wait_cap: Option<Duration>,
    ) -> Result<(), AcquireError> {
        self.acquire_key_as(LockKey::Row(table, rowid), mode, owner, wait_cap)
            .await
    }

    /// Acquire `key` in `mode` for `my_xid`. This blocks until the grant, or
    /// until `wait_cap` expires when the caller gives one. Returns
    /// [`AcquireError::Deadlock`] if a block would close a wait-for cycle, and
    /// [`AcquireError::CapExpired`] when the cap runs out first. Callers map
    /// both to 40P01.
    ///
    /// Conflict-detect and waiter-register happen ATOMICALLY under one guard,
    /// and the holder's `release_all` wakes us via a permit-backed
    /// `notify_one` — so there is no lost-wakeup window and no chance of
    /// registering on a holder that already released.
    #[cfg(test)]
    pub async fn acquire_key(
        &self,
        key: LockKey,
        mode: LockMode,
        my_xid: u64,
        wait_cap: Option<Duration>,
    ) -> Result<(), AcquireError> {
        self.acquire_key_as(key, mode, LockOwner::Xid(my_xid), wait_cap)
            .await
    }

    pub(crate) async fn acquire_key_as(
        &self,
        key: LockKey,
        mode: LockMode,
        owner: LockOwner,
        wait_cap: Option<Duration>,
    ) -> Result<(), AcquireError> {
        let deadline = wait_cap.map(|cap| tokio::time::Instant::now() + cap);
        // Opened lazily, and only once a conflict has actually forced a wait —
        // see [`wait_span`]. An uncontended acquire leaves this `Span::none()`,
        // which allocates nothing and records nothing.
        let mut wait = tracing::Span::none();
        loop {
            let (notify, holder) = {
                let mut g = self.inner.lock().expect("lockmgr");
                match try_acquire_locked(&mut g, key.clone(), mode, owner) {
                    Acquire::Acquired => {
                        g.wait_for.remove(&owner); // no longer waiting
                        wait.record("pg.lock.outcome", "granted");
                        return Ok(());
                    }
                    Acquire::Conflict(holder) => {
                        if wait.is_none() {
                            wait = wait_span(&key, mode, owner);
                        }
                        // The holder can differ between rounds, so this names
                        // the transaction most recently waited on rather than
                        // the first one.
                        let holder_id = match holder {
                            LockOwner::Xid(xid) | LockOwner::Session(xid) => xid,
                        };
                        wait.record("pg.lock.holder_xid", crate::telemetry::integer(holder_id));
                        if matches!(
                            check_cycle(&g.wait_for, holder, owner),
                            CycleCheck::Deadlock
                        ) {
                            g.wait_for.remove(&owner);
                            wait.record("pg.lock.outcome", "deadlock");
                            crate::telemetry::record_error(
                                &wait,
                                DEADLOCK_SQLSTATE,
                                "deadlock detected",
                            );
                            return Err(AcquireError::Deadlock);
                        }
                        g.wait_for.insert(owner, holder);
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
                        g.wait_for.remove(&owner);
                        if let Some(queue) = g.waiters.get_mut(&holder) {
                            queue.retain(|waiter| !Arc::ptr_eq(waiter, &notify));
                            if queue.is_empty() {
                                g.waiters.remove(&holder);
                            }
                        }
                        wait.record("pg.lock.outcome", "cap_expired");
                        crate::telemetry::record_error(
                            &wait,
                            DEADLOCK_SQLSTATE,
                            "canceling statement due to a lock wait timeout",
                        );
                        return Err(AcquireError::CapExpired);
                    }
                }
            }
        }
    }

    /// Number of waiters currently registered against `holder`. Test-only: it
    /// lets the cap-expiry test assert that the abandoned wait was
    /// deregistered.
    #[cfg(test)]
    pub(crate) fn waiter_queue_len(&self, holder: u64) -> usize {
        self.waiter_queue_len_as(LockOwner::Xid(holder))
    }

    #[cfg(test)]
    pub(crate) fn waiter_queue_len_as(&self, holder: LockOwner) -> usize {
        self.inner
            .lock()
            .expect("lockmgr")
            .waiters
            .get(&holder)
            .map_or(0, Vec::len)
    }

    /// Release ONE lock held by `my_xid`. This wakes every waiter blocked on
    /// `my_xid` and clears its edge.
    ///
    /// It is O(1) in the lock-table size, unlike [`Self::release_all`]'s
    /// full-table walk. The vacuum sweep holds at most one row lock at a time
    /// and drops it after every candidate row. A full-table walk per row would
    /// be quadratic whenever a concurrent bulk writer holds a large lock set. A
    /// waiter woken here that was blocked on a DIFFERENT key `my_xid` still
    /// holds simply re-attempts its acquire and re-blocks. That cannot happen
    /// for the single-lock sweep, and it is harmless in general.
    pub fn release_key(&self, key: &LockKey, my_xid: u64) {
        self.release_key_as(key, LockOwner::Xid(my_xid));
    }

    pub(crate) fn release_key_as(&self, key: &LockKey, owner: LockOwner) {
        let to_wake = {
            let mut g = self.inner.lock().expect("lockmgr");
            if let Some(lock) = g.locks.get_mut(key) {
                lock.holders.remove(&owner);
                if lock.holders.is_empty() {
                    g.locks.remove(key);
                }
            }
            g.wait_for.remove(&owner);
            g.waiters.remove(&owner).unwrap_or_default()
        };
        // Permit-backed like `release_all`: `notify_one` stores a permit if
        // the waiter has not yet reached `.await`, so no wakeup is ever lost.
        for n in to_wake {
            n.notify_one();
        }
    }

    /// Release every lock held by `my_xid`, wake its waiters, and clear its
    /// edge.
    pub fn release_all(&self, my_xid: u64) {
        self.release_all_as(LockOwner::Xid(my_xid));
    }

    pub(crate) fn release_all_as(&self, owner: LockOwner) {
        let to_wake = {
            let mut g = self.inner.lock().expect("lockmgr");
            g.locks.retain(|_, lock| {
                lock.holders.remove(&owner);
                !lock.holders.is_empty()
            });
            g.wait_for.remove(&owner);
            g.waiters.remove(&owner).unwrap_or_default()
        };
        // Each per-waiter `Notify` has exactly one consumer; `notify_one` stores
        // a permit if the waiter has not yet reached `.await`, so no wakeup is
        // ever lost.
        for n in to_wake {
            n.notify_one();
        }
    }

    /// Snapshot every lock `my_xid` currently holds for a savepoint boundary.
    pub(crate) fn held_locks_as(&self, owner: LockOwner) -> HashMap<LockKey, LockMode> {
        self.inner
            .lock()
            .expect("lockmgr")
            .locks
            .iter()
            .filter(|(_, lock)| lock.holders.contains(&owner))
            .map(|(key, lock)| (key.clone(), lock.mode))
            .collect()
    }

    /// Restore `my_xid`'s lock set to a savepoint snapshot, releasing locks
    /// acquired later and undoing a later shared-to-exclusive upgrade.
    pub(crate) fn restore_locks_as(&self, owner: LockOwner, snapshot: &HashMap<LockKey, LockMode>) {
        let to_wake = {
            let mut inner = self.inner.lock().expect("lockmgr");
            inner.locks.retain(|key, lock| {
                if !lock.holders.contains(&owner) {
                    return true;
                }
                match snapshot.get(key) {
                    Some(mode) => {
                        lock.mode = *mode;
                        true
                    }
                    None => {
                        lock.holders.remove(&owner);
                        !lock.holders.is_empty()
                    }
                }
            });
            inner.wait_for.remove(&owner);
            inner.waiters.remove(&owner).unwrap_or_default()
        };
        for waiter in to_wake {
            waiter.notify_one();
        }
    }

    #[cfg(test)]
    pub(crate) fn wait_for_register_only(&self, waiter: u64, holder: u64) {
        self.inner
            .lock()
            .expect("lockmgr")
            .wait_for
            .insert(LockOwner::Xid(waiter), LockOwner::Xid(holder));
    }
    #[cfg(test)]
    pub(crate) fn check_cycle(&self, holder: u64, my_xid: u64) -> CycleCheck {
        check_cycle(
            &self.inner.lock().expect("lockmgr").wait_for,
            LockOwner::Xid(holder),
            LockOwner::Xid(my_xid),
        )
    }
}

/// Locked, non-blocking acquire over `&mut Inner`. Idempotent if `my_xid`
/// already holds compatibly; a sole shared holder may upgrade to exclusive.
fn try_acquire_locked(
    inner: &mut Inner,
    key: LockKey,
    mode: LockMode,
    owner: LockOwner,
) -> Acquire {
    match inner.locks.get_mut(&key) {
        None => {
            let mut holders = HashSet::new();
            holders.insert(owner);
            inner.locks.insert(key, HeldLock { mode, holders });
            Acquire::Acquired
        }
        Some(lock) => {
            if lock.holders.contains(&owner) {
                if mode == LockMode::Exclusive && lock.mode == LockMode::Shared {
                    if lock.holders.len() == 1 {
                        lock.mode = LockMode::Exclusive;
                        Acquire::Acquired
                    } else {
                        let other = *lock
                            .holders
                            .iter()
                            .find(|&&holder| holder != owner)
                            .expect("other holder");
                        Acquire::Conflict(other)
                    }
                } else {
                    Acquire::Acquired
                }
            } else if mode == LockMode::Shared && lock.mode == LockMode::Shared {
                lock.holders.insert(owner);
                Acquire::Acquired
            } else {
                Acquire::Conflict(*lock.holders.iter().next().expect("a holder"))
            }
        }
    }
}

/// Would adding `my_xid -> holder` close a cycle? Walk the chain from `holder`;
/// if it reaches `my_xid`, the edge closes a cycle.
fn check_cycle(
    wait_for: &HashMap<LockOwner, LockOwner>,
    holder: LockOwner,
    owner: LockOwner,
) -> CycleCheck {
    let mut cur = holder;
    let mut seen = HashSet::new();
    loop {
        if cur == owner {
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

/// A session's identity in the S3 table/advisory lock tables. Sessions are the
/// lock holders there, not transactions. `LOCK TABLE` and the advisory-lock
/// family both work in a read-only transaction that never assigns an xid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionLockId(pub u64);

/// One session's hold on a relation, with the re-entrancy count `PostgreSQL`
/// keeps (`LOCK TABLE t; LOCK TABLE t;` is two holds of the same mode).
#[derive(Debug, Clone, Copy)]
struct TableHold {
    session: SessionLockId,
    mode: crabka_pgparser::ast::TableLockMode,
}

/// S3: relation-level locks with `PostgreSQL`'s eight modes and conflict matrix.
///
/// Holds are session-scoped, and the end of the transaction that took them
/// releases them together, exactly as `PostgreSQL` releases relation locks.
/// Locks a session already holds never conflict with its own new request, so a
/// transaction can escalate freely.
#[derive(Debug, Default)]
pub struct TableLockManager {
    holds: Mutex<Vec<(crabka_pgcatalog::TableId, TableHold)>>,
}

/// Why a table-lock acquisition failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockError {
    /// The statement asked for `NOWAIT`, and another session holds a
    /// conflicting mode.
    NotAvailable,
}

impl TableLockManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The session that currently blocks `mode` on `table`, if any.
    fn conflicting_holder(
        holds: &[(crabka_pgcatalog::TableId, TableHold)],
        table: crabka_pgcatalog::TableId,
        session: SessionLockId,
        mode: crabka_pgparser::ast::TableLockMode,
    ) -> Option<SessionLockId> {
        holds
            .iter()
            .filter(|(held_table, hold)| *held_table == table && hold.session != session)
            .find(|(_, hold)| mode.conflicts_with(hold.mode))
            .map(|(_, hold)| hold.session)
    }

    /// Take `mode` on `table` for `session`, and record the hold.
    ///
    /// # Errors
    ///
    /// Returns [`TableLockError::NotAvailable`] when another session holds a
    /// conflicting mode.
    pub fn acquire(
        &self,
        table: crabka_pgcatalog::TableId,
        session: SessionLockId,
        mode: crabka_pgparser::ast::TableLockMode,
    ) -> Result<(), TableLockError> {
        let mut holds = self.holds.lock().expect("table lock manager");
        if Self::conflicting_holder(&holds, table, session, mode).is_some() {
            return Err(TableLockError::NotAvailable);
        }
        holds.push((table, TableHold { session, mode }));
        Ok(())
    }

    /// Release every relation lock `session` holds.
    pub fn release_all(&self, session: SessionLockId) {
        self.holds
            .lock()
            .expect("table lock manager")
            .retain(|(_, hold)| hold.session != session);
    }

    pub(crate) fn hold_count_for(&self, session: SessionLockId) -> usize {
        self.holds
            .lock()
            .expect("table lock manager")
            .iter()
            .filter(|(_, hold)| hold.session == session)
            .count()
    }

    pub(crate) fn restore_hold_count(&self, session: SessionLockId, count: usize) {
        let mut seen = 0;
        self.holds
            .lock()
            .expect("table lock manager")
            .retain(|(_, hold)| {
                if hold.session != session {
                    return true;
                }
                seen += 1;
                seen <= count
            });
    }

    /// The modes `session` currently holds on `table`, weakest first.
    #[cfg(test)]
    #[must_use]
    pub fn held_modes(
        &self,
        table: crabka_pgcatalog::TableId,
        session: SessionLockId,
    ) -> Vec<crabka_pgparser::ast::TableLockMode> {
        let mut modes: Vec<_> = self
            .holds
            .lock()
            .expect("table lock manager")
            .iter()
            .filter(|(held_table, hold)| *held_table == table && hold.session == session)
            .map(|(_, hold)| hold.mode)
            .collect();
        modes.sort_unstable();
        modes
    }
}

/// The scope an advisory lock is released at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvisoryScope {
    /// Held until an explicit unlock, or until the session ends.
    Session,
    /// The end of the current transaction releases it automatically.
    Transaction,
}

#[derive(Debug, Clone, Copy)]
struct AdvisoryHold {
    session: SessionLockId,
    key: i64,
    shared: bool,
    scope: AdvisoryScope,
}

/// S3: the advisory-lock family's shared state.
///
/// `PostgreSQL` advisory locks are counted: a second take of the same key needs
/// two unlocks. Both the 64-bit and the `(int4, int4)` key spellings map onto
/// one `i64` key, exactly as `PostgreSQL` packs them.
#[derive(Debug, Default)]
pub struct AdvisoryLockManager {
    holds: Mutex<Vec<AdvisoryHold>>,
}

impl AdvisoryLockManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `PostgreSQL`'s packing of the two-`int4` advisory key spelling.
    #[must_use]
    pub const fn pack_key(high: i32, low: i32) -> i64 {
        ((high as i64) << 32) | (low as u32) as i64
    }

    /// Take `key` for `session` when no other session conflicts. Returns
    /// whether the manager granted the lock (`pg_try_advisory_lock`'s
    /// boolean).
    pub fn try_lock(
        &self,
        key: i64,
        session: SessionLockId,
        shared: bool,
        scope: AdvisoryScope,
    ) -> bool {
        let mut holds = self.holds.lock().expect("advisory lock manager");
        let conflict = holds
            .iter()
            .any(|hold| hold.key == key && hold.session != session && !(shared && hold.shared));
        if conflict {
            return false;
        }
        holds.push(AdvisoryHold {
            session,
            key,
            shared,
            scope,
        });
        true
    }

    /// Drop one hold of `key` in the requested sharing mode. Returns whether a
    /// matching hold existed (`pg_advisory_unlock`'s boolean).
    pub fn unlock(&self, key: i64, session: SessionLockId, shared: bool) -> bool {
        let mut holds = self.holds.lock().expect("advisory lock manager");
        let found = holds.iter().rposition(|hold| {
            hold.key == key
                && hold.session == session
                && hold.shared == shared
                && hold.scope == AdvisoryScope::Session
        });
        match found {
            Some(index) => {
                holds.remove(index);
                true
            }
            None => false,
        }
    }

    /// Release every session-scoped advisory lock `session` holds
    /// (`pg_advisory_unlock_all`).
    pub fn unlock_all(&self, session: SessionLockId) {
        self.holds
            .lock()
            .expect("advisory lock manager")
            .retain(|hold| hold.session != session || hold.scope != AdvisoryScope::Session);
    }

    /// Release `session`'s transaction-scoped advisory locks at transaction end.
    pub fn release_transaction(&self, session: SessionLockId) {
        self.holds
            .lock()
            .expect("advisory lock manager")
            .retain(|hold| hold.session != session || hold.scope != AdvisoryScope::Transaction);
    }

    pub(crate) fn transaction_hold_count(&self, session: SessionLockId) -> usize {
        self.holds
            .lock()
            .expect("advisory lock manager")
            .iter()
            .filter(|hold| hold.session == session && hold.scope == AdvisoryScope::Transaction)
            .count()
    }

    pub(crate) fn restore_transaction_hold_count(&self, session: SessionLockId, count: usize) {
        let mut seen = 0;
        self.holds
            .lock()
            .expect("advisory lock manager")
            .retain(|hold| {
                if hold.session != session || hold.scope != AdvisoryScope::Transaction {
                    return true;
                }
                seen += 1;
                seen <= count
            });
    }

    /// Release everything `session` holds, at disconnect.
    pub fn release_session(&self, session: SessionLockId) {
        self.holds
            .lock()
            .expect("advisory lock manager")
            .retain(|hold| hold.session != session);
    }

    /// How many holds `session` currently has on `key`.
    #[cfg(test)]
    #[must_use]
    pub fn hold_count(&self, key: i64, session: SessionLockId) -> usize {
        self.holds
            .lock()
            .expect("advisory lock manager")
            .iter()
            .filter(|hold| hold.key == key && hold.session == session)
            .count()
    }
}

/// The engine-wide S3 lock state every session shares: relation locks, advisory
/// locks, and the allocator for session lock identities.
#[derive(Debug, Default)]
pub struct SessionLocks {
    pub tables: TableLockManager,
    pub advisory: AdvisoryLockManager,
    next_session: std::sync::atomic::AtomicU64,
}

impl SessionLocks {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: TableLockManager::new(),
            advisory: AdvisoryLockManager::new(),
            next_session: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Hand out a fresh session lock identity.
    pub fn next_session_id(&self) -> SessionLockId {
        SessionLockId(
            self.next_session
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Drop every lock `session` holds, at disconnect.
    pub fn release_session(&self, session: SessionLockId) {
        self.tables.release_all(session);
        self.advisory.release_session(session);
    }
}

#[cfg(test)]
mod session_lock_tests {
    use assert2::assert;
    use crabka_pgparser::ast::TableLockMode;

    use super::*;

    #[test]
    fn the_lock_conflict_matrix_matches_postgres() {
        use TableLockMode as M;
        // PostgreSQL's `LockConflicts[]`, weakest mode first, as a bitmask row
        // per mode: bit i is set when the row's mode conflicts with modes[i].
        let modes = [
            M::AccessShare,
            M::RowShare,
            M::RowExclusive,
            M::ShareUpdateExclusive,
            M::Share,
            M::ShareRowExclusive,
            M::Exclusive,
            M::AccessExclusive,
        ];
        let expected = [
            0b0000_0001_u8,
            0b0000_0011,
            0b0000_1111,
            0b0001_1111,
            0b0011_0111,
            0b0011_1111,
            0b0111_1111,
            0b1111_1111,
        ];
        for (row, mode) in modes.iter().enumerate() {
            let mut mask = 0_u8;
            for (column, other) in modes.iter().enumerate() {
                if mode.conflicts_with(*other) {
                    mask |= 0b1000_0000 >> column;
                }
            }
            assert!(mask == expected[row], "row {row} mask {mask:08b}");
        }
    }

    #[test]
    fn a_session_never_conflicts_with_its_own_relation_locks() {
        let manager = TableLockManager::new();
        let me = SessionLockId(1);
        assert!(
            manager
                .acquire(7, me, TableLockMode::AccessExclusive)
                .is_ok()
        );
        assert!(manager.acquire(7, me, TableLockMode::AccessShare).is_ok());
        assert!(
            manager.held_modes(7, me)
                == vec![TableLockMode::AccessShare, TableLockMode::AccessExclusive]
        );
    }

    #[test]
    fn a_conflicting_mode_from_another_session_is_unavailable_until_release() {
        let manager = TableLockManager::new();
        let holder = SessionLockId(1);
        let other = SessionLockId(2);
        assert!(
            manager
                .acquire(7, holder, TableLockMode::AccessExclusive)
                .is_ok()
        );
        assert!(
            manager.acquire(7, other, TableLockMode::AccessShare)
                == Err(TableLockError::NotAvailable)
        );
        // A different relation is untouched.
        assert!(
            manager
                .acquire(8, other, TableLockMode::AccessShare)
                .is_ok()
        );
        manager.release_all(holder);
        assert!(
            manager
                .acquire(7, other, TableLockMode::AccessShare)
                .is_ok()
        );
    }

    #[test]
    fn advisory_locks_count_shared_and_scope_exactly_like_postgres() {
        let manager = AdvisoryLockManager::new();
        let me = SessionLockId(1);
        let other = SessionLockId(2);
        assert!(manager.try_lock(1, me, false, AdvisoryScope::Session));
        assert!(manager.try_lock(1, me, false, AdvisoryScope::Session));
        assert!(manager.hold_count(1, me) == 2);
        assert!(!manager.try_lock(1, other, false, AdvisoryScope::Session));
        assert!(!manager.try_lock(1, other, true, AdvisoryScope::Session));
        assert!(manager.unlock(1, me, false));
        assert!(manager.unlock(1, me, false));
        assert!(!manager.unlock(1, me, false));
        // Shared holds coexist across sessions but block an exclusive request.
        assert!(manager.try_lock(2, me, true, AdvisoryScope::Session));
        assert!(manager.try_lock(2, other, true, AdvisoryScope::Session));
        assert!(!manager.try_lock(2, SessionLockId(3), false, AdvisoryScope::Session));
        // A transaction-scoped hold is not released by `pg_advisory_unlock`.
        assert!(manager.try_lock(3, me, false, AdvisoryScope::Transaction));
        assert!(!manager.unlock(3, me, false));
        manager.release_transaction(me);
        assert!(manager.hold_count(3, me) == 0);
        manager.unlock_all(me);
        assert!(manager.hold_count(2, me) == 0);
    }

    #[test]
    fn the_two_int4_advisory_key_packs_the_way_postgres_packs_it() {
        assert!(AdvisoryLockManager::pack_key(0, 1) == 1);
        assert!(AdvisoryLockManager::pack_key(1, 0) == 1_i64 << 32);
        assert!(AdvisoryLockManager::pack_key(-1, -1) == -1);
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
            Acquire::Conflict(LockOwner::Xid(10))
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
            Acquire::Conflict(LockOwner::Xid(10))
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
            Acquire::Conflict(LockOwner::Xid(10))
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
