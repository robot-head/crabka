//! Garbage-collection horizon support: snapshot pins and the dead-version rule.
//!
//! The `ProcArray` only registers WRITER xids, so a read-only snapshot (a
//! REPEATABLE READ transaction, or a single statement's READ COMMITTED
//! snapshot) would otherwise be invisible to the garbage horizon: a version
//! whose committed `xmax` was still running when such a snapshot was taken
//! could be pruned out from under it mid-use. [`GcHorizon`] closes that gap:
//! every snapshot consumer registers a [`SnapshotPin`] at its snapshot's
//! `xmin` for exactly as long as the snapshot is in use, and the horizon
//! computation caps itself at the minimum registered pin. Any xid a live
//! snapshot could still consider "running" is `>=` that snapshot's `xmin`
//! `>=` its pin, so no version such a snapshot can see is ever below the
//! horizon.
//!
//! [`GcHorizon`] also caches a monotone `decided_floor`: the highest xid
//! below which every transaction is known decided (terminal in the clog, or
//! absent-and-not-running, i.e. crashed). Horizon computations scan the clog
//! only from this floor, so the per-statement cost is amortized O(1) per xid
//! instead of O(all xids ever) once the floor catches up.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crabka_pgkv::KvError;

use crate::{
    clog::XidStatus,
    xid::{FROZEN_XID, INVALID_XID, Xid},
};

/// Shared garbage-horizon state: registered snapshot pins plus the cached
/// decided floor. One instance per engine, shared by every session.
#[derive(Debug, Default)]
pub struct GcHorizon {
    /// Multiset of pinned snapshot `xmin`s (value = number of holders).
    pins: Mutex<BTreeMap<Xid, usize>>,
    /// Every xid strictly below this value is decided (terminal clog entry,
    /// or absent while not registered as running — a crash leftover that can
    /// never commit). Monotone; purely an in-process scan-cost cache, never a
    /// correctness input on its own.
    decided_floor: AtomicU64,
}

impl GcHorizon {
    /// Empty state: no pins, floor at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a snapshot whose `xmin` must hold the garbage horizon back
    /// until the returned pin is dropped.
    ///
    /// # Panics
    ///
    /// Panics if the internal pin registry mutex is poisoned.
    #[must_use]
    pub fn pin(self: &Arc<Self>, xmin: Xid) -> SnapshotPin {
        let mut pins = self.pins.lock().expect("gc horizon pins");
        *pins.entry(xmin).or_insert(0) += 1;
        drop(pins);
        SnapshotPin {
            horizon: Arc::clone(self),
            xmin,
        }
    }

    /// The lowest currently-pinned snapshot `xmin`, if any snapshot is pinned.
    ///
    /// # Panics
    ///
    /// Panics if the internal pin registry mutex is poisoned.
    #[must_use]
    pub fn min_pinned(&self) -> Option<Xid> {
        self.pins
            .lock()
            .expect("gc horizon pins")
            .keys()
            .next()
            .copied()
    }

    /// The cached decided floor (see the type docs).
    #[must_use]
    pub fn decided_floor(&self) -> Xid {
        self.decided_floor.load(Ordering::SeqCst)
    }

    /// Raise the cached decided floor (monotone; lower values are ignored).
    pub fn advance_decided_floor(&self, floor: Xid) {
        self.decided_floor.fetch_max(floor, Ordering::SeqCst);
    }
}

/// RAII registration of one snapshot's `xmin` in a [`GcHorizon`]. While held,
/// `checkpoint_garbage_horizon`-style computations never return a horizon
/// above this `xmin`, so no version visible to the pinned snapshot is pruned.
#[derive(Debug)]
pub struct SnapshotPin {
    horizon: Arc<GcHorizon>,
    xmin: Xid,
}

impl SnapshotPin {
    /// The pinned snapshot `xmin`.
    #[must_use]
    pub fn xmin(&self) -> Xid {
        self.xmin
    }
}

impl Drop for SnapshotPin {
    fn drop(&mut self) {
        let mut pins = self.horizon.pins.lock().expect("gc horizon pins");
        if let Some(count) = pins.get_mut(&self.xmin) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&self.xmin);
            }
        }
    }
}

/// Is a tuple version `(xmin, xmax)` dead — invisible to every current AND
/// future registered snapshot — at `horizon`?
///
/// `horizon` must satisfy the garbage-horizon contract: every xid strictly
/// below it is decided, and it is no higher than any live registered
/// snapshot's `xmin` (writer xids via the `ProcArray`, reader snapshots via
/// [`GcHorizon`] pins).
///
/// A version is dead iff:
///
/// - (a) its creator aborted (`clog(xmin) == Aborted`), or its creator sits
///   below the horizon with an in-progress/absent clog entry — a crashed
///   transaction that can never commit (any xid below the horizon is not
///   running, so an absent entry is a crash leftover). Either way the version
///   was never visible to any snapshot other than its own — now terminated —
///   transaction; or
/// - (b) it was deleted/superseded by a transaction that committed below the
///   horizon (`xmax != INVALID && xmax < horizon && clog(xmax) == Committed`):
///   the deletion is visible to every snapshot at or above the horizon, so
///   the version itself is visible to none of them.
///
/// The newest committed version of a live row is never dead: its `xmax` is
/// invalid, in-progress, or aborted, so (b) cannot hold, and its `xmin`
/// committed, so (a) cannot hold. `FROZEN_XID` creators also read as absent
/// from the clog but are the always-visible sentinel, explicitly exempt from
/// the crashed-creator arm of (a).
///
/// # Errors
///
/// Returns [`KvError`] when a commit-status lookup fails.
pub fn version_is_dead(
    xmin: Xid,
    xmax: Xid,
    horizon: Xid,
    status: &impl Fn(Xid) -> Result<XidStatus, KvError>,
) -> Result<bool, KvError> {
    match status(xmin)? {
        XidStatus::Aborted => return Ok(true),
        XidStatus::InProgress if xmin < horizon && xmin != FROZEN_XID => return Ok(true),
        XidStatus::InProgress | XidStatus::Committed | XidStatus::Prepared(_) => {}
    }
    Ok(xmax != INVALID_XID && xmax < horizon && matches!(status(xmax)?, XidStatus::Committed))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::xid::FROZEN_XID;

    fn status_map<'a>(
        committed: &'a [Xid],
        aborted: &'a [Xid],
    ) -> impl Fn(Xid) -> Result<XidStatus, KvError> + 'a {
        move |xid| {
            if committed.contains(&xid) {
                Ok(XidStatus::Committed)
            } else if aborted.contains(&xid) {
                Ok(XidStatus::Aborted)
            } else {
                Ok(XidStatus::InProgress)
            }
        }
    }

    #[test]
    fn pin_caps_min_and_releases_on_drop() {
        let horizon = Arc::new(GcHorizon::new());
        assert!(horizon.min_pinned() == None);

        let low = horizon.pin(5);
        let high = horizon.pin(9);
        assert!(horizon.min_pinned() == Some(5));

        drop(low);
        assert!(horizon.min_pinned() == Some(9));
        drop(high);
        assert!(horizon.min_pinned() == None);
    }

    #[test]
    fn duplicate_pins_are_refcounted() {
        let horizon = Arc::new(GcHorizon::new());
        let first = horizon.pin(7);
        let second = horizon.pin(7);
        drop(first);
        assert!(horizon.min_pinned() == Some(7));
        drop(second);
        assert!(horizon.min_pinned() == None);
    }

    #[test]
    fn decided_floor_is_monotone() {
        let horizon = GcHorizon::new();
        assert!(horizon.decided_floor() == 0);
        horizon.advance_decided_floor(10);
        horizon.advance_decided_floor(4);
        assert!(horizon.decided_floor() == 10);
    }

    #[test]
    fn aborted_creator_is_dead_regardless_of_horizon() {
        assert!(version_is_dead(5, INVALID_XID, 0, &status_map(&[], &[5])).expect("status"));
    }

    #[test]
    fn committed_xmax_below_horizon_is_dead() {
        assert!(version_is_dead(5, 6, 7, &status_map(&[5, 6], &[])).expect("status"));
    }

    #[test]
    fn committed_xmax_at_or_above_horizon_survives() {
        assert!(!version_is_dead(5, 6, 6, &status_map(&[5, 6], &[])).expect("status"));
        assert!(!version_is_dead(5, 6, 5, &status_map(&[5, 6], &[])).expect("status"));
    }

    #[test]
    fn live_or_aborted_deleter_never_kills_a_committed_version() {
        // Live version (xmax invalid).
        assert!(!version_is_dead(5, INVALID_XID, 100, &status_map(&[5], &[])).expect("status"));
        // In-progress deleter.
        assert!(!version_is_dead(5, 6, 100, &status_map(&[5], &[])).expect("status"));
        // Aborted deleter: the delete never happened.
        assert!(!version_is_dead(5, 6, 100, &status_map(&[5], &[6])).expect("status"));
    }

    #[test]
    fn crashed_creator_below_the_horizon_is_dead() {
        // Absent clog entry (InProgress) below the horizon: a crash leftover.
        assert!(version_is_dead(5, INVALID_XID, 6, &status_map(&[], &[])).expect("status"));
        // At or above the horizon it may still be a running transaction.
        assert!(!version_is_dead(5, INVALID_XID, 5, &status_map(&[], &[])).expect("status"));
    }

    #[test]
    fn frozen_creator_is_not_dead_by_the_aborted_rule() {
        assert!(
            !version_is_dead(FROZEN_XID, INVALID_XID, 100, &status_map(&[], &[])).expect("status")
        );
    }
}
