//! Garbage-collection horizon support: snapshot pins and the dead-version rule.
//!
//! The `ProcArray` only registers WRITER xids, so a read-only snapshot, that
//! is a REPEATABLE READ transaction or a single statement's READ COMMITTED
//! snapshot, would be invisible to the garbage horizon. A version
//! whose committed `xmax` was still running when such a snapshot was taken
//! could be pruned out from under it mid-use. [`GcHorizon`] closes that gap.
//! Every snapshot consumer registers a [`SnapshotPin`] at its snapshot's
//! `xmin` for exactly as long as the snapshot is in use, and the horizon
//! computation caps itself at the minimum registered pin. Any xid a live
//! snapshot could still consider "running" is `>=` that snapshot's `xmin`
//! `>=` its pin, so no version such a snapshot can see is ever below the
//! horizon.
//!
//! [`GcHorizon`] also caches a monotone `decided_floor`. This is the highest
//! xid below which every transaction is known decided, that is terminal in
//! the clog, or absent and not running, which means crashed. Horizon
//! computations scan the clog only from this floor. Once the floor catches
//! up, the per-statement cost is amortized O(1) per xid instead of O(all xids
//! ever).

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crabka_pgkv::KvError;
use thiserror::Error;

use crate::{
    clog::XidStatus,
    version::TsVersionState,
    xid::{FROZEN_XID, INVALID_XID, Xid},
};

/// A snapshot pin was requested below the horizon's reclaim floor: history at
/// or below the floor may already be physically reclaimed, so the snapshot
/// cannot be served. Callers retry with a freshly allocated timestamp.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("snapshot {requested} is below the reclaim floor {floor}")]
pub struct PinBelowReclaimFloor {
    /// The floor the pin failed against.
    pub floor: u64,
    /// The requested snapshot value.
    pub requested: u64,
}

/// Shared garbage-horizon state: registered snapshot pins plus the cached
/// decided floor. One instance per engine, shared by every session.
#[derive(Debug, Default)]
pub struct GcHorizon {
    /// Multiset of pinned snapshot `xmin`s (value = number of holders).
    pins: Mutex<BTreeMap<Xid, usize>>,
    /// Every xid strictly below this value is decided. It has a terminal clog
    /// entry, or it is absent while not registered as running, which is a
    /// crash leftover that can never commit. The value is monotone. It is
    /// purely an in-process scan-cost cache, and never a correctness input on
    /// its own.
    decided_floor: AtomicU64,
    /// Monotone reclaim floor. History below it may be physically reclaimed,
    /// so [`GcHorizon::pin_above`] refuses pins under it. It rises only while
    /// a caller holds the pin registry lock (`raise_reclaim_floor`), or when a
    /// caller folds in an already-published durable value
    /// (`observe_reclaim_floor`). A pin admitted at value `v` guarantees that
    /// no later raise passes `v`.
    reclaim_floor: AtomicU64,
}

impl GcHorizon {
    /// Empty state: no pins, floor at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a snapshot whose `xmin` must hold the garbage horizon back
    /// until the caller drops the returned pin.
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

    /// Raises the cached decided floor. The floor is monotone, and lower
    /// values are ignored.
    pub fn advance_decided_floor(&self, floor: Xid) {
        self.decided_floor.fetch_max(floor, Ordering::SeqCst);
    }

    /// Registers a snapshot pin at `value` and refuses values below the
    /// reclaim floor. The floor check and the pin insertion happen under one
    /// lock, so a concurrent [`GcHorizon::raise_reclaim_floor`] can never
    /// overtake an admitted pin. The raise either sees the pin and bounds
    /// itself below it, or it happened first and this pin is refused.
    ///
    /// # Errors
    ///
    /// Returns [`PinBelowReclaimFloor`] when `value` is strictly below the
    /// current reclaim floor.
    ///
    /// # Panics
    ///
    /// Panics if the internal pin registry mutex is poisoned.
    pub fn pin_above(self: &Arc<Self>, value: Xid) -> Result<SnapshotPin, PinBelowReclaimFloor> {
        let mut pins = self.pins.lock().expect("gc horizon pins");
        let floor = self.reclaim_floor.load(Ordering::SeqCst);
        if value < floor {
            return Err(PinBelowReclaimFloor {
                floor,
                requested: value,
            });
        }
        *pins.entry(value).or_insert(0) += 1;
        drop(pins);
        Ok(SnapshotPin {
            horizon: Arc::clone(self),
            xmin: value,
        })
    }

    /// Raises the reclaim floor toward `candidate`, bounded by the lowest
    /// registered pin, and returns the resulting floor. The floor is monotone.
    /// A candidate below the current floor, or a pin below it, leaves the
    /// floor unchanged. A reclaim of history strictly below the returned value
    /// is safe with respect to every pin registered on this horizon.
    ///
    /// # Panics
    ///
    /// Panics if the internal pin registry mutex is poisoned.
    pub fn raise_reclaim_floor(&self, candidate: Xid) -> Xid {
        let pins = self.pins.lock().expect("gc horizon pins");
        let bounded = pins
            .keys()
            .next()
            .map_or(candidate, |&min| candidate.min(min));
        let previous = self.reclaim_floor.fetch_max(bounded, Ordering::SeqCst);
        drop(pins);
        previous.max(bounded)
    }

    /// Folds an externally published, durable, reclaim floor into this
    /// horizon. The floor is monotone, and lower values are ignored.
    ///
    /// Unlike [`GcHorizon::raise_reclaim_floor`], this method does NOT consult
    /// pins. The value was already the published floor when the caller read
    /// it, so pins below it can only belong to snapshots admitted against an
    /// older applied state whose history is still intact.
    pub fn observe_reclaim_floor(&self, floor: Xid) {
        self.reclaim_floor.fetch_max(floor, Ordering::SeqCst);
    }

    /// The current reclaim floor (see [`GcHorizon::raise_reclaim_floor`]).
    #[must_use]
    pub fn reclaim_floor(&self) -> Xid {
        self.reclaim_floor.load(Ordering::SeqCst)
    }
}

/// Indices of timestamp-stamped versions that are dead below `floor`. No read
/// at or above `floor` can see them, because a NEWER committed or deleted
/// version also sits at or below `floor` and supersedes them.
///
/// Let `c*` be the highest `commit_ts` among `Committed`/`Deleted` versions
/// with `commit_ts <= floor`. Every `Committed`/`Deleted` version with
/// `commit_ts < c*` is dead: a read at `read_ts >= floor >= c*` resolves to
/// the newest version at or below its `read_ts`, which is the `c*` version or
/// newer. The function always keeps the `c*` version itself, because it IS the
/// visible version for reads in `[c*, next commit)`. It also keeps everything
/// above `floor`. `Intent` versions are never dead, because their outcome is
/// undecided. The function keeps `Aborted` markers so late idempotent
/// resolutions still find them.
///
/// Reads strictly below `floor` may miss pruned history. Callers must make
/// sure that such reads are refused, which is the reclaim-floor contract of
/// [`GcHorizon::pin_above`]. Uncertainty-window semantics do not change.
/// [`crate::visibility::read_verdict`] restarts only on commits ABOVE the
/// read timestamp, and every version above `floor` survives.
///
/// Returns at most `cap` indices, oldest commit first, so one caller's
/// reclamation work is bounded regardless of accumulated chain length.
#[must_use]
pub fn ts_dead_version_indices(versions: &[TsVersionState], floor: u64, cap: usize) -> Vec<usize> {
    let newest_covered = versions
        .iter()
        .filter_map(|state| match *state {
            TsVersionState::Committed { commit_ts } | TsVersionState::Deleted { commit_ts }
                if commit_ts <= floor =>
            {
                Some(commit_ts)
            }
            _ => None,
        })
        .max();
    let Some(newest_covered) = newest_covered else {
        return Vec::new();
    };
    let mut dead: Vec<(u64, usize)> = versions
        .iter()
        .enumerate()
        .filter_map(|(index, state)| match *state {
            TsVersionState::Committed { commit_ts } | TsVersionState::Deleted { commit_ts }
                if commit_ts < newest_covered =>
            {
                Some((commit_ts, index))
            }
            _ => None,
        })
        .collect();
    dead.sort_unstable();
    dead.truncate(cap);
    dead.into_iter().map(|(_, index)| index).collect()
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

/// Is a tuple version `(xmin, xmax)` dead at `horizon`, that is invisible to
/// every current AND future registered snapshot?
///
/// `horizon` must satisfy the garbage-horizon contract: every xid strictly
/// below it is decided, and it is no higher than any live registered
/// snapshot's `xmin`. The `ProcArray` supplies the writer xids, and
/// [`GcHorizon`] pins supply the reader snapshots.
///
/// A version is dead iff:
///
/// - (a) its creator aborted (`clog(xmin) == Aborted`), or its creator sits
///   below the horizon with an in-progress or absent clog entry. Such a
///   creator is a crashed transaction that can never commit, because any xid
///   below the horizon is not running, so an absent entry is a crash
///   leftover. Either way, the version was never visible to any snapshot
///   other than its own transaction, which has now terminated; or
/// - (b) a transaction that committed below the horizon deleted or superseded
///   it (`xmax != INVALID && xmax < horizon && clog(xmax) == Committed`). The
///   deletion is visible to every snapshot at or above the horizon, so the
///   version itself is visible to none of them.
///
/// The newest committed version of a live row is never dead. Its `xmax` is
/// invalid, in-progress, or aborted, so (b) cannot hold, and its `xmin`
/// committed, so (a) cannot hold. `FROZEN_XID` creators also read as absent
/// from the clog, but they are the always-visible sentinel and are explicitly
/// exempt from the crashed-creator arm of (a).
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

    #[test]
    fn reclaim_floor_is_bounded_by_pins_and_released_by_drop() {
        let horizon = Arc::new(GcHorizon::new());
        let pin = horizon.pin_above(5).expect("no floor yet");

        // A raise past the pin is capped at the pin's value.
        assert!(horizon.raise_reclaim_floor(9) == 5);
        assert!(horizon.reclaim_floor() == 5);

        // Dropping the pin lets the next raise pass it.
        drop(pin);
        assert!(horizon.raise_reclaim_floor(9) == 9);
        assert!(horizon.reclaim_floor() == 9);
    }

    #[test]
    fn pin_above_refuses_values_below_the_floor_and_admits_at_it() {
        let horizon = Arc::new(GcHorizon::new());
        assert!(horizon.raise_reclaim_floor(7) == 7);

        assert!(
            horizon
                .pin_above(6)
                .expect_err("a pin strictly below the floor is refused")
                == PinBelowReclaimFloor {
                    floor: 7,
                    requested: 6
                }
        );
        let at = horizon
            .pin_above(7)
            .expect("a pin AT the floor is admitted");
        drop(at);
    }

    #[test]
    fn raise_reclaim_floor_is_monotone() {
        let horizon = GcHorizon::new();
        assert!(horizon.raise_reclaim_floor(10) == 10);
        assert!(horizon.raise_reclaim_floor(4) == 10);
        assert!(horizon.reclaim_floor() == 10);
    }

    #[test]
    fn observe_reclaim_floor_ignores_pins_and_lower_values() {
        let horizon = Arc::new(GcHorizon::new());
        let _pin = horizon.pin_above(3).expect("no floor yet");
        horizon.observe_reclaim_floor(8);
        assert!(horizon.reclaim_floor() == 8);
        horizon.observe_reclaim_floor(2);
        assert!(horizon.reclaim_floor() == 8);
    }

    #[test]
    fn ts_dead_version_indices_covers_the_boundary_cases() {
        struct Case {
            name: &'static str,
            versions: Vec<TsVersionState>,
            floor: u64,
            cap: usize,
            expected: Vec<usize>,
        }
        let committed = |commit_ts| TsVersionState::Committed { commit_ts };
        let deleted = |commit_ts| TsVersionState::Deleted { commit_ts };
        let cases = [
            Case {
                name: "superseded below floor is dead, newest covered survives",
                versions: vec![committed(5), committed(8), committed(20)],
                floor: 10,
                cap: 16,
                expected: vec![0],
            },
            Case {
                name: "a commit exactly at the floor covers older versions",
                versions: vec![committed(5), committed(10)],
                floor: 10,
                cap: 16,
                expected: vec![0],
            },
            Case {
                name: "no covered version means nothing is dead",
                versions: vec![committed(11), committed(12)],
                floor: 10,
                cap: 16,
                expected: vec![],
            },
            Case {
                name: "the single covered version is never dead",
                versions: vec![committed(5)],
                floor: 10,
                cap: 16,
                expected: vec![],
            },
            Case {
                name: "a delete tombstone covers, older versions die, tombstone kept",
                versions: vec![committed(3), committed(5), deleted(8)],
                floor: 10,
                cap: 16,
                expected: vec![0, 1],
            },
            Case {
                name: "intents and aborted markers are never dead and never cover",
                versions: vec![
                    committed(3),
                    TsVersionState::Aborted,
                    TsVersionState::Intent,
                    committed(9),
                ],
                floor: 10,
                cap: 16,
                expected: vec![0],
            },
            Case {
                name: "cap bounds the work and keeps the oldest commits first",
                versions: vec![committed(4), committed(2), committed(6), committed(9)],
                floor: 10,
                cap: 2,
                expected: vec![1, 0],
            },
            Case {
                name: "versions above the floor never die even when superseded",
                versions: vec![committed(15), committed(20)],
                floor: 10,
                cap: 16,
                expected: vec![],
            },
        ];
        for case in cases {
            assert!(
                ts_dead_version_indices(&case.versions, case.floor, case.cap) == case.expected,
                "case: {}",
                case.name
            );
        }
    }
}
