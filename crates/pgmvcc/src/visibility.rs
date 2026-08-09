//! Snapshot-based visibility, Postgres' `HeapTupleSatisfiesMVCC`.
//!
//! A snapshot is `(xmin, xmax, xip[])`. `xmax` is one past the highest
//! assigned xid. `xip` is the set of xids that were running when the snapshot
//! was taken. `xmin` is the lowest of those, a fast "everything below is
//! settled" bound. The clog answers "did this xid commit?". The snapshot
//! answers "before I started?".

use crabka_pgkv::KvError;

use crate::{
    clog::XidStatus,
    version::TsVersionState,
    xid::{FROZEN_XID, INVALID_XID},
};

/// A read snapshot: the running-transaction set as of a point in time.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub xmin: u64,
    pub xmax: u64,
    pub xip: Vec<u64>, // sorted ascending
}

impl Snapshot {
    /// Was `xid` running at the moment this snapshot was taken, or did it
    /// start after that moment?
    fn is_running(&self, xid: u64) -> bool {
        // NOTE: PostgreSQL also treats `xid < self.xmin` as a fast "settled" case.
        // We omit that fast path: such xids fall through to the clog, which gives
        // the identical committed/aborted answer (xmin is the lowest running xid,
        // so everything below it is already settled). Pure optimization, not
        // correctness — safe to add later if clog lookups become hot.
        xid >= self.xmax || self.xip.binary_search(&xid).is_ok()
    }
}

/// Is a transaction's effect visible to this snapshot? True iff it is the
/// caller's own transaction, or it had committed before the snapshot was taken.
fn committed_visible(
    xid: u64,
    snapshot: &Snapshot,
    own: Option<u64>,
    status: &impl Fn(u64) -> Result<XidStatus, KvError>,
) -> Result<bool, KvError> {
    if xid == FROZEN_XID {
        return Ok(true);
    }
    if Some(xid) == own {
        return Ok(true); // my own write (read-your-writes)
    }
    if snapshot.is_running(xid) {
        return Ok(false); // running at, or started after, my snapshot
    }
    Ok(matches!(status(xid)?, XidStatus::Committed)) // settled: ask the clog
}

/// Postgres `HeapTupleSatisfiesMVCC` for a tuple with header `(xmin, xmax)`.
///
/// The tuple is visible iff its creator is visible to the snapshot AND no
/// transaction that is also visible to the snapshot has deleted or superseded
/// it.
///
/// # Errors
///
/// Returns [`KvError`] when commit-status lookup for either transaction fails.
pub fn satisfies_mvcc(
    xmin: u64,
    xmax: u64,
    snapshot: &Snapshot,
    own: Option<u64>,
    status: impl Fn(u64) -> Result<XidStatus, KvError>,
) -> Result<bool, KvError> {
    debug_assert!(
        snapshot.xip.windows(2).all(|w| w[0] <= w[1]),
        "Snapshot.xip must be sorted ascending for binary_search visibility"
    );
    if !committed_visible(xmin, snapshot, own, &status)? {
        return Ok(false);
    }
    if xmax == INVALID_XID {
        return Ok(true);
    }
    Ok(!committed_visible(xmax, snapshot, own, &status)?)
}

/// Timestamp visibility for G-9 sharded-table versions.
///
/// A committed version is visible iff `commit_ts <= read_ts`. Pending intents
/// and aborted versions are excluded. They stay excluded unless the caller
/// resolves the intent through the primary and rewrites it as committed.
#[must_use]
pub const fn satisfies_ts(read_ts: u64, state: TsVersionState) -> bool {
    match state {
        TsVersionState::Committed { commit_ts } => commit_ts <= read_ts,
        TsVersionState::Intent | TsVersionState::Aborted | TsVersionState::Deleted { .. } => false,
    }
}

/// The verdict of a timestamp read against one version under a clock-skew bound.
///
/// A two-valued visible/invisible check is enough when there is a single
/// timestamp authority. Under a distributed clock, a version committed just
/// above the read timestamp may still have happened before the read in real
/// time, within the skew bound. Such a version is neither safely visible nor
/// safely invisible. It is [`Uncertain`], and the read path restarts above
/// it.
///
/// [`Uncertain`]: ReadVerdict::Uncertain
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadVerdict {
    /// The version is committed at or below the read timestamp.
    Visible,
    /// The version is invisible. It is an intent, an abort, a delete marker,
    /// or a commit that is genuinely concurrent with the read, that is beyond
    /// the uncertainty window above the read.
    Invisible,
    /// The version committed inside `(read_ts, read_ts + uncertainty]`: it may
    /// have preceded the read in real time, so the reader must restart above it.
    Uncertain,
}

/// Timestamp visibility with a clock-skew uncertainty window.
///
/// This is the tri-state that the distributed (HLC) read path uses to decide
/// between a visible version, an invisible version, and a read restart.
///
/// A committed version is [`Visible`] when `commit_ts <= read_ts`, [`Uncertain`]
/// when `read_ts < commit_ts <= read_ts + uncertainty`, and [`Invisible`]
/// beyond that. Intents, aborts, and delete markers follow [`satisfies_ts`] and
/// are always [`Invisible`].
///
/// With `uncertainty == 0` the window is empty and the result matches
/// [`satisfies_ts`] exactly: [`Visible`] where it returns `true`, [`Invisible`]
/// where it returns `false`, and never [`Uncertain`], so every centralized
/// (`LogicalTso`) caller sees the same two-valued behavior.
///
/// [`Visible`]: ReadVerdict::Visible
/// [`Invisible`]: ReadVerdict::Invisible
/// [`Uncertain`]: ReadVerdict::Uncertain
#[must_use]
pub const fn read_verdict(read_ts: u64, state: TsVersionState, uncertainty: u64) -> ReadVerdict {
    match state {
        TsVersionState::Committed { commit_ts } => {
            if commit_ts <= read_ts {
                ReadVerdict::Visible
            } else if commit_ts <= read_ts.saturating_add(uncertainty) {
                ReadVerdict::Uncertain
            } else {
                ReadVerdict::Invisible
            }
        }
        TsVersionState::Intent | TsVersionState::Aborted | TsVersionState::Deleted { .. } => {
            ReadVerdict::Invisible
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::{
        clog::XidStatus,
        xid::{FIRST_NORMAL_XID, FROZEN_XID, INVALID_XID},
    };

    // A clog stub: maps xid -> status via a small closure.
    fn status_map<'a>(
        committed: &'a [u64],
        aborted: &'a [u64],
    ) -> impl Fn(u64) -> Result<XidStatus, crabka_pgkv::KvError> + 'a {
        move |x| {
            if committed.contains(&x) {
                Ok(XidStatus::Committed)
            } else if aborted.contains(&x) {
                Ok(XidStatus::Aborted)
            } else {
                Ok(XidStatus::InProgress)
            }
        }
    }

    fn snap(xmax: u64, xip: &[u64]) -> Snapshot {
        let mut xip = xip.to_vec();
        xip.sort_unstable();
        let xmin = xip.first().copied().unwrap_or(xmax);
        Snapshot { xmin, xmax, xip }
    }

    #[test]
    fn committed_before_snapshot_is_visible() {
        let s = snap(10, &[]);
        assert!(satisfies_mvcc(5, 0, &s, None, status_map(&[5], &[])).expect("ok"));
    }

    #[test]
    fn running_at_snapshot_is_invisible() {
        let s = snap(10, &[5]);
        assert!(!satisfies_mvcc(5, 0, &s, None, status_map(&[5], &[])).expect("ok"));
    }

    #[test]
    fn started_after_snapshot_is_invisible() {
        let s = snap(10, &[]);
        assert!(!satisfies_mvcc(12, 0, &s, None, status_map(&[12], &[])).expect("ok"));
    }

    #[test]
    fn aborted_xmin_is_invisible() {
        let s = snap(10, &[]);
        assert!(!satisfies_mvcc(5, 0, &s, None, status_map(&[], &[5])).expect("ok"));
    }

    #[test]
    fn own_xid_is_visible_read_your_writes() {
        let s = snap(7, &[]);
        assert!(satisfies_mvcc(7, 0, &s, Some(7), status_map(&[], &[])).expect("ok"));
    }

    #[test]
    fn committed_visible_delete_hides_row() {
        let s = snap(10, &[]);
        assert!(!satisfies_mvcc(5, 6, &s, None, status_map(&[5, 6], &[])).expect("ok"));
    }

    #[test]
    fn aborted_or_running_delete_does_not_hide_row() {
        let s = snap(10, &[6]); // xmax=6 still running at my snapshot
        assert!(satisfies_mvcc(5, 6, &s, None, status_map(&[5], &[])).expect("ok"));
        let s2 = snap(10, &[]);
        assert!(satisfies_mvcc(5, 6, &s2, None, status_map(&[5], &[6])).expect("ok"));
    }

    #[test]
    fn own_delete_hides_row_from_me() {
        let s = snap(7, &[]);
        assert!(!satisfies_mvcc(7, 7, &s, Some(7), status_map(&[], &[])).expect("ok"));
    }

    #[test]
    fn sorted_multi_element_xip_resolves_correctly() {
        // Snapshot xmax=20, running={5,9,14}: committed row xmin=3 (below xmin=5,
        // settled) should be visible; xmin=9 (in xip, still running) should not.
        let s = snap(20, &[5, 9, 14]);
        assert_eq!(s.xip, vec![5, 9, 14]); // verify snap() sorted them
        assert!(satisfies_mvcc(3, 0, &s, None, status_map(&[3], &[])).expect("ok"));
        assert!(!satisfies_mvcc(9, 0, &s, None, status_map(&[9], &[])).expect("ok"));
    }

    #[test]
    fn frozen_xmin_is_visible_to_old_and_new_snapshots_without_clog_lookup() {
        let snapshots = [snap(FROZEN_XID, &[]), snap(10_000, &[42, 99])];

        for snapshot in snapshots {
            assert!(
                satisfies_mvcc(FROZEN_XID, INVALID_XID, &snapshot, None, |_| {
                    panic!("frozen xmin must not consult the clog")
                })
                .expect("frozen visibility")
            );
        }
    }

    #[test]
    fn frozen_xmin_still_honors_a_visible_delete() {
        let s = snap(10, &[]);
        assert!(
            !satisfies_mvcc(FROZEN_XID, 7, &s, None, status_map(&[7], &[]))
                .expect("visible delete hides frozen row")
        );
    }

    #[test]
    fn timestamp_visibility_excludes_intents_and_aborts() {
        assert!(satisfies_ts(
            10,
            TsVersionState::Committed { commit_ts: 10 }
        ));
        assert!(!satisfies_ts(
            9,
            TsVersionState::Committed { commit_ts: 10 }
        ));
        assert!(!satisfies_ts(100, TsVersionState::Intent));
        assert!(!satisfies_ts(100, TsVersionState::Aborted));
    }

    #[test]
    fn read_verdict_covers_the_boundary_and_window_edges() {
        use assert2::assert;

        let committed = |commit_ts| TsVersionState::Committed { commit_ts };
        // (read_ts, state, uncertainty, expected verdict)
        let cases = [
            // commit_ts == read_ts is visible regardless of the window.
            (10, committed(10), 0, ReadVerdict::Visible),
            (10, committed(10), 5, ReadVerdict::Visible),
            // commit_ts one past the read is invisible with an empty window...
            (10, committed(11), 0, ReadVerdict::Invisible),
            // ...and uncertain once the window admits it.
            (10, committed(11), 1, ReadVerdict::Uncertain),
            (10, committed(11), 5, ReadVerdict::Uncertain),
            // The far edge of the window is still uncertain.
            (10, committed(15), 5, ReadVerdict::Uncertain),
            // Just past the window is genuinely concurrent, hence invisible.
            (10, committed(16), 5, ReadVerdict::Invisible),
            // saturating_add keeps a huge window from overflowing.
            (u64::MAX - 1, committed(u64::MAX), 5, ReadVerdict::Uncertain),
            // Non-committed states never depend on the window.
            (10, TsVersionState::Intent, 5, ReadVerdict::Invisible),
            (10, TsVersionState::Aborted, 5, ReadVerdict::Invisible),
            (
                10,
                TsVersionState::Deleted { commit_ts: 8 },
                5,
                ReadVerdict::Invisible,
            ),
        ];
        for (read_ts, state, uncertainty, expected) in cases {
            assert!(read_verdict(read_ts, state, uncertainty) == expected);
        }
    }

    proptest! {
        #[test]
        fn read_verdict_at_zero_uncertainty_matches_satisfies_ts(
            read_ts in 0_u64..1_000,
            commit_ts in 0_u64..1_000,
            variant in 0_u8..4,
        ) {
            let state = match variant {
                0 => TsVersionState::Committed { commit_ts },
                1 => TsVersionState::Deleted { commit_ts },
                2 => TsVersionState::Intent,
                _ => TsVersionState::Aborted,
            };
            let expected = if satisfies_ts(read_ts, state) {
                ReadVerdict::Visible
            } else {
                ReadVerdict::Invisible
            };
            prop_assert_eq!(read_verdict(read_ts, state, 0), expected);
        }
    }

    proptest! {
        #[test]
        fn non_frozen_committed_and_aborted_xmin_behavior_is_unchanged(
            xid in FIRST_NORMAL_XID..1_000_u64,
            xmax_offset in 1_u64..1_000,
        ) {
            let snapshot = snap(xid.saturating_add(xmax_offset), &[]);

            prop_assert!(satisfies_mvcc(
                xid,
                INVALID_XID,
                &snapshot,
                None,
                status_map(&[xid], &[]),
            ).expect("committed xmin is visible"));

            prop_assert!(!satisfies_mvcc(
                xid,
                INVALID_XID,
                &snapshot,
                None,
                status_map(&[], &[xid]),
            ).expect("aborted xmin is invisible"));
        }

        #[test]
        fn non_frozen_xmax_behavior_is_unchanged(
            xmin in FIRST_NORMAL_XID..1_000_u64,
            xmax in 1_000_u64..2_000,
        ) {
            let snapshot = snap(xmax + 1, &[]);

            prop_assert!(!satisfies_mvcc(
                xmin,
                xmax,
                &snapshot,
                None,
                status_map(&[xmin, xmax], &[]),
            ).expect("committed delete hides row"));

            prop_assert!(satisfies_mvcc(
                xmin,
                xmax,
                &snapshot,
                None,
                status_map(&[xmin], &[xmax]),
            ).expect("aborted delete keeps row visible"));
        }
    }
}
