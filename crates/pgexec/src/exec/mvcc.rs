//! MVCC visibility and locked-row recheck helpers.

use super::*;

/// Was `xid` settled (committed or aborted) before `snapshot` was taken? True
/// iff `xid` was neither still running at, nor started after, the snapshot.
/// This mirrors the negation of `Snapshot::is_running`.
pub(super) fn snapshot_can_see(snapshot: &crabka_pgmvcc::visibility::Snapshot, xid: u64) -> bool {
    xid < snapshot.xmax && !snapshot.xip.contains(&xid)
}

/// Does a tuple already visible to MVCC remain visible to the current command?
/// A command cannot scan tuples it inserted itself, and cannot lose tuples it
/// deletes itself, until the next command counter increment.
pub(super) fn command_can_see(
    xmin: u64,
    xmax: u64,
    cmin: u32,
    cmax: u32,
    own: Option<u64>,
    command_id: Option<u32>,
) -> bool {
    let Some(command_id) = command_id else {
        return true;
    };
    let Some(own) = own else {
        return true;
    };
    (xmin != own || cmin < command_id)
        && (xmax != own || xmax == crabka_pgmvcc::xid::INVALID_XID || cmax >= command_id)
}

/// `satisfies_mvcc`, including PostgreSQL's command-counter exception for a
/// tuple this transaction deleted or superseded in the current command.
pub(super) fn satisfies_mvcc_at_command(
    xmin: u64,
    xmax: u64,
    cmin: u32,
    cmax: u32,
    snapshot: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    command_id: Option<u32>,
    status: impl Fn(u64) -> Result<crabka_pgmvcc::clog::XidStatus, crabka_pgkv::KvError>,
) -> Result<bool, crabka_pgkv::KvError> {
    if !command_can_see(xmin, xmax, cmin, cmax, own, command_id) {
        return Ok(false);
    }
    if command_id.is_some() && Some(xmax) == own && xmax != crabka_pgmvcc::xid::INVALID_XID {
        return crabka_pgmvcc::visibility::satisfies_mvcc(
            xmin,
            crabka_pgmvcc::xid::INVALID_XID,
            snapshot,
            own,
            status,
        );
    }
    crabka_pgmvcc::visibility::satisfies_mvcc(xmin, xmax, snapshot, own, status)
}

/// The global-aware clog resolver handed to `satisfies_mvcc`. Given this range's
/// local xid `Li`, reads this range's clog (`local`); a terminal status is
/// returned unchanged (today's single-range behavior). A `Prepared(Li -> g)`
/// marker is deref'd to range 0's global clog (`global`): if `g` is still
/// in-doubt as of the reader's global snapshot (`gsnap`) it reports `InProgress`
/// (the cross-range row is invisible until the global commit decision); once `g`
/// is settled relative to `gsnap`, range 0's global-clog status for `g` is the
/// answer. So both ranges' Prepared rows flip visible together at the single
/// `Committed(g)` instant.
///
/// For a single-range (non-GTM) engine the caller passes `global = local` and
/// `gsnap = NO_GLOBAL_SNAPSHOT()`; no `Prepared` tuple ever exists there, so the
/// `Prepared` arm is unreachable and behavior is byte-for-byte unchanged.
pub(crate) fn global_status<'a>(
    local: &'a dyn crabka_pgkv::Kv,
    global: &'a dyn crabka_pgkv::Kv,
    gsnap: &'a crabka_pgmvcc::visibility::Snapshot,
) -> impl Fn(u64) -> Result<crabka_pgmvcc::clog::XidStatus, crabka_pgkv::KvError> + 'a {
    use crabka_pgmvcc::clog::XidStatus;
    move |xid| match crabka_pgmvcc::clog::get(local, xid)? {
        XidStatus::Prepared(g) => {
            if g >= gsnap.xmax || gsnap.xip.binary_search(&g).is_ok() {
                Ok(XidStatus::InProgress) // global txn in-doubt as of my global snapshot
            } else {
                Ok(crabka_pgmvcc::clog::get(global, g)?) // settled: range 0's global decision
            }
        }
        other => Ok(other),
    }
}

/// Find the single version of `rowid` visible to `snap` (with own-xid
/// read-your-writes) among already-decoded `versions`. Mirrors `scan_live`'s
/// per-version `satisfies_mvcc` check, but over one rowid's versions.
///
/// Returns the greatest-xmin live version. The MVCC at-most-one-live invariant
/// means at most one version of a rowid is live under any one snapshot, so the
/// selection is unambiguous; choosing the max explicitly (rather than relying on
/// ascending scan order) makes it order-independent and is debug-asserted to see
/// at most one live version.
#[cfg(test)]
pub(super) fn find_visible_one(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snap: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    command_id: Option<u32>,
    versions: &[(u64, u64, Vec<crabka_pgtypes::Datum>)],
) -> Result<Option<(u64, Vec<crabka_pgtypes::Datum>)>, ExecError> {
    let versions = versions
        .iter()
        .map(|(xmin, xmax, row)| (*xmin, *xmax, 0, 0, row.clone()))
        .collect::<Vec<_>>();
    find_visible_one_with_command_ids(kv, global, gsnap, snap, own, command_id, &versions)
        .map(|visible| visible.map(|(xmin, _, _, row)| (xmin, row)))
}

pub(super) fn find_visible_one_with_command_ids(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snap: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    command_id: Option<u32>,
    versions: &[(u64, u64, u32, u32, Vec<crabka_pgtypes::Datum>)],
) -> Result<Option<(u64, u32, u32, Vec<crabka_pgtypes::Datum>)>, ExecError> {
    let mut visible = None;
    let mut live_count = 0;
    for (xmin, xmax, cmin, cmax, row) in versions {
        if satisfies_mvcc_at_command(
            *xmin,
            *xmax,
            *cmin,
            *cmax,
            snap,
            own,
            command_id,
            global_status(kv, global, gsnap),
        )? {
            live_count += 1;
            if visible
                .as_ref()
                .is_none_or(|(current, _, _, _)| xmin > current)
            {
                visible = Some((*xmin, *cmin, *cmax, row.clone()));
            }
        }
    }
    debug_assert!(
        live_count <= 1,
        "find_visible_one_with_command_ids: {live_count} live versions for one rowid under one \
         snapshot — MVCC at-most-one-live invariant violated"
    );
    Ok(visible)
}

/// One decoded version of a row chain, keyed by the xid suffix of its
/// PHYSICAL version key. The key xid normally equals the header `xmin`, but a
/// frozen tuple keeps its original key while its header reads `FROZEN_XID`.
/// Writers must stamp `xmax` on the physical key, never one reconstructed
/// from the header.
struct ChainVersion {
    key_xid: u64,
    xmin: u64,
    xmax: u64,
    cmin: u32,
    cmax: u32,
    row: Vec<crabka_pgtypes::Datum>,
    next_rowid: Option<u64>,
}

fn scan_chain_versions(
    kv: &dyn Kv,
    table: &crabka_pgcatalog::Table,
    rowid: u64,
) -> Result<Vec<ChainVersion>, ExecError> {
    let prefix = crabka_pgkv::key::row_key(table.id, rowid);
    kv.scan_prefix(&prefix)?
        .iter()
        .map(|(key, value)| {
            let (xmin, xmax, cmin, cmax, row, next_rowid) =
                crabka_pgmvcc::version::decode_tuple_with_command_ids_and_update_target(value)?;
            Ok(ChainVersion {
                key_xid: crabka_pgmvcc::version::xid_of_key(key)?,
                xmin,
                xmax,
                cmin,
                cmax,
                row,
                next_rowid,
            })
        })
        .collect()
}

/// The version of `rowid` a write should operate on, as
/// `(rowid, key_xid, xmin, row)`.
///
/// After locking the row, re-read its current versions. Returns the version to
/// operate on, or None to skip. Under REPEATABLE READ, a row changed by a txn
/// that committed after our snapshot is a serialization failure (40001). Under
/// READ COMMITTED, re-find the latest live version (a fresh snapshot).
pub(super) fn eval_plan_qual(
    mutation: &MutationContext<'_>,
    table: &crabka_pgcatalog::Table,
    rowid: u64,
    reads: crate::scope::GeneratedReads<'_>,
) -> Result<Option<(u64, u64, u64, u32, u32, Vec<crabka_pgtypes::Datum>)>, ExecError> {
    let kv = mutation.kv;
    let global = mutation.global;
    let procarray = mutation.procarray;
    let snapshot = mutation.snapshot;
    let xid = mutation.xid;
    let repeatable_read = mutation.repeatable_read;
    // Re-scan just this rowid's versions from disk.
    let mut current_rowid = rowid;
    let mut versions = scan_chain_versions(kv, table, current_rowid)?;
    // Resolve this row's `Prepared(Li -> g)` markers against a SETTLED global view
    // — range 0's global clog read directly — NOT the statement's pre-lock global
    // snapshot (`gsnap`). We hold this row's lock, and a cross-range participant
    // releases a row's lock only AFTER its global decision is durable
    // (commit_release/abort_release run post-decision). So every global txn `g`
    // with a `Prepared` marker on THIS row's versions has already settled in
    // range 0's global clog; a still-in-doubt `g` could not have left a marker
    // here (it would still hold this lock, so we could not have acquired it).
    // Reading the global clog directly under the lock is therefore exact — and is
    // the read-committed-under-lock analogue of how the LOCAL clog is read
    // directly. Using `gsnap` would be stale: a `g` that committed while we were
    // blocked on the lock still appears in-doubt in `gsnap.xip`, hiding its just-
    // committed supersede and losing the update across the 2PC boundary. A settled
    // Snapshot (xmin 0, xmax MAX, empty xip) drives `global_status`'s in-doubt gate
    // (`g >= xmax || xip.contains(g)`) always false, so it reads `clog::get` for g.
    // The LOCAL `snapshot`/`fresh` handling below is unchanged — it is about local
    // creation ordering and is already correct.
    let settled_global = crabka_pgmvcc::visibility::Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    };
    // Is the row's latest committed version deleted/superseded by a transaction
    // NOT visible to our txn snapshot (committed AFTER it), other than ourselves?
    // The resolver derefs a Prepared(xmx -> g) deleter to range 0's global
    // decision so a cross-range supersede is detected exactly when it commits.
    let resolve = global_status(kv, global, &settled_global);
    let changed_since_snapshot = versions.iter().any(|version| {
        version.xmax != crabka_pgmvcc::xid::INVALID_XID
            && version.xmax != xid
            && matches!(
                resolve(version.xmax),
                Ok(crabka_pgmvcc::clog::XidStatus::Committed)
            )
            && !snapshot_can_see(snapshot, version.xmax)
    });
    let mut found = if changed_since_snapshot {
        if repeatable_read {
            return Err(ExecError::SerializationFailure);
        }
        // READ COMMITTED: re-find the latest live version under a FRESH snapshot.
        // An UPDATE's successor has a new physical rowid, so follow the recorded
        // link when the superseded row itself is no longer live.
        let fresh = procarray.snapshot();
        let mut seen = std::collections::BTreeSet::from([rowid]);
        loop {
            if let Some((key_xid, xmin, cmin, cmax, row)) = find_visible_one_keyed(
                kv,
                global,
                &settled_global,
                &fresh,
                Some(xid),
                mutation.command_id,
                &versions,
            )? {
                break Some((current_rowid, key_xid, xmin, cmin, cmax, row));
            }
            let next_rowid = versions
                .iter()
                .filter(|version| {
                    version.next_rowid.is_some()
                        && version.xmax != crabka_pgmvcc::xid::INVALID_XID
                        && matches!(
                            global_status(kv, global, &settled_global)(version.xmax),
                            Ok(crabka_pgmvcc::clog::XidStatus::Committed)
                        )
                })
                .max_by_key(|version| version.key_xid)
                .and_then(|version| version.next_rowid);
            let Some(next_rowid) = next_rowid else {
                break None;
            };
            if !seen.insert(next_rowid) {
                return Err(ExecError::Unsupported(
                    "EvalPlanQual encountered a cyclic update chain".into(),
                ));
            }
            current_rowid = next_rowid;
            versions = scan_chain_versions(kv, table, current_rowid)?;
        }
    } else {
        // No concurrent committed change: find the version visible to our snapshot.
        find_visible_one_keyed(
            kv,
            global,
            &settled_global,
            snapshot,
            Some(xid),
            mutation.command_id,
            &versions,
        )?
        .map(|(key_xid, xmin, cmin, cmax, row)| (rowid, key_xid, xmin, cmin, cmax, row))
    };
    // This version came straight off the disk, where a virtual generated column
    // is a NULL placeholder. Callers re-check the statement's qual against it
    // and may hand it to `RETURNING old.*`, so it is completed here rather than
    // at each of them.
    if let Some((_, _, _, _, _, row)) = &mut found {
        expand_virtual_generated_row(table, row, mutation.eval_ctx, reads)?;
    }
    Ok(found)
}

/// [`find_visible_one`] over [`ChainVersion`]s, additionally returning the
/// visible version's PHYSICAL key xid so callers stamp the key that actually
/// exists (a frozen tuple's header `xmin` no longer names its key).
fn find_visible_one_keyed(
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snap: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    command_id: Option<u32>,
    versions: &[ChainVersion],
) -> Result<Option<(u64, u64, u32, u32, Vec<crabka_pgtypes::Datum>)>, ExecError> {
    let mut visible: Option<(u64, u64, u32, u32, Vec<crabka_pgtypes::Datum>)> = None;
    let mut live_count: usize = 0;
    for version in versions {
        if satisfies_mvcc_at_command(
            version.xmin,
            version.xmax,
            version.cmin,
            version.cmax,
            snap,
            own,
            command_id,
            global_status(kv, global, gsnap),
        )? {
            live_count += 1;
            // Keep the greatest live version — by header xmin, then by key xid
            // (frozen tuples all read xmin == FROZEN_XID; their key xids still
            // order them). See find_visible_one for the invariant discussion.
            if visible.as_ref().is_none_or(|(cur_key, cur_xmin, _, _, _)| {
                (version.xmin, version.key_xid) > (*cur_xmin, *cur_key)
            }) {
                visible = Some((
                    version.key_xid,
                    version.xmin,
                    version.cmin,
                    version.cmax,
                    version.row.clone(),
                ));
            }
        }
    }
    debug_assert!(
        live_count <= 1,
        "find_visible_one_keyed: {live_count} live versions for one rowid under one snapshot \
         — MVCC at-most-one-live invariant violated"
    );
    Ok(visible)
}

#[cfg(test)]
mod tests {
    use crabka_pgmvcc::{clog::XidStatus, visibility::Snapshot};

    use super::{command_can_see, satisfies_mvcc_at_command};

    #[test]
    fn own_versions_follow_command_counter_visibility() {
        let own = Some(7);
        let snapshot = Snapshot {
            xmin: 0,
            xmax: u64::MAX,
            xip: Vec::new(),
        };
        assert!(
            !command_can_see(7, 0, 2, 0, own, Some(2))
                && command_can_see(7, 0, 2, 0, own, Some(3))
                && command_can_see(5, 7, 0, 2, own, Some(2))
                && !command_can_see(5, 7, 0, 2, own, Some(3))
                && command_can_see(7, 0, 2, 0, own, None)
                && !satisfies_mvcc_at_command(7, 0, 2, 0, &snapshot, own, Some(2), |_| Ok(
                    XidStatus::Committed
                ),)
                .expect("visibility check")
                && satisfies_mvcc_at_command(5, 7, 0, 2, &snapshot, own, Some(2), |_| Ok(
                    XidStatus::Committed
                ),)
                .expect("visibility check")
                && !satisfies_mvcc_at_command(5, 7, 0, 2, &snapshot, own, Some(3), |_| Ok(
                    XidStatus::Committed
                ),)
                .expect("visibility check")
        );
    }
}
