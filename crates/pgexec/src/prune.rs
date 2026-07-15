//! Opportunistic dead-version pruning for single-node engines.
//!
//! PostgreSQL retains superseded tuple versions until VACUUM; Crabka's
//! substrate (checkpoint) mode prunes them when a checkpoint rewrites its
//! snapshot against the garbage horizon. Single-node engines (in-memory and
//! `--data-dir`) have no checkpoint pass, so without this module every UPDATE
//! of a row grows its version chain forever and hot-row workloads decay
//! hyperbolically.
//!
//! Instead of a VACUUM statement, the write path prunes opportunistically: a
//! statement that supersedes or deletes a row version folds `Delete` ops for
//! that row's dead versions into the same atomic commit batch. "Dead" is
//! decided against [`crate::procarray::ProcArray::garbage_horizon`] — the
//! minimum across running xids and leased snapshot xmins — so a version is
//! only removed once no present or future snapshot can see it. The pruning
//! statement holds the row's exclusive lock, so the chain scan cannot race a
//! concurrent writer of the same row.
//!
//! Rules (a version below the horizon is dead when):
//! - its deleter committed: `xmax != INVALID_XID`, `xmax < horizon`, and the
//!   clog records `xmax` as `Committed` — every snapshot at or above the
//!   horizon sees the deleter, hiding this version; or
//! - its creator aborted: `xmin < horizon` and the clog records `xmin` as
//!   `Aborted` — the version was never visible and its outcome is terminal.
//!
//! Everything else is kept: live versions (`xmax == INVALID_XID`), versions
//! whose deleter or creator is in-progress or `Prepared` (in-doubt two-phase
//! state must survive until resolution), frozen tuples, and timestamp tuples
//! (sharded tables prune through the checkpoint path).

use crabka_pgkv::{Kv, WriteOp};
use crabka_pgmvcc::{
    clog::{self, XidStatus},
    version,
    xid::{FROZEN_XID, INVALID_XID},
};

use crate::error::ExecError;

/// Rows whose version chain may hold newly-dead versions once `ops` commits:
/// those receiving a version `Put` that stamps a non-invalid `xmax` (an UPDATE
/// superseding a committed version, or a DELETE). Deduplicated and sorted.
pub(crate) fn prune_candidates(ops: &[WriteOp]) -> Vec<(u32, u64)> {
    let mut rows = std::collections::BTreeSet::new();
    for op in ops {
        let WriteOp::Put { key, value } = op else {
            continue;
        };
        let Some(row) = crabka_pgkv::key::table_rowid_of(key) else {
            continue;
        };
        let Ok((_, xmax)) = version::decode_tuple_header(value) else {
            continue; // timestamp tuples and non-version values
        };
        if xmax != INVALID_XID {
            rows.insert(row);
        }
    }
    rows.into_iter().collect()
}

/// Is one stored version value dead below `horizon`, per the module rules?
/// `false` for anything that is not an xid tuple (timestamp tuples, index
/// payloads).
///
/// # Errors
///
/// Returns an error when a clog lookup fails.
pub(crate) fn is_dead_version(kv: &dyn Kv, horizon: u64, value: &[u8]) -> Result<bool, ExecError> {
    let Ok((xmin, xmax)) = version::decode_tuple_header(value) else {
        return Ok(false);
    };
    if xmax != INVALID_XID && xmax < horizon && matches!(clog::get(kv, xmax)?, XidStatus::Committed)
    {
        return Ok(true);
    }
    Ok(xmin != FROZEN_XID && xmin < horizon && matches!(clog::get(kv, xmin)?, XidStatus::Aborted))
}

/// Append `Delete` ops for every version of `rows` that is dead below
/// `horizon`, per the module rules. Decisions read the durable pre-batch
/// state, which never overlaps the batch's own `Put`s: a statement only
/// rewrites versions that are live (or its own) in the durable state, and
/// those are always kept here.
///
/// # Errors
///
/// Returns an error when the version-chain scan or a clog lookup fails.
pub(crate) fn append_prune_ops(
    kv: &dyn Kv,
    horizon: u64,
    rows: &[(u32, u64)],
    ops: &mut Vec<WriteOp>,
) -> Result<(), ExecError> {
    for &(table, rowid) in rows {
        let prefix = crabka_pgkv::key::row_key(table, rowid);
        for (key, value) in kv.scan_prefix(&prefix)? {
            if is_dead_version(kv, horizon, &value)? {
                ops.push(WriteOp::Delete { key });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv, WriteOp};
    use crabka_pgmvcc::{
        clog::{self, XidStatus},
        version::{encode_ts_tuple, encode_tuple, version_key_xid},
        xid::{FROZEN_XID, INVALID_XID},
    };
    use crabka_pgtypes::Datum;

    use super::*;

    const TABLE: u32 = 7;
    const ROW: u64 = 42;

    fn store_with_clog(statuses: &[(u64, XidStatus)]) -> Arc<MemKv> {
        let kv = Arc::new(MemKv::new());
        let ops: Vec<WriteOp> = statuses
            .iter()
            .map(|&(xid, status)| clog::put_op(xid, status))
            .collect();
        kv.write_batch(&ops).expect("seed clog");
        kv
    }

    #[test]
    fn candidates_are_rows_with_a_superseding_or_deleting_put() {
        let superseding = WriteOp::Put {
            key: version_key_xid(TABLE, ROW, 3),
            value: encode_tuple(3, 9, &[Datum::Int4(1)]),
        };
        let fresh_insert = WriteOp::Put {
            key: version_key_xid(TABLE, 43, 9),
            value: encode_tuple(9, INVALID_XID, &[Datum::Int4(2)]),
        };
        let clog_entry = clog::put_op(9, XidStatus::Committed);
        let delete_op = WriteOp::Delete {
            key: version_key_xid(TABLE, 44, 2),
        };
        let duplicate = superseding.clone();

        let candidates =
            prune_candidates(&[superseding, fresh_insert, clog_entry, delete_op, duplicate]);

        assert!(candidates == vec![(TABLE, ROW)]);
    }

    #[test]
    fn candidates_skip_timestamp_tuples() {
        let ts_put = WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_ts(TABLE, ROW, 5),
            value: encode_ts_tuple(
                5,
                crabka_pgmvcc::version::TsVersionState::Deleted { commit_ts: 8 },
                &[],
            ),
        };

        assert!(prune_candidates(&[ts_put]).is_empty());
    }

    #[test]
    fn prune_table_drives_deletes_from_horizon_and_clog() {
        struct Case {
            name: &'static str,
            xmin: u64,
            xmax: u64,
            clog: &'static [(u64, XidStatus)],
            horizon: u64,
            pruned: bool,
        }
        let cases = [
            Case {
                name: "superseded below horizon by committed deleter",
                xmin: 2,
                xmax: 4,
                clog: &[(2, XidStatus::Committed), (4, XidStatus::Committed)],
                horizon: 10,
                pruned: true,
            },
            Case {
                name: "deleter at the horizon is kept",
                xmin: 2,
                xmax: 10,
                clog: &[(2, XidStatus::Committed), (10, XidStatus::Committed)],
                horizon: 10,
                pruned: false,
            },
            Case {
                name: "in-progress deleter is kept",
                xmin: 2,
                xmax: 4,
                clog: &[(2, XidStatus::Committed)],
                horizon: 10,
                pruned: false,
            },
            Case {
                name: "prepared deleter is kept until resolution",
                xmin: 2,
                xmax: 4,
                clog: &[(2, XidStatus::Committed), (4, XidStatus::Prepared(900))],
                horizon: 10,
                pruned: false,
            },
            Case {
                name: "aborted deleter keeps the version live",
                xmin: 2,
                xmax: 4,
                clog: &[(2, XidStatus::Committed), (4, XidStatus::Aborted)],
                horizon: 10,
                pruned: false,
            },
            Case {
                name: "aborted creator below horizon",
                xmin: 2,
                xmax: INVALID_XID,
                clog: &[(2, XidStatus::Aborted)],
                horizon: 10,
                pruned: true,
            },
            Case {
                name: "aborted creator at the horizon is kept",
                xmin: 10,
                xmax: INVALID_XID,
                clog: &[(10, XidStatus::Aborted)],
                horizon: 10,
                pruned: false,
            },
            Case {
                name: "prepared creator is kept until resolution",
                xmin: 2,
                xmax: INVALID_XID,
                clog: &[(2, XidStatus::Prepared(900))],
                horizon: 10,
                pruned: false,
            },
            Case {
                name: "live committed version is kept",
                xmin: 2,
                xmax: INVALID_XID,
                clog: &[(2, XidStatus::Committed)],
                horizon: 10,
                pruned: false,
            },
            Case {
                name: "frozen live version is kept",
                xmin: FROZEN_XID,
                xmax: INVALID_XID,
                clog: &[],
                horizon: 10,
                pruned: false,
            },
        ];

        for case in cases {
            let kv = store_with_clog(case.clog);
            let key = version_key_xid(TABLE, ROW, case.xmin);
            kv.put(
                key.clone(),
                encode_tuple(case.xmin, case.xmax, &[Datum::Int4(1)]),
            )
            .expect("seed version");

            let mut ops = Vec::new();
            append_prune_ops(kv.as_ref(), case.horizon, &[(TABLE, ROW)], &mut ops).expect("prune");

            let expected: Vec<WriteOp> = if case.pruned {
                vec![WriteOp::Delete { key }]
            } else {
                Vec::new()
            };
            assert!(ops == expected, "case: {}", case.name);
        }
    }

    #[test]
    fn prune_scans_only_the_candidate_row() {
        let kv = store_with_clog(&[(2, XidStatus::Committed), (3, XidStatus::Committed)]);
        // Same shape of garbage on two rows; only ROW is a candidate.
        for rowid in [ROW, ROW + 1] {
            kv.put(
                version_key_xid(TABLE, rowid, 2),
                encode_tuple(2, 3, &[Datum::Int4(1)]),
            )
            .expect("seed superseded");
            kv.put(
                version_key_xid(TABLE, rowid, 3),
                encode_tuple(3, INVALID_XID, &[Datum::Int4(2)]),
            )
            .expect("seed live");
        }

        let mut ops = Vec::new();
        append_prune_ops(kv.as_ref(), 10, &[(TABLE, ROW)], &mut ops).expect("prune");

        assert!(
            ops == vec![WriteOp::Delete {
                key: version_key_xid(TABLE, ROW, 2)
            }]
        );
    }
}
