//! MVCC row-version-chain pruning.

use super::*;

/// Result of pruning one rowid's version chain.
pub(crate) struct ChainPrune {
    /// `WriteOp`s reclaiming the chain: `Delete`s for dead version keys and
    /// their orphaned local secondary-index entries, plus (for `vacuum`)
    /// `Put`s freezing surviving sub-horizon tuple headers. Empty when
    /// nothing on the chain needs work.
    pub ops: Vec<crabka_pgkv::WriteOp>,
    /// Number of tuple versions deleted by `ops`.
    pub versions: u64,
    /// Number of secondary-index entries deleted by `ops`.
    pub index_entries: u64,
    /// Number of surviving tuple versions frozen by `ops`.
    pub frozen: u64,
}

/// Delete ops reclaiming `rowid`'s dead versions (and the local secondary-index
/// entries no surviving version still needs), judged at `horizon`.
///
/// A version is dead per [`crabka_pgmvcc::gc::version_is_dead`]: its creator
/// aborted, or a transaction that committed below `horizon` deleted/superseded
/// it. `horizon` must come from `checkpoint_garbage_horizon`, which caps it at
/// the oldest running writer xid, the lowest registered snapshot pin, and the
/// first non-terminal clog entry.
///
/// Snapshot safety: every snapshot consumer registers a `GcHorizon` pin at its
/// snapshot `xmin` for as long as the snapshot is in use (REPEATABLE READ
/// transactions pin at BEGIN until COMMIT/ROLLBACK; autocommit and READ
/// COMMITTED statements pin for the statement's duration), so a version some
/// live snapshot still sees, one whose committed deleter is in that
/// snapshot's `xip` or above its `xmax`, keeps `horizon` at or below the
/// deleter's xid and is never selected here. Un-pinned readers do not exist:
/// every statement path pins before taking its snapshot, and the pin value
/// (the ProcArray xmin at pin time) is monotonically `<=` any snapshot xmin
/// taken afterwards. `MemKv`/`FjallKv` scans additionally materialize their
/// results eagerly (`KvScan` is a `Vec`), so even a scan that raced an earlier
/// horizon computation observes an atomic before-or-after state of each prune
/// batch, never a partially deleted chain.
///
/// Lock interaction: callers must hold `rowid`'s exclusive row lock (UPDATE/
/// DELETE already do; `vacuum` takes it per row). Dead version KEYS can never
/// collide with a concurrent writer's puts. A writer only writes the newest
/// committed version's key (it stamps `xmax`) and its own new key, and neither
/// is ever dead. But the survivor computation for shared index entries must
/// not race a writer that re-adds the same indexed values for this rowid.
///
/// Engine kinds: sound everywhere, because the returned ops are folded into
/// the caller's own commit batch. On replicated engines they replicate
/// through the WAL and replay deterministically. Global 2PC writes are
/// self-protecting: an undecided enlisted xid reads as `Prepared` (which
/// [`crabka_pgmvcc::gc::version_is_dead`] never treats as dead), and global
/// xids sit numerically above every local horizon.
///
/// One rowid-chain prune request (see [`prune_rowid_chain_ops`]).
pub(crate) struct ChainPruneRequest<'a> {
    /// The row whose version chain is pruned.
    pub rowid: u64,
    /// The garbage horizon (from `checkpoint_garbage_horizon`).
    pub horizon: u64,
    /// Version-key xids this batch itself (re)writes. They are never deleted
    /// or frozen, whatever their current on-disk state.
    pub keep_xids: &'a [u64],
    /// The row this batch is writing, when any; its indexed values count as
    /// survivors so a shared index entry is never deleted out from under the
    /// incoming version.
    pub new_row: Option<&'a [Datum]>,
    /// When given (`vacuum` only), additionally rewrite every surviving
    /// version whose creator committed below this floor to `FROZEN_XID`
    /// (visible to every snapshot without a clog lookup). That is the
    /// precondition for a truncation of the clog below the horizon. The freeze
    /// is invisible to every snapshot: a registered snapshot's `xmin` is at or
    /// above the
    /// horizon, so a committed sub-horizon creator was already
    /// settled-and-committed for it.
    pub freeze_below: Option<u64>,
}

/// Rate-limited write-path reclamation telemetry accumulated between
/// emissions (process-wide; see [`log_prune_engagement`]).
#[derive(Default)]
struct PruneEngagementLog {
    /// When the previous line was emitted; `None` before the first.
    last_emitted: Option<std::time::Instant>,
    /// Row chains examined since the previous line.
    rows: u64,
    /// Dead versions selected for deletion since the previous line.
    pruned: u64,
}

static PRUNE_ENGAGEMENT: std::sync::LazyLock<std::sync::Mutex<PruneEngagementLog>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(PruneEngagementLog::default()));

/// Emit at most one `xid_chain_prune_engaged` debug line per second.
///
/// The line carries the current horizon and the chain/deletion counts
/// accumulated since the previous line. A live node that logs a low `horizon`
/// with growing `rows` and zero `pruned` shows the write path consults the
/// horizon but finds nothing dead. A non-zero `pruned` confirms end-to-end
/// reclamation.
fn log_prune_engagement(horizon: u64, pruned: u64) {
    const EMIT_EVERY: std::time::Duration = std::time::Duration::from_secs(1);
    let mut log = PRUNE_ENGAGEMENT.lock().expect("prune engagement log");
    log.rows += 1;
    log.pruned += pruned;
    let now = std::time::Instant::now();
    let due = log
        .last_emitted
        .is_none_or(|last| now.duration_since(last) >= EMIT_EVERY);
    if !due {
        return;
    }
    tracing::debug!(
        horizon,
        pruned = log.pruned,
        rows = log.rows,
        "xid_chain_prune_engaged"
    );
    log.last_emitted = Some(now);
    log.rows = 0;
    log.pruned = 0;
}

pub(crate) fn prune_rowid_chain_ops(
    kv: &dyn Kv,
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    request: &ChainPruneRequest<'_>,
) -> Result<ChainPrune, ExecError> {
    let &ChainPruneRequest {
        rowid,
        horizon,
        keep_xids,
        new_row,
        freeze_below,
    } = request;
    let status = |xid| crabka_pgmvcc::clog::get(kv, xid);
    let mut dead: Vec<(Vec<u8>, Vec<Datum>)> = Vec::new();
    let mut surviving: Vec<Vec<Datum>> = Vec::new();
    let mut freeze: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (key, value) in kv.scan_prefix(&crabka_pgkv::key::row_key(table.id, rowid))? {
        let (xmin, xmax, row) = crabka_pgmvcc::version::decode_tuple(&value)?;
        let key_xid = crabka_pgmvcc::version::xid_of_key(&key)?;
        if !keep_xids.contains(&key_xid)
            && crabka_pgmvcc::gc::version_is_dead(xmin, xmax, horizon, &status)?
        {
            dead.push((key, row));
            continue;
        }
        if let Some(floor) = freeze_below
            && !keep_xids.contains(&key_xid)
            && xmin != crabka_pgmvcc::xid::FROZEN_XID
            && xmin < floor
            && matches!(status(xmin)?, crabka_pgmvcc::clog::XidStatus::Committed)
        {
            freeze.push((key, value.clone()));
        }
        surviving.push(row);
    }
    log_prune_engagement(horizon, dead.len() as u64);
    if dead.is_empty() && freeze.is_empty() {
        return Ok(ChainPrune {
            ops: Vec::new(),
            versions: 0,
            index_entries: 0,
            frozen: 0,
        });
    }
    let mut ops: Vec<crabka_pgkv::WriteOp> = Vec::new();
    let frozen = freeze.len() as u64;
    for (key, value) in freeze {
        ops.push(crabka_pgkv::WriteOp::Put {
            key,
            value: crabka_pgmvcc::version::freeze_tuple_xmin(&value)?,
        });
    }
    let mut index_entries_pruned: u64 = 0;
    // An index entry key `(values, rowid)` is SHARED by every version of this
    // row carrying `values`: delete it only when no surviving version — nor
    // the row this batch is writing — still carries those values. Chains are
    // short (pruning keeps them O(1)), so linear survivor probes suffice.
    let mut removed: Vec<(crabka_pgcatalog::IndexId, Vec<Datum>)> = Vec::new();
    for index in local_indexes {
        let mut survivor_entries = Vec::new();
        for row in &surviving {
            survivor_entries.extend(index_entries(table, index, row)?);
        }
        if let Some(row) = new_row {
            survivor_entries.extend(index_entries(table, index, row)?);
        }
        for (_, row) in &dead {
            for values in index_entries(table, index, row)? {
                if survivor_entries.contains(&values)
                    || removed
                        .iter()
                        .any(|(id, prior)| *id == index.id && *prior == values)
                {
                    continue;
                }
                ops.push(crabka_pgkv::WriteOp::Delete {
                    key: crabka_pgkv::key::secondary_index_entry_key(
                        table.id, index.id, &values, rowid,
                    ),
                });
                removed.push((index.id, values));
                index_entries_pruned += 1;
            }
        }
    }
    let versions = dead.len() as u64;
    ops.extend(
        dead.into_iter()
            .map(|(key, _)| crabka_pgkv::WriteOp::Delete { key }),
    );
    Ok(ChainPrune {
        ops,
        versions,
        index_entries: index_entries_pruned,
        frozen,
    })
}
