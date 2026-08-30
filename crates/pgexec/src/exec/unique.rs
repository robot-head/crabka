//! Local unique and exclusion constraint enforcement.

use super::*;

pub(super) type PendingUniqueKey = (crabka_pgcatalog::IndexId, Vec<Datum>);

pub(super) async fn enforce_unique_local_index_updates(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    indexes: &[crabka_pgcatalog::Index],
    rowid: u64,
    old_row: &[Datum],
    new_row: &[Datum],
    writes: &mut StatementWrites,
) -> Result<(), ExecError> {
    for index in indexes
        .iter()
        .filter(|index| index.unique && index.exclusion_operators().is_none())
    {
        if !index_applies(table, index, new_row)? {
            continue;
        }
        let old_values = indexed_values(table, index, old_row)?;
        let new_values = indexed_values(table, index, new_row)?;
        if index_applies(table, index, old_row)? && old_values == new_values {
            // The indexed key is untouched: no probe, and — crucially for
            // write throughput — no key lock (a PK-preserving UPDATE takes
            // only its row lock).
            continue;
        }
        enforce_unique_local_index(write_ctx, table, index, rowid, new_values, writes).await?;
    }
    for index in indexes
        .iter()
        .filter(|index| index.exclusion_operators().is_some())
    {
        let old_values = indexed_values(table, index, old_row)?;
        let new_values = indexed_values(table, index, new_row)?;
        if old_values != new_values {
            enforce_exclusion_constraint(write_ctx, table, index, rowid, new_values, writes)
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn enforce_unique_local_indexes(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    indexes: &[crabka_pgcatalog::Index],
    rowid: u64,
    row: &[Datum],
    writes: &mut StatementWrites,
) -> Result<(), ExecError> {
    for index in indexes
        .iter()
        .filter(|index| index.unique && index.exclusion_operators().is_none())
    {
        if !index_applies(table, index, row)? {
            continue;
        }
        let values = indexed_values(table, index, row)?;
        enforce_unique_local_index(write_ctx, table, index, rowid, values, writes).await?;
    }
    for index in indexes
        .iter()
        .filter(|index| index.exclusion_operators().is_some())
    {
        let values = indexed_values(table, index, row)?;
        enforce_exclusion_constraint(write_ctx, table, index, rowid, values, writes).await?;
    }
    Ok(())
}

/// Refuse an empty range in the `WITHOUT OVERLAPS` column of a temporal key
/// (23514).
///
/// An empty range overlaps nothing, so every such row would pass the constraint
/// and the key would silently stop being a key. `PostgreSQL` checks this at
/// write time rather than as a `CHECK`, and only for `WITHOUT OVERLAPS` — a
/// hand-written `EXCLUDE … WITH &&` happily stores empty ranges.
pub(super) fn reject_empty_without_overlaps(
    table: &Table,
    index: &crabka_pgcatalog::Index,
    values: &[Datum],
) -> Result<(), ExecError> {
    if !index.without_overlaps {
        return Ok(());
    }
    let (Some(column), Some(value)) = (index.columns.last(), values.last()) else {
        return Ok(());
    };
    let empty = match value {
        Datum::Range(range) => range.empty,
        Datum::Multirange(multirange) => multirange.ranges.is_empty(),
        _ => false,
    };
    if empty {
        return Err(ExecError::EmptyWithoutOverlapsValue {
            column: column.clone(),
            relation: table.name.name.clone(),
        });
    }
    Ok(())
}

pub(super) async fn enforce_exclusion_constraint(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    rowid: u64,
    values: Vec<Datum>,
    writes: &mut StatementWrites,
) -> Result<(), ExecError> {
    reject_empty_without_overlaps(table, index, &values)?;
    // Boxed: this scan's future is large, and the enclosing write path is
    // itself reached recursively (a set-returning function calling a query
    // calling a write), so inlining it here would grow every frame of that
    // recursion.
    let Some(holder) = Box::pin(exclusion_conflict(
        write_ctx,
        table,
        index,
        Some(rowid),
        &values,
        writes,
    ))
    .await?
    else {
        if index.exclusion_operators().is_some() && !values.iter().any(Datum::is_null) {
            writes
                .pending_exclusion_keys
                .entry(index.id)
                .or_default()
                .push((rowid, values));
        }
        return Ok(());
    };
    Err(exclusion_violation(
        write_ctx, table, index, &values, &holder,
    ))
}

/// The key of a live row this one cannot coexist with under `index`, or `None`
/// when it is free to be stored.
///
/// Shared by enforcement and by `ON CONFLICT` arbitration, which need the same
/// answer and differ only in what they do with it: a violation, or a skipped
/// row. `self_rowid` is the row being written, excluded from its own check;
/// arbitration passes `None` because the proposed row has no version yet.
///
/// A NULL anywhere in the key cannot conflict — `PostgreSQL`'s exclusion
/// operators are strict, so a NULL comparison is unknown, not true.
pub(super) async fn exclusion_conflict(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    self_rowid: Option<u64>,
    values: &[Datum],
    writes: &StatementWrites,
) -> Result<Option<Vec<Datum>>, ExecError> {
    let Some(operators) = index.exclusion_operators() else {
        return Ok(None);
    };
    if values.iter().any(Datum::is_null) {
        return Ok(None);
    }
    // ponytail: This deliberately serializes per constraint. A spatial lock
    // structure can replace it when GiST indexes become physical rather than
    // catalog-only; correctness needs only this one coarse key today.
    write_ctx
        .lockmgr
        .acquire_key_as(
            crate::lockmgr::LockKey::UniqueKey(crabka_pgkv::key::secondary_index_entry_prefix(
                table.id,
                index.id,
                &[],
            )),
            crate::lockmgr::LockMode::Exclusive,
            write_ctx.lock_owner,
            write_ctx.lock_wait_cap,
        )
        .await
        .map_err(lock_acquire_error)?;

    let current_visibility = all_committed_snapshot();
    let rows = scan_live(
        write_ctx.kv,
        write_ctx.global,
        &current_visibility,
        &current_visibility,
        Some(write_ctx.xid),
        table,
    )?;
    for (holder_rowid, _xmin, holder_row) in rows {
        if Some(holder_rowid) == self_rowid || !writes.holder_still_holds(index.id, holder_rowid) {
            continue;
        }
        let holder = indexed_values(table, index, &holder_row)?;
        if exclusion_keys_conflict(&operators, values, &holder)? {
            return Ok(Some(holder));
        }
    }
    if let Some(pending) = writes.pending_exclusion_keys.get(&index.id) {
        for (holder_rowid, holder) in pending {
            if Some(*holder_rowid) != self_rowid
                && exclusion_keys_conflict(&operators, values, holder)?
            {
                return Ok(Some(holder.clone()));
            }
        }
    }
    Ok(None)
}

pub(super) fn exclusion_keys_conflict(
    operators: &[crabka_pgcatalog::ExclusionOperator],
    left: &[Datum],
    right: &[Datum],
) -> Result<bool, ExecError> {
    for ((operator, left), right) in operators.iter().zip(left).zip(right) {
        if left.is_null() || right.is_null() {
            return Ok(false);
        }
        let conflicts = match operator {
            crabka_pgcatalog::ExclusionOperator::Equal => {
                crabka_pgtypes::ops::compare(left, right)? == Some(std::cmp::Ordering::Equal)
            }
            crabka_pgcatalog::ExclusionOperator::Overlaps => match (left, right) {
                (Datum::Range(left), Datum::Range(right)) => {
                    crabka_pgtypes::range::overlaps(left, right)?
                }
                (Datum::Multirange(left), Datum::Multirange(right)) => {
                    crabka_pgtypes::multirange::overlaps(left, right)?
                }
                (Datum::Multirange(left), Datum::Range(right)) => {
                    crabka_pgtypes::multirange::overlaps_range(left, right)?
                }
                (Datum::Range(left), Datum::Multirange(right)) => {
                    crabka_pgtypes::multirange::overlaps_range(right, left)?
                }
                _ => return Err(ExecError::UndefinedFunction("operator &&".into())),
            },
        };
        if !conflicts {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn exclusion_violation(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    proposed: &[Datum],
    existing: &[Datum],
) -> ExecError {
    let describer = write_ctx.describer();
    let describe = |values: &[Datum]| {
        describer.index_key(
            write_ctx.catalog_kv,
            table,
            &index.columns,
            values,
            write_ctx.eval_ctx,
        )
    };
    // `check_exclusion_or_unique_constraint` describes both keys and prints the
    // pair only if it got both. A caller that may not read the relation gets the
    // bare sentence, which says nothing the primary message did not.
    let detail = match (describe(proposed), describe(existing)) {
        (Some(proposed), Some(existing)) => {
            format!("Key {proposed} conflicts with existing key {existing}.")
        }
        _ => "Key conflicts with existing key.".to_string(),
    };
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "23P01",
            format!(
                "conflicting key value violates exclusion constraint \"{}\"",
                index.name
            ),
        )
        .with_detail(detail)
        .with_schema(table.name.schema.clone())
        .with_table(table.name.name.clone())
        .with_constraint(index.name.clone()),
    )
}

/// The 23505 a write raises when its key is already taken, with the `DETAIL`
/// `PostgreSQL`'s `_bt_check_unique` attaches to it.
pub(super) fn unique_violation(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    values: &[Datum],
) -> ExecError {
    ExecError::UniqueViolation(Box::new(crate::error::UniqueViolation {
        index: index.name.clone(),
        table: table.name.clone(),
        key: write_ctx.describer().index_key(
            write_ctx.catalog_kv,
            table,
            &index.columns,
            values,
            write_ctx.eval_ctx,
        ),
    }))
}

pub(super) async fn enforce_unique_local_index(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    rowid: u64,
    values: Vec<Datum>,
    writes: &mut StatementWrites,
) -> Result<(), ExecError> {
    if values.iter().any(Datum::is_null) {
        // SQL unique ignores NULLs: nothing to enforce, so no key lock either.
        return Ok(());
    }
    if index.deferral.is_deferrable() {
        // A DEFERRABLE constraint is not checked as the row is written: this
        // key may be one another row of the same statement is about to give up,
        // and under `INITIALLY DEFERRED` a later statement may still repair it.
        // The key lock is taken here all the same — the entry is in the KV from
        // here on, so a concurrent writer of the same key must still queue
        // behind this transaction's outcome.
        acquire_unique_key_lock(write_ctx, table, index, &values).await?;
        writes.unique_checks.push(crate::fk::PendingUniqueCheck {
            table: table.name.clone(),
            index: index.clone(),
            rowid,
            values,
        });
        return Ok(());
    }
    // The claim spans the whole statement, so a `WITH` item and the body cannot
    // both write the same key.
    let pending_key = (index.id, values.clone());
    if !writes.pending_unique_keys.insert(pending_key) {
        return Err(unique_violation(write_ctx, table, index, &values));
    }
    let holders = lock_and_probe_unique_key(write_ctx, table, index, &values).await?;
    // A holder whose key an earlier part of this statement freed is a version
    // this command has already superseded: PostgreSQL's uniqueness check does
    // not see it either.
    if holders
        .iter()
        .any(|holder| holder.rowid != rowid && writes.holder_still_holds(index.id, holder.rowid))
    {
        return Err(unique_violation(write_ctx, table, index, &values));
    }
    Ok(())
}

/// Take `values`' unique-key lock and return the rows that currently hold that
/// key.
///
/// This is the lock-and-probe half of unique enforcement, shared with
/// `ON CONFLICT` arbitration.
///
/// Serializes check-then-write PER KEY: takes this key's exclusive lock (in the
/// row-lock manager, so it shares the deadlock wait-for graph and is released
/// with the row locks at COMMIT/ROLLBACK) before probing. Without it, two
/// concurrent writers of the same key would both pass the probe, because
/// neither sees the other's uncommitted version, and both would commit. A
/// waiter that wakes here
/// after the holder's terminal outcome probes the then-current committed state:
/// a holder if it committed, none if it rolled back.
///
/// The probe reads exactly this key instead of scanning the whole table, under
/// the scan path's visibility (all-committed local + global snapshots plus our
/// own xid), and `lookup_local_index_equal` resolves each candidate rowid
/// through MVCC and re-checks its visible row's values. So dead entries left by
/// old versions or aborted writers never count.
///
/// `acquire_key` is idempotent for a holder xid, so a caller that already locked
/// this key (arbitration, before the insert path re-enforces uniqueness) can
/// call again without self-deadlocking.
pub(super) async fn lock_and_probe_unique_key(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    values: &[Datum],
) -> Result<Vec<ScannedRow>, ExecError> {
    acquire_unique_key_lock(write_ctx, table, index, values).await?;
    let mvcc = write_ctx.mvcc_read();
    probe_unique_key(write_ctx, mvcc.kv, table, index, values)
}

/// The lock half of [`lock_and_probe_unique_key`], for the deferrable path,
/// which claims the key now and proves it holds it alone later.
pub(super) async fn acquire_unique_key_lock(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    values: &[Datum],
) -> Result<(), ExecError> {
    write_ctx
        .lockmgr
        .acquire_key_as(
            crate::lockmgr::LockKey::UniqueKey(crabka_pgkv::key::secondary_index_entry_prefix(
                table.id, index.id, values,
            )),
            crate::lockmgr::LockMode::Exclusive,
            write_ctx.lock_owner,
            write_ctx.lock_wait_cap,
        )
        .await
        .map_err(lock_acquire_error)
}

/// The probe half: the rows that hold `values` on `index` right now, read
/// through `kv`.
///
/// `kv` is a parameter rather than `write_ctx.kv` because the deferred recheck
/// reads a view with the statement's pending batch layered over the store,
/// exactly as the referential drain does.
pub(super) fn probe_unique_key(
    write_ctx: &WriteContext<'_>,
    kv: &dyn Kv,
    table: &Table,
    index: &crabka_pgcatalog::Index,
    values: &[Datum],
) -> Result<Vec<ScannedRow>, ExecError> {
    let mvcc = write_ctx.mvcc_read();
    let current_visibility = all_committed_snapshot();
    let probe = MvccReadContext {
        kv,
        global: mvcc.global,
        global_snapshot: &current_visibility,
        snapshot: &current_visibility,
        own: mvcc.own,
        command_id: mvcc.command_id,
    };
    lookup_local_index_equal(&probe, table, index, values)
}

/// The unique local indexes that arbitrate an `ON CONFLICT` clause, resolved
/// once per statement (before the row loop) from the clause's conflict target.
///
/// - `Columns` matches every unique local index whose column SET equals the
///   inference set. PostgreSQL's inference is order-insensitive even though an
///   index's columns are ordered, so `ON CONFLICT (b, a)` arbitrates a
///   `UNIQUE (a, b)` index. No match is 42P10.
/// - `Columns` with an index predicate (`ON CONFLICT (c) WHERE …`) still
///   matches a full unique index: its implicit predicate is true, which every
///   inference predicate implies. Partial-index matching is not implemented.
/// - `OnConstraint` matches by index name, restricted to indexes that back a
///   constraint, because PostgreSQL rejects `ON CONSTRAINT` naming a plain
///   index.
///   No match is 42704.
/// - `None` (reachable only with `DO NOTHING`; the parser rejects a bare
///   `DO UPDATE`) arbitrates every unique local index. An empty result is legal:
///   a table with no unique index simply never conflicts.
///
/// Global unique indexes never reach here, because `writable_local_indexes`
/// refuses them for every write on the table.
pub(crate) fn resolve_arbiter_indexes(
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    target: &crabka_pgparser::ast::OnConflictTarget,
) -> Result<Vec<crabka_pgcatalog::Index>, ExecError> {
    use crabka_pgparser::ast::OnConflictTarget;

    // Inference by column list arbitrates on equality alone, so an
    // exclusion-enforced index can never satisfy it — `ON CONFLICT (id,
    // valid_at)` against a `WITHOUT OVERLAPS` key is 42P10 in PostgreSQL, not a
    // match. A bare `DO NOTHING` names no columns and does catch them.
    let equality = || {
        local_indexes
            .iter()
            .filter(|index| index.unique && index.exclusion_operators().is_none())
    };
    let arbitrable = || {
        local_indexes
            .iter()
            .filter(|index| index.unique || index.exclusion_operators().is_some())
    };
    match target {
        OnConflictTarget::None => Ok(arbitrable().cloned().collect()),
        OnConflictTarget::Columns {
            columns,
            index_predicate: _,
            ..
        } => {
            for column in columns {
                if table.column_index(column).is_none() {
                    return Err(ExecError::UndefinedColumn(column.clone()));
                }
            }
            let wanted: BTreeSet<&str> = columns.iter().map(String::as_str).collect();
            let arbiters: Vec<_> = equality()
                .filter(|index| {
                    index
                        .columns
                        .iter()
                        .map(String::as_str)
                        .collect::<BTreeSet<_>>()
                        == wanted
                })
                .cloned()
                .collect();
            if arbiters.is_empty() {
                return Err(ExecError::OnConflictNoArbiter);
            }
            Ok(arbiters)
        }
        OnConflictTarget::OnConstraint(name) => local_indexes
            .iter()
            .find(|index| index.constraint.is_some() && index.name == *name)
            .map(|index| vec![index.clone()])
            .ok_or_else(|| ExecError::UndefinedConstraint {
                name: name.clone(),
                table: table.name.to_string(),
            }),
    }
    .and_then(|arbiters| reject_deferrable_arbiter(table, &arbiters).map(|()| arbiters))
}

/// `ON CONFLICT` cannot arbitrate on a `DEFERRABLE` key.
///
/// Speculative insertion decides the row's fate before the statement ends,
/// which is earlier than a deferrable constraint is willing to answer, so
/// `ExecCheckIndexConstraints` refuses instead of guessing. The refusal covers
/// a bare `DO NOTHING` too, whose arbiter set is every unique index the
/// relation has.
pub(super) fn reject_deferrable_arbiter(
    table: &Table,
    arbiters: &[crabka_pgcatalog::Index],
) -> Result<(), ExecError> {
    let Some(index) = arbiters.iter().find(|index| index.deferral.is_deferrable()) else {
        return Ok(());
    };
    Err(ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "55000",
            "ON CONFLICT does not support deferrable unique constraints/exclusion constraints as \
             arbiters",
        )
        .with_schema(table.name.schema.clone())
        .with_table(table.name.name.clone())
        .with_constraint(index.name.clone()),
    ))
}

/// What one `VALUES` row of an `INSERT … ON CONFLICT` should do, decided by
/// [`arbitrate_insert_row`].
pub(super) enum InsertRowPlan {
    /// No arbiter conflicts: insert the proposed row through the normal path.
    Insert,
    /// `DO NOTHING` on a conflict: the row is skipped entirely. There are no
    /// ops, no RETURNING row, and it does not count towards the command tag.
    Skip,
    /// `DO UPDATE` on a conflict: the stored row to update, already locked and
    /// re-read under [`eval_plan_qual`].
    Update {
        rowid: u64,
        cur_key_xid: u64,
        cur_xmin: u64,
        cur_cmin: u32,
        cur_row: Vec<Datum>,
    },
}

/// Decide what an `INSERT … ON CONFLICT` does with one proposed row.
///
/// Probes the arbiter indexes in catalog order (`list_table_indexes` sorts by
/// name, so the choice of conflicting index is deterministic) and stops at the
/// first conflict. An arbiter whose key holds a NULL cannot conflict, because
/// SQL unique treats NULLs as distinct. That matches the enforcement path's own
/// short-circuit.
///
/// A key already claimed by an earlier row of THIS statement lives only in the
/// pending op batch, invisible to a KV probe, so it is checked separately:
/// `DO NOTHING` skips the row and `DO UPDATE` raises 21000. That reproduces
/// PostgreSQL's `INSERT … VALUES (1), (1)` semantics exactly.
///
/// Termination: the outer loop restarts only after adding a holder rowid to
/// `discarded` (its row vanished under the lock, or no longer carries the
/// arbiter key). Every probed key is held under an exclusive key lock for the
/// rest of the transaction, so the holder sets can only shrink and no new
/// holder can appear. `discarded` grows strictly on each restart, bounded by
/// the rows already in the table.
pub(super) async fn arbitrate_insert_row(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    arbiters: &[crabka_pgcatalog::Index],
    on_conflict: &crabka_pgparser::ast::OnConflict,
    proposed: &[Datum],
    writes: &StatementWrites,
) -> Result<InsertRowPlan, ExecError> {
    use crabka_pgparser::ast::OnConflictAction;

    let do_update = matches!(on_conflict.action, OnConflictAction::DoUpdate { .. });
    // An exclusion-enforced arbiter has no single conflicting row to update:
    // the proposed row may overlap several at once. PostgreSQL refuses the
    // combination outright, before it looks at any data.
    if do_update
        && arbiters
            .iter()
            .any(|index| index.exclusion_operators().is_some())
    {
        return Err(ExecError::Unsupported(
            "ON CONFLICT DO UPDATE not supported with exclusion constraints".into(),
        ));
    }
    let mut discarded: HashSet<u64> = HashSet::new();
    'arbitration: loop {
        for index in arbiters {
            let values = indexed_values(table, index, proposed)?;
            if values.iter().any(Datum::is_null) {
                continue;
            }
            // An overlap conflict is not a key collision, so it is found by the
            // same scan enforcement uses rather than by a key probe. Only
            // `DO NOTHING` reaches here — `DO UPDATE` was refused above.
            if index.exclusion_operators().is_some() {
                if Box::pin(exclusion_conflict(
                    write_ctx, table, index, None, &values, writes,
                ))
                .await?
                .is_some()
                {
                    return Ok(InsertRowPlan::Skip);
                }
                continue;
            }
            if writes
                .pending_unique_keys
                .contains(&(index.id, values.clone()))
            {
                if do_update {
                    return Err(ExecError::OnConflictAffectsRowTwice);
                }
                return Ok(InsertRowPlan::Skip);
            }
            let holders = lock_and_probe_unique_key(write_ctx, table, index, &values).await?;
            // The proposed row has no version of its own yet, so every visible
            // holder is a genuine conflict.
            // A holder whose key an earlier part of this statement freed no
            // longer conflicts: that version has already been superseded.
            let Some(holder) = holders.into_iter().find(|holder| {
                !discarded.contains(&holder.rowid)
                    && writes.holder_still_holds(index.id, holder.rowid)
            }) else {
                continue;
            };
            if !do_update {
                return Ok(InsertRowPlan::Skip);
            }
            if writes.is_claimed(table.id, holder.rowid) {
                return Err(ExecError::OnConflictAffectsRowTwice);
            }
            // The probe deliberately reads all-committed visibility, so it finds
            // rows committed after our snapshot. `eval_plan_qual`'s own 40001
            // check keys off xmax stamps and would not catch a freshly inserted
            // row that our snapshot cannot see, so REPEATABLE READ needs this
            // explicit guard — without it the upsert would silently update a row
            // it cannot read.
            if write_ctx.repeatable_read
                && holder.xmin != write_ctx.xid
                && !snapshot_can_see(write_ctx.snapshot, holder.xmin)
            {
                return Err(ExecError::SerializationFailure);
            }
            // Key lock first, then row lock — the established order everywhere
            // on the write path, so upserts add no new deadlock shapes.
            write_ctx
                .lockmgr
                .acquire_as(
                    table.id,
                    holder.rowid,
                    crate::lockmgr::LockMode::Exclusive,
                    write_ctx.lock_owner,
                    write_ctx.lock_wait_cap,
                )
                .await
                .map_err(lock_acquire_error)?;
            // READ COMMITTED: the probe reads all-committed visibility, so a
            // holder committed while we waited on the key lock is real but
            // invisible to our statement snapshot. `eval_plan_qual`'s own
            // read-committed refresh only fires on an `xmax` stamp, and a
            // concurrent INSERT leaves none — so re-read such a holder under a
            // fresh snapshot. Discarding it instead would fall through to the
            // insert path and raise 23505, breaking PostgreSQL's guarantee that
            // ON CONFLICT DO UPDATE yields an atomic insert-or-update outcome
            // even under high concurrency.
            let refreshed;
            let mutation = if !write_ctx.repeatable_read
                && holder.xmin != write_ctx.xid
                && !snapshot_can_see(write_ctx.snapshot, holder.xmin)
            {
                refreshed = write_ctx.procarray.snapshot();
                MutationContext {
                    snapshot: &refreshed,
                    ..write_ctx.mutation()
                }
            } else {
                write_ctx.mutation()
            };
            let Some((cur_rowid, cur_key_xid, cur_xmin, cur_cmin, _cur_cmax, cur_row)) =
                eval_plan_qual(
                    &mutation,
                    table,
                    holder.rowid,
                    crate::scope::GeneratedReads::every(),
                )?
            else {
                // Concurrently deleted: re-arbitrate without it.
                discarded.insert(holder.rowid);
                continue 'arbitration;
            };
            if indexed_values(table, index, &cur_row)? != values {
                // The row under the lock no longer carries the arbiter key.
                discarded.insert(holder.rowid);
                continue 'arbitration;
            }
            return Ok(InsertRowPlan::Update {
                rowid: cur_rowid,
                cur_key_xid,
                cur_xmin,
                cur_cmin,
                cur_row,
            });
        }
        return Ok(InsertRowPlan::Insert);
    }
}

/// One locked row's in-place replacement: the version this write operates on
/// (`cur_key_xid`/`cur_xmin`/`cur_row`, as returned by [`eval_plan_qual`]) and
/// the post-image to store.
pub(super) struct LockedRowUpdate<'a> {
    pub(super) rowid: u64,
    pub(super) cur_key_xid: u64,
    pub(super) cur_xmin: u64,
    pub(super) cur_cmin: u32,
    pub(super) cur_row: &'a [Datum],
    pub(super) next: &'a [Datum],
}
