//! Statement-local write state and deferred constraint checks.

use super::*;

/// The write state every part of one statement shares: the `WITH` list's
/// data-modifying items and the statement body.
///
/// `PostgreSQL` runs all of those parts as ONE command. They read one snapshot,
/// but they are not independent writers. Three rules only hold if this state is
/// per statement and not per part:
///
/// - a unique index is enforced across the whole command, so
///   `WITH i AS (INSERT INTO t VALUES (1)) INSERT INTO t VALUES (1)` is 23505;
/// - a row one part modified is never modified again by another (`UPDATE` and
///   `DELETE` skip it, `MERGE` and `ON CONFLICT DO UPDATE` raise 21000);
/// - a unique key a part freed is available to a later part, because
///   `PostgreSQL`'s uniqueness check ignores a tuple its own command superseded.
#[derive(Debug, Default)]
pub(super) struct StatementWrites {
    /// Unique-index keys claimed by rows this statement staged. They live only
    /// in the pending op batch, which a KV probe cannot see.
    pub(super) pending_unique_keys: HashSet<PendingUniqueKey>,
    /// Exclusion keys staged by this statement, which are not visible in KV
    /// until the statement's batch commits.
    pub(super) pending_exclusion_keys: HashMap<crabka_pgcatalog::IndexId, Vec<(u64, Vec<Datum>)>>,
    /// `(index, rowid)` pairs whose key this statement freed — a deleted row, or
    /// an updated row whose indexed values changed. A row holds exactly one key
    /// per index, so the rowid identifies the freed key. The superseded version
    /// is still in the KV, so the probe still finds it and must discount it.
    pub(super) released_unique_keys: HashSet<(crabka_pgcatalog::IndexId, u64)>,
    /// Every `(table, rowid)` this statement has already updated or deleted,
    /// whether by its own DML or by a referential action.
    pub(super) row_claims: HashSet<(TableId, u64)>,
    /// The outer command's claims, shared only with PL/pgSQL trigger SQL that
    /// re-enters through a fresh write actor.
    pub(super) command_row_claims: Option<CommandRowClaims>,
    pub(super) trigger_write: bool,
    /// Which `(table, rowid, constraint)` triples a referential action has
    /// already written. See [`StatementWrites::claim_row_for_action`].
    pub(super) action_claims: HashSet<(TableId, u64, String)>,
    /// Rule actions without `OLD`/`NEW` are statement actions.  The row-rule
    /// executor visits each source row, so this records the ones it has
    /// already run for the command.
    pub(super) statement_rule_actions: HashSet<(u32, usize)>,
    /// The referential checks this statement owes, appended by the write hooks
    /// and drained once, after the `WITH` list AND the body, because
    /// `PostgreSQL` treats the whole command as one trigger-firing unit.
    pub(super) fk_checks: crate::fk::FkCheckQueue,
    /// The uniqueness rechecks a `DEFERRABLE` `PRIMARY KEY` or `UNIQUE`
    /// constraint owes, appended as each row claims its key and drained once
    /// the whole command is done — which is the check point `DEFERRABLE
    /// INITIALLY IMMEDIATE` names, and the moment an `INITIALLY DEFERRED` one
    /// is promoted to the transaction's queue.
    pub(super) unique_checks: Vec<crate::fk::PendingUniqueCheck>,
    /// The relations a `TRUNCATE` is emptying, empty for every other statement.
    ///
    /// `TRUNCATE` desugars to one unfiltered `DELETE` per relation, and those
    /// deletes must not fire `ON DELETE CASCADE`: `PostgreSQL`'s `TRUNCATE`
    /// refuses a child outside the set and `CASCADE` widens the *set* instead.
    /// Carried here so the desugared `DELETE` can suppress exactly the
    /// parent-side keys whose child is being truncated too.
    pub(super) truncate_set: BTreeSet<TableId>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RowClaim {
    Claimed,
    Statement,
    Trigger(CommandOperation),
}

impl StatementWrites {
    pub(super) fn for_command(
        command_row_claims: Option<&CommandRowClaims>,
        trigger_write: bool,
    ) -> Self {
        Self {
            command_row_claims: command_row_claims.cloned(),
            trigger_write,
            ..Self::default()
        }
    }

    pub(super) fn claim_statement_rule_action(
        &mut self,
        rule_oid: u32,
        action_index: usize,
    ) -> bool {
        self.statement_rule_actions.insert((rule_oid, action_index))
    }

    /// Claim a row for the command's own DML. `false` means anything else in
    /// this statement already modified it, which is what stops this part from
    /// modifying it a second time.
    ///
    /// `PostgreSQL`'s "a command modifies a given row at most once" rule is
    /// about the command's own `ModifyTable` nodes, and every one of them runs
    /// before the trigger queue does, so a referential action can never be what
    /// this refuses.
    pub(super) fn claim_row(
        &mut self,
        table: TableId,
        rowid: u64,
        xmin: u64,
        operation: CommandOperation,
    ) -> RowClaim {
        let row = (table, rowid);
        if !self.row_claims.insert(row) {
            return RowClaim::Statement;
        }
        let Some(command_row_claims) = &self.command_row_claims else {
            return RowClaim::Claimed;
        };
        let mut command_row_claims = command_row_claims.lock().expect("command row claims");
        let version = (table, rowid, xmin);
        if let Some(previous) = command_row_claims.operations.get_mut(&version) {
            self.row_claims.remove(&row);
            if *previous == operation {
                RowClaim::Trigger(operation)
            } else if self.trigger_write {
                *previous = operation;
                command_row_claims.row_operations.insert(row, operation);
                RowClaim::Claimed
            } else {
                RowClaim::Trigger(*previous)
            }
        } else if !self.trigger_write
            && let Some(previous) = command_row_claims.row_operations.get(&row)
        {
            self.row_claims.remove(&row);
            RowClaim::Trigger(*previous)
        } else {
            command_row_claims.operations.insert(version, operation);
            command_row_claims.row_operations.insert(row, operation);
            RowClaim::Claimed
        }
    }

    pub(super) fn trigger_replaced_claim(
        &self,
        table: TableId,
        rowid: u64,
        xmin: u64,
        operation: CommandOperation,
    ) -> bool {
        self.command_row_claims.as_ref().is_some_and(|claims| {
            claims
                .lock()
                .expect("command row claims")
                .operations
                .get(&(table, rowid, xmin))
                .is_some_and(|current| *current != operation)
        })
    }

    /// Claim a row for one constraint's referential action. `false` means *that
    /// constraint's* action has already written this row.
    ///
    /// A referential action is not one of the command's `ModifyTable` nodes: it
    /// runs as a separate query the trigger queue issues, so it reaches a row
    /// the command itself already modified, and so does a *second* constraint's
    /// action reach a row the first one has just rewritten. This is how one
    /// `DELETE` of a doubly-referenced parent key nulls both referencing
    /// columns and not one.
    ///
    /// This refuses one constraint that comes back around to a row its own
    /// action already wrote. The drain folds each action's ops into the view it
    /// reads, so a cascade cycle already terminates on the data exactly as
    /// `PostgreSQL`'s does. A deleted row reads as gone, and a re-keyed one no
    /// longer matches. This bounds the work at one write per
    /// `(row, constraint)` whatever the data does.
    pub(super) fn claim_row_for_action(
        &mut self,
        table: TableId,
        rowid: u64,
        constraint: &str,
    ) -> bool {
        if !self
            .action_claims
            .insert((table, rowid, constraint.to_string()))
        {
            return false;
        }
        self.row_claims.insert((table, rowid));
        true
    }

    /// Has anything in this command already modified this row? The predicate
    /// `ON CONFLICT DO UPDATE` raises 21000 on, where *what* touched the row
    /// makes no difference. The upsert may not be the second thing to reach it.
    pub(super) fn is_claimed(&self, table: TableId, rowid: u64) -> bool {
        self.row_claims.contains(&(table, rowid))
    }

    /// Does the probe's `holder` still hold the key it was found under, or did
    /// an earlier part of this statement free it?
    pub(super) fn holder_still_holds(&self, index: crabka_pgcatalog::IndexId, rowid: u64) -> bool {
        !self.released_unique_keys.contains(&(index, rowid))
    }

    /// Record the unique keys `rowid` gives up: every one when the row is
    /// deleted (`next` is `None`), and only the ones whose indexed values change
    /// when it is updated.
    pub(super) fn release_row_keys(
        &mut self,
        table: &Table,
        indexes: &[crabka_pgcatalog::Index],
        rowid: u64,
        old_row: &[Datum],
        next: Option<&[Datum]>,
    ) -> Result<(), ExecError> {
        for index in indexes.iter().filter(|index| index.unique) {
            let old_values = indexed_values(table, index, old_row)?;
            if let Some(next) = next
                && indexed_values(table, index, next)? == old_values
            {
                continue;
            }
            self.released_unique_keys.insert((index.id, rowid));
        }
        Ok(())
    }

    /// An UPDATE's replacement tuple lives at a new heap rowid. Deferrable
    /// unique checks are queued before allocation, so retarget their proof to
    /// the tuple that now owns the new key.
    pub(super) fn retarget_unique_checks(&mut self, table: &Table, old_rowid: u64, new_rowid: u64) {
        for check in &mut self.unique_checks {
            if check.table == table.name && check.rowid == old_rowid {
                check.rowid = new_rowid;
            }
        }
    }
}

impl<'a> WriteContext<'a> {
    /// The context the foreign-key drain probes and scans through.
    ///
    /// The row store it reads is `staged`, this statement's write batch layered
    /// over the real one. The drain's whole premise is that the statement's rows
    /// already exist, and they only reach the KV when the session commits the
    /// batch.
    fn fk_exec<'b>(&'b self, staged: &'b StagedKv<'b>) -> crate::fk::FkExecContext<'b>
    where
        'a: 'b,
    {
        crate::fk::FkExecContext {
            catalog_kv: self.catalog_kv,
            kv: staged,
            global: self.global,
            global_snapshot: self.global_snapshot,
            snapshot: self.snapshot,
            xid: self.xid,
            eval_ctx: self.eval_ctx,
        }
    }

    /// Move the transaction's deferred-check store out of its mutex for the
    /// duration of a drain, which needs it by `&mut` across `await` points. The
    /// lock is held only for the swap, never across an await; the store is put
    /// back by [`WriteContext::restore_deferred_fk`] on every exit path.
    fn take_deferred_fk(&self) -> Option<crate::fk::DeferredConstraints> {
        self.deferred_fk
            .map(|store| std::mem::take(&mut *store.lock().expect("deferred constraints mutex")))
    }

    fn restore_deferred_fk(&self, store: Option<crate::fk::DeferredConstraints>) {
        if let Some(slot) = self.deferred_fk
            && let Some(store) = store
        {
            *slot.lock().expect("deferred constraints mutex") = store;
        }
    }
}

/// Both sides of a foreign key name the same lock identity, the referenced
/// index's entry prefix. That is exactly what the uniqueness check already
/// locks, so this is a [`crate::lockmgr::LockKey::UniqueKey`] acquire in the
/// row-lock manager and no new lock mode exists.
///
/// The child side takes it SHARED, so many rows that reference one parent key
/// never convoy. The parent side takes it EXCLUSIVE, because it removes or
/// moves the key. Key locks and row locks share one wait-for graph, so the
/// engine still reports a cycle across both as 40P01, and it releases both
/// together at COMMIT/ROLLBACK.
impl crate::fk::FkKeyLocks for WriteContext<'_> {
    async fn lock_key(&self, key: Vec<u8>, mode: crate::fk::FkLockMode) -> Result<(), ExecError> {
        let mode = match mode {
            crate::fk::FkLockMode::Shared => crate::lockmgr::LockMode::Shared,
            crate::fk::FkLockMode::Exclusive => crate::lockmgr::LockMode::Exclusive,
        };
        self.lockmgr
            .acquire_key_as(
                crate::lockmgr::LockKey::UniqueKey(key),
                mode,
                self.lock_owner,
                self.lock_wait_cap,
            )
            .await
            .map_err(lock_acquire_error)
    }
}

/// The write path a referential action re-enters, over the *outer statement's*
/// [`StatementWrites`].
///
/// `PostgreSQL` runs each referential action as its own query over the row's
/// current image, so neither an earlier `UPDATE` in the same command nor another
/// constraint's action exempts a child row from the action its parent's deletion
/// fires. Each action's ops are folded straight back into the staged view before
/// it returns, which is what makes "current image" true here: the next action to
/// reach the row reads what this one wrote, and a cascade cycle terminates
/// because the row it comes back to reads as deleted or off-key.
///
/// [`claim_row_for_action`] then only bounds the work, and refuses one
/// constraint a second write of the same row.
///
/// [`claim_row_for_action`]: StatementWrites::claim_row_for_action
struct StatementCascade<'a, 'w> {
    write_ctx: &'a WriteContext<'w>,
    writes: &'a mut StatementWrites,
    /// The view both this and the drain read through: the statement's pending
    /// batch over the store, grown by every op an action produces. An action
    /// re-reads its row here rather than in the store, so it changes the image
    /// the command, or an earlier action, last wrote.
    staged: &'a StagedKv<'a>,
    /// One index-set read per cascaded *relation*, not per cascaded row: a
    /// cascade walks a chain of relations and revisits each many times.
    indexes: HashMap<TableId, Vec<crabka_pgcatalog::Index>>,
}

impl crate::fk::FkCascade for StatementCascade<'_, '_> {
    fn begin_action(
        &mut self,
        table: &Table,
        delete: bool,
        updated: &[usize],
    ) -> Result<(), ExecError> {
        let event = if delete {
            crate::trigger::DmlEvent::Delete
        } else {
            crate::trigger::DmlEvent::Update
        };
        let columns = updated
            .iter()
            .filter_map(|index| table.columns.get(*index))
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        crate::trigger::fire_statement(
            self.write_ctx.catalog_kv,
            table,
            event,
            crabka_pgcatalog::trigger::TriggerTiming::Before,
            &columns,
            self.write_ctx.eval_ctx,
        )
    }

    fn end_action(
        &mut self,
        table: &Table,
        delete: bool,
        updated: &[usize],
    ) -> Result<(), ExecError> {
        let event = if delete {
            crate::trigger::DmlEvent::Delete
        } else {
            crate::trigger::DmlEvent::Update
        };
        let columns = updated
            .iter()
            .filter_map(|index| table.columns.get(*index))
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        crate::trigger::fire_statement(
            self.write_ctx.catalog_kv,
            table,
            event,
            crabka_pgcatalog::trigger::TriggerTiming::After,
            &columns,
            self.write_ctx.eval_ctx,
        )
    }

    async fn modify_row(
        &mut self,
        request: crate::fk::FkCascadeRequest<'_>,
    ) -> Result<(crate::fk::FkCascadeOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
        let crate::fk::FkCascadeRequest {
            table,
            rowid,
            change,
            constraint,
        } = request;
        // Split the borrows: the index cache and the statement's write
        // bookkeeping are both reached mutably from the same `&mut self`.
        let Self {
            write_ctx,
            writes,
            staged,
            indexes,
        } = self;
        let write_ctx = *write_ctx;
        let staged = *staged;
        let ctx = write_ctx.eval_ctx;
        let mut ops = Vec::new();
        let local_indexes = match indexes.entry(table.id) {
            std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(writable_local_indexes(write_ctx.catalog_kv, table)?)
            }
        };
        write_ctx
            .lockmgr
            .acquire_as(
                table.id,
                rowid,
                crate::lockmgr::LockMode::Exclusive,
                write_ctx.lock_owner,
                write_ctx.lock_wait_cap,
            )
            .await
            .map_err(lock_acquire_error)?;
        let Some((cur_rowid, cur_key_xid, cur_xmin, cur_cmin, _cur_cmax, cur_row)) =
            eval_plan_qual(
                &write_ctx.staged_mutation(staged),
                table,
                rowid,
                crate::scope::GeneratedReads::every(),
            )?
        else {
            // Deleted by a concurrent committed transaction, or by this command's
            // own DML: nothing references the parent through this row any more.
            return Ok((crate::fk::FkCascadeOutcome::Skipped, ops));
        };
        // A row this constraint's own action already wrote. It runs before any
        // op is built, so a revisited row leaves the batch untouched as well as
        // unrecursed. A row the command's own DML — or another constraint's
        // action — modified passes, because the action is a command of its own.
        if !writes.claim_row_for_action(table.id, cur_rowid, constraint) {
            return Ok((crate::fk::FkCascadeOutcome::Skipped, ops));
        }
        let (next, updated_columns) = match change {
            crate::fk::FkRowChange::Delete => (None, Vec::new()),
            crate::fk::FkRowChange::Assign(pairs) => {
                let mut next = cur_row.clone();
                let mut updated = Vec::new();
                for (ordinal, value) in pairs {
                    let ty = cascade_column(table, ordinal)?.ty;
                    next[ordinal] = coerce(value, ty, ctx)?;
                    updated.push(table.columns[ordinal].name.clone());
                }
                (Some(next), updated)
            }
            crate::fk::FkRowChange::AssignDefaults(ordinals) => {
                let mut next = cur_row.clone();
                let mut updated = Vec::new();
                for ordinal in ordinals {
                    next[ordinal] = default_value(cascade_column(table, ordinal)?, ctx)?;
                    updated.push(table.columns[ordinal].name.clone());
                }
                (Some(next), updated)
            }
        };
        let Some(next) = next else {
            if crate::trigger::fire_before_row(
                write_ctx.catalog_kv,
                crate::trigger::WriteTarget {
                    table,
                    check: &crate::rls::WriteChecks::exempt(
                        crate::rls::CheckExemption::ReferentialAction,
                    ),
                },
                crate::trigger::DmlEvent::Delete,
                &[],
                Some(&cur_row),
                None,
                ctx,
            )?
            .is_none()
            {
                return Ok((crate::fk::FkCascadeOutcome::Skipped, ops));
            }
            apply_locked_row_delete(
                write_ctx,
                table,
                local_indexes,
                &LockedRowDelete {
                    rowid: cur_rowid,
                    cur_key_xid,
                    cur_xmin,
                    cur_cmin,
                    cur_row: &cur_row,
                },
                writes,
                &mut ops,
            )?;
            crate::trigger::fire_after_row(
                write_ctx.catalog_kv,
                table,
                crate::trigger::DmlEvent::Delete,
                &[],
                Some(&cur_row),
                None,
                ctx,
            )?;
            staged.stage(&ops);
            return Ok((crate::fk::FkCascadeOutcome::Applied { new_row: None }, ops));
        };
        let Some(next) = crate::trigger::fire_before_row(
            write_ctx.catalog_kv,
            crate::trigger::WriteTarget {
                table,
                check: &crate::rls::WriteChecks::exempt(
                    crate::rls::CheckExemption::ReferentialAction,
                ),
            },
            crate::trigger::DmlEvent::Update,
            &updated_columns,
            Some(&cur_row),
            Some(next),
            ctx,
        )?
        else {
            return Ok((crate::fk::FkCascadeOutcome::Skipped, ops));
        };
        // The follow-on checks a cascaded update owes are computed by the drain
        // from the row it returns, so this write queues none of its own.
        let no_hooks = crate::fk::StatementFkContext::default();
        apply_locked_row_update(
            write_ctx,
            table,
            local_indexes,
            &no_hooks,
            &LockedRowUpdate {
                rowid: cur_rowid,
                cur_key_xid,
                cur_xmin,
                cur_cmin,
                cur_row: &cur_row,
                next: &next,
            },
            writes,
            &mut ops,
        )
        .await?;
        crate::trigger::fire_after_row(
            write_ctx.catalog_kv,
            table,
            crate::trigger::DmlEvent::Update,
            &updated_columns,
            Some(&cur_row),
            Some(&next),
            ctx,
        )?;
        staged.stage(&ops);
        Ok((
            crate::fk::FkCascadeOutcome::Applied {
                new_row: Some(next),
            },
            ops,
        ))
    }
}

/// Run the referential checks a statement queued, once its whole `WITH` list and
/// body have staged their rows.
///
/// `PostgreSQL` fires its `AFTER ROW` trigger queue once for the whole command,
/// which is why nothing here happens inline: a `NOT DEFERRABLE` self-referencing
/// `INSERT INTO t (id, boss) VALUES (1, 1)` succeeds because the row is in place
/// by the time the check runs. A referential action re-enters the write path
/// through [`StatementCascade`], which shares this statement's
/// [`StatementWrites`]. So a cascade that comes back around to a row *an
/// action* already changed stops and does not recurse, while a row the
/// statement's own DML modified is still the action's to change.
pub(super) async fn drain_statement_fk_checks(
    write_ctx: &WriteContext<'_>,
    writes: &mut StatementWrites,
    staged: &[crabka_pgkv::WriteOp],
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    if writes.fk_checks.is_empty() {
        return Ok(Vec::new());
    }
    let mut queue = std::mem::take(&mut writes.fk_checks);
    let staged_kv = StagedKv::new(write_ctx.kv, staged);
    let exec = write_ctx.fk_exec(&staged_kv);
    let mut cascade = StatementCascade {
        write_ctx,
        writes,
        staged: &staged_kv,
        indexes: HashMap::new(),
    };
    let mut deferred = write_ctx.take_deferred_fk();
    let drained = crate::fk::drain_statement_checks(
        &exec,
        write_ctx,
        &mut cascade,
        &mut queue,
        deferred.as_mut(),
    )
    .await;
    write_ctx.restore_deferred_fk(deferred);
    drained
}

/// Run the checks a transaction deferred, at `COMMIT` or at
/// `SET CONSTRAINTS … IMMEDIATE`.
///
/// Every earlier statement's rows are in the KV under this transaction's xid by
/// now, so the drain reads storage directly (an empty staged batch) and finds
/// a re-supplied key. This is what makes `DELETE; INSERT; COMMIT`
/// succeed under a deferred `NO ACTION`. A referential action re-enters the
/// write path through the same [`StatementCascade`] the statement drain uses,
/// over write bookkeeping of its own: the statements whose rows these checks
/// describe are finished, so there is none to share.
pub(crate) async fn drain_deferred_fk_checks(
    write_ctx: &WriteContext<'_>,
    checks: Vec<crate::fk::PendingCheck>,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    if checks.is_empty() {
        return Ok(Vec::new());
    }
    let staged_kv = StagedKv::new(write_ctx.kv, &[]);
    let exec = write_ctx.fk_exec(&staged_kv);
    let mut writes = StatementWrites::default();
    let mut cascade = StatementCascade {
        write_ctx,
        writes: &mut writes,
        staged: &staged_kv,
        indexes: HashMap::new(),
    };
    let ops = crate::fk::drain_deferred_checks(&exec, write_ctx, &mut cascade, checks).await?;
    drop(cascade);
    // A referential action can put a row onto a DEFERRABLE key, and this is the
    // last check point the transaction has, so those are judged here rather
    // than queued for a statement that will never come. The overlay already
    // holds the action's own ops: the drain folds them in as it runs.
    let queued = std::mem::take(&mut writes.unique_checks);
    settle_unique_checks(write_ctx, &staged_kv, queued, None)?;
    Ok(ops)
}

/// Settle the uniqueness rechecks this command queued for its `DEFERRABLE`
/// keys.
///
/// This is the check point `PostgreSQL` gives a `DEFERRABLE INITIALLY
/// IMMEDIATE` constraint: the end of the statement, once every row it wrote
/// exists. `UPDATE unique_tbl SET i = i + 1` therefore succeeds, because by the
/// time the first row's new key is judged, the row that used to hold it has
/// moved on.
///
/// A constraint that is deferred right now — declared `INITIALLY DEFERRED`, or
/// moved there by `SET CONSTRAINTS` — is promoted to the transaction's queue
/// instead, and `COMMIT` runs it. Outside a transaction block there is nowhere
/// to promote to and nothing a later statement could repair, so everything runs
/// here.
pub(super) fn drain_statement_unique_checks(
    write_ctx: &WriteContext<'_>,
    writes: &mut StatementWrites,
    staged: &[crabka_pgkv::WriteOp],
) -> Result<(), ExecError> {
    if writes.unique_checks.is_empty() {
        return Ok(());
    }
    let queued = std::mem::take(&mut writes.unique_checks);
    let staged_kv = StagedKv::new(write_ctx.kv, staged);
    let mut store = write_ctx.take_deferred_fk();
    let outcome = settle_unique_checks(write_ctx, &staged_kv, queued, store.as_mut());
    // The store goes back whatever the outcome: a failed check aborts the
    // transaction, and the teardown is what discards the queue.
    write_ctx.restore_deferred_fk(store);
    outcome
}

/// Run each queued check whose constraint is checked now, and hand the rest to
/// the transaction's queue. Stops at the first violation, as `PostgreSQL`'s
/// after-trigger queue does.
fn settle_unique_checks(
    write_ctx: &WriteContext<'_>,
    staged: &StagedKv<'_>,
    queued: Vec<crate::fk::PendingUniqueCheck>,
    mut store: Option<&mut crate::fk::DeferredConstraints>,
) -> Result<(), ExecError> {
    for check in queued {
        let deferred = store
            .as_ref()
            .is_some_and(|store| store.modes().is_index_deferred(&check.index));
        if let (true, Some(store)) = (deferred, store.as_mut()) {
            store.defer_unique(check);
        } else {
            run_unique_recheck(write_ctx, staged, &check)?;
        }
    }
    Ok(())
}

/// Run the uniqueness rechecks a transaction deferred, at `COMMIT` or at
/// `SET CONSTRAINTS … IMMEDIATE`.
///
/// Every earlier statement's rows are in the KV under this transaction's xid by
/// now, so the drain reads storage directly. That is what makes
/// `INSERT; DELETE; COMMIT` succeed under a deferred key.
///
/// # Errors
///
/// Reports the 23505 of the first key that two live rows still hold.
pub(crate) fn drain_deferred_unique_checks(
    write_ctx: &WriteContext<'_>,
    checks: &[crate::fk::PendingUniqueCheck],
) -> Result<(), ExecError> {
    for check in checks {
        run_unique_recheck(write_ctx, write_ctx.kv, check)?;
    }
    Ok(())
}

/// Prove that the row a deferred check names still holds its key alone.
///
/// Three of the four outcomes are silence. The relation may have been dropped,
/// in which case `PostgreSQL` drops the queued events with it. The row may no
/// longer hold the key, having been deleted or moved onto another one, in which
/// case the event has nothing left to prove. Or it may hold the key by itself,
/// which is the constraint being satisfied.
fn run_unique_recheck(
    write_ctx: &WriteContext<'_>,
    kv: &dyn Kv,
    check: &crate::fk::PendingUniqueCheck,
) -> Result<(), ExecError> {
    let Ok(table) = crabka_pgcatalog::get_table(write_ctx.catalog_kv, &check.table) else {
        return Ok(());
    };
    let holders = probe_unique_key(write_ctx, kv, &table, &check.index, &check.values)?;
    if !holders.iter().any(|holder| holder.rowid == check.rowid) {
        return Ok(());
    }
    if holders.len() > 1 {
        return Err(unique_violation(
            write_ctx,
            &table,
            &check.index,
            &check.values,
        ));
    }
    Ok(())
}

/// The column a referential action names by ordinal. The ordinals come from the
/// same catalog relation the request carries, so a miss is catalog corruption.
fn cascade_column(table: &Table, ordinal: usize) -> Result<&Column, ExecError> {
    table
        .columns
        .get(ordinal)
        .ok_or_else(|| ExecError::UndefinedTableColumn {
            column: ordinal.to_string(),
            table: table.name.to_string(),
        })
}
