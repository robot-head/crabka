//! Locked-row heap writes and local index entry generation.

use super::*;

pub(super) async fn apply_locked_row_update(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    fk: &crate::fk::StatementFkContext,
    update: &LockedRowUpdate<'_>,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<u64, ExecError> {
    let LockedRowUpdate {
        rowid,
        cur_key_xid,
        cur_xmin,
        cur_cmin,
        cur_row,
        next,
    } = *update;
    enforce_not_null(table, next, write_ctx.eval_ctx)?;
    enforce_unique_local_index_updates(
        write_ctx,
        table,
        local_indexes,
        rowid,
        cur_row,
        next,
        writes,
    )
    .await?;
    // Append only — the probe needs the KV and the lock manager, and it must not
    // run until the statement's rows exist. A side whose key is unchanged queues
    // nothing, which is what keeps a non-key update of a hot parent row off the
    // key lock entirely.
    if !fk.is_empty() {
        writes.fk_checks.after_update(fk, rowid, cur_row, next)?;
    }
    // Whatever keys the superseded version held and this one does not are free
    // for a later part of the same statement to claim.
    writes.release_row_keys(table, local_indexes, rowid, cur_row, Some(next))?;
    let xid = write_ctx.xid;
    let (new_rowid, sequence_op) = write_ctx.seq.alloc(write_ctx.kv, table.id, 1)?;
    ops.extend(sequence_op);
    writes.retarget_unique_checks(table, rowid, new_rowid);
    // The old physical tuple keeps its identity and receives this command's
    // xmax/cmax. The new version gets an identity at the heap tail, even when
    // both versions belong to this transaction.
    ops.push(crabka_pgkv::WriteOp::Put {
        key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, cur_key_xid),
        value: encode_table_tuple_with_update_target(
            table,
            cur_xmin,
            xid,
            cur_cmin,
            write_ctx.command_id,
            new_rowid,
            cur_row,
        ),
    });
    ops.push(crabka_pgkv::WriteOp::Put {
        key: crabka_pgkv::key::update_target_key(table.id, rowid),
        value: new_rowid.to_be_bytes().to_vec(),
    });
    ops.push(crabka_pgkv::WriteOp::Put {
        key: crabka_pgmvcc::version::version_key_xid(table.id, new_rowid, xid),
        value: encode_table_tuple(
            table,
            xid,
            crabka_pgmvcc::xid::INVALID_XID,
            write_ctx.command_id,
            0,
            next,
        ),
    });
    ops.extend(local_index_entry_ops(
        table,
        local_indexes,
        new_rowid,
        next,
    )?);
    // Opportunistic per-rowid chain pruning (local engines only):
    // we hold this row's exclusive lock and just re-read its chain,
    // so reclaim its dead versions in the same commit batch. The
    // versions this statement writes (`cur_xmin`, `xid`) are never
    // pruned, and `next`'s indexed values count as survivors.
    if let Some(horizon) = write_ctx.prune_horizon {
        ops.extend(
            prune_rowid_chain_ops(
                write_ctx.kv,
                table,
                local_indexes,
                &ChainPruneRequest {
                    rowid,
                    horizon,
                    keep_xids: &[cur_key_xid],
                    new_row: Some(next),
                    freeze_below: None,
                },
            )?
            .ops,
        );
        // An UPDATE moves its successor to a new physical rowid. Reclaim the
        // previous physical tuple once a later update has made its deleter
        // old enough; otherwise each move leaves an unreachable one-row chain.
        // ponytail: table scan; add a reverse update-target index if large-table
        // update churn makes this measurable.
        for predecessor in update_predecessors(write_ctx.kv, table.id, rowid)? {
            ops.extend(
                prune_rowid_chain_ops(
                    write_ctx.kv,
                    table,
                    local_indexes,
                    &ChainPruneRequest {
                        rowid: predecessor,
                        horizon,
                        keep_xids: &[],
                        new_row: None,
                        freeze_below: None,
                    },
                )?
                .ops,
            );
        }
    }
    Ok(new_rowid)
}

/// Physical tuples whose committed update target is `rowid`.
fn update_predecessors(
    kv: &dyn crabka_pgkv::Kv,
    table_id: u32,
    rowid: u64,
) -> Result<Vec<u64>, ExecError> {
    let mut predecessors = std::collections::BTreeSet::new();
    for (key, value) in kv.scan_prefix(&crabka_pgkv::key::table_prefix(table_id))? {
        let Some((_, predecessor, _)) = crabka_pgkv::key::primary_version_of(&key) else {
            continue;
        };
        let (_, _, _, _, _, target) =
            crabka_pgmvcc::version::decode_tuple_with_command_ids_and_update_target(&value)?;
        if target == Some(rowid) {
            predecessors.insert(predecessor);
        }
    }
    Ok(predecessors.into_iter().collect())
}

/// One locked row's tombstone: the version this delete operates on, exactly as
/// [`eval_plan_qual`] returned it.
pub(super) struct LockedRowDelete<'a> {
    pub(super) rowid: u64,
    pub(super) cur_key_xid: u64,
    pub(super) cur_xmin: u64,
    pub(super) cur_cmin: u32,
    pub(super) cur_row: &'a [Datum],
}

/// Stage the writes that delete a locked row.
///
/// Those are the unique keys it frees, the MVCC tombstone, and opportunistic
/// chain pruning. `DELETE` and a cascaded `ON DELETE CASCADE` share this. Their
/// stored-row mutation is identical once the row is locked and re-read.
///
/// Queues no referential check of its own: the caller knows whether this delete
/// is the statement's (which queues through [`crate::fk::FkCheckQueue`]) or a
/// referential action's (whose follow-on checks the drain derives itself).
pub(super) fn apply_locked_row_delete(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    delete: &LockedRowDelete<'_>,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<(), ExecError> {
    let LockedRowDelete {
        rowid,
        cur_key_xid,
        cur_xmin,
        cur_cmin,
        cur_row,
    } = *delete;
    let xid = write_ctx.xid;
    // The row's unique keys are free for a later part of this statement to
    // claim, even though its superseded version is still in the KV for the probe
    // to find.
    writes.release_row_keys(table, local_indexes, rowid, cur_row, None)?;
    if cur_xmin == xid {
        // Deleting my own uncommitted version: PostgreSQL stamps xmax=xid so it
        // is invisible to me. version_key is the same key; overwrite it with
        // xmax set.
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, xid),
            value: encode_table_tuple(table, xid, xid, cur_cmin, write_ctx.command_id, cur_row),
        });
    } else {
        // Set xmax = my xid on the matched version (keep its row bytes),
        // targeting its PHYSICAL key — see `apply_locked_row_update`.
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, cur_key_xid),
            value: encode_table_tuple(
                table,
                cur_xmin,
                xid,
                cur_cmin,
                write_ctx.command_id,
                cur_row,
            ),
        });
    }
    // Opportunistic per-rowid chain pruning (local engines only). The tombstoned
    // current version survives (its xmax is our in-progress xid), so its index
    // entries stay; an engine-level `vacuum` reclaims the chain once the delete
    // commits below a future horizon.
    if let Some(horizon) = write_ctx.prune_horizon {
        ops.extend(
            prune_rowid_chain_ops(
                write_ctx.kv,
                table,
                local_indexes,
                &ChainPruneRequest {
                    rowid,
                    horizon,
                    keep_xids: &[cur_key_xid, xid],
                    new_row: None,
                    freeze_below: None,
                },
            )?
            .ops,
        );
    }
    Ok(())
}

/// One `ON CONFLICT DO UPDATE` application: the clause's assignments and filter,
/// the locked stored row they run against, and the proposed row bound as
/// `excluded`.
pub(super) struct ConflictUpdate<'a> {
    pub(super) assignments: &'a [(String, Expr)],
    pub(super) filter: Option<&'a Expr>,
    pub(super) rowid: u64,
    pub(super) cur_key_xid: u64,
    pub(super) cur_xmin: u64,
    pub(super) cur_cmin: u32,
    pub(super) cur_row: &'a [Datum],
    pub(super) proposed: &'a [Datum],
}

/// Run `DO UPDATE`'s filter and assignments against a locked conflicting row and
/// stage the resulting update. Returns the post-image (for RETURNING), or `None`
/// when the `WHERE` is not true. That row is then neither inserted nor updated
/// and produces no RETURNING row, though its row and key locks stay held, as
/// PostgreSQL's do.
///
/// Both the filter and the assignment right-hand sides evaluate against
/// [`Scope::insert_conflict`] over the stored row concatenated with the proposed
/// row, so `excluded.c` reads the proposed value and `t.c` the stored one. Every
/// column name appears under both qualifiers, which makes a bare reference
/// ambiguous (42702). That is PostgreSQL's behavior, where `DO UPDATE SET
/// v = v + 1` is an error and must be written `t.v` or `excluded.v`.
pub(super) async fn apply_insert_conflict_update(
    write_ctx: &WriteContext<'_>,
    table: &Table,
    local_indexes: &[crabka_pgcatalog::Index],
    fk: &crate::fk::StatementFkContext,
    update: &ConflictUpdate<'_>,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<Option<(Vec<Datum>, u64)>, ExecError> {
    let ctx = write_ctx.eval_ctx;
    // `ON CONFLICT DO UPDATE` reaches its row through the arbiter index probe,
    // not through `write_candidate_rows`, so it has no share of the structural
    // USING gate or of the privilege test that sits beside it, and needs both
    // named here. `SELECT` on top of the `INSERT` and `UPDATE` the surrounding
    // insert path already demanded, because this arm hands the *stored* row to
    // the `DO UPDATE SET` expressions and to `RETURNING`, which is a read of it.
    crate::privilege::require(
        &write_ctx.privileges(),
        &table.name,
        &table.owner,
        crate::privilege::RelationKind::Table,
        crate::privilege::Privilege::Select,
    )?;
    // PostgreSQL raises rather than skipping: quietly declining to update a row
    // the caller has just been told exists would itself disclose the row's
    // existence, and the caller would see neither an insert nor an update.
    crate::rls::RowSecurityCheck::compile(
        &write_ctx.policy_read_ctx(),
        table,
        crabka_pgcatalog::policy::PolicyCommand::Update,
        crate::rls::CheckSubject::TargetRow,
    )?
    .permit_row(table, update.cur_row, ctx)?;
    let scope = Scope::insert_conflict(table);
    let mut bindings = update.cur_row.to_vec();
    // The `excluded` half is the row the INSERT proposed, and it has been
    // through `finish_written_row`, which blanks every `VIRTUAL` generated
    // column because that row is the one about to be stored. The `DO UPDATE`
    // assignments and `WHERE` read it as a row rather than as storage, so its
    // virtual columns are materialized here — on this copy alone. Expanding
    // `update.proposed` itself would put a computed value into the row a
    // fall-through to the insert path writes. `PostgreSQL` draws the same line:
    // its rewriter expands `EXCLUDED.<virtual column>` into the generation
    // expression and leaves the tuple it proposes untouched.
    //
    // Every column, not the statement's own set: the target half beside it is
    // read back by `eval_plan_qual` under
    // [`crate::scope::GeneratedReads::every`], and one half of a scope narrower
    // than the other would answer `t.c` and `excluded.c` differently.
    let mut excluded = update.proposed.to_vec();
    expand_virtual_generated_row(
        table,
        &mut excluded,
        ctx,
        crate::scope::GeneratedReads::every(),
    )?;
    bindings.extend_from_slice(&excluded);
    if !row_matches(update.filter, &scope, &bindings, ctx)? {
        return Ok(None);
    }
    let mut next = update.cur_row.to_vec();
    for (column, expr) in update.assignments {
        // Assignment targets are unqualified column names of the target table,
        // resolved exactly as the UPDATE arm resolves its own (42703 on miss).
        let idx = table
            .column_index(column)
            .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
        let value = eval_assignment_value(expr, table.columns[idx].ty, &scope, &bindings, ctx)?;
        next[idx] = coerce(value, table.columns[idx].ty, ctx)?;
    }
    let updated_columns = update
        .assignments
        .iter()
        .map(|(column, _)| column.clone())
        .collect::<Vec<_>>();
    let Some(next) = crate::trigger::fire_before_row(
        write_ctx.catalog_kv,
        crate::trigger::WriteTarget {
            table,
            check: &write_ctx.row_check(
                table,
                crabka_pgcatalog::policy::PolicyCommand::Update,
                &updated_columns,
            )?,
        },
        crate::trigger::DmlEvent::Update,
        &updated_columns,
        Some(update.cur_row),
        Some(next),
        ctx,
    )?
    else {
        return Ok(None);
    };
    let new_rowid = apply_locked_row_update(
        write_ctx,
        table,
        local_indexes,
        fk,
        &LockedRowUpdate {
            rowid: update.rowid,
            cur_key_xid: update.cur_key_xid,
            cur_xmin: update.cur_xmin,
            cur_cmin: update.cur_cmin,
            cur_row: update.cur_row,
            next: &next,
        },
        writes,
        ops,
    )
    .await?;
    crate::trigger::fire_after_row(
        write_ctx.catalog_kv,
        table,
        crate::trigger::DmlEvent::Update,
        &updated_columns,
        Some(update.cur_row),
        Some(&next),
        ctx,
    )?;
    Ok(Some((next, new_rowid)))
}

pub(super) fn all_committed_snapshot() -> crabka_pgmvcc::visibility::Snapshot {
    crabka_pgmvcc::visibility::Snapshot {
        xmin: 0,
        xmax: u64::MAX,
        xip: Vec::new(),
    }
}

pub(super) fn local_index_entry_ops(
    table: &Table,
    indexes: &[crabka_pgcatalog::Index],
    rowid: u64,
    row: &[Datum],
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let mut ops = Vec::new();
    for index in indexes {
        for values in index_entries(table, index, row)? {
            ops.push(crabka_pgkv::WriteOp::Put {
                key: crabka_pgkv::key::secondary_index_entry_key(
                    table.id, index.id, &values, rowid,
                ),
                value: Vec::new(),
            });
        }
    }
    Ok(ops)
}

pub(super) fn index_entries(
    table: &Table,
    index: &crabka_pgcatalog::Index,
    row: &[Datum],
) -> Result<Vec<Vec<Datum>>, ExecError> {
    if !index_applies(table, index, row)? {
        return Ok(Vec::new());
    }
    if index.method == crabka_pgcatalog::IndexMethod::Btree {
        return indexed_values(table, index, row).map(|values| vec![values]);
    }
    if index.method != crabka_pgcatalog::IndexMethod::Gin {
        return Ok(Vec::new());
    }
    let column = table
        .column_index(&index.columns[0])
        .ok_or_else(|| ExecError::UndefinedColumn(index.columns[0].clone()))?;
    match &row[column] {
        Datum::Null => Ok(Vec::new()),
        Datum::TsVector(vector) => Ok(vector
            .0
            .iter()
            .map(|lexeme| vec![Datum::Text(lexeme.text.clone())])
            .collect()),
        got => Err(crate::func::type_error("tsvector", got)),
    }
}

pub(super) fn index_applies(
    table: &Table,
    index: &crabka_pgcatalog::Index,
    row: &[Datum],
) -> Result<bool, ExecError> {
    let Some(predicate) = &index.predicate else {
        return Ok(true);
    };
    let expression = crabka_pgparser::parser::parse_expression(predicate)?;
    Ok(crate::eval::eval(
        &expression,
        &Scope::single(table, &table.name.name),
        row,
        &crate::clock::EvalCtx::test_default(),
    )? == Datum::Bool(true))
}

pub(super) fn indexed_values(
    table: &Table,
    index: &crabka_pgcatalog::Index,
    row: &[Datum],
) -> Result<Vec<Datum>, ExecError> {
    index
        .columns
        .iter()
        .map(|column| {
            if let Some(expression) = crabka_pgcatalog::index_key_expression(column) {
                let expression = crabka_pgparser::parser::parse_expression(expression)?;
                return crate::eval::eval(
                    &expression,
                    &Scope::single(table, &table.name.name),
                    row,
                    &crate::clock::EvalCtx::test_default(),
                );
            }
            let column_index = table
                .column_index(column)
                .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
            Ok(row[column_index].clone())
        })
        .collect()
}
