//! COPY FROM write execution.

use super::*;

/// The relation a `COPY … FROM` loads into, and the column list it fills.
///
/// `PostgreSQL`'s grammar has no `COPY ( <query> ) FROM` spelling, so the target
/// of a load is always a relation; carrying just those two fields keeps the
/// write paths free of the statement's framing options, which are already
/// resolved by the time rows reach them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CopyIntoTarget<'a> {
    pub(crate) name: &'a crabka_pgparser::ast::RelationRef,
    pub(crate) columns: &'a Option<Vec<String>>,
}

pub(crate) async fn execute_copy_write(
    write_ctx: &WriteContext<'_>,
    target: CopyIntoTarget<'_>,
    rows: &[crate::copyfmt::CopyRow<'_>],
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let catalog_kv = write_ctx.catalog_kv;
    let kv = write_ctx.kv;
    let seq = write_ctx.seq;
    let snapshot_xid = write_ctx.xid;
    let ctx = write_ctx.eval_ctx;
    let resolution = ctx.resolution();
    let mut ops = Vec::new();
    let table = crabka_pgcatalog::get_table(
        catalog_kv,
        &resolve_relation(
            catalog_kv,
            resolution,
            target.name,
            SchemaDisposition::Utility,
        )?,
    )?;
    // Hoisted out of the row loop: the target's index set is the same for every
    // row, so reading it per row was a catalog round trip per row. A row that
    // routes to a partition leaf belongs to a different relation, so that case
    // — and only that case — reads again below.
    let parent_indexes = writable_local_indexes(catalog_kv, &table)?;
    // The relation the COPY named governs every routed row, exactly as it does
    // for a partitioned INSERT. `COPY … FROM` into a relation under row
    // security is refused before the statement runs (see the session's copy-in
    // start), so this check is the second line rather than the first.
    // Hoisted for the same reason as the index set, and cached per routed leaf
    // below: one resolution per relation, never one per row.
    let parent_fk = crate::fk::StatementFkContext::resolve(catalog_kv, &table)?;
    let mut leaf_fk: HashMap<TableId, crate::fk::StatementFkContext> = HashMap::new();
    let mut writes =
        StatementWrites::for_command(write_ctx.command_row_claims, write_ctx.trigger_write);
    let target_idx = resolve_copy_targets(&table, target.columns)?;
    let copied_columns = WriteContext::modified_columns(&table, &target_idx);
    let copy_check = write_ctx.row_check(
        &table,
        crabka_pgcatalog::policy::PolicyCommand::Insert,
        &copied_columns,
    )?;
    let n_rows = rows.len() as u64;
    crate::trigger::fire_statement(
        catalog_kv,
        &table,
        crate::trigger::DmlEvent::Insert,
        crabka_pgcatalog::trigger::TriggerTiming::Before,
        &[],
        ctx,
    )?;
    if n_rows == 0 {
        crate::trigger::fire_statement(
            catalog_kv,
            &table,
            crate::trigger::DmlEvent::Insert,
            crabka_pgcatalog::trigger::TriggerTiming::After,
            &[],
            ctx,
        )?;
        return Ok((command("COPY 0"), ops));
    }
    let (start, seq_op) = seq.alloc(kv, table.id, n_rows)?;
    if let Some(op) = seq_op {
        ops.push(op);
    }
    let partitioned = crate::partition::is_partitioned(catalog_kv, &table.name)?;
    let mut copied = 0_u64;
    // The relation every `CONTEXT` line names, which is the one the statement
    // copied into even for a row that routes to a partition leaf.
    let copied_into = table.name.name.clone();
    // Each of these borrows nothing, so it is one branch and one clone of a
    // short name per *failing* row and nothing at all per successful one.
    let at_line = |error, row: &crate::copyfmt::CopyRow<'_>| {
        copy_row_context(
            error,
            &copied_into,
            row,
            crate::copyfmt::CopyContext::Line { raw: row.raw },
        )
    };
    let at_line_number = |error, row: &crate::copyfmt::CopyRow<'_>| {
        copy_row_context(
            error,
            &copied_into,
            row,
            crate::copyfmt::CopyContext::LineNumber,
        )
    };
    for (rowid, row) in (start..).zip(rows.iter()) {
        copy_row_width(&table, &target_idx, row)?;
        let full = build_copy_row(&table, &target_idx, row, ctx)?;
        // COPY into a partitioned parent routes each row exactly as INSERT
        // does; the reserved rowid block belongs to the parent, so a routed row
        // takes one from its own leaf instead.
        let (table, rowid, full, routed) = if partitioned {
            let (leaf, leaf_row) =
                route_row_to_leaf(write_ctx, &table, &full).map_err(|error| at_line(error, row))?;
            let (leaf_rowid, seq_op) = seq.alloc(kv, leaf.id, 1)?;
            ops.extend(seq_op);
            (leaf, leaf_rowid, leaf_row, true)
        } else {
            check_partition_constraint(write_ctx, &table, &full, &copied_columns)
                .map_err(|error| at_line(error, row))?;
            (table.clone(), rowid, full, false)
        };
        let Some(full) = crate::trigger::fire_before_row(
            catalog_kv,
            crate::trigger::WriteTarget {
                table: &table,
                check: &copy_check,
            },
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(full),
            ctx,
        )
        .map_err(|error| at_line(error, row))?
        else {
            continue;
        };
        let routed_indexes = if routed {
            Some(writable_local_indexes(catalog_kv, &table)?)
        } else {
            None
        };
        let local_indexes = routed_indexes.as_deref().unwrap_or(&parent_indexes);
        let fk_ctx = if routed {
            match leaf_fk.entry(table.id) {
                std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(crate::fk::StatementFkContext::resolve(catalog_kv, &table)?)
                }
            }
        } else {
            &parent_fk
        };
        // A duplicate key is raised where PostgreSQL raises it — at the point
        // the row reaches the index — and by then its line buffer has been
        // flushed, so the context is the line *number* with no line quoted.
        enforce_unique_local_indexes(write_ctx, &table, local_indexes, rowid, &full, &mut writes)
            .await
            .map_err(|error| at_line_number(error, row))?;
        if !fk_ctx.is_empty() {
            writes.fk_checks.after_insert(fk_ctx, rowid, &full)?;
        }
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(table.id, rowid, snapshot_xid),
            value: encode_table_tuple(
                &table,
                snapshot_xid,
                crabka_pgmvcc::xid::INVALID_XID,
                write_ctx.command_id,
                0,
                &full,
            ),
        });
        ops.extend(local_index_entry_ops(&table, local_indexes, rowid, &full)?);
        crate::trigger::fire_after_row(
            catalog_kv,
            &table,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(&full),
            ctx,
        )?;
        copied += 1;
    }
    // `COPY` is one command, so its referential checks fire once, after every
    // row is staged — the same timing an `INSERT` of the same rows would give.
    let fk_ops = drain_statement_fk_checks(write_ctx, &mut writes, &ops).await?;
    ops.extend(fk_ops);
    drain_statement_unique_checks(write_ctx, &mut writes, &ops)?;
    crate::trigger::fire_statement(
        catalog_kv,
        &table,
        crate::trigger::DmlEvent::Insert,
        crabka_pgcatalog::trigger::TriggerTiming::After,
        &[],
        ctx,
    )?;
    Ok((command(&format!("COPY {copied}")), ops))
}

pub(crate) fn execute_timestamp_copy_write(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    seq: &crate::seq::SequenceManager,
    target: CopyIntoTarget<'_>,
    rows: &[crate::copyfmt::CopyRow<'_>],
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let resolution = ctx.resolution();
    let table = crabka_pgcatalog::get_table(
        catalog_kv,
        &resolve_relation(
            catalog_kv,
            resolution,
            target.name,
            SchemaDisposition::Utility,
        )?,
    )?;
    if !table_uses_global_visibility(&table) {
        return Err(ExecError::Unsupported(
            "timestamp COPY requires a sharded table".into(),
        ));
    }
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?;
    if indexes.iter().any(|index| {
        index.placement == crabka_pgcatalog::IndexPlacement::Local
            || (index.placement == crabka_pgcatalog::IndexPlacement::Global && index.unique)
    }) {
        return Err(ExecError::Unsupported(
            "COPY index maintenance for sharded tables is not supported".into(),
        ));
    }
    let global_indexes = indexes
        .iter()
        .filter(|index| index.placement == crabka_pgcatalog::IndexPlacement::Global)
        .collect::<Vec<_>>();
    let target_idx = resolve_targets(&table, target.columns)?;
    let n_rows = rows.len() as u64;
    if n_rows == 0 {
        return Ok(TimestampWritePlan {
            result: command("COPY 0"),
            writes: Vec::new(),
            fresh_rowid_writes: Vec::new(),
            commit_ops: Vec::new(),
        });
    }
    let (start, seq_op) = seq.alloc(kv, table.id, n_rows)?;
    let mut writes = Vec::with_capacity(rows.len());
    for (rowid, copied) in (start..).zip(rows.iter()) {
        copy_row_width(&table, &target_idx, copied)?;
        let mut row = build_copy_row(&table, &target_idx, copied, ctx)?;
        // A sharded `COPY` fires no row trigger at all, so the settle that
        // [`crate::trigger::fire_before_row`] performs everywhere else is
        // spelled here — under the same `CONTEXT` line the local path gives a
        // row its constraints reject.
        finish_written_row(&table, &mut row, ctx).map_err(|error| {
            copy_row_context(
                error,
                &table.name.name,
                copied,
                crate::copyfmt::CopyContext::Line { raw: copied.raw },
            )
        })?;
        writes.push(TimestampWrite {
            table_id: table.id,
            bucket: hash_bucket_for_row(&table, &row)?,
            rowid,
            global_index_intents: global_index_intents_for_row(
                &table,
                &global_indexes,
                rowid,
                &row,
            )?,
            row,
            delete: false,
        });
    }
    Ok(TimestampWritePlan {
        result: command(&format!("COPY {n_rows}")),
        fresh_rowid_writes: (0..writes.len()).collect(),
        writes,
        commit_ops: seq_op.into_iter().collect(),
    })
}
