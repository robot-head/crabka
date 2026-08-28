//! Timestamp-transaction DML planning for sharded tables.

use super::*;

/// The role a timestamp write is authorized as.
///
/// The same `PUBLIC` → bootstrap-superuser resolution
/// [`ForeignCtx::effective_role`] applies, spelled here because this path is
/// given an [`crate::clock::EvalCtx`] and no `ForeignCtx` at all.
fn timestamp_write_role(ctx: &crate::clock::EvalCtx) -> String {
    if ctx.current_user == crabka_pgcatalog::PUBLIC_ROLE {
        crabka_pgcatalog::BOOTSTRAP_ROLE.to_string()
    } else {
        ctx.current_user.clone()
    }
}

/// Build timestamp-transaction writes for sharded-table autocommit DML.
pub(crate) fn execute_timestamp_write(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    seq: &crate::seq::SequenceManager,
    stmt: &Statement,
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let resolution = ctx.resolution();
    match stmt {
        Statement::Insert { returning, .. }
        | Statement::Update { returning, .. }
        | Statement::Delete { returning, .. }
            if returning.is_some() =>
        {
            return Err(ExecError::Unsupported(
                "RETURNING on sharded timestamp writes is not supported".into(),
            ));
        }
        _ => {}
    }
    if let Statement::Update { table, only, .. } | Statement::Delete { table, only, .. } = stmt
        && !*only
    {
        let name = resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?;
        if crate::inheritance::has_children(catalog_kv, &name)? {
            return Err(ExecError::Unsupported(format!(
                "UPDATE/DELETE on sharded table \"{name}\" is not supported while it has \\
                 inheritance children: the statement would have to write every relation below it, \\
                 and a sharded write is planned against one. Write ONLY \"{name}\", or each child, \\
                 instead"
            )));
        }
    }
    if let Statement::Insert {
        on_conflict: Some(_),
        ..
    } = stmt
    {
        return Err(ExecError::Unsupported(
            "ON CONFLICT on sharded timestamp writes is not supported".into(),
        ));
    }

    let table_name = match stmt {
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. } => table,
        _ => {
            return Err(ExecError::Unsupported(
                "not a timestamp write statement".into(),
            ));
        }
    };
    let table_name = &resolve_relation(
        catalog_kv,
        resolution,
        table_name,
        SchemaDisposition::Reference,
    )?;
    let table = crabka_pgcatalog::get_table(catalog_kv, table_name)?;
    if !table_uses_global_visibility(&table) {
        return Err(ExecError::Unsupported(
            "timestamp writes require a sharded table".into(),
        ));
    }
    crate::privilege::require(
        &crate::privilege::PrivilegeCtx::new(catalog_kv, &timestamp_write_role(ctx)),
        &table.name,
        &table.owner,
        crate::privilege::RelationKind::Table,
        crate::privilege::Privilege::for_written_row(match stmt {
            Statement::Insert { .. } => crabka_pgcatalog::policy::PolicyCommand::Insert,
            Statement::Update { .. } => crabka_pgcatalog::policy::PolicyCommand::Update,
            _ => crabka_pgcatalog::policy::PolicyCommand::Delete,
        }),
    )?;
    if table.row_security {
        return Err(ExecError::Unsupported(format!(
            "row-level security on sharded relation \"{}\" is not supported",
            table.name.name
        )));
    }
    let indexes = crabka_pgcatalog::list_table_indexes(catalog_kv, &table.name)?;
    let global_indexes: Vec<_> = indexes
        .iter()
        .filter(|index| index.placement == crabka_pgcatalog::IndexPlacement::Global)
        .collect();
    if global_indexes.iter().any(|index| index.unique) {
        return Err(ExecError::Unsupported(
            "unique global indexes are not supported until global enforcement exists".into(),
        ));
    }
    if indexes
        .iter()
        .any(|index| index.placement == crabka_pgcatalog::IndexPlacement::Local)
    {
        return Err(ExecError::Unsupported(
            "local index maintenance for sharded timestamp writes is blocked on G-6".into(),
        ));
    }

    match stmt {
        Statement::Insert {
            columns,
            indirections,
            source,
            ..
        } => {
            let crabka_pgparser::ast::InsertSource::Values(rows) = source else {
                return Err(ExecError::Unsupported(
                    "INSERT ... SELECT / DEFAULT VALUES on sharded tables is not supported".into(),
                ));
            };
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Insert,
                crabka_pgcatalog::trigger::TriggerTiming::Before,
                &[],
                ctx,
            )?;
            let plan = execute_timestamp_insert(
                catalog_kv,
                kv,
                seq,
                &table,
                &global_indexes,
                columns,
                indirections,
                rows,
                ctx,
            )?;
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Insert,
                crabka_pgcatalog::trigger::TriggerTiming::After,
                &[],
                ctx,
            )?;
            Ok(plan)
        }
        Statement::Update {
            assignments,
            from,
            filter,
            ..
        } => {
            if !from.is_empty() {
                return Err(ExecError::Unsupported(
                    "UPDATE ... FROM on sharded tables is not supported".into(),
                ));
            }
            let updated = assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect::<Vec<_>>();
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Update,
                crabka_pgcatalog::trigger::TriggerTiming::Before,
                &updated,
                ctx,
            )?;
            let plan = execute_timestamp_update(
                catalog_kv,
                kv,
                seq,
                &table,
                &global_indexes,
                assignments,
                filter.as_ref(),
                ctx,
            )?;
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Update,
                crabka_pgcatalog::trigger::TriggerTiming::After,
                &updated,
                ctx,
            )?;
            Ok(plan)
        }
        Statement::Delete { using, filter, .. } => {
            if !using.is_empty() {
                return Err(ExecError::Unsupported(
                    "DELETE ... USING on sharded tables is not supported".into(),
                ));
            }
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Delete,
                crabka_pgcatalog::trigger::TriggerTiming::Before,
                &[],
                ctx,
            )?;
            let plan = execute_timestamp_delete(
                catalog_kv,
                kv,
                &table,
                &global_indexes,
                filter.as_ref(),
                ctx,
            )?;
            crate::trigger::fire_statement(
                catalog_kv,
                &table,
                crate::trigger::DmlEvent::Delete,
                crabka_pgcatalog::trigger::TriggerTiming::After,
                &[],
                ctx,
            )?;
            Ok(plan)
        }
        _ => Err(ExecError::Unsupported(
            "this statement is not supported on sharded tables".into(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_timestamp_insert(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    seq: &crate::seq::SequenceManager,
    table: &Table,
    global_indexes: &[&crabka_pgcatalog::Index],
    columns: &Option<Vec<String>>,
    indirections: &Option<Vec<Vec<TargetIndirection>>>,
    rows: &[Vec<Expr>],
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    if rows.is_empty() {
        return Ok(TimestampWritePlan {
            result: command("INSERT 0 0"),
            writes: Vec::new(),
            fresh_rowid_writes: Vec::new(),
            commit_ops: Vec::new(),
        });
    }
    let width = rows.first().map_or(0, Vec::len);
    if rows.iter().any(|row| row.len() != width) {
        return Err(ExecError::ValuesColumnCount);
    }
    let target_idx = resolve_insert_targets(table, columns, indirections, width)?;
    let proposed_rows = rows.len() as u64;
    let (start, seq_op) = seq.alloc(kv, table.id, proposed_rows)?;
    let mut writes = Vec::with_capacity(rows.len());
    for (rowid, row_exprs) in (start..).zip(rows.iter()) {
        let full =
            build_insert_row_with_subscripts(table, &target_idx, indirections, row_exprs, ctx)?;
        let Some(full) = crate::trigger::fire_before_row(
            catalog_kv,
            crate::trigger::WriteTarget {
                table,
                check: &crate::rls::WriteChecks::exempt(
                    crate::rls::CheckExemption::ShardedRelation,
                ),
            },
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(full),
            ctx,
        )?
        else {
            continue;
        };
        let bucket = hash_bucket_for_row(table, &full)?;
        crate::trigger::fire_after_row(
            catalog_kv,
            table,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(&full),
            ctx,
        )?;
        writes.push(TimestampWrite {
            table_id: table.id,
            bucket,
            rowid,
            global_index_intents: global_index_intents_for_row(
                table,
                global_indexes,
                rowid,
                &full,
            )?,
            row: full,
            delete: false,
        });
    }
    let n_rows = writes.len();
    Ok(TimestampWritePlan {
        result: command(&format!("INSERT 0 {n_rows}")),
        fresh_rowid_writes: (0..writes.len()).collect(),
        writes,
        commit_ops: seq_op.into_iter().collect(),
    })
}

fn execute_timestamp_update(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    seq: &crate::seq::SequenceManager,
    table: &Table,
    global_indexes: &[&crabka_pgcatalog::Index],
    assignments: &[crabka_pgparser::ast::Assignment],
    filter: Option<&Expr>,
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let scope = Scope::single(table, &table.name.name);
    let targets = assignments
        .iter()
        .map(
            |assignment| match (&assignment.targets[..], &assignment.value) {
                ([column], crabka_pgparser::ast::AssignmentValue::Expr(expr)) => table
                    .column_index(column)
                    .map(|index| (index, expr))
                    .ok_or_else(|| ExecError::UndefinedColumn(column.clone())),
                _ => Err(ExecError::Unsupported(
                    "multi-column SET on sharded tables is not supported".into(),
                )),
            },
        )
        .collect::<Result<Vec<_>, ExecError>>()?;
    let rows = scan_ts_live_interval(kv, kv, table, ReadTimestamp::MAX, None, RowInterval::ALL)?;
    let filter = crate::bind::bind_optional(filter, &scope)?;
    let filter = filter.as_ref().map(crate::bind::BoundExpr::expr);
    let mut matched = Vec::new();
    for row in rows {
        if row_matches(filter, &scope, &row.row, ctx)? {
            matched.push(row);
        }
    }
    if matched.is_empty() {
        return Ok(TimestampWritePlan {
            result: command("UPDATE 0"),
            writes: Vec::new(),
            fresh_rowid_writes: Vec::new(),
            commit_ops: Vec::new(),
        });
    }
    let updated = assignments
        .iter()
        .flat_map(|assignment| assignment.targets.iter().cloned())
        .collect::<Vec<_>>();
    let mut updates = Vec::with_capacity(matched.len());
    for ScannedRow { rowid, row, .. } in matched {
        let mut next = row.clone();
        for (index, expr) in &targets {
            let value = eval_assignment_value(expr, table.columns[*index].ty, &scope, &row, ctx)?;
            next[*index] = coerce(value, table.columns[*index].ty, ctx)?;
        }
        let Some(next) = crate::trigger::fire_before_row(
            catalog_kv,
            crate::trigger::WriteTarget {
                table,
                check: &crate::rls::WriteChecks::exempt(
                    crate::rls::CheckExemption::ShardedRelation,
                ),
            },
            crate::trigger::DmlEvent::Update,
            &updated,
            Some(&row),
            Some(next),
            ctx,
        )?
        else {
            continue;
        };
        let old_bucket = hash_bucket_for_row(table, &row)?;
        let bucket = hash_bucket_for_row(table, &next)?;
        updates.push((rowid, row, next, old_bucket, bucket));
    }
    let moved = updates
        .iter()
        .filter(|(_, _, _, old_bucket, bucket)| old_bucket != bucket)
        .count();
    let (mut next_rowid, seq_op) = if moved == 0 {
        (0, None)
    } else {
        seq.alloc(kv, table.id, moved as u64)?
    };
    let mut writes = Vec::with_capacity(updates.len() + moved);
    let mut fresh_rowid_writes = Vec::with_capacity(moved);
    for (rowid, row, next, old_bucket, bucket) in updates {
        let moved = old_bucket != bucket;
        let target_rowid = if moved {
            let fresh = next_rowid;
            next_rowid += 1;
            fresh
        } else {
            rowid
        };
        let global_index_intents = if moved {
            writes.push(TimestampWrite {
                table_id: table.id,
                bucket: old_bucket,
                rowid,
                global_index_intents: global_index_delete_intents_for_row(
                    table,
                    global_indexes,
                    rowid,
                    &row,
                )?,
                row: row.clone(),
                delete: true,
            });
            global_index_intents_for_row(table, global_indexes, target_rowid, &next)?
        } else {
            let mut intents =
                global_index_delete_intents_for_row(table, global_indexes, rowid, &row)?;
            intents.extend(global_index_intents_for_row(
                table,
                global_indexes,
                target_rowid,
                &next,
            )?);
            intents
        };
        crate::trigger::fire_after_row(
            catalog_kv,
            table,
            crate::trigger::DmlEvent::Update,
            &updated,
            Some(&row),
            Some(&next),
            ctx,
        )?;
        if moved {
            fresh_rowid_writes.push(writes.len());
        }
        writes.push(TimestampWrite {
            table_id: table.id,
            bucket,
            rowid: target_rowid,
            global_index_intents,
            row: next,
            delete: false,
        });
    }
    Ok(TimestampWritePlan {
        result: command(&format!(
            "UPDATE {}",
            writes.iter().filter(|write| !write.delete).count()
        )),
        writes,
        fresh_rowid_writes,
        commit_ops: seq_op.into_iter().collect(),
    })
}

fn execute_timestamp_delete(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    table: &Table,
    global_indexes: &[&crabka_pgcatalog::Index],
    filter: Option<&Expr>,
    ctx: &crate::clock::EvalCtx,
) -> Result<TimestampWritePlan, ExecError> {
    let scope = Scope::single(table, &table.name.name);
    let rows = scan_ts_live_interval(kv, kv, table, ReadTimestamp::MAX, None, RowInterval::ALL)?;
    let filter = crate::bind::bind_optional(filter, &scope)?;
    let filter = filter.as_ref().map(crate::bind::BoundExpr::expr);
    let mut writes = Vec::new();
    for ScannedRow { rowid, row, .. } in rows {
        if !row_matches(filter, &scope, &row, ctx)? {
            continue;
        }
        if crate::trigger::fire_before_row(
            catalog_kv,
            crate::trigger::WriteTarget {
                table,
                check: &crate::rls::WriteChecks::exempt(crate::rls::CheckExemption::RemovesRows),
            },
            crate::trigger::DmlEvent::Delete,
            &[],
            Some(&row),
            None,
            ctx,
        )?
        .is_none()
        {
            continue;
        }
        let global_index_intents =
            global_index_delete_intents_for_row(table, global_indexes, rowid, &row)?;
        let bucket = hash_bucket_for_row(table, &row)?;
        crate::trigger::fire_after_row(
            catalog_kv,
            table,
            crate::trigger::DmlEvent::Delete,
            &[],
            Some(&row),
            None,
            ctx,
        )?;
        writes.push(TimestampWrite {
            table_id: table.id,
            bucket,
            rowid,
            row,
            delete: true,
            global_index_intents,
        });
    }
    Ok(TimestampWritePlan {
        result: command(&format!("DELETE {}", writes.len())),
        writes,
        fresh_rowid_writes: Vec::new(),
        commit_ops: Vec::new(),
    })
}

pub(super) fn hash_bucket_for_row(table: &Table, row: &[Datum]) -> Result<Option<u32>, ExecError> {
    let Some(crabka_pgcatalog::ShardingStrategy::Hash(hash)) = &table.sharding else {
        return Ok(None);
    };
    // A row's bucket is the hash of the one shard column, which is the arity
    // `SHARDED BY HASH` accepts. A wider catalog entry — attachable through the
    // catalog API, which does not gate arity — has no row encoding here: the
    // gateway derives a statement's route from every hash column's bytes, so a
    // row placed under the hash of the first column alone would sit in a range
    // that routing never visits. Refuse the write instead of misplacing it.
    let [column] = hash.columns.as_slice() else {
        return Err(ExecError::Unsupported(
            "hash sharding requires exactly one hash column".into(),
        ));
    };
    let index = table
        .column_index(column)
        .ok_or_else(|| ExecError::Unsupported("hash sharding catalog column mismatch".into()))?;
    let bytes = match &row[index] {
        Datum::Int4(value) => value.to_be_bytes().to_vec(),
        Datum::Int8(value) => value.to_be_bytes().to_vec(),
        Datum::Text(value) => value.as_bytes().to_vec(),
        Datum::Bytea(value) => value.clone(),
        // A `regclass` hashes on its oid: the name it renders is derived from
        // the catalog, so only the oid is stable enough to place a row.
        Datum::Regclass(value) => value.oid.to_be_bytes().to_vec(),
        Datum::Null => Vec::new(),
        _ => {
            return Err(ExecError::Unsupported(
                "hash shard key type is not supported".into(),
            ));
        }
    };
    crabka_pgkv::key::hash_bucket(&bytes, hash.buckets)
        .map(Some)
        .ok_or_else(|| ExecError::Unsupported("invalid hash sharding bucket count".into()))
}

pub(super) fn global_index_intents_for_row(
    table: &Table,
    indexes: &[&crabka_pgcatalog::Index],
    rowid: u64,
    row: &[Datum],
) -> Result<Vec<crate::timestamp_txn::GlobalIndexIntent>, ExecError> {
    indexes
        .iter()
        .map(|index| {
            let indexed_values = index
                .columns
                .iter()
                .map(|column| {
                    let column_index = table
                        .column_index(column)
                        .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
                    Ok(row[column_index].clone())
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            Ok(crate::timestamp_txn::GlobalIndexIntent {
                index_id: index.id,
                indexed_values,
                base_table_id: table.id,
                base_rowid: rowid,
                unique: index.unique,
                delete: false,
            })
        })
        .collect()
}

fn global_index_delete_intents_for_row(
    table: &Table,
    indexes: &[&crabka_pgcatalog::Index],
    rowid: u64,
    row: &[Datum],
) -> Result<Vec<crate::timestamp_txn::GlobalIndexIntent>, ExecError> {
    indexes
        .iter()
        .map(|index| {
            Ok(crate::timestamp_txn::GlobalIndexIntent {
                index_id: index.id,
                indexed_values: indexed_values(table, index, row)?,
                base_table_id: table.id,
                base_rowid: rowid,
                unique: index.unique,
                delete: true,
            })
        })
        .collect()
}
