use super::*;

///
/// `reach` says how far below its target the statement may go. A statement the
/// session submitted carries its own answer in `ONLY`, and passes
/// [`Reach::of`]. The engine's own desugarings — the per-relation pieces of a
/// tree write, a view rewrite, the unfiltered `DELETE`s a `TRUNCATE` becomes —
/// pass [`Reach::Storage`], because they have already walked the inheritance
/// tree themselves but still need a partitioned target to expand. That
pub(super) async fn execute_write_body(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
    reach: Reach,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    // What this statement asks its target to carry. Taken from the text the
    // session wrote and not from `resolved`, because folding a subquery to the
    // value it stands for can only remove a reference, never add one.
    let refs = &crate::scope::StatementRefs::of_write(stmt);
    let resolved = resolve_write_subqueries(write_ctx, ctes, stmt)?;
    let stmt = &resolved;
    let resolution = write_ctx.eval_ctx.resolution();
    let catalog_kv = write_ctx.catalog_kv;
    let kv = write_ctx.kv;
    let lockmgr = write_ctx.lockmgr;
    let seq = write_ctx.seq;
    let xid = write_ctx.xid;
    let lock_owner = write_ctx.lock_owner;
    let ctx = write_ctx.eval_ctx;
    let mut ops: Vec<crabka_pgkv::WriteOp> = Vec::new();
    if let Statement::Insert {
        table,
        on_conflict: Some(_),
        ..
    } = stmt
    {
        let name = resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?;
        let relation = crate::trigger::relation_trigger_table(catalog_kv, &name)?;
        if crabka_pgcatalog::rule::rules_for_table(catalog_kv, relation.id)?
            .into_iter()
            .any(|rule| {
                matches!(
                    rule.event,
                    crabka_pgcatalog::rule::RuleEvent::Insert
                        | crabka_pgcatalog::rule::RuleEvent::Update
                )
            })
        {
            return Err(ExecError::Unsupported(
                "INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules".into(),
            ));
        }
    }
    match stmt {
        // The read body of a statement whose `WITH` list modified data. The CTE
        // relations are already in `ctes`; the query itself runs read-only.
        Statement::Query(q) => {
            let read = write_ctx.read_ctx(ctes);
            let rel = if let Some(_locking) = &q.locking {
                let crabka_pgparser::ast::SetExpr::Query(crabka_pgparser::ast::QueryBody::Select(
                    select,
                )) = &q.body
                else {
                    return Err(ExecError::Unsupported(
                        "locking rule actions must be SELECT statements".into(),
                    ));
                };
                let mut select = select.clone();
                select.locking = q.locking.clone();
                execute_read_locking_relation(
                    &read,
                    write_ctx.procarray,
                    write_ctx.lockmgr,
                    write_ctx.lock_owner,
                    write_ctx.repeatable_read,
                    write_ctx.lock_wait_cap,
                    &select,
                )
                .await?
            } else {
                crate::query::query_to_relation(&read, q)?
            };
            let tag = format!("SELECT {}", rel.rows.len());
            Ok((
                WriteOutcome {
                    tag,
                    returning: Some(rel),
                },
                ops,
            ))
        }
        Statement::Merge { table, .. }
            if has_write_rewrite_rule(catalog_kv, resolution, table)? =>
        {
            Err(ExecError::UnsupportedWithDetail {
                message: format!("cannot execute MERGE on relation \"{}\"", table.name),
                detail: "MERGE is not supported for relations with rules.".into(),
            })
        }
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. }
        | Statement::Merge { table, .. }
            if is_view_ref(catalog_kv, resolution, table)? =>
        {
            Box::pin(execute_view_dml(write_ctx, ctes, stmt, writes)).await
        }
        Statement::Insert { table, .. } if is_partitioned_ref(catalog_kv, resolution, table)? => {
            partitioned_insert(write_ctx, ctes, stmt, writes).await
        }
        // `reach`, not the statement's own `ONLY`: a `TRUNCATE` desugars to an
        // unfiltered `DELETE` that says `ONLY` to stop the inheritance walk it
        // has already done, and reading that as the user's `ONLY` would leave a
        // partitioned parent full. Under a real `ONLY` this arm is skipped and
        // the plain path writes the parent's own — empty — row space, so
        // `DELETE FROM ONLY parted` reports `DELETE 0` and touches no leaf.
        Statement::Update { table, .. } | Statement::Delete { table, .. }
            if reach.spans_partitions() && is_partitioned_ref(catalog_kv, resolution, table)? =>
        {
            Box::pin(partitioned_dml(write_ctx, ctes, stmt, writes)).await
        }
        // After the partitioned guard, so a target that is both a partitioned
        // parent and an inheritance parent routes by partition first. `ONLY`
        // asks for exactly the un-expanded write, and a childless target has
        // nothing to expand into, so both fall through to the plain arms and
        // pay one children-index probe.
        Statement::Update { table, .. } | Statement::Delete { table, .. }
            if reach == Reach::Tree && has_inheritance_children(catalog_kv, resolution, table)? =>
        {
            Box::pin(inherited_dml(write_ctx, ctes, stmt, writes)).await
        }
        Statement::Merge { table, .. } if is_partitioned_ref(catalog_kv, resolution, table)? => {
            Err(ExecError::Unsupported(
                "MERGE into a partitioned table is not supported: a source row that matches no \
                 target row would have to be routed, and the matched/not-matched decision spans \
                 every partition at once"
                    .into(),
            ))
        }
        Statement::Insert {
            table,
            alias,
            columns,
            indirections,
            source,
            on_conflict,
            returning,
            ..
        } => {
            let table =
                &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?;
            let t = crabka_pgcatalog::get_table(catalog_kv, table)?;
            let (target_idx, rows) =
                insert_source_rows(write_ctx, ctes, &t, columns, indirections, source)?;
            let rows = &rows;
            if rows.is_empty() {
                return Ok((WriteOutcome::command("INSERT 0 0".into()), ops));
            }
            // A DO ALSO action gets its own evaluation of `NEW`, after the
            // base statement has produced all its rows.  Materialize only
            // those base rows before entering the per-row write path.
            let prebuilt_rows = crabka_pgcatalog::rule::rules_for_table(catalog_kv, t.id)?
                .into_iter()
                .any(|rule| {
                    rule_is_enabled(rule.enabled)
                        && rule.event == crabka_pgcatalog::rule::RuleEvent::Insert
                        && !rule.instead
                        && rule.action.to_ascii_lowercase().contains("new.")
                })
                .then(|| {
                    rows.iter()
                        .map(|row| {
                            build_insert_row_with_subscripts(
                                &t,
                                &target_idx,
                                indirections,
                                row,
                                ctx,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?;
            let local_indexes = writable_local_indexes(catalog_kv, &t)?;
            let fk_ctx = crate::fk::StatementFkContext::resolve(catalog_kv, &t)?;
            // An `INSERT` reaches no `DmlSource`, so it builds the one thing a
            // `DmlSource` would have given it: the target's own scope, with the
            // system columns the statement asked for appended after it. The
            // values go in each returned row's source block, which is where the
            // joined paths put theirs.
            let qualifier = table_qualifier(&t, alias);
            let mut returning_scope = Scope::single(&t, qualifier);
            let stamp = crate::scope::SystemColumns::of(Some(refs), &t).stamp(t.id)?;
            stamp.extend_scope(&mut returning_scope, qualifier);
            let supplied = WriteContext::modified_columns(&t, &target_idx);
            let insert_check = write_ctx.row_check(
                &t,
                crabka_pgcatalog::policy::PolicyCommand::Insert,
                &supplied,
            )?;
            // Arbiter resolution is statement-level: a bad conflict target is an
            // error even when no row would have conflicted.
            let arbiters = match on_conflict {
                Some(on_conflict) => {
                    resolve_arbiter_indexes(&t, &local_indexes, &on_conflict.target)?
                }
                None => Vec::new(),
            };
            // The command tag's N: inserted rows plus rows updated by DO UPDATE.
            // Rows skipped by DO NOTHING or by a false DO UPDATE … WHERE do not
            // count. Without an ON CONFLICT clause this always ends at rows.len().
            let mut inserted_or_updated: u64 = 0;
            // Reserve a contiguous block of rowids atomically. In Durable mode the
            // SequenceManager persists the new next-rowid itself (seq_op is None).
            // In Replicated mode it returns the seq Put for us to fold into this
            // same commit batch (max-merged by the replicated state machine).
            // Rows that end up skipped or updated leave their reserved rowid
            // unused, exactly as PostgreSQL burns a sequence value per proposed row.
            let n_rows = rows.len() as u64;
            let (start, seq_op) = seq.alloc(kv, t.id, n_rows)?;
            if let Some(op) = seq_op {
                ops.push(op);
            }
            let mut returned_rows = returning
                .as_ref()
                .map(|_| Vec::with_capacity(rows.len()))
                .unwrap_or_default();
            let mut rule_returning = None;
            for (offset, (rowid, row_exprs)) in (start..).zip(rows.iter()).enumerate() {
                // `insert_source_rows` already sized the target list to the
                // source's width, so every row fills exactly `target_idx`.
                // Defaults, coercion and NOT NULL apply to the proposed row even
                // when ON CONFLICT would go on to skip it — PostgreSQL raises
                // 23502 on a DO NOTHING row too.
                let full = prebuilt_rows.as_ref().map_or_else(
                    || {
                        build_insert_row_with_subscripts(
                            &t,
                            &target_idx,
                            indirections,
                            row_exprs,
                            ctx,
                        )
                    },
                    |rows| Ok(rows[offset].clone()),
                )?;
                let (instead, rule_ops, action_returning) = Box::pin(fire_insert_rules(
                    write_ctx,
                    ctes,
                    &t,
                    &target_idx,
                    row_exprs,
                    &full,
                    None,
                    returning.is_some(),
                    writes,
                ))
                .await?;
                ops.extend(rule_ops);
                append_rule_returning(&mut rule_returning, action_returning)?;
                if instead {
                    continue;
                }
                let Some(full) = crate::trigger::fire_before_row(
                    catalog_kv,
                    crate::trigger::WriteTarget {
                        table: &t,
                        check: &insert_check,
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
                check_partition_constraint(write_ctx, &t, &full, &supplied)?;
                if let Some(on_conflict) = on_conflict {
                    let plan =
                        arbitrate_insert_row(write_ctx, &t, &arbiters, on_conflict, &full, writes)
                            .await?;
                    match plan {
                        InsertRowPlan::Insert => {}
                        // The arbiter key lock stays held to COMMIT/ROLLBACK, as
                        // it does for every other unique-key decision.
                        InsertRowPlan::Skip => continue,
                        // NB: the conflicting row's rowid, not this VALUES row's
                        // reserved one — that reservation goes unused.
                        InsertRowPlan::Update {
                            rowid: holder_rowid,
                            cur_key_xid,
                            cur_xmin,
                            cur_cmin,
                            cur_row,
                        } => {
                            let crabka_pgparser::ast::OnConflictAction::DoUpdate {
                                assignments,
                                filter,
                            } = &on_conflict.action
                            else {
                                unreachable!("only DO UPDATE plans a row update")
                            };
                            let updated = apply_insert_conflict_update(
                                write_ctx,
                                &t,
                                &local_indexes,
                                &fk_ctx,
                                &ConflictUpdate {
                                    assignments,
                                    filter: filter.as_ref(),
                                    rowid: holder_rowid,
                                    cur_key_xid,
                                    cur_xmin,
                                    cur_cmin,
                                    cur_row: &cur_row,
                                    proposed: &full,
                                },
                                writes,
                                &mut ops,
                            )
                            .await?;
                            // A DO UPDATE … WHERE that is not true leaves the row
                            // neither inserted nor updated, with no RETURNING row.
                            let Some((next, new_rowid)) = updated else {
                                continue;
                            };
                            let _ = writes.claim_row(
                                t.id,
                                holder_rowid,
                                cur_xmin,
                                CommandOperation::Updated,
                            );
                            if returning.is_some() {
                                let mut system = Vec::new();
                                stamp.extend_row(
                                    &mut system,
                                    new_rowid,
                                    xid,
                                    0,
                                    write_ctx.command_id,
                                    0,
                                );
                                returned_rows.push(ReturnedRow::updated(
                                    next,
                                    cur_row,
                                    system,
                                    holder_rowid,
                                    new_rowid,
                                    cur_xmin,
                                    cur_cmin,
                                    xid,
                                    write_ctx.command_id,
                                ));
                            }
                            inserted_or_updated += 1;
                            continue;
                        }
                    }
                }
                // No conflict (or no ON CONFLICT clause): the plain insert path.
                // Re-locking the arbiter keys here is idempotent, and this also
                // enforces 23505 on the unique indexes that do NOT arbitrate —
                // PostgreSQL's ordering.
                enforce_unique_local_indexes(write_ctx, &t, &local_indexes, rowid, &full, writes)
                    .await?;
                // Append only, never probe: the check runs once the statement's
                // rows exist, which is what makes a self-referencing
                // `INSERT INTO t (id, boss) VALUES (1, 1)` succeed under a
                // NOT DEFERRABLE constraint, exactly as it does in PostgreSQL.
                if !fk_ctx.is_empty() {
                    writes.fk_checks.after_insert(&fk_ctx, rowid, &full)?;
                }
                if returning.is_some() {
                    let mut system = Vec::new();
                    stamp.extend_row(&mut system, rowid, xid, 0, write_ctx.command_id, 0);
                    returned_rows.push(ReturnedRow {
                        new: Some(full.clone()),
                        old: None,
                        source: system,
                        old_xmin: 0,
                        old_xmax: 0,
                        old_cmin: 0,
                        old_cmax: 0,
                        new_xmin: xid,
                        new_xmax: 0,
                        new_cmin: write_ctx.command_id,
                        new_cmax: 0,
                        action: None,
                        old_identity: NO_ROW_IDENTITY,
                        new_identity: rowid,
                    });
                }
                ops.push(crabka_pgkv::WriteOp::Put {
                    key: crabka_pgmvcc::version::version_key_xid(t.id, rowid, xid),
                    value: encode_table_tuple(
                        &t,
                        xid,
                        crabka_pgmvcc::xid::INVALID_XID,
                        write_ctx.command_id,
                        0,
                        &full,
                    ),
                });
                ops.extend(local_index_entry_ops(&t, &local_indexes, rowid, &full)?);
                crate::trigger::fire_after_row(
                    catalog_kv,
                    &t,
                    crate::trigger::DmlEvent::Insert,
                    &[],
                    None,
                    Some(&full),
                    ctx,
                )?;
                inserted_or_updated += 1;
            }
            let tag = format!("INSERT 0 {inserted_or_updated}");
            if let Some(returning) = rule_returning {
                return Ok((
                    WriteOutcome {
                        tag,
                        returning: Some(returning),
                    },
                    ops,
                ));
            }
            let spec = ReturningSpec::new(
                &t,
                qualifier,
                returning.as_ref(),
                Some(&returning_scope),
                false,
            )?;
            Ok((spec.outcome(tag, returned_rows, ctx)?, ops))
        }
        Statement::Update {
            table,
            alias,
            assignments,
            from,
            filter,
            returning,
            ..
        } => {
            let table =
                &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?;
            if table.schema == crate::search_path::PG_CATALOG && table.name == "pg_class" {
                return update_pg_class_statistics(
                    write_ctx,
                    ctes,
                    alias.as_deref(),
                    assignments,
                    from,
                    filter.as_ref(),
                    returning.as_ref(),
                );
            }
            let t = crabka_pgcatalog::get_table(catalog_kv, table)?;
            if matches_nothing_rule(
                catalog_kv,
                &t,
                crabka_pgcatalog::rule::RuleEvent::Update,
                None,
                None,
                ctx,
            )? {
                return Ok((WriteOutcome::command("UPDATE 0".into()), ops));
            }
            let local_indexes = writable_local_indexes(catalog_kv, &t)?;
            let fk_ctx = crate::fk::StatementFkContext::resolve(catalog_kv, &t)?;
            let qualifier = table_qualifier(&t, alias);
            let read = write_ctx.read_ctx(ctes);
            let source = DmlSource::build(write_ctx, ctes, &t, qualifier, from, Some(refs))?;
            let filter = source.resolve_filter(&read, filter.as_ref())?;
            let mut binder = filter
                .as_ref()
                .map(|filter| validate_correlated_subqueries(&read, filter, &source.scope))
                .transpose()?
                .unwrap_or(false)
                .then(|| LateralBinder::new(catalog_kv, read.fctx.resolution, read.ctes));
            let bound_filter = if binder.is_none() {
                source.bind_filter(filter.as_ref())?
            } else {
                None
            };
            let targets = resolve_assignments(write_ctx, ctes, &t, assignments)?;
            let spec = ReturningSpec::new(
                &t,
                qualifier,
                returning.as_ref(),
                Some(&source.scope),
                false,
            )?;
            let mut n: u64 = 0;
            let mut returned_rows = returning.as_ref().map(|_| Vec::new()).unwrap_or_default();
            let mut rule_returning = None;
            // The `SET` list, which is both what the triggers are told changed
            // and the set a rejected row may be described by.
            let updated_columns: Vec<String> = assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect();
            let update_check = write_ctx.row_check(
                &t,
                crabka_pgcatalog::policy::PolicyCommand::Update,
                &updated_columns,
            )?;
            // The virtual generated columns this statement can reach in the row
            // it is about to overwrite. A `SET` right-hand side, the `WHERE`, a
            // `FROM` item's `ON` and a `RETURNING old.…` are all in `refs`.
            let reads = read_generated_reads(&t, Some(refs), qualifier);
            for (rowid, scanned_xmin, scanned_row) in write_candidate_rows(
                write_ctx,
                &t,
                crate::privilege::WriteAction::Update,
                source.probe_filter(filter.as_ref()),
                crate::privilege::dml_reads_target(
                    &t,
                    qualifier,
                    filter.as_ref(),
                    returning.as_ref(),
                    assignments,
                ),
                reads,
            )? {
                // 1. Filter on the snapshot-visible row FIRST — do not lock rows
                //    that don't match the WHERE clause (avoids over-locking and
                //    restores row-level write concurrency for different rows).
                let matches = if let Some(binder) = &mut binder {
                    source.first_match_correlated(
                        &read,
                        filter.as_ref(),
                        &scanned_row,
                        rowid,
                        scanned_xmin,
                        binder,
                    )?
                } else {
                    source.first_match(
                        bound_filter.as_ref(),
                        &scanned_row,
                        rowid,
                        scanned_xmin,
                        ctx,
                    )?
                };
                if matches.is_none() {
                    continue;
                }
                // 2. Lock only matching candidates.
                lockmgr
                    .acquire_as(
                        t.id,
                        rowid,
                        crate::lockmgr::LockMode::Exclusive,
                        lock_owner,
                        write_ctx.lock_wait_cap,
                    )
                    .await
                    .map_err(lock_acquire_error)?;
                // 3. EvalPlanQual: re-read this row under the lock and decide what to
                //    operate on (40001 under RR if changed since our snapshot).
                let Some((cur_rowid, cur_key_xid, cur_xmin, cur_cmin, _cur_cmax, cur_row)) =
                    eval_plan_qual(&write_ctx.mutation(), &t, rowid, reads)?
                else {
                    continue; // deleted by a concurrent committed txn — skip
                };
                // 4. Re-check the filter on the (possibly re-found) current row —
                //    under READ COMMITTED the row may have changed since the scan.
                //    A joined UPDATE updates each target row once, using the first
                //    source row it matches (PostgreSQL leaves the choice
                //    unspecified when several match).
                let Some(joined) = (if let Some(binder) = &mut binder {
                    source.first_match_correlated(
                        &read,
                        filter.as_ref(),
                        &cur_row,
                        cur_rowid,
                        cur_xmin,
                        binder,
                    )?
                } else {
                    source.first_match(bound_filter.as_ref(), &cur_row, cur_rowid, cur_xmin, ctx)?
                }) else {
                    continue; // no longer matches the WHERE clause
                };
                match writes.claim_row(t.id, cur_rowid, cur_xmin, CommandOperation::Updated) {
                    RowClaim::Claimed => {}
                    RowClaim::Statement => continue,
                    RowClaim::Trigger(operation) => {
                        return Err(trigger_modified_row_error(operation));
                    }
                }
                let next = apply_assignments(&t, &targets, &source.scope, &joined, ctx)?;
                let (instead, rule_ops, action_returning) = Box::pin(fire_row_rules(
                    write_ctx,
                    ctes,
                    &t,
                    crabka_pgcatalog::rule::RuleEvent::Update,
                    Some(&cur_row),
                    Some(&next),
                    true,
                    returning.is_some(),
                    writes,
                ))
                .await?;
                ops.extend(rule_ops);
                append_rule_returning(&mut rule_returning, action_returning)?;
                if instead {
                    continue;
                }
                let Some(next) = crate::trigger::fire_before_row(
                    catalog_kv,
                    crate::trigger::WriteTarget {
                        table: &t,
                        check: &update_check,
                    },
                    crate::trigger::DmlEvent::Update,
                    &updated_columns,
                    Some(&cur_row),
                    Some(next),
                    ctx,
                )?
                else {
                    continue;
                };
                check_partition_constraint(write_ctx, &t, &next, &updated_columns)?;
                if writes.trigger_replaced_claim(
                    t.id,
                    cur_rowid,
                    cur_xmin,
                    CommandOperation::Updated,
                ) {
                    return Err(trigger_modified_row_error(CommandOperation::Updated));
                }
                let new_rowid = apply_locked_row_update(
                    write_ctx,
                    &t,
                    &local_indexes,
                    &fk_ctx,
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
                    catalog_kv,
                    &t,
                    crate::trigger::DmlEvent::Update,
                    &updated_columns,
                    Some(&cur_row),
                    Some(&next),
                    ctx,
                )?;
                let (_, rule_ops, action_returning) = Box::pin(fire_row_rules(
                    write_ctx,
                    ctes,
                    &t,
                    crabka_pgcatalog::rule::RuleEvent::Update,
                    Some(&cur_row),
                    Some(&next),
                    false,
                    false,
                    writes,
                ))
                .await?;
                ops.extend(rule_ops);
                append_rule_returning(&mut rule_returning, action_returning)?;
                if returning.is_some() {
                    returned_rows.push(ReturnedRow::updated(
                        next,
                        cur_row,
                        joined[t.columns.len()..].to_vec(),
                        cur_rowid,
                        new_rowid,
                        cur_xmin,
                        cur_cmin,
                        xid,
                        write_ctx.command_id,
                    ));
                }
                n += 1;
            }
            let tag = format!("UPDATE {n}");
            if let Some(returning) = rule_returning {
                return Ok((
                    WriteOutcome {
                        tag,
                        returning: Some(returning),
                    },
                    ops,
                ));
            }
            Ok((spec.outcome(tag, returned_rows, ctx)?, ops))
        }
        Statement::Delete {
            table,
            alias,
            using,
            filter,
            returning,
            ..
        } => {
            let table =
                &resolve_relation(catalog_kv, resolution, table, SchemaDisposition::Reference)?;
            let t = crabka_pgcatalog::get_table(catalog_kv, table)?;
            if !writes.truncate_set.contains(&t.id)
                && matches_nothing_rule(
                    catalog_kv,
                    &t,
                    crabka_pgcatalog::rule::RuleEvent::Delete,
                    None,
                    None,
                    ctx,
                )?
            {
                return Ok((WriteOutcome::command("DELETE 0".into()), ops));
            }
            let local_indexes = writable_local_indexes(catalog_kv, &t)?;
            // A `TRUNCATE` desugars to one of these per relation; the truncate
            // set suppresses exactly the parent-side keys whose child is being
            // emptied in the same statement, so no referential action fires.
            let fk_ctx = crate::fk::StatementFkContext::resolve_for_truncate(
                catalog_kv,
                &t,
                &writes.truncate_set,
            )?;
            let is_truncate = writes.truncate_set.contains(&t.id);
            let qualifier = table_qualifier(&t, alias);
            let read = write_ctx.read_ctx(ctes);
            let source = DmlSource::build(write_ctx, ctes, &t, qualifier, using, Some(refs))?;
            let filter = source.resolve_filter(&read, filter.as_ref())?;
            let mut binder = filter
                .as_ref()
                .map(|filter| validate_correlated_subqueries(&read, filter, &source.scope))
                .transpose()?
                .unwrap_or(false)
                .then(|| LateralBinder::new(catalog_kv, read.fctx.resolution, read.ctes));
            let bound_filter = if binder.is_none() {
                source.bind_filter(filter.as_ref())?
            } else {
                None
            };
            let spec = ReturningSpec::new(
                &t,
                qualifier,
                returning.as_ref(),
                Some(&source.scope),
                false,
            )?;
            let mut n: u64 = 0;
            let mut returned_rows = returning.as_ref().map(|_| Vec::new()).unwrap_or_default();
            let mut rule_returning = None;
            // A `DELETE`'s only reach into the row it removes is its `WHERE`,
            // its `USING` join and its `RETURNING` — all of them in `refs`. A
            // `TRUNCATE` desugars to one with none of the three, so it reaches
            // nothing, which is why it can empty a relation holding a row whose
            // generation expression overflows.
            let reads = read_generated_reads(&t, Some(refs), qualifier);
            for (rowid, scanned_xmin, scanned_row) in write_candidate_rows(
                write_ctx,
                &t,
                // A `TRUNCATE` desugars to one of these, and PostgreSQL
                // authorizes it with the TRUNCATE privilege rather than DELETE.
                // The truncate set is the statement's own record of which
                // relations it is emptying, so it is also the honest answer to
                // "which privilege authorized this delete".
                if is_truncate {
                    crate::privilege::WriteAction::Truncate
                } else {
                    crate::privilege::WriteAction::Delete
                },
                source.probe_filter(filter.as_ref()),
                crate::privilege::dml_reads_target(
                    &t,
                    qualifier,
                    filter.as_ref(),
                    returning.as_ref(),
                    &[],
                ),
                reads,
            )? {
                // 1. Filter on the snapshot-visible row FIRST — do not lock rows
                //    that don't match the WHERE clause.
                let matches = if let Some(binder) = &mut binder {
                    source.first_match_correlated(
                        &read,
                        filter.as_ref(),
                        &scanned_row,
                        rowid,
                        scanned_xmin,
                        binder,
                    )?
                } else {
                    source.first_match(
                        bound_filter.as_ref(),
                        &scanned_row,
                        rowid,
                        scanned_xmin,
                        ctx,
                    )?
                };
                if matches.is_none() {
                    continue;
                }
                // 2. Lock only matching candidates.
                lockmgr
                    .acquire_as(
                        t.id,
                        rowid,
                        crate::lockmgr::LockMode::Exclusive,
                        lock_owner,
                        write_ctx.lock_wait_cap,
                    )
                    .await
                    .map_err(lock_acquire_error)?;
                // 3. EvalPlanQual: re-read this row under the lock.
                let Some((cur_rowid, cur_key_xid, cur_xmin, cur_cmin, _cur_cmax, cur_row)) =
                    eval_plan_qual(&write_ctx.mutation(), &t, rowid, reads)?
                else {
                    if writes.trigger_replaced_claim(
                        t.id,
                        rowid,
                        scanned_xmin,
                        CommandOperation::Deleted,
                    ) {
                        return Err(trigger_modified_row_error(CommandOperation::Updated));
                    }
                    continue; // already deleted by a concurrent committed txn
                };
                // 4. Re-check filter on the (possibly re-found) current row.
                let Some(joined) = (if let Some(binder) = &mut binder {
                    source.first_match_correlated(
                        &read,
                        filter.as_ref(),
                        &cur_row,
                        cur_rowid,
                        cur_xmin,
                        binder,
                    )?
                } else {
                    source.first_match(bound_filter.as_ref(), &cur_row, cur_rowid, cur_xmin, ctx)?
                }) else {
                    continue; // no longer matches the WHERE clause
                };
                match writes.claim_row(t.id, cur_rowid, cur_xmin, CommandOperation::Deleted) {
                    RowClaim::Claimed => {}
                    RowClaim::Statement => continue,
                    RowClaim::Trigger(operation) => {
                        return Err(trigger_modified_row_error(operation));
                    }
                }
                if !is_truncate {
                    let (instead, rule_ops, action_returning) = Box::pin(fire_row_rules(
                        write_ctx,
                        ctes,
                        &t,
                        crabka_pgcatalog::rule::RuleEvent::Delete,
                        Some(&cur_row),
                        None,
                        true,
                        returning.is_some(),
                        writes,
                    ))
                    .await?;
                    ops.extend(rule_ops);
                    append_rule_returning(&mut rule_returning, action_returning)?;
                    if instead {
                        continue;
                    }
                }
                if !is_truncate
                    && crate::trigger::fire_before_row(
                        catalog_kv,
                        crate::trigger::WriteTarget {
                            table: &t,
                            check: &crate::rls::WriteChecks::exempt(
                                crate::rls::CheckExemption::RemovesRows,
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
                    continue;
                }
                if writes.trigger_replaced_claim(
                    t.id,
                    cur_rowid,
                    cur_xmin,
                    CommandOperation::Deleted,
                ) {
                    return Err(trigger_modified_row_error(CommandOperation::Deleted));
                }
                // Append only: the parent-side probe needs the KV and the lock
                // manager, and the row's tombstone is only staged, so the check
                // waits for the end of the statement.
                if !fk_ctx.is_empty() {
                    writes
                        .fk_checks
                        .after_delete(&fk_ctx, cur_rowid, &cur_row)?;
                }
                if returning.is_some() {
                    returned_rows.push(ReturnedRow {
                        new: None,
                        old: Some(cur_row.clone()),
                        source: joined[t.columns.len()..].to_vec(),
                        old_xmin: cur_xmin,
                        old_xmax: xid,
                        old_cmin: cur_cmin,
                        old_cmax: write_ctx.command_id,
                        new_xmin: 0,
                        new_xmax: 0,
                        new_cmin: 0,
                        new_cmax: 0,
                        action: None,
                        old_identity: cur_rowid,
                        new_identity: NO_ROW_IDENTITY,
                    });
                }
                apply_locked_row_delete(
                    write_ctx,
                    &t,
                    &local_indexes,
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
                if !is_truncate {
                    crate::trigger::fire_after_row(
                        catalog_kv,
                        &t,
                        crate::trigger::DmlEvent::Delete,
                        &[],
                        Some(&cur_row),
                        None,
                        ctx,
                    )?;
                    let (_, rule_ops, action_returning) = Box::pin(fire_row_rules(
                        write_ctx,
                        ctes,
                        &t,
                        crabka_pgcatalog::rule::RuleEvent::Delete,
                        Some(&cur_row),
                        None,
                        false,
                        false,
                        writes,
                    ))
                    .await?;
                    ops.extend(rule_ops);
                    append_rule_returning(&mut rule_returning, action_returning)?;
                }
                n += 1;
            }
            let tag = format!("DELETE {n}");
            if let Some(returning) = rule_returning {
                return Ok((
                    WriteOutcome {
                        tag,
                        returning: Some(returning),
                    },
                    ops,
                ));
            }
            Ok((spec.outcome(tag, returned_rows, ctx)?, ops))
        }
        Statement::Merge { .. } => Box::pin(execute_merge(write_ctx, ctes, stmt, writes)).await,
        Statement::Cluster(target) => {
            execute_cluster(write_ctx, target.as_ref(), writes, &mut ops).await?;
            Ok((WriteOutcome::command("CLUSTER".into()), ops))
        }
        Statement::Truncate {
            targets,
            restart_identity,
            cascade,
        } => {
            if *restart_identity {
                return Err(ExecError::Unsupported(
                    "TRUNCATE RESTART IDENTITY is not supported: SERIAL sequence ownership is not tracked".into(),
                ));
            }
            // Validate every name (and refuse sharded targets) before touching
            // any table: the statement is all-or-nothing across the list.
            //
            // A target without `ONLY` stands for its whole inheritance tree, so
            // this list is already wider than the one written down; `CASCADE`
            // widens it further below.
            let written = truncate_names(catalog_kv, resolution, targets)?;
            let mut named = Vec::with_capacity(written.len());
            for name in
                resolve_relations(catalog_kv, resolution, &written, SchemaDisposition::Utility)?
            {
                if let Some(error) = truncate_wrong_kind(catalog_kv, &name) {
                    return Err(error);
                }
                let t = crabka_pgcatalog::get_table(catalog_kv, &name)?;
                if table_uses_global_visibility(&t) {
                    return Err(ExecError::Unsupported(
                        "TRUNCATE on sharded tables is not supported".into(),
                    ));
                }
                named.push(t);
            }
            // `TRUNCATE` does not fire `ON DELETE CASCADE`: it refuses when a
            // relation outside the set references one inside it, and `CASCADE`
            // widens the *set* instead. Divergence: PostgreSQL also emits a
            // `NOTICE: truncate cascades to table "…"` per relation `CASCADE`
            // pulls in, which this engine has no NoticeResponse path for, so
            // `TruncateSet::cascaded` is computed and left unemitted.
            let set = crate::fk::expand_truncate_set(catalog_kv, &named, *cascade)?;
            // Every relation the statement will empty, including the ones
            // CASCADE pulled in, before the first row of the first one is
            // touched: TRUNCATE is all-or-nothing, so a denial has to arrive
            // before any work is done. The desugared DELETEs below carry
            // `WriteAction::Truncate` and are authorized by this, not by the
            // DELETE privilege.
            for table in &set.tables {
                crate::privilege::require(
                    &write_ctx.privileges(),
                    &table.name,
                    &table.owner,
                    crate::privilege::RelationKind::Table,
                    crate::privilege::Privilege::Truncate,
                )?;
            }
            // Carried on the statement's write state so each desugared DELETE
            // suppresses exactly the parent-side keys whose child is in the set
            // — by construction every one of them, so no action ever fires.
            writes.truncate_set = set.ids();
            // Desugar to an unfiltered DELETE per table: TRUNCATE shares the
            // MVCC write path (row locks, xmax stamping, rollback) rather
            // than clearing storage, so it is transactional like PostgreSQL's.
            for table in &set.tables {
                let delete = Statement::Delete {
                    table: crabka_pgparser::ast::RelationRef {
                        schema: Some(table.name.schema.clone()),
                        name: table.name.name.clone(),
                    },
                    only: true,
                    alias: None,
                    filter: None,
                    where_current_of: None,
                    using: Vec::new(),
                    returning: None,
                    with: None,
                };
                let (_, delete_ops) = Box::pin(execute_write_body(
                    write_ctx,
                    ctes,
                    &delete,
                    writes,
                    Reach::Storage,
                ))
                .await?;
                ops.extend(delete_ops);
            }
            Ok((WriteOutcome::command("TRUNCATE TABLE".into()), ops))
        }
        _ => Err(ExecError::Unsupported("not a write statement".into())),
    }
}
