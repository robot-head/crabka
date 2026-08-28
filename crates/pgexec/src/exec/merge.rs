//! MERGE statement execution.

use super::*;

/// `MERGE INTO target USING source ON cond WHEN …`.
///
/// The source relation and the target's visible rows are both materialized
/// against the statement snapshot, then joined on `ON`. Source rows drive the
/// `MATCHED` and `NOT MATCHED [BY TARGET]` clauses; a second pass over the
/// target rows no source row joined drives `NOT MATCHED BY SOURCE`. A target
/// row that two clauses would touch is `PostgreSQL`'s 21000.
pub(super) async fn execute_merge(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    use crabka_pgparser::ast::{MergeAction, MergeMatchKind, MergeSource};

    let resolution = write_ctx.eval_ctx.resolution();
    let Statement::Merge {
        table,
        alias,
        source,
        on,
        clauses,
        returning,
        ..
    } = stmt
    else {
        return Err(ExecError::Unsupported("not a MERGE statement".into()));
    };
    let ctx = write_ctx.eval_ctx;
    let table = &resolve_relation(
        write_ctx.catalog_kv,
        resolution,
        table,
        SchemaDisposition::Reference,
    )?;
    let t = crabka_pgcatalog::get_table(write_ctx.catalog_kv, table)?;
    for assignments in clauses.iter().filter_map(|clause| match &clause.action {
        MergeAction::Update(assignments) => Some(assignments.as_slice()),
        MergeAction::DoNothing | MergeAction::Insert { .. } | MergeAction::Delete => None,
    }) {
        dml_assignments::validate_assignment_targets(&t, assignments)?;
    }
    let local_indexes = writable_local_indexes(write_ctx.catalog_kv, &t)?;
    let fk_ctx = crate::fk::StatementFkContext::resolve(write_ctx.catalog_kv, &t)?;
    let qualifier = table_qualifier(&t, alias);
    reject_merge_when_system_columns(clauses, qualifier)?;
    let refs = crate::scope::StatementRefs::of_write(stmt);
    let stamp = crate::scope::SystemColumns::of(Some(&refs), &t).stamp(t.id)?;
    let mut target_scope = Scope::single(&t, qualifier);
    stamp.extend_scope(&mut target_scope, qualifier);
    let mut ops: Vec<crabka_pgkv::WriteOp> = Vec::new();

    let read = write_ctx.read_ctx(ctes);
    let source_rel = match source {
        MergeSource::Table { name, alias } => {
            let source_name = alias.as_deref().unwrap_or(&name.name);
            if source_name == qualifier {
                return Err(ExecError::Remote(
                    crabka_pgwire::error::PgError::error(
                        "42712",
                        format!("name \"{source_name}\" specified more than once"),
                    )
                    .with_detail("The name is used both as MERGE target table and data source."),
                ));
            }
            let te = crabka_pgparser::ast::TableExpr::Table {
                name: name.clone(),
                only: false,
                alias: alias.clone(),
                columns: None,
                sample: None,
            };
            build_from(&read, std::slice::from_ref(&te), None, None, None, None)?
        }
        MergeSource::Query {
            query,
            alias,
            columns,
        } => {
            let rel = crate::query::query_to_relation(&read, query).map_err(|error| {
                from_resolution::explain_outer_reference(
                    error,
                    &target_scope,
                    from_resolution::OuterReference::Target,
                )
            })?;
            crate::values::requalify_derived(rel, alias, columns)?
        }
    };
    let source_width = source_rel.scope.width();
    let target_width = t.columns.len();
    let mut scope = Scope::single(&t, qualifier);
    scope.extend(&source_rel.scope);
    stamp.extend_scope(&mut scope, qualifier);
    validate_merge_clause_scopes(clauses, &target_scope, &source_rel.scope, &scope)?;
    let spec = ReturningSpec::new(&t, qualifier, returning.as_ref(), Some(&scope), true)?;

    // MERGE reaches rows through its own join, so `SELECT` policies decide
    // which rows it sees and the clauses it was written with decide which
    // privileges it needs. Both are settled here, before the first row is read:
    // `PostgreSQL` checks a MERGE's privileges at statement start whether or
    // not a particular `WHEN` clause ever fires.
    let mut target_rows = write_candidate_rows(
        write_ctx,
        &t,
        crate::privilege::WriteAction::Merge(crate::privilege::MergeClauses::of(clauses)),
        None,
        true,
        crate::scope::GeneratedReads::every(),
    )?;
    // A MERGE rewritten from a view sees only the rows the view presents. The
    // qual filters the scan and not the join, so a row the view hides is not
    // "not matched by source" — it is not there at all. See
    // [`WriteContext::merge_target_qual`].
    if let Some(qual) = write_ctx.merge_target_qual {
        let target_scope = Scope::single(&t, qualifier);
        let bound = crate::bind::BoundExpr::new(qual, &target_scope)?;
        let mut kept = Vec::with_capacity(target_rows.len());
        for row in target_rows {
            if row_matches(Some(bound.expr()), &target_scope, &row.2, ctx)? {
                kept.push(row);
            }
        }
        target_rows = kept;
    }
    // The `UPDATE` and `DELETE` policies then judge each row an action reaches,
    // one row at a time, and raise rather than skip — see `MergeRowSecurity`.
    let row_security = MergeRowSecurity::compile(write_ctx, &t)?;
    let mut matched: HashSet<u64> = HashSet::new();
    let mut returned_rows = Vec::new();
    let mut n: u64 = 0;

    // Every (source, target) pair evaluates the same ON condition, so its
    // column references are resolved once against the joined scope.
    let on_bound = crate::bind::BoundExpr::new(on, &scope)?;
    for source_row in &source_rel.rows {
        let mut any_match = false;
        for (rowid, xmin, target_row) in &target_rows {
            let mut joined = target_row.clone();
            joined.extend_from_slice(source_row);
            stamp.extend_row(&mut joined, *rowid, *xmin, 0, write_ctx.command_id, 0);
            if !row_matches(Some(on_bound.expr()), &scope, &joined, ctx)? {
                continue;
            }
            any_match = true;
            matched.insert(*rowid);
            let Some(when) =
                pick_merge_clause(clauses, MergeMatchKind::Matched, &scope, &joined, ctx)?
            else {
                continue;
            };
            if matches!(when.action, MergeAction::DoNothing) {
                continue;
            }
            // A row any part of this statement already modified — an earlier
            // WHEN clause, or a data-modifying WITH item — is PostgreSQL's 21000.
            if !matches!(
                writes.claim_row(t.id, *rowid, *xmin, CommandOperation::UpdatedOrDeleted),
                RowClaim::Claimed
            ) {
                return Err(merge_row_touched_twice());
            }
            let applied = Box::pin(apply_merge_row_action(
                write_ctx,
                &MergeRowAction {
                    table: &t,
                    local_indexes: &local_indexes,
                    fk: &fk_ctx,
                    ctes,
                    scope: &scope,
                    source_width,
                    stamp: &stamp,
                    rowid: *rowid,
                    joined: &joined,
                    action: &when.action,
                    security: &row_security,
                },
                writes,
                &mut ops,
            ))
            .await
            .map_err(merge_trigger_error)?;
            if writes.trigger_replaced_claim(
                t.id,
                *rowid,
                *xmin,
                CommandOperation::UpdatedOrDeleted,
            ) {
                return Err(trigger_modified_row_error(
                    CommandOperation::UpdatedOrDeleted,
                ));
            }
            if let Some(row) = applied {
                n += 1;
                if spec.active {
                    returned_rows.push(row);
                }
            }
        }
        if any_match {
            continue;
        }
        let mut joined = vec![Datum::Null; target_width];
        joined.extend_from_slice(source_row);
        joined.extend(std::iter::repeat_n(Datum::Null, stamp.width()));
        let Some(when) = pick_merge_clause(
            clauses,
            MergeMatchKind::NotMatchedByTarget,
            &source_rel.scope,
            source_row,
            ctx,
        )?
        else {
            continue;
        };
        let MergeAction::Insert {
            columns,
            indirections,
            values,
            ..
        } = &when.action
        else {
            continue; // DO NOTHING
        };
        let exprs: Vec<Expr> = values.clone().unwrap_or_default();
        // `INSERT DEFAULT VALUES` has no target list at all; otherwise a MERGE
        // insert action obeys the same arity rule as a plain INSERT, and reports
        // it with the same two messages.
        let target_idx = if values.is_none() {
            Vec::new()
        } else {
            resolve_insert_targets(&t, columns, indirections, exprs.len())?
        };
        // A `NOT MATCHED` action can see only its source row.
        // A literal keeps its unresolved form so `build_insert_row` applies the
        // same `unknown`-literal typing a plain INSERT would; everything else is
        // folded against the joined row, which the source columns live in.
        let evaluated = exprs
            .iter()
            .zip(&target_idx)
            .map(|(expr, slot)| match expr {
                Expr::Default | Expr::StringLiteral(_) | Expr::BitStringLiteral(_) => {
                    Ok(expr.clone())
                }
                _ => crate::eval::eval(expr, &source_rel.scope, source_row, ctx).map(|value| {
                    Expr::Const {
                        value,
                        ty: t.columns[*slot].ty,
                    }
                }),
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        let full =
            build_insert_row_with_subscripts(&t, &target_idx, indirections, &evaluated, ctx)?;
        let merge_insert_check = write_ctx.row_check(
            &t,
            crabka_pgcatalog::policy::PolicyCommand::Insert,
            &WriteContext::modified_columns(&t, &target_idx),
        )?;
        let Some(full) = crate::trigger::fire_before_row(
            write_ctx.catalog_kv,
            crate::trigger::WriteTarget {
                table: &t,
                check: &merge_insert_check,
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
        let (rowid, seq_op) = write_ctx.seq.alloc(write_ctx.kv, t.id, 1)?;
        if let Some(op) = seq_op {
            ops.push(op);
        }
        enforce_unique_local_indexes(write_ctx, &t, &local_indexes, rowid, &full, writes).await?;
        if !fk_ctx.is_empty() {
            writes.fk_checks.after_insert(&fk_ctx, rowid, &full)?;
        }
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_xid(t.id, rowid, write_ctx.xid),
            value: encode_table_tuple(
                &t,
                write_ctx.xid,
                crabka_pgmvcc::xid::INVALID_XID,
                write_ctx.command_id,
                0,
                &full,
            ),
        });
        ops.extend(local_index_entry_ops(&t, &local_indexes, rowid, &full)?);
        crate::trigger::fire_after_row(
            write_ctx.catalog_kv,
            &t,
            crate::trigger::DmlEvent::Insert,
            &[],
            None,
            Some(&full),
            ctx,
        )?;
        n += 1;
        if spec.active {
            returned_rows.push(ReturnedRow {
                new: Some(full),
                old: None,
                source: source_row.clone(),
                old_xmin: 0,
                old_xmax: 0,
                old_cmin: 0,
                old_cmax: 0,
                new_xmin: write_ctx.xid,
                new_xmax: 0,
                new_cmin: write_ctx.command_id,
                new_cmax: 0,
                action: Some("INSERT"),
                old_identity: NO_ROW_IDENTITY,
                new_identity: NO_ROW_IDENTITY,
            });
        }
    }

    for (rowid, xmin, target_row) in &target_rows {
        if matched.contains(rowid) {
            continue;
        }
        let mut target_only = target_row.clone();
        stamp.extend_row(&mut target_only, *rowid, *xmin, 0, write_ctx.command_id, 0);
        let mut joined = target_row.clone();
        joined.extend(std::iter::repeat_n(Datum::Null, source_width));
        stamp.extend_row(&mut joined, *rowid, *xmin, 0, write_ctx.command_id, 0);
        let Some(when) = pick_merge_clause(
            clauses,
            MergeMatchKind::NotMatchedBySource,
            &target_scope,
            &target_only,
            ctx,
        )?
        else {
            continue;
        };
        if matches!(when.action, MergeAction::DoNothing) {
            continue;
        }
        if !matches!(
            writes.claim_row(t.id, *rowid, *xmin, CommandOperation::UpdatedOrDeleted),
            RowClaim::Claimed
        ) {
            return Err(merge_row_touched_twice());
        }
        let applied = Box::pin(apply_merge_row_action(
            write_ctx,
            &MergeRowAction {
                table: &t,
                local_indexes: &local_indexes,
                fk: &fk_ctx,
                ctes,
                scope: &target_scope,
                source_width,
                stamp: &stamp,
                rowid: *rowid,
                joined: &joined,
                action: &when.action,
                security: &row_security,
            },
            writes,
            &mut ops,
        ))
        .await
        .map_err(merge_trigger_error)?;
        if writes.trigger_replaced_claim(t.id, *rowid, *xmin, CommandOperation::UpdatedOrDeleted) {
            return Err(trigger_modified_row_error(
                CommandOperation::UpdatedOrDeleted,
            ));
        }
        if let Some(row) = applied {
            n += 1;
            if spec.active {
                returned_rows.push(row);
            }
        }
    }

    Ok((spec.outcome(format!("MERGE {n}"), returned_rows, ctx)?, ops))
}

fn reject_merge_when_system_columns(
    clauses: &[crabka_pgparser::ast::MergeWhen],
    target: &str,
) -> Result<(), ExecError> {
    let mut forbidden = None;
    for clause in clauses {
        let Some(condition) = &clause.condition else {
            continue;
        };
        crate::grouping::visit_expr(condition, &mut |node| {
            if forbidden.is_none()
                && let Expr::Column {
                    table: Some(table),
                    name,
                } = node
                && table == target
                && crate::scope::is_system_column(name)
                && name != crate::scope::TABLEOID_COLUMN
            {
                forbidden = Some(name.clone());
            }
        });
    }
    forbidden.map_or(Ok(()), |name| {
        Err(ExecError::InvalidObjectDefinition(format!(
            "cannot use system column \"{name}\" in MERGE WHEN condition"
        )))
    })
}

/// Check expressions against the relations that exist for each `WHEN` kind.
///
/// The executor keeps one joined scope so its row layout stays uniform. A
/// `NOT MATCHED` row has no target and a `NOT MATCHED BY SOURCE` row has no
/// source, though, so bind their expressions before execution rather than
/// letting a NULL-filled joined row make an invalid reference look valid.
fn validate_merge_clause_scopes(
    clauses: &[crabka_pgparser::ast::MergeWhen],
    target: &Scope,
    source: &Scope,
    joined: &Scope,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::{AssignmentValue, MergeAction, MergeMatchKind};

    let mut terminal_kinds = Vec::new();
    for clause in clauses {
        if terminal_kinds.contains(&clause.kind) {
            return Err(ExecError::Syntax(
                "unreachable WHEN clause specified after unconditional WHEN clause".into(),
            ));
        }
        if clause.condition.is_none() {
            terminal_kinds.push(clause.kind);
        }
    }
    let names = |scope: &Scope| {
        scope
            .columns
            .iter()
            .filter_map(|column| column.qualifier.clone())
            .collect::<HashSet<_>>()
    };
    let target_names = names(target);
    let source_names = names(source);
    let no_names = HashSet::new();
    for clause in clauses {
        let (visible, hidden) = match clause.kind {
            MergeMatchKind::Matched => (joined, &no_names),
            MergeMatchKind::NotMatchedByTarget => (source, &target_names),
            MergeMatchKind::NotMatchedBySource => (target, &source_names),
        };
        let bind = |expr: &Expr| {
            crate::bind::BoundExpr::new(expr, visible)
                .map(|_| ())
                .map_err(|error| merge_hidden_relation_error(error, hidden))
        };
        if let Some(condition) = &clause.condition {
            bind(condition)?;
        }
        match &clause.action {
            MergeAction::Insert {
                values: Some(values),
                ..
            } => {
                for value in values {
                    bind(value)?;
                }
            }
            MergeAction::Update(assignments) => {
                for assignment in assignments {
                    match &assignment.value {
                        AssignmentValue::Expr(value) => bind(value)?,
                        AssignmentValue::Row(values) => {
                            for value in values {
                                bind(value)?;
                            }
                        }
                        AssignmentValue::Subquery(_) => {}
                    }
                }
            }
            MergeAction::Delete
            | MergeAction::DoNothing
            | MergeAction::Insert { values: None, .. } => {}
        }
    }
    Ok(())
}

fn merge_hidden_relation_error(error: ExecError, hidden: &HashSet<String>) -> ExecError {
    match error {
        ExecError::MissingFromEntry(table) if hidden.contains(&table) => {
            ExecError::InvalidFromEntry {
                table,
                note: crate::error::FromEntryNote::TargetRelation,
            }
        }
        error => error,
    }
}

fn merge_row_touched_twice() -> ExecError {
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(
            "21000",
            "MERGE command cannot affect row a second time",
        )
        .with_hint("Ensure that not more than one source row matches any one target row."),
    )
}

/// The first `WHEN` clause of `kind` whose `AND` condition holds for this row.
fn pick_merge_clause<'a>(
    clauses: &'a [crabka_pgparser::ast::MergeWhen],
    kind: crabka_pgparser::ast::MergeMatchKind,
    scope: &Scope,
    row: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<Option<&'a crabka_pgparser::ast::MergeWhen>, ExecError> {
    for clause in clauses.iter().filter(|c| c.kind == kind) {
        if row_matches(clause.condition.as_ref(), scope, row, ctx)? {
            return Ok(Some(clause));
        }
    }
    Ok(None)
}

struct MergeRowAction<'a> {
    table: &'a Table,
    local_indexes: &'a [crabka_pgcatalog::Index],
    fk: &'a crate::fk::StatementFkContext,
    ctes: &'a crate::cte::CteContext,
    scope: &'a Scope,
    source_width: usize,
    stamp: &'a crate::scope::SystemStamp,
    rowid: u64,
    joined: &'a [Datum],
    action: &'a crabka_pgparser::ast::MergeAction,
    security: &'a MergeRowSecurity,
}

/// The `USING` qual each `MERGE` action judges an already-matched target row
/// against.
///
/// A plain `UPDATE` or `DELETE` applies its command's `USING` qual as a filter:
/// a row that fails it was never a candidate, and the statement reports a lower
/// count. A `MERGE` cannot do that. It found the row through its own join,
/// under the `SELECT` policies, and it has already decided which `WHEN` clause
/// the row meets — so dropping the row now would silently turn a matched row
/// into a row that matched nothing. `PostgreSQL` raises instead:
///
/// ```text
/// ERROR:  42501: target row violates row-level security policy (USING expression) for table "t"
/// ```
///
/// which is exactly [`crate::rls::CheckSubject::TargetRow`], the same shape
/// `INSERT … ON CONFLICT DO UPDATE` uses for the row it found and is about to
/// change.
///
/// Both quals are compiled once per statement rather than once per row. A
/// relation without row security costs nothing to compile: the decision
/// short-circuits on the relation's own flag before it reads a policy.
struct MergeRowSecurity {
    update: crate::rls::RowSecurityCheck,
    delete: crate::rls::RowSecurityCheck,
}

impl MergeRowSecurity {
    fn compile(write_ctx: &WriteContext<'_>, table: &Table) -> Result<Self, ExecError> {
        use crabka_pgcatalog::policy::PolicyCommand;
        let governor = write_ctx.governor(table);
        let compile = |command| {
            crate::rls::RowSecurityCheck::compile(
                &write_ctx.policy_read_ctx(),
                governor,
                command,
                crate::rls::CheckSubject::TargetRow,
            )
        };
        Ok(Self {
            update: compile(PolicyCommand::Update)?,
            delete: compile(PolicyCommand::Delete)?,
        })
    }
}

/// Apply an `UPDATE`/`DELETE` merge action to one already-matched target row,
/// under the same lock + `EvalPlanQual` recheck the ordinary write path uses.
async fn apply_merge_row_action(
    write_ctx: &WriteContext<'_>,
    request: &MergeRowAction<'_>,
    writes: &mut StatementWrites,
    ops: &mut Vec<crabka_pgkv::WriteOp>,
) -> Result<Option<ReturnedRow>, ExecError> {
    use crabka_pgparser::ast::MergeAction;

    let t = request.table;
    let ctx = write_ctx.eval_ctx;
    write_ctx
        .lockmgr
        .acquire_as(
            t.id,
            request.rowid,
            crate::lockmgr::LockMode::Exclusive,
            write_ctx.lock_owner,
            write_ctx.lock_wait_cap,
        )
        .await
        .map_err(lock_acquire_error)?;
    let Some((cur_rowid, cur_key_xid, cur_xmin, cur_cmin, _cur_cmax, cur_row)) = eval_plan_qual(
        &write_ctx.mutation(),
        t,
        request.rowid,
        crate::scope::GeneratedReads::every(),
    )?
    else {
        return Ok(None); // deleted by a concurrent committed transaction
    };
    let source = request.joined[t.columns.len()..t.columns.len() + request.source_width].to_vec();
    match request.action {
        MergeAction::Update(assignments) => {
            request.security.update.permit_row(t, &cur_row, ctx)?;
            let targets = resolve_assignments(write_ctx, request.ctes, t, assignments)?;
            let mut joined = cur_row.clone();
            joined.extend_from_slice(&source);
            request
                .stamp
                .extend_row(&mut joined, cur_rowid, cur_xmin, 0, write_ctx.command_id, 0);
            let next = apply_assignments(t, &targets, request.scope, &joined, ctx)?;
            let updated_columns = assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect::<Vec<_>>();
            let Some(next) = crate::trigger::fire_before_row(
                write_ctx.catalog_kv,
                crate::trigger::WriteTarget {
                    table: t,
                    check: &write_ctx.row_check(
                        t,
                        crabka_pgcatalog::policy::PolicyCommand::Update,
                        &updated_columns,
                    )?,
                },
                crate::trigger::DmlEvent::Update,
                &updated_columns,
                Some(&cur_row),
                Some(next),
                ctx,
            )?
            else {
                return Ok(None);
            };
            let new_rowid = apply_locked_row_update(
                write_ctx,
                t,
                request.local_indexes,
                request.fk,
                &LockedRowUpdate {
                    rowid: cur_rowid,
                    cur_key_xid,
                    cur_xmin,
                    cur_cmin,
                    cur_row: &cur_row,
                    next: &next,
                },
                writes,
                ops,
            )
            .await?;
            crate::trigger::fire_after_row(
                write_ctx.catalog_kv,
                t,
                crate::trigger::DmlEvent::Update,
                &updated_columns,
                Some(&cur_row),
                Some(&next),
                ctx,
            )?;
            Ok(Some(ReturnedRow {
                new: Some(next),
                old: Some(cur_row),
                source,
                old_xmin: cur_xmin,
                old_xmax: write_ctx.xid,
                old_cmin: cur_cmin,
                old_cmax: write_ctx.command_id,
                new_xmin: write_ctx.xid,
                new_xmax: 0,
                new_cmin: write_ctx.command_id,
                new_cmax: 0,
                action: Some("UPDATE"),
                old_identity: cur_rowid,
                new_identity: new_rowid,
            }))
        }
        MergeAction::Delete => {
            request.security.delete.permit_row(t, &cur_row, ctx)?;
            if crate::trigger::fire_before_row(
                write_ctx.catalog_kv,
                crate::trigger::WriteTarget {
                    table: t,
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
                return Ok(None);
            }
            // The deleted row's unique keys are free for a later part of this
            // statement, exactly as on the plain DELETE path.
            writes.release_row_keys(t, request.local_indexes, request.rowid, &cur_row, None)?;
            if !request.fk.is_empty() {
                writes
                    .fk_checks
                    .after_delete(request.fk, request.rowid, &cur_row)?;
            }
            if cur_xmin == write_ctx.xid {
                ops.push(crabka_pgkv::WriteOp::Put {
                    key: crabka_pgmvcc::version::version_key_xid(
                        t.id,
                        request.rowid,
                        write_ctx.xid,
                    ),
                    value: encode_table_tuple(
                        t,
                        write_ctx.xid,
                        write_ctx.xid,
                        0,
                        write_ctx.command_id,
                        &cur_row,
                    ),
                });
            } else {
                ops.push(crabka_pgkv::WriteOp::Put {
                    key: crabka_pgmvcc::version::version_key_xid(t.id, request.rowid, cur_key_xid),
                    value: encode_table_tuple(
                        t,
                        cur_xmin,
                        write_ctx.xid,
                        0,
                        write_ctx.command_id,
                        &cur_row,
                    ),
                });
            }
            crate::trigger::fire_after_row(
                write_ctx.catalog_kv,
                t,
                crate::trigger::DmlEvent::Delete,
                &[],
                Some(&cur_row),
                None,
                ctx,
            )?;
            Ok(Some(ReturnedRow {
                new: None,
                old: Some(cur_row),
                source,
                old_xmin: cur_xmin,
                old_xmax: write_ctx.xid,
                old_cmin: cur_cmin,
                old_cmax: write_ctx.command_id,
                new_xmin: 0,
                new_xmax: 0,
                new_cmin: 0,
                new_cmax: 0,
                action: Some("DELETE"),
                old_identity: NO_ROW_IDENTITY,
                new_identity: NO_ROW_IDENTITY,
            }))
        }
        MergeAction::DoNothing | MergeAction::Insert { .. } => Ok(None),
    }
}
