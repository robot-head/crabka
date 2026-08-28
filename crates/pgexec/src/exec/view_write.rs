use super::*;

/// The catalog's spelling of a parsed check-option level.
///
/// The two enums are deliberately separate: `crabka_pgcatalog` is the durable
/// catalog and depends on no SQL grammar, so the executor is where the syntax
/// becomes storage.
pub(crate) const fn catalog_check_option(
    level: crabka_pgparser::ast::ViewCheckOption,
) -> crabka_pgcatalog::ViewCheckOption {
    match level {
        crabka_pgparser::ast::ViewCheckOption::Local => crabka_pgcatalog::ViewCheckOption::Local,
        crabka_pgparser::ast::ViewCheckOption::Cascaded => {
            crabka_pgcatalog::ViewCheckOption::Cascaded
        }
    }
}

/// A write statement rewritten onto the relation underneath a chain of views.
struct RewrittenViewWrite {
    stmt: Statement,
    /// The role the rewritten write is decided under — the innermost view's
    /// owner, unless a `security_invoker` view keeps the caller's identity.
    role: String,
    /// The check options collected on the way down, ready for the row that
    /// reaches storage.
    checks: Vec<crate::viewwrite::ViewCheck>,
    /// The views' `WHERE`s, for a `MERGE`, which has no `WHERE` of its own to
    /// fold them into. `None` for the other three statements, whose rewritten
    /// filter already carries them.
    target_qual: Option<Expr>,
}

/// Enforce the check options this statement was rewritten through against a row
/// an `INSTEAD OF` trigger produced.
///
/// The rewritten path gets this for free through
/// [`crate::rls::WriteChecks`], which only the table write path reaches. A chain
/// that bottoms out at a trigger-bearing view needs the same judgement made
/// here, on the view's own rowtype, which is the row `PostgreSQL` judges too.
fn permit_view_checks(
    write_ctx: &WriteContext<'_>,
    view: &Table,
    row: &[Datum],
    modified: &[String],
) -> Result<(), ExecError> {
    let ctx = write_ctx.eval_ctx;
    let scope = Scope::single(view, &view.name.name);
    for check in write_ctx.view_checks {
        if !row_matches(Some(&check.qual), &scope, row, ctx)? {
            return Err(ExecError::ViewCheckOptionViolation {
                view: check.view.clone(),
                row: describe_row(write_ctx, view, row, modified),
            });
        }
    }
    Ok(())
}

/// Rewrite `stmt` from the view it names onto the relation underneath.
fn rewrite_view_write(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    name: &crabka_pgcatalog::RelationName,
) -> Result<RewrittenViewWrite, ExecError> {
    use crabka_pgparser::ast::{InsertSource, SelectItem};

    let catalog_kv = write_ctx.catalog_kv;
    let resolution = write_ctx.eval_ctx.resolution();
    let stored = crabka_pgcatalog::get_view(catalog_kv, name)?;
    let writes = view_writes(stmt);
    let alias = view_write_alias(stmt);
    let qualifier = view_write_qualifier(name, alias).to_string();
    let privileges = write_ctx.privileges();
    let permit = |view: &crabka_pgcatalog::View, role: &str| {
        let ctx = crate::privilege::PrivilegeCtx::new(catalog_kv, role);
        for write in &writes {
            crate::privilege::require(
                &ctx,
                &view.name,
                &view.owner,
                crate::privilege::RelationKind::View,
                view_command_privilege(write.command()),
            )?;
        }
        Ok(())
    };
    let instead = |relation: &crabka_pgcatalog::RelationName| {
        let id = crate::catalog_rel::view_oids(catalog_kv)?
            .get(relation)
            .copied()
            .and_then(|oid| u32::try_from(oid).ok())
            .unwrap_or(0);
        crate::trigger::has_instead_row_trigger(
            catalog_kv,
            id,
            crate::trigger::DmlEvent::Insert,
            &[],
        )
        .and_then(|insert| {
            Ok(insert
                || crate::trigger::has_instead_row_trigger(
                    catalog_kv,
                    id,
                    crate::trigger::DmlEvent::Update,
                    &[],
                )?
                || crate::trigger::has_instead_row_trigger(
                    catalog_kv,
                    id,
                    crate::trigger::DmlEvent::Delete,
                    &[],
                )?)
        })
    };
    let view_ctx = crate::viewwrite::ViewWriteCtx {
        kv: catalog_kv,
        resolution,
        instead_trigger: &instead,
        permit: &permit,
    };
    let _ = &privileges;
    let rewrite = crate::viewwrite::resolve(
        &view_ctx,
        &stored,
        &writes,
        &qualifier,
        write_ctx.fctx.effective_role(),
    )?;
    let target = relation_ref_of(&rewrite.target);
    let sub = |expr: &Expr| rewrite.rewrite_statement_expr(expr, &qualifier);
    // Every column a statement names has to be one of the view's, exactly as
    // PostgreSQL resolves it against the view's rowtype before rewriting. Left
    // unchecked, a name the view does not project would resolve against the
    // base relation the rewrite substitutes in, and a write would silently read
    // a column the view hides.
    let check_names = |exprs: &[&Expr], strict: bool| -> Result<(), ExecError> {
        for expr in exprs {
            rewrite.reject_foreign_columns(expr, &qualifier, strict)?;
        }
        Ok(())
    };
    // The base columns an insert through the view assigns, from the column list
    // it wrote or — with none — the view's leading columns truncated to what
    // the source supplies. That is the rule `resolve_insert_targets` applies to
    // a table, applied one level up so the truncation counts *view* columns.
    // Shared with `MERGE`'s insert action, which spells the same thing.
    let insert_targets = |columns: &Option<Vec<String>>,
                          width: usize,
                          write: crate::viewwrite::ViewWrite|
     -> Result<Vec<String>, ExecError> {
        let named: Vec<String> = match columns {
            Some(written) => written.clone(),
            None => rewrite
                .columns
                .iter()
                .take(width)
                .map(|column| column.name.clone())
                .collect(),
        };
        let mapped = named
            .iter()
            .map(|column| rewrite.assignable(column, &name.name, write))
            .collect::<Result<Vec<_>, ExecError>>()?;
        // Two of the view's columns may select the same base column
        // (`SELECT a, b, a AS aa`), and an insert that names both would assign
        // it twice. PostgreSQL reports that against the *base* column, and
        // reports it here rather than letting the second value quietly win.
        let mut assigned = std::collections::HashSet::new();
        if let Some(repeated) = mapped.iter().find(|column| !assigned.insert(*column)) {
            return Err(ExecError::Syntax(format!(
                "multiple assignments to same column \"{repeated}\""
            )));
        }
        Ok(mapped)
    };
    // The same for an assignment list, shared with `MERGE`'s update action.
    let update_assignments = |assignments: &[crabka_pgparser::ast::Assignment],
                              write: crate::viewwrite::ViewWrite|
     -> Result<Vec<crabka_pgparser::ast::Assignment>, ExecError> {
        assignments
            .iter()
            .map(|assignment| {
                Ok(crabka_pgparser::ast::Assignment {
                    targets: assignment
                        .targets
                        .iter()
                        .map(|column| rewrite.assignable(column, &name.name, write))
                        .collect::<Result<Vec<_>, ExecError>>()?,
                    indirections: assignment.indirections.clone(),
                    value: rewrite_assignment_value(&assignment.value, &sub),
                })
            })
            .collect()
    };
    let returning_items = |returning: &Option<crabka_pgparser::ast::Returning>| {
        returning.as_ref().map(|returning| {
            let items = returning
                .items
                .iter()
                .flat_map(|item| match item {
                    SelectItem::Wildcard => rewrite.wildcard_items(),
                    SelectItem::QualifiedWildcard(written) if *written == qualifier => {
                        rewrite.wildcard_items()
                    }
                    other => vec![other.clone()],
                })
                .map(|item| match item {
                    // The output column keeps the name the *view* gave it.
                    // Substitution replaces a bare `aa` with the base column it
                    // was selected from, and a column reference names itself —
                    // so without an explicit alias the rewrite would rename the
                    // user's RETURNING column out from under them.
                    SelectItem::Expr { expr, alias } => SelectItem::Expr {
                        alias: alias.or_else(|| match &expr {
                            Expr::Column { name, .. } => Some(name.clone()),
                            _ => None,
                        }),
                        expr: sub(&expr),
                    },
                    other => other,
                })
                .collect();
            crabka_pgparser::ast::Returning {
                old_alias: returning.old_alias.clone(),
                new_alias: returning.new_alias.clone(),
                items,
            }
        })
    };

    let stmt = match stmt {
        Statement::Insert {
            columns,
            indirections,
            source,
            on_conflict,
            returning,
            ..
        } => {
            let width = match source {
                InsertSource::Values(rows) => rows.first().map_or(0, Vec::len),
                InsertSource::DefaultValues => 0,
                InsertSource::Query(query) => crate::query::describe_query_expr_with_ctes(
                    catalog_kv, resolution, query, ctes,
                )?
                .len(),
            };
            let mapped = insert_targets(
                columns,
                width,
                crate::viewwrite::ViewWrite::direct(crate::viewwrite::ViewCommand::Insert),
            )?;
            // Judged before substitution, on the list the user wrote: after it
            // every reference names a base column, including the ones `*`
            // expanded to, and there is nothing left to tell apart.
            rewrite.reject_foreign_returning(returning.as_ref(), &qualifier, true)?;
            let returning =
                returning_items(returning).map(|returning| crabka_pgparser::ast::Returning {
                    items: returning
                        .items
                        .iter()
                        .map(|item| match item {
                            SelectItem::Expr { expr, alias } => SelectItem::Expr {
                                expr: crate::viewwrite::ViewRewrite::unqualify_expr(
                                    expr, &qualifier,
                                ),
                                alias: alias.clone(),
                            },
                            other => other.clone(),
                        })
                        .collect(),
                    ..returning
                });
            let on_conflict = on_conflict
                .as_ref()
                .map(|clause| rewrite_view_conflict(clause, &rewrite, &qualifier, &name.name))
                .transpose()?;
            Statement::Insert {
                table: target,
                alias: alias.map(str::to_owned),
                columns: Some(mapped),
                indirections: indirections.clone(),
                source: source.clone(),
                with: None,
                on_conflict,
                returning,
            }
        }
        Statement::Update {
            assignments,
            from,
            filter,
            returning,
            ..
        } => {
            check_names(&filter.iter().collect::<Vec<_>>(), from.is_empty())?;
            rewrite.reject_foreign_returning(returning.as_ref(), &qualifier, from.is_empty())?;
            let assignments = update_assignments(
                assignments,
                crate::viewwrite::ViewWrite::direct(crate::viewwrite::ViewCommand::Update),
            )?;
            Statement::Update {
                table: target,
                only: true,
                with: None,
                alias: Some(qualifier.clone()),
                assignments,
                from: from.clone(),
                where_current_of: None,
                filter: rewrite.restrict(filter.as_ref().map(&sub)),
                returning: returning_items(returning),
            }
        }
        Statement::Delete {
            using,
            filter,
            returning,
            ..
        } => {
            check_names(&filter.iter().collect::<Vec<_>>(), using.is_empty())?;
            rewrite.reject_foreign_returning(returning.as_ref(), &qualifier, using.is_empty())?;
            Statement::Delete {
                table: target,
                only: true,
                with: None,
                alias: Some(qualifier.clone()),
                using: using.clone(),
                where_current_of: None,
                filter: rewrite.restrict(filter.as_ref().map(&sub)),
                returning: returning_items(returning),
            }
        }
        Statement::Merge {
            source,
            on,
            clauses,
            returning,
            ..
        } => {
            use crabka_pgparser::ast::{MergeAction, MergeWhen};

            // A MERGE always has a source relation of its own, so an
            // unqualified name in the `ON` condition, a clause condition or the
            // `RETURNING` list may be the source's rather than the view's and
            // cannot be judged against the view alone — the same reason an
            // `UPDATE … FROM` relaxes the test.
            check_names(&[on], false)?;
            rewrite.reject_foreign_returning(returning.as_ref(), &qualifier, false)?;
            let clauses = clauses
                .iter()
                .map(|when| {
                    if let Some(condition) = &when.condition {
                        check_names(&[condition], false)?;
                    }
                    let action = match &when.action {
                        MergeAction::DoNothing => MergeAction::DoNothing,
                        MergeAction::Delete => MergeAction::Delete,
                        MergeAction::Update(assignments) => {
                            MergeAction::Update(update_assignments(
                                assignments,
                                crate::viewwrite::ViewWrite::merged(
                                    crate::viewwrite::ViewCommand::Update,
                                ),
                            )?)
                        }
                        // `INSERT DEFAULT VALUES` names no column and supplies
                        // no value, so it stays spelled that way rather than
                        // becoming an empty target list.
                        MergeAction::Insert {
                            columns,
                            indirections,
                            overriding,
                            values: Some(values),
                        } => MergeAction::Insert {
                            columns: Some(insert_targets(
                                columns,
                                values.len(),
                                crate::viewwrite::ViewWrite::merged(
                                    crate::viewwrite::ViewCommand::Insert,
                                ),
                            )?),
                            indirections: indirections.clone(),
                            overriding: *overriding,
                            values: Some(values.iter().map(&sub).collect()),
                        },
                        MergeAction::Insert {
                            overriding,
                            values: None,
                            ..
                        } => MergeAction::Insert {
                            columns: None,
                            indirections: None,
                            overriding: *overriding,
                            values: None,
                        },
                    };
                    Ok(MergeWhen {
                        kind: when.kind,
                        condition: when.condition.as_ref().map(&sub),
                        action,
                    })
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            Statement::Merge {
                table: target,
                with: None,
                alias: Some(qualifier.clone()),
                source: source.clone(),
                on: sub(on),
                clauses,
                returning: returning_items(returning),
            }
        }
        _ => unreachable!("view DML only accepts INSERT, UPDATE, DELETE, or MERGE"),
    };
    // A MERGE reaches its target rows through a scan the statement never
    // filters, so the views' quals cannot ride the statement text and travel
    // beside it instead.
    let target_qual = matches!(stmt, Statement::Merge { .. })
        .then(|| rewrite.restrict(None))
        .flatten();
    Ok(RewrittenViewWrite {
        stmt,
        role: rewrite.run_as.clone(),
        checks: rewrite.row_checks(&qualifier),
        target_qual,
    })
}

/// Rewrite an `ON CONFLICT` clause from the view's columns onto the relation
/// underneath.
///
/// The arbiter's inference columns and the `DO UPDATE SET` targets are
/// assignments and take the same refusal an ordinary target column does; the
/// expressions go through the `excluded`-aware rewrite.
fn rewrite_view_conflict(
    clause: &crabka_pgparser::ast::OnConflict,
    rewrite: &crate::viewwrite::ViewRewrite,
    qualifier: &str,
    view: &str,
) -> Result<crabka_pgparser::ast::OnConflict, ExecError> {
    use crabka_pgparser::ast::{OnConflict, OnConflictAction, OnConflictTarget};

    let write = crate::viewwrite::ViewWrite::direct(crate::viewwrite::ViewCommand::Insert);
    // An INSERT has no alias to hang the statement's qualifier on, so a
    // reference to the *stored* row moves onto the target's bare relation name;
    // `excluded` keeps its own qualifier, which is not the statement's.
    let target_name = rewrite.target.name.clone();
    let sub = |expr: &Expr| {
        crate::viewwrite::ViewRewrite::requalify_expr(
            &rewrite.rewrite_conflict_expr(expr, qualifier),
            qualifier,
            &target_name,
        )
    };
    let target = match &clause.target {
        OnConflictTarget::Columns {
            columns,
            inference_columns,
            index_predicate,
        } => OnConflictTarget::Columns {
            columns: columns
                .iter()
                .map(|column| rewrite.assignable(column, view, write))
                .collect::<Result<Vec<_>, ExecError>>()?,
            inference_columns: inference_columns.clone(),
            index_predicate: index_predicate.as_ref().map(&sub),
        },
        other => other.clone(),
    };
    let action = match &clause.action {
        OnConflictAction::DoNothing => OnConflictAction::DoNothing,
        OnConflictAction::DoUpdate {
            assignments,
            filter,
        } => OnConflictAction::DoUpdate {
            assignments: assignments
                .iter()
                .map(|(column, expr)| Ok((rewrite.assignable(column, view, write)?, sub(expr))))
                .collect::<Result<Vec<_>, ExecError>>()?,
            filter: filter.as_ref().map(&sub),
        },
    };
    Ok(OnConflict { target, action })
}

/// Apply a rewrite to an assignment's right-hand side, whichever spelling it
/// has.
fn rewrite_assignment_value(
    value: &crabka_pgparser::ast::AssignmentValue,
    sub: &impl Fn(&Expr) -> Expr,
) -> crabka_pgparser::ast::AssignmentValue {
    use crabka_pgparser::ast::AssignmentValue;
    match value {
        AssignmentValue::Expr(expr) => AssignmentValue::Expr(sub(expr)),
        AssignmentValue::Row(exprs) => AssignmentValue::Row(exprs.iter().map(sub).collect()),
        AssignmentValue::Subquery(query) => AssignmentValue::Subquery(query.clone()),
    }
}

/// A `RelationRef` naming a resolved relation, for a statement this executor
/// builds rather than parses.
fn relation_ref_of(name: &crabka_pgcatalog::RelationName) -> crabka_pgparser::ast::RelationRef {
    crabka_pgparser::ast::RelationRef {
        schema: Some(name.schema.clone()),
        name: name.name.clone(),
    }
}

/// Whether the `INSTEAD OF` row triggers on `view` perform this `MERGE`'s
/// actions instead of the rewrite.
///
/// A `MERGE` is answered by the triggers only when every command its clauses
/// reach has one, and rewritten only when none of them does. `PostgreSQL`
/// refuses the mixture, but which refusal it raises depends on what the actions
/// with no trigger could otherwise have done:
///
/// * The body is automatically updatable, so those actions had a rewrite
///   available and the view now asks for both mechanisms at once. That is the
///   0A000 naming the view.
/// * The body is not, so those actions have no way to run at all, and the
///   refusal is the ordinary "cannot … view" one owed to the first of them —
///   the trigger a user has to add, not the mixture.
///
/// # Errors
///
/// One of those two refusals, and catalog errors.
fn merge_instead_of_triggers(
    write_ctx: &WriteContext<'_>,
    view: &Table,
    stored: &crabka_pgcatalog::View,
    stmt: &Statement,
) -> Result<bool, ExecError> {
    let writes = view_writes(stmt);
    let mut uncovered = Vec::new();
    let mut covered = 0_usize;
    for write in writes {
        let event = match write.command() {
            crate::viewwrite::ViewCommand::Insert => crate::trigger::DmlEvent::Insert,
            crate::viewwrite::ViewCommand::Update => crate::trigger::DmlEvent::Update,
            crate::viewwrite::ViewCommand::Delete => crate::trigger::DmlEvent::Delete,
        };
        if crate::trigger::has_instead_row_trigger(write_ctx.catalog_kv, view.id, event, &[])? {
            covered += 1;
        } else {
            uncovered.push(write);
        }
    }
    let Some(first_uncovered) = uncovered.first().copied() else {
        return Ok(covered > 0);
    };
    if covered == 0 {
        return Ok(false);
    }
    match crate::viewwrite::body_refusal(stored) {
        Some(detail) => Err(ExecError::ViewNotUpdatable {
            message: first_uncovered.refusal(&view.name.name),
            detail,
            hint: first_uncovered.hint(),
        }),
        None => Err(ExecError::Remote(
            crabka_pgwire::error::PgError::error(
                "0A000",
                format!("cannot merge into view \"{}\"", view.name.name),
            )
            .with_detail(
                "MERGE is not supported for views with INSTEAD OF triggers for some actions but \
                 not all.",
            )
            .with_hint(
                "To enable merging into the view, either provide a full set of INSTEAD OF \
                 triggers or drop the existing INSTEAD OF triggers.",
            ),
        )),
    }
}

/// The privilege one command performed through a view needs on it.
const fn view_command_privilege(
    command: crate::viewwrite::ViewCommand,
) -> crate::privilege::Privilege {
    match command {
        crate::viewwrite::ViewCommand::Insert => crate::privilege::Privilege::Insert,
        crate::viewwrite::ViewCommand::Update => crate::privilege::Privilege::Update,
        crate::viewwrite::ViewCommand::Delete => crate::privilege::Privilege::Delete,
    }
}

/// Dispatch a write that named a view.
///
/// Two paths, and which one applies is decided by whether the view carries an
/// `INSTEAD OF` row trigger for this event: the trigger performs the write
/// itself, or — when there is none — the statement is rewritten onto the
/// relation underneath and handed back to [`execute_write_body`].
pub(super) async fn execute_view_dml(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let reference = match stmt {
        Statement::Insert { table, .. }
        | Statement::Update { table, .. }
        | Statement::Delete { table, .. }
        | Statement::Merge { table, .. } => table,
        _ => unreachable!("view DML only accepts INSERT, UPDATE, DELETE, or MERGE"),
    };
    let name = resolve_relation(
        write_ctx.catalog_kv,
        write_ctx.eval_ctx.resolution(),
        reference,
        SchemaDisposition::Reference,
    )?;
    let view = crate::trigger::relation_trigger_table(write_ctx.catalog_kv, &name)?;
    let ctx = write_ctx.eval_ctx;
    let (event, updated) = match stmt {
        Statement::Insert { .. } => (crate::trigger::DmlEvent::Insert, Vec::new()),
        Statement::Update { assignments, .. } => (
            crate::trigger::DmlEvent::Update,
            assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect(),
        ),
        _ => (crate::trigger::DmlEvent::Delete, Vec::new()),
    };
    // A MERGE whose every clause is `DO NOTHING` performs no command, so it
    // asks the view for no privilege, no updatability and no rewrite — and it
    // reaches no row, whatever the join finds. `PostgreSQL` accepts it on a
    // view no write could ever be rewritten through, which is the only way a
    // read-only view can appear as a MERGE target at all.
    if let Statement::Merge { .. } = stmt
        && view_writes(stmt).is_empty()
    {
        return Ok((WriteOutcome::command("MERGE 0".into()), Vec::new()));
    }
    let instead = if let Statement::Merge { .. } = stmt {
        let stored = crabka_pgcatalog::get_view(write_ctx.catalog_kv, &name)?;
        merge_instead_of_triggers(write_ctx, &view, &stored, stmt)?
    } else {
        crate::trigger::has_instead_row_trigger(write_ctx.catalog_kv, view.id, event, &updated)?
    };
    let rewrite_event = match event {
        crate::trigger::DmlEvent::Insert => crabka_pgcatalog::rule::RuleEvent::Insert,
        crate::trigger::DmlEvent::Update => crabka_pgcatalog::rule::RuleEvent::Update,
        crate::trigger::DmlEvent::Delete => crabka_pgcatalog::rule::RuleEvent::Delete,
        crate::trigger::DmlEvent::Truncate => unreachable!("views cannot be truncated"),
    };
    if !matches!(stmt, Statement::Merge { .. })
        && has_instead_view_rule(write_ctx.catalog_kv, &view, rewrite_event)?
    {
        return Box::pin(execute_view_rewrite_rules(
            write_ctx, write_ctx, ctes, stmt, &view, true, writes,
        ))
        .await;
    }
    if !instead {
        let rewritten = rewrite_view_write(write_ctx, ctes, stmt, &name)?;
        let inner = WriteContext {
            fctx: ForeignCtx {
                current_user: &rewritten.role,
                ..write_ctx.fctx
            },
            view_checks: &rewritten.checks,
            merge_target_qual: rewritten.target_qual.as_ref(),
            ..*write_ctx
        };
        let (outcome, mut ops) = Box::pin(execute_write_body(
            &inner,
            ctes,
            &rewritten.stmt,
            writes,
            Reach::Storage,
        ))
        .await?;
        if matches!(stmt, Statement::Merge { .. }) {
            return Ok((outcome, ops));
        }
        let staged = StagedKv::new(write_ctx.kv, &ops);
        let action_ctx = WriteContext {
            kv: &staged,
            // The rewritten base write is this command's first query.  A
            // `DO ALSO` action runs after it, at the next command counter, so
            // its scan can see the base rows now layered over storage.
            command_id: write_ctx.command_id.wrapping_add(1),
            ..*write_ctx
        };
        let (_, action_ops) = Box::pin(execute_view_rewrite_rules(
            write_ctx,
            &action_ctx,
            ctes,
            stmt,
            &view,
            false,
            writes,
        ))
        .await?;
        ops.extend(action_ops);
        return Ok((outcome, ops));
    }

    match stmt {
        Statement::Insert {
            alias,
            columns,
            indirections,
            source,
            on_conflict,
            returning,
            ..
        } => {
            if on_conflict.is_some() {
                return Err(ExecError::Unsupported(
                    "INSERT ... ON CONFLICT is not supported on views".into(),
                ));
            }
            let (targets, rows) =
                insert_source_rows(write_ctx, ctes, &view, columns, indirections, source)?;
            let spec = ReturningSpec::new(
                &view,
                table_qualifier(&view, alias),
                returning.as_ref(),
                None,
                false,
            )?;
            let mut returned = Vec::new();
            let mut count = 0_u64;
            for row in rows {
                // A view reaches no `BEFORE ROW` trigger — its own row trigger
                // is `INSTEAD OF` — so the settle that
                // [`crate::trigger::fire_before_row`] performs for a stored
                // relation is spelled here. A view has no generated column and
                // no `CHECK` of its own, so this is the domain and `NOT NULL`
                // of its column types; the view's own `WITH CHECK OPTION` is
                // judged below, on the row the trigger returns.
                let mut proposed =
                    build_insert_row_with_subscripts(&view, &targets, indirections, &row, ctx)?;
                finish_written_row(&view, &mut proposed, ctx)?;
                let Some(result) = crate::trigger::fire_instead_row(
                    write_ctx.catalog_kv,
                    &view,
                    crate::trigger::DmlEvent::Insert,
                    &[],
                    None,
                    Some(proposed),
                    ctx,
                )?
                else {
                    continue;
                };
                permit_view_checks(
                    write_ctx,
                    &view,
                    &result,
                    &WriteContext::modified_columns(&view, &targets),
                )?;
                count += 1;
                if returning.is_some() {
                    returned.push(ReturnedRow {
                        new: Some(result),
                        old: None,
                        source: Vec::new(),
                        old_xmin: 0,
                        old_xmax: 0,
                        old_cmin: 0,
                        old_cmax: 0,
                        new_xmin: 0,
                        new_xmax: 0,
                        new_cmin: 0,
                        new_cmax: 0,
                        action: None,
                        old_identity: NO_ROW_IDENTITY,
                        new_identity: NO_ROW_IDENTITY,
                    });
                }
            }
            Ok((
                spec.outcome(format!("INSERT 0 {count}"), returned, ctx)?,
                Vec::new(),
            ))
        }
        Statement::Update {
            alias,
            assignments,
            from,
            filter,
            returning,
            ..
        } => {
            let qualifier = table_qualifier(&view, alias);
            let read = write_ctx.read_ctx(ctes);
            let target_expr = crabka_pgparser::ast::TableExpr::Table {
                name: reference.clone(),
                // Pinned to `true` whatever the statement wrote. UPDATE and
                // DELETE do not descend into inheritance children yet, so
                // scanning them here would collect rows the write path has no
                // way to address. `Statement::{Update,Delete}::only` carries
                // what was written; this is the line that starts reading it the
                // day DML recursion lands.
                only: true,
                alias: alias.clone(),
                columns: None,
                sample: None,
            };
            let target_rows = build_from(
                &read,
                std::slice::from_ref(&target_expr),
                None,
                None,
                None,
                None,
            )?
            .rows;
            // `None`: a view's rows come out of its own query and carry no
            // storage identity, so the target offers no system column.
            let source = DmlSource::build(write_ctx, ctes, &view, qualifier, from, None)?;
            let filter = source.resolve_filter(&read, filter.as_ref())?;
            let mut binder = filter
                .as_ref()
                .map(|filter| validate_correlated_subqueries(&read, filter, &source.scope))
                .transpose()?
                .unwrap_or(false)
                .then(|| LateralBinder::new(read.catalog_kv, read.fctx.resolution, read.ctes));
            let bound_filter = if binder.is_none() {
                source.bind_filter(filter.as_ref())?
            } else {
                None
            };
            let targets = resolve_assignments(write_ctx, ctes, &view, assignments)?;
            let spec = ReturningSpec::new(
                &view,
                qualifier,
                returning.as_ref(),
                Some(&source.scope),
                false,
            )?;
            let updated: Vec<String> = assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect();
            let mut returned = Vec::new();
            let mut count = 0_u64;
            for old in target_rows {
                let Some(joined) = (if let Some(binder) = &mut binder {
                    source.first_match_correlated(
                        &read,
                        filter.as_ref(),
                        &old,
                        NO_ROW_IDENTITY,
                        0,
                        binder,
                    )?
                } else {
                    source.first_match(bound_filter.as_ref(), &old, NO_ROW_IDENTITY, 0, ctx)?
                }) else {
                    continue;
                };
                // Settled here for the reason the insert arm above states.
                let mut proposed = apply_assignments(&view, &targets, &source.scope, &joined, ctx)?;
                finish_written_row(&view, &mut proposed, ctx)?;
                let Some(result) = crate::trigger::fire_instead_row(
                    write_ctx.catalog_kv,
                    &view,
                    crate::trigger::DmlEvent::Update,
                    &updated,
                    Some(&old),
                    Some(proposed),
                    ctx,
                )?
                else {
                    continue;
                };
                permit_view_checks(write_ctx, &view, &result, &updated)?;
                count += 1;
                if returning.is_some() {
                    returned.push(ReturnedRow::updated(
                        result,
                        old,
                        joined[view.columns.len()..].to_vec(),
                        NO_ROW_IDENTITY,
                        NO_ROW_IDENTITY,
                        0,
                        0,
                        0,
                        0,
                    ));
                }
            }
            Ok((
                spec.outcome(format!("UPDATE {count}"), returned, ctx)?,
                Vec::new(),
            ))
        }
        Statement::Delete {
            alias,
            using,
            filter,
            returning,
            ..
        } => {
            let qualifier = table_qualifier(&view, alias);
            let read = write_ctx.read_ctx(ctes);
            let target_expr = crabka_pgparser::ast::TableExpr::Table {
                name: reference.clone(),
                // Pinned to `true` whatever the statement wrote. UPDATE and
                // DELETE do not descend into inheritance children yet, so
                // scanning them here would collect rows the write path has no
                // way to address. `Statement::{Update,Delete}::only` carries
                // what was written; this is the line that starts reading it the
                // day DML recursion lands.
                only: true,
                alias: alias.clone(),
                columns: None,
                sample: None,
            };
            let target_rows = build_from(
                &read,
                std::slice::from_ref(&target_expr),
                None,
                None,
                None,
                None,
            )?
            .rows;
            // `None`, for the reason the view `UPDATE` above passes it.
            let source = DmlSource::build(write_ctx, ctes, &view, qualifier, using, None)?;
            let filter = source.resolve_filter(&read, filter.as_ref())?;
            let mut binder = filter
                .as_ref()
                .map(|filter| validate_correlated_subqueries(&read, filter, &source.scope))
                .transpose()?
                .unwrap_or(false)
                .then(|| LateralBinder::new(read.catalog_kv, read.fctx.resolution, read.ctes));
            let bound_filter = if binder.is_none() {
                source.bind_filter(filter.as_ref())?
            } else {
                None
            };
            let spec = ReturningSpec::new(
                &view,
                qualifier,
                returning.as_ref(),
                Some(&source.scope),
                false,
            )?;
            let mut returned = Vec::new();
            let mut count = 0_u64;
            for old in target_rows {
                let Some(joined) = (if let Some(binder) = &mut binder {
                    source.first_match_correlated(
                        &read,
                        filter.as_ref(),
                        &old,
                        NO_ROW_IDENTITY,
                        0,
                        binder,
                    )?
                } else {
                    source.first_match(bound_filter.as_ref(), &old, NO_ROW_IDENTITY, 0, ctx)?
                }) else {
                    continue;
                };
                let Some(result) = crate::trigger::fire_instead_row(
                    write_ctx.catalog_kv,
                    &view,
                    crate::trigger::DmlEvent::Delete,
                    &[],
                    Some(&old),
                    None,
                    ctx,
                )?
                else {
                    continue;
                };
                count += 1;
                if returning.is_some() {
                    returned.push(ReturnedRow {
                        new: None,
                        old: Some(result),
                        source: joined[view.columns.len()..].to_vec(),
                        old_xmin: 0,
                        old_xmax: 0,
                        old_cmin: 0,
                        old_cmax: 0,
                        new_xmin: 0,
                        new_xmax: 0,
                        new_cmin: 0,
                        new_cmax: 0,
                        action: None,
                        old_identity: NO_ROW_IDENTITY,
                        new_identity: NO_ROW_IDENTITY,
                    });
                }
            }
            Ok((
                spec.outcome(format!("DELETE {count}"), returned, ctx)?,
                Vec::new(),
            ))
        }
        // A MERGE whose actions are all covered by `INSTEAD OF` triggers gets
        // here. `PostgreSQL` runs it by firing the trigger for whichever action
        // each row takes; nothing does that yet, and a rewrite would write the
        // relation underneath a view whose author asked for the trigger to
        // decide instead.
        Statement::Merge { .. } => Err(ExecError::Unsupported(
            "MERGE into a view with INSTEAD OF triggers is not supported".into(),
        )),
        _ => unreachable!(),
    }
}
