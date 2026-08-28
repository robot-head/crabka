use super::*;

/// The write path (INSERT/UPDATE/DELETE) with concurrent writers (SP6).
///
/// It builds version write ops tagged with the transaction's `xid` and returns
/// them without writing. The session batches and commits them once. INSERT
/// allocates rowids through the `SequenceManager`; UPDATE/DELETE lock each
/// candidate row and re-check its state under EvalPlanQual. A concurrent
/// committed change is 40001 under REPEATABLE READ, or a re-find under READ
/// COMMITTED. Reads resolve with `satisfies_mvcc` and the txn's own xid
/// (read-your-writes).
pub(crate) async fn execute_write(
    write_ctx: &WriteContext<'_>,
    stmt: &Statement,
) -> Result<(QueryResult, Vec<crabka_pgkv::WriteOp>), ExecError> {
    crate::srf::reject_write_calls(stmt)?;
    let span = execute_write_span(write_ctx, stmt);
    let triggers_before = crate::trigger::fired_count();
    let mut writes =
        StatementWrites::for_command(write_ctx.command_row_claims, write_ctx.trigger_write);
    let written = execute_write_with_ctes(write_ctx, write_ctx.ctes, stmt, &mut writes)
        .instrument(span.clone())
        .await;
    let (outcome, ops) = match written {
        Ok(written) => written,
        Err(error) => {
            let rendered = error.clone().into_pg();
            crate::telemetry::record_error(&span, &rendered.code, &rendered.message);
            return Err(error);
        }
    };
    record_write_outcome(
        &span,
        &outcome,
        &ops,
        crate::trigger::fired_count().saturating_sub(triggers_before),
    );
    Ok((outcome.into_result(write_ctx.eval_ctx), ops))
}

/// Build the span covering one data-modifying statement's execution.
///
/// This is guarded, not built unconditionally. A resolution of the target
/// relation costs a name resolution and a catalog read, and a span macro's
/// field expressions evaluate whether or not the callsite is enabled.
fn execute_write_span(write_ctx: &WriteContext<'_>, stmt: &Statement) -> tracing::Span {
    if !tracing::enabled!(target: crate::telemetry::EXEC_TARGET, tracing::Level::DEBUG) {
        return tracing::Span::none();
    }
    let span = tracing::debug_span!(
        target: crate::telemetry::EXEC_TARGET,
        "pg.execute_write",
        otel.kind = "internal",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        db.response.status_code = tracing::field::Empty,
        "error.type" = tracing::field::Empty,
        db.collection.name = tracing::field::Empty,
        pg.table_id = tracing::field::Empty,
        pg.rows_affected = tracing::field::Empty,
        pg.write_ops = tracing::field::Empty,
        pg.index_ops = tracing::field::Empty,
        pg.fk_checks = tracing::field::Empty,
        pg.triggers_fired = tracing::field::Empty,
        pg.returning = tracing::field::Empty,
    );
    if let Some(relation) = crate::telemetry::statement_relation(stmt) {
        span.record("db.collection.name", relation.name.as_str());
        // A statement whose target does not resolve — a `CREATE TABLE AS`, a
        // name that is about to fail — records the name it wrote and no id,
        // rather than failing the span build.
        if let Ok(resolved) = resolve_relation(
            write_ctx.catalog_kv,
            write_ctx.fctx.resolution,
            relation,
            SchemaDisposition::Reference,
        ) && let Ok(table) = crabka_pgcatalog::get_table(write_ctx.catalog_kv, &resolved)
        {
            span.record("pg.table_id", crate::telemetry::integer(table.id));
        }
    }
    span
}

/// What a data-modifying statement produced: the command tag, plus the relation
/// its `RETURNING` clause projected (absent when the statement had none).
pub(crate) struct WriteOutcome {
    pub(super) tag: String,
    pub(super) returning: Option<Relation>,
}

impl WriteOutcome {
    pub(super) fn command(tag: String) -> Self {
        Self {
            tag,
            returning: None,
        }
    }

    fn into_result(self, ctx: &crate::clock::EvalCtx) -> QueryResult {
        match self.returning {
            None => QueryResult::Command { tag: self.tag },
            Some(rel) => {
                let fields = rel
                    .scope
                    .columns
                    .iter()
                    .map(|c| field(&c.name, c.ty))
                    .collect();
                rows_result_with_tag(fields, &rel.rows, ctx.output_style(), self.tag)
            }
        }
    }
}

/// Run the whole of one data-modifying statement: its `WITH` list, its body, and
/// then the referential checks all of those queued.
///
/// The drain is here rather than in each part because `PostgreSQL` treats the
/// `WITH`-list-plus-body as ONE command and fires its `AFTER ROW` trigger queue
/// once for it.
async fn execute_write_with_ctes(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let mut statement_triggers = Vec::new();
    if let Some(with) = statement_with_clause(stmt) {
        for cte in &with.ctes {
            if let crabka_pgparser::ast::CteBody::Dml(dml) = &cte.body {
                statement_triggers.extend(statement_trigger_targets(write_ctx, dml)?);
            }
        }
    }
    statement_triggers.extend(statement_trigger_targets(write_ctx, stmt)?);
    for (table, event, updated) in &statement_triggers {
        crate::trigger::fire_statement(
            write_ctx.catalog_kv,
            table,
            *event,
            crabka_pgcatalog::trigger::TriggerTiming::Before,
            updated,
            write_ctx.eval_ctx,
        )?;
    }
    let (outcome, mut ops) = execute_write_parts(write_ctx, ctes, stmt, writes).await?;
    let fk_ops = drain_statement_fk_checks(write_ctx, writes, &ops).await?;
    ops.extend(fk_ops);
    drain_statement_unique_checks(write_ctx, writes, &ops)?;
    for (table, event, updated) in statement_triggers.iter().rev() {
        crate::trigger::fire_statement(
            write_ctx.catalog_kv,
            table,
            *event,
            crabka_pgcatalog::trigger::TriggerTiming::After,
            updated,
            write_ctx.eval_ctx,
        )?;
    }
    Ok((outcome, ops))
}

/// Every relation a `TRUNCATE` empties, before `CASCADE` is consulted.
///
/// A target without `ONLY` stands for its whole inheritance tree, exactly as it
/// does for `UPDATE` and `DELETE`: `TRUNCATE parent` leaves an inheritance
/// child empty in `PostgreSQL`, and `TRUNCATE ONLY parent` is the spelling that
/// spares it. Both call sites read this list — the one that empties the
/// relations and the one that collects their statement triggers — so a child
/// pulled in here is both truncated and gets its `BEFORE TRUNCATE` trigger
/// fired, which is what `PostgreSQL` does.
///
/// Names are deduplicated: two targets in one list may share a descendant, and
/// so may the two routes to one relation in a multiple-inheritance DAG.
pub(super) fn truncate_names(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    targets: &[crabka_pgparser::ast::TruncateTarget],
) -> Result<Vec<crabka_pgparser::ast::RelationRef>, ExecError> {
    let mut out = Vec::with_capacity(targets.len());
    let mut seen = HashSet::new();
    for target in targets {
        let named = resolve_relation(kv, resolution, &target.name, SchemaDisposition::Utility)?;
        // The one member of the `ONLY` family that refuses rather than answers.
        // `SELECT`/`UPDATE`/`DELETE … ONLY` over a partitioned parent all read
        // or write its own — empty — row space and report nothing done, but
        // `TRUNCATE ONLY` is 42809 on 18.4, on the grounds that a partitioned
        // parent has no storage to truncate and the statement is therefore a
        // mistake rather than a no-op.
        if target.only && crate::partition::is_partitioned(kv, &named)? {
            return Err(ExecError::TruncateOnlyPartitioned);
        }
        let mut tree = vec![named.clone()];
        if !target.only {
            tree.extend(crate::inheritance::descendants(kv, &named)?);
        }
        for relation in tree {
            if seen.insert(relation.clone()) {
                out.push(crabka_pgparser::ast::RelationRef::qualified(
                    &relation.schema,
                    &relation.name,
                ));
            }
        }
    }
    Ok(out)
}

fn statement_trigger_targets(
    write_ctx: &WriteContext<'_>,
    stmt: &Statement,
) -> Result<Vec<(Table, crate::trigger::DmlEvent, Vec<String>)>, ExecError> {
    if let Statement::Update { table, .. } = stmt {
        let named = resolve_relation(
            write_ctx.catalog_kv,
            write_ctx.eval_ctx.resolution(),
            table,
            SchemaDisposition::Reference,
        )?;
        if named.schema == crate::search_path::PG_CATALOG && named.name == "pg_class" {
            return Ok(Vec::new());
        }
    }
    if let Statement::Truncate {
        targets, cascade, ..
    } = stmt
    {
        let written = truncate_names(
            write_ctx.catalog_kv,
            write_ctx.eval_ctx.resolution(),
            targets,
        )?;
        let names = resolve_relations(
            write_ctx.catalog_kv,
            write_ctx.eval_ctx.resolution(),
            &written,
            SchemaDisposition::Utility,
        )?;
        let tables = names
            .iter()
            .map(|name| {
                // This runs before the TRUNCATE arm itself, so a wrong-kind
                // target has to be refused here or it is reported as missing.
                match truncate_wrong_kind(write_ctx.catalog_kv, name) {
                    Some(error) => Err(error),
                    None => Ok(crabka_pgcatalog::get_table(write_ctx.catalog_kv, name)?),
                }
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        return Ok(
            crate::fk::expand_truncate_set(write_ctx.catalog_kv, &tables, *cascade)?
                .tables
                .into_iter()
                .map(|table| (table, crate::trigger::DmlEvent::Truncate, Vec::new()))
                .collect(),
        );
    }
    if let Statement::Insert {
        table,
        on_conflict:
            Some(crabka_pgparser::ast::OnConflict {
                action: crabka_pgparser::ast::OnConflictAction::DoUpdate { assignments, .. },
                ..
            }),
        ..
    } = stmt
    {
        let name = resolve_relation(
            write_ctx.catalog_kv,
            write_ctx.eval_ctx.resolution(),
            table,
            SchemaDisposition::Reference,
        )?;
        let table = crate::trigger::relation_trigger_table(write_ctx.catalog_kv, &name)?;
        return Ok(vec![
            (table.clone(), crate::trigger::DmlEvent::Insert, Vec::new()),
            (
                table,
                crate::trigger::DmlEvent::Update,
                assignments
                    .iter()
                    .map(|(column, _)| column.clone())
                    .collect(),
            ),
        ]);
    }
    if let Statement::Merge { table, clauses, .. } = stmt {
        let name = resolve_relation(
            write_ctx.catalog_kv,
            write_ctx.eval_ctx.resolution(),
            table,
            SchemaDisposition::Reference,
        )?;
        let table = crate::trigger::relation_trigger_table(write_ctx.catalog_kv, &name)?;
        let mut insert = false;
        let mut delete = false;
        let mut updated = Vec::new();
        for clause in clauses {
            match &clause.action {
                crabka_pgparser::ast::MergeAction::Insert { .. } => insert = true,
                crabka_pgparser::ast::MergeAction::Delete => delete = true,
                crabka_pgparser::ast::MergeAction::Update(assignments) => {
                    for column in assignments
                        .iter()
                        .flat_map(|assignment| assignment.targets.iter())
                    {
                        if !updated.contains(column) {
                            updated.push(column.clone());
                        }
                    }
                }
                crabka_pgparser::ast::MergeAction::DoNothing => {}
            }
        }
        let mut targets = Vec::new();
        if insert {
            targets.push((table.clone(), crate::trigger::DmlEvent::Insert, Vec::new()));
        }
        if !updated.is_empty() {
            targets.push((table.clone(), crate::trigger::DmlEvent::Update, updated));
        }
        if delete {
            targets.push((table, crate::trigger::DmlEvent::Delete, Vec::new()));
        }
        return Ok(targets);
    }
    let (reference, event, updated) = match stmt {
        Statement::Insert { table, .. } => (table, crate::trigger::DmlEvent::Insert, Vec::new()),
        Statement::Update {
            table, assignments, ..
        } => (
            table,
            crate::trigger::DmlEvent::Update,
            assignments
                .iter()
                .flat_map(|assignment| assignment.targets.iter().cloned())
                .collect(),
        ),
        Statement::Delete { table, .. } => (table, crate::trigger::DmlEvent::Delete, Vec::new()),
        _ => return Ok(Vec::new()),
    };
    let name = resolve_relation(
        write_ctx.catalog_kv,
        write_ctx.eval_ctx.resolution(),
        reference,
        SchemaDisposition::Reference,
    )?;
    let table = crate::trigger::relation_trigger_table(write_ctx.catalog_kv, &name)?;
    if crabka_pgcatalog::get_view(write_ctx.catalog_kv, &name).is_ok()
        && !crate::trigger::has_instead_row_trigger(
            write_ctx.catalog_kv,
            table.id,
            event,
            &updated,
        )?
    {
        return Ok(Vec::new());
    }
    Ok(vec![(table, event, updated)])
}

/// Evaluate the statement's `WITH` list, then the statement body against that
/// CTE scope.
///
/// The `WITH` list includes any data-modifying entries, which run exactly once
/// each whether or not the body references them. The referential checks they
/// queue are left for [`execute_write_with_ctes`] to drain once for all of
/// them.
///
/// Every entry sees the statement's own snapshot: a data-modifying CTE's rows
/// are staged as write ops and never written to the KV here, so neither a later
/// CTE nor the body can observe them, exactly as in `PostgreSQL`.
///
/// The parts run in `PostgreSQL`'s order, which is observable whenever two of
/// them touch the same row (whichever runs first is the one whose change
/// survives, because [`StatementWrites`] then holds the row against the other).
/// `PostgreSQL` runs a data-modifying item when something first demands its
/// rows, and runs the items nothing demands AFTER the main query, in reverse
/// list order, the order `ExecPostprocessPlan` walks `es_auxmodifytables`.
async fn execute_write_parts(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    let Some(with) = statement_with_clause(stmt) else {
        return execute_write_body(write_ctx, ctes, stmt, writes, Reach::of(stmt)).await;
    };
    let mut ops = Vec::new();
    let mut scope = ctes.child();
    // The body is stripped up front: the reference check below must see it
    // without the `WITH` list, whose own names would shadow the item.
    let body = statement_without_with(stmt);
    let mut deferred = Vec::new();
    for (index, cte) in with.ctes.iter().enumerate() {
        let rel = match &cte.body {
            crabka_pgparser::ast::CteBody::Query(_) => {
                let read = write_ctx.read_ctx(&scope);
                crate::cte::evaluate_cte_relation(&read, cte, with.recursive, &scope)?
            }
            crabka_pgparser::ast::CteBody::Dml(dml) => {
                reject_unsupported_rule_for_data_modifying_cte(write_ctx, dml)?;
                if !cte_is_referenced(with, &body, index, &cte.name) {
                    // Nothing demands its rows, so it runs after the body.
                    deferred.push(dml);
                    continue;
                }
                let (outcome, cte_ops) = Box::pin(execute_write_body(
                    write_ctx,
                    &scope,
                    dml,
                    writes,
                    Reach::of(dml),
                ))
                .await?;
                ops.extend(cte_ops);
                let Some(rel) = outcome.returning else {
                    // Only a *reference* to a data-modifying item without a
                    // RETURNING clause is refused, and this item is referenced.
                    return Err(ExecError::Unsupported(format!(
                        "WITH query \"{}\" does not have a RETURNING clause",
                        cte.name
                    )));
                };
                // `evaluate_cte_relation` applies the aliases for a query item.
                crate::cte::apply_cte_column_aliases(rel, &cte.name, &cte.columns)?
            }
        };
        scope.insert(cte.name.clone(), rel);
    }
    let (outcome, body_ops) = Box::pin(execute_write_body(
        write_ctx,
        &scope,
        &body,
        writes,
        Reach::of(&body),
    ))
    .await?;
    ops.extend(body_ops);
    for dml in deferred.into_iter().rev() {
        let (_, cte_ops) = Box::pin(execute_write_body(
            write_ctx,
            &scope,
            dml,
            writes,
            Reach::of(dml),
        ))
        .await?;
        ops.extend(cte_ops);
    }
    Ok((outcome, ops))
}

fn reject_unsupported_rule_for_data_modifying_cte(
    write_ctx: &WriteContext<'_>,
    stmt: &Statement,
) -> Result<(), ExecError> {
    use crabka_pgcatalog::rule::RuleEvent;

    let (reference, event) = match stmt {
        Statement::Insert { table, .. } => (table, RuleEvent::Insert),
        Statement::Update { table, .. } => (table, RuleEvent::Update),
        Statement::Delete { table, .. } => (table, RuleEvent::Delete),
        _ => return Ok(()),
    };
    let table = resolve_relation(
        write_ctx.catalog_kv,
        write_ctx.eval_ctx.resolution(),
        reference,
        SchemaDisposition::Reference,
    )?;
    let table = crabka_pgcatalog::get_table(write_ctx.catalog_kv, &table)?;
    for rule in crabka_pgcatalog::rule::rules_for_table(write_ctx.catalog_kv, table.id)? {
        if rule.event != event || !rule_is_enabled(rule.enabled) {
            continue;
        }
        if rule.instead && rule.condition.is_some() {
            return Err(ExecError::Unsupported(
                "conditional DO INSTEAD rules are not supported for data-modifying statements in WITH"
                    .into(),
            ));
        }
        if !rule.instead {
            return Err(ExecError::Unsupported(
                "DO ALSO rules are not supported for data-modifying statements in WITH".into(),
            ));
        }
        if rule.action.eq_ignore_ascii_case("nothing") {
            return Err(ExecError::Unsupported(
                "DO INSTEAD NOTHING rules are not supported for data-modifying statements in WITH"
                    .into(),
            ));
        }
        let action = rule
            .action
            .strip_prefix('(')
            .and_then(|action| action.strip_suffix(')'))
            .unwrap_or(&rule.action);
        let actions = crabka_pgparser::parse(action)?;
        if actions.len() > 1 {
            return Err(ExecError::Unsupported(
                "multi-statement DO INSTEAD rules are not supported for data-modifying statements in WITH"
                    .into(),
            ));
        }
        match actions.as_slice() {
            [Statement::Notify { .. }] => {
                return Err(ExecError::Unsupported(
                    "DO INSTEAD NOTIFY rules are not supported for data-modifying statements in WITH"
                        .into(),
                ));
            }
            [
                Statement::Insert {
                    source: crabka_pgparser::ast::InsertSource::Query(_),
                    ..
                },
            ] => {
                return Err(ExecError::Unsupported(
                    "INSERT ... SELECT rule actions are not supported for queries having data-modifying statements in WITH"
                        .into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Whether anything after `WITH` item `index` names it: a later item, or the
/// statement body.
fn cte_is_referenced(
    with: &crabka_pgparser::ast::WithClause,
    stmt: &Statement,
    index: usize,
    name: &str,
) -> bool {
    with.ctes[index + 1..]
        .iter()
        .any(|later| match &later.body {
            crabka_pgparser::ast::CteBody::Query(query) => {
                crate::cte::query_references(query, name)
            }
            crabka_pgparser::ast::CteBody::Dml(dml) => statement_references_relation(dml, name),
        })
        || statement_references_relation(stmt, name)
}

/// Whether a statement's relation positions name `name`.
fn statement_references_relation(stmt: &Statement, name: &str) -> bool {
    use crabka_pgparser::ast::{CreateAsSource, InsertSource, MergeSource};
    match stmt {
        Statement::Query(query) => crate::cte::query_references(query, name),
        Statement::CreateTableAs { source, .. } => match source {
            CreateAsSource::Query(query) => crate::cte::query_references(query, name),
            CreateAsSource::Execute { .. } => false,
        },
        Statement::Insert { source, .. } => match source {
            InsertSource::Query(query) => crate::cte::query_references(query, name),
            InsertSource::Values(_) | InsertSource::DefaultValues => false,
        },
        Statement::Update { from, .. } => from.iter().any(|item| table_expr_references(item, name)),
        Statement::Delete { using, .. } => {
            using.iter().any(|item| table_expr_references(item, name))
        }
        Statement::Merge { source, .. } => match source {
            MergeSource::Table { name: source, .. } => source.name == *name,
            MergeSource::Query { query, .. } => crate::cte::query_references(query, name),
        },
        _ => false,
    }
}

fn table_expr_references(item: &crabka_pgparser::ast::TableExpr, name: &str) -> bool {
    use crabka_pgparser::ast::TableExpr;
    match item {
        TableExpr::Table { name: source, .. } => source.name == *name,
        TableExpr::Derived { subquery, .. } => crate::cte::query_references(subquery, name),
        TableExpr::Join { left, right, .. } => {
            table_expr_references(left, name) || table_expr_references(right, name)
        }
        _ => false,
    }
}

/// The `WITH` list attached to a statement, when it has one.
pub(crate) fn statement_with_clause(stmt: &Statement) -> Option<&crabka_pgparser::ast::WithClause> {
    match stmt {
        Statement::Query(q) => q.with.as_ref(),
        Statement::CreateTableAs {
            source: crabka_pgparser::ast::CreateAsSource::Query(query),
            ..
        } => query.with.as_ref(),
        Statement::Insert { with, .. }
        | Statement::Update { with, .. }
        | Statement::Delete { with, .. }
        | Statement::Merge { with, .. } => with.as_ref(),
        _ => None,
    }
}

/// The same statement with its `WITH` list removed. The CTE relations are
/// already materialized into the scope the body executes against.
fn statement_without_with(stmt: &Statement) -> Statement {
    let mut stmt = stmt.clone();
    match &mut stmt {
        Statement::Query(q) => q.with = None,
        Statement::CreateTableAs {
            source: crabka_pgparser::ast::CreateAsSource::Query(query),
            ..
        } => query.with = None,
        Statement::Insert { with, .. }
        | Statement::Update { with, .. }
        | Statement::Delete { with, .. }
        | Statement::Merge { with, .. } => *with = None,
        _ => {}
    }
    stmt
}

/// Fold every uncorrelated subquery in a data-modifying statement's expression
/// clauses to a constant, under this statement's snapshot and CTE scope.
///
/// The write path's evaluator executes no subqueries of its own, so this is what
/// lets `UPDATE … WHERE k IN (SELECT …)` and a `MERGE` condition over a CTE work
/// at all. Each one is evaluated once for the statement, which is `PostgreSQL`'s
/// behavior for an uncorrelated subquery.
pub(super) fn resolve_write_subqueries(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    stmt: &Statement,
) -> Result<Statement, ExecError> {
    use crabka_pgparser::ast::{
        AssignmentValue, InsertSource, MergeAction, OnConflictAction, Returning, SelectItem,
    };

    let read = write_ctx.read_ctx(ctes);
    let resolve = |expr: &Expr| crate::subquery::resolve_expr(&read, expr);
    let resolve_opt = |expr: &Option<Expr>| -> Result<Option<Expr>, ExecError> {
        expr.as_ref().map(&resolve).transpose()
    };
    let resolve_assignments =
        |assignments: &mut Vec<crabka_pgparser::ast::Assignment>| -> Result<(), ExecError> {
            for assignment in assignments {
                match &mut assignment.value {
                    AssignmentValue::Expr(expr) => *expr = resolve(expr)?,
                    AssignmentValue::Row(items) => {
                        for item in items {
                            *item = resolve(item)?;
                        }
                    }
                    AssignmentValue::Subquery(_) => {}
                }
            }
            Ok(())
        };
    // `RETURNING` is evaluated per written row, but an uncorrelated subquery in
    // it has the same value for every one of them — so it folds here with the
    // rest of the statement rather than being refused. A `RETURNING *` carries
    // no expression to fold.
    let resolve_returning = |returning: &mut Option<Returning>| -> Result<(), ExecError> {
        let Some(returning) = returning.as_mut() else {
            return Ok(());
        };
        for item in &mut returning.items {
            if let SelectItem::Expr { expr, .. } = item {
                *expr = resolve(expr)?;
            }
        }
        Ok(())
    };
    // `ON CONFLICT DO UPDATE`'s assignments and its `WHERE` are the update the
    // statement runs when the insert collides. They are ordinary write-side
    // expressions and fold like `UPDATE`'s own.
    let resolve_on_conflict =
        |on_conflict: &mut Option<crabka_pgparser::ast::OnConflict>| -> Result<(), ExecError> {
            let Some(on_conflict) = on_conflict.as_mut() else {
                return Ok(());
            };
            match &mut on_conflict.action {
                OnConflictAction::DoUpdate {
                    assignments,
                    filter,
                } => {
                    for (_, expr) in assignments.iter_mut() {
                        *expr = resolve(expr)?;
                    }
                    *filter = resolve_opt(filter)?;
                }
                OnConflictAction::DoNothing => {}
            }
            Ok(())
        };

    let mut stmt = stmt.clone();
    match &mut stmt {
        Statement::Insert {
            source,
            on_conflict,
            returning,
            ..
        } => {
            // An `INSERT … SELECT` source is a query, and the read path folds
            // its own subqueries; only the `VALUES` form holds expressions this
            // pass owns.
            if let InsertSource::Values(rows) = source {
                for row in rows {
                    for value in row {
                        *value = resolve(value)?;
                    }
                }
            }
            resolve_on_conflict(on_conflict)?;
            resolve_returning(returning)?;
        }
        Statement::Update {
            assignments,
            returning,
            ..
        } => {
            resolve_assignments(assignments)?;
            // The DML source resolves this after its target and `FROM` scopes
            // exist, so correlated subqueries keep their outer bindings.
            resolve_returning(returning)?;
        }
        Statement::Delete { returning, .. } => {
            // See UPDATE above.
            resolve_returning(returning)?;
        }
        Statement::Merge {
            on,
            clauses,
            returning,
            ..
        } => {
            *on = resolve(on)?;
            for clause in clauses {
                clause.condition = resolve_opt(&clause.condition)?;
                match &mut clause.action {
                    MergeAction::Update(assignments) => resolve_assignments(assignments)?,
                    MergeAction::Insert {
                        values: Some(values),
                        ..
                    } => {
                        for value in values {
                            *value = resolve(value)?;
                        }
                    }
                    MergeAction::Insert { .. } | MergeAction::Delete | MergeAction::DoNothing => {}
                }
            }
            resolve_returning(returning)?;
        }
        _ => {}
    }
    Ok(stmt)
}
