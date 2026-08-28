//! Runtime execution of rewrite rules.

use super::*;

/// distinction cannot be spelled in the AST's one boolean; see [`Reach`].
/// Execute one rule action after its `OLD`/`NEW` images have been bound.
///
/// `NOTIFY` is the one supported utility action. It shares the pending queue
/// used by a client-issued `NOTIFY` and `pg_notify`, so the outer write's
/// transaction still owns delivery, deduplication, and rollback.
pub(super) async fn execute_rule_action(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    action: &Statement,
    owner: &str,
    writes: &mut StatementWrites,
) -> Result<(WriteOutcome, Vec<crabka_pgkv::WriteOp>), ExecError> {
    if let Statement::Notify { channel, payload } = action {
        let pending = write_ctx.eval_ctx.notify.as_ref().ok_or_else(|| {
            ExecError::Unsupported("NOTIFY rule actions require a SQL session".into())
        })?;
        pending
            .lock()
            .expect("notify pending mutex")
            .queue_notify(channel, payload.as_deref().unwrap_or_default())
            .map_err(crate::session::notify_queue_error)?;
        return Ok((WriteOutcome::command("NOTIFY".into()), Vec::new()));
    }
    let action_ctx = WriteContext {
        fctx: ForeignCtx {
            current_user: owner,
            ..write_ctx.fctx
        },
        ..*write_ctx
    };
    Box::pin(execute_write_body(
        &action_ctx,
        ctes,
        action,
        writes,
        Reach::of(action),
    ))
    .await
}

pub(super) fn append_rule_returning(
    collected: &mut Option<Relation>,
    next: Option<Relation>,
) -> Result<(), ExecError> {
    let Some(next) = next else {
        return Ok(());
    };
    match collected {
        Some(collected) => {
            if collected.scope != next.scope {
                return Err(ExecError::Unsupported(
                    "INSTEAD rule actions must have matching RETURNING lists".into(),
                ));
            }
            collected.rows.extend(next.rows);
        }
        slot => *slot = Some(next),
    }
    Ok(())
}

fn rule_condition_matches(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    condition: &Expr,
    scope: &Scope,
    row: &[Datum],
) -> Result<bool, ExecError> {
    let read = write_ctx.read_ctx(ctes);
    let condition = crate::subquery::resolve_expr_skipping(&read, condition, &mut |node| {
        expression_contains_correlated_subquery(&read, node, scope)
    })?;
    if validate_correlated_subqueries(&read, &condition, scope)? {
        let mut binder = LateralBinder::new(read.catalog_kv, read.fctx.resolution, read.ctes);
        row_matches_correlated(&read, Some(&condition), scope, row, &mut binder)
    } else {
        row_matches(Some(&condition), scope, row, write_ctx.eval_ctx)
    }
}

/// Run the currently supported row-rule action shape against one inserted row.
/// The action re-enters the ordinary write executor after binding its row image,
/// so its target gets the same defaults, constraints, indexes and triggers as a
/// statement the client wrote directly.
pub(super) async fn fire_insert_rules(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    table: &Table,
    target_idx: &[usize],
    source_row: &[Expr],
    new_row: &[Datum],
    only_instead: Option<bool>,
    capture_returning: bool,
    writes: &mut StatementWrites,
) -> Result<(bool, Vec<crabka_pgkv::WriteOp>, Option<Relation>), ExecError> {
    let mut ops = Vec::new();
    let mut instead_matched = false;
    let mut returning = None;
    for rule in crabka_pgcatalog::rule::rules_for_table(write_ctx.catalog_kv, table.id)? {
        if !rule_is_enabled(rule.enabled)
            || rule.event != crabka_pgcatalog::rule::RuleEvent::Insert
            || only_instead.is_some_and(|instead| rule.instead != instead)
        {
            continue;
        }
        if let Some(condition) = rule.condition.as_deref() {
            let condition = crabka_pgparser::parser::parse_expression(condition)?;
            if !rule_condition_matches(
                write_ctx,
                ctes,
                &condition,
                &Scope::single(table, "new"),
                new_row,
            )? {
                continue;
            }
        }
        instead_matched |= rule.instead;
        if rule.action.eq_ignore_ascii_case("nothing") {
            continue;
        }
        // PostgreSQL's rewrite expansion evaluates the source expression tree
        // for each action. In particular, an omitted serial column evaluates
        // its `nextval` again, rather than copying the value the base insert
        // stored.
        let action_row = if rule.instead {
            new_row.to_vec()
        } else {
            build_insert_row(table, target_idx, source_row, write_ctx.eval_ctx)?
        };
        let source = rule
            .action
            .strip_prefix('(')
            .and_then(|action| action.strip_suffix(')'))
            .unwrap_or(&rule.action);
        for (action_index, mut action) in crabka_pgparser::parse(source)?.into_iter().enumerate() {
            if rule_action_is_statement_level(&rule)
                && !writes.claim_statement_rule_action(rule.oid, action_index)
            {
                continue;
            }
            bind_rule_action(&mut action, table, None, Some(&action_row))?;
            let query_action = matches!(action, Statement::Query(_));
            let (outcome, action_ops) =
                execute_rule_action(write_ctx, ctes, &action, &table.owner, writes).await?;
            ops.extend(action_ops);
            if query_action || (rule.instead && capture_returning) {
                append_rule_returning(&mut returning, outcome.returning)?;
            }
        }
    }
    Ok((instead_matched, ops, returning))
}

pub(super) fn matches_nothing_rule(
    catalog_kv: &dyn Kv,
    table: &Table,
    event: crabka_pgcatalog::rule::RuleEvent,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
    ctx: &crate::clock::EvalCtx,
) -> Result<bool, ExecError> {
    for rule in crabka_pgcatalog::rule::rules_for_table(catalog_kv, table.id)? {
        if !rule_is_enabled(rule.enabled)
            || rule.event != event
            || !rule.instead
            || !rule.action.eq_ignore_ascii_case("nothing")
        {
            continue;
        }
        let Some(condition) = rule.condition else {
            return Ok(true);
        };
        if old.is_none() && new.is_none() {
            continue;
        }
        let mut scope = Scope::empty();
        let mut row = Vec::new();
        if let Some(old) = old {
            scope.extend(&Scope::single(table, "old"));
            row.extend_from_slice(old);
        }
        if let Some(new) = new {
            scope.extend(&Scope::single(table, "new"));
            row.extend_from_slice(new);
        }
        let condition = crabka_pgparser::parser::parse_expression(&condition)?;
        if row_matches(Some(&condition), &scope, &row, ctx)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn fire_row_rules(
    write_ctx: &WriteContext<'_>,
    ctes: &crate::cte::CteContext,
    table: &Table,
    event: crabka_pgcatalog::rule::RuleEvent,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
    instead: bool,
    capture_returning: bool,
    writes: &mut StatementWrites,
) -> Result<(bool, Vec<crabka_pgkv::WriteOp>, Option<Relation>), ExecError> {
    let mut ops = Vec::new();
    let mut matched = false;
    let mut returning = None;
    for rule in crabka_pgcatalog::rule::rules_for_table(write_ctx.catalog_kv, table.id)? {
        if !rule_is_enabled(rule.enabled) || rule.event != event || rule.instead != instead {
            continue;
        }
        if let Some(condition) = rule.condition.as_deref() {
            let mut scope = Scope::empty();
            let mut row = Vec::new();
            if let Some(old) = old {
                scope.extend(&Scope::single(table, "old"));
                row.extend_from_slice(old);
            }
            if let Some(new) = new {
                scope.extend(&Scope::single(table, "new"));
                row.extend_from_slice(new);
            }
            let condition = crabka_pgparser::parser::parse_expression(condition)?;
            if !rule_condition_matches(write_ctx, ctes, &condition, &scope, &row)? {
                continue;
            }
        }
        matched = true;
        if rule.action.eq_ignore_ascii_case("nothing") {
            continue;
        }
        let source = rule
            .action
            .strip_prefix('(')
            .and_then(|action| action.strip_suffix(')'))
            .unwrap_or(&rule.action);
        for (action_index, mut action) in crabka_pgparser::parse(source)?.into_iter().enumerate() {
            if rule_action_is_statement_level(&rule)
                && !writes.claim_statement_rule_action(rule.oid, action_index)
            {
                continue;
            }
            bind_rule_action(&mut action, table, old, new)?;
            let query_action = matches!(action, Statement::Query(_));
            let (outcome, action_ops) =
                execute_rule_action(write_ctx, ctes, &action, &table.owner, writes).await?;
            ops.extend(action_ops);
            if query_action || (instead && capture_returning) {
                append_rule_returning(&mut returning, outcome.returning)?;
            }
        }
    }
    Ok((matched, ops, returning))
}

pub(super) fn rule_action_is_statement_level(rule: &crabka_pgcatalog::rule::Rule) -> bool {
    rule.condition.is_none()
        && !rule.action.to_ascii_lowercase().contains("old.")
        && !rule.action.to_ascii_lowercase().contains("new.")
}

pub(crate) fn rule_is_enabled(enabled: crabka_pgcatalog::trigger::TriggerEnabled) -> bool {
    let role = crate::session::current_setting_runtime("session_replication_role", false)
        .ok()
        .flatten()
        .unwrap_or_else(|| "origin".into());
    match enabled {
        crabka_pgcatalog::trigger::TriggerEnabled::Disabled => false,
        crabka_pgcatalog::trigger::TriggerEnabled::Always => true,
        crabka_pgcatalog::trigger::TriggerEnabled::Origin => role != "replica",
        crabka_pgcatalog::trigger::TriggerEnabled::Replica => role == "replica",
    }
}

pub(super) fn has_write_rewrite_rule(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<bool, ExecError> {
    let name = resolve_relation(kv, resolution, reference, SchemaDisposition::Reference)?;
    let table = crate::trigger::relation_trigger_table(kv, &name)?;
    Ok(crabka_pgcatalog::rule::rules_for_table(kv, table.id)?
        .into_iter()
        .any(|rule| {
            rule_is_enabled(rule.enabled) && rule.event != crabka_pgcatalog::rule::RuleEvent::Select
        }))
}
