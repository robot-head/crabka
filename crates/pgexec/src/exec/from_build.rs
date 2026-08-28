use super::*;

/// Build the relation for one FROM list (comma items folded as cross joins).
pub(crate) fn build_from(
    read_ctx: &crate::subquery::SubCtx<'_>,
    from: &[crabka_pgparser::ast::TableExpr],
    bounds: Option<&ScanBounds>,
    scan_plan: Option<&crate::plan_dist::DistributedScanPlan>,
    filter: Option<&Expr>,
    pruned_columns: Option<&[ColumnBinding]>,
) -> Result<Relation, ExecError> {
    let mut iter = from.iter();
    let first = iter
        .next()
        .ok_or_else(|| ExecError::Unsupported("build_from on empty FROM".into()))?;
    reject_from_clause_aggregates(
        read_ctx,
        first,
        &crabka_pgparser::ast::JoinConstraint::None,
        &Scope::empty(),
    )?;
    let mut acc = prune_relation_columns(
        build_table_expr(read_ctx, first, bounds, scan_plan, filter)?,
        pruned_columns,
    );
    let mut acc_is_security_free = security_free_from_item(read_ctx, first);
    for te in iter {
        let next_is_security_free = security_free_from_item(read_ctx, te);
        acc = append_from_item(
            read_ctx,
            acc,
            te,
            crabka_pgparser::ast::JoinKind::Cross,
            &crabka_pgparser::ast::JoinConstraint::None,
            filter,
            pruned_columns,
            acc_is_security_free,
            next_is_security_free,
        )?;
        acc_is_security_free &= next_is_security_free;
    }
    Ok(acc)
}

/// Reject an aggregate in `te` — or in the join constraint attaching it — that
/// `PostgreSQL` assigns to the query level whose FROM clause `outer` describes.
fn reject_from_clause_aggregates(
    read_ctx: &crate::subquery::SubCtx<'_>,
    te: &crabka_pgparser::ast::TableExpr,
    constraint: &crabka_pgparser::ast::JoinConstraint,
    outer: &Scope,
) -> Result<(), ExecError> {
    FromClauseAggregatePass {
        levels: AggregateLevels {
            read_ctx,
            statement: outer,
        },
    }
    .check(te, constraint)
}

/// Join one more FROM item onto the accumulated relation.
pub(crate) fn append_from_item(
    read_ctx: &crate::subquery::SubCtx<'_>,
    acc: Relation,
    te: &crabka_pgparser::ast::TableExpr,
    kind: crabka_pgparser::ast::JoinKind,
    constraint: &crabka_pgparser::ast::JoinConstraint,
    filter: Option<&Expr>,
    pruned_columns: Option<&[ColumnBinding]>,
    acc_is_security_free: bool,
    next_is_security_free: bool,
) -> Result<Relation, ExecError> {
    reject_from_clause_aggregates(read_ctx, te, constraint, &acc.scope)?;
    if !is_lateral_item(te, &acc.scope) {
        let mut acc = acc;
        let mut next = build_table_expr(read_ctx, te, None, None, None).map_err(|error| {
            if item_is_lateral(te) {
                error
            } else {
                explain_outer_reference(error, &acc.scope, OuterReference::Sibling)
            }
        })?;
        next = prune_relation_columns(next, pruned_columns);
        if let Some(filter) = filter {
            push_local_where(
                &mut acc,
                &mut next,
                kind,
                filter,
                read_ctx.eval_ctx,
                acc_is_security_free,
                next_is_security_free,
            )?;
        }
        let pushed_constraint = filter
            .filter(|filter| {
                matches!(kind, crabka_pgparser::ast::JoinKind::Cross)
                    && matches!(constraint, crabka_pgparser::ast::JoinConstraint::None)
                    && immutable_row_predicate(filter)
                    && {
                        let mut scope = acc.scope.clone();
                        scope.extend(&next.scope);
                        crate::eval::check_predicate_resolves(filter, &scope).is_ok()
                    }
            })
            .map(|filter| crabka_pgparser::ast::JoinConstraint::On(filter.clone()));
        return join_relations(
            acc,
            next,
            kind,
            pushed_constraint.as_ref().unwrap_or(constraint),
            read_ctx.eval_ctx,
            read_ctx.join_policy(),
        );
    }
    let mut acc = acc;
    if let Some(filter) = filter {
        push_left_where(
            &mut acc,
            kind,
            filter,
            read_ctx.eval_ctx,
            acc_is_security_free,
        )?;
    }
    lateral_join(read_ctx, acc, te, kind, constraint)
}

/// Whether every relation this FROM item reads is a synthesized catalog
/// relation, which cannot have a row-security policy.
pub(crate) fn security_free_from_item(
    read_ctx: &crate::subquery::SubCtx<'_>,
    item: &crabka_pgparser::ast::TableExpr,
) -> bool {
    use crabka_pgparser::ast::TableExpr;
    match item {
        TableExpr::Table { name, .. } => {
            if name.schema.is_none()
                && (read_ctx.ctes.lookup(&name.name).is_some()
                    || read_ctx
                        .eval_ctx
                        .transition_relations
                        .as_ref()
                        .is_some_and(|runtime| {
                            runtime
                                .lock()
                                .expect("transition relation mutex")
                                .contains_key(&name.name)
                        }))
            {
                return false;
            }
            crate::relname::resolve_relation(
                read_ctx.catalog_kv,
                read_ctx.fctx.resolution,
                name,
                crate::relname::SchemaDisposition::Reference,
            )
            .is_ok_and(|name| is_virtual_relation(&name))
        }
        TableExpr::Join { left, right, .. } => {
            security_free_from_item(read_ctx, left) && security_free_from_item(read_ctx, right)
        }
        _ => false,
    }
}
