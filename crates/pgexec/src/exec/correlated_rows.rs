use super::*;

/// Rewrite every select-list, `ORDER BY` and `DISTINCT ON` expression that reads
/// the source row into a reference to a hidden column, and return the plan that
/// fills those columns in.
///
/// Doing it as a rewrite rather than by threading a subquery context down into
/// the projection is what keeps an ordinary select list on its existing path:
/// once the statement carries no correlated expression, nothing downstream can
/// tell this pass ran.
pub(super) fn plan_correlated_row_exprs(
    read_ctx: &crate::subquery::SubCtx<'_>,
    select: &SelectStmt,
    outer: &Scope,
) -> Result<(SelectStmt, Option<CorrelatedRowExprs>), ExecError> {
    // With no source row there is nothing to correlate to. Grouping is excluded
    // for a different reason: its output row is a fold of many source rows, and
    // a hidden column carrying one source row's value would not survive it.
    // `PostgreSQL` allows the shape when the correlation only reads grouped
    // columns; here it keeps reporting the missing FROM entry it reports today.
    if outer.width() == 0 || crate::grouping::is_grouping_query(select) {
        return Ok((select.clone(), None));
    }
    // An aggregate written inside a sub-select can belong to THIS query level,
    // which makes the statement a grouping query that [`is_grouping_query`]
    // cannot see because no aggregate is written at this level. Deferring then
    // answers one row per source row where `PostgreSQL` folds them to one, so
    // such a statement stays on the ordinary path with the rest of them.
    if defers_statement_level_aggregate(read_ctx, select, outer) {
        return Ok((select.clone(), None));
    }
    let mut plan = CorrelatedRowExprs::default();
    let mut rewritten = select.clone();
    for item in &mut rewritten.projection {
        let SelectItem::Expr { expr, alias } = item else {
            continue;
        };
        let Some(marker) = plan.defer(read_ctx, expr, outer)? else {
            continue;
        };
        // The marker is a column reference, and an unaliased item is labelled
        // after its expression — so the label has to be pinned before the
        // expression it was derived from is gone.
        if alias.is_none() {
            *alias = Some(derived_name(expr));
        }
        *expr = marker;
    }
    for item in &mut rewritten.order_by {
        if let Some(marker) = plan.defer(read_ctx, &item.expr, outer)? {
            item.expr = marker;
        }
    }
    if let crabka_pgparser::ast::DistinctClause::On(on) = &mut rewritten.distinct {
        for expr in on {
            if let Some(marker) = plan.defer(read_ctx, expr, outer)? {
                *expr = marker;
            }
        }
    }
    if plan.exprs.is_empty() {
        return Ok((select.clone(), None));
    }
    Ok((rewritten, Some(plan)))
}

/// The reference standing in for the deferred expression at `index`.
pub(super) fn correlated_marker(index: usize) -> Expr {
    Expr::Column {
        table: Some(crate::scope::CORRELATED_QUALIFIER.to_string()),
        name: index.to_string(),
    }
}

/// Is this binding one of the hidden columns [`plan_correlated_row_exprs`] adds?
/// `SELECT *` must not expand to them.
pub(super) fn is_correlated_binding(c: &ColumnBinding) -> bool {
    c.qualifier.as_deref() == Some(crate::scope::CORRELATED_QUALIFIER)
}

/// Does a select list or `ORDER BY` refer to a hidden correlated column?
pub(super) fn select_mentions_correlated_marker(s: &SelectStmt) -> bool {
    s.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_mentions_correlated_marker(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }) || s
        .order_by
        .iter()
        .any(|item| expr_mentions_correlated_marker(&item.expr))
}

fn expr_mentions_correlated_marker(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Column { table: Some(table), .. } if table == crate::scope::CORRELATED_QUALIFIER
    ) || expr_children(expr)
        .into_iter()
        .any(expr_mentions_correlated_marker)
}

/// Evaluate the deferred expressions for every source row, appending their
/// values as the hidden columns the rewritten statement refers to.
///
/// Each row is bound and folded exactly the way a correlated `WHERE` is, so a
/// dead CASE branch or an unselected COALESCE argument still does not run its
/// subquery, and an uncorrelated subquery nested under one runs once.
pub(super) fn materialize_correlated_row_exprs(
    read_ctx: &crate::subquery::SubCtx<'_>,
    plan: CorrelatedRowExprs,
    scope: &mut Scope,
    rows: &mut [Vec<Datum>],
) -> Result<(), ExecError> {
    let CorrelatedRowExprs {
        sources: _,
        exprs,
        bindings,
        initplans,
        scalar_lookups,
    } = plan;
    let mut binder =
        LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes)
            .with_initplans(initplans)
            .with_scalar_lookups(scalar_lookups);
    for row in rows.iter_mut() {
        let mut values = Vec::with_capacity(exprs.len());
        for (walk, expr) in exprs.iter().enumerate() {
            binder.walking(walk);
            let (bound, _) = binder.bind_expr(expr, scope, row)?;
            let folded =
                fold_correlated_lazy_expressions(read_ctx, &bound, scope, row, &mut binder)?;
            let initialized = resolve_lazy_initplans(read_ctx, &folded, &mut binder)?;
            let resolved = crate::subquery::resolve_expr(read_ctx, &initialized)?;
            values.push(crate::eval::eval(&resolved, scope, row, read_ctx.eval_ctx)?);
        }
        row.extend(values);
    }
    scope.columns.extend(bindings);
    Ok(())
}

/// Resolve a SELECT while deferring only WHERE subtrees that read the SELECT's
/// source row. Ordinary siblings retain the existing once-only resolution.
///
/// Other clauses deliberately stay on their existing path. Projection, HAVING,
/// and window expressions run at different stages; evaluating them here would
/// incorrectly execute a subquery for rows an earlier stage discards.
pub(super) fn prepare_correlated_subqueries(
    read_ctx: &crate::subquery::SubCtx<'_>,
    select: &SelectStmt,
    outer: &Scope,
) -> Result<
    (
        SelectStmt,
        bool,
        Vec<LazyInitPlan>,
        Vec<CorrelatedScalarLookup>,
    ),
    ExecError,
> {
    let mut without_filter = select.clone();
    let filter = without_filter.filter.take();
    let mut resolved = crate::subquery::resolve_in_select(read_ctx, &without_filter)?;
    let correlated = filter
        .as_ref()
        .map(|filter| validate_correlated_subqueries(read_ctx, filter, outer))
        .transpose()?
        .unwrap_or(false);
    let mut initplans = Vec::new();
    let mut scalar_lookups = Vec::new();
    resolved.filter = filter
        .as_ref()
        .map(|filter| {
            let planned = install_lazy_initplans(
                read_ctx,
                filter,
                outer,
                false,
                &mut initplans,
                &mut scalar_lookups,
            )?;
            crate::subquery::resolve_expr_skipping(read_ctx, &planned, &mut |node| {
                let candidate = scalar_lookup_parts(node).is_some()
                    || direct_subquery(node).is_some()
                    || matches!(node, Expr::Func(_) | Expr::Case { .. });
                scalar_lookup_parts(node).is_some()
                    || candidate && expression_contains_correlated_subquery(read_ctx, node, outer)
            })
        })
        .transpose()?;
    if correlated {
        let nulls = vec![Datum::Null; outer.width()];
        let mut binder =
            LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes);
        let (bound, _) = binder.bind_expr(
            resolved
                .filter
                .as_ref()
                .expect("a correlated filter is present"),
            outer,
            &nulls,
        )?;
        let typed =
            type_without_evaluating_subqueries(read_ctx, &bound, &initplans, &scalar_lookups)?;
        crate::eval::check_predicate_resolves(&typed, outer)?;
        let result_type = crate::eval::infer_type(&typed, outer)?;
        if result_type != ColumnType::Bool {
            return Err(ExecError::TypeMismatch(format!(
                "argument of WHERE must be type boolean, not type {}",
                result_type.name()
            )));
        }
    }
    Ok((resolved, correlated, initplans, scalar_lookups))
}

pub(super) fn validate_correlated_subqueries(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
    outer: &Scope,
) -> Result<bool, ExecError> {
    let mut correlated = false;
    if let Some(query) = direct_subquery(expr) {
        let nulls = vec![Datum::Null; outer.width()];
        let mut binder =
            LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes);
        let (bound, direct_correlated) = binder.bind_query(query, outer, &nulls)?;
        let fields = crate::query::describe_query_expr_with_ctes(
            read_ctx.catalog_kv,
            read_ctx.fctx.resolution,
            &bound,
            read_ctx.ctes,
        )?;
        if !matches!(expr, Expr::Exists(_)) && fields.len() != 1 {
            return Err(ExecError::SubqueryColumns);
        }
        correlated |= direct_correlated;
    }
    for child in expr_children(expr) {
        correlated |= validate_correlated_subqueries(read_ctx, child, outer)?;
    }
    Ok(correlated)
}

pub(super) fn contains_subquery(expr: &Expr) -> bool {
    direct_subquery(expr).is_some() || expr_children(expr).into_iter().any(contains_subquery)
}

fn contains_pending_subquery(expr: &Expr) -> bool {
    initplan_parts(expr).is_some()
        || scalar_lookup_parts(expr).is_some()
        || direct_subquery(expr).is_some()
        || expr_children(expr)
            .into_iter()
            .any(contains_pending_subquery)
}

pub(super) fn expression_contains_correlated_subquery(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
    outer: &Scope,
) -> bool {
    if scalar_lookup_parts(expr).is_some() {
        return true;
    }
    if let Some(query) = direct_subquery(expr) {
        let nulls = vec![Datum::Null; outer.width()];
        let mut binder =
            LateralBinder::new(read_ctx.catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes);
        if binder
            .bind_query(query, outer, &nulls)
            .is_ok_and(|(_, correlated)| correlated)
        {
            return true;
        }
    }
    expr_children(expr)
        .into_iter()
        .any(|child| expression_contains_correlated_subquery(read_ctx, child, outer))
}

pub(super) fn direct_subquery(expr: &Expr) -> Option<&crabka_pgparser::ast::QueryExpr> {
    match expr {
        Expr::ScalarSubquery(query) | Expr::ArraySubquery(query) | Expr::Exists(query) => {
            Some(query)
        }
        Expr::InSubquery { subquery, .. } | Expr::Quantified { subquery, .. } => Some(subquery),
        _ => None,
    }
}

pub(super) fn row_matches_correlated(
    read_ctx: &crate::subquery::SubCtx<'_>,
    filter: Option<&Expr>,
    outer: &Scope,
    row: &[Datum],
    binder: &mut LateralBinder<'_>,
) -> Result<bool, ExecError> {
    let Some(filter) = filter else {
        return Ok(true);
    };
    // A direct correlated subquery is reduced to one scalar for this outer row.
    // Its relation buffers are dead before the next row; keep only explicit
    // lookup caches (which own their own retained representation) in the
    // statement ledger.
    let temporary = binder
        .scalar_lookups
        .is_empty()
        .then(|| read_ctx.statement_memory.reserve());
    let (bound, correlated) = binder.bind_expr(filter, outer, row)?;
    debug_assert!(correlated);
    let lazy = fold_correlated_lazy_expressions(read_ctx, &bound, outer, row, binder)?;
    let initialized = resolve_lazy_initplans(read_ctx, &lazy, binder)?;
    let resolved = crate::subquery::resolve_expr(read_ctx, &initialized)?;
    let result = row_matches(Some(&resolved), outer, row, read_ctx.eval_ctx);
    drop(temporary);
    result
}

/// Fold lazy expressions containing deferred subqueries one selected branch or
/// argument at a time. Eager subquery rewriting must not execute a dead CASE
/// branch, an unused COALESCE argument, or the right operand of a boolean
/// connective the left operand has already settled.
fn fold_correlated_lazy_expressions(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
    scope: &Scope,
    row: &[Datum],
    binder: &mut LateralBinder<'_>,
) -> Result<Expr, ExecError> {
    // A boolean connective settles on its left operand alone whenever the
    // three-valued table has a wildcard row for it: `false AND anything` is
    // false and `true OR anything` is true, for `anything` including NULL. The
    // right operand's subqueries are then dead and must not run — the whole
    // point of a `WHERE cheap_predicate AND EXISTS (expensive)` is that the
    // expensive half is reached only by the rows the cheap half admits.
    //
    // The short-circuit in `eval` cannot do this job. Everything reaching the
    // evaluator here has already been through `subquery::resolve_expr`, which
    // executes every subquery in the tree before the connective is looked at,
    // so by then the work is spent. Laziness has to happen where subqueries
    // are still unresolved, which is here.
    if let Expr::Binary {
        op: op @ (BinaryOp::And | BinaryOp::Or),
        left,
        right,
    } = expr
        && contains_pending_subquery(right)
    {
        let value = eval_correlated_child(read_ctx, left, scope, row, binder)?;
        let settled = match (op, &value) {
            (BinaryOp::And, Datum::Bool(false)) => Some(false),
            (BinaryOp::Or, Datum::Bool(true)) => Some(true),
            _ => None,
        };
        if let Some(settled) = settled {
            return Ok(Expr::Const {
                value: Datum::Bool(settled),
                ty: ColumnType::Bool,
            });
        }
        // Undecided: the right operand still decides the answer, so it is
        // folded as usual. The left operand is carried across as the constant
        // it just evaluated to, so its own subqueries run once, not twice.
        let typed = type_without_evaluating_subqueries(
            read_ctx,
            left,
            &binder.initplans,
            &binder.scalar_lookups,
        )?;
        let left_type = crate::eval::infer_type(&typed, scope)?;
        return Ok(Expr::Binary {
            op: *op,
            left: Box::new(Expr::Const {
                value,
                ty: left_type,
            }),
            right: Box::new(fold_correlated_lazy_expressions(
                read_ctx, right, scope, row, binder,
            )?),
        });
    }

    if let Expr::Func(call) = expr
        && call.name == "coalesce"
        && contains_pending_subquery(expr)
    {
        let typed = type_without_evaluating_subqueries(
            read_ctx,
            expr,
            &binder.initplans,
            &binder.scalar_lookups,
        )?;
        if let Expr::Func(typed_call) = &typed
            && typed_call.name == "coalesce"
            && crate::routine::plpgsql_declared_call_type(read_ctx.catalog_kv, typed_call)?
                .is_none()
        {
            let result_type = crate::eval::infer_type(&typed, scope)?;
            let value = crate::func::eval_scalar(call, None, read_ctx.eval_ctx, |arg| {
                eval_correlated_child(read_ctx, arg, scope, row, binder)
            })?;
            return Ok(Expr::Const {
                value: crate::eval::cast_value(&value, result_type, &read_ctx.eval_ctx.time_zone)?,
                ty: result_type,
            });
        }
    }

    if let Expr::Case {
        operand,
        whens,
        else_result,
    } = expr
        && contains_pending_subquery(expr)
    {
        let typed = type_without_evaluating_subqueries(
            read_ctx,
            expr,
            &binder.initplans,
            &binder.scalar_lookups,
        )?;
        let result_type = crate::eval::infer_type(&typed, scope)?;
        let selected = if let Some(operand) = operand {
            let value = eval_correlated_child(read_ctx, operand, scope, row, binder)?;
            let mut selected = None;
            for (when, then) in whens {
                let candidate = eval_correlated_child(read_ctx, when, scope, row, binder)?;
                if crate::eval::apply_binary(
                    crabka_pgparser::ast::BinaryOp::Eq,
                    &value,
                    &candidate,
                    read_ctx.eval_ctx,
                )? == Datum::Bool(true)
                {
                    selected = Some(then);
                    break;
                }
            }
            selected.or(else_result.as_deref())
        } else {
            let mut selected = None;
            for (when, then) in whens {
                match eval_correlated_child(read_ctx, when, scope, row, binder)? {
                    Datum::Bool(true) => {
                        selected = Some(then);
                        break;
                    }
                    Datum::Bool(false) | Datum::Null => {}
                    _ => {
                        return Err(ExecError::TypeMismatch(
                            "argument of CASE/WHEN must be type boolean".into(),
                        ));
                    }
                }
            }
            selected.or(else_result.as_deref())
        };
        let value = match selected {
            Some(selected) => eval_correlated_child(read_ctx, selected, scope, row, binder)?,
            None => Datum::Null,
        };
        return Ok(Expr::Const {
            value: crate::eval::cast_value(&value, result_type, &read_ctx.eval_ctx.time_zone)?,
            ty: result_type,
        });
    }

    let mut folded = expr.clone();
    for child in expr_children_mut(&mut folded) {
        *child = fold_correlated_lazy_expressions(read_ctx, child, scope, row, binder)?;
    }
    Ok(folded)
}

fn eval_correlated_child(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
    scope: &Scope,
    row: &[Datum],
    binder: &mut LateralBinder<'_>,
) -> Result<Datum, ExecError> {
    let folded = fold_correlated_lazy_expressions(read_ctx, expr, scope, row, binder)?;
    let initialized = resolve_lazy_initplans(read_ctx, &folded, binder)?;
    let resolved = crate::subquery::resolve_expr(read_ctx, &initialized)?;
    crate::eval::eval(&resolved, scope, row, read_ctx.eval_ctx)
}

fn resolve_lazy_initplans(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
    binder: &mut LateralBinder<'_>,
) -> Result<Expr, ExecError> {
    if let Some((index, key)) = scalar_lookup_parts(expr) {
        let resolved = binder.resolve_scalar_lookup(read_ctx, index, key)?;
        return resolve_lazy_initplans(read_ctx, &resolved, binder);
    }
    if let Some((index, lhs)) = initplan_parts(expr) {
        let resolved = binder.resolve_initplan(read_ctx, index, lhs)?;
        return resolve_lazy_initplans(read_ctx, &resolved, binder);
    }
    let mut initialized = expr.clone();
    for child in expr_children_mut(&mut initialized) {
        *child = resolve_lazy_initplans(read_ctx, child, binder)?;
    }
    Ok(initialized)
}

fn replace_initplan_markers_with_typed_nulls(
    expr: &Expr,
    initplans: &[LazyInitPlan],
    scalar_lookups: &[CorrelatedScalarLookup],
) -> Result<Expr, ExecError> {
    if let Some((index, _)) = scalar_lookup_parts(expr) {
        let plan = scalar_lookups.get(index).ok_or_else(|| {
            ExecError::Unsupported("invalid correlated scalar lookup marker".into())
        })?;
        return Ok(Expr::Const {
            value: Datum::Null,
            ty: plan.result_type,
        });
    }
    if let Some((index, _)) = initplan_parts(expr) {
        let plan = initplans
            .get(index)
            .ok_or_else(|| ExecError::Unsupported("invalid deferred subquery marker".into()))?;
        return Ok(Expr::Const {
            value: Datum::Null,
            ty: plan.result_type,
        });
    }
    let mut typed = expr.clone();
    for child in expr_children_mut(&mut typed) {
        *child = replace_initplan_markers_with_typed_nulls(child, initplans, scalar_lookups)?;
    }
    Ok(typed)
}

pub(super) fn type_without_evaluating_subqueries(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
    initplans: &[LazyInitPlan],
    scalar_lookups: &[CorrelatedScalarLookup],
) -> Result<Expr, ExecError> {
    let typed = replace_initplan_markers_with_typed_nulls(expr, initplans, scalar_lookups)?;
    let typed = replace_subqueries_with_typed_nulls(read_ctx, &typed)?;
    let typed = crate::subquery::resolve_expr_skipping(read_ctx, &typed, &mut |node| {
        direct_subquery(node).is_some()
    })?;
    replace_subqueries_with_typed_nulls(read_ctx, &typed)
}

/// Replace subquery nodes with typed NULLs without executing them, so CASE can
/// determine its common result type before choosing a branch.
pub(super) fn replace_subqueries_with_typed_nulls(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expr: &Expr,
) -> Result<Expr, ExecError> {
    let typed_null = |query: &crabka_pgparser::ast::QueryExpr| {
        let fields = crate::query::describe_query_expr_with_ctes(
            read_ctx.catalog_kv,
            read_ctx.fctx.resolution,
            query,
            read_ctx.ctes,
        )?;
        if fields.len() != 1 {
            return Err(ExecError::SubqueryColumns);
        }
        column_type_from_catalog_oid(read_ctx.catalog_kv, fields[0].type_oid)
    };
    match expr {
        Expr::ScalarSubquery(query) => Ok(Expr::Const {
            value: Datum::Null,
            ty: typed_null(query)?,
        }),
        Expr::ArraySubquery(query) => {
            let ty = typed_null(query)?;
            let elem = match ty {
                ColumnType::Array(elem) => elem,
                ty => crabka_pgtypes::ElemType::from_column_type(ty).ok_or_else(|| {
                    ExecError::Unsupported(format!("arrays of {} are not supported", ty.name()))
                })?,
            };
            Ok(Expr::Const {
                value: Datum::Null,
                ty: ColumnType::Array(elem),
            })
        }
        Expr::Exists(query) => {
            crate::query::describe_query_expr_with_ctes(
                read_ctx.catalog_kv,
                read_ctx.fctx.resolution,
                query,
                read_ctx.ctes,
            )?;
            Ok(Expr::Const {
                value: Datum::Null,
                ty: ColumnType::Bool,
            })
        }
        Expr::InSubquery { subquery, .. } | Expr::Quantified { subquery, .. } => {
            typed_null(subquery)?;
            Ok(Expr::Const {
                value: Datum::Null,
                ty: ColumnType::Bool,
            })
        }
        _ => {
            let mut typed = expr.clone();
            for child in expr_children_mut(&mut typed) {
                *child = replace_subqueries_with_typed_nulls(read_ctx, child)?;
            }
            Ok(typed)
        }
    }
}
