use super::*;

/// Apply DISTINCT / ORDER BY / OFFSET / LIMIT and projection, returning the
/// projected output Datum rows. Shared by the top-level row path and derived
/// tables. `ctx` carries the session zone + transaction/statement clock used by
/// temporal eval.
/// Apply DISTINCT / ORDER BY / OFFSET / LIMIT and projection using the
/// enclosing statement's shared blocking-memory budget.
pub(crate) fn project_rows_ordered_with_memory(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    kept: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    // A select list with a set-returning function expands rows BELOW DISTINCT,
    // ORDER BY and LIMIT (PostgreSQL's ProjectSet), so it owns the whole
    // sort/dedup/limit shape rather than sharing the one-row-in-one-row-out one.
    if crate::srf::exprs_contain_srf(out_exprs) || crate::srf::order_by_contains_srf(&s.order_by) {
        return crate::srf::project_rows_ordered_with_memory(
            s,
            scope,
            fields,
            out_exprs,
            kept,
            ctx,
            statement_memory,
        );
    }
    let window = RowWindow {
        offset: eval_row_count(s.offset.as_ref(), RowCountClause::Offset, ctx)?,
        limit: eval_row_count(s.limit.as_ref(), RowCountClause::Limit, ctx)?,
        with_ties: s.with_ties,
    };
    // Only plain DISTINCT restricts ORDER BY to the select-list output; DISTINCT
    // ON sorts the source rows, so its keys may name source-only columns.
    let require_output = matches!(s.distinct, crabka_pgparser::ast::DistinctClause::Distinct);
    let order_keys =
        resolve_select_order_keys(&s.order_by, scope, fields, out_exprs, require_output)?;

    if matches!(s.distinct, crabka_pgparser::ast::DistinctClause::Distinct) {
        for expr in out_exprs {
            crate::eval::require_equality_operator(crate::eval::infer_type(expr, scope)?)?;
        }
    }
    let distinct_on = distinct_on_plan(s, scope, fields, out_exprs, &order_keys)?;
    if let Some(plan) = &distinct_on {
        for expr in &plan.group {
            crate::eval::require_equality_operator(crate::eval::infer_type(expr, scope)?)?;
        }
    }
    for key in &order_keys {
        let expr = match key {
            SelectOrderKey::Output(index) => &out_exprs[*index],
            SelectOrderKey::SourceExpr(expr) => expr,
        };
        crate::eval::require_ordering_operator(crate::eval::infer_type(expr, scope)?)?;
    }

    // SP39: SELECT DISTINCT projects FIRST, dedups output rows, then ORDER BY
    // sorts the deduped output. PostgreSQL requires every sort key to refer to
    // the select-list output (ordinal, alias/name, or the exact select expression).
    if matches!(s.distinct, crabka_pgparser::ast::DistinctClause::Distinct) {
        let mut projected = project_rows(out_exprs, scope, &kept, ctx)?;
        ensure_blocking_rows_fit(&projected, statement_memory)?;
        let mut seen: std::collections::HashSet<Vec<Datum>> = std::collections::HashSet::new();
        projected.retain(|r| seen.insert(r.clone()));
        let keyed: Vec<(Vec<Datum>, Vec<Datum>)> = projected
            .into_iter()
            .map(|r| {
                let keys = order_keys
                    .iter()
                    .map(|k| match k {
                        SelectOrderKey::Output(i) => r[*i].clone(),
                        SelectOrderKey::SourceExpr(_) => {
                            unreachable!("DISTINCT order keys are output-only")
                        }
                    })
                    .collect();
                (keys, r)
            })
            .collect();
        let mut keyed = keyed;
        if !s.order_by.is_empty() {
            keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
        }
        return Ok(apply_row_window(keyed, window, &s.order_by));
    }

    // Non-DISTINCT keeps the existing source-row ordering shape so non-projected
    // source expressions still work, but output ordinals/labels evaluate the
    // corresponding projection expression for each source row.
    let Some(plan) = distinct_on else {
        let mut keyed =
            key_source_rows(&order_keys, out_exprs, scope, kept, ctx, statement_memory)?;
        if !order_keys.is_empty() {
            keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
        }
        let kept = apply_row_window(keyed, window, &s.order_by);
        return project_rows(out_exprs, scope, &kept, ctx);
    };
    // DISTINCT ON dedups a stream sorted by `plan.sort`, which is not always the
    // query's own ORDER BY — the sort decides which row of each group survives,
    // ORDER BY only decides how the survivors come out.
    let dedup_keys: Vec<SelectOrderKey> = plan
        .sort
        .iter()
        .map(|item| SelectOrderKey::SourceExpr(item.expr.clone()))
        .collect();
    let mut keyed = key_source_rows(&dedup_keys, out_exprs, scope, kept, ctx, statement_memory)?;
    if !dedup_keys.is_empty() {
        // A stable sort is load-bearing for DISTINCT ON without an ORDER BY:
        // PostgreSQL keeps the first row of each key group in input order.
        keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &plan.sort));
    }
    let survivors = keep_first_per_distinct_on_group(keyed, &plan.group, scope, ctx)?;
    // Re-key on the query's ORDER BY and sort the survivors into it, the way
    // PostgreSQL puts a Sort above the Unique when the two differ. The sort is
    // stable, so it is a no-op when the dedup ordering already satisfies it.
    let rows = survivors.into_iter().map(|(_, row)| row).collect();
    let mut keyed = key_source_rows(&order_keys, out_exprs, scope, rows, ctx, statement_memory)?;
    if !order_keys.is_empty() {
        keyed.sort_by(|a, b| order_cmp(&a.0, &b.0, &s.order_by));
    }
    let kept = apply_row_window(keyed, window, &s.order_by);
    project_rows(out_exprs, scope, &kept, ctx)
}

/// Pair each source row with the values of `keys`, under the blocking-query
/// memory budget. With no keys the rows pass through unmeasured. Nothing is
/// sorted, so nothing extra is held.
fn key_source_rows(
    keys: &[SelectOrderKey],
    out_exprs: &[Expr],
    scope: &Scope,
    rows: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<KeyedRows, ExecError> {
    if keys.is_empty() {
        return Ok(rows.into_iter().map(|row| (Vec::new(), row)).collect());
    }
    let keys: Vec<crate::bind::BoundExpr> = keys
        .iter()
        .map(|key| match key {
            SelectOrderKey::Output(i) => crate::bind::BoundExpr::new(&out_exprs[*i], scope),
            SelectOrderKey::SourceExpr(expr) => crate::bind::BoundExpr::new(expr, scope),
        })
        .collect::<Result<_, _>>()?;
    let mut keyed: KeyedRows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(keys.len());
        for key in &keys {
            values.push(crate::eval::eval(key.expr(), scope, &row, ctx)?);
        }
        statement_memory.charge(
            crate::scanner::datum_row_bytes(&values)
                .saturating_add(crate::scanner::datum_row_bytes(&row)),
        )?;
        keyed.push((values, row));
    }
    Ok(keyed)
}

/// How a `DISTINCT ON` query dedups and sorts.
pub(crate) struct DistinctOnPlan {
    /// The expressions consecutive rows are grouped by, each already resolved
    /// through the SQL92 rules (`DISTINCT ON (1)` names a select-list column).
    pub(crate) group: Vec<Expr>,
    /// The order the rows must be in before that grouping, which is what decides
    /// which row of each group survives.
    pub(crate) sort: Vec<crabka_pgparser::ast::OrderItem>,
}

/// Resolve `DISTINCT ON` against the query's `ORDER BY`, or `None` when the
/// query has no `DISTINCT ON` at all.
///
/// `PostgreSQL`'s compatibility rule (`transformDistinctOnClause`) is
/// **one-directional**, and it is not a set-match. It walks the ORDER BY keys
/// adopting each one that is also a `DISTINCT ON` expression; `42P10` fires
/// only once an ORDER BY key has been *skipped*, and then in two places: for a
/// later ORDER BY key that is in the `ON` list, and for any `ON` expression the
/// ORDER BY never adopted. So `DISTINCT ON (a, b) … ORDER BY a` is valid (`b` is
/// appended with default `ASC NULLS LAST` semantics), while
/// `DISTINCT ON (a, b) … ORDER BY a, c` is not: `c` is skipped and `b` still
/// needs appending.
///
/// When the resulting dedup sort is shorter than the ORDER BY, `PostgreSQL`
/// sorts by the whole ORDER BY instead (`create_distinct_paths`); that ordering
/// still satisfies the grouping, and its trailing keys are what pick the
/// surviving row of each group.
pub(crate) fn distinct_on_plan(
    s: &SelectStmt,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    order_keys: &[SelectOrderKey],
) -> Result<Option<DistinctOnPlan>, ExecError> {
    let Some(on) = s.distinct.on_exprs() else {
        return Ok(None);
    };
    let group = on
        .iter()
        .map(|expr| resolve_sql92_expr(expr, scope, fields, out_exprs, SQL92_DISTINCT_ON))
        .collect::<Result<Vec<_>, ExecError>>()?;
    let ordered: Vec<(&Expr, &crabka_pgparser::ast::OrderItem)> = order_keys
        .iter()
        .zip(&s.order_by)
        .map(|(key, item)| match key {
            SelectOrderKey::Output(i) => (&out_exprs[*i], item),
            SelectOrderKey::SourceExpr(expr) => (expr, item),
        })
        .collect();

    let mut sort: Vec<crabka_pgparser::ast::OrderItem> = Vec::new();
    let mut skipped = false;
    for (expr, item) in &ordered {
        if !group
            .iter()
            .any(|key| order_output_exprs_equivalent(scope, key, expr))
        {
            skipped = true;
            continue;
        }
        if skipped {
            return Err(ExecError::InvalidColumnReference(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions".into(),
            ));
        }
        sort.push(crabka_pgparser::ast::OrderItem {
            expr: (*expr).clone(),
            asc: item.asc,
            nulls_first: item.nulls_first,
        });
    }
    for key in &group {
        if sort
            .iter()
            .any(|item| order_output_exprs_equivalent(scope, &item.expr, key))
        {
            continue;
        }
        // An ON expression the ORDER BY never adopted has to be appended to the
        // dedup sort — which is only sound while the adopted keys are still the
        // ORDER BY's own leading keys. Once an ORDER BY key has been skipped
        // they are not, so PostgreSQL rejects the query here.
        if skipped {
            return Err(ExecError::InvalidColumnReference(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions".into(),
            ));
        }
        sort.push(crabka_pgparser::ast::OrderItem {
            expr: key.clone(),
            asc: true,
            nulls_first: false,
        });
    }
    if sort.len() < ordered.len() {
        sort = ordered
            .iter()
            .map(|(expr, item)| crabka_pgparser::ast::OrderItem {
                expr: (*expr).clone(),
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
            .collect();
    }
    Ok(Some(DistinctOnPlan { group, sort }))
}

/// The clause name `DISTINCT ON` position errors carry.
const SQL92_DISTINCT_ON: crate::sql92::Sql92Clause = crate::sql92::Sql92Clause::DistinctOn;

/// Resolve one `DISTINCT ON` expression through `PostgreSQL`'s SQL92 rules to
/// the expression it stands for: an integer constant is a select-list position,
/// a bare name matching an output label is that column, and anything else is
/// itself.
fn resolve_sql92_expr(
    expr: &Expr,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    clause: crate::sql92::Sql92Clause,
) -> Result<Expr, ExecError> {
    if let Some(index) = crate::sql92::output_position(expr, fields.len(), clause)? {
        return Ok(out_exprs[index].clone());
    }
    if let Expr::Column { table: None, name } = expr
        && let Some(index) = output_label_index(scope, fields, out_exprs, name)?
    {
        return Ok(out_exprs[index].clone());
    }
    Ok(expr.clone())
}

/// Rows paired with the ORDER BY key vector they sort on.
type KeyedRows = Vec<(Vec<Datum>, Vec<Datum>)>;

/// Keep the first row of each `DISTINCT ON` key group. The rows are already in
/// the order that decides which row wins, so this is a single pass over
/// consecutive-equal groups, the shape `PostgreSQL`'s `Unique` node has.
fn keep_first_per_distinct_on_group(
    keyed: KeyedRows,
    on: &[Expr],
    scope: &Scope,
    ctx: &crate::clock::EvalCtx,
) -> Result<KeyedRows, ExecError> {
    let on = crate::bind::bind_all(on, scope)?;
    let mut out: KeyedRows = Vec::new();
    let mut previous: Option<Vec<Datum>> = None;
    for (keys, row) in keyed {
        let group = on
            .iter()
            .map(|expr| crate::eval::eval(expr.expr(), scope, &row, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        if previous.as_ref() == Some(&group) {
            continue;
        }
        previous = Some(group);
        out.push((keys, row));
    }
    Ok(out)
}

fn ensure_blocking_rows_fit(
    rows: &[Vec<Datum>],
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<(), ExecError> {
    for row in rows {
        statement_memory.charge_row(row)?;
    }
    Ok(())
}

pub(super) fn project_order_limit_relation(
    s: &SelectStmt,
    scope: &Scope,
    kept: Vec<Vec<Datum>>,
    ctx: &crate::clock::EvalCtx,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Relation, ExecError> {
    let (fields, out_exprs, tys) = resolve_projection(&s.projection, scope)?;
    let rows = project_rows_ordered_with_memory(
        s,
        scope,
        &fields,
        &out_exprs,
        kept,
        ctx,
        statement_memory,
    )?;
    Ok(Relation {
        scope: Scope {
            columns: fields
                .iter()
                .zip(&tys)
                .map(|(field, ty)| ColumnBinding {
                    exposure: Exposure::Output,
                    qualifier: None,
                    name: field.name.clone(),
                    ty: *ty,
                })
                .collect(),
            ..Default::default()
        },
        rows,
    })
}

/// Evaluate the projection expressions for each source row, yielding output
/// Datum rows (one `Datum` per output column).
pub(crate) fn project_rows(
    out_exprs: &[Expr],
    scope: &Scope,
    rows: &[Vec<Datum>],
    ctx: &crate::clock::EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let out_exprs = crate::bind::bind_all(out_exprs, scope)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cells = Vec::with_capacity(out_exprs.len());
        for e in &out_exprs {
            cells.push(crate::eval::eval(e.expr(), scope, row, ctx)?);
        }
        out.push(cells);
    }
    Ok(out)
}

/// Encode projected Datum rows into a `QueryResult::Rows` (text + binary cells).
///
/// `tz` is the session time zone (`EvalCtx::time_zone`) used for `Timestamptz`
/// text rendering. Task 9 threads it from the per-statement `EvalCtx`; a
/// UTC/epoch context reproduces prior behavior until the session builds it.
pub(crate) fn rows_result(
    fields: Vec<FieldDescription>,
    projected: &[Vec<Datum>],
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> QueryResult {
    rows_result_with_tag(
        fields,
        projected,
        style,
        format!("SELECT {}", projected.len()),
    )
}

pub(crate) fn rows_result_with_tag(
    fields: Vec<FieldDescription>,
    projected: &[Vec<Datum>],
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
    tag: String,
) -> QueryResult {
    let rows: Vec<Vec<Option<Cell>>> = projected
        .iter()
        .map(|r| r.iter().map(|d| datum_to_cell(d, style)).collect())
        .collect();
    QueryResult::Rows { fields, rows, tag }
}

/// One resolved ORDER BY key for a plain SELECT. SQL92-style output references
/// (`ORDER BY 1`, `ORDER BY alias`) are represented as output indices; all other
/// expressions are evaluated against the source/group scope.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SelectOrderKey {
    Output(usize),
    SourceExpr(Expr),
}

/// Resolve SELECT ORDER BY items using PostgreSQL's SQL92 rules:
/// integer constant -> output ordinal, bare output label -> output column, and
/// everything else -> source expression unless `require_output` is true.
pub(crate) fn resolve_select_order_keys(
    order_by: &[OrderItem],
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    require_output: bool,
) -> Result<Vec<SelectOrderKey>, ExecError> {
    order_by
        .iter()
        .map(|item| resolve_select_order_key(item, scope, fields, out_exprs, require_output))
        .collect()
}

pub(super) fn resolve_select_order_key(
    item: &OrderItem,
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    require_output: bool,
) -> Result<SelectOrderKey, ExecError> {
    if let Some(index) =
        crate::sql92::output_position(&item.expr, fields.len(), crate::sql92::Sql92Clause::OrderBy)?
    {
        return Ok(SelectOrderKey::Output(index));
    }

    if let Expr::Column { table: None, name } = &item.expr
        && let Some(i) = output_label_index(scope, fields, out_exprs, name)?
    {
        return Ok(SelectOrderKey::Output(i));
    }

    if require_output {
        if let Some(i) = out_exprs
            .iter()
            .position(|e| order_output_exprs_equivalent(scope, e, &item.expr))
        {
            return Ok(SelectOrderKey::Output(i));
        }
        if let Expr::Column {
            table: Some(table),
            name,
        } = &item.expr
        {
            scope.resolve(Some(table), name)?;
        }
        return Err(ExecError::InvalidColumnReference(
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list".into(),
        ));
    }

    Ok(SelectOrderKey::SourceExpr(item.expr.clone()))
}

fn output_label_index(
    scope: &Scope,
    fields: &[FieldDescription],
    out_exprs: &[Expr],
    name: &str,
) -> Result<Option<usize>, ExecError> {
    let mut found = None;
    for (i, f) in fields.iter().enumerate() {
        if f.name == name {
            if let Some(prev) = found {
                if !order_output_exprs_equivalent(scope, &out_exprs[prev], &out_exprs[i]) {
                    return Err(ExecError::AmbiguousOrderBy(name.to_string()));
                }
            } else {
                found = Some(i);
            }
        }
    }
    Ok(found)
}

fn order_output_exprs_equivalent(scope: &Scope, a: &Expr, b: &Expr) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (
            Expr::Column {
                table: table_a,
                name: name_a,
            },
            Expr::Column {
                table: table_b,
                name: name_b,
            },
        ) => {
            // Through `Scope::canonical`, so that a USING/NATURAL join input and
            // the merged column PostgreSQL builds from it count as one key:
            // `SELECT DISTINCT x … ORDER BY ja.x` is legal over a LEFT join.
            let left = scope.resolve(table_a.as_deref(), name_a);
            let right = scope.resolve(table_b.as_deref(), name_b);
            matches!((left, right), (Ok(left), Ok(right))
                if scope.canonical(left) == scope.canonical(right))
        }
        _ => false,
    }
}
