use super::*;

/// Run a SELECT to a `Relation` with an already-evaluated CTE scope. `WITH`
/// belongs to `QueryExpr`; this function handles the SELECT body under that scope.
pub(crate) fn select_to_relation_with_ctes(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Relation, ExecError> {
    // The whole-row references of THIS statement, which narrow the hidden
    // liveness markers its outer joins carry. Established before anything below
    // reads a relation, and replacing whatever the caller reached here with, so
    // a view body, a derived table or a correlated subquery is measured by its
    // own text and never by the text that reached it — a narrower set inherited
    // from an enclosing statement would be a marker missing where this one reads
    // it.
    //
    // Read off the statement as written rather than as rewritten below: subquery
    // resolution only folds subqueries into constants and internally qualified
    // markers, so the references it leaves are a subset of these, and collecting
    // first is what puts every read path under this statement's own set.
    let refs = crate::scope::StatementRefs::of_select(s);
    let read_ctx = &read_ctx.with_refs(&refs);
    let catalog_kv = read_ctx.catalog_kv;
    let ctes = read_ctx.ctes;
    let ctx = read_ctx.eval_ctx;
    let fctx = read_ctx.fctx;
    reject_nested_relation_locking(s)?;

    // Correlated WHERE subqueries need the current source row, while
    // uncorrelated ones must retain SP34's once-only folding. Only queries whose
    // WHERE contains a subquery pay for the schema description used to tell the
    // two apart.
    let PlannedSubqueries {
        select: resolved,
        correlated_filter: correlated,
        initplans,
        scalar_lookups,
        row_exprs,
    } = resolve_select_subqueries(read_ctx, s)?;
    let s = &resolved;
    crate::window::reject_misplaced_calls(s)?;
    crate::grouping::reject_misplaced_calls(s)?;
    crate::srf::reject_misplaced_calls(s)?;
    if let Some((relation, state)) = crate::plan::exec::try_execute_result_with_state(s, ctx)? {
        read_ctx.record_plan_state(state);
        return Ok(relation);
    }
    if !correlated && row_exprs.is_none() && !crate::scope::wants_system_column(read_ctx.refs) {
        if let Some(relation) = try_execute_partial_aggregate_pushdown(read_ctx, s)? {
            return Ok(relation);
        }
        if let Some(relation) = try_execute_local_streaming_aggregate(read_ctx, s)? {
            return Ok(relation);
        }
        if let Some(relation) = try_execute_local_join_count(read_ctx, s)? {
            return Ok(relation);
        }
    }
    if !correlated
        && row_exprs.is_none()
        && let Some((relation, state)) =
            crate::plan::exec::try_execute_seq_scan_with_state(read_ctx, s)?
    {
        read_ctx.record_plan_state(state);
        return Ok(relation);
    }
    let relation = if s.from.is_empty() {
        reject_from_less_wildcard(&s.projection)?;
        Relation {
            scope: Scope::empty(),
            rows: vec![vec![]],
        }
    } else if crate::grouping::is_degenerate_grouping(s) {
        // A degenerate grouping query answers the same rows over any input, so
        // it is answered over none: `create_degenerate_grouping_paths` throws
        // the scan away rather than filter it, which is why
        // `SELECT 1 FROM t WHERE 1/a = 1 HAVING 1 < 2` never divides by zero.
        //
        // The relation is still built, because everything the FROM clause
        // raises is raised whether or not the plan reads it — a missing
        // relation, a missing column, a policy, a privilege. Only its rows are
        // dropped, and only the `WHERE` goes unevaluated. Nothing below has a
        // second path for the empty input: an input relation that simply *is*
        // empty already takes this one.
        let mut relation = build_from(read_ctx, &s.from, None, None, None, None)?;
        relation.rows.clear();
        relation
    } else {
        // SP40 Task 14: when the FROM is EXACTLY one foreign base table, extract
        // `_partition`/`_offset` bounds from the WHERE and push them into the
        // scan. The WHERE is still applied below, so this only ever reads less —
        // it never changes the result set.
        let pushed = if is_single_foreign_table(catalog_kv, &s.from, ctes, fctx) {
            Some(extract_scan_bounds(s.filter.as_ref()))
        } else {
            None
        };
        let scan_plan = single_table_scan_plan(read_ctx, s)?;
        let live_columns = (!correlated)
            .then(|| live_from_columns(read_ctx, s))
            .flatten();
        build_from(
            read_ctx,
            &s.from,
            pushed.as_ref(),
            scan_plan.as_ref(),
            s.filter.as_ref(),
            live_columns.as_deref(),
        )?
    };
    let mut binder = correlated.then(|| {
        LateralBinder::new(catalog_kv, read_ctx.fctx.resolution, read_ctx.ctes)
            .with_initplans(initplans)
            .with_scalar_lookups(scalar_lookups)
    });
    // The uncorrelated filter is the same expression for every row, so its
    // column references are resolved once here instead of once per row.
    let Relation {
        mut scope,
        rows: source_rows,
    } = relation;
    let bound_filter = if binder.is_none() {
        crate::bind::bind_optional(s.filter.as_ref(), &scope)?
    } else {
        None
    };
    let bound_filter = bound_filter
        .as_ref()
        .map(|filter| fold_literal_reg_casts(filter.expr(), ctx))
        .transpose()?;
    let mut kept = Vec::new();
    for row in source_rows {
        let matches = if let Some(binder) = &mut binder {
            row_matches_correlated(read_ctx, s.filter.as_ref(), &scope, &row, binder)?
        } else {
            row_matches(bound_filter.as_ref(), &scope, &row, ctx)?
        };
        if matches {
            kept.push(row);
        }
    }
    // A correlated select-list / ORDER BY / DISTINCT ON expression is evaluated
    // here, once per surviving source row, and parked in a hidden column. The
    // clauses below then see plain column references and run unchanged.
    if let Some(row_exprs) = row_exprs {
        materialize_correlated_row_exprs(read_ctx, row_exprs, &mut scope, &mut kept)?;
    }
    // Window functions run above WHERE/GROUP BY/HAVING and below DISTINCT/ORDER
    // BY/LIMIT, so they own the whole projection shape for the queries that use
    // them (including the grouped ones).
    let (fields, mut out_exprs, tys) = if crate::window::has_window_calls(s) {
        let (fields, tys, rows) =
            crate::window::execute_with_memory(s, &scope, kept, ctx, &read_ctx.statement_memory)?;
        return Ok(Relation {
            scope: projected_scope(&fields, &tys),
            rows,
        });
    } else {
        resolve_projection(&s.projection, &scope)?
    };
    let mut grouping_select = s.clone();
    if crate::grouping::is_grouping_query(s) {
        let group_by =
            crate::grouping::substitute_group_references(&s.group_by, &scope, &fields, &out_exprs)?;
        if crate::srf::exprs_contain_srf(&group_by) {
            let (expanded_scope, rewritten, expanded_rows) =
                crate::srf::expand_expressions_with_memory(
                    &scope,
                    kept,
                    &group_by,
                    ctx,
                    &read_ctx.statement_memory,
                )?;
            for (group, rewritten) in group_by.iter().zip(&rewritten) {
                for (item, output) in grouping_select.projection.iter_mut().zip(&mut out_exprs) {
                    if let SelectItem::Expr { expr, .. } = item
                        && expr == group
                    {
                        *expr = rewritten.clone();
                        *output = rewritten.clone();
                    }
                }
            }
            grouping_select.group_by = rewritten;
            scope = expanded_scope;
            kept = expanded_rows;
        }
    }
    let out_scope = projected_scope(&fields, &tys);
    let rows = if crate::grouping::is_grouping_query(s) {
        if crate::srf::exprs_contain_srf(&out_exprs) {
            // Aggregate first, then ProjectSet. The inner aggregate only needs
            // scalar output fields; ProjectSet evaluates the SRF-bearing ones
            // over those finished group rows.
            let mut aggregate = grouping_select.clone();
            for item in &mut aggregate.projection {
                if let SelectItem::Expr { expr, .. } = item
                    && crate::srf::exprs_contain_srf(std::slice::from_ref(expr))
                {
                    *expr = Expr::NullLiteral;
                }
            }
            let rows = crate::grouping::aggregate_rows_with_memory(
                &aggregate,
                &scope,
                kept,
                ctx,
                &read_ctx.statement_memory,
            )?;
            let mut aggregate_scope = projected_scope(&fields, &tys);
            for column in &mut aggregate_scope.columns {
                column.qualifier = Some(POSITION_QUALIFIER.to_string());
            }
            let exprs = out_exprs
                .iter()
                .enumerate()
                .map(|(index, expr)| {
                    if crate::srf::exprs_contain_srf(std::slice::from_ref(expr)) {
                        expr.clone()
                    } else {
                        Expr::Column {
                            table: Some(POSITION_QUALIFIER.to_string()),
                            name: index.to_string(),
                        }
                    }
                })
                .collect::<Vec<_>>();
            project_rows_ordered_with_memory(
                &grouping_select,
                &aggregate_scope,
                &fields,
                &exprs,
                rows,
                ctx,
                &read_ctx.statement_memory,
            )?
        } else {
            crate::grouping::aggregate_rows_with_memory(
                &grouping_select,
                &scope,
                kept,
                ctx,
                &read_ctx.statement_memory,
            )?
        }
    } else {
        project_rows_ordered_with_memory(
            s,
            &scope,
            &fields,
            &out_exprs,
            kept,
            ctx,
            &read_ctx.statement_memory,
        )?
    };
    Ok(Relation {
        scope: out_scope,
        rows,
    })
}
