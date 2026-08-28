use super::*;

pub(super) fn try_execute_partial_aggregate_pushdown(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Option<Relation>, ExecError> {
    if !is_plain_partial_aggregate_select(s) {
        return Ok(None);
    }
    let Some((table, qualifier)) = single_sharded_base_table(
        read_ctx.catalog_kv,
        read_ctx.fctx.resolution,
        s,
        read_ctx.ctes,
    )?
    else {
        return Ok(None);
    };
    // The range owners fold the aggregate over every visible row, which is not
    // the same set as the rows a policy admits, and a sum cannot be un-summed
    // afterwards. Shadowing the raw table with the proof leaves no way to reach
    // it unproven.
    let Some(table) =
        crate::rls::UnrestrictedTable::read(&read_ctx.privileges(), &read_ctx.rls(), &table)?
    else {
        return Ok(None);
    };
    let table = table.get();
    // As in `local_streaming_aggregate_plan`: the range owners fold over stored
    // bytes, which hold nothing for a virtual generated column.
    if has_virtual_generated(table) {
        return Ok(None);
    }
    let spec = if s.group_by.is_empty() {
        crate::plan_dist::plan_scan(table, s.filter.as_ref(), &s.projection).partial_aggregate
    } else {
        crate::plan_dist::grouped_partial_aggregate_for_select(table, &s.projection, &s.group_by)
    };
    let Some(spec) = spec else {
        return Ok(None);
    };
    let predicate = match crate::plan_dist::strict_predicate_for_filter(table, s.filter.as_ref()) {
        Ok(predicate) => predicate,
        Err(_) => return Ok(None),
    };
    let scope = Scope::single(table, &qualifier);
    let (fields, _out_exprs, tys) = resolve_projection(&s.projection, &scope)?;
    let rows = read_ctx.range_scanner.scan(ScanRequest {
        local: read_ctx.kv,
        global: read_ctx.global,
        global_snapshot: read_ctx.gsnap,
        snapshot: read_ctx.snapshot,
        own_xid: read_ctx.own,
        command_id: read_ctx.command_id,
        read_ts: None,
        own_start_ts: None,
        table,
        interval: RowInterval::ALL,
        predicate,
        projection: crate::ProjectionPushdown::All,
        partial_aggregate: Some(spec.clone()),
        top_k: None,
    })?;
    let rows = crate::scanner::finalize_partial_aggregate_rows(rows, &spec)?;
    let out_scope = Scope {
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
    };
    Ok(Some(Relation {
        scope: out_scope,
        rows: rows.into_iter().map(|row| row.row).collect(),
    }))
}

/// Whether a direct `count(*)` INNER/LEFT join has the specialized executor
/// shape. The generic nested-loop plan defers this exact form so it can count
/// indexed matches without retaining the joined rows.
pub(super) fn uses_local_join_count_shape(s: &SelectStmt) -> bool {
    if s.filter.is_some() || !s.group_by.is_empty() || !select_modifiers_allow_partial_aggregate(s)
    {
        return false;
    }
    let [
        SelectItem::Expr {
            expr: Expr::Func(call),
            ..
        },
    ] = s.projection.as_slice()
    else {
        return false;
    };
    if call.name != "count"
        || call.distinct
        || !matches!(call.args, FuncArgs::Star)
        || call.filter.is_some()
    {
        return false;
    }
    matches!(
        s.from.as_slice(),
        [crabka_pgparser::ast::TableExpr::Join {
            kind: crabka_pgparser::ast::JoinKind::Inner | crabka_pgparser::ast::JoinKind::Left,
            ..
        }]
    )
}

/// Whether the generic nested-loop planner should leave this count to the
/// no-output join counter. Non-equality aggregates retain their `Aggregate`
/// plan node, which is also the EXPLAIN contract for that wider shape.
pub(crate) fn should_defer_local_join_count_plan(s: &SelectStmt) -> bool {
    if !uses_local_join_count_shape(s) {
        return false;
    }
    let [
        crabka_pgparser::ast::TableExpr::Join {
            constraint:
                crabka_pgparser::ast::JoinConstraint::On(crabka_pgparser::ast::Expr::Binary {
                    op: crabka_pgparser::ast::BinaryOp::Eq,
                    left,
                    right,
                }),
            ..
        },
    ] = s.from.as_slice()
    else {
        return false;
    };
    matches!(left.as_ref(), Expr::Column { .. }) && matches!(right.as_ref(), Expr::Column { .. })
}

/// Count one direct local INNER/LEFT join without retaining its output rows.
/// The narrow shape keeps every other aggregate query on the ordinary grouping
/// path; in particular, a WHERE/FILTER/HAVING needs materialized joined rows.
pub(super) fn try_execute_local_join_count(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Option<Relation>, ExecError> {
    if !uses_local_join_count_shape(s) {
        return Ok(None);
    }
    let [
        crabka_pgparser::ast::TableExpr::Join {
            left,
            right,
            kind,
            constraint,
        },
    ] = s.from.as_slice()
    else {
        unreachable!("local join count shape has one INNER or LEFT join");
    };
    if !is_plain_local_join_table(read_ctx, left)? || !is_plain_local_join_table(read_ctx, right)? {
        return Ok(None);
    }

    let left = build_table_expr(read_ctx, left, None, None, None)?;
    let right = build_table_expr(read_ctx, right, None, None, None)?;
    let count = count_join_rows(
        &left,
        &right,
        *kind,
        constraint,
        read_ctx.eval_ctx,
        read_ctx.blocking_query_memory,
    )?;
    let (fields, _, types) = resolve_projection(&s.projection, &Scope::empty())?;
    Ok(Some(Relation {
        scope: projected_scope(&fields, &types),
        rows: vec![vec![Datum::Int8(count)]],
    }))
}

fn is_plain_local_join_table(
    read_ctx: &crate::subquery::SubCtx<'_>,
    expression: &crabka_pgparser::ast::TableExpr,
) -> Result<bool, ExecError> {
    let crabka_pgparser::ast::TableExpr::Table {
        name,
        columns: None,
        sample: None,
        ..
    } = expression
    else {
        return Ok(false);
    };
    if name.schema.is_none() && read_ctx.ctes.lookup(&name.name).is_some() {
        return Ok(false);
    }
    let Some(table) = scan_plan_table(read_ctx.catalog_kv, read_ctx.fctx.resolution, name)? else {
        return Ok(false);
    };
    // The counting path returns a count and no rows, so nothing downstream can
    // filter it: a side under row security keeps the ordinary path, which
    // materializes both sides through the gate and counts the same join.
    Ok(!table.sharded
        && crate::rls::UnrestrictedTable::read(&read_ctx.privileges(), &read_ctx.rls(), &table)?
            .is_some())
}

/// Stream a supported local aggregate through per-page partial-aggregate
/// folding, instead of a fold over every visible row after it is materialized.
///
/// Fires on exactly one ordinary non-sharded base table when every projection
/// item decomposes into scalar expressions over pushdown-model aggregate calls
/// (`CAST(count(*) AS BIGINT)`, `COALESCE(sum(x), 0)`, `sum(a) / count(*)`, …
/// — or the narrow grouped shape), with a WHERE that parses into the strict
/// pushdown predicate subset (or, for `count(*)`, an OR of two such filters).
/// Everything else — `DISTINCT`, `HAVING`, bare ungrouped columns, aggregates
/// over non-column arguments, whole-row reads, non-pushdown filters — keeps the
/// materializing scan and its whole-result memory budget.
pub(crate) fn try_execute_local_streaming_aggregate(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Option<Relation>, ExecError> {
    if !is_streamable_aggregate_select(s) {
        return Ok(None);
    }
    let Some((table, qualifier)) = single_local_base_table(
        read_ctx.catalog_kv,
        read_ctx.fctx.resolution,
        s,
        read_ctx.ctes,
    )?
    else {
        return Ok(None);
    };
    // The fold happens page by page inside the scanner, over every visible row,
    // before any policy qual could have removed one. Shadowing keeps the raw
    // table out of reach for the rest of the function.
    let Some(table) =
        crate::rls::UnrestrictedTable::read(&read_ctx.privileges(), &read_ctx.rls(), &table)?
    else {
        return Ok(None);
    };
    let table = table.get();
    let Some(plan) = local_streaming_aggregate_plan(table, s) else {
        return Ok(None);
    };
    let predicates = match crate::plan_dist::strict_predicate_for_filter(table, s.filter.as_ref()) {
        Ok(predicate) => vec![predicate],
        Err(_) => {
            let Some(predicates) = count_star_or_predicates(table, s, &plan) else {
                return Ok(None);
            };
            Vec::from(predicates)
        }
    };
    // An equality probe over a local index reads less than any table scan:
    // keep that existing path (and its materializing budget) for those filters.
    if predicates.len() == 1
        && choose_local_index_equality(read_ctx.catalog_kv, table, &predicates[0])?.is_some()
    {
        return Ok(None);
    }
    let scope = Scope::single(table, &qualifier);
    let (fields, out_exprs, tys) = resolve_projection(&s.projection, &scope)?;
    let states = predicates
        .into_iter()
        .map(|predicate| {
            crate::scanner::collect_partial_aggregates_bounded(
                read_ctx.range_scanner,
                ScanRequest {
                    local: read_ctx.kv,
                    global: read_ctx.global,
                    global_snapshot: read_ctx.gsnap,
                    snapshot: read_ctx.snapshot,
                    own_xid: read_ctx.own,
                    command_id: read_ctx.command_id,
                    read_ts: None,
                    own_start_ts: None,
                    table,
                    interval: RowInterval::ALL,
                    predicate,
                    projection: crate::ProjectionPushdown::All,
                    partial_aggregate: None,
                    top_k: None,
                },
                plan.specs(),
                read_ctx.statement_memory.limit(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let rows = match &plan {
        StreamingAggregatePlan::Scalar { calls, specs } => {
            let values = match <[Vec<Vec<ScannedRow>>; 1]>::try_from(states) {
                Ok([states]) => finalize_scalar_streaming_aggregates(states, specs)?,
                Err(states) => {
                    let Ok(states) = <[Vec<Vec<ScannedRow>>; 3]>::try_from(states) else {
                        return Err(invalid_scalar_aggregate_shape());
                    };
                    finalize_count_star_union(states, specs)?
                }
            };
            vec![crate::agg::eval_over_aggregate_values(
                &out_exprs,
                &scope,
                calls,
                &values,
                read_ctx.eval_ctx,
            )?]
        }
        StreamingAggregatePlan::Grouped(spec) => {
            let Ok([states]) = <[Vec<Vec<ScannedRow>>; 1]>::try_from(states) else {
                return Err(ExecError::Unsupported(
                    "grouped partial aggregate streaming expects one predicate".into(),
                ));
            };
            let Ok([state]) = <[Vec<ScannedRow>; 1]>::try_from(states) else {
                return Err(ExecError::Unsupported(
                    "grouped partial aggregate streaming expects exactly one spec".into(),
                ));
            };
            crate::scanner::finalize_partial_aggregate_rows(state, spec)?
                .into_iter()
                .map(|row| row.row)
                .collect()
        }
    };
    let out_scope = Scope {
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
    };
    Ok(Some(Relation {
        scope: out_scope,
        rows,
    }))
}

/// The one safe disjunction for local streaming: `count(*)` over two strict
/// predicates. Three bounded scans compute `left + right - intersection`
/// without materializing the matching rows.
fn count_star_or_predicates(
    table: &Table,
    s: &SelectStmt,
    plan: &StreamingAggregatePlan,
) -> Option<[PredicatePushdown; 3]> {
    let StreamingAggregatePlan::Scalar { calls, specs } = plan else {
        return None;
    };
    let ([call], [spec]) = (calls.as_slice(), specs.as_slice()) else {
        return None;
    };
    if call.name != "count"
        || call.distinct
        || !matches!(call.args, FuncArgs::Star)
        || call.filter.is_some()
        || spec.function != crate::PartialAggregateFunction::Count
        || spec.column.is_some()
    {
        return None;
    }
    let Expr::Binary {
        op: BinaryOp::Or,
        left,
        right,
    } = s.filter.as_ref()?
    else {
        return None;
    };
    let PredicatePushdown::Conjunctive(left) =
        crate::plan_dist::strict_predicate_for_filter(table, Some(left)).ok()?
    else {
        return None;
    };
    let PredicatePushdown::Conjunctive(right) =
        crate::plan_dist::strict_predicate_for_filter(table, Some(right)).ok()?
    else {
        return None;
    };
    let mut intersection = left.clone();
    intersection.extend(right.iter().cloned());
    Some([
        PredicatePushdown::Conjunctive(left),
        PredicatePushdown::Conjunctive(right),
        PredicatePushdown::Conjunctive(intersection),
    ])
}

fn finalize_count_star_union(
    [left, right, intersection]: [Vec<Vec<ScannedRow>>; 3],
    specs: &[crate::PartialAggregateSpec],
) -> Result<Vec<Datum>, ExecError> {
    let [spec] = specs else {
        return Err(invalid_scalar_aggregate_shape());
    };
    let count = |states| -> Result<i64, ExecError> {
        let values = finalize_scalar_streaming_aggregates(states, std::slice::from_ref(spec))?;
        let [Datum::Int8(count)] = values.as_slice() else {
            return Err(invalid_scalar_aggregate_shape());
        };
        Ok(*count)
    };
    let (left, right, intersection) = (count(left)?, count(right)?, count(intersection)?);
    if intersection > left || intersection > right {
        return Err(invalid_scalar_aggregate_shape());
    }
    let count = (left - intersection)
        .checked_add(right)
        .ok_or_else(|| ExecError::Unsupported("streamed COUNT union exceeds int8 range".into()))?;
    Ok(vec![Datum::Int8(count)])
}

/// How the local streaming path computes a supported aggregate SELECT.
enum StreamingAggregatePlan {
    /// No GROUP BY: stream one partial spec per distinct aggregate call, then
    /// evaluate each projection expression over the finalized values.
    Scalar {
        /// Deduped aggregate calls, aligned index-for-index with `specs` (and
        /// with the finalized values fed to the outer-expression evaluation).
        calls: Vec<crabka_pgparser::ast::FuncCall>,
        specs: Vec<crate::PartialAggregateSpec>,
    },
    /// The narrow grouped shape: one spec whose finalized rows ARE the output
    /// rows. Those rows are the group key columns, then the aggregate, ordered
    /// by key.
    Grouped(crate::PartialAggregateSpec),
}

impl StreamingAggregatePlan {
    fn specs(&self) -> &[crate::PartialAggregateSpec] {
        match self {
            Self::Scalar { specs, .. } => specs,
            Self::Grouped(spec) => std::slice::from_ref(spec),
        }
    }
}

/// Decompose the SELECT into a streaming plan: the single grouped spec for the
/// grouped shape; with no GROUP BY, the deduped aggregate calls (each inside
/// the pushdown model) with everything around them scalar expressions to
/// evaluate over the finalized values. `None` when any part falls outside the
/// model, and the caller then keeps the materializing scan.
fn local_streaming_aggregate_plan(table: &Table, s: &SelectStmt) -> Option<StreamingAggregatePlan> {
    // The fold runs inside the scanner, over the rows as they sit in storage,
    // where a virtual generated column is a NULL placeholder. Such a relation
    // takes the ordinary path, which materializes the value first.
    if has_virtual_generated(table) {
        return None;
    }
    if !s.group_by.is_empty() {
        return crate::plan_dist::grouped_partial_aggregate_for_select(
            table,
            &s.projection,
            &s.group_by,
        )
        .map(StreamingAggregatePlan::Grouped);
    }
    let mut calls = Vec::new();
    for item in &s.projection {
        let SelectItem::Expr { expr, .. } = item else {
            return None;
        };
        if !crate::agg::collect_streamable_aggregate_calls(expr, &mut calls) {
            return None;
        }
    }
    // No aggregate anywhere means this is not an aggregate query at all (one
    // output row per table row) — never a streaming-fold candidate.
    if calls.is_empty() {
        return None;
    }
    let specs = calls
        .iter()
        .map(|call| crate::plan_dist::partial_aggregate_for_call(table, call))
        .collect::<Option<Vec<_>>>()?;
    Some(StreamingAggregatePlan::Scalar { calls, specs })
}

/// Finalize each scalar spec's streamed partial state into the aggregate's
/// SQL-visible value, in spec order.
fn finalize_scalar_streaming_aggregates(
    states: Vec<Vec<ScannedRow>>,
    specs: &[crate::PartialAggregateSpec],
) -> Result<Vec<Datum>, ExecError> {
    states
        .into_iter()
        .zip(specs)
        .map(|(state, spec)| {
            let finalized = crate::scanner::finalize_partial_aggregate_rows(state, spec)?;
            let [ScannedRow { row, .. }] = finalized.as_slice() else {
                return Err(invalid_scalar_aggregate_shape());
            };
            let [value] = row.as_slice() else {
                return Err(invalid_scalar_aggregate_shape());
            };
            Ok(value.clone())
        })
        .collect()
}

fn invalid_scalar_aggregate_shape() -> ExecError {
    ExecError::Unsupported("scalar partial aggregate produced an invalid merged shape".into())
}

/// Match a FROM that is exactly one ordinary local base table.
///
/// CTE, view, virtual-catalog, sharded, and foreign relations all resolve
/// through their own scan paths, so they deliberately return `None` here.
fn single_local_base_table(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    s: &SelectStmt,
    ctes: &crate::cte::CteContext,
) -> Result<Option<(Table, String)>, ExecError> {
    let [
        crabka_pgparser::ast::TableExpr::Table {
            name,
            only,
            alias,
            columns: None,
            sample: None,
            ..
        },
    ] = s.from.as_slice()
    else {
        return Ok(None);
    };
    if name.schema.is_none() && ctes.lookup(&name.name).is_some() {
        return Ok(None);
    }
    let name = &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?;
    if is_virtual_relation(name) {
        return Ok(None);
    }
    match crabka_pgcatalog::get_view(catalog_kv, name) {
        Ok(_) => return Ok(None),
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {}
        Err(error) => return Err(error.into()),
    }
    let table = match crabka_pgcatalog::get_table(catalog_kv, name) {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    // An unpopulated materialized view is an error to read, and the pushdowns
    // are exactly where that would go unnoticed: a fold over the row space
    // never opens the general scan path, so `count(*)` would answer zero for a
    // relation the general path refuses.
    require_populated(&table)?;
    // A partitioned parent owns no rows of its own: the streaming fold would
    // read its empty row space and answer for the whole hierarchy.
    if table.sharded
        || table.foreign.is_some()
        || crate::partition::is_partitioned(catalog_kv, name)?
        || reads_inheritance_children(catalog_kv, *only, name)?
    {
        return Ok(None);
    }
    let qualifier = alias.clone().unwrap_or_else(|| table.name.name.clone());
    Ok(Some((table, qualifier)))
}

/// Whether reading `name` here means reading rows this relation does not
/// physically hold — an inheritance parent named without `ONLY`.
///
/// The aggregate pushdowns fold over exactly one relation's row space, so a
/// parent whose children hold rows would report its own rows only, while a
/// plain `SELECT` over the same FROM returns the whole tree. `ONLY` asks for
/// just the parent's rows, which is what the pushdown already computes.
///
/// The wire path's streaming cursor asks it for the same reason: it scans one
/// relation's row space too, so a parent named without `ONLY` is a shape it
/// cannot serve.
pub(crate) fn reads_inheritance_children(
    catalog_kv: &dyn Kv,
    only: bool,
    name: &crabka_pgcatalog::RelationName,
) -> Result<bool, ExecError> {
    if only {
        return Ok(false);
    }
    // The cheap form on purpose: the answer is a boolean, and every read of an
    // ordinary childless relation asks it. `children_of` would decode a name
    // list to throw it away.
    crate::inheritance::has_children(catalog_kv, name)
}

/// Match a FROM that is exactly one sharded base table.
///
/// CTE, view, virtual-catalog, local, and foreign relations all resolve
/// through their own scan paths, so they deliberately return `None` here, and
/// so does a relation that does not exist at all, whose undefined-table
/// error surfaces from the materializing path instead.
fn single_sharded_base_table(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    s: &SelectStmt,
    ctes: &crate::cte::CteContext,
) -> Result<Option<(Table, String)>, ExecError> {
    let [
        crabka_pgparser::ast::TableExpr::Table {
            name,
            only,
            alias,
            columns: None,
            sample: None,
            ..
        },
    ] = s.from.as_slice()
    else {
        return Ok(None);
    };
    if name.schema.is_none() && ctes.lookup(&name.name).is_some() {
        return Ok(None);
    }
    let name = &resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference)?;
    if is_virtual_relation(name) {
        return Ok(None);
    }
    match crabka_pgcatalog::get_view(catalog_kv, name) {
        Ok(_) => return Ok(None),
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => {}
        Err(error) => return Err(error.into()),
    }
    let table = match crabka_pgcatalog::get_table(catalog_kv, name) {
        Ok(table) => table,
        Err(crabka_pgcatalog::CatalogError::UndefinedTable(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    require_populated(&table)?;
    if !table.sharded
        || table.foreign.is_some()
        || reads_inheritance_children(catalog_kv, *only, name)?
    {
        return Ok(None);
    }
    let qualifier = alias.clone().unwrap_or_else(|| table.name.name.clone());
    Ok(Some((table, qualifier)))
}

fn is_plain_partial_aggregate_select(s: &SelectStmt) -> bool {
    if !select_modifiers_allow_partial_aggregate(s) {
        return false;
    }
    let Some(SelectItem::Expr {
        expr: Expr::Func(call),
        ..
    }) = s.projection.last()
    else {
        return false;
    };
    if call.distinct {
        return false;
    }
    matches!(call.name.as_str(), "count" | "sum" | "avg" | "min" | "max")
        && matches!(&call.args, FuncArgs::Star | FuncArgs::Exprs(_))
}

/// Cheap AST pre-filter for the local streaming path: the modifier shape the
/// partial-aggregate model supports, every projection item an expression, and
/// an aggregate call somewhere in the projection. The per-item decomposition in
/// `local_streaming_aggregate_plan` does the precise streamability check.
fn is_streamable_aggregate_select(s: &SelectStmt) -> bool {
    select_modifiers_allow_partial_aggregate(s)
        && s.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => crate::agg::contains_aggregate(expr),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
}

/// No `DISTINCT` / `HAVING` / `LIMIT` / `OFFSET`, and an ORDER BY only as the
/// exact ascending echo of the GROUP BY key (the order the grouped partial
/// fold already produces).
fn select_modifiers_allow_partial_aggregate(s: &SelectStmt) -> bool {
    // A window query needs every row the WHERE kept, unaggregated: the window
    // node runs above the grouping, so no partial-aggregate scan may replace it.
    if crate::window::has_window_calls(s) {
        return false;
    }
    // A grouping-set clause folds each row into several groups; the partial
    // aggregate model has one group key per row, so it cannot express it.
    if s.grouping.is_some() {
        return false;
    }
    if s.distinct.dedups() || s.having.is_some() || s.limit.is_some() || s.offset.is_some() {
        return false;
    }
    s.order_by.is_empty()
        || (s.order_by.len() == s.group_by.len()
            && s.order_by
                .iter()
                .zip(&s.group_by)
                .all(|(order, group)| order.asc && order.expr == *group))
}

/// The scan plan for a `SELECT` whose `FROM` is exactly one stored relation.
///
/// Split out of `select_to_relation_with_ctes` rather than written inline: that
/// function sits on the recursion path of a `plpgsql` set-returning function
/// calling itself, and in an unoptimized build every local it holds is
/// multiplied by the recursion depth.
pub(super) fn single_table_scan_plan(
    read_ctx: &crate::subquery::SubCtx<'_>,
    s: &SelectStmt,
) -> Result<Option<crate::plan_dist::DistributedScanPlan>, ExecError> {
    let [
        crabka_pgparser::ast::TableExpr::Table {
            name,
            alias,
            columns: None,
            sample: None,
            ..
        },
    ] = s.from.as_slice()
    else {
        return Ok(None);
    };
    if name.schema.is_some() || read_ctx.ctes.lookup(&name.name).is_some() {
        return Ok(None);
    }
    let Some(table) = scan_plan_table(read_ctx.catalog_kv, read_ctx.fctx.resolution, name)? else {
        return Ok(None);
    };
    let mut plan = crate::plan_dist::plan_scan(&table, s.filter.as_ref(), &s.projection);
    plan.projection = crate::ProjectionPushdown::All;
    plan.partial_aggregate = None;
    let qualifier = alias.as_deref().unwrap_or(&table.name.name);
    plan.top_k = top_k_pushdown_for_relation(read_ctx, &table, qualifier, s)?;
    Ok(Some(plan))
}

/// The per-range top-K request for a relation proven not to be under row
/// security.
///
/// Top-K truncates the row set inside the scanner, before any policy qual runs,
/// so a restricted relation must not have one. It takes the proof rather than a
/// `&Table` so that is a compile error rather than a review comment;
/// [`crate::rls::sanitize_scan_plan`] strips a top-K at the scan-request
/// construction site as well, since a plan can reach that site from more than
/// one caller.
fn top_k_pushdown_for_relation(
    read_ctx: &crate::subquery::SubCtx<'_>,
    table: &Table,
    qualifier: &str,
    s: &SelectStmt,
) -> Result<Option<crate::TopKSpec>, ExecError> {
    // A statement that reads a system column keeps the full scan. The pushdown
    // below resolves the select list and ORDER BY against a bare
    // `Scope::single`, which has none, so it would report the 42703 the ordinary
    // path is about to answer — and its `table.columns[column]` lookup indexes
    // the DECLARED columns by a scope index, which a system column sits past.
    if crate::scope::wants_system_column(read_ctx.refs) {
        return Ok(None);
    }
    let Some(unrestricted) =
        crate::rls::UnrestrictedTable::read(&read_ctx.privileges(), &read_ctx.rls(), table)?
    else {
        return Ok(None);
    };
    top_k_pushdown_for_select(unrestricted, qualifier, s)
}

fn top_k_pushdown_for_select(
    unrestricted: crate::rls::UnrestrictedTable<'_>,
    qualifier: &str,
    s: &SelectStmt,
) -> Result<Option<crate::TopKSpec>, ExecError> {
    let table = unrestricted.get();
    // A select-list set-returning function expands each source row into many, so
    // a LIMIT pushed onto the SOURCE scan would cut rows the expansion still owes.
    if !table.sharded
        || !is_top_k_candidate(s)
        || crate::srf::projection_contains_srf(&s.projection)
    {
        return Ok(None);
    }
    if crate::plan_dist::strict_predicate_for_filter(table, s.filter.as_ref()).is_err() {
        return Ok(None);
    }
    // A correlated select-list or ORDER BY expression lives in a hidden column
    // materialized ABOVE the scan, which the base-table scope this pushdown
    // resolves against does not carry. Such a query keeps the full scan.
    if select_mentions_correlated_marker(s) {
        return Ok(None);
    }
    let scope = Scope::single(table, qualifier);
    let (fields, out_exprs, _tys) = resolve_projection(&s.projection, &scope)?;
    let mut order_by = Vec::with_capacity(s.order_by.len());
    for order_item in &s.order_by {
        let order_key = resolve_select_order_key(order_item, &scope, &fields, &out_exprs, false)?;
        let Some(column) = top_k_column_index(&scope, &out_exprs, &order_key)? else {
            return Ok(None);
        };
        let order_column = &table.columns[column];
        if !order_column.not_null || !is_top_k_column_type_supported(order_column.ty) {
            return Ok(None);
        }
        order_by.push(crate::TopKColumn {
            column,
            asc: order_item.asc,
        });
    }
    let limit = u64::try_from(constant_limit(s).expect("candidate has a positive limit"))
        .map_err(|_| ExecError::Unsupported("top-k LIMIT is outside u64 range".into()))?;
    Ok(Some(crate::TopKSpec { order_by, limit }))
}

fn is_top_k_candidate(s: &SelectStmt) -> bool {
    // A window function sees the rows before LIMIT, so a top-k scan that stops
    // early would change its result.
    if crate::window::has_window_calls(s) {
        return false;
    }
    if s.distinct.dedups()
        || !s.group_by.is_empty()
        || s.having.is_some()
        || s.offset.is_some()
        || s.with_ties
        || crate::agg::is_aggregate_query(s)
        || s.order_by.is_empty()
    {
        return false;
    }
    constant_limit(s).is_some_and(|limit| limit > 0)
}

/// The `LIMIT` as a plain integer constant, or `None` when it is absent or is an
/// expression. Scan pushdown runs before evaluation, so only a literal count can
/// bound the scan; anything else keeps the full-scan path and is applied to the
/// materialized rows.
fn constant_limit(s: &SelectStmt) -> Option<i64> {
    match s.limit.as_ref()? {
        Expr::IntLiteral(text) => text.parse().ok(),
        _ => None,
    }
}

fn top_k_column_index(
    scope: &Scope,
    out_exprs: &[Expr],
    order_key: &SelectOrderKey,
) -> Result<Option<usize>, ExecError> {
    let expr = match order_key {
        SelectOrderKey::Output(index) => &out_exprs[*index],
        SelectOrderKey::SourceExpr(expr) => expr,
    };
    let Expr::Column {
        table: qualifier,
        name,
    } = expr
    else {
        return Ok(None);
    };
    let column = scope.resolve(qualifier.as_deref(), name)?;
    Ok(Some(column))
}

fn is_top_k_column_type_supported(ty: ColumnType) -> bool {
    matches!(ty, ColumnType::Int4 | ColumnType::Int8 | ColumnType::Text)
}

pub(crate) fn table_uses_global_visibility(table: &Table) -> bool {
    table.sharded
}

/// Refuse `SHARDED BY HASH (col)` on a column whose values the shard-key hasher
/// cannot turn into bytes, at CREATE TABLE rather than at every INSERT.
///
/// A missing hash column is *not* reported here, because the catalog's own
/// validation raises the undefined-column error for that.
pub(super) fn ensure_hash_shard_key_types_are_supported(
    columns: &[Column],
    sharding: Option<&crabka_pgcatalog::ShardingStrategy>,
) -> Result<(), ExecError> {
    let Some(crabka_pgcatalog::ShardingStrategy::Hash(hash)) = sharding else {
        return Ok(());
    };
    for column in hash
        .columns
        .iter()
        .filter_map(|name| columns.iter().find(|column| column.name == *name))
    {
        if !hash_shard_key_type_is_supported(column.ty) {
            return Err(ExecError::Unsupported(format!(
                "hash shard key column \"{}\" of type {} is not supported",
                column.name,
                column.ty.name()
            )));
        }
    }
    Ok(())
}

/// The column types [`hash_bucket_for_row`] can hash: those stored as an
/// `Int4`, `Int8`, `Text`, or `Bytea` datum. Everything else would fail on the
/// write path, so a table is never created with such a key: `boolean`,
/// `double precision`, `numeric`, the date/time types, `jsonb`, and arrays.
fn hash_shard_key_type_is_supported(ty: ColumnType) -> bool {
    matches!(
        ty,
        ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Text
            | ColumnType::Varchar(_)
            | ColumnType::Char(_)
            | ColumnType::Bytea
            | ColumnType::Uuid
            | ColumnType::Regclass
    )
}

pub(super) fn hash_sharding_from_ast(
    sharding: &crabka_pgparser::ast::ShardingSpec,
) -> Result<crabka_pgcatalog::ShardingStrategy, ExecError> {
    match sharding {
        crabka_pgparser::ast::ShardingSpec::Hash(hash) => {
            // Redundant for SQL input: the grammar refuses a `SHARDED BY HASH`
            // list of any length but one outright (42601), so this never fires
            // for a parsed statement. It is the gate for the callers that build
            // the AST directly, and it matches the arity the row encoder in
            // [`hash_bucket_for_row`] has an encoding for — one column, the
            // only arity that agrees with the route the gateway computes.
            if hash.columns.len() != 1 {
                return Err(ExecError::Unsupported(
                    "hash sharding requires exactly one column".into(),
                ));
            }
            if hash.buckets == 0 || !hash.buckets.is_power_of_two() {
                return Err(ExecError::Unsupported(
                    "hash sharding bucket count must be a power of two".into(),
                ));
            }
            Ok(crabka_pgcatalog::ShardingStrategy::Hash(
                crabka_pgcatalog::HashSharding {
                    columns: hash.columns.clone(),
                    buckets: hash.buckets,
                    co_location_group: hash.co_location_group.clone(),
                },
            ))
        }
    }
}
