//! The first executable planner node.
//!
//! P0a moves read shapes here one node at a time.  A FROM-less scalar SELECT is
//! already a real `Result` node: it has no scan to delegate and so is the safe
//! first cut-over from the legacy read path.

use std::collections::BTreeSet;

use crabka_pgparser::ast::{
    DistinctClause, QueryExpr, SelectItem, SelectStmt, TableExpr, ValuesStmt,
};

use crate::{
    bind::{BoundExpr, bind_optional},
    error::ExecError,
    exec,
    join::Relation,
    scope::Scope,
};

use super::query::{Executor, Plan, PlanNode, PlanState, RestrictInfo, TargetEntry};

/// Execute the subset of SELECT that is exactly one scalar `Result` node.
/// Returns `None` for every shape that still needs a later P0a node.
pub(crate) fn try_execute_result(
    select: &SelectStmt,
    ctx: &crate::clock::EvalCtx,
) -> Result<Option<Relation>, ExecError> {
    let Some(ResultPlan { plan, fields, tys }) = plan_result(select)? else {
        return Ok(None);
    };
    let mut state = PlanState::new(plan, Scope::empty());
    ResultExecutor { ctx, fields, tys }
        .execute(&mut state)
        .map(Some)
}

/// Execute the simple single-table slice of SELECT through `SeqScan`.
///
/// More elaborate tails remain on the established path until their own plan
/// nodes land, so this node owns only scan, filter, and scalar projection.
pub(crate) fn try_execute_seq_scan(
    read_ctx: &crate::subquery::SubCtx<'_>,
    select: &SelectStmt,
) -> Result<Option<Relation>, ExecError> {
    let Some(SeqScanPlan {
        plan,
        source,
        fields,
        tys,
        limit,
        offset,
        order_by,
        sort_positions,
        aggregate,
        project_set,
        window,
    }) = plan_seq_scan(read_ctx, select)?
    else {
        return Ok(None);
    };
    let mut state = PlanState::new(plan, Scope::empty());
    if let Some(select) = aggregate {
        AggregateExecutor {
            read_ctx,
            source,
            fields,
            tys,
            select,
        }
        .execute(&mut state)
        .map(Some)
    } else if let Some(select) = project_set {
        ProjectSetExecutor {
            read_ctx,
            source,
            fields,
            tys,
            select,
        }
        .execute(&mut state)
        .map(Some)
    } else if let Some(select) = window {
        WindowAggExecutor {
            read_ctx,
            source,
            select,
        }
        .execute(&mut state)
        .map(Some)
    } else if matches!(state.plan.node, PlanNode::Limit { .. }) {
        LimitExecutor {
            read_ctx,
            source,
            fields,
            tys,
            limit,
            offset,
            order_by,
            sort_positions,
        }
        .execute(&mut state)
        .map(Some)
    } else {
        execute_seq_scan_input(
            &mut state,
            read_ctx,
            source,
            fields,
            tys,
            order_by,
            sort_positions,
        )
        .map(Some)
    }
}

/// Execute a `VALUES` query through its `ValuesScan` node, including the query
/// expression's ORDER BY/OFFSET/LIMIT tail.
pub(crate) fn execute_values(
    ctx: &crate::subquery::SubCtx<'_>,
    query: &QueryExpr,
    values: &ValuesStmt,
) -> Result<Relation, ExecError> {
    let plan = Plan {
        target_list: Vec::new(),
        quals: Vec::new(),
        node: PlanNode::ValuesScan,
    };
    let mut state = PlanState::new(plan, Scope::empty());
    ValuesExecutor { ctx, query, values }.execute(&mut state)
}

struct ResultPlan {
    plan: Plan,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
}

fn plan_result(select: &SelectStmt) -> Result<Option<ResultPlan>, ExecError> {
    if !select.from.is_empty()
        || !matches!(select.distinct, DistinctClause::All)
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.with_ties
        || !select.group_by.is_empty()
        || select.grouping.is_some()
        || select.having.is_some()
        || is_ungrouped_aggregate(select)
        || crate::grouping::is_grouping_query(select)
        || crate::window::has_window_calls(select)
    {
        return Ok(None);
    }

    exec::reject_from_less_wildcard(&select.projection)?;
    let scope = Scope::empty();
    let (fields, exprs, tys): (
        Vec<crabka_pgwire::engine::FieldDescription>,
        Vec<crabka_pgparser::ast::Expr>,
        Vec<crabka_pgtypes::ColumnType>,
    ) = exec::resolve_projection(&select.projection, &scope)?;
    if crate::srf::exprs_contain_srf(&exprs) {
        return Ok(None);
    }
    let target_list = bind_target_list(&exprs, &fields, &scope)?;
    let quals = bind_optional(select.filter.as_ref(), &scope)?
        .into_iter()
        .map(|clause| RestrictInfo {
            clause,
            is_pushed_down: false,
            security_level: 0,
            leakproof: true,
            required_relids: BTreeSet::new(),
        })
        .collect();
    let plan = Plan {
        target_list,
        quals,
        node: PlanNode::Result,
    };
    Ok(Some(ResultPlan { plan, fields, tys }))
}

struct SeqScanPlan {
    plan: Plan,
    source: TableExpr,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
    limit: Option<crabka_pgparser::ast::Expr>,
    offset: Option<crabka_pgparser::ast::Expr>,
    order_by: Vec<crabka_pgparser::ast::OrderItem>,
    sort_positions: Vec<usize>,
    aggregate: Option<SelectStmt>,
    project_set: Option<SelectStmt>,
    window: Option<SelectStmt>,
}

fn plan_seq_scan(
    read_ctx: &crate::subquery::SubCtx<'_>,
    select: &SelectStmt,
) -> Result<Option<SeqScanPlan>, ExecError> {
    let [source] = select.from.as_slice() else {
        return Ok(None);
    };
    if matches!(source, TableExpr::Function { .. }) {
        return plan_function_scan(read_ctx, select, source);
    }
    if !crate::exec::is_direct_stored_base_table(read_ctx, source) {
        return Ok(None);
    }
    let aggregate = needs_aggregate_node(select);
    if aggregate
        && (matches!(
            select.distinct,
            DistinctClause::On(_) | DistinctClause::Distinct
        ) || !select.order_by.is_empty()
            || select.limit.is_some()
            || select.offset.is_some()
            || select.grouping.is_some())
    {
        return Ok(None);
    }
    if !aggregate && matches!(select.distinct, DistinctClause::On(_)) {
        return Ok(None);
    }
    if select.with_ties {
        return Ok(None);
    }
    if !aggregate && !select.group_by.is_empty() {
        return Ok(None);
    }
    if !aggregate && select.having.is_some() {
        return Ok(None);
    }
    if !aggregate && crate::grouping::is_grouping_query(select) {
        return Ok(None);
    }
    if !select
        .group_by
        .iter()
        .all(|expr| matches!(expr, crabka_pgparser::ast::Expr::Column { .. }))
    {
        return Ok(None);
    }
    let window = crate::window::has_window_calls(select);
    if window
        && (aggregate
            || crate::srf::projection_contains_srf(&select.projection)
            || !matches!(select.distinct, DistinctClause::All)
            || !select.order_by.is_empty()
            || select.limit.is_some()
            || select.offset.is_some())
    {
        return Ok(None);
    }

    // The legacy path already turns filtered indexed tables into bounded index
    // probes. Keep that access path until P3 supplies an index scan leaf.
    if select.filter.is_some() {
        let TableExpr::Table { name, .. } = source else {
            return Ok(None);
        };
        let relation = crate::relname::resolve_relation(
            read_ctx.catalog_kv,
            read_ctx.fctx.resolution,
            name,
            crate::relname::SchemaDisposition::Reference,
        )?;
        if !crabka_pgcatalog::list_table_indexes(read_ctx.catalog_kv, &relation)?.is_empty() {
            return Ok(None);
        }
    }

    let scope = crate::exec::build_from_schema_of_select(
        read_ctx.catalog_kv,
        read_ctx.fctx.resolution,
        select,
        read_ctx.ctes,
    )?
    .scope;
    if window {
        let quals = bind_optional(select.filter.as_ref(), &scope)?
            .into_iter()
            .map(|clause| RestrictInfo {
                clause,
                is_pushed_down: false,
                security_level: 0,
                leakproof: true,
                required_relids: BTreeSet::from([1]),
            })
            .collect();
        let scan = Plan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: PlanNode::SeqScan { scanrelid: 1 },
        };
        let filter = Plan {
            target_list: Vec::new(),
            quals,
            node: PlanNode::Filter {
                input: Box::new(scan),
            },
        };
        return Ok(Some(SeqScanPlan {
            plan: Plan {
                target_list: Vec::new(),
                quals: Vec::new(),
                node: PlanNode::WindowAgg {
                    input: Box::new(filter),
                },
            },
            source: source.clone(),
            fields: Vec::new(),
            tys: Vec::new(),
            limit: None,
            offset: None,
            order_by: Vec::new(),
            sort_positions: Vec::new(),
            aggregate: None,
            project_set: None,
            window: Some(select.clone()),
        }));
    }
    let (fields, exprs, tys) = exec::resolve_projection(&select.projection, &scope)?;
    let project_set = crate::srf::exprs_contain_srf(&exprs);
    if project_set
        && (aggregate
            || !matches!(select.distinct, DistinctClause::All)
            || !select.order_by.is_empty()
            || select.limit.is_some()
            || select.offset.is_some())
    {
        return Ok(None);
    }
    let target_list = bind_target_list(&exprs, &fields, &scope)?;
    let distinct = matches!(select.distinct, DistinctClause::Distinct);
    if distinct {
        for ty in &tys {
            crate::eval::require_equality_operator(*ty)?;
        }
    }
    let quals = bind_optional(select.filter.as_ref(), &scope)?
        .into_iter()
        .map(|clause| RestrictInfo {
            clause,
            is_pushed_down: false,
            security_level: 0,
            leakproof: true,
            required_relids: BTreeSet::from([1]),
        })
        .collect();
    let order_keys =
        exec::resolve_select_order_keys(&select.order_by, &scope, &fields, &exprs, false)?;
    let mut sort_positions = Vec::with_capacity(order_keys.len());
    for key in order_keys {
        let exec::SelectOrderKey::Output(index) = key else {
            return Ok(None);
        };
        crate::eval::require_ordering_operator(tys[index])?;
        sort_positions.push(index);
    }
    let scan = Plan {
        target_list: Vec::new(),
        quals: Vec::new(),
        node: PlanNode::SeqScan { scanrelid: 1 },
    };
    let filter = Plan {
        target_list: if aggregate || project_set {
            Vec::new()
        } else {
            target_list.clone()
        },
        quals,
        node: PlanNode::Filter {
            input: Box::new(scan),
        },
    };
    let aggregate = if aggregate {
        Plan {
            target_list: target_list.clone(),
            quals: Vec::new(),
            node: PlanNode::Aggregate {
                input: Box::new(filter),
            },
        }
    } else {
        filter
    };
    let project_set = if project_set {
        Plan {
            target_list: target_list.clone(),
            quals: Vec::new(),
            node: PlanNode::ProjectSet {
                input: Box::new(aggregate),
            },
        }
    } else {
        aggregate
    };
    let unique = if distinct {
        Plan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: PlanNode::Unique {
                input: Box::new(project_set),
            },
        }
    } else {
        project_set
    };
    let sort = if select.order_by.is_empty() {
        unique
    } else {
        Plan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: PlanNode::Sort {
                input: Box::new(unique),
            },
        }
    };
    let plan = if select.limit.is_some() || select.offset.is_some() {
        Plan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: PlanNode::Limit {
                input: Box::new(sort),
            },
        }
    } else {
        sort
    };
    Ok(Some(SeqScanPlan {
        plan,
        source: source.clone(),
        fields,
        tys,
        limit: select.limit.clone(),
        offset: select.offset.clone(),
        order_by: select.order_by.clone(),
        sort_positions,
        aggregate: needs_aggregate_node(select).then(|| select.clone()),
        project_set: crate::srf::exprs_contain_srf(&exprs).then(|| select.clone()),
        window: None,
    }))
}

fn plan_function_scan(
    read_ctx: &crate::subquery::SubCtx<'_>,
    select: &SelectStmt,
    source: &TableExpr,
) -> Result<Option<SeqScanPlan>, ExecError> {
    let TableExpr::Function {
        functions, lateral, ..
    } = source
    else {
        return Ok(None);
    };
    if *lateral
        || crate::routine::expands_as_table(read_ctx.catalog_kv, functions)
        || !matches!(select.distinct, DistinctClause::All)
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || crate::grouping::is_grouping_query(select)
        || crate::window::has_window_calls(select)
    {
        return Ok(None);
    }
    let scope = crate::exec::build_from_schema_of_select(
        read_ctx.catalog_kv,
        read_ctx.fctx.resolution,
        select,
        read_ctx.ctes,
    )?
    .scope;
    let (fields, exprs, tys) = exec::resolve_projection(&select.projection, &scope)?;
    if crate::srf::exprs_contain_srf(&exprs) {
        return Ok(None);
    }
    let plan = Plan {
        target_list: bind_target_list(&exprs, &fields, &scope)?,
        quals: bind_optional(select.filter.as_ref(), &scope)?
            .into_iter()
            .map(|clause| RestrictInfo {
                clause,
                is_pushed_down: false,
                security_level: 0,
                leakproof: true,
                required_relids: BTreeSet::from([1]),
            })
            .collect(),
        node: PlanNode::Filter {
            input: Box::new(Plan {
                target_list: Vec::new(),
                quals: Vec::new(),
                node: PlanNode::FunctionScan,
            }),
        },
    };
    Ok(Some(SeqScanPlan {
        plan,
        source: source.clone(),
        fields,
        tys,
        limit: None,
        offset: None,
        order_by: Vec::new(),
        sort_positions: Vec::new(),
        aggregate: None,
        project_set: None,
        window: None,
    }))
}

fn needs_aggregate_node(select: &SelectStmt) -> bool {
    !select.group_by.is_empty()
        || select.projection.iter().any(|item| {
            matches!(item, SelectItem::Expr { expr, .. } if crate::agg::contains_aggregate(expr))
        })
        || select
            .order_by
            .iter()
            .any(|item| crate::agg::contains_aggregate(&item.expr))
        || select
            .having
            .as_ref()
            .is_some_and(crate::agg::contains_aggregate)
}

fn bind_target_list(
    exprs: &[crabka_pgparser::ast::Expr],
    fields: &[crabka_pgwire::engine::FieldDescription],
    scope: &Scope,
) -> Result<Vec<TargetEntry>, ExecError> {
    exprs
        .iter()
        .zip(fields)
        .enumerate()
        .map(|(index, (expr, field))| {
            Ok(TargetEntry {
                expr: BoundExpr::new(expr, scope)?,
                resno: index + 1,
                resname: field.name.clone(),
            })
        })
        .collect()
}

fn is_ungrouped_aggregate(select: &SelectStmt) -> bool {
    crate::agg::is_aggregate_query(select)
        && select.group_by.is_empty()
        && select.grouping.is_none()
        && select.having.is_none()
}

struct ResultExecutor<'a> {
    ctx: &'a crate::clock::EvalCtx,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
}

impl Executor for ResultExecutor<'_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        if !matches!(state.plan.node, PlanNode::Result) {
            return Err(ExecError::Unsupported(
                "ResultExecutor received a non-Result plan".into(),
            ));
        }
        crate::session::check_query_canceled()?;
        state.begin_loop();
        let row = Vec::new();
        for qual in &state.plan.quals {
            if !exec::row_matches(Some(qual.clause.expr()), &state.scope, &row, self.ctx)? {
                state.remove_row();
                return Ok(Relation {
                    scope: exec::projected_scope(&self.fields, &self.tys),
                    rows: Vec::new(),
                });
            }
        }
        state.emit_row();
        let exprs: Vec<_> = state
            .plan
            .target_list
            .iter()
            .map(|target| target.expr.expr().clone())
            .collect();
        Ok(Relation {
            scope: exec::projected_scope(&self.fields, &self.tys),
            rows: exec::project_rows(&exprs, &state.scope, &[row], self.ctx)?,
        })
    }
}

struct SeqScanExecutor<'a, 'b> {
    read_ctx: &'a crate::subquery::SubCtx<'b>,
    source: TableExpr,
}

impl Executor for SeqScanExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        if !matches!(state.plan.node, PlanNode::SeqScan { scanrelid: 1 }) {
            return Err(ExecError::Unsupported(
                "SeqScanExecutor received a non-SeqScan plan".into(),
            ));
        }
        crate::session::check_query_canceled()?;
        state.begin_loop();
        let TableExpr::Table { name, .. } = &self.source else {
            return Err(ExecError::Unsupported(
                "SeqScanExecutor received a non-table source".into(),
            ));
        };
        let name = crate::relname::resolve_relation(
            self.read_ctx.catalog_kv,
            self.read_ctx.fctx.resolution,
            name,
            crate::relname::SchemaDisposition::Reference,
        )?;
        let relation =
            exec::scan_stored_base_table(self.read_ctx, &self.source, &name, None, None)?;
        state.scope = relation.scope.clone();
        for _ in &relation.rows {
            state.emit_row();
        }
        Ok(relation)
    }
}

struct FunctionScanExecutor<'a, 'b> {
    read_ctx: &'a crate::subquery::SubCtx<'b>,
    source: TableExpr,
}

impl Executor for FunctionScanExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        if !matches!(state.plan.node, PlanNode::FunctionScan) {
            return Err(ExecError::Unsupported(
                "FunctionScanExecutor received a non-FunctionScan plan".into(),
            ));
        }
        crate::session::check_query_canceled()?;
        let TableExpr::Function {
            functions,
            with_ordinality,
            rows_from,
            alias,
            column_aliases,
            ..
        } = &self.source
        else {
            return Err(ExecError::Unsupported(
                "FunctionScanExecutor received a non-function source".into(),
            ));
        };
        state.begin_loop();
        let relation = crate::srf::from_item_with_memory(
            functions,
            *with_ordinality,
            *rows_from,
            alias.as_deref(),
            column_aliases,
            self.read_ctx.eval_ctx,
            &self.read_ctx.statement_memory,
        )?;
        state.scope = relation.scope.clone();
        for _ in &relation.rows {
            state.emit_row();
        }
        Ok(relation)
    }
}

struct FilterExecutor<'a, 'b> {
    read_ctx: &'a crate::subquery::SubCtx<'b>,
    source: TableExpr,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
}

impl Executor for FilterExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        let relation = execute_filter_input(state, self.read_ctx, self.source.clone())?;
        project_filter_rows(
            state,
            relation,
            &self.fields,
            &self.tys,
            self.read_ctx.eval_ctx,
        )
    }
}

fn execute_filter_input(
    state: &mut PlanState,
    read_ctx: &crate::subquery::SubCtx<'_>,
    source: TableExpr,
) -> Result<Relation, ExecError> {
    let PlanNode::Filter { input } = &state.plan.node else {
        return Err(ExecError::Unsupported(
            "FilterExecutor received a non-Filter plan".into(),
        ));
    };
    crate::session::check_query_canceled()?;
    let mut child = PlanState::new((**input).clone(), Scope::empty());
    let relation = match child.plan.node {
        PlanNode::SeqScan { .. } => SeqScanExecutor { read_ctx, source }.execute(&mut child)?,
        PlanNode::FunctionScan => FunctionScanExecutor { read_ctx, source }.execute(&mut child)?,
        _ => {
            return Err(ExecError::Unsupported(
                "FilterExecutor input was neither SeqScan nor FunctionScan".into(),
            ));
        }
    };
    state.begin_loop();
    filter_relation_rows(state, relation, read_ctx.eval_ctx)
}

#[cfg(test)]
fn execute_filter_rows(
    state: &mut PlanState,
    relation: Relation,
    fields: &[crabka_pgwire::engine::FieldDescription],
    tys: &[crabka_pgtypes::ColumnType],
    ctx: &crate::clock::EvalCtx,
) -> Result<Relation, ExecError> {
    let relation = filter_relation_rows(state, relation, ctx)?;
    project_filter_rows(state, relation, fields, tys, ctx)
}

fn project_filter_rows(
    state: &PlanState,
    relation: Relation,
    fields: &[crabka_pgwire::engine::FieldDescription],
    tys: &[crabka_pgtypes::ColumnType],
    ctx: &crate::clock::EvalCtx,
) -> Result<Relation, ExecError> {
    let exprs: Vec<_> = state
        .plan
        .target_list
        .iter()
        .map(|target| target.expr.expr().clone())
        .collect();
    Ok(Relation {
        scope: exec::projected_scope(fields, tys),
        rows: exec::project_rows(&exprs, &relation.scope, &relation.rows, ctx)?,
    })
}

fn filter_relation_rows(
    state: &mut PlanState,
    relation: Relation,
    ctx: &crate::clock::EvalCtx,
) -> Result<Relation, ExecError> {
    state.scope = relation.scope.clone();
    let mut kept = Vec::with_capacity(relation.rows.len());
    for row in relation.rows {
        let mut matched = true;
        for qual in &state.plan.quals {
            if !exec::row_matches(Some(qual.clause.expr()), &state.scope, &row, ctx)? {
                matched = false;
                break;
            }
        }
        if matched {
            state.emit_row();
            kept.push(row);
        } else {
            state.remove_row();
        }
    }
    Ok(Relation {
        scope: state.scope.clone(),
        rows: kept,
    })
}

struct AggregateExecutor<'a, 'b> {
    read_ctx: &'a crate::subquery::SubCtx<'b>,
    source: TableExpr,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
    select: SelectStmt,
}

impl Executor for AggregateExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        let PlanNode::Aggregate { input } = &state.plan.node else {
            return Err(ExecError::Unsupported(
                "AggregateExecutor received a non-Aggregate plan".into(),
            ));
        };
        crate::session::check_query_canceled()?;
        let mut child = PlanState::new((**input).clone(), Scope::empty());
        let relation = execute_filter_input(&mut child, self.read_ctx, self.source.clone())?;
        state.begin_loop();
        let rows = crate::agg::aggregate_rows_with_memory(
            &self.select,
            &relation.scope,
            relation.rows,
            self.read_ctx.eval_ctx,
            &self.read_ctx.statement_memory,
        )?;
        state.scope = exec::projected_scope(&self.fields, &self.tys);
        for _ in &rows {
            state.emit_row();
        }
        Ok(Relation {
            scope: state.scope.clone(),
            rows,
        })
    }
}

struct ProjectSetExecutor<'a, 'b> {
    read_ctx: &'a crate::subquery::SubCtx<'b>,
    source: TableExpr,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
    select: SelectStmt,
}

impl Executor for ProjectSetExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        let PlanNode::ProjectSet { input } = &state.plan.node else {
            return Err(ExecError::Unsupported(
                "ProjectSetExecutor received a non-ProjectSet plan".into(),
            ));
        };
        crate::session::check_query_canceled()?;
        let mut child = PlanState::new((**input).clone(), Scope::empty());
        let relation = execute_filter_input(&mut child, self.read_ctx, self.source.clone())?;
        let (_, exprs, _) = exec::resolve_projection(&self.select.projection, &relation.scope)?;
        state.begin_loop();
        let rows = crate::srf::project_rows_ordered_with_memory(
            &self.select,
            &relation.scope,
            &self.fields,
            &exprs,
            relation.rows,
            self.read_ctx.eval_ctx,
            &self.read_ctx.statement_memory,
        )?;
        state.scope = exec::projected_scope(&self.fields, &self.tys);
        for _ in &rows {
            state.emit_row();
        }
        Ok(Relation {
            scope: state.scope.clone(),
            rows,
        })
    }
}

struct WindowAggExecutor<'a, 'b> {
    read_ctx: &'a crate::subquery::SubCtx<'b>,
    source: TableExpr,
    select: SelectStmt,
}

impl Executor for WindowAggExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        let PlanNode::WindowAgg { input } = &state.plan.node else {
            return Err(ExecError::Unsupported(
                "WindowAggExecutor received a non-WindowAgg plan".into(),
            ));
        };
        crate::session::check_query_canceled()?;
        let mut child = PlanState::new((**input).clone(), Scope::empty());
        let relation = execute_filter_input(&mut child, self.read_ctx, self.source.clone())?;
        state.begin_loop();
        let (fields, tys, rows) = crate::window::execute(
            &self.select,
            &relation.scope,
            relation.rows,
            self.read_ctx.eval_ctx,
        )?;
        state.scope = exec::projected_scope(&fields, &tys);
        for _ in &rows {
            state.emit_row();
        }
        Ok(Relation {
            scope: state.scope.clone(),
            rows,
        })
    }
}

struct LimitExecutor<'a, 'b> {
    read_ctx: &'a crate::subquery::SubCtx<'b>,
    source: TableExpr,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
    limit: Option<crabka_pgparser::ast::Expr>,
    offset: Option<crabka_pgparser::ast::Expr>,
    order_by: Vec<crabka_pgparser::ast::OrderItem>,
    sort_positions: Vec<usize>,
}

impl Executor for LimitExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        let PlanNode::Limit { input } = &state.plan.node else {
            return Err(ExecError::Unsupported(
                "LimitExecutor received a non-Limit plan".into(),
            ));
        };
        crate::session::check_query_canceled()?;
        let mut child = PlanState::new((**input).clone(), Scope::empty());
        let relation = execute_seq_scan_input(
            &mut child,
            self.read_ctx,
            self.source.clone(),
            self.fields.clone(),
            self.tys.clone(),
            self.order_by.clone(),
            self.sort_positions.clone(),
        )?;
        let offset = crate::exec::eval_row_count(
            self.offset.as_ref(),
            crate::exec::RowCountClause::Offset,
            self.read_ctx.eval_ctx,
        )?;
        let limit = crate::exec::eval_row_count(
            self.limit.as_ref(),
            crate::exec::RowCountClause::Limit,
            self.read_ctx.eval_ctx,
        )?;
        state.begin_loop();
        Ok(limit_relation_rows(state, relation, offset, limit))
    }
}

fn execute_seq_scan_input(
    state: &mut PlanState,
    read_ctx: &crate::subquery::SubCtx<'_>,
    source: TableExpr,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
    order_by: Vec<crabka_pgparser::ast::OrderItem>,
    sort_positions: Vec<usize>,
) -> Result<Relation, ExecError> {
    match &state.plan.node {
        PlanNode::Filter { .. } => FilterExecutor {
            read_ctx,
            source,
            fields,
            tys,
        }
        .execute(state),
        PlanNode::Sort { .. } => SortExecutor {
            read_ctx,
            source,
            fields,
            tys,
            order_by,
            sort_positions,
        }
        .execute(state),
        PlanNode::Unique { .. } => UniqueExecutor {
            read_ctx,
            source,
            fields,
            tys,
        }
        .execute(state),
        _ => Err(ExecError::Unsupported(
            "single-table plan input was neither Filter, Unique, nor Sort".into(),
        )),
    }
}

struct SortExecutor<'a, 'b> {
    read_ctx: &'a crate::subquery::SubCtx<'b>,
    source: TableExpr,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
    order_by: Vec<crabka_pgparser::ast::OrderItem>,
    sort_positions: Vec<usize>,
}

struct UniqueExecutor<'a, 'b> {
    read_ctx: &'a crate::subquery::SubCtx<'b>,
    source: TableExpr,
    fields: Vec<crabka_pgwire::engine::FieldDescription>,
    tys: Vec<crabka_pgtypes::ColumnType>,
}

impl Executor for UniqueExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        let PlanNode::Unique { input } = &state.plan.node else {
            return Err(ExecError::Unsupported(
                "UniqueExecutor received a non-Unique plan".into(),
            ));
        };
        crate::session::check_query_canceled()?;
        let mut child = PlanState::new((**input).clone(), Scope::empty());
        let relation = execute_seq_scan_input(
            &mut child,
            self.read_ctx,
            self.source.clone(),
            self.fields.clone(),
            self.tys.clone(),
            Vec::new(),
            Vec::new(),
        )?;
        state.begin_loop();
        unique_relation_rows(state, relation, &self.read_ctx.statement_memory)
    }
}

fn unique_relation_rows(
    state: &mut PlanState,
    mut relation: Relation,
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Relation, ExecError> {
    let input = std::mem::take(&mut relation.rows);
    let reservation = statement_memory.reserve();
    let mut seen = std::collections::HashSet::with_capacity(input.len());
    let mut rows = Vec::with_capacity(input.len());
    for row in input {
        if seen.insert(row.clone()) {
            reservation.memory().charge_row(&row)?;
            state.emit_row();
            rows.push(row);
        } else {
            state.remove_row();
        }
    }
    relation.rows = rows;
    state.scope = relation.scope.clone();
    Ok(relation)
}

impl Executor for SortExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        let PlanNode::Sort { input } = &state.plan.node else {
            return Err(ExecError::Unsupported(
                "SortExecutor received a non-Sort plan".into(),
            ));
        };
        crate::session::check_query_canceled()?;
        let mut child = PlanState::new((**input).clone(), Scope::empty());
        let relation = execute_seq_scan_input(
            &mut child,
            self.read_ctx,
            self.source.clone(),
            self.fields.clone(),
            self.tys.clone(),
            Vec::new(),
            Vec::new(),
        )?;
        state.begin_loop();
        sort_relation_rows(
            state,
            relation,
            &self.order_by,
            &self.sort_positions,
            &self.read_ctx.statement_memory,
        )
    }
}

fn sort_relation_rows(
    state: &mut PlanState,
    mut relation: Relation,
    order_by: &[crabka_pgparser::ast::OrderItem],
    positions: &[usize],
    statement_memory: &crate::scanner::StatementMemory,
) -> Result<Relation, ExecError> {
    let reservation = statement_memory.reserve();
    let mut keyed = Vec::with_capacity(relation.rows.len());
    for row in relation.rows {
        let keys: Vec<_> = positions.iter().map(|&index| row[index].clone()).collect();
        reservation
            .memory()
            .charge(crate::scanner::datum_row_bytes(&keys))?;
        keyed.push((keys, row));
    }
    keyed.sort_by(|left, right| exec::order_cmp(&left.0, &right.0, order_by));
    relation.rows = keyed.into_iter().map(|(_, row)| row).collect();
    state.scope = relation.scope.clone();
    for _ in &relation.rows {
        state.emit_row();
    }
    Ok(relation)
}

fn limit_relation_rows(
    state: &mut PlanState,
    mut relation: Relation,
    offset: Option<i64>,
    limit: Option<i64>,
) -> Relation {
    let offset = offset
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0);
    let limit = limit
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(usize::MAX);
    relation.rows = relation.rows.into_iter().skip(offset).take(limit).collect();
    state.scope = relation.scope.clone();
    for _ in &relation.rows {
        state.emit_row();
    }
    relation
}

struct ValuesExecutor<'a, 'b> {
    ctx: &'a crate::subquery::SubCtx<'b>,
    query: &'a QueryExpr,
    values: &'a ValuesStmt,
}

impl Executor for ValuesExecutor<'_, '_> {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError> {
        if !matches!(state.plan.node, PlanNode::ValuesScan) {
            return Err(ExecError::Unsupported(
                "ValuesExecutor received a non-ValuesScan plan".into(),
            ));
        }
        crate::session::check_query_canceled()?;
        state.begin_loop();
        let mut relation = crate::values::values_to_relation_with_ctes(self.ctx, self.values)?;
        let order_by = crate::subquery::resolve_order_items(self.ctx, &self.query.order_by)?;
        let window = crate::exec::query_row_window(self.ctx, self.query)?;
        crate::values::apply_query_order(&mut relation, &order_by, window, self.ctx.eval_ctx)?;
        state.scope = relation.scope.clone();
        for _ in &relation.rows {
            state.emit_row();
        }
        Ok(relation)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use crabka_pgparser::ast::{Expr, GroupingClause, QueryBody, SetExpr, Statement};
    use crabka_pgtypes::{ColumnType, Datum};

    use super::*;
    use crate::scope::{ColumnBinding, Exposure};

    fn select(sql: &str) -> SelectStmt {
        let statements = crabka_pgparser::parser::parse(sql).expect("query parses");
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("expected one query")
        };
        let query = query.clone();
        let SetExpr::Query(QueryBody::Select(select)) = query.body else {
            panic!("expected SELECT body")
        };
        SelectStmt {
            order_by: query.order_by,
            limit: query.limit,
            offset: query.offset,
            with_ties: query.with_ties,
            locking: query.locking,
            ..*select
        }
    }

    #[test]
    fn result_plan_keeps_one_based_target_positions() {
        let planned = plan_result(&select("SELECT 1 AS one, 2 AS two"))
            .expect("plan ok")
            .expect("Result plan");

        assert_eq!(
            planned
                .plan
                .target_list
                .iter()
                .map(|target| (target.resname.as_str(), target.resno))
                .collect::<Vec<_>>(),
            vec![("one", 1), ("two", 2)]
        );
    }

    #[test]
    fn target_entries_keep_one_based_positions_for_scan_plans() {
        let scope = Scope::empty();
        let exprs = [
            crabka_pgparser::parser::parse_expression("1").expect("first expression"),
            crabka_pgparser::parser::parse_expression("2").expect("second expression"),
        ];
        let fields = [
            exec::field("one", ColumnType::Int4),
            exec::field("two", ColumnType::Int4),
        ];

        let target_list = bind_target_list(&exprs, &fields, &scope).expect("target list binds");

        assert_eq!(
            target_list
                .iter()
                .map(|target| (target.resname.as_str(), target.resno))
                .collect::<Vec<_>>(),
            vec![("one", 1), ("two", 2)]
        );
    }

    #[test]
    fn aggregate_node_detection_keeps_plain_rows_out_of_grouping() {
        assert!(needs_aggregate_node(&select("SELECT a FROM t GROUP BY a")));
        assert!(needs_aggregate_node(&select("SELECT count(*) FROM t")));
        assert!(!needs_aggregate_node(&select("SELECT a FROM t")));
        assert!(!needs_aggregate_node(&select("SELECT grouping(a) FROM t")));
    }

    #[test]
    fn result_plan_declines_every_shape_owned_by_a_later_node() {
        for sql in [
            "SELECT 1 FROM t",
            "SELECT DISTINCT 1",
            "SELECT 1 ORDER BY 1",
            "SELECT 1 LIMIT 1",
            "SELECT 1 OFFSET 0",
            "SELECT 1 GROUP BY 1",
            "SELECT count(*)",
            "SELECT row_number() OVER ()",
        ] {
            assert!(plan_result(&select(sql)).expect(sql).is_none(), "{sql}");
        }

        let mut group_by = select("SELECT 1");
        group_by.group_by = vec![crabka_pgparser::parser::parse_expression("1").expect("expr")];
        assert!(plan_result(&group_by).expect("group by").is_none());

        let mut grouping = select("SELECT 1");
        grouping.grouping = Some(GroupingClause {
            distinct: false,
            items: Vec::new(),
        });
        assert!(plan_result(&grouping).expect("grouping").is_none());

        let mut having = select("SELECT 1");
        having.having = Some(crabka_pgparser::parser::parse_expression("true").expect("expr"));
        assert!(plan_result(&having).expect("having").is_none());
    }

    #[test]
    fn result_executor_emits_or_rejects_its_single_row() {
        let ctx = crate::clock::EvalCtx::test_default();
        let emitted = try_execute_result(&select("SELECT 2 + 3 WHERE true"), &ctx)
            .expect("execute ok")
            .expect("Result plan");
        let rejected = try_execute_result(&select("SELECT 2 + 3 WHERE false"), &ctx)
            .expect("execute ok")
            .expect("Result plan");

        assert_eq!(emitted.rows, vec![vec![crabka_pgtypes::Datum::Int4(5)]]);
        assert!(rejected.rows.is_empty());
    }

    #[test]
    fn result_executor_observes_query_cancellation_before_work() {
        let canceled = Arc::new(AtomicBool::new(true));
        let result = crate::session::with_query_cancel_runtime(Some(Arc::clone(&canceled)), || {
            try_execute_result(
                &select("SELECT 2 + 3"),
                &crate::clock::EvalCtx::test_default(),
            )
        });

        assert!(result.is_err());
        assert!(canceled.load(Ordering::Acquire));
    }

    #[test]
    fn seq_scan_filters_then_projects_and_counts_rows() {
        let scope = Scope {
            columns: vec![ColumnBinding {
                exposure: Exposure::Output,
                qualifier: Some("t".into()),
                name: "a".into(),
                ty: ColumnType::Int4,
            }],
        };
        let projection = Expr::Column {
            table: Some("t".into()),
            name: "a".into(),
        };
        let filter = crabka_pgparser::parser::parse_expression("t.a > 1").expect("filter");
        let plan = Plan {
            target_list: vec![TargetEntry {
                expr: BoundExpr::new(&projection, &scope).expect("bound projection"),
                resno: 1,
                resname: "a".into(),
            }],
            quals: vec![RestrictInfo {
                clause: BoundExpr::new(&filter, &scope).expect("bound filter"),
                is_pushed_down: false,
                security_level: 0,
                leakproof: true,
                required_relids: BTreeSet::from([1]),
            }],
            node: PlanNode::Filter {
                input: Box::new(Plan {
                    target_list: Vec::new(),
                    quals: Vec::new(),
                    node: PlanNode::SeqScan { scanrelid: 1 },
                }),
            },
        };
        let mut state = PlanState::new(plan, Scope::empty());
        state.begin_loop();
        let relation = execute_filter_rows(
            &mut state,
            Relation {
                scope,
                rows: vec![vec![Datum::Int4(1)], vec![Datum::Int4(2)]],
            },
            &[exec::field("a", ColumnType::Int4)],
            &[ColumnType::Int4],
            &crate::clock::EvalCtx::test_default(),
        )
        .expect("scan executes");

        assert_eq!(relation.rows, vec![vec![Datum::Int4(2)]]);
        assert_eq!((state.nloops, state.ntuples, state.rows_removed), (1, 1, 1));
    }

    #[test]
    fn limit_node_applies_offset_and_limit_after_its_input() {
        let plan = Plan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: PlanNode::Limit {
                input: Box::new(Plan {
                    target_list: Vec::new(),
                    quals: Vec::new(),
                    node: PlanNode::Result,
                }),
            },
        };
        let mut state = PlanState::new(plan, Scope::empty());
        state.begin_loop();
        let relation = limit_relation_rows(
            &mut state,
            Relation {
                scope: Scope::empty(),
                rows: vec![
                    vec![Datum::Int4(1)],
                    vec![Datum::Int4(2)],
                    vec![Datum::Int4(3)],
                ],
            },
            Some(1),
            Some(1),
        );

        assert_eq!(relation.rows, vec![vec![Datum::Int4(2)]]);
        assert_eq!((state.nloops, state.ntuples, state.rows_removed), (1, 1, 0));
    }

    #[test]
    fn unique_node_charges_its_retained_keys() {
        let plan = Plan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: PlanNode::Unique {
                input: Box::new(Plan {
                    target_list: Vec::new(),
                    quals: Vec::new(),
                    node: PlanNode::Result,
                }),
            },
        };
        let mut state = PlanState::new(plan, Scope::empty());
        state.begin_loop();

        let error = unique_relation_rows(
            &mut state,
            Relation {
                scope: Scope::empty(),
                rows: vec![vec![Datum::Int4(1)]],
            },
            &crate::scanner::StatementMemory::new(crabka_units::bytes(0)),
        )
        .expect_err("retained distinct rows must respect statement memory")
        .into_pg();

        assert_eq!(error.code, "53200");
    }
}
