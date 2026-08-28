use crabka_pgkv::Kv;
use crabka_pgparser::ast::{QueryBody, QueryExpr, SetExpr};
use crabka_pgwire::engine::FieldDescription;

use crate::{clock::EvalCtx, error::ExecError, join::Relation, scope::Scope, subquery::SubCtx};

pub(crate) fn query_to_relation(ctx: &SubCtx<'_>, q: &QueryExpr) -> Result<Relation, ExecError> {
    query_to_relation_with_ctes(ctx, q)
}

pub(crate) fn query_to_relation_with_ctes(
    ctx: &SubCtx<'_>,
    q: &QueryExpr,
) -> Result<Relation, ExecError> {
    let query_ctes = crate::cte::evaluate_with_clause(ctx, q.with.as_ref(), simple_cte_limit(q))?;
    let query_ctx = ctx.with_ctes(&query_ctes);
    match &q.body {
        SetExpr::Query(QueryBody::Select(s)) => {
            if q.locking.is_some() {
                return Err(ExecError::Unsupported(
                    "locking SELECT must use execute_read_locking".into(),
                ));
            }
            let s = crate::plan::exec::select_with_query_tail(q, s);
            crate::window::reject_misplaced_calls(&s)?;
            crate::grouping::reject_misplaced_calls(&s)?;
            crate::srf::reject_misplaced_calls(&s)?;
            if !crate::exec::select_contains_subquery(&s) {
                let planned = crate::subquery::resolve_in_select(&query_ctx, &s)?;
                if let Some((relation, state)) =
                    crate::plan::exec::try_execute_result_with_state(&planned, ctx.eval_ctx)?
                {
                    query_ctx.record_plan_state(state);
                    return Ok(relation);
                }
                let refs = crate::scope::StatementRefs::of_select(&planned);
                let plan_ctx = query_ctx.with_refs(&refs);
                if !crate::scope::wants_system_column(plan_ctx.refs)
                    && let Some(relation) =
                        crate::exec::try_execute_local_streaming_aggregate(&plan_ctx, &planned)?
                {
                    return Ok(relation);
                }
                if let Some((relation, state)) =
                    crate::plan::exec::try_execute_seq_scan_with_state(&plan_ctx, &planned)?
                {
                    plan_ctx.record_plan_state(state);
                    return Ok(relation);
                }
                return crate::exec::select_to_relation_with_ctes(&query_ctx, &planned);
            }
            crate::exec::select_to_relation_with_ctes(&query_ctx, &s)
        }
        SetExpr::Query(QueryBody::Values(v)) => crate::plan::exec::execute_values(&query_ctx, q, v),
        SetExpr::Query(QueryBody::Nested(nested)) => {
            if q.locking.is_some() {
                return Err(ExecError::Unsupported(
                    "locking SELECT must use execute_read_locking".into(),
                ));
            }
            let mut rel = query_to_relation_with_ctes(&query_ctx, nested)?;
            let order_by = crate::subquery::resolve_order_items(&query_ctx, &q.order_by)?;
            let window = crate::exec::query_row_window(&query_ctx, q)?;
            crate::values::apply_query_order(&mut rel, &order_by, window, ctx.eval_ctx)?;
            Ok(rel)
        }
        SetExpr::SetOp { .. } => {
            let order_by = crate::subquery::resolve_order_items(&query_ctx, &q.order_by)?;
            let window = crate::exec::query_row_window(&query_ctx, q)?;
            crate::setops::set_expr_to_relation(&query_ctx, &q.body, &order_by, window)
        }
    }
}

/// A plain CTE scan can stop producing when its enclosing query has a literal
/// `LIMIT`. Other shapes can need rows beyond their output limit (for example,
/// a filter, sort, or join), so they intentionally retain CTE materialization.
fn simple_cte_limit(q: &QueryExpr) -> Option<(&str, usize)> {
    let SetExpr::Query(QueryBody::Select(select)) = &q.body else {
        return None;
    };
    let [
        crabka_pgparser::ast::TableExpr::Table {
            name,
            only: false,
            columns: None,
            sample: None,
            ..
        },
    ] = select.from.as_slice()
    else {
        return None;
    };
    if name.schema.is_some()
        || !q.order_by.is_empty()
        || q.offset.is_some()
        || q.with_ties
        || q.locking.is_some()
        || !matches!(q.limit, Some(crabka_pgparser::ast::Expr::IntLiteral(_)))
        || !matches!(
            select.projection.as_slice(),
            [crabka_pgparser::ast::SelectItem::Wildcard]
        )
        || !matches!(select.distinct, crabka_pgparser::ast::DistinctClause::All)
        || select.filter.is_some()
        || !select.group_by.is_empty()
        || select.grouping.is_some()
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.window_calls.is_empty()
    {
        return None;
    }
    let crabka_pgparser::ast::Expr::IntLiteral(limit) = q.limit.as_ref()? else {
        return None;
    };
    limit.parse().ok().map(|limit| (name.name.as_str(), limit))
}

pub(crate) fn describe_query_expr(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    q: &QueryExpr,
) -> Result<Vec<FieldDescription>, ExecError> {
    let ctes = crate::cte::CteContext::empty();
    describe_query_expr_inner(catalog_kv, resolution, q, &ctes, true)
}

pub(crate) fn describe_query_expr_with_ctes(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    q: &QueryExpr,
    ctes: &crate::cte::CteContext,
) -> Result<Vec<FieldDescription>, ExecError> {
    describe_query_expr_inner(catalog_kv, resolution, q, ctes, false)
}

fn describe_query_expr_inner(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    q: &QueryExpr,
    ctes: &crate::cte::CteContext,
    allow_locking: bool,
) -> Result<Vec<FieldDescription>, ExecError> {
    if !allow_locking && q.locking.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not supported in CTEs or derived tables".into(),
        ));
    }
    if allow_locking && q.locking.is_some() && q.with.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE with CTEs is not supported".into(),
        ));
    }
    let query_ctes =
        crate::cte::describe_with_clause(catalog_kv, resolution, q.with.as_ref(), ctes)?;
    match &q.body {
        SetExpr::Query(QueryBody::Select(s)) => {
            if !allow_locking {
                crate::exec::reject_nested_relation_locking(s)?;
            }
            let scope = if s.from.is_empty() {
                crate::exec::reject_from_less_wildcard(&s.projection)?;
                Scope::empty()
            } else {
                crate::exec::build_from_schema_of_select(catalog_kv, resolution, s, &query_ctes)?
                    .scope
            };
            let projection = crate::subquery::resolve_types_in_projection_with_ctes(
                catalog_kv,
                resolution,
                &s.projection,
                &query_ctes,
            )?;
            // A window call's result is a synthetic column of the row the
            // projection resolves against, so Describe types it exactly as
            // execution does.
            let scope = crate::window::describe_scope(s, &scope)?;
            let (mut fields, _exprs, _tys) = crate::exec::resolve_projection(&projection, &scope)?;
            for (field, item) in fields.iter_mut().zip(&s.projection) {
                if let crabka_pgparser::ast::SelectItem::Expr { expr, alias } = item {
                    field.name = alias
                        .clone()
                        .unwrap_or_else(|| crate::exec::derived_name(expr));
                }
            }
            Ok(fields)
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let rel = crate::values::values_schema_relation_with_ctes(
                catalog_kv,
                resolution,
                v,
                &query_ctes,
            )?;
            Ok(rel
                .scope
                .columns
                .iter()
                .map(|c| crate::exec::field(&c.name, c.ty))
                .collect())
        }
        SetExpr::Query(QueryBody::Nested(nested)) => {
            describe_query_expr_inner(catalog_kv, resolution, nested, &query_ctes, false)
        }
        SetExpr::SetOp { .. } => {
            crate::setops::describe_set_expr_with_ctes(catalog_kv, resolution, &q.body, &query_ctes)
        }
    }
}

pub(crate) fn relation_to_rows_result(
    rel: Relation,
    ctx: &EvalCtx,
) -> crabka_pgwire::engine::QueryResult {
    let fields = rel
        .scope
        .columns
        .iter()
        .map(|c| crate::exec::field(&c.name, c.ty))
        .collect();
    crate::exec::rows_result(fields, &rel.rows, ctx.output_style())
}

#[cfg(test)]
mod tests {
    use crabka_pgwire::engine::{Engine, QueryResult, Session};

    use crate::SqlEngine;

    async fn run(sql: &str) -> QueryResult {
        SqlEngine::new()
            .connect()
            .simple_query(sql)
            .await
            .expect("query ok")
            .pop()
            .expect("one result")
    }

    fn cells(result: QueryResult) -> Vec<Vec<Option<String>>> {
        match result {
            QueryResult::Rows { rows, .. } => rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8")))
                        .collect()
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn top_level_select_values_and_setops_use_query_pipeline() {
        assert_eq!(cells(run("SELECT 1").await), vec![vec![Some("1".into())]]);
        assert_eq!(
            cells(run("VALUES (2), (1) ORDER BY 1").await),
            vec![vec![Some("1".into())], vec![Some("2".into())]]
        );
        assert_eq!(
            cells(run("SELECT 1 UNION SELECT 2 ORDER BY 1").await),
            vec![vec![Some("1".into())], vec![Some("2".into())]]
        );
        assert_eq!(
            cells(run("(VALUES (2), (1) ORDER BY 1) LIMIT 1").await),
            vec![vec![Some("1".into())]]
        );
    }

    #[tokio::test]
    async fn fromless_select_uses_the_result_plan_for_its_filter_and_projection() {
        assert_eq!(
            cells(run("SELECT 2 + 3 WHERE true").await),
            vec![vec![Some("5".into())]]
        );
        assert_eq!(
            cells(run("SELECT 2 + 3 WHERE false").await),
            Vec::<Vec<Option<String>>>::new()
        );
    }
}
