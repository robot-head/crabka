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
    let query_ctes = crate::cte::evaluate_with_clause(ctx, q.with.as_ref())?;
    let query_ctx = ctx.with_ctes(&query_ctes);
    match &q.body {
        SetExpr::Query(QueryBody::Select(s)) => {
            if q.locking.is_some() {
                return Err(ExecError::Unsupported(
                    "locking SELECT must use execute_read_locking".into(),
                ));
            }
            let mut s = (**s).clone();
            s.order_by = q.order_by.clone();
            s.limit = q.limit.clone();
            s.offset = q.offset.clone();
            s.with_ties = q.with_ties;
            s.locking = q.locking.clone();
            crate::exec::select_to_relation_with_ctes(&query_ctx, &s)
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let mut rel = crate::values::values_to_relation_with_ctes(&query_ctx, v)?;
            let order_by = crate::subquery::resolve_order_items(&query_ctx, &q.order_by)?;
            let window = crate::exec::query_row_window(&query_ctx, q)?;
            crate::values::apply_query_order(&mut rel, &order_by, window, ctx.eval_ctx)?;
            Ok(rel)
        }
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
            let (fields, _exprs, _tys) = crate::exec::resolve_projection(&projection, &scope)?;
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
}
