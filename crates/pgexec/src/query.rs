use crabka_pgkv::Kv;
use crabka_pgmvcc::visibility::Snapshot;
use crabka_pgparser::ast::{QueryBody, QueryExpr, SetExpr};
use crabka_pgwire::engine::FieldDescription;

use crate::{
    clock::EvalCtx, error::ExecError, join::Relation, scanner::RangeScanner, scope::Scope,
};

#[allow(clippy::too_many_arguments)]
pub(crate) fn query_to_relation(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    q: &QueryExpr,
    ctx: &EvalCtx,
    fctx: crate::exec::ForeignCtx,
    range_scanner: &dyn RangeScanner,
) -> Result<Relation, ExecError> {
    let ctes = crate::cte::CteContext::empty();
    query_to_relation_with_ctes(
        catalog_kv,
        kv,
        global,
        gsnap,
        snapshot,
        own,
        q,
        &ctes,
        ctx,
        fctx,
        range_scanner,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn query_to_relation_with_ctes(
    catalog_kv: &dyn Kv,
    kv: &dyn Kv,
    global: &dyn Kv,
    gsnap: &Snapshot,
    snapshot: &Snapshot,
    own: Option<u64>,
    q: &QueryExpr,
    ctes: &crate::cte::CteContext,
    ctx: &EvalCtx,
    fctx: crate::exec::ForeignCtx,
    range_scanner: &dyn RangeScanner,
) -> Result<Relation, ExecError> {
    let query_ctes = crate::cte::evaluate_with_clause(
        catalog_kv,
        kv,
        global,
        gsnap,
        snapshot,
        own,
        q.with.as_ref(),
        ctes,
        ctx,
        fctx,
        range_scanner,
    )?;
    let sub_ctx = crate::subquery::SubCtx {
        catalog_kv,
        kv,
        global,
        gsnap,
        snapshot,
        own,
        ctes: &query_ctes,
        eval_ctx: ctx,
        fctx,
        range_scanner,
    };
    match &q.body {
        SetExpr::Query(QueryBody::Select(s)) => {
            if q.locking.is_some() {
                return Err(ExecError::Unsupported(
                    "locking SELECT must use execute_read_locking".into(),
                ));
            }
            let mut s = (**s).clone();
            s.order_by = q.order_by.clone();
            s.limit = q.limit;
            s.offset = q.offset;
            s.locking = q.locking;
            crate::exec::select_to_relation_with_ctes(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                &s,
                &query_ctes,
                ctx,
                fctx,
                range_scanner,
            )
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let mut rel = crate::values::values_to_relation_with_ctes(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                v,
                &query_ctes,
                ctx,
                fctx,
                range_scanner,
            )?;
            let order_by = crate::subquery::resolve_order_items(&sub_ctx, &q.order_by)?;
            crate::values::apply_query_order(&mut rel, &order_by, q.offset, q.limit, ctx)?;
            Ok(rel)
        }
        SetExpr::Query(QueryBody::Nested(nested)) => {
            if q.locking.is_some() {
                return Err(ExecError::Unsupported(
                    "locking SELECT must use execute_read_locking".into(),
                ));
            }
            let mut rel = query_to_relation_with_ctes(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                nested,
                &query_ctes,
                ctx,
                fctx,
                range_scanner,
            )?;
            let order_by = crate::subquery::resolve_order_items(&sub_ctx, &q.order_by)?;
            crate::values::apply_query_order(&mut rel, &order_by, q.offset, q.limit, ctx)?;
            Ok(rel)
        }
        SetExpr::SetOp { .. } => {
            let order_by = crate::subquery::resolve_order_items(&sub_ctx, &q.order_by)?;
            crate::setops::set_expr_to_relation(
                catalog_kv,
                kv,
                global,
                gsnap,
                snapshot,
                own,
                &q.body,
                &order_by,
                q.offset,
                q.limit,
                &query_ctes,
                ctx,
                fctx,
                range_scanner,
            )
        }
    }
}

pub(crate) fn describe_query_expr(
    catalog_kv: &dyn Kv,
    q: &QueryExpr,
) -> Result<Vec<FieldDescription>, ExecError> {
    let ctes = crate::cte::CteContext::empty();
    describe_query_expr_inner(catalog_kv, q, &ctes, true)
}

pub(crate) fn describe_query_expr_with_ctes(
    catalog_kv: &dyn Kv,
    q: &QueryExpr,
    ctes: &crate::cte::CteContext,
) -> Result<Vec<FieldDescription>, ExecError> {
    describe_query_expr_inner(catalog_kv, q, ctes, false)
}

fn describe_query_expr_inner(
    catalog_kv: &dyn Kv,
    q: &QueryExpr,
    ctes: &crate::cte::CteContext,
    allow_locking: bool,
) -> Result<Vec<FieldDescription>, ExecError> {
    if !allow_locking && q.locking.is_some() {
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE is not supported in CTEs or derived tables".into(),
        ));
    }
    if allow_locking
        && q.locking.is_some()
        && let Some(with) = &q.with
    {
        crate::cte::reject_recursive(with)?;
        return Err(ExecError::Unsupported(
            "FOR UPDATE/SHARE with CTEs is not supported".into(),
        ));
    }
    let query_ctes = crate::cte::describe_with_clause(catalog_kv, q.with.as_ref(), ctes)?;
    match &q.body {
        SetExpr::Query(QueryBody::Select(s)) => {
            if !allow_locking {
                crate::exec::reject_nested_relation_locking(s)?;
            }
            let scope = if s.from.is_empty() {
                Scope::empty()
            } else {
                crate::exec::build_from_schema_with_ctes(catalog_kv, &s.from, &query_ctes)?.scope
            };
            let projection = crate::subquery::resolve_types_in_projection_with_ctes(
                catalog_kv,
                &s.projection,
                &query_ctes,
            )?;
            let (fields, _exprs, _tys) = crate::exec::resolve_projection(&projection, &scope)?;
            Ok(fields)
        }
        SetExpr::Query(QueryBody::Values(v)) => {
            let rel = crate::values::values_schema_relation_with_ctes(catalog_kv, v, &query_ctes)?;
            Ok(rel
                .scope
                .columns
                .iter()
                .map(|c| crate::exec::field(&c.name, c.ty))
                .collect())
        }
        SetExpr::Query(QueryBody::Nested(nested)) => {
            describe_query_expr_inner(catalog_kv, nested, &query_ctes, false)
        }
        SetExpr::SetOp { .. } => {
            crate::setops::describe_set_expr_with_ctes(catalog_kv, &q.body, &query_ctes)
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
    crate::exec::rows_result(fields, &rel.rows, &ctx.time_zone)
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
