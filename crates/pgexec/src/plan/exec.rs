//! The first executable planner node.
//!
//! P0a moves read shapes here one node at a time.  A FROM-less scalar SELECT is
//! already a real `Result` node: it has no scan to delegate and so is the safe
//! first cut-over from the legacy read path.

use std::collections::BTreeSet;

use crabka_pgparser::ast::{DistinctClause, SelectStmt};

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
    let target_list = exprs
        .iter()
        .zip(&fields)
        .enumerate()
        .map(|(index, (expr, field))| {
            Ok(TargetEntry {
                expr: BoundExpr::new(expr, &scope)?,
                resno: index + 1,
                resname: field.name.clone(),
            })
        })
        .collect::<Result<_, ExecError>>()?;
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

#[cfg(test)]
mod tests {
    use crabka_pgparser::ast::{GroupingClause, QueryBody, SetExpr, Statement};

    use super::*;

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
}
