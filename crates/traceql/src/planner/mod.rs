//! `TraceQL` planner entry points.

mod selector;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

use crate::ast::{Aggregate, ComparisonOp, Field, Pipeline, Query, SpansetExpr, StructuralOp};
use crate::error::{Result, TraceqlError};
use crate::span_columns::{COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_SPAN_ID, COL_TRACE_ID};
use crate::store::SpanStore;

pub(crate) struct PlannerContext {
    pub tenant: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

pub(crate) struct PlannedSpanset {
    pub ctx: SessionContext,
    pub plan: LogicalPlan,
}

pub(crate) async fn plan_query<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    q: &Query,
) -> Result<PlannedSpanset> {
    if !q.pipeline.is_empty() {
        return plan_spanset_sql(store, ctx, &q.root, &q.pipeline).await;
    }
    match &q.root {
        SpansetExpr::Selector(fe) => selector::plan_selector(store, ctx, fe).await,
        SpansetExpr::And(_, _) | SpansetExpr::Or(_, _) | SpansetExpr::Structural { .. } => {
            plan_spanset_sql(store, ctx, &q.root, &[]).await
        }
    }
}

async fn plan_spanset_sql<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    root: &SpansetExpr,
    pipeline: &[Pipeline],
) -> Result<PlannedSpanset> {
    let scan = store
        .scan(&ctx.tenant, &[], ctx.start_ns, ctx.end_ns)
        .await?;
    let spanset_sql = spanset_to_sql(root, &selector::ident(&scan.span_table))?;
    let sql = pipeline_to_sql(&spanset_sql, pipeline)?;
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
    })
}

fn pipeline_to_sql(spanset_sql: &str, pipeline: &[Pipeline]) -> Result<String> {
    match pipeline {
        []
        | [
            Pipeline::Aggregate(
                Aggregate::Count
                | Aggregate::Sum(_)
                | Aggregate::Avg(_)
                | Aggregate::Min(_)
                | Aggregate::Max(_),
            ),
        ] => Ok(format!("SELECT * FROM ({spanset_sql}) AS q")),
        [
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::Filter { op, value },
        ] => {
            let trace = selector::ident(COL_TRACE_ID);
            let pred = aggregate_filter_sql("COUNT(*)", *op, *value)?;
            Ok(format!(
                "WITH matched AS ({spanset_sql}), \
                 passing AS (SELECT {trace} FROM matched GROUP BY {trace} HAVING {pred}) \
                 SELECT matched.* FROM matched JOIN passing ON matched.{trace} = passing.{trace}"
            ))
        }
        [
            Pipeline::Aggregate(
                agg @ (Aggregate::Sum(_)
                | Aggregate::Avg(_)
                | Aggregate::Min(_)
                | Aggregate::Max(_)),
            ),
            Pipeline::Filter { op, value },
        ] => aggregate_filter_sql_query(spanset_sql, agg, *op, *value),
        [
            Pipeline::Aggregate(
                Aggregate::Sum(_) | Aggregate::Avg(_) | Aggregate::Min(_) | Aggregate::Max(_),
            ),
            Pipeline::By(by),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(
                Aggregate::Sum(_) | Aggregate::Avg(_) | Aggregate::Min(_) | Aggregate::Max(_),
            ),
        ] => grouped_aggregate_sql(spanset_sql, by, None),
        [Pipeline::Aggregate(Aggregate::Count), Pipeline::By(by)]
        | [Pipeline::By(by), Pipeline::Aggregate(Aggregate::Count)] => {
            grouped_aggregate_sql(spanset_sql, by, None)
        }
        [
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::Filter { op, value },
        ] => grouped_aggregate_sql(spanset_sql, by, Some(("COUNT(*)".to_string(), *op, *value))),
        [
            Pipeline::Aggregate(
                agg @ (Aggregate::Sum(_)
                | Aggregate::Avg(_)
                | Aggregate::Min(_)
                | Aggregate::Max(_)),
            ),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(
                agg @ (Aggregate::Sum(_)
                | Aggregate::Avg(_)
                | Aggregate::Min(_)
                | Aggregate::Max(_)),
            ),
            Pipeline::Filter { op, value },
        ] => grouped_aggregate_sql(
            spanset_sql,
            by,
            Some((aggregate_expr_sql(agg)?, *op, *value)),
        ),
        _ => Err(TraceqlError::Unsupported(format!(
            "pipeline shape {pipeline:?} is not implemented yet"
        ))),
    }
}

fn grouped_aggregate_sql(
    spanset_sql: &str,
    by: &[Field],
    filter: Option<(String, ComparisonOp, f64)>,
) -> Result<String> {
    let Some((expr, op, value)) = filter else {
        return Ok(format!("SELECT * FROM ({spanset_sql}) AS q"));
    };
    let group_cols = by
        .iter()
        .map(|field| selector::field_to_column(field).map(|col| selector::ident(&col)))
        .collect::<Result<Vec<_>>>()?;
    let group_exprs = group_cols.join(", ");
    let join_pred = group_cols
        .iter()
        .map(|col| format!("matched.{col} = passing.{col}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let pred = aggregate_filter_sql(&expr, op, value)?;
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         passing AS (SELECT {group_exprs} FROM matched GROUP BY {group_exprs} HAVING {pred}) \
         SELECT matched.* FROM matched JOIN passing ON {join_pred}"
    ))
}

fn aggregate_filter_sql_query(
    spanset_sql: &str,
    agg: &Aggregate,
    op: ComparisonOp,
    value: f64,
) -> Result<String> {
    let trace = selector::ident(COL_TRACE_ID);
    let expr = aggregate_expr_sql(agg)?;
    let pred = aggregate_filter_sql(&expr, op, value)?;
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         passing AS (SELECT {trace} FROM matched GROUP BY {trace} HAVING {pred}) \
         SELECT matched.* FROM matched JOIN passing ON matched.{trace} = passing.{trace}"
    ))
}

fn aggregate_expr_sql(agg: &Aggregate) -> Result<String> {
    let (func, field) = match agg {
        Aggregate::Sum(field) => ("SUM", field),
        Aggregate::Avg(field) => ("AVG", field),
        Aggregate::Min(field) => ("MIN", field),
        Aggregate::Max(field) => ("MAX", field),
        _ => {
            return Err(TraceqlError::Unsupported(format!(
                "aggregate {agg:?} is not supported in scalar filters"
            )));
        }
    };
    Ok(format!(
        "{func}({})",
        selector::ident(&selector::field_to_column(field)?)
    ))
}

fn aggregate_filter_sql(expr: &str, op: ComparisonOp, value: f64) -> Result<String> {
    if !value.is_finite() {
        return Err(TraceqlError::Plan(
            "pipeline filter value is not finite".into(),
        ));
    }
    let op = match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Neq => "!=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Re | ComparisonOp::Nre => {
            return Err(TraceqlError::Unsupported(
                "regex filter on pipeline scalar is not supported".into(),
            ));
        }
    };
    Ok(format!("{expr} {op} {value}"))
}

fn spanset_to_sql(expr: &SpansetExpr, table: &str) -> Result<String> {
    match expr {
        SpansetExpr::Selector(fe) => selector::selector_sql(table, fe),
        SpansetExpr::Or(lhs, rhs) => Ok(format!(
            "({}) UNION ({})",
            spanset_to_sql(lhs, table)?,
            spanset_to_sql(rhs, table)?
        )),
        SpansetExpr::And(lhs, rhs) => {
            let l = spanset_to_sql(lhs, table)?;
            let r = spanset_to_sql(rhs, table)?;
            let trace = selector::ident(COL_TRACE_ID);
            Ok(format!(
                "(SELECT l.* FROM ({l}) AS l WHERE EXISTS (SELECT 1 FROM ({r}) AS r WHERE r.{trace} = l.{trace})) \
                 UNION \
                 (SELECT r.* FROM ({r}) AS r WHERE EXISTS (SELECT 1 FROM ({l}) AS l WHERE l.{trace} = r.{trace}))"
            ))
        }
        SpansetExpr::Structural { op, lhs, rhs } => {
            let b = spanset_to_sql(lhs, table)?;
            let a = spanset_to_sql(rhs, table)?;
            let pred = structural_predicate_sql(structural_base_op(*op));
            if structural_is_negated(*op) {
                return Ok(format!(
                    "SELECT DISTINCT b.* FROM ({b}) AS b LEFT JOIN ({a}) AS a ON {pred} \
                     WHERE a.{} IS NULL",
                    selector::ident(COL_SPAN_ID)
                ));
            }
            if structural_is_union(*op) {
                return Ok(format!(
                    "(SELECT DISTINCT b.* FROM ({b}) AS b JOIN ({a}) AS a ON {pred}) \
                     UNION \
                     (SELECT DISTINCT a.* FROM ({b}) AS b JOIN ({a}) AS a ON {pred})"
                ));
            }
            Ok(format!(
                "SELECT DISTINCT b.* FROM ({b}) AS b JOIN ({a}) AS a ON {pred}"
            ))
        }
    }
}

fn structural_predicate_sql(op: StructuralOp) -> String {
    let trace = selector::ident(COL_TRACE_ID);
    let left = selector::ident(COL_NS_LEFT);
    let right = selector::ident(COL_NS_RIGHT);
    let parent = selector::ident(COL_PARENT_ID);
    let span_id = selector::ident(COL_SPAN_ID);
    let trace_eq = format!("b.{trace} = a.{trace}");
    match op {
        StructuralOp::Descendant => {
            format!("{trace_eq} AND b.{left} > a.{left} AND b.{right} < a.{right}")
        }
        StructuralOp::Ancestor => {
            format!("{trace_eq} AND b.{left} < a.{left} AND b.{right} > a.{right}")
        }
        StructuralOp::Child => format!("{trace_eq} AND b.{parent} = a.{left}"),
        StructuralOp::Parent => format!("{trace_eq} AND a.{parent} = b.{left}"),
        StructuralOp::Sibling => {
            format!("{trace_eq} AND b.{parent} = a.{parent} AND b.{span_id} != a.{span_id}")
        }
        StructuralOp::NegDescendant
        | StructuralOp::NegAncestor
        | StructuralOp::NegChild
        | StructuralOp::NegParent
        | StructuralOp::UnionDescendant
        | StructuralOp::UnionAncestor
        | StructuralOp::UnionChild
        | StructuralOp::UnionParent
        | StructuralOp::UnionSibling => unreachable!("mode variants are normalized first"),
    }
}

fn structural_base_op(op: StructuralOp) -> StructuralOp {
    match op {
        StructuralOp::NegDescendant | StructuralOp::UnionDescendant => StructuralOp::Descendant,
        StructuralOp::NegAncestor | StructuralOp::UnionAncestor => StructuralOp::Ancestor,
        StructuralOp::NegChild | StructuralOp::UnionChild => StructuralOp::Child,
        StructuralOp::NegParent | StructuralOp::UnionParent => StructuralOp::Parent,
        StructuralOp::UnionSibling => StructuralOp::Sibling,
        StructuralOp::Descendant
        | StructuralOp::Ancestor
        | StructuralOp::Child
        | StructuralOp::Parent
        | StructuralOp::Sibling => op,
    }
}

fn structural_is_negated(op: StructuralOp) -> bool {
    matches!(
        op,
        StructuralOp::NegDescendant
            | StructuralOp::NegAncestor
            | StructuralOp::NegChild
            | StructuralOp::NegParent
    )
}

fn structural_is_union(op: StructuralOp) -> bool {
    matches!(
        op,
        StructuralOp::UnionDescendant
            | StructuralOp::UnionAncestor
            | StructuralOp::UnionChild
            | StructuralOp::UnionParent
            | StructuralOp::UnionSibling
    )
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use datafusion::arrow::array::AsArray;

    use super::*;
    use crate::InMemorySpanStore;
    use crate::parser::parse;
    use crate::result::AttrValue;
    use crate::span_columns::{COL_NAME, InputSpan};

    fn span_with_parent(
        id: u8,
        parent: Option<u8>,
        trace_id: [u8; 16],
        name: &str,
        duration_nanos: i64,
        attrs: Vec<(&str, AttrValue)>,
    ) -> InputSpan {
        InputSpan {
            trace_id,
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: name.into(),
            kind: 0,
            start_unix_nano: i64::from(id),
            duration_nanos,
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        }
    }

    fn span(id: u8, name: &str, duration_nanos: i64, attrs: Vec<(&str, AttrValue)>) -> InputSpan {
        span_with_parent(id, None, [1; 16], name, duration_nanos, attrs)
    }

    async fn execute(planned: PlannedSpanset) -> Result<Vec<RecordBatch>> {
        Ok(planned
            .ctx
            .execute_logical_plan(planned.plan)
            .await?
            .collect()
            .await?)
    }

    async fn planned(query: &str, store: &InMemorySpanStore) -> Result<Vec<RecordBatch>> {
        let q = parse(query)?;
        execute(
            plan_query(
                store,
                &PlannerContext {
                    tenant: "t".into(),
                    start_ns: 0,
                    end_ns: 10_000,
                },
                &q,
            )
            .await?,
        )
        .await
    }

    fn first_name(batches: &[RecordBatch]) -> String {
        batches[0]
            .column_by_name(COL_NAME)
            .unwrap()
            .as_string::<i32>()
            .value(0)
            .to_string()
    }

    fn names(batches: &[RecordBatch]) -> Vec<String> {
        let mut out = Vec::new();
        for batch in batches {
            let arr = batch.column_by_name(COL_NAME).unwrap().as_string::<i32>();
            for i in 0..arr.len() {
                out.push(arr.value(i).to_string());
            }
        }
        out.sort_unstable();
        out
    }

    fn span_ids(batches: &[RecordBatch]) -> Vec<[u8; 8]> {
        let mut out = Vec::new();
        for batch in batches {
            let arr = batch
                .column_by_name(crate::COL_SPAN_ID)
                .unwrap()
                .as_fixed_size_binary();
            for i in 0..arr.len() {
                out.push(arr.value(i).try_into().unwrap());
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    #[tokio::test]
    async fn selector_matches_attribute_value() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(
                    1,
                    "root",
                    50,
                    vec![("http.method", AttrValue::Str("GET".into()))],
                ),
                span(
                    2,
                    "db",
                    50,
                    vec![("http.method", AttrValue::Str("POST".into()))],
                ),
            ],
        );
        let out = planned("{ .http.method = \"GET\" }", &store).await.unwrap();
        assert!(out.iter().map(RecordBatch::num_rows).sum::<usize>() == 1);
        assert!(first_name(&out) == "root");
    }

    #[tokio::test]
    async fn selector_matches_intrinsic_duration() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![span(1, "short", 50, vec![]), span(2, "long", 150, vec![])],
        );
        let out = planned("{ span:duration > 100 }", &store).await.unwrap();
        assert!(out.iter().map(RecordBatch::num_rows).sum::<usize>() == 1);
        assert!(first_name(&out) == "long");
    }

    #[tokio::test]
    async fn intra_brace_and_matches_one_span() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "a-only", 50, vec![("a", AttrValue::Int(1))]),
                span(2, "b-only", 50, vec![("b", AttrValue::Int(2))]),
                span(
                    3,
                    "both",
                    50,
                    vec![("a", AttrValue::Int(1)), ("b", AttrValue::Int(2))],
                ),
            ],
        );
        let out = planned("{ .a = 1 && .b = 2 }", &store).await.unwrap();
        assert!(out.iter().map(RecordBatch::num_rows).sum::<usize>() == 1);
        assert!(first_name(&out) == "both");
    }

    #[tokio::test]
    async fn regex_is_fully_anchored() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "one", 50, vec![("name", AttrValue::Str("abc".into()))]),
                span(2, "two", 50, vec![("name", AttrValue::Str("xabc".into()))]),
            ],
        );
        let out = planned("{ .name =~ \"ab.*\" }", &store).await.unwrap();
        assert!(out.iter().map(RecordBatch::num_rows).sum::<usize>() == 1);
        assert!(first_name(&out) == "one");
    }

    #[tokio::test]
    async fn inter_brace_and_matches_different_spans_same_trace() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span_with_parent(
                    1,
                    None,
                    [1; 16],
                    "a-only",
                    50,
                    vec![("a", AttrValue::Int(1))],
                ),
                span_with_parent(
                    2,
                    None,
                    [1; 16],
                    "b-only",
                    50,
                    vec![("b", AttrValue::Int(2))],
                ),
            ],
        );
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![span_with_parent(
                3,
                None,
                [2; 16],
                "other-a",
                50,
                vec![("a", AttrValue::Int(1))],
            )],
        );

        let out = planned("{ .a = 1 } && { .b = 2 }", &store).await.unwrap();
        assert!(names(&out) == vec!["a-only".to_string(), "b-only".to_string()]);
    }

    fn structural_store() -> InMemorySpanStore {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span_with_parent(
                    1,
                    None,
                    [9; 16],
                    "root",
                    1,
                    vec![("svc", AttrValue::Str("a".into()))],
                ),
                span_with_parent(
                    2,
                    Some(1),
                    [9; 16],
                    "child-x",
                    1,
                    vec![("svc", AttrValue::Str("b".into()))],
                ),
                span_with_parent(
                    4,
                    Some(2),
                    [9; 16],
                    "grand-y",
                    1,
                    vec![("svc", AttrValue::Str("c".into()))],
                ),
                span_with_parent(
                    3,
                    Some(1),
                    [9; 16],
                    "child-z",
                    1,
                    vec![("svc", AttrValue::Str("b".into()))],
                ),
            ],
        );
        store.push_trace(
            "t",
            "svc",
            "other-root",
            vec![
                span_with_parent(
                    5,
                    None,
                    [8; 16],
                    "other-root",
                    1,
                    vec![("svc", AttrValue::Str("a".into()))],
                ),
                span_with_parent(
                    6,
                    Some(5),
                    [8; 16],
                    "other-child",
                    1,
                    vec![("svc", AttrValue::Str("d".into()))],
                ),
            ],
        );
        store
    }

    #[tokio::test]
    async fn structural_descendant_returns_lhs_descendant_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } >> { .svc = \"a\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[4; 8]]);
    }

    #[tokio::test]
    async fn structural_child_uses_parent_id_eq_anchor_left() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } > { .svc = \"a\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn structural_sibling_excludes_self() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } ~ { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn structural_ancestor_returns_lhs_ancestor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } << { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[1; 8]]);
    }

    #[tokio::test]
    async fn structural_parent_returns_direct_parent_only() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } < { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8]]);
    }

    #[tokio::test]
    async fn structural_join_is_trace_isolated() {
        let store = structural_store();
        let out = planned("{ .svc = \"d\" } >> { .svc = \"a\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[6; 8]]);
    }

    #[tokio::test]
    async fn negated_ancestor_returns_lhs_spans_without_anchor_match() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } !<< { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[3; 8]]);
    }

    #[tokio::test]
    async fn negated_parent_uses_parent_id_anti_join() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } !< { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[3; 8]]);
    }

    #[tokio::test]
    async fn union_descendant_returns_lhs_and_anchor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } &>> { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [4; 8]]);
    }

    #[tokio::test]
    async fn union_sibling_deduplicates_spans_matching_both_sides() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } &~ { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn count_by_filter_keeps_spans_from_passing_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 1, vec![("svc", AttrValue::Str("db".into()))]),
            ],
        );
        let out = planned("{ .svc != nil } | count() | by(span.svc) > 1", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn avg_filter_keeps_spans_from_passing_traces() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span_with_parent(
                    1,
                    None,
                    [1; 16],
                    "fast-a",
                    20,
                    vec![("svc", AttrValue::Str("api".into()))],
                ),
                span_with_parent(
                    2,
                    None,
                    [1; 16],
                    "fast-b",
                    40,
                    vec![("svc", AttrValue::Str("api".into()))],
                ),
            ],
        );
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span_with_parent(
                    3,
                    None,
                    [2; 16],
                    "slow-a",
                    200,
                    vec![("svc", AttrValue::Str("api".into()))],
                ),
                span_with_parent(
                    4,
                    None,
                    [2; 16],
                    "slow-b",
                    400,
                    vec![("svc", AttrValue::Str("api".into()))],
                ),
            ],
        );

        let out = planned("{ .svc = \"api\" } | avg(span:duration) > 100", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["slow-a".to_string(), "slow-b".to_string()]);
    }

    #[tokio::test]
    async fn avg_without_filter_preserves_matched_spans() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 40, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 200, vec![("svc", AttrValue::Str("db".into()))]),
            ],
        );

        let out = planned("{ .svc = \"api\" } | avg(span:duration)", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn avg_by_filter_keeps_spans_from_passing_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 40, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 200, vec![("svc", AttrValue::Str("db".into()))]),
                span(4, "db-b", 400, vec![("svc", AttrValue::Str("db".into()))]),
            ],
        );

        let out = planned(
            "{ .svc != nil } | avg(span:duration) | by(span.svc) > 100",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["db-a".to_string(), "db-b".to_string()]);
    }
}
