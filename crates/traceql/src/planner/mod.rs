//! `TraceQL` planner entry points.

mod selector;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

use crate::ast::{
    Aggregate, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query, Scope, SpansetExpr,
    StructuralOp,
};
use crate::error::{Result, TraceqlError};
use crate::span_columns::{COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_SPAN_ID, COL_TRACE_ID};
use crate::store::{MatchCmp, MatchScope, MatchValue, ScanOptions, SpanMatcher, SpanStore};

pub(crate) struct PlannerContext {
    pub tenant: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub scan_options: ScanOptions,
}

pub(crate) struct PlannedSpanset {
    pub ctx: SessionContext,
    pub plan: LogicalPlan,
    /// Bytes inspected by the primary span scan (threaded to
    /// `SearchResponse::inspected_bytes`). Nested structural-join tables re-scan
    /// the same blocks, so only the primary scan is counted.
    pub inspected_bytes: u64,
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
    let scan_options = scan_options_with_pipeline_projections(&ctx.scan_options, pipeline);
    let scan = store
        .scan_with_options(&ctx.tenant, &[], ctx.start_ns, ctx.end_ns, &scan_options)
        .await?;
    let inspected_bytes = scan.inspected_bytes;
    let nested_tables = register_nested_selector_tables(store, ctx, &scan.ctx, root).await?;
    let spanset_sql = spanset_to_sql(root, &selector::ident(&scan.span_table), &nested_tables)?;
    let sql = pipeline_to_sql(&spanset_sql, pipeline)?;
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
        inspected_bytes,
    })
}

fn scan_options_with_pipeline_projections(
    options: &ScanOptions,
    pipeline: &[Pipeline],
) -> ScanOptions {
    let mut options = options.clone();
    for matcher in pipeline_nested_projection_matchers(pipeline) {
        if !options.projection_matchers.contains(&matcher) {
            options.projection_matchers.push(matcher);
        }
    }
    options
}

fn pipeline_nested_projection_matchers(pipeline: &[Pipeline]) -> Vec<SpanMatcher> {
    let mut out = Vec::new();
    for stage in pipeline {
        match stage {
            Pipeline::By(fields) | Pipeline::Select(fields) => {
                for field in fields {
                    push_nested_projection_matcher(&mut out, field);
                }
            }
            Pipeline::Aggregate(agg) => {
                if let Some(field) = aggregate_projection_field(agg) {
                    push_nested_projection_matcher(&mut out, field);
                }
            }
            Pipeline::Filter { .. }
            | Pipeline::TopK(_)
            | Pipeline::BottomK(_)
            | Pipeline::Compare { .. }
            | Pipeline::Coalesce
            | Pipeline::With(_) => {}
        }
    }
    out
}

fn aggregate_projection_field(agg: &Aggregate) -> Option<&Field> {
    match agg {
        Aggregate::Sum(field)
        | Aggregate::Avg(field)
        | Aggregate::Min(field)
        | Aggregate::Max(field)
        | Aggregate::SumOverTime(field)
        | Aggregate::AvgOverTime(field)
        | Aggregate::MinOverTime(field)
        | Aggregate::MaxOverTime(field)
        | Aggregate::HistogramOverTime(field)
        | Aggregate::QuantileOverTime { field, .. } => Some(field),
        Aggregate::Count | Aggregate::Rate | Aggregate::CountOverTime => None,
    }
}

fn push_nested_projection_matcher(out: &mut Vec<SpanMatcher>, field: &Field) {
    let Some(matcher) = nested_projection_matcher(field) else {
        return;
    };
    if !out.contains(&matcher) {
        out.push(matcher);
    }
}

fn nested_projection_matcher(field: &Field) -> Option<SpanMatcher> {
    let (scope, key) = match &field.scope {
        Scope::Event => (MatchScope::Event, field.key.clone()),
        Scope::Link => (MatchScope::Link, field.key.clone()),
        Scope::Intrinsic(Intrinsic::EventName) => (MatchScope::Intrinsic, "event:name".into()),
        Scope::Intrinsic(Intrinsic::EventTimeSinceStart) => {
            (MatchScope::Intrinsic, "event:timeSinceStart".into())
        }
        Scope::Intrinsic(Intrinsic::LinkTraceId) => (MatchScope::Intrinsic, "link:traceID".into()),
        Scope::Intrinsic(Intrinsic::LinkSpanId) => (MatchScope::Intrinsic, "link:spanID".into()),
        // A by()/select field on a regular span or resource attribute must be
        // projected too: grouping reads it as a column (`GROUP BY attr.X`), but
        // the scan otherwise materializes attrs only from the selector's filter
        // matchers, so `rate() by(span.http.method)` fails with "missing column
        // attr.http.method". This is projection-only — projection_matchers do not
        // filter (the scan filters on the attr arrays separately), so spans
        // lacking the attribute still appear under the nil group.
        Scope::Both => (MatchScope::Both, field.key.clone()),
        Scope::Span => (MatchScope::Span, field.key.clone()),
        Scope::Resource => (MatchScope::Resource, field.key.clone()),
        Scope::Parent | Scope::Instrumentation | Scope::Intrinsic(_) => return None,
    };
    Some(SpanMatcher {
        scope,
        key,
        op: MatchCmp::Neq,
        value: MatchValue::Nil,
        negated: false,
    })
}

async fn register_nested_selector_tables<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    target_ctx: &SessionContext,
    root: &SpansetExpr,
) -> Result<Vec<(FieldExpr, String)>> {
    let mut selectors = Vec::new();
    collect_nested_selectors(root, &mut selectors);

    let mut tables = Vec::new();
    for (idx, selector) in selectors.into_iter().enumerate() {
        let table_name = format!("nested_selector_{idx}");
        let scan = store
            .scan_with_options(
                &ctx.tenant,
                &selector::field_expr_to_matchers(&selector),
                ctx.start_ns,
                ctx.end_ns,
                &ctx.scan_options,
            )
            .await?;
        let batches = collect_table(&scan.ctx, &scan.span_table).await?;
        register_batches(target_ctx, &table_name, batches)?;
        tables.push((selector, table_name));
    }
    Ok(tables)
}

fn collect_nested_selectors(expr: &SpansetExpr, out: &mut Vec<FieldExpr>) {
    match expr {
        SpansetExpr::Selector(fe) if selector::has_nested_scope(fe) => {
            if !out.iter().any(|existing| existing == fe.as_ref()) {
                out.push((**fe).clone());
            }
        }
        SpansetExpr::Selector(_) => {}
        SpansetExpr::And(lhs, rhs)
        | SpansetExpr::Or(lhs, rhs)
        | SpansetExpr::Structural { lhs, rhs, .. } => {
            collect_nested_selectors(lhs, out);
            collect_nested_selectors(rhs, out);
        }
    }
}

async fn collect_table(ctx: &SessionContext, table: &str) -> Result<Vec<RecordBatch>> {
    Ok(ctx.table(table).await?.collect().await?)
}

fn register_batches(
    ctx: &SessionContext,
    table_name: &str,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let schema = batches
        .first()
        .map_or_else(crate::span_columns::span_schema, RecordBatch::schema);
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(())
}

fn pipeline_to_sql(spanset_sql: &str, pipeline: &[Pipeline]) -> Result<String> {
    if pipeline.iter().all(is_search_preserving_pipeline_stage) {
        return Ok(format!("SELECT * FROM ({spanset_sql}) AS q"));
    }

    let normalized_pipeline;
    let pipeline = if pipeline.iter().any(is_inert_pipeline_stage) {
        normalized_pipeline = pipeline
            .iter()
            .filter(|stage| !is_inert_pipeline_stage(stage))
            .cloned()
            .collect::<Vec<_>>();
        normalized_pipeline.as_slice()
    } else {
        pipeline
    };

    match pipeline {
        [] => Ok(format!("SELECT * FROM ({spanset_sql}) AS q")),
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
        _ => grouped_pipeline_sql(spanset_sql, pipeline),
    }
}

fn is_search_preserving_pipeline_stage(stage: &Pipeline) -> bool {
    matches!(
        stage,
        Pipeline::By(_)
            | Pipeline::Select(_)
            | Pipeline::Coalesce
            | Pipeline::With(_)
            | Pipeline::Aggregate(
                Aggregate::Count
                    | Aggregate::Sum(_)
                    | Aggregate::Avg(_)
                    | Aggregate::Min(_)
                    | Aggregate::Max(_)
            )
    )
}

fn is_inert_pipeline_stage(stage: &Pipeline) -> bool {
    matches!(
        stage,
        Pipeline::Select(_) | Pipeline::Coalesce | Pipeline::With(_)
    )
}

fn grouped_pipeline_sql(spanset_sql: &str, pipeline: &[Pipeline]) -> Result<String> {
    if let Some(by) = grouped_no_filter_by(pipeline) {
        return grouped_aggregate_sql(spanset_sql, by, None);
    }
    if let Some(sql) = grouped_rank_pipeline_sql(spanset_sql, pipeline)? {
        return Ok(sql);
    }
    if let Some(sql) = ungrouped_rank_pipeline_sql(spanset_sql, pipeline)? {
        return Ok(sql);
    }

    match pipeline {
        [
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::Filter { op, value },
            Pipeline::By(by),
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
            Pipeline::Aggregate(
                agg @ (Aggregate::Sum(_)
                | Aggregate::Avg(_)
                | Aggregate::Min(_)
                | Aggregate::Max(_)),
            ),
            Pipeline::Filter { op, value },
            Pipeline::By(by),
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

fn grouped_rank_pipeline_sql(spanset_sql: &str, pipeline: &[Pipeline]) -> Result<Option<String>> {
    let Some((agg, by, rank, pre_filter, post_filter)) = grouped_rank_pipeline_parts(pipeline)
    else {
        return Ok(None);
    };
    if !is_search_preserving_aggregate(agg) {
        return Ok(None);
    }
    Ok(Some(grouped_rank_sql(
        spanset_sql,
        by,
        &aggregate_rank_expr_sql(agg)?,
        rank_limit(rank)?,
        pre_filter,
        post_filter,
    )?))
}

type RankFilter = Option<(ComparisonOp, f64)>;
type UngroupedRankParts<'a> = (&'a Aggregate, &'a Pipeline, RankFilter);

fn grouped_rank_pipeline_parts(
    pipeline: &[Pipeline],
) -> Option<(&Aggregate, &[Field], &Pipeline, RankFilter, RankFilter)> {
    match pipeline {
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::By(by),
        ] => Some((agg, by, rank, None, None)),
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::By(by),
        ] => Some((agg, by, rank, Some((*op, *value)), None)),
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::Filter { op, value },
            Pipeline::By(by),
        ] => Some((agg, by, rank, None, Some((*op, *value)))),
        _ => None,
    }
}

fn ungrouped_rank_pipeline_sql(spanset_sql: &str, pipeline: &[Pipeline]) -> Result<Option<String>> {
    let Some((agg, rank, filter)) = ungrouped_rank_pipeline_parts(pipeline) else {
        return Ok(None);
    };
    if !is_search_preserving_aggregate(agg) {
        return Ok(None);
    }
    let rank = rank_limit(rank)?;
    if rank.k == 0 {
        return Ok(Some(ungrouped_rank_sql(spanset_sql, rank)));
    }
    let Some((op, value)) = filter else {
        return Ok(Some(ungrouped_rank_sql(spanset_sql, rank)));
    };
    Ok(Some(aggregate_filter_sql_query_any(
        spanset_sql,
        agg,
        op,
        value,
    )?))
}

fn ungrouped_rank_pipeline_parts(pipeline: &[Pipeline]) -> Option<UngroupedRankParts<'_>> {
    match pipeline {
        [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ] => Some((agg, rank, None)),
        [
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
        ]
        | [
            Pipeline::Aggregate(agg),
            rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_)),
            Pipeline::Filter { op, value },
        ] => Some((agg, rank, Some((*op, *value)))),
        _ => None,
    }
}

fn grouped_no_filter_by(pipeline: &[Pipeline]) -> Option<&[Field]> {
    match pipeline {
        [Pipeline::Aggregate(agg), Pipeline::By(by)]
        | [Pipeline::By(by), Pipeline::Aggregate(agg)]
        | [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            Pipeline::Coalesce,
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            Pipeline::Coalesce,
        ] if is_search_preserving_aggregate(agg) => Some(by),
        _ => None,
    }
}

fn is_search_preserving_aggregate(agg: &Aggregate) -> bool {
    matches!(
        agg,
        Aggregate::Count
            | Aggregate::Sum(_)
            | Aggregate::Avg(_)
            | Aggregate::Min(_)
            | Aggregate::Max(_)
    )
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
        .map(|field| selector::ident(&selector::field_to_column(field)))
        .collect::<Vec<_>>();
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

fn grouped_rank_sql(
    spanset_sql: &str,
    by: &[Field],
    expr: &str,
    rank: RankLimit,
    pre_filter: Option<(ComparisonOp, f64)>,
    post_filter: Option<(ComparisonOp, f64)>,
) -> Result<String> {
    let group_cols = by
        .iter()
        .map(|field| selector::ident(&selector::field_to_column(field)))
        .collect::<Vec<_>>();
    let group_exprs = group_cols.join(", ");
    let join_pred = group_cols
        .iter()
        .map(|col| format!("matched.{col} = passing.{col}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let direction = match rank.direction {
        RankDirection::Top => "DESC",
        RankDirection::Bottom => "ASC",
    };
    let having = if let Some((op, value)) = pre_filter {
        format!(" HAVING {}", aggregate_filter_sql(expr, op, value)?)
    } else {
        String::new()
    };
    let passing_source = if let Some((op, value)) = post_filter {
        let pred = aggregate_filter_sql("rank_value", op, value)?;
        format!("SELECT * FROM ranked WHERE {pred}")
    } else {
        "SELECT * FROM ranked".to_string()
    };
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         ranked AS (SELECT {group_exprs}, {expr} AS rank_value FROM matched GROUP BY {group_exprs} \
                    {having} ORDER BY rank_value {direction} LIMIT {}), \
         passing AS ({passing_source}) \
         SELECT matched.* FROM matched JOIN passing ON {join_pred}",
        rank.k
    ))
}

fn ungrouped_rank_sql(spanset_sql: &str, rank: RankLimit) -> String {
    if rank.k == 0 {
        return format!("SELECT * FROM ({spanset_sql}) AS q WHERE FALSE");
    }
    format!("SELECT * FROM ({spanset_sql}) AS q")
}

fn aggregate_filter_sql_query(
    spanset_sql: &str,
    agg: &Aggregate,
    op: ComparisonOp,
    value: f64,
) -> Result<String> {
    aggregate_filter_sql_query_any(spanset_sql, agg, op, value)
}

fn aggregate_filter_sql_query_any(
    spanset_sql: &str,
    agg: &Aggregate,
    op: ComparisonOp,
    value: f64,
) -> Result<String> {
    let trace = selector::ident(COL_TRACE_ID);
    let expr = match agg {
        Aggregate::Count => "COUNT(*)".to_string(),
        _ => aggregate_expr_sql(agg)?,
    };
    let pred = aggregate_filter_sql(&expr, op, value)?;
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         passing AS (SELECT {trace} FROM matched GROUP BY {trace} HAVING {pred}) \
         SELECT matched.* FROM matched JOIN passing ON matched.{trace} = passing.{trace}"
    ))
}

#[derive(Clone, Copy)]
enum RankDirection {
    Top,
    Bottom,
}

#[derive(Clone, Copy)]
struct RankLimit {
    direction: RankDirection,
    k: usize,
}

fn rank_limit(pipeline: &Pipeline) -> Result<RankLimit> {
    match pipeline {
        Pipeline::TopK(k) => Ok(RankLimit {
            direction: RankDirection::Top,
            k: *k,
        }),
        Pipeline::BottomK(k) => Ok(RankLimit {
            direction: RankDirection::Bottom,
            k: *k,
        }),
        other => Err(TraceqlError::Unsupported(format!(
            "expected topk/bottomk, got {other:?}"
        ))),
    }
}

fn aggregate_rank_expr_sql(agg: &Aggregate) -> Result<String> {
    match agg {
        Aggregate::Count => Ok("COUNT(*)".to_string()),
        Aggregate::Sum(_) | Aggregate::Avg(_) | Aggregate::Min(_) | Aggregate::Max(_) => {
            aggregate_expr_sql(agg)
        }
        _ => Err(TraceqlError::Unsupported(format!(
            "aggregate {agg:?} is not supported in search ranking"
        ))),
    }
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
        selector::ident(&selector::field_to_column(field))
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

fn spanset_to_sql(
    expr: &SpansetExpr,
    table: &str,
    nested_tables: &[(FieldExpr, String)],
) -> Result<String> {
    match expr {
        SpansetExpr::Selector(fe) if selector::has_nested_scope(fe) => {
            let Some((_, table_name)) = nested_tables
                .iter()
                .find(|(candidate, _)| candidate == fe.as_ref())
            else {
                return Err(TraceqlError::Plan(
                    "nested selector table was not registered".into(),
                ));
            };
            let nested_table = selector::ident(table_name);
            if selector::has_parent_scope(fe) {
                selector::selector_sql_with_parent_table(&nested_table, table, fe)
            } else {
                Ok(format!("SELECT * FROM {nested_table}"))
            }
        }
        SpansetExpr::Selector(fe) => selector::selector_sql(table, fe),
        SpansetExpr::Or(lhs, rhs) => Ok(format!(
            "({}) UNION ({})",
            spanset_to_sql(lhs, table, nested_tables)?,
            spanset_to_sql(rhs, table, nested_tables)?
        )),
        SpansetExpr::And(lhs, rhs) => {
            let l = spanset_to_sql(lhs, table, nested_tables)?;
            let r = spanset_to_sql(rhs, table, nested_tables)?;
            let trace = selector::ident(COL_TRACE_ID);
            Ok(format!(
                "(SELECT l.* FROM ({l}) AS l WHERE EXISTS (SELECT 1 FROM ({r}) AS r WHERE r.{trace} = l.{trace})) \
                 UNION \
                 (SELECT r.* FROM ({r}) AS r WHERE EXISTS (SELECT 1 FROM ({l}) AS l WHERE l.{trace} = r.{trace}))"
            ))
        }
        SpansetExpr::Structural { op, lhs, rhs } => {
            let b = spanset_to_sql(rhs, table, nested_tables)?;
            let a = spanset_to_sql(lhs, table, nested_tables)?;
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
    use crate::result::{AttrValue, EventRef};
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
            events: Vec::new(),
            links: Vec::new(),
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
                    scan_options: ScanOptions::default(),
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
    async fn grouped_pipeline_filters_by_nested_event_intrinsic() {
        let mut miss_one = span(1, "miss-one", 50, vec![]);
        miss_one.events = vec![EventRef {
            time_since_start_nano: 10,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut miss_two = span(2, "miss-two", 50, vec![]);
        miss_two.events = vec![EventRef {
            time_since_start_nano: 20,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut hit = span(3, "hit", 50, vec![]);
        hit.events = vec![EventRef {
            time_since_start_nano: 30,
            name: "cache.hit".into(),
            attributes: Vec::new(),
        }];
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "svc", "root", vec![miss_one, miss_two, hit]);

        let out = planned(
            "{ event:name != nil } | count() by (event:name) > 1",
            &store,
        )
        .await
        .unwrap();

        assert!(names(&out) == vec!["miss-one".to_string(), "miss-two".to_string()]);
    }

    #[tokio::test]
    async fn grouped_pipeline_by_nested_event_intrinsic_counts_all_events_without_nested_selector()
    {
        let mut one = span(1, "one", 50, vec![("svc", AttrValue::Str("api".into()))]);
        one.events = vec![
            EventRef {
                time_since_start_nano: 10,
                name: "cache.miss".into(),
                attributes: Vec::new(),
            },
            EventRef {
                time_since_start_nano: 20,
                name: "cache.hit".into(),
                attributes: Vec::new(),
            },
        ];
        let mut two = span(2, "two", 50, vec![("svc", AttrValue::Str("api".into()))]);
        two.events = vec![
            EventRef {
                time_since_start_nano: 30,
                name: "cache.wait".into(),
                attributes: Vec::new(),
            },
            EventRef {
                time_since_start_nano: 40,
                name: "cache.hit".into(),
                attributes: Vec::new(),
            },
        ];
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "svc", "root", vec![one, two]);

        let out = planned("{ .svc = \"api\" } | count() by (event:name) > 1", &store)
            .await
            .unwrap();

        assert!(names(&out) == vec!["one".to_string(), "two".to_string()]);
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
    async fn structural_descendant_returns_rhs_descendant_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } >> { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[4; 8]]);
    }

    #[tokio::test]
    async fn structural_child_returns_rhs_direct_children() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } > { .svc = \"b\" }", &store)
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
    async fn structural_ancestor_returns_rhs_ancestor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } << { .svc = \"a\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[1; 8]]);
    }

    #[tokio::test]
    async fn structural_parent_returns_direct_parent_only() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } < { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8]]);
    }

    #[tokio::test]
    async fn structural_join_is_trace_isolated() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } >> { .svc = \"d\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[6; 8]]);
    }

    #[tokio::test]
    async fn negated_ancestor_returns_rhs_spans_without_anchor_match() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } !<< { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[3; 8]]);
    }

    #[tokio::test]
    async fn negated_descendant_returns_rhs_spans_without_descendant_match() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } !>> { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn negated_child_returns_rhs_spans_without_direct_child_match() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } !> { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[4; 8]]);
    }

    #[tokio::test]
    async fn negated_parent_uses_parent_id_anti_join() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } !< { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[3; 8]]);
    }

    #[tokio::test]
    async fn union_descendant_returns_rhs_and_anchor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } &>> { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [4; 8]]);
    }

    #[tokio::test]
    async fn union_ancestor_returns_rhs_and_anchor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } &<< { .svc = \"a\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[1; 8], [4; 8]]);
    }

    #[tokio::test]
    async fn union_child_returns_rhs_and_anchor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } &> { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[1; 8], [2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn union_parent_returns_rhs_and_anchor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } &< { .svc = \"b\" }", &store)
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
    async fn count_filter_accepts_literal_arithmetic_threshold() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "api-c", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(4, "db-a", 1, vec![("svc", AttrValue::Str("db".into()))]),
            ],
        );

        let out = planned("{ .svc != nil } | count() | by(span.svc) > 1 + 1", &store)
            .await
            .unwrap();

        assert!(
            names(&out)
                == vec![
                    "api-a".to_string(),
                    "api-b".to_string(),
                    "api-c".to_string()
                ]
        );
    }

    #[tokio::test]
    async fn count_filter_then_by_preserves_spans_from_passing_traces() {
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
                    "api-a",
                    1,
                    vec![("svc", AttrValue::Str("api".into()))],
                ),
                span_with_parent(
                    2,
                    None,
                    [1; 16],
                    "api-b",
                    1,
                    vec![("svc", AttrValue::Str("api".into()))],
                ),
                span_with_parent(
                    3,
                    None,
                    [2; 16],
                    "db-a",
                    1,
                    vec![("svc", AttrValue::Str("db".into()))],
                ),
            ],
        );

        let out = planned("{ .svc != nil } | count() > 1 | by(span.svc)", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn preserving_stage_before_count_filter_is_ignored_for_search() {
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

        let out = planned(
            "{ .svc = \"api\" } | select(span:duration, span.svc) | count() > 1",
            &store,
        )
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
    async fn avg_filter_then_by_preserves_spans_from_passing_traces() {
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

        let out = planned(
            "{ .svc = \"api\" } | avg(span:duration) > 100 | by(span.svc)",
            &store,
        )
        .await
        .unwrap();
        assert!(
            names(&out)
                == vec![
                    "fast-a".to_string(),
                    "fast-b".to_string(),
                    "slow-a".to_string(),
                    "slow-b".to_string()
                ]
        );
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
    async fn select_preserves_matched_spans_for_search_projection() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "db-a", 40, vec![("svc", AttrValue::Str("db".into()))]),
            ],
        );

        let out = planned(
            "{ .svc = \"api\" } | select(span:duration, span.svc)",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["api-a".to_string()]);
    }

    #[tokio::test]
    async fn select_coalesce_preserves_matched_spans_for_search() {
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

        let out = planned(
            "{ .svc = \"api\" } | select(span:duration, span.svc) | coalesce()",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn by_coalesce_preserves_matched_spans_for_search() {
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

        let out = planned("{ .svc != nil } | by(span.svc) | coalesce()", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]);
    }

    #[tokio::test]
    async fn by_without_aggregate_preserves_matched_spans_for_search() {
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

        let out = planned("{ .svc != nil } | by(span.svc)", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]);
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

    #[tokio::test]
    async fn avg_by_coalesce_preserves_matched_spans_for_search() {
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

        let out = planned(
            "{ .svc != nil } | avg(span:duration) | by(span.svc) | coalesce()",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]);
    }

    #[tokio::test]
    async fn count_by_topk_and_bottomk_keep_spans_from_ranked_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 40, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 200, vec![("svc", AttrValue::Str("db".into()))]),
                span(
                    4,
                    "cache-a",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    5,
                    "cache-b",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    6,
                    "cache-c",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
            ],
        );

        let top = planned("{ .svc != nil } | count() | by(span.svc) | topk(1)", &store)
            .await
            .unwrap();
        assert!(
            names(&top)
                == vec![
                    "cache-a".to_string(),
                    "cache-b".to_string(),
                    "cache-c".to_string()
                ]
        );

        let bottom = planned(
            "{ .svc != nil } | count() | by(span.svc) | bottomk(1)",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&bottom) == vec!["db-a".to_string()]);
    }

    #[tokio::test]
    async fn count_by_topk_filter_keeps_spans_from_ranked_passing_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 40, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 200, vec![("svc", AttrValue::Str("db".into()))]),
                span(
                    4,
                    "cache-a",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    5,
                    "cache-b",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    6,
                    "cache-c",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
            ],
        );

        let out = planned(
            "{ .svc != nil } | count() | by(span.svc) | topk(2) > 2",
            &store,
        )
        .await
        .unwrap();
        assert!(
            names(&out)
                == vec![
                    "cache-a".to_string(),
                    "cache-b".to_string(),
                    "cache-c".to_string()
                ]
        );
    }

    #[tokio::test]
    async fn count_by_filter_topk_ranks_spans_from_passing_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 40, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 200, vec![("svc", AttrValue::Str("db".into()))]),
                span(
                    4,
                    "cache-a",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    5,
                    "cache-b",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    6,
                    "cache-c",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
            ],
        );

        let out = planned(
            "{ .svc != nil } | count() by(span.svc) > 1 | topk(1)",
            &store,
        )
        .await
        .unwrap();
        assert!(
            names(&out)
                == vec![
                    "cache-a".to_string(),
                    "cache-b".to_string(),
                    "cache-c".to_string()
                ]
        );
    }

    #[tokio::test]
    async fn count_filter_topk_by_ranks_passing_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 40, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 200, vec![("svc", AttrValue::Str("db".into()))]),
                span(
                    4,
                    "cache-a",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    5,
                    "cache-b",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    6,
                    "cache-c",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
            ],
        );

        let out = planned(
            "{ .svc != nil } | count() > 1 | topk(1) | by(span.svc)",
            &store,
        )
        .await
        .unwrap();
        assert!(
            names(&out)
                == vec![
                    "cache-a".to_string(),
                    "cache-b".to_string(),
                    "cache-c".to_string()
                ]
        );
    }

    #[tokio::test]
    async fn count_topk_filter_by_keeps_spans_from_ranked_passing_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 40, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 200, vec![("svc", AttrValue::Str("db".into()))]),
                span(
                    4,
                    "cache-a",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    5,
                    "cache-b",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    6,
                    "cache-c",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
            ],
        );

        let out = planned(
            "{ .svc != nil } | count() | topk(2) > 2 | by(span.svc)",
            &store,
        )
        .await
        .unwrap();
        assert!(
            names(&out)
                == vec![
                    "cache-a".to_string(),
                    "cache-b".to_string(),
                    "cache-c".to_string()
                ]
        );
    }

    #[tokio::test]
    async fn count_topk_by_keeps_spans_from_ranked_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 40, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 200, vec![("svc", AttrValue::Str("db".into()))]),
                span(
                    4,
                    "cache-a",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    5,
                    "cache-b",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
                span(
                    6,
                    "cache-c",
                    10,
                    vec![("svc", AttrValue::Str("cache".into()))],
                ),
            ],
        );

        let top = planned("{ .svc != nil } | count() | topk(1) | by(span.svc)", &store)
            .await
            .unwrap();
        assert!(
            names(&top)
                == vec![
                    "cache-a".to_string(),
                    "cache-b".to_string(),
                    "cache-c".to_string()
                ]
        );
    }

    #[tokio::test]
    async fn count_topk_without_by_preserves_all_matched_spans() {
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

        let top = planned("{ .svc != nil } | count() | topk(1)", &store)
            .await
            .unwrap();
        assert!(names(&top) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]);

        let bottom = planned("{ .svc != nil } | count() | bottomk(1)", &store)
            .await
            .unwrap();
        assert!(
            names(&bottom) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]
        );
    }

    #[tokio::test]
    async fn count_topk_filter_gates_ungrouped_ranked_spans() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![span(1, "api-a", 20, vec![]), span(2, "api-b", 40, vec![])],
        );
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![span_with_parent(3, None, [2; 16], "db-a", 200, vec![])],
        );

        let out = planned("{ span:name != nil } | count() | topk(1) > 1", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn count_filter_topk_gates_ungrouped_ranked_spans() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![span(1, "api-a", 20, vec![]), span(2, "api-b", 40, vec![])],
        );
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![span_with_parent(3, None, [2; 16], "db-a", 200, vec![])],
        );

        let out = planned("{ span:name != nil } | count() > 1 | topk(1)", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }
}
