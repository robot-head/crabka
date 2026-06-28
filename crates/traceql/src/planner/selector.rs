use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::ast::{ComparisonOp, Field, FieldExpr, Intrinsic, Scope, Value};
use crate::error::{Result, TraceqlError};
use crate::planner::{PlannedSpanset, PlannerContext};
use crate::span_columns::{
    ATTR_PREFIX, COL_CHILD_COUNT, COL_DURATION, COL_EVENT_NAME, COL_EVENT_TIME_SINCE_START,
    COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND, COL_LINK_SPAN_ID,
    COL_LINK_TRACE_ID, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID,
    COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_STATUS_CODE, COL_STATUS_MESSAGE,
    COL_TRACE_DURATION, COL_TRACE_ID,
};
use crate::store::{MatchCmp, MatchScope, MatchValue, SpanMatcher, SpanStore};

pub(crate) async fn plan_selector<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    fe: &FieldExpr,
) -> Result<PlannedSpanset> {
    if has_nested_scope(fe)
        && let Some(disjuncts) = field_expr_to_matcher_disjuncts(fe)
        && disjuncts.len() > 1
    {
        return plan_selector_disjuncts(store, ctx, &disjuncts).await;
    }

    let matchers = field_expr_to_matchers(fe);
    let scan = store
        .scan_with_options(
            &ctx.tenant,
            &matchers,
            ctx.start_ns,
            ctx.end_ns,
            &ctx.scan_options,
        )
        .await?;
    let inspected_bytes = scan.inspected_bytes;
    let parent_table = if needs_unfiltered_parent_table(fe) {
        register_unfiltered_parent_table(store, ctx, &scan.ctx).await?
    } else {
        scan.span_table.clone()
    };
    if !has_nested_scope(fe)
        && !has_parent_scope(fe)
        && field_expr_to_matcher_disjuncts(fe).is_some_and(|disjuncts| disjuncts.len() == 1)
    {
        let plan = scan
            .ctx
            .table(&scan.span_table)
            .await?
            .into_unoptimized_plan();
        return Ok(PlannedSpanset {
            ctx: scan.ctx,
            plan,
            inspected_bytes,
        });
    }
    let table = ident(&scan.span_table);
    let sql = selector_sql_with_parent_table(&table, &ident(&parent_table), fe)?;
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
        inspected_bytes,
    })
}

async fn plan_selector_disjuncts<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    disjuncts: &[Vec<SpanMatcher>],
) -> Result<PlannedSpanset> {
    let mut batches = Vec::new();
    let mut schema = None;
    let mut inspected_bytes = 0_u64;
    for matchers in disjuncts {
        let scan = store
            .scan_with_options(
                &ctx.tenant,
                matchers,
                ctx.start_ns,
                ctx.end_ns,
                &ctx.scan_options,
            )
            .await?;
        inspected_bytes = inspected_bytes.saturating_add(scan.inspected_bytes);
        let mut scan_batches = collect_table(&scan.ctx, &scan.span_table).await?;
        if schema.is_none() {
            schema = scan_batches.first().map(RecordBatch::schema);
        }
        batches.append(&mut scan_batches);
    }

    let schema = schema.unwrap_or_else(crate::span_columns::span_schema);
    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("spans", Arc::new(table))?;
    let df = ctx.sql("SELECT DISTINCT * FROM spans").await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx,
        plan,
        inspected_bytes,
    })
}

async fn register_unfiltered_parent_table<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    target_ctx: &SessionContext,
) -> Result<String> {
    let parent_scan = store
        .scan_with_options(
            &ctx.tenant,
            &[],
            ctx.start_ns,
            ctx.end_ns,
            &ctx.scan_options,
        )
        .await?;
    let batches = collect_table(&parent_scan.ctx, &parent_scan.span_table).await?;
    let schema = batches
        .first()
        .map_or_else(crate::span_columns::span_schema, RecordBatch::schema);
    let table = MemTable::try_new(schema, vec![batches])?;
    let table_name = "parent_spans";
    target_ctx.register_table(table_name, Arc::new(table))?;
    Ok(table_name.to_string())
}

async fn collect_table(ctx: &SessionContext, table: &str) -> Result<Vec<RecordBatch>> {
    Ok(ctx.table(table).await?.collect().await?)
}

pub(crate) fn selector_sql(table: &str, fe: &FieldExpr) -> Result<String> {
    selector_sql_with_parent_table(table, table, fe)
}

pub(crate) fn selector_sql_with_parent_table(
    table: &str,
    parent_table: &str,
    fe: &FieldExpr,
) -> Result<String> {
    if has_nested_scope(fe) {
        if has_parent_scope(fe)
            && let Some(predicate) = parent_field_expr_to_sql_qualified(fe, "s", "p")?
        {
            let trace = ident(COL_TRACE_ID);
            let parent = ident(COL_PARENT_ID);
            let left = ident(COL_NS_LEFT);
            return Ok(format!(
                "SELECT s.* FROM {table} AS s JOIN {parent_table} AS p \
                 ON s.{trace} = p.{trace} AND s.{parent} = p.{left} \
                 WHERE {predicate}"
            ));
        }
        return Ok(format!("SELECT * FROM {table}"));
    }
    if has_parent_scope(fe) {
        let predicate = field_expr_to_sql_qualified(fe, "s", "p")?;
        let trace = ident(COL_TRACE_ID);
        let parent = ident(COL_PARENT_ID);
        let left = ident(COL_NS_LEFT);
        Ok(format!(
            "SELECT s.* FROM {table} AS s JOIN {table} AS p \
             ON s.{trace} = p.{trace} AND s.{parent} = p.{left} \
             WHERE {predicate}"
        ))
    } else {
        let predicate = field_expr_to_sql(fe)?;
        Ok(format!("SELECT * FROM {table} WHERE {predicate}"))
    }
}

fn parent_field_expr_to_sql_qualified(
    fe: &FieldExpr,
    span_alias: &str,
    parent_alias: &str,
) -> Result<Option<String>> {
    match fe {
        FieldExpr::Comparison { lhs, op, rhs } if matches!(lhs.scope, Scope::Parent) => Ok(Some(
            comparison_to_sql_qualified(lhs, *op, rhs, span_alias, parent_alias)?,
        )),
        FieldExpr::Field(field) if matches!(field.scope, Scope::Parent) => Ok(Some(format!(
            "{} IS NOT NULL",
            qualified_field_ident(field, span_alias, parent_alias)
        ))),
        FieldExpr::And(a, b) => {
            let left = parent_field_expr_to_sql_qualified(a, span_alias, parent_alias)?;
            let right = parent_field_expr_to_sql_qualified(b, span_alias, parent_alias)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(format!("({left} AND {right})")),
                (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
                (None, None) => None,
            })
        }
        FieldExpr::Or(a, b) => {
            let left = parent_field_expr_to_sql_qualified(a, span_alias, parent_alias)?;
            let right = parent_field_expr_to_sql_qualified(b, span_alias, parent_alias)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(format!("({left} OR {right})")),
                (Some(_) | None, None) | (None, Some(_)) => None,
            })
        }
        FieldExpr::Not(inner) => {
            Ok(
                parent_field_expr_to_sql_qualified(inner, span_alias, parent_alias)?
                    .map(|predicate| format!("(NOT {predicate})")),
            )
        }
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) | FieldExpr::Const(_) => Ok(None),
    }
}

pub(crate) fn field_to_column(field: &Field) -> String {
    let col = match &field.scope {
        Scope::Intrinsic(i) => match i {
            Intrinsic::Name => COL_NAME,
            Intrinsic::Duration => COL_DURATION,
            Intrinsic::Kind => COL_KIND,
            Intrinsic::Status => COL_STATUS_CODE,
            Intrinsic::StatusMessage => COL_STATUS_MESSAGE,
            Intrinsic::Id => COL_SPAN_ID,
            Intrinsic::ParentId => COL_PARENT_SPAN_ID,
            Intrinsic::TraceDuration => COL_TRACE_DURATION,
            Intrinsic::TraceRootName => COL_ROOT_SPAN_NAME,
            Intrinsic::TraceRootService => COL_ROOT_SERVICE_NAME,
            Intrinsic::TraceId => COL_TRACE_ID,
            Intrinsic::NestedSetLeft => COL_NS_LEFT,
            Intrinsic::NestedSetRight => COL_NS_RIGHT,
            Intrinsic::NestedSetParent => COL_PARENT_ID,
            Intrinsic::ChildCount => COL_CHILD_COUNT,
            Intrinsic::InstrumentationName => COL_INSTRUMENTATION_NAME,
            Intrinsic::InstrumentationVersion => COL_INSTRUMENTATION_VERSION,
            Intrinsic::EventName => COL_EVENT_NAME,
            Intrinsic::EventTimeSinceStart => COL_EVENT_TIME_SINCE_START,
            Intrinsic::LinkTraceId => COL_LINK_TRACE_ID,
            Intrinsic::LinkSpanId => COL_LINK_SPAN_ID,
        },
        Scope::Both | Scope::Resource if field.key == "service.name" => COL_ROOT_SERVICE_NAME,
        Scope::Both
        | Scope::Span
        | Scope::Resource
        | Scope::Parent
        | Scope::Event
        | Scope::Link
        | Scope::Instrumentation => return format!("{ATTR_PREFIX}{}", field.key),
    };
    col.to_string()
}

pub(crate) fn field_expr_to_sql(fe: &FieldExpr) -> Result<String> {
    match fe {
        FieldExpr::Comparison { lhs, op, rhs } => comparison_to_sql(lhs, *op, rhs),
        FieldExpr::And(a, b) => Ok(format!(
            "({} AND {})",
            field_expr_to_sql(a)?,
            field_expr_to_sql(b)?
        )),
        FieldExpr::Or(a, b) => Ok(format!(
            "({} OR {})",
            field_expr_to_sql(a)?,
            field_expr_to_sql(b)?
        )),
        FieldExpr::Not(inner) => Ok(format!("(NOT {})", field_expr_to_sql(inner)?)),
        FieldExpr::Field(field) => Ok(format!("{} IS NOT NULL", ident(&field_to_column(field)))),
        // `{}` / `{ true }` => match every span; `{ false }` => match none.
        FieldExpr::Const(value) => Ok(if *value {
            "TRUE".into()
        } else {
            "FALSE".into()
        }),
    }
}

pub(crate) fn has_nested_scope(fe: &FieldExpr) -> bool {
    match fe {
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => {
            matches!(lhs.scope, Scope::Event | Scope::Link)
                || matches!(
                    lhs.scope,
                    Scope::Intrinsic(
                        Intrinsic::EventName
                            | Intrinsic::EventTimeSinceStart
                            | Intrinsic::LinkTraceId
                            | Intrinsic::LinkSpanId
                    )
                )
        }
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => has_nested_scope(a) || has_nested_scope(b),
        FieldExpr::Not(inner) => has_nested_scope(inner),
        FieldExpr::Const(_) => false,
    }
}

pub(crate) fn has_parent_scope(fe: &FieldExpr) -> bool {
    match fe {
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => {
            matches!(lhs.scope, Scope::Parent)
        }
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => has_parent_scope(a) || has_parent_scope(b),
        FieldExpr::Not(inner) => has_parent_scope(inner),
        FieldExpr::Const(_) => false,
    }
}

fn needs_unfiltered_parent_table(fe: &FieldExpr) -> bool {
    has_nested_scope(fe) && has_parent_scope(fe)
}

fn field_expr_to_sql_qualified(
    fe: &FieldExpr,
    span_alias: &str,
    parent_alias: &str,
) -> Result<String> {
    match fe {
        FieldExpr::Comparison { lhs, op, rhs } => {
            comparison_to_sql_qualified(lhs, *op, rhs, span_alias, parent_alias)
        }
        FieldExpr::And(a, b) => Ok(format!(
            "({} AND {})",
            field_expr_to_sql_qualified(a, span_alias, parent_alias)?,
            field_expr_to_sql_qualified(b, span_alias, parent_alias)?
        )),
        FieldExpr::Or(a, b) => Ok(format!(
            "({} OR {})",
            field_expr_to_sql_qualified(a, span_alias, parent_alias)?,
            field_expr_to_sql_qualified(b, span_alias, parent_alias)?
        )),
        FieldExpr::Not(inner) => Ok(format!(
            "(NOT {})",
            field_expr_to_sql_qualified(inner, span_alias, parent_alias)?
        )),
        FieldExpr::Field(field) => Ok(format!(
            "{} IS NOT NULL",
            qualified_field_ident(field, span_alias, parent_alias)
        )),
        FieldExpr::Const(value) => Ok(if *value {
            "TRUE".into()
        } else {
            "FALSE".into()
        }),
    }
}

pub(crate) fn comparison_to_sql(field: &Field, op: ComparisonOp, value: &Value) -> Result<String> {
    let col = ident(&field_to_column(field));
    Ok(match (op, value) {
        (ComparisonOp::Eq, Value::Nil) => format!("{col} IS NULL"),
        (ComparisonOp::Neq, Value::Nil) => format!("{col} IS NOT NULL"),
        (ComparisonOp::Re, Value::Str(pattern)) => {
            format!("regexp_like({col}, {})", string_lit(&anchored(pattern)))
        }
        (ComparisonOp::Nre, Value::Str(pattern)) => {
            format!("NOT regexp_like({col}, {})", string_lit(&anchored(pattern)))
        }
        (ComparisonOp::Eq, v) => format!("{col} = {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Neq, v) => format!("{col} != {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Lt, v) => format!("{col} < {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Lte, v) => format!("{col} <= {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Gt, v) => format!("{col} > {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Gte, v) => format!("{col} >= {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Re | ComparisonOp::Nre, _) => {
            return Err(TraceqlError::Plan(
                "regex comparison requires string value".into(),
            ));
        }
    })
}

fn comparison_to_sql_qualified(
    field: &Field,
    op: ComparisonOp,
    value: &Value,
    span_alias: &str,
    parent_alias: &str,
) -> Result<String> {
    let col = qualified_field_ident(field, span_alias, parent_alias);
    Ok(match (op, value) {
        (ComparisonOp::Eq, Value::Nil) => format!("{col} IS NULL"),
        (ComparisonOp::Neq, Value::Nil) => format!("{col} IS NOT NULL"),
        (ComparisonOp::Re, Value::Str(pattern)) => {
            format!("regexp_like({col}, {})", string_lit(&anchored(pattern)))
        }
        (ComparisonOp::Nre, Value::Str(pattern)) => {
            format!("NOT regexp_like({col}, {})", string_lit(&anchored(pattern)))
        }
        (ComparisonOp::Eq, v) => format!("{col} = {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Neq, v) => format!("{col} != {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Lt, v) => format!("{col} < {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Lte, v) => format!("{col} <= {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Gt, v) => format!("{col} > {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Gte, v) => format!("{col} >= {}", comparison_value_sql(field, v)?),
        (ComparisonOp::Re | ComparisonOp::Nre, _) => {
            return Err(TraceqlError::Plan(
                "regex comparison requires string value".into(),
            ));
        }
    })
}

fn qualified_field_ident(field: &Field, span_alias: &str, parent_alias: &str) -> String {
    let alias = if matches!(field.scope, Scope::Parent) {
        parent_alias
    } else {
        span_alias
    };
    format!("{alias}.{}", ident(&field_to_column(field)))
}

pub(crate) fn field_expr_to_matchers(fe: &FieldExpr) -> Vec<SpanMatcher> {
    match fe {
        FieldExpr::And(a, b) => {
            let mut out = field_expr_to_matchers(a);
            out.extend(field_expr_to_matchers(b));
            out
        }
        FieldExpr::Comparison { .. } => matcher_from_field_expr(fe).into_iter().collect(),
        FieldExpr::Not(inner) if has_nested_scope(inner) => {
            field_expr_to_negated_matcher_disjuncts(inner)
                .filter(|disjuncts| disjuncts.len() == 1)
                .and_then(|mut disjuncts| disjuncts.pop())
                .unwrap_or_default()
        }
        // A constant filter carries no per-span matcher; the SQL predicate
        // (`TRUE`/`FALSE`) is authoritative, so it contributes no pre-filter.
        FieldExpr::Or(_, _) | FieldExpr::Not(_) | FieldExpr::Field(_) | FieldExpr::Const(_) => {
            vec![]
        }
    }
}

fn field_expr_to_matcher_disjuncts(fe: &FieldExpr) -> Option<Vec<Vec<SpanMatcher>>> {
    match fe {
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) => {
            Some(vec![vec![matcher_from_field_expr(fe).expect(
                "comparison and field expressions lower to matchers",
            )]])
        }
        // A constant filter is the identity/annihilator of the matcher DNF, not
        // "unrepresentable": `true` is match-all (one disjunct with no matchers,
        // the AND-identity) and `false` is match-none (zero disjuncts). Returning
        // `None` here would poison an enclosing `And` via `?`, dropping the
        // sibling's matchers and the attribute columns they project (e.g.
        // `{ span.http.method != nil && true }` would lose the `attr.http.method`
        // projection and fail to plan).
        FieldExpr::Const(value) => Some(if *value { vec![vec![]] } else { vec![] }),
        FieldExpr::And(a, b) => {
            let left = field_expr_to_matcher_disjuncts(a)?;
            let right = field_expr_to_matcher_disjuncts(b)?;
            Some(
                left.iter()
                    .flat_map(|l| {
                        right.iter().map(move |r| {
                            let mut out = l.clone();
                            out.extend(r.clone());
                            out
                        })
                    })
                    .collect(),
            )
        }
        FieldExpr::Or(a, b) => {
            let mut out = field_expr_to_matcher_disjuncts(a)?;
            out.extend(field_expr_to_matcher_disjuncts(b)?);
            Some(out)
        }
        FieldExpr::Not(inner) if has_nested_scope(inner) => {
            field_expr_to_negated_matcher_disjuncts(inner)
        }
        FieldExpr::Not(_) => None,
    }
}

fn field_expr_to_negated_matcher_disjuncts(fe: &FieldExpr) -> Option<Vec<Vec<SpanMatcher>>> {
    match fe {
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) => {
            matcher_from_field_expr(fe).map(|matcher| vec![vec![negate_matcher(matcher)]])
        }
        FieldExpr::Or(a, b) => {
            let left = field_expr_to_negated_matcher_disjuncts(a)?;
            let right = field_expr_to_negated_matcher_disjuncts(b)?;
            Some(
                left.iter()
                    .flat_map(|l| {
                        right.iter().map(move |r| {
                            let mut out = l.clone();
                            out.extend(r.clone());
                            out
                        })
                    })
                    .collect(),
            )
        }
        FieldExpr::And(a, b) => {
            let mut out = field_expr_to_negated_matcher_disjuncts(a)?;
            out.extend(field_expr_to_negated_matcher_disjuncts(b)?);
            Some(out)
        }
        FieldExpr::Not(inner) => field_expr_to_matcher_disjuncts(inner),
        // Negated constant: `!true` is match-none (zero disjuncts), `!false` is
        // match-all (one empty disjunct). Mirrors the non-negated identity so a
        // `Const` sibling never poisons an enclosing conjunction.
        FieldExpr::Const(value) => Some(if *value { vec![] } else { vec![vec![]] }),
    }
}

fn matcher_from_field_expr(fe: &FieldExpr) -> Option<SpanMatcher> {
    match fe {
        FieldExpr::Comparison { lhs, op, rhs } => Some(SpanMatcher {
            scope: match_scope(&lhs.scope),
            key: matcher_key(lhs),
            op: match_cmp(*op),
            value: match_value(rhs),
            negated: false,
        }),
        FieldExpr::Field(field) => Some(SpanMatcher {
            scope: match_scope(&field.scope),
            key: matcher_key(field),
            op: MatchCmp::Neq,
            value: MatchValue::Nil,
            negated: false,
        }),
        FieldExpr::And(_, _) | FieldExpr::Or(_, _) | FieldExpr::Not(_) | FieldExpr::Const(_) => {
            None
        }
    }
}

fn negate_matcher(mut matcher: SpanMatcher) -> SpanMatcher {
    matcher.negated = !matcher.negated;
    matcher
}

fn matcher_key(field: &Field) -> String {
    match &field.scope {
        Scope::Intrinsic(intrinsic) => intrinsic_match_key(intrinsic).to_string(),
        _ => field.key.clone(),
    }
}

fn intrinsic_match_key(intrinsic: &Intrinsic) -> &'static str {
    match intrinsic {
        Intrinsic::Name => "span:name",
        Intrinsic::Duration => "span:duration",
        Intrinsic::Kind => "span:kind",
        Intrinsic::Status => "span:status",
        Intrinsic::StatusMessage => "span:statusMessage",
        Intrinsic::Id => "span:id",
        Intrinsic::ParentId => "span:parentID",
        Intrinsic::TraceDuration => "trace:duration",
        Intrinsic::TraceRootName => "trace:rootName",
        Intrinsic::TraceRootService => "trace:rootService",
        Intrinsic::TraceId => "trace:id",
        Intrinsic::NestedSetLeft => "span:nestedSetLeft",
        Intrinsic::NestedSetRight => "span:nestedSetRight",
        Intrinsic::NestedSetParent => "span:nestedSetParent",
        Intrinsic::ChildCount => "span:childCount",
        Intrinsic::InstrumentationName => "instrumentation:name",
        Intrinsic::InstrumentationVersion => "instrumentation:version",
        Intrinsic::EventName => "event:name",
        Intrinsic::EventTimeSinceStart => "event:timeSinceStart",
        Intrinsic::LinkTraceId => "link:traceID",
        Intrinsic::LinkSpanId => "link:spanID",
    }
}

fn match_scope(scope: &Scope) -> MatchScope {
    match scope {
        Scope::Both => MatchScope::Both,
        Scope::Span => MatchScope::Span,
        Scope::Resource => MatchScope::Resource,
        Scope::Parent => MatchScope::Parent,
        Scope::Event => MatchScope::Event,
        Scope::Link => MatchScope::Link,
        Scope::Instrumentation => MatchScope::Instrumentation,
        Scope::Intrinsic(_) => MatchScope::Intrinsic,
    }
}

fn match_cmp(op: ComparisonOp) -> MatchCmp {
    match op {
        ComparisonOp::Eq => MatchCmp::Eq,
        ComparisonOp::Neq => MatchCmp::Neq,
        ComparisonOp::Lt => MatchCmp::Lt,
        ComparisonOp::Lte => MatchCmp::Lte,
        ComparisonOp::Gt => MatchCmp::Gt,
        ComparisonOp::Gte => MatchCmp::Gte,
        ComparisonOp::Re => MatchCmp::Re,
        ComparisonOp::Nre => MatchCmp::Nre,
    }
}

fn match_value(value: &Value) -> MatchValue {
    match value {
        Value::Str(v) => MatchValue::Str(v.clone()),
        Value::Int(v) | Value::Duration(v) => MatchValue::Int(*v),
        Value::Float(v) => MatchValue::Float(*v),
        Value::Bool(v) => MatchValue::Bool(*v),
        Value::Nil => MatchValue::Nil,
    }
}

fn value_sql(value: &Value) -> Result<String> {
    match value {
        Value::Str(v) => Ok(string_lit(v)),
        Value::Int(v) | Value::Duration(v) => Ok(v.to_string()),
        Value::Float(v) => {
            if !v.is_finite() {
                return Err(TraceqlError::Plan("comparison value is not finite".into()));
            }
            Ok(v.to_string())
        }
        Value::Bool(v) => Ok(v.to_string()),
        Value::Nil => Err(TraceqlError::Plan(
            "nil only supports equality comparisons".into(),
        )),
    }
}

fn comparison_value_sql(field: &Field, value: &Value) -> Result<String> {
    if matches!(
        field.scope,
        Scope::Intrinsic(Intrinsic::Kind | Intrinsic::Status)
    ) && let Value::Str(name) = value
    {
        return enum_value_sql(&field.scope, name);
    }
    let width = match field.scope {
        Scope::Intrinsic(Intrinsic::TraceId | Intrinsic::LinkTraceId) => Some(16),
        Scope::Intrinsic(Intrinsic::Id | Intrinsic::ParentId | Intrinsic::LinkSpanId) => Some(8),
        _ => None,
    };
    if let Some(width) = width {
        let Value::Str(hex) = value else {
            return Err(TraceqlError::Plan(format!(
                "{} comparisons require a hex string value",
                intrinsic_name(&field.scope)
            )));
        };
        return fixed_hex_lit(hex, width);
    }
    value_sql(value)
}

fn enum_value_sql(scope: &Scope, name: &str) -> Result<String> {
    let normalized = name.to_ascii_lowercase();
    let value = match scope {
        Scope::Intrinsic(Intrinsic::Status) => status_enum_value(&normalized),
        Scope::Intrinsic(Intrinsic::Kind) => kind_enum_value(&normalized),
        _ => {
            return Err(TraceqlError::Plan(format!(
                "unknown {} enum value {name:?}",
                intrinsic_name(scope)
            )));
        }
    };
    value.map(|v| v.to_string()).ok_or_else(|| {
        TraceqlError::Plan(format!(
            "unknown {} enum value {name:?}",
            intrinsic_name(scope)
        ))
    })
}

fn status_enum_value(name: &str) -> Option<i32> {
    match name {
        "unset" => Some(0),
        "ok" => Some(1),
        "error" => Some(2),
        _ => None,
    }
}

fn kind_enum_value(name: &str) -> Option<i32> {
    match name {
        "unspecified" => Some(0),
        "internal" => Some(1),
        "server" => Some(2),
        "client" => Some(3),
        "producer" => Some(4),
        "consumer" => Some(5),
        _ => None,
    }
}

fn intrinsic_name(scope: &Scope) -> &'static str {
    match scope {
        Scope::Intrinsic(Intrinsic::TraceId) => "trace:id",
        Scope::Intrinsic(Intrinsic::Id) => "span:id",
        Scope::Intrinsic(Intrinsic::ParentId) => "span:parentID",
        Scope::Intrinsic(Intrinsic::Kind) => "span:kind",
        Scope::Intrinsic(Intrinsic::Status) => "span:status",
        _ => "intrinsic",
    }
}

fn fixed_hex_lit(hex: &str, width: usize) -> Result<String> {
    let expected_len = width * 2;
    if hex.len() != expected_len {
        return Err(TraceqlError::Plan(format!(
            "expected {expected_len} hex characters, got {}",
            hex.len()
        )));
    }
    if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(TraceqlError::Plan(
            "hex string contains non-hex characters".into(),
        ));
    }
    Ok(format!("X'{}'", hex.to_ascii_lowercase()))
}

pub(crate) fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn string_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn anchored(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::SpansetExpr;
    use crate::parser::parse;

    fn selector(query: &str) -> FieldExpr {
        let q = parse(query).unwrap();
        let SpansetExpr::Selector(fe) = q.root else {
            panic!("selector")
        };
        *fe
    }

    #[test]
    fn conjunctive_comparisons_become_prefilter_matchers() {
        let ms = field_expr_to_matchers(&selector("{ .a = 1 && .b =~ \"x\" }"));
        assert!(ms.len() == 2);
        assert!(ms[0].key == "a");
        assert!(ms[0].op == MatchCmp::Eq);
        assert!(ms[0].value == MatchValue::Int(1));
        assert!(ms[1].key == "b");
        assert!(ms[1].op == MatchCmp::Re);
        assert!(ms[1].value == MatchValue::Str("x".into()));
    }

    #[test]
    fn disjunction_does_not_prefilter() {
        let ms = field_expr_to_matchers(&selector("{ .a = 1 || .b = 2 }"));
        assert!(ms.is_empty());
    }

    #[test]
    fn const_true_is_and_identity_in_matcher_disjuncts() {
        // `{ .a != nil }` is one disjunct of one matcher; ANDing `&& true` (a
        // `FieldExpr::Const(true)`) must NOT collapse the DNF to `None`, which
        // would drop the `attr.a` projection and make planning fail. The const
        // is the AND-identity: the disjuncts are unchanged.
        let base = field_expr_to_matcher_disjuncts(&selector("{ .a != nil }")).unwrap();
        for q in ["{ .a != nil && true }", "{ true && .a != nil }"] {
            let with_const = field_expr_to_matcher_disjuncts(&selector(q)).unwrap();
            assert!(with_const == base, "{q}: {with_const:?} != {base:?}");
        }
        // The prefilter matcher is still collected so the scan projects `attr.a`.
        let ms = field_expr_to_matchers(&selector("{ .a != nil && true }"));
        assert!(ms.len() == 1 && ms[0].key == "a");
    }

    #[test]
    fn const_false_is_match_none_in_matcher_disjuncts() {
        // `false` is the annihilator: zero disjuncts (match nothing), and ANDing
        // it in drops all other disjuncts.
        assert!(
            field_expr_to_matcher_disjuncts(&selector("{ false }")).unwrap()
                == Vec::<Vec<SpanMatcher>>::new()
        );
        assert!(
            field_expr_to_matcher_disjuncts(&selector("{ .a != nil && false }"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn const_true_alone_is_single_empty_match_all_disjunct() {
        // `{}` / `{ true }` => exactly one disjunct with no matchers, which the
        // planner treats as an unfiltered (match-all) scan.
        let d = field_expr_to_matcher_disjuncts(&selector("{ true }")).unwrap();
        assert!(d.len() == 1 && d[0].is_empty());
    }

    #[test]
    fn nil_comparison_is_presence_prefilter() {
        let ms = field_expr_to_matchers(&selector("{ .a != nil }"));
        assert!(ms.len() == 1);
        assert!(ms[0].key == "a");
        assert!(ms[0].op == MatchCmp::Neq);
        assert!(ms[0].value == MatchValue::Nil);
    }

    #[test]
    fn duration_value_maps_to_integer_nanos() {
        let ms = field_expr_to_matchers(&selector("{ span:duration > 100ms }"));
        assert!(ms[0].scope == MatchScope::Intrinsic);
        assert!(ms[0].value == MatchValue::Int(100_000_000));
    }

    #[test]
    fn non_finite_folded_float_comparison_errors_cleanly() {
        // A non-finite folded float (e.g. from overflowing float multiplication)
        // must be rejected by the SQL emitter rather than interpolated as a
        // literal `inf`/`NaN`, which DataFusion cannot parse.
        let field = Field {
            scope: Scope::Both,
            key: "x".into(),
        };
        for bad in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let err = comparison_to_sql(&field, ComparisonOp::Gt, &Value::Float(bad));
            assert!(matches!(err, Err(TraceqlError::Plan(_))));
        }
        // Finite floats still produce SQL.
        let ok = comparison_to_sql(&field, ComparisonOp::Gt, &Value::Float(1.5));
        assert!(ok.is_ok());
    }

    fn intrinsic_field(intrinsic: Intrinsic) -> Field {
        Field {
            scope: Scope::Intrinsic(intrinsic),
            key: String::new(),
        }
    }

    fn attr_field(scope: Scope, key: &str) -> Field {
        Field {
            scope,
            key: key.into(),
        }
    }

    // ---- field_to_column: every intrinsic + every attribute scope ----

    #[test]
    fn field_to_column_maps_all_intrinsics() {
        let cases = [
            (Intrinsic::Name, COL_NAME),
            (Intrinsic::Duration, COL_DURATION),
            (Intrinsic::Kind, COL_KIND),
            (Intrinsic::Status, COL_STATUS_CODE),
            (Intrinsic::StatusMessage, COL_STATUS_MESSAGE),
            (Intrinsic::Id, COL_SPAN_ID),
            (Intrinsic::ParentId, COL_PARENT_SPAN_ID),
            (Intrinsic::TraceDuration, COL_TRACE_DURATION),
            (Intrinsic::TraceRootName, COL_ROOT_SPAN_NAME),
            (Intrinsic::TraceRootService, COL_ROOT_SERVICE_NAME),
            (Intrinsic::TraceId, COL_TRACE_ID),
            (Intrinsic::NestedSetLeft, COL_NS_LEFT),
            (Intrinsic::NestedSetRight, COL_NS_RIGHT),
            (Intrinsic::NestedSetParent, COL_PARENT_ID),
            (Intrinsic::ChildCount, COL_CHILD_COUNT),
            (Intrinsic::InstrumentationName, COL_INSTRUMENTATION_NAME),
            (
                Intrinsic::InstrumentationVersion,
                COL_INSTRUMENTATION_VERSION,
            ),
            (Intrinsic::EventName, COL_EVENT_NAME),
            (Intrinsic::EventTimeSinceStart, COL_EVENT_TIME_SINCE_START),
            (Intrinsic::LinkTraceId, COL_LINK_TRACE_ID),
            (Intrinsic::LinkSpanId, COL_LINK_SPAN_ID),
        ];
        for (intrinsic, expected) in cases {
            let col = field_to_column(&intrinsic_field(intrinsic.clone()));
            assert!(col == expected, "intrinsic {intrinsic:?} -> {col}");
        }
    }

    #[test]
    fn field_to_column_service_name_resolves_to_root_service() {
        // `service.name` short-circuits to the root-service column for both the
        // ambiguous (Both) and explicit Resource scopes.
        assert!(field_to_column(&attr_field(Scope::Both, "service.name")) == COL_ROOT_SERVICE_NAME);
        assert!(
            field_to_column(&attr_field(Scope::Resource, "service.name")) == COL_ROOT_SERVICE_NAME
        );
    }

    #[test]
    fn field_to_column_attribute_scopes_get_attr_prefix() {
        for scope in [
            Scope::Both,
            Scope::Span,
            Scope::Resource,
            Scope::Parent,
            Scope::Event,
            Scope::Link,
            Scope::Instrumentation,
        ] {
            let col = field_to_column(&attr_field(scope.clone(), "region"));
            assert!(
                col == format!("{ATTR_PREFIX}region"),
                "scope {scope:?} -> {col}"
            );
        }
    }

    // ---- comparison_to_sql: every operator, nil, regex ----

    #[test]
    fn comparison_to_sql_covers_all_operators() {
        let field = attr_field(Scope::Both, "x");
        let col = ident(&field_to_column(&field));
        let cases = [
            (ComparisonOp::Eq, format!("{col} = 1")),
            (ComparisonOp::Neq, format!("{col} != 1")),
            (ComparisonOp::Lt, format!("{col} < 1")),
            (ComparisonOp::Lte, format!("{col} <= 1")),
            (ComparisonOp::Gt, format!("{col} > 1")),
            (ComparisonOp::Gte, format!("{col} >= 1")),
        ];
        for (op, expected) in cases {
            let sql = comparison_to_sql(&field, op, &Value::Int(1)).unwrap();
            assert!(sql == expected, "{op:?} -> {sql}");
        }
    }

    #[test]
    fn comparison_to_sql_nil_uses_null_predicates() {
        let field = attr_field(Scope::Both, "x");
        let col = ident(&field_to_column(&field));
        assert!(
            comparison_to_sql(&field, ComparisonOp::Eq, &Value::Nil).unwrap()
                == format!("{col} IS NULL")
        );
        assert!(
            comparison_to_sql(&field, ComparisonOp::Neq, &Value::Nil).unwrap()
                == format!("{col} IS NOT NULL")
        );
    }

    #[test]
    fn comparison_to_sql_regex_is_anchored() {
        let field = attr_field(Scope::Both, "x");
        let col = ident(&field_to_column(&field));
        let re = comparison_to_sql(&field, ComparisonOp::Re, &Value::Str("ab".into())).unwrap();
        assert!(re == format!("regexp_like({col}, '^(?:ab)$')"));
        let nre = comparison_to_sql(&field, ComparisonOp::Nre, &Value::Str("ab".into())).unwrap();
        assert!(nre == format!("NOT regexp_like({col}, '^(?:ab)$')"));
    }

    #[test]
    fn comparison_to_sql_regex_against_non_string_errors() {
        let field = attr_field(Scope::Both, "x");
        for op in [ComparisonOp::Re, ComparisonOp::Nre] {
            let err = comparison_to_sql(&field, op, &Value::Int(3));
            assert!(matches!(err, Err(TraceqlError::Plan(_))));
        }
    }

    // ---- field_expr_to_sql: And / Or / Not / Field ----

    #[test]
    fn field_expr_to_sql_combines_boolean_operators() {
        let sql = field_expr_to_sql(&selector("{ .a = 1 && .b = 2 }")).unwrap();
        assert!(sql == "(\"attr.a\" = 1 AND \"attr.b\" = 2)");

        let sql = field_expr_to_sql(&selector("{ .a = 1 || .b = 2 }")).unwrap();
        assert!(sql == "(\"attr.a\" = 1 OR \"attr.b\" = 2)");

        let sql = field_expr_to_sql(&selector("{ !(.a = 1) }")).unwrap();
        assert!(sql == "(NOT \"attr.a\" = 1)");
    }

    #[test]
    fn field_expr_to_sql_bare_field_is_presence_check() {
        let sql = field_expr_to_sql(&selector("{ .a }")).unwrap();
        assert!(sql == "\"attr.a\" IS NOT NULL");
    }

    // ---- selector_sql variants: span-only, parent-join, nested ----

    #[test]
    fn selector_sql_plain_predicate_filters_table() {
        let sql = selector_sql("\"spans\"", &selector("{ .a = 1 }")).unwrap();
        assert!(sql == "SELECT * FROM \"spans\" WHERE \"attr.a\" = 1");
    }

    #[test]
    fn selector_sql_parent_scope_emits_self_join() {
        let sql = selector_sql("\"spans\"", &selector("{ parent.a = 1 }")).unwrap();
        // Parent scope joins the table to itself on trace_id / parent_id linkage
        // and qualifies the parent predicate with the `p` alias.
        assert!(sql.contains("FROM \"spans\" AS s JOIN \"spans\" AS p"));
        assert!(sql.contains("WHERE p.\"attr.a\" = 1"));
        assert!(sql.contains("s.\"parent_id\" = p.\"nested_set_left\""));
    }

    #[test]
    fn selector_sql_nested_scope_without_parent_selects_all() {
        // An event/link scoped selector has its filtering applied at scan time,
        // so the SQL projection is an unfiltered passthrough.
        let sql = selector_sql("\"spans\"", &selector("{ event.foo = 1 }")).unwrap();
        assert!(sql == "SELECT * FROM \"spans\"");
    }

    #[test]
    fn selector_sql_nested_and_parent_emits_qualified_parent_join() {
        // Mixing a nested (event) scope with a parent scope drives the
        // parent-qualified branch of `selector_sql_with_parent_table`.
        let fe = selector("{ event.foo = 1 && parent.a = 2 }");
        let sql = selector_sql_with_parent_table("\"spans\"", "\"parents\"", &fe).unwrap();
        assert!(sql.contains("FROM \"spans\" AS s JOIN \"parents\" AS p"));
        assert!(sql.contains("p.\"attr.a\" = 2"));
    }

    // ---- parent_field_expr_to_sql_qualified: And/Or/Not pruning ----

    #[test]
    fn parent_predicate_extracts_only_parent_conjuncts() {
        // AND keeps the parent conjunct and drops the non-parent one.
        let fe = selector("{ parent.a = 1 && .b = 2 }");
        let pred = parent_field_expr_to_sql_qualified(&fe, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "p.\"attr.a\" = 1");
    }

    #[test]
    fn parent_predicate_keeps_both_parent_conjuncts() {
        let fe = selector("{ parent.a = 1 && parent.b = 2 }");
        let pred = parent_field_expr_to_sql_qualified(&fe, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "(p.\"attr.a\" = 1 AND p.\"attr.b\" = 2)");
    }

    #[test]
    fn parent_predicate_bare_parent_field_is_presence() {
        let fe = selector("{ parent.a }");
        let pred = parent_field_expr_to_sql_qualified(&fe, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "p.\"attr.a\" IS NOT NULL");
    }

    #[test]
    fn parent_predicate_or_requires_both_sides_parent() {
        // A mixed OR cannot be pushed into the parent join (no safe predicate).
        let mixed = selector("{ parent.a = 1 || .b = 2 }");
        assert!(
            parent_field_expr_to_sql_qualified(&mixed, "s", "p")
                .unwrap()
                .is_none()
        );

        // Both sides parent -> a parent OR predicate is produced.
        let both = selector("{ parent.a = 1 || parent.b = 2 }");
        let pred = parent_field_expr_to_sql_qualified(&both, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "(p.\"attr.a\" = 1 OR p.\"attr.b\" = 2)");
    }

    #[test]
    fn parent_predicate_negation_wraps_inner() {
        let fe = selector("{ !(parent.a = 1) }");
        let pred = parent_field_expr_to_sql_qualified(&fe, "s", "p")
            .unwrap()
            .unwrap();
        assert!(pred == "(NOT p.\"attr.a\" = 1)");
    }

    #[test]
    fn parent_predicate_non_parent_leaf_yields_none() {
        let fe = selector("{ .b = 2 }");
        assert!(
            parent_field_expr_to_sql_qualified(&fe, "s", "p")
                .unwrap()
                .is_none()
        );
        let bare = selector("{ .b }");
        assert!(
            parent_field_expr_to_sql_qualified(&bare, "s", "p")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn parent_predicate_and_of_non_parent_leaves_yields_none() {
        // Drives the And arm where both sides lower to None (no parent conjunct
        // anywhere) -> the whole And predicate is None.
        let fe = selector("{ .a = 1 && .b = 2 }");
        assert!(
            parent_field_expr_to_sql_qualified(&fe, "s", "p")
                .unwrap()
                .is_none()
        );
    }

    // ---- field_expr_to_sql_qualified: parent alias routing ----

    #[test]
    fn qualified_sql_routes_parent_to_parent_alias() {
        let fe = selector("{ parent.a = 1 && .b = 2 }");
        let sql = field_expr_to_sql_qualified(&fe, "s", "p").unwrap();
        assert!(sql == "(p.\"attr.a\" = 1 AND s.\"attr.b\" = 2)");

        let fe = selector("{ parent.a = 1 || .b = 2 }");
        let sql = field_expr_to_sql_qualified(&fe, "s", "p").unwrap();
        assert!(sql == "(p.\"attr.a\" = 1 OR s.\"attr.b\" = 2)");

        let fe = selector("{ !(parent.a = 1) }");
        let sql = field_expr_to_sql_qualified(&fe, "s", "p").unwrap();
        assert!(sql == "(NOT p.\"attr.a\" = 1)");

        let bare = selector("{ parent.a }");
        let sql = field_expr_to_sql_qualified(&bare, "s", "p").unwrap();
        assert!(sql == "p.\"attr.a\" IS NOT NULL");
    }

    // ---- comparison_value_sql: enums, hex widths, errors ----

    #[test]
    fn comparison_value_sql_maps_status_enum() {
        let status = intrinsic_field(Intrinsic::Status);
        for (name, code) in [("unset", 0), ("ok", 1), ("error", 2), ("ERROR", 2)] {
            let sql = comparison_value_sql(&status, &Value::Str(name.into())).unwrap();
            assert!(sql == code.to_string(), "status {name} -> {sql}");
        }
        let err = comparison_value_sql(&status, &Value::Str("bogus".into()));
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn comparison_value_sql_maps_kind_enum() {
        let kind = intrinsic_field(Intrinsic::Kind);
        for (name, code) in [
            ("unspecified", 0),
            ("internal", 1),
            ("server", 2),
            ("client", 3),
            ("producer", 4),
            ("consumer", 5),
            ("Server", 2),
        ] {
            let sql = comparison_value_sql(&kind, &Value::Str(name.into())).unwrap();
            assert!(sql == code.to_string(), "kind {name} -> {sql}");
        }
        let err = comparison_value_sql(&kind, &Value::Str("bogus".into()));
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn comparison_value_sql_non_string_enum_falls_through_to_int() {
        // A numeric value against an enum intrinsic skips enum mapping and is
        // emitted as a plain integer literal.
        let kind = intrinsic_field(Intrinsic::Kind);
        let sql = comparison_value_sql(&kind, &Value::Int(3)).unwrap();
        assert!(sql == "3");
    }

    #[test]
    fn comparison_value_sql_trace_id_requires_16_byte_hex() {
        let trace = intrinsic_field(Intrinsic::TraceId);
        let hex = "0123456789abcdef0123456789abcdef"; // 32 chars = 16 bytes
        let sql = comparison_value_sql(&trace, &Value::Str(hex.into())).unwrap();
        assert!(sql == format!("X'{hex}'"));

        // Wrong length is rejected.
        let err = comparison_value_sql(&trace, &Value::Str("abcd".into()));
        assert!(matches!(err, Err(TraceqlError::Plan(_))));

        // Non-string value is rejected with the hex-string error.
        let err = comparison_value_sql(&trace, &Value::Int(1));
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn comparison_value_sql_span_id_requires_8_byte_hex() {
        for intrinsic in [Intrinsic::Id, Intrinsic::ParentId, Intrinsic::LinkSpanId] {
            let field = intrinsic_field(intrinsic.clone());
            let hex = "0011223344556677"; // 16 chars = 8 bytes
            let sql = comparison_value_sql(&field, &Value::Str(hex.into())).unwrap();
            assert!(sql == format!("X'{hex}'"), "{intrinsic:?}");
        }
        let link_trace = intrinsic_field(Intrinsic::LinkTraceId);
        let hex = "0123456789abcdef0123456789abcdef";
        let sql = comparison_value_sql(&link_trace, &Value::Str(hex.into())).unwrap();
        assert!(sql == format!("X'{hex}'"));
    }

    #[test]
    fn comparison_value_sql_uppercases_to_lowercase_hex() {
        let trace = intrinsic_field(Intrinsic::TraceId);
        let hex = "0123456789ABCDEF0123456789ABCDEF";
        let sql = comparison_value_sql(&trace, &Value::Str(hex.into())).unwrap();
        assert!(sql == "X'0123456789abcdef0123456789abcdef'");
    }

    #[test]
    fn fixed_hex_lit_rejects_non_hex_characters() {
        // Right length but a non-hex digit ('g') -> error.
        let err = fixed_hex_lit("0123456789abcdeg", 8);
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn comparison_value_sql_plain_field_uses_value_sql() {
        let field = attr_field(Scope::Both, "x");
        assert!(comparison_value_sql(&field, &Value::Int(7)).unwrap() == "7");
        assert!(comparison_value_sql(&field, &Value::Str("hi".into())).unwrap() == "'hi'");
        assert!(comparison_value_sql(&field, &Value::Bool(true)).unwrap() == "true");
        assert!(comparison_value_sql(&field, &Value::Duration(5)).unwrap() == "5");
    }

    // ---- value_sql: bool literal, nil error ----

    #[test]
    fn value_sql_bool_and_nil() {
        assert!(value_sql(&Value::Bool(true)).unwrap() == "true");
        assert!(value_sql(&Value::Bool(false)).unwrap() == "false");
        let err = value_sql(&Value::Nil);
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    // ---- intrinsic_name: error-message labels ----

    #[test]
    fn intrinsic_name_labels_known_intrinsics() {
        assert!(intrinsic_name(&Scope::Intrinsic(Intrinsic::TraceId)) == "trace:id");
        assert!(intrinsic_name(&Scope::Intrinsic(Intrinsic::Id)) == "span:id");
        assert!(intrinsic_name(&Scope::Intrinsic(Intrinsic::ParentId)) == "span:parentID");
        assert!(intrinsic_name(&Scope::Intrinsic(Intrinsic::Kind)) == "span:kind");
        assert!(intrinsic_name(&Scope::Intrinsic(Intrinsic::Status)) == "span:status");
        // Anything else collapses to the generic label.
        assert!(intrinsic_name(&Scope::Both) == "intrinsic");
        assert!(intrinsic_name(&Scope::Intrinsic(Intrinsic::Name)) == "intrinsic");
    }

    #[test]
    fn enum_value_sql_non_enum_scope_errors() {
        // enum_value_sql guards against being called for a non-enum scope.
        let err = enum_value_sql(&Scope::Both, "ok");
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    // ---- comparison_to_sql_qualified: operators + nil + regex ----

    #[test]
    fn comparison_to_sql_qualified_covers_operators_nil_and_regex() {
        let field = attr_field(Scope::Parent, "a");
        let col = qualified_field_ident(&field, "s", "p");
        assert!(
            comparison_to_sql_qualified(&field, ComparisonOp::Eq, &Value::Int(1), "s", "p")
                .unwrap()
                == format!("{col} = 1")
        );
        assert!(
            comparison_to_sql_qualified(&field, ComparisonOp::Neq, &Value::Int(1), "s", "p")
                .unwrap()
                == format!("{col} != 1")
        );
        assert!(
            comparison_to_sql_qualified(&field, ComparisonOp::Lt, &Value::Int(1), "s", "p")
                .unwrap()
                == format!("{col} < 1")
        );
        assert!(
            comparison_to_sql_qualified(&field, ComparisonOp::Lte, &Value::Int(1), "s", "p")
                .unwrap()
                == format!("{col} <= 1")
        );
        assert!(
            comparison_to_sql_qualified(&field, ComparisonOp::Gt, &Value::Int(1), "s", "p")
                .unwrap()
                == format!("{col} > 1")
        );
        assert!(
            comparison_to_sql_qualified(&field, ComparisonOp::Gte, &Value::Int(1), "s", "p")
                .unwrap()
                == format!("{col} >= 1")
        );
        // nil
        assert!(
            comparison_to_sql_qualified(&field, ComparisonOp::Eq, &Value::Nil, "s", "p").unwrap()
                == format!("{col} IS NULL")
        );
        assert!(
            comparison_to_sql_qualified(&field, ComparisonOp::Neq, &Value::Nil, "s", "p").unwrap()
                == format!("{col} IS NOT NULL")
        );
        // regex
        assert!(
            comparison_to_sql_qualified(
                &field,
                ComparisonOp::Re,
                &Value::Str("x".into()),
                "s",
                "p"
            )
            .unwrap()
                == format!("regexp_like({col}, '^(?:x)$')")
        );
        assert!(
            comparison_to_sql_qualified(
                &field,
                ComparisonOp::Nre,
                &Value::Str("x".into()),
                "s",
                "p"
            )
            .unwrap()
                == format!("NOT regexp_like({col}, '^(?:x)$')")
        );
        // regex against non-string errors
        let err = comparison_to_sql_qualified(&field, ComparisonOp::Re, &Value::Int(1), "s", "p");
        assert!(matches!(err, Err(TraceqlError::Plan(_))));
    }

    #[test]
    fn qualified_field_ident_routes_non_parent_to_span_alias() {
        let span = attr_field(Scope::Span, "a");
        assert!(qualified_field_ident(&span, "s", "p") == "s.\"attr.a\"");
        let parent = attr_field(Scope::Parent, "a");
        assert!(qualified_field_ident(&parent, "s", "p") == "p.\"attr.a\"");
    }

    // ---- matcher disjuncts: nested negation + Or prefilter ----

    #[test]
    fn nested_negation_lowers_to_negated_matcher_disjuncts() {
        // `!{ event.foo = 1 }` is a nested scope negation, which lowers to a
        // single disjunct of one negated matcher and is usable as a prefilter.
        let ms = field_expr_to_matchers(&selector("{ !(event.foo = 1) }"));
        assert!(ms.len() == 1);
        assert!(ms[0].scope == MatchScope::Event);
        assert!(ms[0].key == "foo");
        assert!(ms[0].negated);
        assert!(ms[0].op == MatchCmp::Eq);
    }

    #[test]
    fn non_nested_negation_does_not_prefilter() {
        // A negation over a non-nested scope returns no matchers.
        let ms = field_expr_to_matchers(&selector("{ !(.a = 1) }"));
        assert!(ms.is_empty());
    }

    #[test]
    fn or_of_comparisons_produces_disjunct_per_branch() {
        let disjuncts = field_expr_to_matcher_disjuncts(&selector("{ .a = 1 || .b = 2 }")).unwrap();
        assert!(disjuncts.len() == 2);
        assert!(disjuncts[0][0].key == "a");
        assert!(disjuncts[1][0].key == "b");
    }

    #[test]
    fn and_of_comparisons_cross_products_into_single_disjunct() {
        let disjuncts = field_expr_to_matcher_disjuncts(&selector("{ .a = 1 && .b = 2 }")).unwrap();
        assert!(disjuncts.len() == 1);
        assert!(disjuncts[0].len() == 2);
    }

    #[test]
    fn top_level_negation_of_non_nested_has_no_disjuncts() {
        // `field_expr_to_matcher_disjuncts` returns None for a non-nested Not,
        // signalling the prefilter cannot be derived.
        assert!(field_expr_to_matcher_disjuncts(&selector("{ !(.a = 1) }")).is_none());
    }

    #[test]
    fn nested_de_morgan_negation_expands_disjuncts() {
        // !(event.a = 1 || event.b = 2) -> AND of two negated matchers -> single disjunct.
        let disjuncts =
            field_expr_to_matcher_disjuncts(&selector("{ !(event.a = 1 || event.b = 2) }"))
                .unwrap();
        assert!(disjuncts.len() == 1);
        assert!(disjuncts[0].len() == 2);
        assert!(disjuncts[0].iter().all(|m| m.negated));
    }

    #[test]
    fn nested_de_morgan_negation_of_and_expands_to_two_disjuncts() {
        // !(event.a = 1 && event.b = 2) -> OR of two negated matchers -> two disjuncts.
        let disjuncts =
            field_expr_to_matcher_disjuncts(&selector("{ !(event.a = 1 && event.b = 2) }"))
                .unwrap();
        assert!(disjuncts.len() == 2);
        assert!(disjuncts.iter().all(|d| d.len() == 1 && d[0].negated));
    }

    #[test]
    fn double_nested_negation_restores_positive_matcher() {
        // !!(event.a = 1) -> back to a non-negated matcher.
        let disjuncts =
            field_expr_to_matcher_disjuncts(&selector("{ !(!(event.a = 1)) }")).unwrap();
        assert!(disjuncts.len() == 1);
        assert!(disjuncts[0].len() == 1);
        assert!(!disjuncts[0][0].negated);
    }

    // ---- matcher_from_field_expr & friends: scope / cmp / value mapping ----

    #[test]
    fn matcher_from_bare_field_is_presence_neq_nil() {
        let m = matcher_from_field_expr(&selector("{ resource.region }")).unwrap();
        assert!(m.scope == MatchScope::Resource);
        assert!(m.key == "region");
        assert!(m.op == MatchCmp::Neq);
        assert!(m.value == MatchValue::Nil);
        assert!(!m.negated);
    }

    #[test]
    fn matcher_from_boolean_expr_is_none() {
        assert!(matcher_from_field_expr(&selector("{ .a = 1 && .b = 2 }")).is_none());
        assert!(matcher_from_field_expr(&selector("{ .a = 1 || .b = 2 }")).is_none());
        assert!(matcher_from_field_expr(&selector("{ !(.a = 1) }")).is_none());
    }

    #[test]
    fn match_scope_covers_every_scope() {
        assert!(match_scope(&Scope::Both) == MatchScope::Both);
        assert!(match_scope(&Scope::Span) == MatchScope::Span);
        assert!(match_scope(&Scope::Resource) == MatchScope::Resource);
        assert!(match_scope(&Scope::Parent) == MatchScope::Parent);
        assert!(match_scope(&Scope::Event) == MatchScope::Event);
        assert!(match_scope(&Scope::Link) == MatchScope::Link);
        assert!(match_scope(&Scope::Instrumentation) == MatchScope::Instrumentation);
        assert!(match_scope(&Scope::Intrinsic(Intrinsic::Name)) == MatchScope::Intrinsic);
    }

    #[test]
    fn match_cmp_covers_every_operator() {
        assert!(match_cmp(ComparisonOp::Eq) == MatchCmp::Eq);
        assert!(match_cmp(ComparisonOp::Neq) == MatchCmp::Neq);
        assert!(match_cmp(ComparisonOp::Lt) == MatchCmp::Lt);
        assert!(match_cmp(ComparisonOp::Lte) == MatchCmp::Lte);
        assert!(match_cmp(ComparisonOp::Gt) == MatchCmp::Gt);
        assert!(match_cmp(ComparisonOp::Gte) == MatchCmp::Gte);
        assert!(match_cmp(ComparisonOp::Re) == MatchCmp::Re);
        assert!(match_cmp(ComparisonOp::Nre) == MatchCmp::Nre);
    }

    #[test]
    fn match_value_covers_every_value_kind() {
        assert!(match_value(&Value::Str("x".into())) == MatchValue::Str("x".into()));
        assert!(match_value(&Value::Int(3)) == MatchValue::Int(3));
        assert!(match_value(&Value::Duration(9)) == MatchValue::Int(9));
        assert!(match_value(&Value::Float(1.5)) == MatchValue::Float(1.5));
        assert!(match_value(&Value::Bool(true)) == MatchValue::Bool(true));
        assert!(match_value(&Value::Nil) == MatchValue::Nil);
    }

    #[test]
    fn matcher_key_uses_intrinsic_canonical_names() {
        let cases = [
            (Intrinsic::Name, "span:name"),
            (Intrinsic::Duration, "span:duration"),
            (Intrinsic::Kind, "span:kind"),
            (Intrinsic::Status, "span:status"),
            (Intrinsic::StatusMessage, "span:statusMessage"),
            (Intrinsic::Id, "span:id"),
            (Intrinsic::ParentId, "span:parentID"),
            (Intrinsic::TraceDuration, "trace:duration"),
            (Intrinsic::TraceRootName, "trace:rootName"),
            (Intrinsic::TraceRootService, "trace:rootService"),
            (Intrinsic::TraceId, "trace:id"),
            (Intrinsic::NestedSetLeft, "span:nestedSetLeft"),
            (Intrinsic::NestedSetRight, "span:nestedSetRight"),
            (Intrinsic::NestedSetParent, "span:nestedSetParent"),
            (Intrinsic::ChildCount, "span:childCount"),
            (Intrinsic::InstrumentationName, "instrumentation:name"),
            (Intrinsic::InstrumentationVersion, "instrumentation:version"),
            (Intrinsic::EventName, "event:name"),
            (Intrinsic::EventTimeSinceStart, "event:timeSinceStart"),
            (Intrinsic::LinkTraceId, "link:traceID"),
            (Intrinsic::LinkSpanId, "link:spanID"),
        ];
        for (intrinsic, expected) in cases {
            let key = matcher_key(&intrinsic_field(intrinsic.clone()));
            assert!(key == expected, "{intrinsic:?} -> {key}");
        }
        // Non-intrinsic scopes keep the raw attribute key.
        assert!(matcher_key(&attr_field(Scope::Span, "http.method")) == "http.method");
    }

    // ---- ident / string_lit / anchored escaping ----

    #[test]
    fn ident_escapes_embedded_quotes() {
        assert!(ident("a\"b") == "\"a\"\"b\"");
    }

    #[test]
    fn string_lit_escapes_single_quotes() {
        assert!(string_lit("a'b") == "'a''b'");
    }

    #[test]
    fn anchored_wraps_pattern() {
        assert!(anchored("ab") == "^(?:ab)$");
    }

    // ---- has_nested_scope / has_parent_scope across combinators ----

    #[test]
    fn has_nested_scope_detects_event_link_and_intrinsics() {
        assert!(has_nested_scope(&selector("{ event.foo = 1 }")));
        assert!(has_nested_scope(&selector("{ link.foo = 1 }")));
        assert!(has_nested_scope(&selector("{ event:name = \"x\" }")));
        assert!(has_nested_scope(&selector("{ link:traceID = \"x\" }")));
        assert!(has_nested_scope(&selector("{ .a = 1 || event.b = 2 }")));
        assert!(has_nested_scope(&selector("{ !(link.b = 2) }")));
        assert!(!has_nested_scope(&selector("{ .a = 1 && .b = 2 }")));
    }

    #[test]
    fn has_parent_scope_detects_parent_across_combinators() {
        assert!(has_parent_scope(&selector("{ parent.a = 1 }")));
        assert!(has_parent_scope(&selector("{ .a = 1 && parent.b = 2 }")));
        assert!(has_parent_scope(&selector("{ !(parent.b = 2) }")));
        assert!(!has_parent_scope(&selector("{ .a = 1 }")));
    }

    #[test]
    fn unfiltered_parent_table_is_needed_only_for_nested_parent_selectors() {
        assert!(!needs_unfiltered_parent_table(&selector(
            "{ event:name = \"x\" }"
        )));
        assert!(!needs_unfiltered_parent_table(&selector(
            "{ parent.a = 1 }"
        )));
        assert!(needs_unfiltered_parent_table(&selector(
            "{ event:name = \"x\" && parent.a = 1 }"
        )));
    }

    #[test]
    fn negate_matcher_toggles_flag() {
        let m = SpanMatcher {
            scope: MatchScope::Span,
            key: "a".into(),
            op: MatchCmp::Eq,
            value: MatchValue::Int(1),
            negated: false,
        };
        let n = negate_matcher(m.clone());
        assert!(n.negated);
        let back = negate_matcher(n);
        assert!(!back.negated);
    }
}
