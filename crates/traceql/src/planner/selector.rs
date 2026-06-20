use crate::ast::{ComparisonOp, Field, FieldExpr, Intrinsic, Scope, Value};
use crate::error::{Result, TraceqlError};
use crate::planner::{PlannedSpanset, PlannerContext};
use crate::span_columns::{
    ATTR_PREFIX, COL_DURATION, COL_KIND, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID,
    COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_STATUS_CODE,
    COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID,
};
use crate::store::{MatchCmp, MatchScope, MatchValue, SpanMatcher, SpanStore};

pub(crate) async fn plan_selector<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    fe: &FieldExpr,
) -> Result<PlannedSpanset> {
    let matchers = field_expr_to_matchers(fe);
    let scan = store
        .scan(&ctx.tenant, &matchers, ctx.start_ns, ctx.end_ns)
        .await?;
    let predicate = field_expr_to_sql(fe)?;
    let sql = format!(
        "SELECT * FROM {} WHERE {predicate}",
        ident(&scan.span_table)
    );
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
    })
}

pub(crate) fn field_to_column(field: &Field) -> Result<String> {
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
            Intrinsic::ChildCount
            | Intrinsic::EventName
            | Intrinsic::EventTimeSinceStart
            | Intrinsic::LinkTraceId
            | Intrinsic::LinkSpanId
            | Intrinsic::InstrumentationName
            | Intrinsic::InstrumentationVersion => {
                return Err(TraceqlError::Unsupported(format!(
                    "intrinsic {i:?} is not mapped to a scalar span column yet"
                )));
            }
        },
        Scope::Both
        | Scope::Span
        | Scope::Resource
        | Scope::Parent
        | Scope::Event
        | Scope::Link
        | Scope::Instrumentation => return Ok(format!("{ATTR_PREFIX}{}", field.key)),
    };
    Ok(col.to_string())
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
        FieldExpr::Field(field) => Ok(format!("{} IS NOT NULL", ident(&field_to_column(field)?))),
    }
}

pub(crate) fn comparison_to_sql(field: &Field, op: ComparisonOp, value: &Value) -> Result<String> {
    let col = ident(&field_to_column(field)?);
    Ok(match (op, value) {
        (ComparisonOp::Eq, Value::Nil) => format!("{col} IS NULL"),
        (ComparisonOp::Neq, Value::Nil) => format!("{col} IS NOT NULL"),
        (ComparisonOp::Re, Value::Str(pattern)) => {
            format!("regexp_like({col}, {})", string_lit(&anchored(pattern)))
        }
        (ComparisonOp::Nre, Value::Str(pattern)) => {
            format!("NOT regexp_like({col}, {})", string_lit(&anchored(pattern)))
        }
        (ComparisonOp::Eq, v) => format!("{col} = {}", value_sql(v)?),
        (ComparisonOp::Neq, v) => format!("{col} != {}", value_sql(v)?),
        (ComparisonOp::Lt, v) => format!("{col} < {}", value_sql(v)?),
        (ComparisonOp::Lte, v) => format!("{col} <= {}", value_sql(v)?),
        (ComparisonOp::Gt, v) => format!("{col} > {}", value_sql(v)?),
        (ComparisonOp::Gte, v) => format!("{col} >= {}", value_sql(v)?),
        (ComparisonOp::Re | ComparisonOp::Nre, _) => {
            return Err(TraceqlError::Plan(
                "regex comparison requires string value".into(),
            ));
        }
    })
}

pub(crate) fn field_expr_to_matchers(fe: &FieldExpr) -> Vec<SpanMatcher> {
    match fe {
        FieldExpr::And(a, b) => {
            let mut out = field_expr_to_matchers(a);
            out.extend(field_expr_to_matchers(b));
            out
        }
        FieldExpr::Comparison { lhs, op, rhs } => vec![SpanMatcher {
            scope: match_scope(&lhs.scope),
            key: lhs.key.clone(),
            op: match_cmp(*op),
            value: match_value(rhs),
        }],
        FieldExpr::Or(_, _) | FieldExpr::Not(_) | FieldExpr::Field(_) => vec![],
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
        Value::Float(v) => Ok(v.to_string()),
        Value::Bool(v) => Ok(v.to_string()),
        Value::Nil => Err(TraceqlError::Plan(
            "nil only supports equality comparisons".into(),
        )),
    }
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
}
