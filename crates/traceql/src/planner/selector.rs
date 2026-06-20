use crate::ast::{ComparisonOp, Field, FieldExpr, Intrinsic, Scope, Value};
use crate::error::{Result, TraceqlError};
use crate::planner::{PlannedSpanset, PlannerContext};
use crate::span_columns::{
    ATTR_PREFIX, COL_CHILD_COUNT, COL_DURATION, COL_INSTRUMENTATION_NAME,
    COL_INSTRUMENTATION_VERSION, COL_KIND, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID,
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
    let table = ident(&scan.span_table);
    let sql = selector_sql(&table, fe)?;
    let df = scan.ctx.sql(&sql).await?;
    let plan = df.into_unoptimized_plan();
    Ok(PlannedSpanset {
        ctx: scan.ctx,
        plan,
    })
}

pub(crate) fn selector_sql(table: &str, fe: &FieldExpr) -> Result<String> {
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
            Intrinsic::ChildCount => COL_CHILD_COUNT,
            Intrinsic::InstrumentationName => COL_INSTRUMENTATION_NAME,
            Intrinsic::InstrumentationVersion => COL_INSTRUMENTATION_VERSION,
            Intrinsic::EventName
            | Intrinsic::EventTimeSinceStart
            | Intrinsic::LinkTraceId
            | Intrinsic::LinkSpanId => {
                return Err(TraceqlError::Unsupported(format!(
                    "intrinsic {i:?} is not mapped to a scalar span column yet"
                )));
            }
        },
        Scope::Both | Scope::Resource if field.key == "service.name" => COL_ROOT_SERVICE_NAME,
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

fn has_parent_scope(fe: &FieldExpr) -> bool {
    match fe {
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => {
            matches!(lhs.scope, Scope::Parent)
        }
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => has_parent_scope(a) || has_parent_scope(b),
        FieldExpr::Not(inner) => has_parent_scope(inner),
    }
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
            qualified_field_ident(field, span_alias, parent_alias)?
        )),
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
    let col = qualified_field_ident(field, span_alias, parent_alias)?;
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

fn qualified_field_ident(field: &Field, span_alias: &str, parent_alias: &str) -> Result<String> {
    let alias = if matches!(field.scope, Scope::Parent) {
        parent_alias
    } else {
        span_alias
    };
    Ok(format!("{alias}.{}", ident(&field_to_column(field)?)))
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

fn comparison_value_sql(field: &Field, value: &Value) -> Result<String> {
    let width = match field.scope {
        Scope::Intrinsic(Intrinsic::TraceId) => Some(16),
        Scope::Intrinsic(Intrinsic::Id | Intrinsic::ParentId) => Some(8),
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

fn intrinsic_name(scope: &Scope) -> &'static str {
    match scope {
        Scope::Intrinsic(Intrinsic::TraceId) => "trace:id",
        Scope::Intrinsic(Intrinsic::Id) => "span:id",
        Scope::Intrinsic(Intrinsic::ParentId) => "span:parentID",
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
