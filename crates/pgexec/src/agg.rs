//! SP27: aggregate functions + `GROUP BY` / `HAVING`.
//!
//! A whole table lives on a single range (`RangeMap::range_for_table`), so an
//! aggregate query executes entirely inside one `execute_read` on one engine.
//! This module is therefore a pure, deterministic fold over the already-correct
//! MVCC-visible row set — no cross-range scatter/gather, no new lock, no new
//! visibility rule, no new interleaving (see the SP27 design doc for why this
//! single-range/pure-data feature warrants no Stateright model).
//!
//! Supported: `COUNT(*)`, `COUNT(x)`, `SUM(x)`, `MIN(x)`, `MAX(x)`, their
//! `DISTINCT` forms, multi-key `GROUP BY`, and `HAVING`. `AVG` is deferred until
//! a `numeric`/float type exists.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall, SelectItem, SelectStmt};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType, TypeError, ops};
use crabka_pgwire::engine::QueryResult;

use crate::{clock::EvalCtx, error::ExecError, scope::Scope};

/// Maximum expression-tree depth the grouped evaluator (`eval_grouped_depth`)
/// will recurse before returning `54001` (statement_too_complex). Mirrors
/// `eval::MAX_EVAL_DEPTH` (3x headroom over the parser's parse-time AST depth cap
/// of 50, below the test-thread overflow point) — defense-in-depth behind the
/// parser cap (a tree this deep can never reach here in practice).
const MAX_GROUPED_DEPTH: usize = 150;

/// The aggregate functions crabgresql supports. SP30 added `Avg` (returns float8,
/// since there is no `numeric`) and float8 support for `Sum`/`Min`/`Max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// `array_agg(x)` — the inputs as a one-dimensional array, in input order.
    ArrayAgg,
    /// `jsonb_agg(x)` — the inputs as a JSON array, in input order.
    JsonbAgg,
    /// `jsonb_object_agg(key, value)` — the inputs as a JSON object.
    JsonbObjectAgg,
}

impl AggFunc {
    /// Do NULL inputs contribute a row? `count`/`sum`/`avg`/`min`/`max` skip
    /// them; the collecting aggregates keep them (a NULL becomes a NULL array
    /// element or a JSON `null` value).
    fn keeps_nulls(self) -> bool {
        matches!(
            self,
            AggFunc::ArrayAgg | AggFunc::JsonbAgg | AggFunc::JsonbObjectAgg
        )
    }
}

/// Classify a (lowercased — the lexer lowercases unquoted idents) function name.
/// `None` means "not a known aggregate" (the caller then tries the scalar-function
/// path / reports an undefined function).
fn aggregate_func(name: &str) -> Option<AggFunc> {
    match name {
        "count" => Some(AggFunc::Count),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        "array_agg" => Some(AggFunc::ArrayAgg),
        "jsonb_agg" => Some(AggFunc::JsonbAgg),
        "jsonb_object_agg" => Some(AggFunc::JsonbObjectAgg),
        _ => None,
    }
}

/// Does `e` (or any subexpression) call a known aggregate function?
pub(crate) fn contains_aggregate(e: &Expr) -> bool {
    match e {
        Expr::Func(fc) => {
            aggregate_func(&fc.name).is_some()
                || match &fc.args {
                    FuncArgs::Star => false,
                    FuncArgs::Exprs(args) => args.iter().any(contains_aggregate),
                }
        }
        Expr::Unary { expr, .. } => contains_aggregate(expr),
        Expr::Binary { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        // SP28: recurse through predicate + conditional expressions.
        Expr::IsNull { expr, .. } => contains_aggregate(expr),
        Expr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        Expr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        Expr::Like { expr, pattern, .. } => contains_aggregate(expr) || contains_aggregate(pattern),
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || whens
                    .iter()
                    .any(|(c, r)| contains_aggregate(c) || contains_aggregate(r))
                || else_result.as_deref().is_some_and(contains_aggregate)
        }
        // SP31: a cast over an aggregate is an aggregate (`sum(x)::int8`).
        Expr::Cast { expr, .. } => contains_aggregate(expr),
        // The array expression forms are aggregates iff a child is
        // (`ARRAY[count(*)]`, `arr[max(i)]`, `x = ANY(array_agg(y))`).
        Expr::ArrayLiteral(items) => items.iter().any(contains_aggregate),
        Expr::Subscript { base, index } => contains_aggregate(base) || contains_aggregate(index),
        Expr::QuantifiedArray { expr, array, .. } => {
            contains_aggregate(expr) || contains_aggregate(array)
        }
        _ => false,
    }
}

/// A `SELECT` is an *aggregate query* iff it groups, has `HAVING`, or any
/// aggregate call appears in the projection or `ORDER BY`.
pub(crate) fn is_aggregate_query(s: &SelectStmt) -> bool {
    !s.group_by.is_empty()
        || s.having.is_some()
        || s.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => contains_aggregate(expr),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
        || s.order_by.iter().any(|o| contains_aggregate(&o.expr))
}

/// Error for a function call reached by scalar `eval` (i.e. NOT a resolved
/// aggregate position): a known aggregate there is misplaced/nested (42803);
/// anything else is an undefined function (42883).
pub(crate) fn func_in_scalar_context_error(fc: &FuncCall) -> ExecError {
    if aggregate_func(&fc.name).is_some() {
        ExecError::Grouping(format!(
            "aggregate function \"{}\" is not allowed here \
             (aggregates cannot be nested)",
            fc.name
        ))
    } else {
        undefined_function(&fc.name)
    }
}

/// The result column type of an aggregate call, for RowDescription — also
/// validating name, arity, and argument type (all mapped to 42883).
pub(crate) fn func_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    let Some(func) = aggregate_func(&fc.name) else {
        return Err(undefined_function(&fc.name));
    };
    match func {
        AggFunc::Count => {
            count_arity(fc)?;
            Ok(ColumnType::Int8) // count(*) / count(x) -> bigint
        }
        AggFunc::Sum => {
            let arg = single_value_arg(fc)?;
            let t = crate::eval::infer_type(arg, scope)?;
            match t {
                // sum(int4)/sum(int8) -> bigint (PG: int8 sums to numeric — a
                // remaining documented deviation). sum(float8) -> float8;
                // SP32: sum(numeric) -> numeric.
                ColumnType::Int4 | ColumnType::Int8 => Ok(ColumnType::Int8),
                ColumnType::Float8 => Ok(ColumnType::Float8),
                _ if t.is_numeric() => Ok(ColumnType::Numeric(None)),
                other => Err(undefined_for_arg("sum", other)),
            }
        }
        // SP32: avg(int)/avg(numeric) -> numeric (exact PG parity now that numeric
        // exists — retiring SP30's float8 deviation); avg(float8) -> float8.
        AggFunc::Avg => {
            let arg = single_value_arg(fc)?;
            let t = crate::eval::infer_type(arg, scope)?;
            match t {
                ColumnType::Float8 => Ok(ColumnType::Float8),
                ColumnType::Int4 | ColumnType::Int8 => Ok(ColumnType::Numeric(None)),
                _ if t.is_numeric() => Ok(ColumnType::Numeric(None)),
                other => Err(undefined_for_arg("avg", other)),
            }
        }
        // min/max preserve the argument's type.
        AggFunc::Min | AggFunc::Max => {
            let arg = single_value_arg(fc)?;
            crate::eval::infer_type(arg, scope)
        }
        // array_agg(x) -> x[]; an element type crabka has no array type for is 0A000.
        AggFunc::ArrayAgg => {
            let arg = single_value_arg(fc)?;
            let t = crate::eval::infer_type(arg, scope)?;
            array_of(t)
        }
        AggFunc::JsonbAgg => {
            single_value_arg(fc)?;
            Ok(ColumnType::Jsonb)
        }
        AggFunc::JsonbObjectAgg => {
            let (key, _) = two_value_args(fc)?;
            crate::eval::infer_type(key, scope)?;
            Ok(ColumnType::Jsonb)
        }
    }
}

/// The array type over `elem`, or 0A000 when crabka has no array type for it.
fn array_of(elem: ColumnType) -> Result<ColumnType, ExecError> {
    ColumnType::array_of(elem).ok_or_else(|| {
        ExecError::Unsupported(format!("arrays of {} are not supported", elem.name()))
    })
}

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

fn undefined_for_arg(name: &str, t: ColumnType) -> ExecError {
    ExecError::UndefinedFunction(format!("function {}({}) does not exist", name, t.name()))
}

/// `count` accepts `*` or exactly one argument.
fn count_arity(fc: &FuncCall) -> Result<(), ExecError> {
    match &fc.args {
        FuncArgs::Star => Ok(()),
        FuncArgs::Exprs(args) if args.len() == 1 => Ok(()),
        _ => Err(undefined_function("count")),
    }
}

/// The single value argument of `sum`/`min`/`max` (and `count(x)`); errors
/// (42883) for the wrong arity or the `*` form.
fn single_value_arg(fc: &FuncCall) -> Result<&Expr, ExecError> {
    match &fc.args {
        FuncArgs::Exprs(args) if args.len() == 1 => Ok(&args[0]),
        _ => Err(undefined_function(&fc.name)),
    }
}

/// The `(key, value)` arguments of `jsonb_object_agg`; 42883 for any other arity.
fn two_value_args(fc: &FuncCall) -> Result<(&Expr, &Expr), ExecError> {
    match &fc.args {
        FuncArgs::Exprs(args) if args.len() == 2 => Ok((&args[0], &args[1])),
        _ => Err(undefined_function(&fc.name)),
    }
}

/// A resolved aggregate to compute: the function, its argument (`None` only for
/// `count(*)`), the argument's static type (SP30 — picks the int vs float
/// accumulator for `sum`/`avg`; `None` for `count(*)`), and whether `DISTINCT`.
/// `PartialEq` lets identical aggregates share a single accumulator (deduped at
/// collection time).
#[derive(Debug, Clone, PartialEq)]
struct AggSpec {
    func: AggFunc,
    arg: Option<Expr>,
    /// `jsonb_object_agg`'s VALUE argument (`arg` is then its key). `None` for
    /// every other aggregate, all of which take at most one argument.
    value_arg: Option<Expr>,
    arg_type: Option<ColumnType>,
    distinct: bool,
}

/// Build the spec for one aggregate call, validating arity, argument type, and
/// the no-nested-aggregate rule.
fn spec_of(fc: &FuncCall, scope: &Scope) -> Result<AggSpec, ExecError> {
    let func = aggregate_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    match func {
        AggFunc::Count => match &fc.args {
            FuncArgs::Star => Ok(AggSpec {
                func,
                arg: None,
                value_arg: None,
                arg_type: None,
                distinct: fc.distinct,
            }),
            FuncArgs::Exprs(args) if args.len() == 1 => {
                reject_nested_aggregate(&args[0])?;
                let arg_type = crate::eval::infer_type(&args[0], scope)?;
                Ok(AggSpec {
                    func,
                    arg: Some(args[0].clone()),
                    value_arg: None,
                    arg_type: Some(arg_type),
                    distinct: fc.distinct,
                })
            }
            _ => Err(undefined_function("count")),
        },
        // The collecting aggregates: one value argument, any type array_agg has
        // an array type for (jsonb_agg accepts every type).
        AggFunc::ArrayAgg | AggFunc::JsonbAgg => {
            let arg = single_value_arg(fc)?;
            reject_nested_aggregate(arg)?;
            let arg_type = crate::eval::infer_type(arg, scope)?;
            if func == AggFunc::ArrayAgg {
                array_of(arg_type)?;
            }
            Ok(AggSpec {
                func,
                arg: Some(arg.clone()),
                value_arg: None,
                arg_type: Some(arg_type),
                distinct: fc.distinct,
            })
        }
        AggFunc::JsonbObjectAgg => {
            let (key, value) = two_value_args(fc)?;
            reject_nested_aggregate(key)?;
            reject_nested_aggregate(value)?;
            // Both arguments sit on PostgreSQL's `"any"` parameter: every scalar
            // key is accepted and rendered through its output function, and only
            // a container key (`jsonb`, an array) is refused — at RUN time, as
            // 22023, which is why a zero-row `jsonb_object_agg(jsonb_col, v)` is
            // NULL rather than an error. Type-check both expressions now so a bad
            // one still fails at plan time.
            let key_type = crate::eval::infer_type(key, scope)?;
            crate::eval::infer_type(value, scope)?;
            Ok(AggSpec {
                func,
                arg: Some(key.clone()),
                value_arg: Some(value.clone()),
                arg_type: Some(key_type),
                distinct: fc.distinct,
            })
        }
        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
            let arg = single_value_arg(fc)?;
            reject_nested_aggregate(arg)?;
            // Type-check the argument now so RowDescription and folding agree.
            let arg_type = crate::eval::infer_type(arg, scope)?;
            // sum/avg accept only numeric arguments (int4/int8/float8/numeric).
            if matches!(func, AggFunc::Sum | AggFunc::Avg)
                && !matches!(
                    arg_type,
                    ColumnType::Int4 | ColumnType::Int8 | ColumnType::Float8
                )
                && !arg_type.is_numeric()
            {
                return Err(undefined_for_arg(&fc.name, arg_type));
            }
            Ok(AggSpec {
                func,
                arg: Some(arg.clone()),
                value_arg: None,
                arg_type: Some(arg_type),
                distinct: fc.distinct,
            })
        }
    }
}

impl AggSpec {
    /// Evaluate this aggregate's argument expressions for one row: `[value]`, or
    /// `[key, value]` for `jsonb_object_agg`. `DISTINCT` compares and
    /// deduplicates the whole tuple, exactly as PostgreSQL does, so the value
    /// argument is evaluated before that decision rather than after it.
    fn eval_args(
        &self,
        scope: &Scope,
        row: &[Datum],
        ctx: &EvalCtx,
    ) -> Result<Vec<Datum>, ExecError> {
        let arg = self
            .arg
            .as_ref()
            .expect("non-star aggregate has an argument");
        let mut args = vec![crate::eval::eval(arg, scope, row, ctx)?];
        if let Some(value) = &self.value_arg {
            args.push(crate::eval::eval(value, scope, row, ctx)?);
        }
        Ok(args)
    }
}

fn reject_nested_aggregate(arg: &Expr) -> Result<(), ExecError> {
    if contains_aggregate(arg) {
        return Err(ExecError::Grouping(
            "aggregate function calls cannot be nested".into(),
        ));
    }
    Ok(())
}

/// Collect (deduped) every aggregate spec in `e`. A non-aggregate function call
/// is an undefined function (42883).
fn collect_specs(e: &Expr, scope: &Scope, specs: &mut Vec<AggSpec>) -> Result<(), ExecError> {
    match e {
        Expr::Func(fc) if aggregate_func(&fc.name).is_some() => {
            let spec = spec_of(fc, scope)?;
            if !specs.contains(&spec) {
                specs.push(spec);
            }
        }
        // SP29/SP37/SP38: a scalar, date/time, or formatting function may wrap
        // aggregates / grouped columns — gather aggregates from its arguments (the
        // call itself is not an aggregate). `max(extract(year from ts))` reaches the
        // aggregate via the outer scalar/agg traversal; this arm handles such a
        // function wrapping an aggregate (`date_trunc('day', max(ts))`,
        // `to_char(max(ts), 'YYYY')`).
        Expr::Func(fc) if is_wrapping_scalar_func(&fc.name) => {
            if let FuncArgs::Exprs(args) = &fc.args {
                for a in args {
                    collect_specs(a, scope, specs)?;
                }
            }
        }
        Expr::Func(fc) => return Err(undefined_function(&fc.name)),
        Expr::Unary { expr, .. } => collect_specs(expr, scope, specs)?,
        Expr::Binary { left, right, .. } => {
            collect_specs(left, scope, specs)?;
            collect_specs(right, scope, specs)?;
        }
        // SP28: gather aggregates appearing inside predicate / CASE expressions.
        Expr::IsNull { expr, .. } => collect_specs(expr, scope, specs)?,
        Expr::InList { expr, list, .. } => {
            collect_specs(expr, scope, specs)?;
            for e in list {
                collect_specs(e, scope, specs)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_specs(expr, scope, specs)?;
            collect_specs(low, scope, specs)?;
            collect_specs(high, scope, specs)?;
        }
        Expr::Like { expr, pattern, .. } => {
            collect_specs(expr, scope, specs)?;
            collect_specs(pattern, scope, specs)?;
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_specs(o, scope, specs)?;
            }
            for (c, r) in whens {
                collect_specs(c, scope, specs)?;
                collect_specs(r, scope, specs)?;
            }
            if let Some(e) = else_result {
                collect_specs(e, scope, specs)?;
            }
        }
        // SP31: gather aggregates from a cast's operand (`avg(x)::int8`).
        Expr::Cast { expr, .. } => collect_specs(expr, scope, specs)?,
        // The array expression forms: gather from every child.
        Expr::ArrayLiteral(items) => {
            for item in items {
                collect_specs(item, scope, specs)?;
            }
        }
        Expr::Subscript { base, index } => {
            collect_specs(base, scope, specs)?;
            collect_specs(index, scope, specs)?;
        }
        Expr::QuantifiedArray { expr, array, .. } => {
            collect_specs(expr, scope, specs)?;
            collect_specs(array, scope, specs)?;
        }
        _ => {}
    }
    Ok(())
}

/// The scalar function families that may WRAP an aggregate or a grouped column:
/// the call itself is not an aggregate, but its arguments must be traversed.
fn is_wrapping_scalar_func(name: &str) -> bool {
    crate::func::is_scalar(name)
        || crate::datetime_fn::is_datetime_func(name)
        || crate::format_fn::is_format_func(name)
        || crate::json_fn::is_json_func(name)
        || crate::array_fn::is_array_func(name)
}

/// Collect (deduped, in first-appearance order) the aggregate calls of one
/// no-GROUP-BY projection expression, verifying the expression is built only
/// from aggregate calls, constants, and scalar / date-time / formatting
/// functions, operators, predicates, `CASE`, and casts over those.
///
/// Returns `false` for anything else — a bare column, an unknown function, a
/// `DISTINCT` aggregate, a parameter, an unresolved subquery — telling the
/// streaming-aggregate path to keep the materializing scan (and its errors)
/// for that query. Aggregate arguments are NOT descended into: they belong to
/// the pushdown spec, and a non-column argument fails spec construction later.
pub(crate) fn collect_streamable_aggregate_calls(e: &Expr, calls: &mut Vec<FuncCall>) -> bool {
    match e {
        Expr::Func(fc) if aggregate_func(&fc.name).is_some() => {
            if fc.distinct {
                return false;
            }
            if !calls.contains(fc) {
                calls.push(fc.clone());
            }
            true
        }
        Expr::Func(fc) if is_wrapping_scalar_func(&fc.name) => match &fc.args {
            FuncArgs::Star => false,
            FuncArgs::Exprs(args) => args
                .iter()
                .all(|arg| collect_streamable_aggregate_calls(arg, calls)),
        },
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Const { .. } => true,
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } | Expr::Cast { expr, .. } => {
            collect_streamable_aggregate_calls(expr, calls)
        }
        Expr::Binary { left, right, .. } => {
            collect_streamable_aggregate_calls(left, calls)
                && collect_streamable_aggregate_calls(right, calls)
        }
        Expr::InList { expr, list, .. } => {
            collect_streamable_aggregate_calls(expr, calls)
                && list
                    .iter()
                    .all(|e| collect_streamable_aggregate_calls(e, calls))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_streamable_aggregate_calls(expr, calls)
                && collect_streamable_aggregate_calls(low, calls)
                && collect_streamable_aggregate_calls(high, calls)
        }
        Expr::Like { expr, pattern, .. } => {
            collect_streamable_aggregate_calls(expr, calls)
                && collect_streamable_aggregate_calls(pattern, calls)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            operand
                .as_deref()
                .is_none_or(|o| collect_streamable_aggregate_calls(o, calls))
                && whens.iter().all(|(condition, result)| {
                    collect_streamable_aggregate_calls(condition, calls)
                        && collect_streamable_aggregate_calls(result, calls)
                })
                && else_result
                    .as_deref()
                    .is_none_or(|e| collect_streamable_aggregate_calls(e, calls))
        }
        // The array expression forms are deliberately NOT streamable: they need
        // the scope-aware evaluator (element-type unification, subscripting),
        // which the streamed fold does not have. The materializing scan handles
        // them — and its errors — unchanged.
        Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Default
        | Expr::Func(_)
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery { .. }
        | Expr::Quantified { .. }
        | Expr::QuantifiedArray { .. }
        | Expr::ArrayLiteral(_)
        | Expr::Subscript { .. } => false,
    }
}

/// Evaluate no-GROUP-BY projection expressions over already-finalized aggregate
/// values: `values[i]` is the result of `calls[i]`. Aggregate calls resolve by
/// spec lookup exactly as in the materializing fold, so a streamed projection
/// evaluates identically to [`aggregate_rows`] fed the same aggregate results.
pub(crate) fn eval_over_aggregate_values(
    exprs: &[Expr],
    scope: &Scope,
    calls: &[FuncCall],
    values: &[Datum],
    ctx: &EvalCtx,
) -> Result<Vec<Datum>, ExecError> {
    let specs = calls
        .iter()
        .map(|call| spec_of(call, scope))
        .collect::<Result<Vec<_>, ExecError>>()?;
    exprs
        .iter()
        .map(|e| eval_grouped(e, scope, &[], &[], &specs, values, ctx))
        .collect()
}

/// Data-independent validation: every projection / `HAVING` / `ORDER BY`
/// expression must be built from aggregate calls, `GROUP BY` expressions, and
/// constants. A bare ungrouped column → 42803 (even on an empty table).
fn validate_grouped(e: &Expr, group_by: &[Expr]) -> Result<(), ExecError> {
    if let Expr::Func(fc) = e
        && aggregate_func(&fc.name).is_some()
    {
        return Ok(()); // an aggregate may reference any column in its argument
    }
    if group_by.iter().any(|g| g == e) {
        return Ok(()); // matches a grouping expression structurally
    }
    match e {
        Expr::Column { name, .. } => Err(ungrouped_column(name)),
        Expr::Unary { expr, .. } => validate_grouped(expr, group_by),
        Expr::Binary { left, right, .. } => {
            validate_grouped(left, group_by)?;
            validate_grouped(right, group_by)
        }
        // SP29/SP37/SP38: every argument of a scalar, date/time, formatting, jsonb,
        // or array function must itself be grouped-valid (the call as a whole, if it
        // matches a GROUP BY key, was already accepted above).
        Expr::Func(fc) if is_wrapping_scalar_func(&fc.name) => {
            if let FuncArgs::Exprs(args) = &fc.args {
                for a in args {
                    validate_grouped(a, group_by)?;
                }
            }
            Ok(())
        }
        Expr::Func(fc) => Err(undefined_function(&fc.name)),
        // SP28: every child of a predicate / CASE must itself be grouped-valid.
        Expr::IsNull { expr, .. } => validate_grouped(expr, group_by),
        Expr::InList { expr, list, .. } => {
            validate_grouped(expr, group_by)?;
            for e in list {
                validate_grouped(e, group_by)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_grouped(expr, group_by)?;
            validate_grouped(low, group_by)?;
            validate_grouped(high, group_by)
        }
        Expr::Like { expr, pattern, .. } => {
            validate_grouped(expr, group_by)?;
            validate_grouped(pattern, group_by)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            if let Some(o) = operand {
                validate_grouped(o, group_by)?;
            }
            for (c, r) in whens {
                validate_grouped(c, group_by)?;
                validate_grouped(r, group_by)?;
            }
            if let Some(e) = else_result {
                validate_grouped(e, group_by)?;
            }
            Ok(())
        }
        // SP31: a cast is grouped-valid iff its operand is (and an entire cast
        // expression matching a GROUP BY key was already accepted above).
        Expr::Cast { expr, .. } => validate_grouped(expr, group_by),
        // The array expression forms are grouped-valid iff every child is.
        Expr::ArrayLiteral(items) => {
            for item in items {
                validate_grouped(item, group_by)?;
            }
            Ok(())
        }
        Expr::Subscript { base, index } => {
            validate_grouped(base, group_by)?;
            validate_grouped(index, group_by)
        }
        Expr::QuantifiedArray { expr, array, .. } => {
            validate_grouped(expr, group_by)?;
            validate_grouped(array, group_by)
        }
        _ => Ok(()), // literals / params are constants
    }
}

fn ungrouped_column(name: &str) -> ExecError {
    ExecError::Grouping(format!(
        "column \"{name}\" must appear in the GROUP BY clause or be used in an aggregate function"
    ))
}

/// Evaluate an expression in a group's context: aggregate calls resolve to their
/// finalized per-group result; subexpressions matching a `GROUP BY` expression
/// resolve to the group key; everything else recurses. (Validation already
/// guarantees no ungrouped column reaches the `Column` arm.)
fn eval_grouped(
    e: &Expr,
    scope: &Scope,
    group_by: &[Expr],
    key: &[Datum],
    specs: &[AggSpec],
    results: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    eval_grouped_depth(
        e,
        &GroupedEvalContext {
            scope,
            group_by,
            key,
            specs,
            results,
            eval_ctx: ctx,
        },
        0,
    )
}

struct GroupedEvalContext<'a> {
    scope: &'a Scope,
    group_by: &'a [Expr],
    key: &'a [Datum],
    specs: &'a [AggSpec],
    results: &'a [Datum],
    eval_ctx: &'a EvalCtx,
}

/// Depth-tracking core of [`eval_grouped`]. Mirrors `eval::eval_depth`: every
/// recursive descent increments `depth`, and exceeding `MAX_GROUPED_DEPTH`
/// returns `54001`. Defense-in-depth — the parser already caps AST depth, so a
/// tree this deep can never reach here in practice.
fn eval_grouped_depth(
    e: &Expr,
    grouped: &GroupedEvalContext<'_>,
    depth: usize,
) -> Result<Datum, ExecError> {
    let GroupedEvalContext {
        scope,
        group_by,
        key,
        specs,
        results,
        eval_ctx: ctx,
    } = grouped;
    if depth > MAX_GROUPED_DEPTH {
        return Err(ExecError::StackDepthExceeded);
    }
    let d = depth + 1;
    if let Expr::Func(fc) = e
        && aggregate_func(&fc.name).is_some()
    {
        let spec = spec_of(fc, scope)?;
        let i = specs
            .iter()
            .position(|s| *s == spec)
            .ok_or_else(|| ExecError::Grouping("aggregate not resolved".into()))?;
        return Ok(results[i].clone());
    }
    if let Some(i) = group_by.iter().position(|g| g == e) {
        return Ok(key[i].clone());
    }
    match e {
        Expr::IntLiteral(s) => Ok(ops::int_literal(s)?),
        Expr::NumericLiteral(s) => crabka_pgtypes::numeric::parse(s)
            .map(Datum::Numeric)
            .ok_or_else(|| {
                ExecError::Type(TypeError::InvalidText {
                    type_name: "numeric",
                    value: s.clone(),
                })
            }),
        Expr::StringLiteral(s) => Ok(Datum::Text(s.clone())),
        Expr::BoolLiteral(b) => Ok(Datum::Bool(*b)),
        Expr::NullLiteral => Ok(Datum::Null),
        Expr::Param(_) => Err(ExecError::Unsupported(
            "query parameters ($n) are not supported".into(),
        )),
        Expr::Default => Err(ExecError::Unsupported(
            "DEFAULT is only supported in INSERT target values".into(),
        )),
        Expr::Column { name, .. } => Err(ungrouped_column(name)),
        Expr::Unary { op, expr } => {
            let v = eval_grouped_depth(expr, grouped, d)?;
            crate::eval::apply_unary(*op, &v, ctx)
        }
        Expr::Binary { op, left, right } => {
            let l = eval_grouped_depth(left, grouped, d)?;
            let r = eval_grouped_depth(right, grouped, d)?;
            crate::eval::apply_binary_of(*op, left, right, &l, &r, scope, ctx)
        }
        // SP28: predicate + conditional expressions in a grouped context — same
        // combinators as scalar `eval`, recursing through `eval_grouped_depth`.
        Expr::IsNull { expr, negated } => {
            let v = eval_grouped_depth(expr, grouped, d)?;
            Ok(Datum::Bool(v.is_null() ^ *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let x = eval_grouped_depth(expr, grouped, d)?;
            crate::eval::eval_in_list(&x, list, *negated, |e| eval_grouped_depth(e, grouped, d))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let x = eval_grouped_depth(expr, grouped, d)?;
            let lo = eval_grouped_depth(low, grouped, d)?;
            let hi = eval_grouped_depth(high, grouped, d)?;
            crate::eval::eval_between(&x, &lo, &hi, *negated, ctx)
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let s = eval_grouped_depth(expr, grouped, d)?;
            let p = eval_grouped_depth(pattern, grouped, d)?;
            crate::eval::eval_like(&s, &p, *negated, *case_insensitive)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => crate::eval::eval_case(operand.as_deref(), whens, else_result.as_deref(), |e| {
            eval_grouped_depth(e, grouped, d)
        }),
        // SP29: a scalar function over grouped/aggregate arguments — evaluate it
        // with the grouped evaluator as its child-eval closure.
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => {
            crate::func::eval_scalar(fc, ctx, |e| eval_grouped_depth(e, grouped, d))
        }
        // SP37: a date/time function over grouped/aggregate arguments (e.g.
        // `date_trunc('day', max(ts))`) — same pattern, grouped child-eval closure.
        Expr::Func(fc) if crate::datetime_fn::is_datetime_func(&fc.name) => {
            crate::datetime_fn::eval_datetime(fc, ctx, |e| eval_grouped_depth(e, grouped, d))
        }
        // SP38: a formatting function over grouped/aggregate arguments (e.g.
        // `to_char(max(ts), 'YYYY')`) — same pattern, grouped child-eval closure.
        Expr::Func(fc) if crate::format_fn::is_format_func(&fc.name) => {
            crate::format_fn::eval_format(fc, ctx, |e| eval_grouped_depth(e, grouped, d))
        }
        // A jsonb or array function over grouped/aggregate arguments (e.g.
        // `jsonb_build_object('n', count(*))`, `array_length(array_agg(x), 1)`).
        Expr::Func(fc) if crate::json_fn::is_json_func(&fc.name) => {
            crate::json_fn::eval_json(fc, ctx, |e| eval_grouped_depth(e, grouped, d))
        }
        Expr::Func(fc) if crate::array_fn::is_array_func(&fc.name) => {
            crate::array_fn::eval_array(fc, ctx, |e| eval_grouped_depth(e, grouped, d))
        }
        Expr::Func(fc) => Err(undefined_function(&fc.name)),
        // SP31: cast in a grouped context — convert the grouped-evaluated operand
        // using the session zone from `ctx`.
        Expr::Cast { expr, ty } => {
            if let Some(empty) = crate::eval::empty_array_cast(expr, *ty) {
                return Ok(empty);
            }
            let v = eval_grouped_depth(expr, grouped, d)?;
            Ok(crabka_pgtypes::cast::cast(&v, *ty, &ctx.time_zone)?)
        }
        // The array expression forms in a grouped context — same semantics as
        // scalar `eval`, recursing through the grouped evaluator.
        Expr::ArrayLiteral(items) => {
            let elem = crate::eval::array_literal_elem_type(items, scope)?;
            let target = elem.column_type();
            let mut elems = Vec::with_capacity(items.len());
            for item in items {
                let v = eval_grouped_depth(item, grouped, d)?;
                elems.push(crabka_pgtypes::cast::cast(&v, target, &ctx.time_zone)?);
            }
            Ok(Datum::Array(ArrayValue::new(elem, elems)))
        }
        Expr::Subscript { base, index } => {
            let b = eval_grouped_depth(base, grouped, d)?;
            let i = eval_grouped_depth(index, grouped, d)?;
            crate::array_fn::array_subscript(&b, &i)
        }
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => {
            let x = eval_grouped_depth(expr, grouped, d)?;
            let a = eval_grouped_depth(array, grouped, d)?;
            crate::array_fn::eval_quantified(&a, crate::eval::quantifier_of(*all), |elem| {
                crate::eval::apply_binary(*op, &x, elem, ctx)
            })
        }
        // SP34: a resolved subquery constant in a grouped context.
        Expr::Const { value, .. } => Ok(value.clone()),
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery { .. }
        | Expr::Quantified { .. } => Err(ExecError::Unsupported(
            "subqueries are only supported in SELECT".into(),
        )),
    }
}

/// One group's running accumulator for one aggregate: the running [`AccState`]
/// plus, for a `DISTINCT` aggregate, the argument tuples it has yet to fold.
///
/// PostgreSQL implements `DISTINCT` by sorting the WHOLE argument tuple and
/// dropping adjacent duplicates, so a `DISTINCT` aggregate buffers its rows and
/// folds them at [`Acc::finish`] instead of on arrival. Two things follow that a
/// fold-on-arrival "first value seen" set gets wrong:
/// `jsonb_object_agg(DISTINCT k, v)` over `('k',1),('k',2)` keeps BOTH pairs (so
/// the object's last value is `2`, not `1`), and `array_agg(DISTINCT x)` /
/// `jsonb_agg(DISTINCT x)` emit sorted — not first-appearance — order.
struct Acc {
    state: AccState,
    /// `Some` iff the spec is `DISTINCT`: each row's evaluated argument tuple.
    distinct: Option<Vec<Vec<Datum>>>,
}

/// The running value of one aggregate. SP30 splits `Sum` into an integer (`SumI`,
/// accumulated in a checked i64 so `sum(int4)` never overflows prematurely) and a
/// float (`SumF`, accumulated in f64) variant, and adds `Avg` (float8 result).
enum AccState {
    Count {
        n: i64,
    },
    SumI {
        acc: Option<i64>,
    },
    SumF {
        acc: f64,
        any: bool,
    },
    /// SP32: numeric sum (exact, no overflow) — accumulated as a numeric `Datum`.
    SumN {
        acc: Option<Datum>,
    },
    MinMax {
        best: Option<Datum>,
    },
    Avg {
        sum: f64,
        n: i64,
    },
    /// SP32: numeric mean — a numeric running sum and a count, divided at finish
    /// with PostgreSQL's `select_div_scale` (so `avg(int)`/`avg(numeric)` are exact).
    AvgN {
        sum: Option<Datum>,
        n: i64,
    },
    /// `array_agg` — the values in fold order, NULLs included. Empty means zero
    /// rows were folded, which is SQL NULL (not an empty array).
    ArrayAgg {
        elem: ElemType,
        elems: Vec<Datum>,
    },
    /// `jsonb_agg` — the values in fold order, converted to JSON at `finish`.
    JsonbAgg {
        items: Vec<Datum>,
    },
    /// `jsonb_object_agg` — the (key, value) pairs in fold order, built into one
    /// object at `finish` (duplicate keys last-wins, a NULL key is 22023).
    JsonbObjectAgg {
        pairs: Vec<(Datum, Datum)>,
    },
}

impl Acc {
    fn new(spec: &AggSpec) -> Acc {
        Acc {
            state: AccState::new(spec),
            distinct: spec.distinct.then(Vec::new),
        }
    }

    /// Fold one source row into this accumulator — or, under `DISTINCT`, buffer
    /// its argument tuple for the sorted fold [`Acc::finish`] performs.
    fn fold_row(
        &mut self,
        spec: &AggSpec,
        scope: &Scope,
        row: &[Datum],
        ctx: &EvalCtx,
    ) -> Result<(), ExecError> {
        // count(*) counts every row, ignoring NULL/DISTINCT.
        if let (AggFunc::Count, None) = (spec.func, &spec.arg) {
            if let AccState::Count { n } = &mut self.state {
                *n += 1;
            }
            return Ok(());
        }
        let args = spec.eval_args(scope, row, ctx)?;
        // count(x)/sum/avg/min/max ignore NULL arguments; the collecting
        // aggregates keep them (NULL array element / JSON `null` value).
        if args[0].is_null() && !spec.func.keeps_nulls() {
            return Ok(());
        }
        match &mut self.distinct {
            Some(tuples) => {
                tuples.push(args);
                Ok(())
            }
            None => self.state.fold_args(spec, &args, ctx),
        }
    }

    /// This aggregate's value for the group: any buffered `DISTINCT` tuples are
    /// sorted, deduplicated, and folded first.
    fn finish(&mut self, spec: &AggSpec, ctx: &EvalCtx) -> Result<Datum, ExecError> {
        if let Some(tuples) = self.distinct.take() {
            for args in sorted_distinct(tuples)? {
                self.state.fold_args(spec, &args, ctx)?;
            }
        }
        self.state.finish(ctx)
    }
}

/// PostgreSQL's `DISTINCT` input for an aggregate: the argument tuples sorted
/// ascending with adjacent duplicates dropped. Sorting is what makes
/// `array_agg(DISTINCT x)` emit ascending order, and comparing (rather than
/// hashing) is what makes `1.0` and `1.00` one `numeric` value.
fn sorted_distinct(mut tuples: Vec<Vec<Datum>>) -> Result<Vec<Vec<Datum>>, ExecError> {
    // `sort_by` needs a total order, so an incomparable pair is recorded and
    // reported once the sort is over rather than panicking inside it.
    let mut failure: Option<ExecError> = None;
    tuples.sort_by(|a, b| match compare_tuples(a, b) {
        Ok(ord) => ord,
        Err(e) => {
            failure.get_or_insert(e);
            Ordering::Equal
        }
    });
    if let Some(e) = failure {
        return Err(e);
    }
    let mut out: Vec<Vec<Datum>> = Vec::with_capacity(tuples.len());
    for tuple in tuples {
        if let Some(prev) = out.last()
            && compare_tuples(prev, &tuple)? == Ordering::Equal
        {
            continue;
        }
        out.push(tuple);
    }
    Ok(out)
}

/// The order `DISTINCT` sorts and deduplicates in: argument by argument,
/// ascending, NULLs last. Two NULLs are EQUAL here — SQL `DISTINCT` folds NULLs
/// together even though `NULL = NULL` is unknown.
fn compare_tuples(a: &[Datum], b: &[Datum]) -> Result<Ordering, ExecError> {
    for (x, y) in a.iter().zip(b) {
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => ops::compare(x, y)?.expect("non-NULL operands compare"),
        };
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

impl AccState {
    fn new(spec: &AggSpec) -> AccState {
        match spec.func {
            AggFunc::Count => AccState::Count { n: 0 },
            AggFunc::Sum => match spec.arg_type {
                Some(ColumnType::Float8) => AccState::SumF {
                    acc: 0.0,
                    any: false,
                },
                Some(t) if t.is_numeric() => AccState::SumN { acc: None },
                _ => AccState::SumI { acc: None },
            },
            // float8 avg stays in f64; int/numeric avg accumulates exactly.
            AggFunc::Avg => {
                if spec.arg_type == Some(ColumnType::Float8) {
                    AccState::Avg { sum: 0.0, n: 0 }
                } else {
                    AccState::AvgN { sum: None, n: 0 }
                }
            }
            AggFunc::Min | AggFunc::Max => AccState::MinMax { best: None },
            // The argument type was validated by `spec_of`, so it has an array
            // element type; `text` is a harmless stand-in for the impossible case.
            AggFunc::ArrayAgg => AccState::ArrayAgg {
                elem: spec
                    .arg_type
                    .and_then(ElemType::from_column_type)
                    .unwrap_or(ElemType::Text),
                elems: Vec::new(),
            },
            AggFunc::JsonbAgg => AccState::JsonbAgg { items: Vec::new() },
            AggFunc::JsonbObjectAgg => AccState::JsonbObjectAgg { pairs: Vec::new() },
        }
    }

    /// Fold one already-evaluated argument tuple (`[value]`, or `[key, value]`
    /// for `jsonb_object_agg`) into the running value.
    fn fold_args(
        &mut self,
        spec: &AggSpec,
        args: &[Datum],
        ctx: &EvalCtx,
    ) -> Result<(), ExecError> {
        let v = args
            .first()
            .cloned()
            .expect("an aggregate argument tuple is never empty");
        match self {
            AccState::Count { n } => *n += 1,
            AccState::SumI { acc } => {
                let add = as_i64(&v).ok_or_else(|| {
                    undefined_for_arg("sum", v.column_type().unwrap_or(ColumnType::Text))
                })?;
                let next = match acc {
                    Some(cur) => cur
                        .checked_add(add)
                        .ok_or(ExecError::Type(TypeError::Overflow))?,
                    None => add,
                };
                *acc = Some(next);
            }
            AccState::SumF { acc, any } => {
                *acc += as_f64(&v).ok_or_else(|| {
                    undefined_for_arg("sum", v.column_type().unwrap_or(ColumnType::Text))
                })?;
                *any = true;
            }
            // SP32: numeric sum/avg accumulate exactly via the numeric ops (sum's
            // scale is the max input scale; avg defers the division to `finish`).
            AccState::SumN { acc } => {
                *acc = Some(match acc.take() {
                    None => v,
                    Some(cur) => ops::add(&cur, &v)?,
                });
            }
            AccState::AvgN { sum, n } => {
                let vn = crabka_pgtypes::cast::cast(&v, ColumnType::Numeric(None), &ctx.time_zone)?;
                *sum = Some(match sum.take() {
                    None => vn,
                    Some(cur) => ops::add(&cur, &vn)?,
                });
                *n += 1;
            }
            AccState::Avg { sum, n } => {
                *sum += as_f64(&v).ok_or_else(|| {
                    undefined_for_arg("avg", v.column_type().unwrap_or(ColumnType::Text))
                })?;
                *n += 1;
            }
            AccState::MinMax { best } => {
                let take = match best {
                    None => true,
                    Some(cur) => {
                        let ord = ops::compare(&v, cur)?; // both non-null
                        matches!(
                            (spec.func, ord),
                            (AggFunc::Min, Some(Ordering::Less))
                                | (AggFunc::Max, Some(Ordering::Greater))
                        )
                    }
                };
                if take {
                    *best = Some(v);
                }
            }
            // The elements are coerced to the accumulator's element type so the
            // array stays homogeneous (`array_agg(int4_col)` over an int8-typed
            // expression cannot arise, but a `numeric` scale can vary).
            AccState::ArrayAgg { elem, elems } => {
                elems.push(crabka_pgtypes::cast::cast(
                    &v,
                    elem.column_type(),
                    &ctx.time_zone,
                )?);
            }
            AccState::JsonbAgg { items } => items.push(v),
            AccState::JsonbObjectAgg { pairs } => {
                let value = args
                    .get(1)
                    .cloned()
                    .expect("jsonb_object_agg has a value argument");
                pairs.push((v, value));
            }
        }
        Ok(())
    }

    fn finish(&self, ctx: &EvalCtx) -> Result<Datum, ExecError> {
        Ok(match self {
            AccState::Count { n } => Datum::Int8(*n),
            AccState::SumI { acc } => acc.map(Datum::Int8).unwrap_or(Datum::Null),
            // An empty / all-null float sum is NULL (matches the integer sum).
            AccState::SumF { acc, any } => {
                if *any {
                    Datum::Float8(*acc)
                } else {
                    Datum::Null
                }
            }
            // SP32: an empty/all-null numeric sum is NULL; else the exact numeric.
            AccState::SumN { acc } => acc.clone().unwrap_or(Datum::Null),
            AccState::MinMax { best } => best.clone().unwrap_or(Datum::Null),
            // avg over zero non-null rows is NULL; otherwise the float8 mean.
            AccState::Avg { sum, n } => {
                if *n == 0 {
                    Datum::Null
                } else {
                    Datum::Float8(*sum / *n as f64)
                }
            }
            // SP32: numeric mean = sum / count, with PostgreSQL's division scale.
            AccState::AvgN { sum, n } => match sum {
                Some(s) if *n > 0 => ops::div(s, &Datum::Int8(*n))?,
                _ => Datum::Null,
            },
            // PostgreSQL's array_agg over zero rows is NULL, NOT an empty array.
            AccState::ArrayAgg { elem, elems } => {
                if elems.is_empty() {
                    Datum::Null
                } else {
                    Datum::Array(ArrayValue::new(*elem, elems.clone()))
                }
            }
            AccState::JsonbAgg { items } => {
                if items.is_empty() {
                    Datum::Null
                } else {
                    build_jsonb("jsonb_build_array", items.clone(), ctx)?
                }
            }
            AccState::JsonbObjectAgg { pairs } => {
                if pairs.is_empty() {
                    Datum::Null
                } else {
                    let mut flat = Vec::with_capacity(pairs.len() * 2);
                    for (key, value) in pairs {
                        flat.push(key.clone());
                        flat.push(value.clone());
                    }
                    build_jsonb("jsonb_build_object", flat, ctx)?
                }
            }
        })
    }
}

/// Build a `jsonb` aggregate's result through the corresponding `json_fn`
/// builder: `jsonb_agg` is exactly the row-wise fold of `jsonb_build_array`, and
/// `jsonb_object_agg` of `jsonb_build_object`. Routing through the builders keeps
/// ONE set of SQL-value → JSON rules (numeric scale, ISO date spelling, JSON
/// `null` for a SQL NULL value, 22023 for a NULL key, last-wins duplicate keys)
/// instead of a second copy that could drift.
fn build_jsonb(builder: &str, args: Vec<Datum>, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let call = FuncCall {
        name: builder.to_string(),
        distinct: false,
        args: FuncArgs::Exprs(
            args.into_iter()
                .map(|value| {
                    let ty = value.column_type().unwrap_or(ColumnType::Text);
                    Expr::Const { value, ty }
                })
                .collect(),
        ),
    };
    crate::json_fn::eval_json(&call, ctx, |e| match e {
        Expr::Const { value, .. } => Ok(value.clone()),
        _ => Err(ExecError::Unsupported(
            "internal: a jsonb aggregate builds only from constants".into(),
        )),
    })
}

fn as_i64(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        _ => None,
    }
}

fn as_f64(d: &Datum) -> Option<f64> {
    match d {
        Datum::Int4(n) => Some(f64::from(*n)),
        Datum::Int8(n) => Some(*n as f64),
        Datum::Float8(f) => Some(*f),
        _ => None,
    }
}

/// Execute an aggregate query over the already-`WHERE`-filtered `rows`, returning
/// the final `QueryResult::Rows`. Thin wrapper over `aggregate_rows` — the row-
/// producing core shared with derived tables (`select_to_relation`). `ctx` carries
/// the session zone + clock for any temporal evaluation; non-temporal aggregation
/// ignores it (UTC/epoch reproduces prior behavior).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn execute_aggregate(
    s: &SelectStmt,
    scope: &Scope,
    rows: Vec<Vec<Datum>>,
    ctx: &EvalCtx,
) -> Result<QueryResult, ExecError> {
    let (fields, _exprs, _tys) = crate::exec::resolve_projection(&s.projection, scope)?;
    let out_rows = aggregate_rows(s, scope, rows, ctx)?;
    Ok(crate::exec::rows_result(fields, &out_rows, &ctx.time_zone))
}

/// Fold an aggregate query over the already-`WHERE`-filtered `rows`, returning the
/// projected output Datum rows (HAVING / DISTINCT / ORDER BY / OFFSET / LIMIT all
/// applied). `execute_aggregate` renders these to a `QueryResult`; a derived table
/// re-qualifies them under its alias.
pub(crate) fn aggregate_rows(
    s: &SelectStmt,
    scope: &Scope,
    rows: Vec<Vec<Datum>>,
    ctx: &EvalCtx,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    // Output columns: the expressions that produce each column via the shared
    // projection resolver (infer_type now understands aggregate result types).
    let (fields, out_exprs, _tys) = crate::exec::resolve_projection(&s.projection, scope)?;
    let order_keys = crate::exec::resolve_select_order_keys(
        &s.order_by,
        scope,
        &fields,
        &out_exprs,
        s.distinct,
    )?;

    // GROUP BY expressions may not themselves be aggregates.
    for g in &s.group_by {
        if contains_aggregate(g) {
            return Err(ExecError::Grouping(
                "aggregate functions are not allowed in GROUP BY".into(),
            ));
        }
    }

    // Collect (deduped) the aggregates to compute, then validate every output /
    // HAVING / ORDER BY expression is grouped-valid (data-independent).
    let mut specs: Vec<AggSpec> = Vec::new();
    let source_order_exprs = order_keys.iter().filter_map(|key| match key {
        crate::exec::SelectOrderKey::Output(_) => None,
        crate::exec::SelectOrderKey::SourceExpr(expr) => Some(expr),
    });
    for e in out_exprs
        .iter()
        .chain(s.having.iter())
        .chain(source_order_exprs)
    {
        collect_specs(e, scope, &mut specs)?;
        validate_grouped(e, &s.group_by)?;
    }

    // Fold rows into groups, preserving first-appearance order.
    let has_group_by = !s.group_by.is_empty();
    let mut keys: Vec<Vec<Datum>> = Vec::new();
    let mut accs: Vec<Vec<Acc>> = Vec::new();
    let mut index: HashMap<Vec<Datum>, usize> = HashMap::new();
    let mut group_bytes = 0usize;
    for row in &rows {
        let mut key = Vec::with_capacity(s.group_by.len());
        for g in &s.group_by {
            key.push(crate::eval::eval(g, scope, row, ctx)?);
        }
        let gi = match index.get(&key) {
            Some(&i) => i,
            None => {
                let bytes = crate::scanner::datum_row_bytes(&key)
                    .saturating_mul(2)
                    .saturating_add(specs.len().saturating_mul(std::mem::size_of::<Acc>()));
                if group_bytes.saturating_add(bytes) > crate::scanner::BLOCKING_QUERY_MEMORY_BYTES {
                    return Err(crate::scanner::memory_budget_exceeded());
                }
                group_bytes += bytes;
                let i = keys.len();
                index.insert(key.clone(), i);
                keys.push(key);
                accs.push(specs.iter().map(Acc::new).collect());
                i
            }
        };
        for (spec, acc) in specs.iter().zip(accs[gi].iter_mut()) {
            acc.fold_row(spec, scope, row, ctx)?;
        }
    }
    // A bare aggregate (no GROUP BY) over zero rows still yields ONE group.
    if !has_group_by && keys.is_empty() {
        keys.push(Vec::new());
        accs.push(specs.iter().map(Acc::new).collect());
    }

    // Finalize each group: HAVING filter, ORDER BY keys, projected output Datums.
    let mut out: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(keys.len());
    for (key, group_accs) in keys.iter().zip(accs.iter_mut()) {
        let results: Vec<Datum> = group_accs
            .iter_mut()
            .zip(&specs)
            .map(|(acc, spec)| acc.finish(spec, ctx))
            .collect::<Result<_, ExecError>>()?;
        if let Some(h) = &s.having {
            match eval_grouped(h, scope, &s.group_by, key, &specs, &results, ctx)? {
                Datum::Bool(true) => {}
                Datum::Bool(false) | Datum::Null => continue,
                _ => {
                    return Err(ExecError::TypeMismatch(
                        "argument of HAVING must be type boolean".into(),
                    ));
                }
            }
        }
        let mut projected = Vec::with_capacity(out_exprs.len());
        for e in &out_exprs {
            projected.push(eval_grouped(
                e,
                scope,
                &s.group_by,
                key,
                &specs,
                &results,
                ctx,
            )?);
        }
        let mut sort_keys = Vec::with_capacity(order_keys.len());
        for order_key in &order_keys {
            sort_keys.push(match order_key {
                crate::exec::SelectOrderKey::Output(i) => projected[*i].clone(),
                crate::exec::SelectOrderKey::SourceExpr(expr) => {
                    eval_grouped(expr, scope, &s.group_by, key, &specs, &results, ctx)?
                }
            });
        }
        out.push((sort_keys, projected));
    }

    // SP28: SELECT DISTINCT dedups identical projected rows (first appearance).
    if s.distinct {
        let mut seen: HashSet<Vec<Datum>> = HashSet::new();
        out.retain(|(_, proj)| seen.insert(proj.clone()));
    }
    if !s.order_by.is_empty() {
        out.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, &s.order_by));
    }
    // SP28: OFFSET then LIMIT.
    crate::exec::apply_offset_limit(&mut out, s.offset, s.limit);

    Ok(out.into_iter().map(|(_, proj)| proj).collect())
}

#[cfg(test)]
mod tests {
    use crabka_pgcatalog::{Column, Table};
    use crabka_pgparser::ast::{QueryBody, SelectStmt, SetExpr, Statement};
    use crabka_pgwire::engine::Cell;

    use super::*;

    fn table() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![
                Column::new("k", ColumnType::Int4),
                Column::new("v", ColumnType::Int4),
            ],
            sharded: false,
            sharding: None,
            foreign: None,
        }
    }

    /// The table's single-relation scope, or the empty (FROM-less) scope.
    fn scope_of(t: Option<&Table>) -> Scope {
        match t {
            Some(t) => Scope::single(t, &t.name),
            None => Scope::empty(),
        }
    }

    fn parsed_select(sql: &str) -> SelectStmt {
        match crabka_pgparser::parse(sql)
            .expect("parse")
            .pop()
            .expect("one")
        {
            Statement::Query(q) => match q.body {
                SetExpr::Query(QueryBody::Select(s)) => {
                    let mut s = *s;
                    s.order_by = q.order_by;
                    s.limit = q.limit;
                    s.offset = q.offset;
                    s.locking = q.locking;
                    s
                }
                other => panic!("expected select body, got {other:?}"),
            },
            other => panic!("expected select, got {other:?}"),
        }
    }

    /// Parse one SELECT and run it over the given (already-WHERE-filtered) rows.
    fn agg(
        sql: &str,
        t: Option<&Table>,
        rows: Vec<Vec<Datum>>,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        let s = parsed_select(sql);
        assert!(
            is_aggregate_query(&s),
            "test sql must be an aggregate query"
        );
        let ctx = crate::clock::EvalCtx::test_default();
        match execute_aggregate(&s, &scope_of(t), rows, &ctx)? {
            QueryResult::Rows { rows, .. } => Ok(rows
                .into_iter()
                .map(|r| r.into_iter().map(cell_to_datum).collect())
                .collect()),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// Decode a result cell back to a (typed-enough) Datum for assertions: we
    /// compare text-format payloads, so map back to Text/Null and ints by parse.
    fn cell_to_datum(c: Option<Cell>) -> Datum {
        match c {
            None => Datum::Null,
            Some(cell) => {
                let s = String::from_utf8(cell.text.to_vec()).expect("utf8");
                match s.parse::<i64>() {
                    Ok(n) => Datum::Int8(n),
                    Err(_) => Datum::Text(s),
                }
            }
        }
    }

    /// Like `agg`, but returns the raw text-format cells (so float results — which
    /// `cell_to_datum` cannot round-trip cleanly — can be asserted directly).
    fn agg_text(
        sql: &str,
        t: Option<&Table>,
        rows: Vec<Vec<Datum>>,
    ) -> Result<Vec<Vec<Option<String>>>, ExecError> {
        let s = parsed_select(sql);
        let ctx = crate::clock::EvalCtx::test_default();
        match execute_aggregate(&s, &scope_of(t), rows, &ctx)? {
            QueryResult::Rows { rows, .. } => Ok(rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|c| c.map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8")))
                        .collect()
                })
                .collect()),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn r(vals: &[Datum]) -> Vec<Datum> {
        vals.to_vec()
    }

    fn float_table() -> Table {
        let mut t = table();
        t.columns[1].ty = ColumnType::Float8;
        t
    }

    fn int(n: i64) -> Datum {
        Datum::Int8(n)
    }

    #[test]
    fn count_star_counts_all_rows_including_nulls() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Null]),
            r(&[Datum::Int4(2), Datum::Int4(30)]),
        ];
        assert_eq!(
            agg("SELECT count(*) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![int(3)]]
        );
    }

    #[test]
    fn count_and_sum_ignore_nulls() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Null]),
            r(&[Datum::Int4(1), Datum::Int4(5)]),
        ];
        // count(v) = 2 (nulls skipped); sum(v) = 15.
        assert_eq!(
            agg("SELECT count(v), sum(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![int(2), int(15)]]
        );
    }

    #[test]
    fn min_max_over_text_and_int() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Text;
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Text("b".into())]),
            r(&[Datum::Int4(1), Datum::Text("a".into())]),
            r(&[Datum::Int4(1), Datum::Text("c".into())]),
        ];
        assert_eq!(
            agg("SELECT min(v), max(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![Datum::Text("a".into()), Datum::Text("c".into())]]
        );
    }

    #[test]
    fn count_distinct_dedups_non_null() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(7)]),
            r(&[Datum::Int4(1), Datum::Int4(7)]),
            r(&[Datum::Int4(1), Datum::Int4(8)]),
            r(&[Datum::Int4(1), Datum::Null]),
        ];
        assert_eq!(
            agg(
                "SELECT count(DISTINCT v), sum(DISTINCT v) FROM t",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![vec![int(2), int(15)]]
        );
    }

    #[test]
    fn group_by_groups_with_null_as_its_own_group() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(20)]),
            r(&[Datum::Int4(1), Datum::Int4(5)]),
            r(&[Datum::Null, Datum::Int4(99)]),
        ];
        // ORDER BY k makes output deterministic; NULLS LAST for ASC.
        let got = agg(
            "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k",
            Some(&t),
            rows,
        )
        .expect("agg");
        assert_eq!(
            got,
            vec![
                vec![int(1), int(2), int(15)],
                vec![int(2), int(1), int(20)],
                vec![Datum::Null, int(1), int(99)], // the NULL group, last
            ]
        );
    }

    #[test]
    fn having_filters_groups() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(20)]),
        ];
        // only k=1 has count(*) > 1.
        assert_eq!(
            agg(
                "SELECT k FROM t GROUP BY k HAVING count(*) > 1 ORDER BY k",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![vec![int(1)]]
        );
    }

    #[test]
    fn grouping_expression_in_projection() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Int4(20)]),
        ];
        // k+1 is built from the grouping column k -> valid.
        assert_eq!(
            agg("SELECT k + 1, sum(v) FROM t GROUP BY k", Some(&t), rows).expect("agg"),
            vec![vec![int(2), int(30)]]
        );
    }

    #[test]
    fn bare_aggregate_over_empty_table_yields_one_row() {
        let t = table();
        // count = 0, sum/min/max = NULL.
        assert_eq!(
            agg("SELECT count(*), sum(v), min(v) FROM t", Some(&t), vec![]).expect("agg"),
            vec![vec![int(0), Datum::Null, Datum::Null]]
        );
    }

    #[test]
    fn grouped_over_empty_table_yields_zero_rows() {
        let t = table();
        assert!(
            agg("SELECT k, count(*) FROM t GROUP BY k", Some(&t), vec![])
                .expect("agg")
                .is_empty()
        );
    }

    #[test]
    fn ungrouped_column_is_42803() {
        let t = table();
        let err =
            agg("SELECT v, count(*) FROM t GROUP BY k", Some(&t), vec![]).expect_err("ungrouped v");
        assert_eq!(err.into_pg().code, "42803");
    }

    #[test]
    fn unknown_function_is_42883() {
        let t = table();
        // Not an aggregate query unless an aggregate is present, so pair with count(*).
        let s = parsed_select("SELECT frobnicate(v), count(*) FROM t GROUP BY v");
        let err = execute_aggregate(
            &s,
            &scope_of(Some(&t)),
            vec![],
            &crate::clock::EvalCtx::test_default(),
        )
        .expect_err("unknown fn");
        assert_eq!(err.into_pg().code, "42883");
    }

    #[test]
    fn sum_of_text_is_42883() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Text;
        let s = parsed_select("SELECT sum(v) FROM t");
        let err = execute_aggregate(
            &s,
            &scope_of(Some(&t)),
            vec![],
            &crate::clock::EvalCtx::test_default(),
        )
        .expect_err("sum(text)");
        assert_eq!(err.into_pg().code, "42883");
    }

    #[test]
    fn nested_aggregate_is_42803() {
        let t = table();
        let s = parsed_select("SELECT sum(count(v)) FROM t");
        let err = execute_aggregate(
            &s,
            &scope_of(Some(&t)),
            vec![],
            &crate::clock::EvalCtx::test_default(),
        )
        .expect_err("nested");
        assert_eq!(err.into_pg().code, "42803");
    }

    #[test]
    fn sum_overflow_is_22003() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Int8;
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int8(i64::MAX)]),
            r(&[Datum::Int4(1), Datum::Int8(1)]),
        ];
        let err = agg("SELECT sum(v) FROM t", Some(&t), rows).expect_err("overflow");
        assert_eq!(err.into_pg().code, "22003");
    }

    #[test]
    fn count_star_no_from_is_one() {
        // SELECT count(*) with no FROM -> one (empty) row folded -> 1, like PG.
        assert_eq!(
            agg("SELECT count(*)", None, vec![vec![]]).expect("agg"),
            vec![vec![int(1)]]
        );
    }

    #[test]
    fn aggregate_result_types_are_inferred_for_row_description() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Text;
        let s = parsed_select("SELECT count(*), sum(k), min(v), max(k) FROM t GROUP BY k");
        let (fields, _, _) =
            crate::exec::resolve_projection(&s.projection, &scope_of(Some(&t))).expect("fields");
        // count -> int8, sum(int4) -> int8, min(text) -> text, max(int4) -> int4
        assert_eq!(fields[0].type_oid, ColumnType::Int8.oid());
        assert_eq!(fields[1].type_oid, ColumnType::Int8.oid());
        assert_eq!(fields[2].type_oid, ColumnType::Text.oid());
        assert_eq!(fields[3].type_oid, ColumnType::Int4.oid());
        assert_eq!(fields[0].name, "count");
    }

    // ---- SP28: predicate / CASE expressions in a grouped context ----

    #[test]
    fn case_in_having_filters_groups() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(20)]),
        ];
        // A CASE over an aggregate in HAVING keeps only k=1 (count(*) > 1).
        assert_eq!(
            agg(
                "SELECT k FROM t GROUP BY k \
                 HAVING CASE WHEN count(*) > 1 THEN true ELSE false END ORDER BY k",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![vec![int(1)]]
        );
    }

    #[test]
    fn in_list_over_grouped_column_projection() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(20)]),
            r(&[Datum::Int4(3), Datum::Int4(30)]),
        ];
        // `k IN (1, 3)` is built from the grouping column -> valid; bool text "t"/"f".
        assert_eq!(
            agg(
                "SELECT k IN (1, 3) FROM t GROUP BY k ORDER BY k",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![
                vec![Datum::Text("t".into())],
                vec![Datum::Text("f".into())],
                vec![Datum::Text("t".into())],
            ]
        );
    }

    #[test]
    fn ungrouped_column_inside_case_is_42803() {
        let t = table();
        // `v` is neither grouped nor aggregated, even nested inside a CASE.
        let err = agg(
            "SELECT CASE WHEN v > 0 THEN 1 ELSE 0 END FROM t GROUP BY k",
            Some(&t),
            vec![],
        )
        .expect_err("ungrouped v in CASE");
        assert_eq!(err.into_pg().code, "42803");
    }

    #[test]
    fn distinct_aggregate_output_dedups() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(10)]),
            r(&[Datum::Int4(3), Datum::Int4(20)]),
        ];
        // Per-group sum(v) is {10, 10, 20}; SELECT DISTINCT collapses to {10, 20}.
        assert_eq!(
            agg(
                "SELECT DISTINCT sum(v) FROM t GROUP BY k ORDER BY sum(v)",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![vec![int(10)], vec![int(20)]]
        );
    }

    #[test]
    fn aggregate_order_by_position_and_alias_use_projected_output() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(40)]),
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(3), Datum::Int4(30)]),
        ];

        assert_eq!(
            agg(
                "SELECT k, sum(v) AS total FROM t GROUP BY k ORDER BY 2 DESC",
                Some(&t),
                rows.clone()
            )
            .expect("ordinal ORDER BY"),
            vec![
                vec![int(2), int(40)],
                vec![int(3), int(30)],
                vec![int(1), int(20)],
            ]
        );
        assert_eq!(
            agg(
                "SELECT k, sum(v) AS total FROM t GROUP BY k ORDER BY total DESC",
                Some(&t),
                rows.clone()
            )
            .expect("aggregate alias ORDER BY"),
            vec![
                vec![int(2), int(40)],
                vec![int(3), int(30)],
                vec![int(1), int(20)],
            ]
        );
        assert_eq!(
            agg(
                "SELECT k AS g, sum(v) FROM t GROUP BY k ORDER BY g DESC",
                Some(&t),
                rows
            )
            .expect("group alias ORDER BY"),
            vec![
                vec![int(3), int(30)],
                vec![int(2), int(40)],
                vec![int(1), int(20)],
            ]
        );
    }

    #[test]
    fn aggregate_distinct_order_by_requires_output() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(40)]),
            r(&[Datum::Int4(3), Datum::Int4(30)]),
        ];

        assert_eq!(
            agg(
                "SELECT DISTINCT sum(v) AS total FROM t GROUP BY k ORDER BY total DESC",
                Some(&t),
                rows.clone()
            )
            .expect("DISTINCT aggregate alias ORDER BY"),
            vec![vec![int(40)], vec![int(30)], vec![int(10)]]
        );

        assert_eq!(
            agg(
                "SELECT DISTINCT k FROM t GROUP BY k ORDER BY t.k DESC",
                Some(&t),
                rows.clone()
            )
            .expect("qualified DISTINCT aggregate output ORDER BY"),
            vec![vec![int(3)], vec![int(2)], vec![int(1)]]
        );

        let err = agg(
            "SELECT DISTINCT sum(v) FROM t GROUP BY k ORDER BY k",
            Some(&t),
            rows,
        )
        .expect_err("DISTINCT ORDER BY source column");
        let pg = err.into_pg();
        assert_eq!(pg.code, "42P10");
        assert_eq!(
            pg.message,
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
        );
    }

    // ---- SP30: float8 aggregates (avg, and sum/min/max over float8) ----

    #[test]
    fn avg_over_float8_is_the_float_mean() {
        let t = float_table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Float8(1.0)]),
            r(&[Datum::Int4(1), Datum::Float8(2.0)]),
            r(&[Datum::Int4(1), Datum::Null]), // NULL skipped
        ];
        assert_eq!(
            agg_text("SELECT avg(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![Some("1.5".to_string())]]
        );
    }

    #[test]
    fn avg_over_integers_returns_numeric() {
        // SP32: avg(int) is now numeric (exact PG parity — retires SP30's float8
        // deviation), with PostgreSQL's division display scale (16 places for 3/2).
        let t = table(); // v is int4
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(1)]),
            r(&[Datum::Int4(1), Datum::Int4(2)]),
        ];
        assert_eq!(
            agg_text("SELECT avg(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![Some("1.5000000000000000".to_string())]]
        );
        // avg over zero rows is NULL.
        assert_eq!(
            agg_text("SELECT avg(v) FROM t", Some(&t), vec![]).expect("agg"),
            vec![vec![None]]
        );
    }

    #[test]
    fn sum_min_max_over_float8() {
        let t = float_table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Float8(1.5)]),
            r(&[Datum::Int4(1), Datum::Float8(2.0)]),
            r(&[Datum::Int4(1), Datum::Float8(-0.5)]),
        ];
        assert_eq!(
            agg_text("SELECT sum(v), min(v), max(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![
                Some("3".to_string()), // 1.5 + 2.0 - 0.5 = 3.0 → "3"
                Some("-0.5".to_string()),
                Some("2".to_string()),
            ]]
        );
    }

    #[test]
    fn float8_result_types_for_row_description() {
        let t = float_table();
        let s = parsed_select("SELECT avg(v), sum(v), min(v) FROM t");
        let (fields, _, _) =
            crate::exec::resolve_projection(&s.projection, &scope_of(Some(&t))).expect("fields");
        assert_eq!(fields[0].type_oid, ColumnType::Float8.oid()); // avg(float8)
        assert_eq!(fields[1].type_oid, ColumnType::Float8.oid()); // sum(float8)
        assert_eq!(fields[2].type_oid, ColumnType::Float8.oid()); // min(float8)
        // SP32: avg(int) now types as numeric (1700) for RowDescription.
        let it = table();
        let s = parsed_select("SELECT avg(v) FROM t");
        let (fields, _, _) =
            crate::exec::resolve_projection(&s.projection, &scope_of(Some(&it))).expect("fields");
        assert_eq!(fields[0].type_oid, ColumnType::Numeric(None).oid());
        assert_eq!(fields[0].name, "avg");
    }

    #[test]
    fn group_by_and_distinct_over_float8_keys() {
        let t = float_table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Float8(1.5)]),
            r(&[Datum::Int4(2), Datum::Float8(1.5)]),
            r(&[Datum::Int4(3), Datum::Float8(2.5)]),
        ];
        // GROUP BY a float-valued expression groups equal floats together.
        assert_eq!(
            agg_text(
                "SELECT v, count(*) FROM t GROUP BY v ORDER BY v",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![
                vec![Some("1.5".to_string()), Some("2".to_string())],
                vec![Some("2.5".to_string()), Some("1".to_string())],
            ]
        );
    }

    #[test]
    fn avg_of_text_is_42883() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Text;
        let s = parsed_select("SELECT avg(v) FROM t");
        let err = execute_aggregate(
            &s,
            &scope_of(Some(&t)),
            vec![],
            &crate::clock::EvalCtx::test_default(),
        )
        .expect_err("avg(text)");
        assert_eq!(err.into_pg().code, "42883");
    }

    #[test]
    fn is_aggregate_query_detection() {
        fn sel(sql: &str) -> SelectStmt {
            parsed_select(sql)
        }
        assert!(is_aggregate_query(&sel("SELECT count(*) FROM t")));
        assert!(is_aggregate_query(&sel("SELECT k FROM t GROUP BY k")));
        assert!(is_aggregate_query(&sel(
            "SELECT 1 FROM t HAVING count(*) > 0"
        )));
        assert!(is_aggregate_query(&sel(
            "SELECT k FROM t ORDER BY count(*)"
        )));
        assert!(!is_aggregate_query(&sel("SELECT k, v FROM t")));
        assert!(!is_aggregate_query(&sel(
            "SELECT k FROM t WHERE v > 1 ORDER BY k"
        )));
    }

    // ---- SP38: format functions compose with aggregates ----

    /// A table with a `timestamp` column `ts` (and the int key `k`), for the
    /// format-function-over-aggregate composition tests.
    fn ts_table() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![
                Column::new("k", ColumnType::Int4),
                Column::new("ts", ColumnType::Timestamp),
            ],
            sharded: false,
            sharding: None,
            foreign: None,
        }
    }

    fn ts(s: &str) -> Datum {
        Datum::Timestamp(crabka_pgtypes::datetime::parse_timestamp(s).expect("timestamp"))
    }

    #[test]
    fn format_func_wrapping_an_aggregate_composes() {
        // `to_char(max(ts), 'YYYY')`: the format function WRAPS an aggregate.
        // `ts` lives inside `max(...)`, so it must NOT raise 42803 ("must appear in
        // GROUP BY"); the aggregate is collected + computed, and `to_char` is applied
        // to each group's max. Two groups: k=1 max year 2024, k=2 max year 2025.
        let t = ts_table();
        let rows = vec![
            r(&[Datum::Int4(1), ts("2020-03-04 00:00:00")]),
            r(&[Datum::Int4(1), ts("2024-12-31 23:59:59")]),
            r(&[Datum::Int4(2), ts("2025-01-01 00:00:00")]),
        ];
        assert_eq!(
            agg_text(
                "SELECT k, to_char(max(ts), 'YYYY') FROM t GROUP BY k ORDER BY k",
                Some(&t),
                rows
            )
            .expect("format func over aggregate"),
            vec![
                vec![Some("1".into()), Some("2024".into())],
                vec![Some("2".into()), Some("2025".into())],
            ]
        );
    }

    #[test]
    fn aggregate_wrapping_a_format_func_composes() {
        // `max(to_char(ts, 'YYYY'))`: an aggregate WRAPS a format function — the
        // `to_char(ts, 'YYYY')` text is the aggregated argument. max over the
        // year-text {"2020", "2024", "2025"} is "2025".
        let t = ts_table();
        let rows = vec![
            r(&[Datum::Int4(1), ts("2020-03-04 00:00:00")]),
            r(&[Datum::Int4(1), ts("2024-12-31 23:59:59")]),
            r(&[Datum::Int4(1), ts("2025-01-01 00:00:00")]),
        ];
        assert_eq!(
            agg_text("SELECT max(to_char(ts, 'YYYY')) FROM t", Some(&t), rows)
                .expect("aggregate over format func"),
            vec![vec![Some("2025".into())]]
        );
    }

    // ---- the collecting aggregates: array_agg / jsonb_agg / jsonb_object_agg ----

    /// A relation whose rows feed the collecting aggregates.
    fn collect_table() -> Table {
        Table {
            id: 3,
            name: "t".into(),
            columns: vec![
                Column::new("k", ColumnType::Int4),
                Column::new("v", ColumnType::Int4),
                Column::new("s", ColumnType::Text),
            ],
            sharded: false,
            sharding: None,
            foreign: None,
        }
    }

    fn collect_rows() -> Vec<Vec<Datum>> {
        vec![
            r(&[Datum::Int4(1), Datum::Int4(30), Datum::Text("c".into())]),
            r(&[Datum::Int4(1), Datum::Int4(10), Datum::Text("a".into())]),
            r(&[Datum::Int4(2), Datum::Null, Datum::Text("b".into())]),
        ]
    }

    /// The result types are what RowDescription reports, and follow the argument.
    #[test]
    fn collecting_aggregates_infer_their_result_types() {
        let t = collect_table();
        let scope = scope_of(Some(&t));
        let infer = |sql: &str| {
            let s = parsed_select(sql);
            let SelectItem::Expr { expr, .. } = &s.projection[0] else {
                panic!("expected an expression projection")
            };
            crate::eval::infer_type(expr, &scope)
        };
        assert2::assert!(
            infer("SELECT array_agg(v) FROM t").expect("infer")
                == ColumnType::Array(ElemType::Int4)
        );
        assert2::assert!(
            infer("SELECT array_agg(s) FROM t").expect("infer")
                == ColumnType::Array(ElemType::Text)
        );
        assert2::assert!(infer("SELECT jsonb_agg(v) FROM t").expect("infer") == ColumnType::Jsonb);
        assert2::assert!(
            infer("SELECT jsonb_object_agg(s, v) FROM t").expect("infer") == ColumnType::Jsonb
        );
        // A scalar key of any type is accepted (PostgreSQL's `"any"` parameter),
        // so an integer key plans exactly as a text one does.
        assert2::assert!(
            infer("SELECT jsonb_object_agg(v, s) FROM t").expect("infer") == ColumnType::Jsonb
        );
        // The wrong arity is still 42883 at plan time.
        assert2::assert!(
            infer("SELECT jsonb_object_agg(s) FROM t")
                .expect_err("wrong arity")
                .into_pg()
                .code
                == "42883"
        );
        assert2::assert!(
            infer("SELECT array_agg(v, s) FROM t")
                .expect_err("wrong arity")
                .into_pg()
                .code
                == "42883"
        );
    }

    /// The three aggregates accumulate in INPUT order and keep NULL inputs (a
    /// NULL array element, and the JSON `null` literal) — unlike sum/min/max.
    #[test]
    fn collecting_aggregates_accumulate_in_input_order_keeping_nulls() {
        let t = collect_table();
        let cases: &[(&str, &str)] = &[
            ("SELECT array_agg(v) FROM t", "{30,10,NULL}"),
            ("SELECT array_agg(s) FROM t", "{c,a,b}"),
            ("SELECT jsonb_agg(v) FROM t", "[30, 10, null]"),
            (
                "SELECT jsonb_object_agg(s, v) FROM t",
                r#"{"a": 10, "b": null, "c": 30}"#,
            ),
        ];
        for (sql, want) in cases {
            assert2::assert!(
                agg_text(sql, Some(&t), collect_rows()).expect("aggregate")
                    == vec![vec![Some((*want).to_string())]],
                "for {sql}"
            );
        }
    }

    /// Over ZERO rows every collecting aggregate is SQL NULL — `array_agg` in
    /// particular is NULL, not an empty array (PostgreSQL's behavior).
    #[test]
    fn collecting_aggregates_over_zero_rows_are_null() {
        let t = collect_table();
        for sql in [
            "SELECT array_agg(v) FROM t",
            "SELECT jsonb_agg(v) FROM t",
            "SELECT jsonb_object_agg(s, v) FROM t",
        ] {
            assert2::assert!(
                agg_text(sql, Some(&t), Vec::new()).expect("aggregate") == vec![vec![None]],
                "for {sql}"
            );
        }
        // count(*) still reports 0 for the same zero-row group, so the NULL is
        // the aggregate's own empty value, not a missing group.
        assert2::assert!(
            agg_text("SELECT count(*), array_agg(v) FROM t", Some(&t), Vec::new())
                .expect("aggregate")
                == vec![vec![Some("0".into()), None]]
        );
    }

    /// The collecting aggregates group like every other aggregate, and compose
    /// with the array/jsonb function families around them.
    #[test]
    fn collecting_aggregates_group_and_compose() {
        let t = collect_table();
        assert2::assert!(
            agg_text(
                "SELECT k, array_agg(s) FROM t GROUP BY k ORDER BY k",
                Some(&t),
                collect_rows()
            )
            .expect("grouped")
                == vec![
                    vec![Some("1".into()), Some("{c,a}".into())],
                    vec![Some("2".into()), Some("{b}".into())],
                ]
        );
        // An array function OVER the aggregate, and a subscript of it.
        assert2::assert!(
            agg_text(
                "SELECT cardinality(array_agg(s)) FROM t",
                Some(&t),
                collect_rows()
            )
            .expect("wrapped")
                == vec![vec![Some("3".into())]]
        );
        assert2::assert!(
            agg_text("SELECT array_agg(s)[2] FROM t", Some(&t), collect_rows())
                .expect("subscripted")
                == vec![vec![Some("a".into())]]
        );
        // A jsonb function over an aggregate value.
        assert2::assert!(
            agg_text(
                "SELECT jsonb_build_object('n', count(*)) FROM t",
                Some(&t),
                collect_rows()
            )
            .expect("jsonb over aggregate")
                == vec![vec![Some(r#"{"n": 3}"#.to_string())]]
        );
    }

    /// A NULL `jsonb_object_agg` KEY is an error (22023), not a dropped pair.
    #[test]
    fn jsonb_object_agg_rejects_a_null_key() {
        let t = collect_table();
        let rows = vec![r(&[Datum::Int4(1), Datum::Int4(10), Datum::Null])];
        let err =
            agg_text("SELECT jsonb_object_agg(s, v) FROM t", Some(&t), rows).expect_err("null key");
        assert2::assert!(err.into_pg().code == "22023");
    }

    /// `DISTINCT` sorts the WHOLE argument tuple and drops adjacent duplicates,
    /// as PostgreSQL does: the collecting aggregates emit ascending — not
    /// first-appearance — order, and `jsonb_object_agg(DISTINCT k, v)` keeps
    /// every distinct PAIR, so a repeated key still takes its last value. Each
    /// expected row is PostgreSQL 18.4's output over the same rows.
    #[test]
    fn distinct_sorts_and_dedups_the_whole_argument_tuple() {
        let t = collect_table();
        // Ascending in neither aggregated column, with `a` carrying two values
        // and one row duplicated outright.
        let rows = || {
            vec![
                r(&[Datum::Int4(1), Datum::Int4(30), Datum::Text("c".into())]),
                r(&[Datum::Int4(1), Datum::Int4(10), Datum::Text("a".into())]),
                r(&[Datum::Int4(1), Datum::Int4(20), Datum::Text("a".into())]),
                r(&[Datum::Int4(1), Datum::Int4(30), Datum::Text("c".into())]),
            ]
        };
        for (sql, expected) in [
            ("SELECT array_agg(DISTINCT v) FROM t", "{10,20,30}"),
            ("SELECT array_agg(DISTINCT s) FROM t", "{a,c}"),
            ("SELECT jsonb_agg(DISTINCT s) FROM t", r#"["a", "c"]"#),
            (
                "SELECT jsonb_object_agg(DISTINCT s, v) FROM t",
                r#"{"a": 20, "c": 30}"#,
            ),
            (
                "SELECT jsonb_object_agg(s, v) FROM t",
                r#"{"a": 20, "c": 30}"#,
            ),
        ] {
            assert2::assert!(
                agg_text(sql, Some(&t), rows()).expect(sql)
                    == vec![vec![Some(expected.to_string())]],
                "{sql}"
            );
        }
        // The order-insensitive aggregates still see each distinct value once.
        assert2::assert!(
            agg_text(
                "SELECT count(DISTINCT v), sum(DISTINCT v) FROM t",
                Some(&t),
                rows()
            )
            .expect("count/sum")
                == vec![vec![Some("3".to_string()), Some("60".to_string())]]
        );
    }

    /// `jsonb_object_agg`'s key is PostgreSQL's `"any"` parameter: every scalar
    /// type is accepted and rendered through its output function. Only a
    /// container key is refused, and — like a NULL key — it is refused at RUN
    /// time as 22023, so a zero-row aggregate over one is NULL, not an error.
    /// Each expected object is PostgreSQL 18.4's output over the same rows.
    #[test]
    fn jsonb_object_agg_accepts_any_scalar_key() {
        let t = collect_table();
        let rows = || {
            vec![
                r(&[Datum::Int4(1), Datum::Int4(30), Datum::Text("c".into())]),
                r(&[Datum::Int4(1), Datum::Int4(10), Datum::Text("a".into())]),
                r(&[Datum::Int4(1), Datum::Int4(20), Datum::Text("a".into())]),
                r(&[Datum::Int4(1), Datum::Int4(30), Datum::Text("c".into())]),
            ]
        };
        let ints = r#"{"10": "a", "20": "a", "30": "c"}"#;
        for (sql, expected) in [
            ("SELECT jsonb_object_agg(v, s) FROM t", ints),
            ("SELECT jsonb_object_agg(v::int8, s) FROM t", ints),
            ("SELECT jsonb_object_agg(v::float8, s) FROM t", ints),
            ("SELECT jsonb_object_agg(v::text, s) FROM t", ints),
        ] {
            assert2::assert!(
                agg_text(sql, Some(&t), rows()).expect(sql)
                    == vec![vec![Some(expected.to_string())]],
                "{sql}"
            );
        }
        // A container key is 22023 — and only once a row reaches it.
        for sql in [
            "SELECT jsonb_object_agg(to_jsonb(v), s) FROM t",
            "SELECT jsonb_object_agg(ARRAY[v], s) FROM t",
        ] {
            assert2::assert!(
                agg_text(sql, Some(&t), rows())
                    .expect_err(sql)
                    .into_pg()
                    .code
                    == "22023",
                "{sql}"
            );
            assert2::assert!(
                agg_text(sql, Some(&t), Vec::new()).expect(sql) == vec![vec![None]],
                "{sql}"
            );
        }
    }
}
