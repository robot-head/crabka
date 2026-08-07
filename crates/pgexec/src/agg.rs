//! SP27: aggregate functions + `GROUP BY` / `HAVING`.
//!
//! A whole table belongs to a single range, through
//! `RangeMap::range_for_table`, so an aggregate query executes entirely inside
//! one `execute_read` on one engine. This module is therefore a pure,
//! deterministic fold over the already-correct MVCC-visible row set. It adds no
//! cross-range scatter/gather, no new lock, no new visibility rule and no new
//! interleaving. See the SP27 design doc for why this single-range, pure-data
//! feature needs no Stateright model.
//!
//! Supported: `COUNT(*)`, `COUNT(x)`, `SUM(x)`, `MIN(x)`, `MAX(x)`, their
//! `DISTINCT` forms, multi-key `GROUP BY`, and `HAVING`. `AVG` is deferred until
//! a `numeric`/float type exists.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall, SelectItem, SelectStmt};
use crabka_pgtypes::{
    ColumnType, Datum, ElemType, TypeError, json::Layout, numeric::NumericValue, ops,
};
use crabka_pgwire::engine::QueryResult;

use crate::{clock::EvalCtx, error::ExecError, scope::Scope};

/// Maximum expression-tree depth the grouped evaluator (`eval_grouped_depth`)
/// will recurse before it returns `54001` (statement_too_complex).
///
/// This limit mirrors `eval::MAX_EVAL_DEPTH`. It gives 3x headroom over the
/// parser's parse-time AST depth cap of 50, and it stays below the test-thread
/// overflow point. It is defense-in-depth behind the parser cap, because a tree
/// this deep can never reach here in practice.
const MAX_GROUPED_DEPTH: usize = 150;

/// The aggregate functions crabgresql supports.
///
/// SP30 added `Avg`, which returns float8 because there is no `numeric`, and
/// float8 support for `Sum`/`Min`/`Max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// `array_agg(x)`, the inputs as a one-dimensional array, in input order.
    ArrayAgg,
    /// `jsonb_agg(x)`, the inputs as a JSON array, in input order.
    JsonbAgg,
    /// `jsonb_object_agg(key, value)`, the inputs as a JSON object.
    JsonbObjectAgg,
    /// `json_agg(x)` — [`JsonbAgg`](AggFunc::JsonbAgg)'s `json` twin. Not an
    /// alias: the result is `json`, so it keeps input order, keeps duplicate
    /// keys, and inlines a `json` input verbatim.
    JsonAgg,
    /// `json_object_agg(key, value)` — [`JsonbObjectAgg`](AggFunc::JsonbObjectAgg)'s
    /// `json` twin. `jsonb` collapses a repeated key last-wins; this emits every
    /// pair it was given, in fold order.
    JsonObjectAgg,
    /// `string_agg(value, delimiter)` — the values joined by the delimiter.
    StringAgg,
    /// `bool_and(b)` / `every(b)`, true when no input is false.
    BoolAnd,
    /// `bool_or(b)`, true when some input is true.
    BoolOr,
    /// `bit_and`/`bit_or`/`bit_xor`, the integer inputs folded bitwise.
    BitAnd,
    BitOr,
    BitXor,
    RangeAgg,
    RangeIntersect,
    /// The single-variable statistical family. `variance` is `var_samp` and
    /// `stddev` is `stddev_samp`, exactly as PostgreSQL aliases them.
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
    /// The two-variable statistical family, all `float8`-in / `float8`-out and
    /// all written `f(Y, X)`.
    Corr,
    CovarPop,
    CovarSamp,
    RegrCount,
    RegrSxx,
    RegrSyy,
    RegrSxy,
    RegrAvgx,
    RegrAvgy,
    RegrSlope,
    RegrIntercept,
    RegrR2,
}

impl AggFunc {
    /// Does this aggregate take `(Y, X)` instead of a single value?
    fn is_two_variable(self) -> bool {
        matches!(
            self,
            AggFunc::Corr
                | AggFunc::CovarPop
                | AggFunc::CovarSamp
                | AggFunc::RegrCount
                | AggFunc::RegrSxx
                | AggFunc::RegrSyy
                | AggFunc::RegrSxy
                | AggFunc::RegrAvgx
                | AggFunc::RegrAvgy
                | AggFunc::RegrSlope
                | AggFunc::RegrIntercept
                | AggFunc::RegrR2
        )
    }

    /// The single-variable statistical members, as `(sample, take the root)`.
    fn variance_shape(self) -> Option<(bool, bool)> {
        Some(match self {
            AggFunc::VarPop => (false, false),
            AggFunc::VarSamp => (true, false),
            AggFunc::StddevPop => (false, true),
            AggFunc::StddevSamp => (true, true),
            _ => return None,
        })
    }
}

impl AggFunc {
    /// Do NULL inputs contribute a row?
    ///
    /// `count`/`sum`/`avg`/`min`/`max` skip them. The collecting aggregates
    /// keep them, and a NULL becomes a NULL array element or a JSON `null`
    /// value.
    fn keeps_nulls(self) -> bool {
        matches!(
            self,
            AggFunc::ArrayAgg
                | AggFunc::JsonbAgg
                | AggFunc::JsonbObjectAgg
                | AggFunc::JsonAgg
                | AggFunc::JsonObjectAgg
        )
    }

    /// Does this aggregate build `json` rather than `jsonb`? The two families
    /// share every arity, argument and NULL rule and differ only in the type
    /// they return and therefore in how they render.
    fn is_json(self) -> bool {
        matches!(self, AggFunc::JsonAgg | AggFunc::JsonObjectAgg)
    }
}

/// Is `name` one of the aggregates this engine implements?
///
/// An `OVER` clause on a call is legal for a window function or an aggregate
/// and nothing else. The window planner therefore asks this function to tell
/// `PostgreSQL`'s 42809, a real function used with `OVER`, from its 42883, no
/// such function.
pub(crate) fn is_aggregate_name(name: &str) -> bool {
    aggregate_func(name).is_some()
}

/// Classify a lowercased function name. The lexer lowercases unquoted idents.
///
/// `None` means "not a known aggregate". The caller then tries the
/// scalar-function path or reports an undefined function.
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
        "json_agg" => Some(AggFunc::JsonAgg),
        "json_object_agg" => Some(AggFunc::JsonObjectAgg),
        "string_agg" => Some(AggFunc::StringAgg),
        // `every` is SQL's spelling of `bool_and`; `variance`/`stddev` are
        // PostgreSQL's historical aliases for the SAMPLE forms.
        "bool_and" | "every" => Some(AggFunc::BoolAnd),
        "bool_or" => Some(AggFunc::BoolOr),
        "bit_and" => Some(AggFunc::BitAnd),
        "bit_or" => Some(AggFunc::BitOr),
        "bit_xor" => Some(AggFunc::BitXor),
        "range_agg" => Some(AggFunc::RangeAgg),
        "range_intersect_agg" => Some(AggFunc::RangeIntersect),
        "var_pop" => Some(AggFunc::VarPop),
        "var_samp" | "variance" => Some(AggFunc::VarSamp),
        "stddev_pop" => Some(AggFunc::StddevPop),
        "stddev_samp" | "stddev" => Some(AggFunc::StddevSamp),
        "corr" => Some(AggFunc::Corr),
        "covar_pop" => Some(AggFunc::CovarPop),
        "covar_samp" => Some(AggFunc::CovarSamp),
        "regr_count" => Some(AggFunc::RegrCount),
        "regr_sxx" => Some(AggFunc::RegrSxx),
        "regr_syy" => Some(AggFunc::RegrSyy),
        "regr_sxy" => Some(AggFunc::RegrSxy),
        "regr_avgx" => Some(AggFunc::RegrAvgx),
        "regr_avgy" => Some(AggFunc::RegrAvgy),
        "regr_slope" => Some(AggFunc::RegrSlope),
        "regr_intercept" => Some(AggFunc::RegrIntercept),
        "regr_r2" => Some(AggFunc::RegrR2),
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
        Expr::ArrayLiteral(items) | Expr::Row(items) => items.iter().any(contains_aggregate),
        Expr::Subscript { base, index } => contains_aggregate(base) || contains_aggregate(index),
        Expr::QuantifiedArray { expr, array, .. } => {
            contains_aggregate(expr) || contains_aggregate(array)
        }
        _ => false,
    }
}

/// A `SELECT` is an *aggregate query* if and only if it groups, has `HAVING`,
/// or has an aggregate call in the projection or `ORDER BY`.
pub(crate) fn is_aggregate_query(s: &SelectStmt) -> bool {
    !s.group_by.is_empty()
        || s.having.is_some()
        || s.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => contains_aggregate(expr),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
        || s.order_by.iter().any(|o| contains_aggregate(&o.expr))
}

/// Error for a function call reached by scalar `eval`, which is NOT a resolved
/// aggregate position.
///
/// A known aggregate there is misplaced or nested, which is 42803. Every other
/// call is an undefined function, which is 42883.
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

/// The result column type of an aggregate call, for RowDescription.
///
/// This function also validates the name, the arity and the argument type. It
/// maps all three failures to 42883.
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
                // sum(int2)/sum(int4)/sum(int8) -> bigint (PG: int8 sums to
                // numeric — a remaining documented deviation). sum(float4) ->
                // real and sum(float8) -> float8, each accumulating at its own
                // width; SP32: sum(numeric) -> numeric.
                ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 => Ok(ColumnType::Int8),
                ColumnType::Float4 => Ok(ColumnType::Float4),
                ColumnType::Float8 => Ok(ColumnType::Float8),
                // `sum(money)` accumulates in `money`'s own minor units and
                // raises `money out of range`, not `bigint out of range`, on
                // overflow.
                ColumnType::Money => Ok(ColumnType::Money),
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
                // `avg(float4)` is `float8` in PostgreSQL — unlike `sum(float4)`,
                // which stays `real`.
                ColumnType::Float4 | ColumnType::Float8 => Ok(ColumnType::Float8),
                ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 => {
                    Ok(ColumnType::Numeric(None))
                }
                _ if t.is_numeric() => Ok(ColumnType::Numeric(None)),
                other => Err(undefined_for_arg("avg", other)),
            }
        }
        // min/max preserve the argument's type.
        AggFunc::Min | AggFunc::Max => {
            let arg = single_value_arg(fc)?;
            let ty = crate::eval::infer_type(arg, scope)?;
            // `min`/`max` need a btree opclass, and `xid`/`cid` have none, so
            // PostgreSQL simply declares no `min(xid)` aggregate — the error is
            // the missing *function*, not a missing operator.
            if crate::eval::is_scalar_jsonpath(ty)
                || crate::eval::is_uncomparable_scalar(ty)
                || crate::eval::has_no_btree_opclass(ty)
            {
                return Err(undefined_for_arg(&fc.name, ty));
            }
            Ok(ty)
        }
        // array_agg(x) -> x[]; an element type crabka has no array type for is 0A000.
        // `array_agg(anyarray)` stacks its inputs as the outer dimension of one
        // array of the SAME type, so an array argument keeps its own type.
        AggFunc::ArrayAgg => {
            let arg = single_value_arg(fc)?;
            let t = crate::eval::infer_type(arg, scope)?;
            if t.array_element().is_some() {
                Ok(t)
            } else {
                array_of(t)
            }
        }
        AggFunc::JsonbAgg | AggFunc::JsonAgg => {
            single_value_arg(fc)?;
            Ok(json_result_type(func))
        }
        AggFunc::JsonbObjectAgg | AggFunc::JsonObjectAgg => {
            let (key, _) = two_value_args(fc)?;
            crate::eval::infer_type(key, scope)?;
            Ok(json_result_type(func))
        }
        // `string_agg(text, text)` and `string_agg(bytea, bytea)`; the value
        // argument picks the overload and is also the result type.
        AggFunc::StringAgg => {
            let (value, _) = two_value_args(fc)?;
            match crate::eval::infer_type(value, scope)? {
                ColumnType::Bytea => Ok(ColumnType::Bytea),
                ColumnType::Text => Ok(ColumnType::Text),
                other => Err(undefined_for_arg("string_agg", other)),
            }
        }
        AggFunc::BoolAnd | AggFunc::BoolOr => {
            let arg = single_value_arg(fc)?;
            match crate::eval::infer_type(arg, scope)? {
                ColumnType::Bool => Ok(ColumnType::Bool),
                other => Err(undefined_for_arg(&fc.name, other)),
            }
        }
        // The bitwise aggregates keep the integer width they were given.
        AggFunc::BitAnd | AggFunc::BitOr | AggFunc::BitXor => {
            let arg = single_value_arg(fc)?;
            match crate::eval::infer_type(arg, scope)? {
                t @ (ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8) => Ok(t),
                other => Err(undefined_for_arg(&fc.name, other)),
            }
        }
        AggFunc::RangeAgg | AggFunc::RangeIntersect => {
            let arg = single_value_arg(fc)?;
            match crate::eval::infer_type(arg, scope)? {
                ColumnType::Range(range) if func == AggFunc::RangeAgg => {
                    ColumnType::multirange_for_range(range)
                        .ok_or_else(|| undefined_for_arg(&fc.name, ColumnType::Range(range)))
                }
                ty @ ColumnType::Range(_) => Ok(ty),
                ty @ ColumnType::Multirange(_) => Ok(ty),
                other => Err(undefined_for_arg(&fc.name, other)),
            }
        }
        // var_pop/var_samp/stddev_pop/stddev_samp: float8 in, float8 out; every
        // other numeric width accumulates exactly and yields numeric.
        AggFunc::VarPop | AggFunc::VarSamp | AggFunc::StddevPop | AggFunc::StddevSamp => {
            let arg = single_value_arg(fc)?;
            let t = crate::eval::infer_type(arg, scope)?;
            match t {
                ColumnType::Float4 | ColumnType::Float8 => Ok(ColumnType::Float8),
                ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 => {
                    Ok(ColumnType::Numeric(None))
                }
                _ if t.is_numeric() => Ok(ColumnType::Numeric(None)),
                other => Err(undefined_for_arg(&fc.name, other)),
            }
        }
        // The two-variable family is float8-only; `regr_count` alone returns int8.
        AggFunc::RegrCount => {
            let (y, x) = two_value_args(fc)?;
            require_float_arg(&fc.name, y, scope)?;
            require_float_arg(&fc.name, x, scope)?;
            Ok(ColumnType::Int8)
        }
        _ => {
            let (y, x) = two_value_args(fc)?;
            require_float_arg(&fc.name, y, scope)?;
            require_float_arg(&fc.name, x, scope)?;
            Ok(ColumnType::Float8)
        }
    }
}

/// The two-variable statistical aggregates take `float8` parameters. Every
/// numeric width reaches them through PostgreSQL's implicit widening cast.
fn require_float_arg(name: &str, arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    let t = crate::eval::infer_type(arg, scope)?;
    if matches!(
        t,
        ColumnType::Int2
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Float4
            | ColumnType::Float8
    ) || t.is_numeric()
    {
        Ok(())
    } else {
        Err(undefined_for_arg(name, t))
    }
}

/// The array type over `elem`, or 0A000 when crabka has no array type for it.
fn array_of(elem: ColumnType) -> Result<ColumnType, ExecError> {
    ColumnType::array_of(elem).ok_or_else(|| {
        ExecError::Unsupported(format!("arrays of {} are not supported", elem.name()))
    })
}

fn undefined_function(name: &str) -> ExecError {
    // Shared with the scalar surface so `merge_action()` outside a MERGE
    // RETURNING list reports PostgreSQL's misuse error from every dispatch path.
    crate::func::undefined_function(name)
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

/// The single value argument of `sum`/`min`/`max` and `count(x)`.
///
/// This function reports 42883 for the wrong arity and for the `*` form.
fn single_value_arg(fc: &FuncCall) -> Result<&Expr, ExecError> {
    match &fc.args {
        FuncArgs::Exprs(args) if args.len() == 1 => Ok(&args[0]),
        _ => Err(undefined_function(&fc.name)),
    }
}

/// The type a JSON-building aggregate returns — the ONLY thing that separates
/// `json_agg` from `jsonb_agg` at plan time, since the two share their arity and
/// accept the same arguments.
fn json_result_type(func: AggFunc) -> ColumnType {
    if func.is_json() {
        ColumnType::Json
    } else {
        ColumnType::Jsonb
    }
}

/// The `(key, value)` arguments of `jsonb_object_agg`; 42883 for any other arity.
fn two_value_args(fc: &FuncCall) -> Result<(&Expr, &Expr), ExecError> {
    match &fc.args {
        FuncArgs::Exprs(args) if args.len() == 2 => Ok((&args[0], &args[1])),
        _ => Err(undefined_function(&fc.name)),
    }
}

/// A resolved aggregate to compute.
///
/// The spec holds the function, its argument, which is `None` only for
/// `count(*)`, the argument's static type, and whether the call is `DISTINCT`.
/// SP30 uses the static type to pick the int or the float accumulator for
/// `sum`/`avg`, and that type is `None` for `count(*)`. `PartialEq` lets
/// identical aggregates share a single accumulator, deduplicated at collection
/// time.
#[derive(Debug, Clone, PartialEq)]
struct AggSpec {
    func: AggFunc,
    arg: Option<Expr>,
    /// `jsonb_object_agg`'s VALUE argument, where `arg` is then its key.
    ///
    /// This field is `None` for every other aggregate, and all of those take at
    /// most one argument.
    value_arg: Option<Expr>,
    arg_type: Option<ColumnType>,
    distinct: bool,
    /// `agg(...) FILTER (WHERE predicate)`.
    ///
    /// The predicate is evaluated per source row before the argument is read,
    /// so a row the predicate rejects never reaches the accumulator and never
    /// joins the `DISTINCT` buffer.
    filter: Option<Expr>,
}

/// Build the spec for one aggregate call.
///
/// This function validates the arity, the argument type and the
/// no-nested-aggregate rule.
fn spec_of(fc: &FuncCall, scope: &Scope) -> Result<AggSpec, ExecError> {
    let func = aggregate_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    // A FILTER predicate resolves in the same scope as the arguments: it must be
    // boolean, and — like an argument — it may not contain an aggregate itself.
    if let Some(predicate) = &fc.filter {
        if contains_aggregate(predicate) {
            return Err(ExecError::FunctionError {
                sqlstate: "42803",
                message: "aggregate functions are not allowed in FILTER".into(),
            });
        }
        // A bare NULL is `unknown`, which PostgreSQL coerces to boolean — the
        // predicate is then never true, so every row is rejected.
        if !matches!(predicate.as_ref(), Expr::NullLiteral) {
            let ty = crate::eval::infer_type(predicate, scope)?;
            if ty != ColumnType::Bool {
                return Err(ExecError::FunctionError {
                    sqlstate: "42804",
                    message: format!(
                        "argument of FILTER must be type boolean, not type {}",
                        ty.name()
                    ),
                });
            }
        }
    }
    let spec = match func {
        AggFunc::Count => match &fc.args {
            FuncArgs::Star => Ok(AggSpec {
                func,
                arg: None,
                value_arg: None,
                arg_type: None,
                distinct: fc.distinct,
                filter: fc.filter.as_deref().cloned(),
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
                    filter: fc.filter.as_deref().cloned(),
                })
            }
            _ => Err(undefined_function("count")),
        },
        // The collecting aggregates: one value argument, any type array_agg has
        // an array type for (jsonb_agg accepts every type).
        AggFunc::ArrayAgg | AggFunc::JsonbAgg | AggFunc::JsonAgg => {
            let arg = single_value_arg(fc)?;
            reject_nested_aggregate(arg)?;
            let arg_type = crate::eval::infer_type(arg, scope)?;
            // An array argument aggregates into one more dimension of the SAME
            // array type, so it needs no array type of its own.
            if func == AggFunc::ArrayAgg && arg_type.array_element().is_none() {
                array_of(arg_type)?;
            }
            Ok(AggSpec {
                func,
                arg: Some(arg.clone()),
                value_arg: None,
                arg_type: Some(arg_type),
                distinct: fc.distinct,
                filter: fc.filter.as_deref().cloned(),
            })
        }
        AggFunc::JsonbObjectAgg | AggFunc::JsonObjectAgg => {
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
                filter: fc.filter.as_deref().cloned(),
            })
        }
        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
            let arg = single_value_arg(fc)?;
            reject_nested_aggregate(arg)?;
            // Type-check the argument now so RowDescription and folding agree.
            let arg_type = crate::eval::infer_type(arg, scope)?;
            if matches!(func, AggFunc::Min | AggFunc::Max)
                && crate::eval::is_scalar_jsonpath(arg_type)
            {
                return Err(undefined_for_arg(&fc.name, arg_type));
            }
            // sum/avg accept only numeric arguments (int4/int8/float8/numeric),
            // plus — for `sum` alone — `money`: PostgreSQL has `sum(money)` but
            // deliberately no `avg(money)`.
            let accepts = matches!(
                arg_type,
                ColumnType::Int2
                    | ColumnType::Int4
                    | ColumnType::Int8
                    | ColumnType::Float4
                    | ColumnType::Float8
            ) || arg_type.is_numeric()
                || (func == AggFunc::Sum && arg_type == ColumnType::Money);
            if matches!(func, AggFunc::Sum | AggFunc::Avg) && !accepts {
                return Err(undefined_for_arg(&fc.name, arg_type));
            }
            Ok(AggSpec {
                func,
                arg: Some(arg.clone()),
                value_arg: None,
                arg_type: Some(arg_type),
                distinct: fc.distinct,
                filter: fc.filter.as_deref().cloned(),
            })
        }
        // `string_agg(value, delimiter)` and the two-variable statistical
        // family all take a second argument, which the spec carries in the same
        // `value_arg` slot `jsonb_object_agg` uses.
        AggFunc::StringAgg => {
            let (value, delimiter) = two_value_args(fc)?;
            reject_nested_aggregate(value)?;
            reject_nested_aggregate(delimiter)?;
            let arg_type = crate::eval::infer_type(value, scope)?;
            if !matches!(arg_type, ColumnType::Text | ColumnType::Bytea) {
                return Err(undefined_for_arg("string_agg", arg_type));
            }
            crate::eval::infer_type(delimiter, scope)?;
            Ok(AggSpec {
                func,
                arg: Some(value.clone()),
                value_arg: Some(delimiter.clone()),
                arg_type: Some(arg_type),
                distinct: fc.distinct,
                filter: fc.filter.as_deref().cloned(),
            })
        }
        _ if func.is_two_variable() => {
            let (y, x) = two_value_args(fc)?;
            reject_nested_aggregate(y)?;
            reject_nested_aggregate(x)?;
            require_float_arg(&fc.name, y, scope)?;
            require_float_arg(&fc.name, x, scope)?;
            Ok(AggSpec {
                func,
                arg: Some(y.clone()),
                value_arg: Some(x.clone()),
                arg_type: Some(ColumnType::Float8),
                distinct: fc.distinct,
                filter: fc.filter.as_deref().cloned(),
            })
        }
        // The remaining single-argument aggregates: the boolean pair, the
        // bitwise trio, and the single-variable statistical family.
        _ => {
            let arg = single_value_arg(fc)?;
            reject_nested_aggregate(arg)?;
            let arg_type = crate::eval::infer_type(arg, scope)?;
            // `func_result_type` owns the accept/reject rule for each family;
            // running it here keeps plan-time typing and folding in step.
            func_result_type(fc, scope)?;
            Ok(AggSpec {
                func,
                arg: Some(arg.clone()),
                value_arg: None,
                arg_type: Some(arg_type),
                distinct: fc.distinct,
                filter: fc.filter.as_deref().cloned(),
            })
        }
    }?;
    if spec.distinct {
        if let Some(ty) = spec.arg_type {
            crate::eval::require_equality_operator(ty)?;
        }
        if let Some(value) = &spec.value_arg {
            crate::eval::require_equality_operator(crate::eval::infer_type(value, scope)?)?;
        }
    }
    Ok(spec)
}

impl AggSpec {
    /// Evaluate this aggregate's argument expressions for one row.
    ///
    /// The result is `[value]`, or `[key, value]` for `jsonb_object_agg`.
    /// `DISTINCT` compares and deduplicates the whole tuple, exactly as
    /// PostgreSQL does, so this function evaluates the value argument before
    /// that decision and not after it.
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

/// Collect every aggregate spec in `e`, deduplicated.
///
/// A non-aggregate function call is an undefined function, which is 42883.
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
        Expr::Func(fc)
            if is_wrapping_scalar_func(&fc.name)
                || crate::routine::is_plpgsql_scalar_runtime(fc, scope) =>
        {
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
        Expr::ArrayLiteral(items) | Expr::Row(items) => {
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

/// The scalar function families that may WRAP an aggregate or a grouped column.
///
/// The call itself is not an aggregate, but the caller must traverse its
/// arguments.
fn is_wrapping_scalar_func(name: &str) -> bool {
    crate::func::is_scalar(name)
        || crate::datetime_fn::is_datetime_func(name)
        || crate::format_fn::is_format_func(name)
        || crate::json_fn::is_json_func(name)
        || crate::array_fn::is_array_func(name)
}

/// Collect the aggregate calls of one no-GROUP-BY projection expression,
/// deduplicated and in first-appearance order.
///
/// This function also verifies that the expression is built only from aggregate
/// calls, constants, and scalar, date-time and formatting functions, operators,
/// predicates, `CASE`, and casts over those.
///
/// The function returns `false` for every other expression, such as a bare
/// column, an unknown function, a `DISTINCT` aggregate, a parameter or an
/// unresolved subquery. A `false` result tells the streaming-aggregate path to
/// keep the materializing scan, and its errors, for that query. This function
/// does NOT descend into aggregate arguments. They belong to the pushdown spec,
/// and a non-column argument fails spec construction later.
pub(crate) fn collect_streamable_aggregate_calls(e: &Expr, calls: &mut Vec<FuncCall>) -> bool {
    match e {
        // A SQL/JSON expression is not streamable: its operands may contain
        // aggregates, which the streaming path does not rebuild.
        Expr::SqlJson(_) => false,
        Expr::Func(fc) if aggregate_func(&fc.name).is_some() => {
            // A FILTER predicate has to be evaluated per source row, which the
            // streaming path does not do — it would silently aggregate every row.
            // Fall back to the general path, which applies the predicate.
            if fc.distinct || fc.filter.is_some() {
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
        | Expr::BitStringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Const { .. } => true,
        Expr::FieldSelect { base, .. } | Expr::FieldSelectAll(base) => {
            collect_streamable_aggregate_calls(base, calls)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Collate { expr, .. } => collect_streamable_aggregate_calls(expr, calls),
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
        // The array and row expression forms are deliberately NOT streamable:
        // they need the scope-aware evaluator (element-type unification,
        // subscripting, the session zone for a row's text form), which the
        // streamed fold does not have. The materializing scan handles them —
        // and its errors — unchanged.
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
        | Expr::ArraySubquery(_)
        | Expr::Row(_)
        | Expr::Subscript { .. }
        | Expr::ArrayRef { .. } => false,
    }
}

/// Evaluate no-GROUP-BY projection expressions over already-finalized aggregate
/// values, where `values[i]` is the result of `calls[i]`.
///
/// Aggregate calls resolve by spec lookup exactly as in the materializing fold.
/// A streamed projection therefore evaluates identically to [`aggregate_rows`]
/// over the same aggregate results.
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

/// Data-independent validation.
///
/// Every projection, `HAVING` and `ORDER BY` expression must be built from
/// aggregate calls, `GROUP BY` expressions and constants. A bare ungrouped
/// column is 42803, even on an empty table.
///
/// Both `e` and `group_by` have had their column references canonicalized
/// against the input scope by [`aggregate_rows`], so the structural comparison
/// below matches `t.a` against a bare `a` naming the same column.
fn validate_grouped(e: &Expr, group_by: &[Expr], scope: &Scope) -> Result<(), ExecError> {
    if let Expr::Func(fc) = e
        && aggregate_func(&fc.name).is_some()
    {
        return Ok(()); // an aggregate may reference any column in its argument
    }
    if group_by.iter().any(|g| g == e) {
        return Ok(()); // matches a grouping expression structurally
    }
    match e {
        Expr::Column { table, name } => Err(ungrouped_column(table.as_deref(), name)),
        Expr::Unary { expr, .. } => validate_grouped(expr, group_by, scope),
        Expr::Binary { left, right, .. } => {
            validate_grouped(left, group_by, scope)?;
            validate_grouped(right, group_by, scope)
        }
        // SP29/SP37/SP38: every argument of a scalar, date/time, formatting, jsonb,
        // or array function must itself be grouped-valid (the call as a whole, if it
        // matches a GROUP BY key, was already accepted above).
        Expr::Func(fc)
            if is_wrapping_scalar_func(&fc.name)
                || crate::routine::is_plpgsql_scalar_runtime(fc, scope) =>
        {
            if let FuncArgs::Exprs(args) = &fc.args {
                for a in args {
                    validate_grouped(a, group_by, scope)?;
                }
            }
            Ok(())
        }
        Expr::Func(fc) => Err(undefined_function(&fc.name)),
        // SP28: every child of a predicate / CASE must itself be grouped-valid.
        Expr::IsNull { expr, .. } => validate_grouped(expr, group_by, scope),
        Expr::InList { expr, list, .. } => {
            validate_grouped(expr, group_by, scope)?;
            for e in list {
                validate_grouped(e, group_by, scope)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_grouped(expr, group_by, scope)?;
            validate_grouped(low, group_by, scope)?;
            validate_grouped(high, group_by, scope)
        }
        Expr::Like { expr, pattern, .. } => {
            validate_grouped(expr, group_by, scope)?;
            validate_grouped(pattern, group_by, scope)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            if let Some(o) = operand {
                validate_grouped(o, group_by, scope)?;
            }
            for (c, r) in whens {
                validate_grouped(c, group_by, scope)?;
                validate_grouped(r, group_by, scope)?;
            }
            if let Some(e) = else_result {
                validate_grouped(e, group_by, scope)?;
            }
            Ok(())
        }
        // SP31: a cast is grouped-valid iff its operand is (and an entire cast
        // expression matching a GROUP BY key was already accepted above).
        Expr::Cast { expr, .. } => validate_grouped(expr, group_by, scope),
        // The array expression forms are grouped-valid iff every child is.
        Expr::ArrayLiteral(items) | Expr::Row(items) => {
            for item in items {
                validate_grouped(item, group_by, scope)?;
            }
            Ok(())
        }
        Expr::Subscript { base, index } => {
            validate_grouped(base, group_by, scope)?;
            validate_grouped(index, group_by, scope)
        }
        Expr::QuantifiedArray { expr, array, .. } => {
            validate_grouped(expr, group_by, scope)?;
            validate_grouped(array, group_by, scope)
        }
        _ => Ok(()), // literals / params are constants
    }
}

/// `PostgreSQL` names the offending column by its range-table alias. It writes
/// `gs.b` and never a bare `b`, so the qualifier the canonicalized reference
/// carries is part of the message.
fn ungrouped_column(qualifier: Option<&str>, name: &str) -> ExecError {
    let column = qualifier.map_or_else(|| name.to_string(), |table| format!("{table}.{name}"));
    ExecError::Grouping(format!(
        "column \"{column}\" must appear in the GROUP BY clause or be used in an aggregate function"
    ))
}

/// Evaluate an expression in a group's context.
///
/// Aggregate calls resolve to their finalized per-group result. Subexpressions
/// that match a `GROUP BY` expression resolve to the group key. Everything else
/// recurses. Validation already guarantees that no ungrouped column reaches the
/// `Column` arm.
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

/// Depth-tracking core of [`eval_grouped`].
///
/// This function mirrors `eval::eval_depth`. Every recursive descent increments
/// `depth`, and a `depth` above `MAX_GROUPED_DEPTH` returns `54001`. This is
/// defense-in-depth. The parser already caps AST depth, so a tree this deep can
/// never reach here in practice.
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
        Expr::SqlJson(json) => {
            crate::json_fn::eval_sql_json(json, ctx, |child| eval_grouped_depth(child, grouped, d))
        }
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
        // `B'…'` / `X'…'` — already decoded to binary digits by the parser,
        // which also ran `bit_in`, so the value cannot fail here.
        Expr::BitStringLiteral(bits) => Ok(Datum::BitString(
            crabka_pgtypes::BitString::parse(bits, false)
                .expect("the parser validated the bit-string literal"),
        )),
        Expr::BoolLiteral(b) => Ok(Datum::Bool(*b)),
        Expr::NullLiteral => Ok(Datum::Null),
        Expr::Param(_) => Err(ExecError::Unsupported(
            "query parameters ($n) are not supported".into(),
        )),
        Expr::Default => Err(ExecError::Syntax(
            "DEFAULT is not allowed in this context".into(),
        )),
        Expr::Collate { expr, .. } => {
            let value = eval_grouped_depth(expr, grouped, d)?;
            if let Some(ty) = value.column_type() {
                crate::eval::require_collatable(ty)?;
            }
            Ok(value)
        }
        Expr::Column { table, name } => Err(ungrouped_column(table.as_deref(), name)),
        Expr::Unary { op, expr } => {
            let v = eval_grouped_depth(expr, grouped, d)?;
            crate::eval::apply_unary(*op, &v, ctx)
        }
        Expr::Binary { op, left, right } => {
            if let Some(result) = crate::rowexpr::eval_binary(*op, left, right, |e| {
                eval_grouped_depth(e, grouped, d)
            })? {
                return Ok(result);
            }
            let l = eval_grouped_depth(left, grouped, d)?;
            let r = eval_grouped_depth(right, grouped, d)?;
            crate::eval::apply_binary_of(*op, left, right, &l, &r, scope, ctx)
        }
        // A row constructor renders to PostgreSQL's composite text form once the
        // row-wise operations above have had their chance at its fields.
        Expr::Row(items) => crate::rowexpr::eval_row(items, |e| eval_grouped_depth(e, grouped, d)),
        Expr::FieldSelect { base, field } => {
            let value = eval_grouped_depth(base, grouped, d)?;
            crate::eval::select_field(&value, field)
        }
        Expr::FieldSelectAll(_) => Err(ExecError::Unsupported(
            "(row).* is only supported in a SELECT output list".into(),
        )),
        // SP28: predicate + conditional expressions in a grouped context — same
        // combinators as scalar `eval`, recursing through `eval_grouped_depth`.
        Expr::IsNull { expr, negated } => {
            if let Some(result) =
                crate::rowexpr::eval_is_null(expr, *negated, |e| eval_grouped_depth(e, grouped, d))?
            {
                return Ok(result);
            }
            let v = eval_grouped_depth(expr, grouped, d)?;
            Ok(Datum::Bool(v.is_null() ^ *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if let Some(result) = crate::rowexpr::eval_in_list(expr, list, *negated, |e| {
                eval_grouped_depth(e, grouped, d)
            })? {
                return Ok(result);
            }
            let x = eval_grouped_depth(expr, grouped, d)?;
            crate::eval::eval_in_list(expr, &x, list, *negated, ctx, |e| {
                eval_grouped_depth(e, grouped, d)
            })
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
            crate::eval::eval_between((expr, &x), (low, &lo), (high, &hi), *negated, ctx)
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => {
            let s = eval_grouped_depth(expr, grouped, d)?;
            let p = eval_grouped_depth(pattern, grouped, d)?;
            let e = escape
                .as_deref()
                .map(|e| eval_grouped_depth(e, grouped, d))
                .transpose()?;
            crate::eval::eval_like(&s, &p, *negated, *kind, e.as_ref())
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => crate::eval::eval_case(
            operand.as_deref(),
            whens,
            else_result.as_deref(),
            crate::eval::infer_case_type(whens, else_result.as_deref(), scope)?,
            ctx,
            |e| eval_grouped_depth(e, grouped, d),
        ),
        // SP29: a scalar function over grouped/aggregate arguments — evaluate it
        // with the grouped evaluator as its child-eval closure.
        Expr::Func(fc)
            if let Some(result) = crate::routine::eval_plpgsql_scalar_with(fc, ctx, |e| {
                eval_grouped_depth(e, grouped, d)
            }) =>
        {
            result
        }
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => {
            crate::func::eval_scalar(fc, Some(scope), ctx, |e| eval_grouped_depth(e, grouped, d))
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
            crate::eval::cast_value(&v, *ty, &ctx.time_zone)
        }
        // The array expression forms in a grouped context — same semantics as
        // scalar `eval`, recursing through the grouped evaluator.
        Expr::ArrayLiteral(items) => {
            let elem = crate::eval::array_literal_elem_type(items, scope)?;
            let target = elem.column_type();
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                let v = eval_grouped_depth(item, grouped, d)?;
                parts.push(match item {
                    Expr::ArrayLiteral(_) => v,
                    _ => crate::eval::cast_value(&v, target, &ctx.time_zone)?,
                });
            }
            crate::array_fn::build_constructor(elem, parts)
        }
        Expr::ArraySubquery(_) => Err(ExecError::Unsupported(
            "ARRAY(subquery) is only supported in a query context".into(),
        )),
        Expr::Subscript { base, index } => {
            let b = eval_grouped_depth(base, grouped, d)?;
            let i = eval_grouped_depth(index, grouped, d)?;
            crate::array_fn::array_subscript(&b, &i)
        }
        Expr::ArrayRef { base, subscripts } => {
            let b = eval_grouped_depth(base, grouped, d)?;
            let args = subscripts
                .iter()
                .map(|s| match s {
                    crabka_pgparser::ast::ArraySubscript::Index(e) => Ok(
                        crate::array_fn::SubscriptArg::Index(eval_grouped_depth(e, grouped, d)?),
                    ),
                    crabka_pgparser::ast::ArraySubscript::Slice { lower, upper } => {
                        let bound = |e: &Option<Expr>| {
                            e.as_ref()
                                .map(|e| eval_grouped_depth(e, grouped, d))
                                .transpose()
                        };
                        Ok(crate::array_fn::SubscriptArg::Slice {
                            lower: bound(lower)?,
                            upper: bound(upper)?,
                        })
                    }
                })
                .collect::<Result<Vec<_>, ExecError>>()?;
            crate::array_fn::array_ref(&b, &args)
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

/// One group's running accumulator for one aggregate.
///
/// The accumulator holds the running [`AccState`]. For a `DISTINCT` aggregate
/// it also holds the argument tuples it has yet to fold.
///
/// PostgreSQL implements `DISTINCT` by a sort of the WHOLE argument tuple and a
/// drop of adjacent duplicates. A `DISTINCT` aggregate therefore buffers its
/// rows and folds them at [`Acc::finish`] instead of on arrival. Two results
/// follow that a fold-on-arrival "first value seen" set gets wrong.
/// `jsonb_object_agg(DISTINCT k, v)` over `('k',1),('k',2)` keeps BOTH pairs,
/// so the object's last value is `2`, not `1`. `array_agg(DISTINCT x)` and
/// `jsonb_agg(DISTINCT x)` emit sorted order, not first-appearance order.
struct Acc {
    state: AccState,
    /// `Some` if and only if the spec is `DISTINCT`. It holds each row's
    /// evaluated argument tuple.
    distinct: Option<Vec<Vec<Datum>>>,
}

/// The running value of one aggregate.
///
/// SP30 splits `Sum` into an integer variant and a float variant. `SumI`
/// accumulates in a checked i64, so `sum(int4)` never overflows early. `SumF`
/// accumulates in f64. SP30 also adds `Avg`, which has a float8 result.
enum AccState {
    Count {
        n: i64,
    },
    SumI {
        acc: Option<i64>,
    },
    /// `sum(money)` — the same `i64` accumulation as `SumI`, but overflowing
    /// with `money`'s own message rather than the integer one.
    SumMoney {
        acc: Option<i64>,
    },
    SumF {
        acc: f64,
        any: bool,
    },
    /// `sum(float4)`, accumulated in `f32` and returned as `real`, because
    /// PostgreSQL's `sum(real)` transition function is `float4pl`.
    SumF4 {
        acc: f32,
        any: bool,
    },
    /// SP32: numeric sum, exact and without overflow, accumulated as a numeric
    /// `Datum`.
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
    /// SP32: numeric mean. It is a numeric running sum and a count, divided at
    /// finish with PostgreSQL's `select_div_scale`, so `avg(int)` and
    /// `avg(numeric)` are exact.
    AvgN {
        sum: Option<Datum>,
        n: i64,
    },
    /// `array_agg`, the values in fold order, NULLs included.
    ///
    /// An empty state means zero rows were folded, which is SQL NULL and not an
    /// empty array.
    ArrayAgg {
        elem: ElemType,
        elems: Vec<Datum>,
    },
    /// `jsonb_agg` / `json_agg` — the values in fold order, converted to JSON at
    /// `finish`. Both families accumulate identically and diverge only in how
    /// `finish` renders them, so `spec.func` — not the state — picks the type.
    JsonItems {
        items: Vec<Datum>,
    },
    /// `jsonb_object_agg` / `json_object_agg` — the (key, value) pairs in fold
    /// order, built into one object at `finish`. `jsonb` then collapses a
    /// repeated key last-wins; `json` keeps every pair.
    JsonPairs {
        pairs: Vec<(Datum, Datum)>,
    },
    /// `string_agg`, the joined value so far.
    ///
    /// The delimiter comes from each row, as PostgreSQL's transition function
    /// reads it, and is written before every value but the first.
    StringAgg {
        acc: Option<StringAggAcc>,
    },
    /// `bool_and`/`bool_or`/`every`, which record whether any input was true
    /// and whether any input was false. No rows at all is SQL NULL.
    BoolAgg {
        any_true: bool,
        any_false: bool,
        seen: bool,
    },
    /// `bit_and`/`bit_or`/`bit_xor`, the running fold, which keeps the input
    /// width.
    BitAgg {
        acc: Option<Datum>,
    },
    RangeAgg {
        acc: Option<crabka_pgtypes::MultirangeValue>,
    },
    RangeIntersect {
        acc: Option<Datum>,
    },
    /// The `float8` single-variable statistical state: PostgreSQL's
    /// Youngs–Cramer `(N, Sx, Sxx)`, where `Sxx` is the running sum of squared
    /// deviations rather than the sum of squares.
    VarFloat {
        n: f64,
        sx: f64,
        sxx: f64,
    },
    /// The exact `numeric` statistical state `(N, Σx, Σx²)`, finalized by
    /// `numeric::stddev_internal`.
    VarNumeric {
        n: i64,
        sum: NumericValue,
        sum2: NumericValue,
    },
    /// The two-variable Youngs–Cramer state shared by `corr`, `covar_*` and the
    /// whole `regr_*` family.
    Regr {
        n: f64,
        sx: f64,
        sxx: f64,
        sy: f64,
        syy: f64,
        sxy: f64,
    },
}

/// `string_agg`'s running value, in whichever of its two overloads applies.
enum StringAggAcc {
    Text(String),
    Bytea(Vec<u8>),
}

impl Acc {
    fn new(spec: &AggSpec) -> Acc {
        Acc {
            state: AccState::new(spec),
            distinct: spec.distinct.then(Vec::new),
        }
    }

    /// Fold one source row into this accumulator.
    ///
    /// Under `DISTINCT`, buffer the row's argument tuple instead, for the
    /// sorted fold that [`Acc::finish`] does.
    fn fold_row(
        &mut self,
        spec: &AggSpec,
        scope: &Scope,
        row: &[Datum],
        ctx: &EvalCtx,
    ) -> Result<(), ExecError> {
        // FILTER comes first: PostgreSQL decides whether the row participates at
        // all before evaluating the argument, so a rejected row does not count for
        // `count(*)` and never enters the DISTINCT buffer. A predicate that is
        // NULL rejects the row, exactly as a WHERE clause would.
        if let Some(predicate) = &spec.filter {
            let keep = crate::eval::eval(predicate, scope, row, ctx)?;
            if keep != Datum::Bool(true) {
                return Ok(());
            }
        }
        // count(*) counts every row, ignoring NULL/DISTINCT.
        if let (AggFunc::Count, None) = (spec.func, &spec.arg) {
            if let AccState::Count { n } = &mut self.state {
                *n += 1;
            }
            return Ok(());
        }
        let args = spec.eval_args(scope, row, ctx)?;
        // count(x)/sum/avg/min/max and the statistical family ignore rows with
        // a NULL argument — for the two-variable aggregates, a NULL in EITHER
        // position drops the pair. The collecting aggregates keep NULLs (a NULL
        // array element / JSON `null` value).
        if !spec.func.keeps_nulls() && args.iter().any(Datum::is_null) {
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

    /// This aggregate's value for the group.
    ///
    /// This method first sorts, deduplicates and folds any buffered `DISTINCT`
    /// tuples.
    fn finish(&mut self, spec: &AggSpec, ctx: &EvalCtx) -> Result<Datum, ExecError> {
        if let Some(tuples) = self.distinct.take() {
            for args in sorted_distinct(tuples)? {
                self.state.fold_args(spec, &args, ctx)?;
            }
        }
        self.state.finish(spec, ctx)
    }
}

/// PostgreSQL's `DISTINCT` input for an aggregate.
///
/// These are the argument tuples sorted ascending, with adjacent duplicates
/// dropped. The sort is what makes `array_agg(DISTINCT x)` emit ascending
/// order. A comparison, instead of a hash, is what makes `1.0` and `1.00` one
/// `numeric` value.
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

/// The order `DISTINCT` sorts and deduplicates in.
///
/// The order is argument by argument, ascending, with NULLs last. Two NULLs are
/// EQUAL here. SQL `DISTINCT` folds NULLs together even though `NULL = NULL` is
/// unknown.
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
                Some(ColumnType::Float4) => AccState::SumF4 {
                    acc: 0.0,
                    any: false,
                },
                Some(ColumnType::Float8) => AccState::SumF {
                    acc: 0.0,
                    any: false,
                },
                Some(ColumnType::Money) => AccState::SumMoney { acc: None },
                Some(t) if t.is_numeric() => AccState::SumN { acc: None },
                _ => AccState::SumI { acc: None },
            },
            // Both float widths average in f64 (`avg(real)` is `float8` in
            // PostgreSQL); int/numeric avg accumulates exactly.
            AggFunc::Avg => {
                if matches!(spec.arg_type, Some(ColumnType::Float4 | ColumnType::Float8)) {
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
                    .and_then(|t| t.array_element().or_else(|| ElemType::from_column_type(t)))
                    .unwrap_or(ElemType::Text),
                elems: Vec::new(),
            },
            AggFunc::JsonbAgg | AggFunc::JsonAgg => AccState::JsonItems { items: Vec::new() },
            AggFunc::JsonbObjectAgg | AggFunc::JsonObjectAgg => {
                AccState::JsonPairs { pairs: Vec::new() }
            }
            AggFunc::StringAgg => AccState::StringAgg { acc: None },
            AggFunc::BoolAnd | AggFunc::BoolOr => AccState::BoolAgg {
                any_true: false,
                any_false: false,
                seen: false,
            },
            AggFunc::BitAnd | AggFunc::BitOr | AggFunc::BitXor => AccState::BitAgg { acc: None },
            AggFunc::RangeAgg => AccState::RangeAgg { acc: None },
            AggFunc::RangeIntersect => AccState::RangeIntersect { acc: None },
            AggFunc::VarPop | AggFunc::VarSamp | AggFunc::StddevPop | AggFunc::StddevSamp => {
                if matches!(spec.arg_type, Some(ColumnType::Float4 | ColumnType::Float8)) {
                    AccState::VarFloat {
                        n: 0.0,
                        sx: 0.0,
                        sxx: 0.0,
                    }
                } else {
                    AccState::VarNumeric {
                        n: 0,
                        sum: NumericValue::from(0i64),
                        sum2: NumericValue::from(0i64),
                    }
                }
            }
            _ => AccState::Regr {
                n: 0.0,
                sx: 0.0,
                sxx: 0.0,
                sy: 0.0,
                syy: 0.0,
                sxy: 0.0,
            },
        }
    }

    /// Fold one already-evaluated argument tuple into the running value.
    ///
    /// The tuple is `[value]`, or `[key, value]` for `jsonb_object_agg`.
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
            AccState::SumMoney { acc } => {
                let Datum::Money(add) = v else {
                    return Err(undefined_for_arg(
                        "sum",
                        v.column_type().unwrap_or(ColumnType::Text),
                    ));
                };
                *acc = Some(match acc {
                    Some(cur) => crabka_pgtypes::money::add(*cur, add)?,
                    None => add,
                });
            }
            AccState::SumF { acc, any } => {
                *acc += as_f64(&v).ok_or_else(|| {
                    undefined_for_arg("sum", v.column_type().unwrap_or(ColumnType::Text))
                })?;
                *any = true;
            }
            AccState::SumF4 { acc, any } => {
                let Datum::Float4(add) = v else {
                    return Err(undefined_for_arg(
                        "sum",
                        v.column_type().unwrap_or(ColumnType::Text),
                    ));
                };
                // `float4pl` raises 22003 when two finite operands overflow.
                let next = *acc + add;
                if next.is_infinite() && acc.is_finite() && add.is_finite() {
                    return Err(ExecError::Type(TypeError::float_overflow()));
                }
                *acc = next;
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
                        crate::eval::require_runtime_comparison(&v, cur)?;
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
                // An array input is stacked WHOLE — it becomes one outer slice
                // of the result — so it is not coerced to the element type.
                elems.push(if matches!(v, Datum::Array(_)) {
                    v
                } else {
                    crabka_pgtypes::cast::cast(&v, elem.column_type(), &ctx.time_zone)?
                });
            }
            AccState::JsonItems { items } => items.push(v),
            AccState::JsonPairs { pairs } => {
                let value = args
                    .get(1)
                    .cloned()
                    .expect("an object aggregate has a value argument");
                pairs.push((v, value));
            }
            AccState::StringAgg { acc } => {
                let delimiter = args.get(1).cloned().unwrap_or(Datum::Null);
                append_string_agg(acc, &v, &delimiter)?;
            }
            AccState::BoolAgg {
                any_true,
                any_false,
                seen,
            } => {
                let b = match v {
                    Datum::Bool(b) => b,
                    other => {
                        return Err(undefined_for_arg(
                            "bool_and",
                            other.column_type().unwrap_or(ColumnType::Text),
                        ));
                    }
                };
                *seen = true;
                *any_true |= b;
                *any_false |= !b;
            }
            AccState::BitAgg { acc } => {
                *acc = Some(match acc.take() {
                    None => v,
                    Some(cur) => bit_fold(spec.func, &cur, &v)?,
                });
            }
            AccState::RangeAgg { acc } => {
                let multirange = match v {
                    Datum::Range(range) => {
                        let ColumnType::Multirange(ty) = ColumnType::multirange_for_range(range.ty)
                            .ok_or_else(|| {
                                undefined_for_arg("range_agg", ColumnType::Range(range.ty))
                            })?
                        else {
                            unreachable!()
                        };
                        crabka_pgtypes::multirange::from_ranges(ty, vec![range])?
                    }
                    Datum::Multirange(multirange) => multirange,
                    other => {
                        return Err(undefined_for_arg(
                            "range_agg",
                            other.column_type().unwrap_or(ColumnType::Text),
                        ));
                    }
                };
                *acc = Some(match acc.take() {
                    None => multirange,
                    Some(cur) => crabka_pgtypes::multirange::union(&cur, &multirange)?,
                });
            }
            AccState::RangeIntersect { acc } => {
                *acc = Some(match acc.take() {
                    None => v,
                    Some(Datum::Range(cur)) => {
                        let Datum::Range(range) = v else {
                            return Err(undefined_for_arg(
                                "range_intersect_agg",
                                v.column_type().unwrap_or(ColumnType::Text),
                            ));
                        };
                        Datum::Range(crabka_pgtypes::range::intersection(&cur, &range)?)
                    }
                    Some(Datum::Multirange(cur)) => {
                        let Datum::Multirange(multirange) = v else {
                            return Err(undefined_for_arg(
                                "range_intersect_agg",
                                v.column_type().unwrap_or(ColumnType::Text),
                            ));
                        };
                        Datum::Multirange(crabka_pgtypes::multirange::intersection(
                            &cur,
                            &multirange,
                        )?)
                    }
                    Some(_) => unreachable!(),
                });
            }
            // Youngs–Cramer: `Sxx` accumulates squared deviations, which is why
            // the first row contributes nothing to it.
            AccState::VarFloat { n, sx, sxx } => {
                let x = as_f64(&v).ok_or_else(|| {
                    undefined_for_arg("var_pop", v.column_type().unwrap_or(ColumnType::Text))
                })?;
                let previous = *n;
                let previous_sx = *sx;
                *n += 1.0;
                *sx += x;
                if previous > 0.0 {
                    let tmp = x * *n - *sx;
                    *sxx += tmp * tmp / (*n * previous);
                    // `Sxx` must never go infinite: an infinite input has to
                    // reach the finalizer as NaN, and two finite operands whose
                    // combination overflows are the 22003 case instead.
                    if sx.is_infinite() || sxx.is_infinite() {
                        if previous_sx.is_finite() && x.is_finite() {
                            return Err(ExecError::Type(TypeError::float_overflow()));
                        }
                        *sxx = f64::NAN;
                    }
                } else if !x.is_finite() {
                    // A lone NaN/±Infinity leaves `Sxx` at its zero start, which
                    // would report variance zero; poison it so the whole family
                    // finalizes to NaN the way PostgreSQL's does.
                    *sxx = f64::NAN;
                }
            }
            AccState::VarNumeric { n, sum, sum2 } => {
                let x = crabka_pgtypes::cast::cast(&v, ColumnType::Numeric(None), &ctx.time_zone)?;
                let Datum::Numeric(x) = x else {
                    return Err(undefined_for_arg(
                        "var_pop",
                        v.column_type().unwrap_or(ColumnType::Text),
                    ));
                };
                *n += 1;
                *sum = crabka_pgtypes::numeric::add(sum, &x);
                *sum2 = crabka_pgtypes::numeric::add(sum2, &crabka_pgtypes::numeric::mul(&x, &x));
            }
            AccState::Regr {
                n,
                sx,
                sxx,
                sy,
                syy,
                sxy,
            } => {
                // PostgreSQL writes these `f(Y, X)`, so the FIRST argument is y.
                let y = as_f64(&v).ok_or_else(|| {
                    undefined_for_arg("regr", v.column_type().unwrap_or(ColumnType::Text))
                })?;
                let x = args
                    .get(1)
                    .and_then(as_f64)
                    .ok_or_else(|| undefined_for_arg("regr", ColumnType::Text))?;
                let seen_before = *n;
                let (x_sum_before, y_sum_before) = (*sx, *sy);
                *n += 1.0;
                *sx += x;
                *sy += y;
                if seen_before > 0.0 {
                    let tmp_x = x * *n - *sx;
                    let tmp_y = y * *n - *sy;
                    let scale = 1.0 / (*n * seen_before);
                    *sxx += tmp_x * tmp_x * scale;
                    *syy += tmp_y * tmp_y * scale;
                    *sxy += tmp_x * tmp_y * scale;
                    // As for the one-variable state: an infinite sum of squared
                    // deviations is either 22003 (all the inputs feeding it were
                    // finite) or the NaN an infinite input has to become.
                    if sx.is_infinite()
                        || sxx.is_infinite()
                        || sy.is_infinite()
                        || syy.is_infinite()
                        || sxy.is_infinite()
                    {
                        let x_finite = x_sum_before.is_finite() && x.is_finite();
                        let y_finite = y_sum_before.is_finite() && y.is_finite();
                        if ((sx.is_infinite() || sxx.is_infinite()) && x_finite)
                            || ((sy.is_infinite() || syy.is_infinite()) && y_finite)
                            || (sxy.is_infinite() && x_finite && y_finite)
                        {
                            return Err(ExecError::Type(TypeError::float_overflow()));
                        }
                        if sxx.is_infinite() {
                            *sxx = f64::NAN;
                        }
                        if syy.is_infinite() {
                            *syy = f64::NAN;
                        }
                        if sxy.is_infinite() {
                            *sxy = f64::NAN;
                        }
                    }
                } else {
                    if !x.is_finite() {
                        *sxx = f64::NAN;
                        *sxy = f64::NAN;
                    }
                    if !y.is_finite() {
                        *syy = f64::NAN;
                        *sxy = f64::NAN;
                    }
                }
            }
        }
        Ok(())
    }

    fn finish(&self, spec: &AggSpec, ctx: &EvalCtx) -> Result<Datum, ExecError> {
        Ok(match self {
            AccState::Count { n } => Datum::Int8(*n),
            AccState::SumI { acc } => acc.map(Datum::Int8).unwrap_or(Datum::Null),
            AccState::SumMoney { acc } => acc.map(Datum::Money).unwrap_or(Datum::Null),
            // An empty / all-null float sum is NULL (matches the integer sum).
            AccState::SumF { acc, any } => {
                if *any {
                    Datum::Float8(*acc)
                } else {
                    Datum::Null
                }
            }
            AccState::SumF4 { acc, any } => {
                if *any {
                    Datum::Float4(*acc)
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
                    crate::array_fn::build_constructor(*elem, elems.clone())?
                }
            }
            // Zero rows is SQL NULL, not an empty array/object — and it is NULL
            // even when a row WOULD have failed, so the run-time key checks below
            // are never reached for an empty group.
            AccState::JsonItems { items } => {
                if items.is_empty() {
                    Datum::Null
                } else if spec.func.is_json() {
                    build_json_array(items, ctx)?
                } else {
                    build_jsonb("jsonb_build_array", items.clone(), ctx)?
                }
            }
            AccState::JsonPairs { pairs } => {
                if pairs.is_empty() {
                    Datum::Null
                } else if spec.func.is_json() {
                    build_json_object(pairs, ctx)?
                } else {
                    let mut flat = Vec::with_capacity(pairs.len() * 2);
                    for (key, value) in pairs {
                        flat.push(key.clone());
                        flat.push(value.clone());
                    }
                    build_jsonb("jsonb_build_object", flat, ctx)?
                }
            }
            // string_agg over zero non-NULL rows is NULL, not an empty string.
            AccState::StringAgg { acc } => match acc {
                None => Datum::Null,
                Some(StringAggAcc::Text(s)) => Datum::Text(s.clone()),
                Some(StringAggAcc::Bytea(b)) => Datum::Bytea(b.clone()),
            },
            AccState::BoolAgg {
                any_true,
                any_false,
                seen,
            } => {
                if !*seen {
                    Datum::Null
                } else if spec.func == AggFunc::BoolAnd {
                    Datum::Bool(!*any_false)
                } else {
                    Datum::Bool(*any_true)
                }
            }
            AccState::BitAgg { acc } => acc.clone().unwrap_or(Datum::Null),
            AccState::RangeAgg { acc } => acc.clone().map(Datum::Multirange).unwrap_or(Datum::Null),
            AccState::RangeIntersect { acc } => acc.clone().unwrap_or(Datum::Null),
            AccState::VarFloat { n, sxx, .. } => {
                let (sample, sqrt) = spec
                    .func
                    .variance_shape()
                    .expect("a VarFloat accumulator belongs to the variance family");
                // PostgreSQL clamps a numerator driven negative by roundoff.
                let sxx = clamp_non_negative(*sxx);
                if *n <= if sample { 1.0 } else { 0.0 } {
                    Datum::Null
                } else {
                    let variance = if sample { sxx / (*n - 1.0) } else { sxx / *n };
                    Datum::Float8(if sqrt { variance.sqrt() } else { variance })
                }
            }
            AccState::VarNumeric { n, sum, sum2 } => {
                let (sample, sqrt) = spec
                    .func
                    .variance_shape()
                    .expect("a VarNumeric accumulator belongs to the variance family");
                crabka_pgtypes::numeric::stddev_internal(*n, sum, sum2, sample, sqrt)
                    .map_or(Datum::Null, Datum::Numeric)
            }
            AccState::Regr {
                n,
                sx,
                sxx,
                sy,
                syy,
                sxy,
            } => finish_regr(spec.func, *n, *sx, *sxx, *sy, *syy, *sxy),
        })
    }
}

/// Append one row to a `string_agg` accumulator. This function writes the
/// delimiter before every value but the first.
fn append_string_agg(
    acc: &mut Option<StringAggAcc>,
    value: &Datum,
    delimiter: &Datum,
) -> Result<(), ExecError> {
    match value {
        Datum::Text(s) => {
            let separator = match delimiter {
                Datum::Null => "",
                Datum::Text(d) => d.as_str(),
                other => {
                    return Err(undefined_for_arg(
                        "string_agg",
                        other.column_type().unwrap_or(ColumnType::Text),
                    ));
                }
            };
            match acc {
                Some(StringAggAcc::Text(current)) => {
                    current.push_str(separator);
                    current.push_str(s);
                }
                _ => *acc = Some(StringAggAcc::Text(s.clone())),
            }
            Ok(())
        }
        Datum::Bytea(b) => {
            let separator: &[u8] = match delimiter {
                Datum::Null => &[],
                Datum::Bytea(d) => d,
                other => {
                    return Err(undefined_for_arg(
                        "string_agg",
                        other.column_type().unwrap_or(ColumnType::Text),
                    ));
                }
            };
            match acc {
                Some(StringAggAcc::Bytea(current)) => {
                    current.extend_from_slice(separator);
                    current.extend_from_slice(b);
                }
                _ => *acc = Some(StringAggAcc::Bytea(b.clone())),
            }
            Ok(())
        }
        other => Err(undefined_for_arg(
            "string_agg",
            other.column_type().unwrap_or(ColumnType::Text),
        )),
    }
}

/// One step of `bit_and`/`bit_or`/`bit_xor`, which keeps the integer width.
fn bit_fold(func: AggFunc, a: &Datum, b: &Datum) -> Result<Datum, ExecError> {
    let apply = |x: i64, y: i64| match func {
        AggFunc::BitAnd => x & y,
        AggFunc::BitOr => x | y,
        _ => x ^ y,
    };
    Ok(match (a, b) {
        (Datum::Int2(x), Datum::Int2(y)) => Datum::Int2(apply(i64::from(*x), i64::from(*y)) as i16),
        (Datum::Int4(x), Datum::Int4(y)) => Datum::Int4(apply(i64::from(*x), i64::from(*y)) as i32),
        _ => {
            let (x, y) = (as_i64(a), as_i64(b));
            match (x, y) {
                (Some(x), Some(y)) => Datum::Int8(apply(x, y)),
                _ => {
                    return Err(undefined_for_arg(
                        "bit_and",
                        b.column_type().unwrap_or(ColumnType::Text),
                    ));
                }
            }
        }
    })
}

/// PostgreSQL's `if (Sxx < 0.0) Sxx = 0.0` roundoff clamp on a sum of squared
/// deviations.
///
/// `f64::max` is the wrong tool. It *drops* a NaN operand and keeps the other
/// one. A non-finite input puts a NaN into the accumulator, and `f64::max`
/// would turn that NaN back into a variance of zero.
fn clamp_non_negative(value: f64) -> f64 {
    if value < 0.0 { 0.0 } else { value }
}

/// Finalize the two-variable Youngs–Cramer state into whichever member of the
/// family asked for it.
///
/// Every undefined case, such as too few rows or a zero spread, is SQL NULL,
/// exactly as PostgreSQL's finalizers return it.
fn finish_regr(func: AggFunc, n: f64, sx: f64, sxx: f64, sy: f64, syy: f64, sxy: f64) -> Datum {
    if n < 1.0 && func != AggFunc::RegrCount {
        return Datum::Null;
    }
    // Roundoff can drive either sum of squared deviations slightly negative.
    let sxx = clamp_non_negative(sxx);
    let syy = clamp_non_negative(syy);
    match func {
        AggFunc::RegrCount => Datum::Int8(n as i64),
        AggFunc::RegrSxx => Datum::Float8(sxx),
        AggFunc::RegrSyy => Datum::Float8(syy),
        AggFunc::RegrSxy => Datum::Float8(sxy),
        AggFunc::RegrAvgx => Datum::Float8(sx / n),
        AggFunc::RegrAvgy => Datum::Float8(sy / n),
        AggFunc::CovarPop => Datum::Float8(sxy / n),
        AggFunc::CovarSamp if n < 2.0 => Datum::Null,
        AggFunc::CovarSamp => Datum::Float8(sxy / (n - 1.0)),
        AggFunc::Corr if sxx == 0.0 || syy == 0.0 => Datum::Null,
        AggFunc::Corr => Datum::Float8(sxy / (sxx * syy).sqrt()),
        AggFunc::RegrSlope | AggFunc::RegrIntercept | AggFunc::RegrR2 if n < 2.0 || sxx == 0.0 => {
            Datum::Null
        }
        AggFunc::RegrSlope => Datum::Float8(sxy / sxx),
        AggFunc::RegrIntercept => Datum::Float8((sy - sx * sxy / sxx) / n),
        AggFunc::RegrR2 if syy == 0.0 => Datum::Float8(1.0),
        AggFunc::RegrR2 => Datum::Float8((sxy * sxy) / (sxx * syy)),
        _ => Datum::Null,
    }
}

/// Build a `jsonb` aggregate's result through the corresponding `json_fn`
/// builder.
///
/// `jsonb_agg` is exactly the row-wise fold of `jsonb_build_array`, and
/// `jsonb_object_agg` is the row-wise fold of `jsonb_build_object`. A route
/// through the builders keeps ONE set of SQL-value to JSON rules instead of a
/// second copy that could drift. Those rules cover numeric scale, ISO date
/// spelling, JSON `null` for a SQL NULL value, 22023 for a NULL key, and
/// last-wins duplicate keys.
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
        filter: None,
    };
    crate::json_fn::eval_json(&call, ctx, |e| match e {
        Expr::Const { value, .. } => Ok(value.clone()),
        _ => Err(ExecError::Unsupported(
            "internal: a jsonb aggregate builds only from constants".into(),
        )),
    })
}

/// `json_agg`'s array.
///
/// The `json` family cannot route through a [`JsonbValue`](crabka_pgtypes::jsonb::JsonbValue)
/// the way [`build_jsonb`] does, because building one is exactly what `json_agg`
/// must not do: `jsonb` would sort an object element's keys, collapse its
/// duplicates and rewrite its spacing, and preserving all three is the whole
/// difference between the two types. Each element is rendered on its own through
/// `to_json` and the pieces are joined under `PostgreSQL`'s layout instead.
fn build_json_array(items: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let mut out = String::from("[");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push_str(Layout::Spaced.comma());
            // `json_agg_transfn`'s "add some whitespace if structured type and
            // not first item": an array or composite element — and only one of
            // those, and only after a separator — is preceded by a line feed.
            // `jsonb_agg` has no such rule, so this is a `json`-only wart.
            if matches!(item, Datum::Array(_) | Datum::Record(_)) {
                out.push_str("\n ");
            }
        }
        out.push_str(&element_json(item, ctx)?);
    }
    out.push(']');
    Ok(Datum::Json(out))
}

/// `json_object_agg`'s object — the one constructor that pads its braces.
///
/// Every pair the fold collected is emitted, in fold order: `json` has no notion
/// of a duplicate key, so `json_object_agg(k, v)` over `('a',1),('a',2)` is
/// `{ "a" : 1, "a" : 2 }` where `jsonb_object_agg` keeps only the last.
fn build_json_object(pairs: &[(Datum, Datum)], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let layout = Layout::Padded;
    let mut out = String::from("{");
    out.push_str(layout.pad());
    for (i, (key, value)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push_str(layout.comma());
        }
        out.push_str(&json_object_key(key, ctx)?);
        out.push_str(layout.colon());
        out.push_str(&element_json(value, ctx)?);
    }
    out.push_str(layout.pad());
    out.push('}');
    Ok(Datum::Json(out))
}

/// One value nested inside a `json` aggregate's result.
///
/// Always [`Layout::Compact`]: a constructor's spacing describes the document it
/// is building and never reaches inside a value, which is why
/// `json_agg(ROW(1,2))` is `[{"f1":1,"f2":2}]` and not `[{"f1" : 1, "f2" : 2}]`.
fn element_json(d: &Datum, ctx: &EvalCtx) -> Result<String, ExecError> {
    crate::json_fn::to_json_text(d, Layout::Compact, ctx)
}

/// `json_object_agg`'s KEY, as the quoted JSON string `PostgreSQL` writes.
///
/// `datum_to_json_internal` renders a key exactly as it renders a value and then
/// quotes whatever was not already a string, so a key follows the JSON spelling
/// rather than the SQL one: `true` not `t`, `2020-01-02T03:04:05` not the
/// space-separated form, and `1.50` keeping its scale.
///
/// Both rejections happen at RUN time — which is why a zero-row
/// `json_object_agg(json_col, v)` is NULL rather than an error — and the NULL one
/// does NOT share the `jsonb` family's SQLSTATE: `json_object_agg` raises 22004
/// where `jsonb_object_agg` raises 22023.
fn json_object_key(key: &Datum, ctx: &EvalCtx) -> Result<String, ExecError> {
    match key {
        Datum::Null => {
            return Err(ExecError::FunctionError {
                sqlstate: "22004",
                message: "null value not allowed for object key".into(),
            });
        }
        Datum::Json(_) | Datum::Jsonb(_) | Datum::Array(_) | Datum::Record(_) => {
            return Err(ExecError::FunctionError {
                sqlstate: "22023",
                message: "key value must be scalar, not array, composite, or json".into(),
            });
        }
        _ => {}
    }
    let rendered = element_json(key, ctx)?;
    // A key that rendered as a JSON string already carries its quotes and its
    // escapes; anything else is a bare literal whose text becomes the key.
    if rendered.starts_with('"') {
        Ok(rendered)
    } else {
        Ok(crabka_pgtypes::json::quote(&rendered))
    }
}

fn as_i64(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int2(n) => Some(i64::from(*n)),
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        _ => None,
    }
}

fn as_f64(d: &Datum) -> Option<f64> {
    match d {
        Datum::Int2(n) => Some(f64::from(*n)),
        Datum::Int4(n) => Some(f64::from(*n)),
        Datum::Int8(n) => Some(*n as f64),
        Datum::Float4(f) => Some(f64::from(*f)),
        Datum::Float8(f) => Some(*f),
        _ => None,
    }
}

/// Execute an aggregate query over the already-`WHERE`-filtered `rows` and
/// return the final `QueryResult::Rows`.
///
/// This function is a thin wrapper over `aggregate_rows`, the row-producing
/// core that derived tables share through `select_to_relation`. `ctx` carries
/// the session zone and clock for any temporal evaluation. Non-temporal
/// aggregation ignores `ctx`, and UTC/epoch reproduces prior behavior.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn execute_aggregate(
    s: &SelectStmt,
    scope: &Scope,
    rows: Vec<Vec<Datum>>,
    ctx: &EvalCtx,
) -> Result<QueryResult, ExecError> {
    let (fields, _exprs, _tys) = crate::exec::resolve_projection(&s.projection, scope)?;
    let out_rows = aggregate_rows(s, scope, rows, ctx)?;
    Ok(crate::exec::rows_result(
        fields,
        &out_rows,
        ctx.output_style(),
    ))
}

/// Fold an aggregate query over the already-`WHERE`-filtered `rows` and return
/// the projected output Datum rows.
///
/// HAVING, DISTINCT, ORDER BY, OFFSET and LIMIT are all applied.
/// `execute_aggregate` renders these rows to a `QueryResult`. A derived table
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
    // Only plain DISTINCT restricts ORDER BY to the select-list output; DISTINCT
    // ON sorts before projecting, so its ORDER BY may name source expressions.
    let require_output = matches!(s.distinct, crabka_pgparser::ast::DistinctClause::Distinct);
    let order_keys = crate::exec::resolve_select_order_keys(
        &s.order_by,
        scope,
        &fields,
        &out_exprs,
        require_output,
    )?;

    // `DISTINCT ON` runs over the GROUPED output — the same plan the row path
    // uses, evaluated per group instead of per row: `plan.sort` decides which
    // group of each `plan.group` run survives, and the query's own ORDER BY then
    // sorts the survivors. It is resolved from the expressions as WRITTEN, before
    // the canonicalization below, because its own compatibility rule compares the
    // `ON` list against the select list and the ORDER BY as the query spells them.
    let distinct_on = crate::exec::distinct_on_plan(s, scope, &fields, &out_exprs, &order_keys)?;

    for expr in &s.group_by {
        crate::eval::require_equality_operator(crate::eval::infer_type(expr, scope)?)?;
    }
    if matches!(s.distinct, crabka_pgparser::ast::DistinctClause::Distinct) {
        for expr in &out_exprs {
            crate::eval::require_equality_operator(crate::eval::infer_type(expr, scope)?)?;
        }
    }
    if let Some(plan) = &distinct_on {
        for expr in &plan.group {
            crate::eval::require_equality_operator(crate::eval::infer_type(expr, scope)?)?;
        }
    }
    for key in &order_keys {
        let expr = match key {
            crate::exec::SelectOrderKey::Output(index) => &out_exprs[*index],
            crate::exec::SelectOrderKey::SourceExpr(expr) => expr,
        };
        crate::eval::require_ordering_operator(crate::eval::infer_type(expr, scope)?)?;
    }

    // Every clause evaluated above the grouping is matched against the GROUP BY
    // list by the column each reference resolves to, not by how it was spelled,
    // so all of them are canonicalized once here (PostgreSQL compares the
    // underlying variables, which is why `SELECT t.a … GROUP BY a` is valid).
    let canonical = |e: &Expr| crate::grouping::canonicalize_columns(e, scope);
    let group_by: Vec<Expr> = s.group_by.iter().map(&canonical).collect();
    let out_exprs: Vec<Expr> = out_exprs.iter().map(&canonical).collect();
    let having = s.having.as_ref().map(&canonical);
    let order_keys: Vec<crate::exec::SelectOrderKey> = order_keys
        .into_iter()
        .map(|key| match key {
            crate::exec::SelectOrderKey::SourceExpr(expr) => {
                crate::exec::SelectOrderKey::SourceExpr(canonical(&expr))
            }
            output => output,
        })
        .collect();
    let distinct_on = distinct_on.map(|plan| crate::exec::DistinctOnPlan {
        group: plan.group.iter().map(&canonical).collect(),
        sort: plan
            .sort
            .into_iter()
            .map(|item| crabka_pgparser::ast::OrderItem {
                expr: canonical(&item.expr),
                ..item
            })
            .collect(),
    });

    // GROUP BY expressions may not themselves be aggregates.
    for g in &group_by {
        if contains_aggregate(g) {
            return Err(ExecError::Grouping(
                "aggregate functions are not allowed in GROUP BY".into(),
            ));
        }
    }

    // Collect (deduped) the aggregates to compute, then validate every output /
    // HAVING / ORDER BY / DISTINCT ON expression is grouped-valid
    // (data-independent).
    let mut specs: Vec<AggSpec> = Vec::new();
    let source_order_exprs = order_keys.iter().filter_map(|key| match key {
        crate::exec::SelectOrderKey::Output(_) => None,
        crate::exec::SelectOrderKey::SourceExpr(expr) => Some(expr),
    });
    let distinct_on_exprs = distinct_on
        .iter()
        .flat_map(|plan| plan.group.iter().chain(plan.sort.iter().map(|i| &i.expr)));
    for e in out_exprs
        .iter()
        .chain(having.iter())
        .chain(distinct_on_exprs)
        .chain(source_order_exprs)
    {
        collect_specs(e, scope, &mut specs)?;
        validate_grouped(e, &group_by, scope)?;
    }

    // Fold rows into groups, preserving first-appearance order.
    let has_group_by = !group_by.is_empty();
    let mut keys: Vec<Vec<Datum>> = Vec::new();
    let mut accs: Vec<Vec<Acc>> = Vec::new();
    let mut index: HashMap<Vec<Datum>, usize> = HashMap::new();
    let mut group_bytes = 0usize;
    // The grouping keys are the same expressions for every row, so resolve
    // their column references once rather than once per row.
    let bound_group_by = crate::bind::bind_all(&group_by, scope);
    for row in &rows {
        let mut key = Vec::with_capacity(bound_group_by.len());
        for g in &bound_group_by {
            key.push(crate::eval::eval(g.expr(), scope, row, ctx)?);
        }
        let gi = match index.get(&key) {
            Some(&i) => i,
            None => {
                let bytes = crate::scanner::datum_row_bytes(&key)
                    .saturating_mul(2)
                    .saturating_add(specs.len().saturating_mul(std::mem::size_of::<Acc>()));
                if crate::scanner::exceeds_query_memory(
                    group_bytes.saturating_add(bytes),
                    crate::scanner::BLOCKING_QUERY_MEMORY,
                ) {
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
    let mut out: Vec<GroupOutput> = Vec::with_capacity(keys.len());
    for (key, group_accs) in keys.iter().zip(accs.iter_mut()) {
        let results: Vec<Datum> = group_accs
            .iter_mut()
            .zip(&specs)
            .map(|(acc, spec)| acc.finish(spec, ctx))
            .collect::<Result<_, ExecError>>()?;
        let grouped = |e: &Expr| eval_grouped(e, scope, &group_by, key, &specs, &results, ctx);
        if let Some(h) = &having {
            match grouped(h)? {
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
            projected.push(grouped(e)?);
        }
        let mut sort_keys = Vec::with_capacity(order_keys.len());
        for order_key in &order_keys {
            sort_keys.push(match order_key {
                crate::exec::SelectOrderKey::Output(i) => projected[*i].clone(),
                crate::exec::SelectOrderKey::SourceExpr(expr) => grouped(expr)?,
            });
        }
        let (mut dedup_keys, mut on_keys) = (Vec::new(), Vec::new());
        if let Some(plan) = &distinct_on {
            for item in &plan.sort {
                dedup_keys.push(grouped(&item.expr)?);
            }
            for expr in &plan.group {
                on_keys.push(grouped(expr)?);
            }
        }
        out.push(GroupOutput {
            sort_keys,
            dedup_keys,
            on_keys,
            projected,
        });
    }

    // `DISTINCT ON` dedups a stream sorted by its own plan, which is not always
    // the query's ORDER BY: that sort decides which group survives, ORDER BY only
    // decides how the survivors come out. The final sort is stable, so it is a
    // no-op when the dedup ordering already satisfies it.
    if let Some(plan) = &distinct_on {
        out.sort_by(|a, b| crate::exec::order_cmp(&a.dedup_keys, &b.dedup_keys, &plan.sort));
        let mut previous: Option<Vec<Datum>> = None;
        out.retain(|group| {
            let first = previous.as_ref() != Some(&group.on_keys);
            previous = Some(group.on_keys.clone());
            first
        });
    }
    if !s.order_by.is_empty() {
        out.sort_by(|a, b| crate::exec::order_cmp(&a.sort_keys, &b.sort_keys, &s.order_by));
    }
    // SP28: SELECT DISTINCT dedups identical projected rows (first appearance).
    if distinct_on.is_none() && s.distinct.dedups() {
        let mut seen: HashSet<Vec<Datum>> = HashSet::new();
        out.retain(|group| seen.insert(group.projected.clone()));
    }
    let out: Vec<(Vec<Datum>, Vec<Datum>)> = out
        .into_iter()
        .map(|group| (group.sort_keys, group.projected))
        .collect();
    // SP28: OFFSET then LIMIT.
    let window = crate::exec::RowWindow {
        offset: crate::exec::eval_row_count(
            s.offset.as_ref(),
            crate::exec::RowCountClause::Offset,
            ctx,
        )?,
        limit: crate::exec::eval_row_count(
            s.limit.as_ref(),
            crate::exec::RowCountClause::Limit,
            ctx,
        )?,
        with_ties: s.with_ties,
    };

    Ok(crate::exec::apply_row_window(out, window, &s.order_by))
}

/// One finalized group on its way out of [`aggregate_rows`].
///
/// The group holds the ORDER BY sort vector, the `DISTINCT ON` dedup-sort and
/// grouping vectors, which are both empty without that clause, and the
/// projected output row.
struct GroupOutput {
    sort_keys: Vec<Datum>,
    dedup_keys: Vec<Datum>,
    on_keys: Vec<Datum>,
    projected: Vec<Datum>,
}

#[cfg(test)]
mod tests {
    use crabka_pgcatalog::{Column, RelationName, Table};
    use crabka_pgparser::ast::{QueryBody, SelectStmt, SetExpr, Statement};
    use crabka_pgwire::engine::Cell;

    use super::*;

    fn table() -> Table {
        Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![
                Column::new("k", ColumnType::Int4),
                Column::new("v", ColumnType::Int4),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    /// The table's single-relation scope, or the empty FROM-less scope.
    fn scope_of(t: Option<&Table>) -> Scope {
        match t {
            Some(t) => Scope::single(t, &t.name.name),
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
                    s.with_ties = q.with_ties;
                    s.locking = q.locking;
                    s
                }
                other => panic!("expected select body, got {other:?}"),
            },
            other => panic!("expected select, got {other:?}"),
        }
    }

    /// Parse one SELECT and run it over the given rows, which are already
    /// WHERE-filtered.
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

    /// Decode a result cell back to a Datum for assertions.
    ///
    /// The tests compare text-format payloads, so this helper maps back to Text
    /// and Null, and parses integers.
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

    /// Like `agg`, but returns the raw text-format cells.
    ///
    /// `cell_to_datum` cannot round-trip a float result cleanly, so a test can
    /// assert the raw cells directly.
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
    fn range_aggregates_fold_ranges_and_multiranges() {
        let ty = ColumnType::builtin_range(crabka_pgtypes::oids::INT4RANGE).expect("int4range");
        let ColumnType::Range(range_ty) = ty else {
            unreachable!()
        };
        let mut t = table();
        t.columns = vec![Column::new("v", ty)];
        let range = |text| {
            Datum::Range(
                crabka_pgtypes::range::parse(text, range_ty, &jiff::tz::TimeZone::UTC)
                    .expect("range"),
            )
        };
        assert_eq!(
            agg_text(
                "SELECT range_intersect_agg(v) FROM t",
                Some(&t),
                vec![vec![range("[1,5)")], vec![range("[3,7)")]]
            )
            .expect("aggregate"),
            vec![vec![Some("[3,5)".into())]]
        );
        assert_eq!(
            agg_text("SELECT range_intersect_agg(v) FROM t", Some(&t), vec![])
                .expect("empty aggregate"),
            vec![vec![None]]
        );

        assert_eq!(
            agg_text(
                "SELECT range_agg(v) FROM t",
                Some(&t),
                vec![
                    vec![range("[1,3)")],
                    vec![range("[3,5)")],
                    vec![range("[8,9)")]
                ],
            )
            .expect("range aggregate"),
            vec![vec![Some("{[1,5),[8,9)}".into())]]
        );

        let multi_ty = ColumnType::builtin_multirange(crabka_pgtypes::oids::INT4MULTIRANGE)
            .expect("int4multirange");
        let ColumnType::Multirange(multi_ref) = multi_ty else {
            unreachable!()
        };
        t.columns = vec![Column::new("v", multi_ty)];
        let multirange = |text| {
            Datum::Multirange(
                crabka_pgtypes::multirange::parse(text, multi_ref, &jiff::tz::TimeZone::UTC)
                    .expect("multirange"),
            )
        };
        assert_eq!(
            agg_text(
                "SELECT range_agg(v), range_intersect_agg(v) FROM t",
                Some(&t),
                vec![
                    vec![multirange("{[1,6),[10,12)}")],
                    vec![multirange("{[4,14)}")],
                ],
            )
            .expect("multirange aggregates"),
            vec![vec![
                Some("{[1,14)}".into()),
                Some("{[4,6),[10,12)}".into()),
            ]]
        );
    }

    /// `sum`/`avg`/`min`/`max` over the two new scalar types, in both the type
    /// PostgreSQL reports and the text it prints.
    ///
    /// Every expectation is a PostgreSQL 18.4 `pg_typeof` and value pair.
    /// `sum(int2)` is `bigint`, so it grows past `int2` instead of overflowing.
    /// `avg(int2)` is scale-padded `numeric`. `sum(real)` stays `real`, while
    /// `avg(real)` widens to `double precision`. `min` and `max` keep the
    /// argument type.
    #[test]
    fn int2_and_float4_aggregate_result_types_match_postgres() {
        use assert2::assert;
        let typed = |ty: ColumnType| {
            let mut t = table();
            t.columns[1].ty = ty;
            t
        };
        // sum over the two int2 extremes cannot fit int2 — proving it is int8.
        let int2_table = typed(ColumnType::Int2);
        let int2_rows = vec![
            r(&[Datum::Int4(1), Datum::Int2(32_767)]),
            r(&[Datum::Int4(1), Datum::Int2(32_767)]),
            r(&[Datum::Int4(1), Datum::Null]),
        ];
        assert!(
            agg_text(
                "SELECT sum(v), avg(v), min(v), max(v), count(v) FROM t",
                Some(&int2_table),
                int2_rows,
            )
            .expect("int2 agg")
                == vec![vec![
                    Some("65534".to_string()),
                    Some("32767.000000000000".to_string()),
                    Some("32767".to_string()),
                    Some("32767".to_string()),
                    Some("2".to_string()),
                ]]
        );
        // `sum(real)` accumulates in f32 and prints through float4out (the
        // scientific branch here); `avg(real)` is float8, so 1/3 keeps its
        // double-precision digits.
        let float4_table = typed(ColumnType::Float4);
        let float4_rows = vec![
            r(&[Datum::Int4(1), Datum::Float4(1_000_000.0)]),
            r(&[Datum::Int4(1), Datum::Float4(1.5)]),
        ];
        assert!(
            agg_text(
                "SELECT sum(v), min(v), max(v) FROM t",
                Some(&float4_table),
                float4_rows.clone(),
            )
            .expect("float4 agg")
                == vec![vec![
                    Some("1.0000015e+06".to_string()),
                    Some("1.5".to_string()),
                    Some("1e+06".to_string()),
                ]]
        );
        assert!(
            agg_text(
                "SELECT avg(v) FROM t",
                Some(&float4_table),
                vec![
                    r(&[Datum::Int4(1), Datum::Float4(1.0)]),
                    r(&[Datum::Int4(1), Datum::Float4(1.0)]),
                    r(&[Datum::Int4(1), Datum::Float4(1.0)]),
                    r(&[Datum::Int4(1), Datum::Float4(0.0)]),
                ],
            )
            .expect("float4 avg")
                == vec![vec![Some("0.75".to_string())]]
        );
        // Zero non-NULL rows is NULL at both widths, not zero.
        for (ty, value) in [
            (ColumnType::Int2, Datum::Null),
            (ColumnType::Float4, Datum::Null),
        ] {
            let t = typed(ty);
            assert!(
                agg_text(
                    "SELECT sum(v), avg(v), min(v), max(v) FROM t",
                    Some(&t),
                    vec![r(&[Datum::Int4(1), value])],
                )
                .expect("empty agg")
                    == vec![vec![None, None, None, None]],
                "{ty:?}"
            );
        }
        // And the RowDescription types the wire reports.
        let expected: &[(ColumnType, [ColumnType; 4])] = &[
            (
                ColumnType::Int2,
                [
                    ColumnType::Int8,
                    ColumnType::Numeric(None),
                    ColumnType::Int2,
                    ColumnType::Int2,
                ],
            ),
            (
                ColumnType::Float4,
                [
                    ColumnType::Float4,
                    ColumnType::Float8,
                    ColumnType::Float4,
                    ColumnType::Float4,
                ],
            ),
        ];
        for (arg, [sum, avg, min, max]) in expected {
            let t = typed(*arg);
            let scope = scope_of(Some(&t));
            for (sql, want) in [("sum", sum), ("avg", avg), ("min", min), ("max", max)] {
                let call = match crabka_pgparser::parser::parse_expr_for_test(&format!("{sql}(v)"))
                    .expect("parse")
                {
                    Expr::Func(fc) => fc,
                    other => panic!("expected a function call, got {other:?}"),
                };
                assert!(
                    func_result_type(&call, &scope).expect("result type") == *want,
                    "{sql}({arg:?})"
                );
            }
        }
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

    /// A table with a `timestamp` column `ts` and the int key `k`, for the
    /// format-function-over-aggregate composition tests.
    fn ts_table() -> Table {
        Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![
                Column::new("k", ColumnType::Int4),
                Column::new("ts", ColumnType::Timestamp),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
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
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![
                Column::new("k", ColumnType::Int4),
                Column::new("v", ColumnType::Int4),
                Column::new("s", ColumnType::Text),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    fn collect_rows() -> Vec<Vec<Datum>> {
        vec![
            r(&[Datum::Int4(1), Datum::Int4(30), Datum::Text("c".into())]),
            r(&[Datum::Int4(1), Datum::Int4(10), Datum::Text("a".into())]),
            r(&[Datum::Int4(2), Datum::Null, Datum::Text("b".into())]),
        ]
    }

    /// The result types are what RowDescription reports, and they follow the
    /// argument.
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

    /// The three aggregates accumulate in INPUT order and keep NULL inputs, as
    /// a NULL array element and as the JSON `null` literal. `sum`, `min` and
    /// `max` do not.
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

    /// Over ZERO rows every collecting aggregate is SQL NULL. `array_agg` in
    /// particular is NULL and not an empty array, which is PostgreSQL's
    /// behavior.
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
    /// as PostgreSQL does.
    ///
    /// The collecting aggregates emit ascending order, not first-appearance
    /// order. `jsonb_object_agg(DISTINCT k, v)` keeps every distinct PAIR, so a
    /// repeated key still takes its last value. Each expected row is PostgreSQL
    /// 18.4's output over the same rows.
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

    /// `jsonb_object_agg`'s key is PostgreSQL's `"any"` parameter.
    ///
    /// Every scalar type is accepted and rendered through its output function.
    /// Only a container key is refused. Like a NULL key, it is refused at RUN
    /// time as 22023, so a zero-row aggregate over one is NULL and not an
    /// error. Each expected object is PostgreSQL 18.4's output over the same
    /// rows.
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

    // ---- json_agg / json_object_agg: the `json` twins of the jsonb pair ----

    /// A relation carrying one `json` document and the same text as `jsonb`, so
    /// a single fold shows both what `json` preserves and what `jsonb` discards.
    fn json_doc_table() -> Table {
        Table {
            id: 4,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![
                Column::new("k", ColumnType::Int4),
                Column::new("doc", ColumnType::Json),
                Column::new("jb", ColumnType::Jsonb),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    /// The same two documents as `json` and as `jsonb`. Both have insignificant
    /// spacing and one has its keys out of order, which is the whole point.
    fn json_doc_rows() -> Vec<Vec<Datum>> {
        [r#"{"b":1,  "a":2}"#, "[3,   4]"]
            .into_iter()
            .map(|text| {
                r(&[
                    Datum::Int4(1),
                    Datum::Json(text.to_string()),
                    Datum::Jsonb(crabka_pgtypes::jsonb::parse(text).expect("jsonb literal")),
                ])
            })
            .collect()
    }

    /// `json_agg`/`json_object_agg` return `json`, and their `jsonb` twins are
    /// untouched. The result type is what RowDescription reports, so getting it
    /// wrong would send `jsonb`'s OID for a `json` column.
    #[test]
    fn json_aggregates_return_json_and_jsonb_ones_still_return_jsonb() {
        let t = collect_table();
        let scope = scope_of(Some(&t));
        let infer = |sql: &str| {
            let s = parsed_select(sql);
            let SelectItem::Expr { expr, .. } = &s.projection[0] else {
                panic!("expected an expression projection")
            };
            crate::eval::infer_type(expr, &scope)
        };
        for (sql, want) in [
            ("SELECT json_agg(v) FROM t", ColumnType::Json),
            ("SELECT json_agg(s) FROM t", ColumnType::Json),
            ("SELECT json_object_agg(s, v) FROM t", ColumnType::Json),
            // A scalar key of any type sits on PostgreSQL's `"any"` parameter.
            ("SELECT json_object_agg(v, s) FROM t", ColumnType::Json),
            ("SELECT jsonb_agg(v) FROM t", ColumnType::Jsonb),
            ("SELECT jsonb_object_agg(s, v) FROM t", ColumnType::Jsonb),
        ] {
            assert2::assert!(infer(sql).expect(sql) == want, "{sql}");
        }
    }

    /// Every argument shape PostgreSQL 18.4 answers with 42883 (`function
    /// json_agg(…) does not exist`) — including the bare `json_agg()` and the
    /// `json_agg(*)` that parses into the same zero-argument call.
    #[test]
    fn json_aggregates_reject_the_wrong_arity_at_plan_time() {
        let t = collect_table();
        let scope = scope_of(Some(&t));
        let infer = |sql: &str| {
            let s = parsed_select(sql);
            let SelectItem::Expr { expr, .. } = &s.projection[0] else {
                panic!("expected an expression projection")
            };
            crate::eval::infer_type(expr, &scope)
        };
        for sql in [
            "SELECT json_agg() FROM t",
            "SELECT json_agg(*) FROM t",
            "SELECT json_agg(v, s) FROM t",
            "SELECT json_object_agg(s) FROM t",
            "SELECT json_object_agg() FROM t",
            "SELECT json_object_agg(k, s, v) FROM t",
        ] {
            let err = infer(sql).expect_err(sql).into_pg();
            assert2::assert!(err.code == "42883", "{sql}: {err:?}");
        }
    }

    /// `json_agg` is `jsonb_agg`'s twin in everything but rendering: same input
    /// order, same JSON `null` for a NULL input, same NULL over zero rows — and a
    /// different spacing, which for `json_object_agg` alone pads its braces.
    /// Every expected string is PostgreSQL 18.4's output over the same rows.
    #[test]
    fn json_aggregates_render_postgres_json_spacing() {
        let t = collect_table();
        for (sql, want) in [
            ("SELECT json_agg(v) FROM t", "[30, 10, null]"),
            ("SELECT json_agg(s) FROM t", r#"["c", "a", "b"]"#),
            (
                "SELECT json_object_agg(s, v) FROM t",
                r#"{ "c" : 30, "a" : 10, "b" : null }"#,
            ),
            // The `jsonb` pair keeps sorting its keys and tightening its spacing.
            ("SELECT jsonb_agg(v) FROM t", "[30, 10, null]"),
            (
                "SELECT jsonb_object_agg(s, v) FROM t",
                r#"{"a": 10, "b": null, "c": 30}"#,
            ),
        ] {
            assert2::assert!(
                agg_text(sql, Some(&t), collect_rows()).expect(sql)
                    == vec![vec![Some(want.to_string())]],
                "{sql}"
            );
        }
    }

    /// Over zero rows both are SQL NULL, not an empty `[]`/`{}` — and NULL even
    /// when a row would have raised, so the key checks never run for an empty
    /// group.
    #[test]
    fn json_aggregates_over_zero_rows_are_null() {
        let t = collect_table();
        for sql in [
            "SELECT json_agg(v) FROM t",
            "SELECT json_object_agg(s, v) FROM t",
            "SELECT json_object_agg(ARRAY[v], s) FROM t",
        ] {
            assert2::assert!(
                agg_text(sql, Some(&t), Vec::new()).expect(sql) == vec![vec![None]],
                "{sql}"
            );
        }
    }

    /// A `json` element is INLINED verbatim — spacing, key order and all — where
    /// the same text as `jsonb` comes back sorted and re-spaced. This is the one
    /// behaviour that makes `json_agg` more than a differently-typed alias.
    #[test]
    fn json_agg_inlines_a_json_element_verbatim() {
        let t = json_doc_table();
        for (sql, want) in [
            (
                "SELECT json_agg(doc) FROM t",
                r#"[{"b":1,  "a":2}, [3,   4]]"#,
            ),
            (
                "SELECT jsonb_agg(jb) FROM t",
                r#"[{"a": 2, "b": 1}, [3, 4]]"#,
            ),
            (
                "SELECT json_object_agg(k, doc) FROM t",
                r#"{ "1" : {"b":1,  "a":2}, "1" : [3,   4] }"#,
            ),
        ] {
            assert2::assert!(
                agg_text(sql, Some(&t), json_doc_rows()).expect(sql)
                    == vec![vec![Some(want.to_string())]],
                "{sql}"
            );
        }
    }

    /// `json_agg_transfn`'s "add some whitespace if structured type and not
    /// first item": an ARRAY or composite element is preceded by a line feed,
    /// and nothing else is — not a scalar, and not a `json` element that merely
    /// happens to hold an array. `jsonb_agg` and `json_object_agg` never do it.
    #[test]
    fn json_agg_line_feeds_before_a_structured_element_only() {
        let t = collect_table();
        assert2::assert!(
            agg_text("SELECT json_agg(ARRAY[v]) FROM t", Some(&t), collect_rows())
                .expect("array elements")
                == vec![vec![Some("[[30], \n [10], \n [null]]".to_string())]]
        );
        // A `json` element holding an array is JSONTYPE_JSON, not an array, so
        // it gets the plain separator.
        let j = json_doc_table();
        assert2::assert!(
            agg_text("SELECT json_agg(doc) FROM t", Some(&j), json_doc_rows())
                .expect("json elements")
                == vec![vec![Some(r#"[{"b":1,  "a":2}, [3,   4]]"#.to_string())]]
        );
        // Neither the jsonb twin nor the object form wraps anything.
        assert2::assert!(
            agg_text(
                "SELECT jsonb_agg(ARRAY[v]) FROM t",
                Some(&t),
                collect_rows()
            )
            .expect("jsonb arrays")
                == vec![vec![Some("[[30], [10], [null]]".to_string())]]
        );
        assert2::assert!(
            agg_text(
                "SELECT json_object_agg(s, ARRAY[v]) FROM t",
                Some(&t),
                collect_rows()
            )
            .expect("object arrays")
                == vec![vec![Some(
                    r#"{ "c" : [30], "a" : [10], "b" : [null] }"#.to_string()
                )]]
        );
    }

    /// `json` has no notion of a duplicate key, so `json_object_agg` emits every
    /// pair it folded; `jsonb_object_agg` over the same rows keeps only the last
    /// value for each key. `DISTINCT` still sorts and dedups the whole PAIR, so
    /// a key with two values survives twice.
    #[test]
    fn json_object_agg_keeps_duplicate_keys_where_jsonb_collapses_them() {
        let t = collect_table();
        let rows = || {
            vec![
                r(&[Datum::Int4(1), Datum::Int4(30), Datum::Text("c".into())]),
                r(&[Datum::Int4(1), Datum::Int4(10), Datum::Text("a".into())]),
                r(&[Datum::Int4(1), Datum::Int4(20), Datum::Text("a".into())]),
                r(&[Datum::Int4(1), Datum::Int4(30), Datum::Text("c".into())]),
            ]
        };
        for (sql, want) in [
            (
                "SELECT json_object_agg(s, v) FROM t",
                r#"{ "c" : 30, "a" : 10, "a" : 20, "c" : 30 }"#,
            ),
            (
                "SELECT json_object_agg(DISTINCT s, v) FROM t",
                r#"{ "a" : 10, "a" : 20, "c" : 30 }"#,
            ),
            ("SELECT json_agg(s) FROM t", r#"["c", "a", "a", "c"]"#),
            ("SELECT json_agg(DISTINCT s) FROM t", r#"["a", "c"]"#),
            // The jsonb twin still collapses to one value per key.
            (
                "SELECT jsonb_object_agg(s, v) FROM t",
                r#"{"a": 20, "c": 30}"#,
            ),
        ] {
            assert2::assert!(
                agg_text(sql, Some(&t), rows()).expect(sql) == vec![vec![Some(want.to_string())]],
                "{sql}"
            );
        }
    }

    /// `json_object_agg`'s key is rendered through the JSON conversion and then
    /// quoted, so it follows the JSON spelling rather than the SQL one — `true`
    /// not `t`, and a numeric keeping its scale. Each expected object is
    /// PostgreSQL 18.4's output over the same key.
    #[test]
    fn json_object_agg_quotes_any_scalar_key() {
        let t = collect_table();
        for (key, want) in [
            (Datum::Int4(7), r#"{ "7" : 30 }"#),
            (Datum::Int8(7), r#"{ "7" : 30 }"#),
            (Datum::Float8(1.5), r#"{ "1.5" : 30 }"#),
            (Datum::Float8(f64::NAN), r#"{ "NaN" : 30 }"#),
            (Datum::Bool(true), r#"{ "true" : 30 }"#),
            (
                Datum::Numeric(crabka_pgtypes::numeric::parse("1.50").expect("numeric")),
                r#"{ "1.50" : 30 }"#,
            ),
            (
                Datum::Date(jiff::civil::date(2020, 1, 2)),
                r#"{ "2020-01-02" : 30 }"#,
            ),
            // A quote inside the key is escaped, not dropped.
            (Datum::Text(r#"a"b"#.into()), r#"{ "a\"b" : 30 }"#),
            (Datum::Text(String::new()), r#"{ "" : 30 }"#),
        ] {
            let rows = vec![r(&[Datum::Int4(1), Datum::Int4(30), key.clone()])];
            // `s` is the text column; a non-text key rides in as a constant.
            let sql = "SELECT json_object_agg(s, v) FROM t";
            let mut t2 = t.clone();
            t2.columns[2].ty = key.column_type().expect("a typed key");
            assert2::assert!(
                agg_text(sql, Some(&t2), rows).expect(sql) == vec![vec![Some(want.to_string())]],
                "{key:?}"
            );
        }
    }

    /// The two run-time key rejections, and the SQLSTATE that is NOT shared with
    /// the `jsonb` twin: a NULL `json_object_agg` key is 22004 where
    /// `jsonb_object_agg` raises 22023. A container key is 22023 for both.
    #[test]
    fn json_object_agg_rejects_null_and_container_keys() {
        let t = collect_table();
        let with_null_key = || vec![r(&[Datum::Int4(1), Datum::Int4(10), Datum::Null])];
        let err = agg_text(
            "SELECT json_object_agg(s, v) FROM t",
            Some(&t),
            with_null_key(),
        )
        .expect_err("null key")
        .into_pg();
        assert2::assert!(
            (err.code.as_str(), err.message.as_str())
                == ("22004", "null value not allowed for object key"),
            "{err:?}"
        );
        // The jsonb twin keeps its own (different) SQLSTATE.
        assert2::assert!(
            agg_text(
                "SELECT jsonb_object_agg(s, v) FROM t",
                Some(&t),
                with_null_key()
            )
            .expect_err("null key")
            .into_pg()
            .code
                == "22023"
        );
        // A container key is 22023 with PostgreSQL's wording, for both families.
        let container = "key value must be scalar, not array, composite, or json";
        for sql in [
            "SELECT json_object_agg(ARRAY[v], s) FROM t",
            "SELECT json_object_agg(to_jsonb(v), s) FROM t",
            "SELECT jsonb_object_agg(ARRAY[v], s) FROM t",
        ] {
            let err = agg_text(sql, Some(&t), collect_rows())
                .expect_err(sql)
                .into_pg();
            assert2::assert!(
                (err.code.as_str(), err.message.as_str()) == ("22023", container),
                "{sql}: {err:?}"
            );
        }
        // A `json` document is a container key too, not a scalar that happens to
        // hold text.
        let j = json_doc_table();
        let err = agg_text(
            "SELECT json_object_agg(doc, k) FROM t",
            Some(&j),
            json_doc_rows(),
        )
        .expect_err("json key")
        .into_pg();
        assert2::assert!(
            (err.code.as_str(), err.message.as_str()) == ("22023", container),
            "{err:?}"
        );
    }

    /// `FILTER` runs before the fold, so a rejected row never reaches the JSON
    /// document at all. Both expected strings are PostgreSQL 18.4's.
    #[test]
    fn json_aggregates_respect_filter() {
        let t = collect_table();
        for (sql, want) in [
            ("SELECT json_agg(v) FILTER (WHERE v > 15) FROM t", "[30]"),
            (
                "SELECT json_object_agg(s, v) FILTER (WHERE v > 15) FROM t",
                r#"{ "c" : 30 }"#,
            ),
        ] {
            assert2::assert!(
                agg_text(sql, Some(&t), collect_rows()).expect(sql)
                    == vec![vec![Some(want.to_string())]],
                "{sql}"
            );
        }
    }

    /// PostgreSQL accepts `json_agg(x) OVER ()` — an aggregate used as a window
    /// function is legal, and it is `is_aggregate_name` that tells the window
    /// planner so. Without the name registered the call would be 42883.
    #[test]
    fn json_aggregate_names_are_known_to_the_window_planner() {
        for name in [
            "json_agg",
            "json_object_agg",
            "jsonb_agg",
            "jsonb_object_agg",
        ] {
            assert2::assert!(is_aggregate_name(name), "{name}");
        }
        // The `_strict`/`_unique` variants PostgreSQL also has are NOT
        // implemented, so they stay 42883 rather than silently aggregating.
        for name in [
            "json_agg_strict",
            "json_object_agg_strict",
            "json_object_agg_unique",
            "json_object_agg_unique_strict",
            "jsonb_agg_strict",
            "jsonb_object_agg_strict",
            "jsonb_object_agg_unique",
            "jsonb_object_agg_unique_strict",
        ] {
            assert2::assert!(!is_aggregate_name(name), "{name}");
        }
    }

    /// One statistical-aggregate case: the SQL, the input rows, and the text of
    /// each expected output column.
    type StatCase<'a> = (&'a str, Vec<Vec<Datum>>, &'a [&'a str]);

    /// A table shaped for the aggregate families added beside the scalar
    /// breadth work. It holds a text value, a boolean, an integer, a numeric,
    /// and the `(y, x)` pair the two-variable statistics take.
    fn stats_table() -> Table {
        Table {
            id: 2,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![
                Column::new("s", ColumnType::Text),
                Column::new("b", ColumnType::Bool),
                Column::new("i", ColumnType::Int4),
                Column::new("q", ColumnType::Numeric(None)),
                Column::new("y", ColumnType::Float8),
                Column::new("x", ColumnType::Float8),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    /// The rows every statistical expectation below was measured against on a
    /// PostgreSQL 18.4 oracle.
    ///
    /// Row 3 has a NULL text, bool and int, and row 4 has a NULL `y`, so these
    /// rows exercise each family's NULL rule.
    fn stats_rows() -> Vec<Vec<Datum>> {
        let num = |n: i64| Datum::Numeric(crabka_pgtypes::numeric::from_i64(n));
        vec![
            vec![
                Datum::Text("a".into()),
                Datum::Bool(true),
                Datum::Int4(3),
                num(1),
                Datum::Float8(1.0),
                Datum::Float8(1.0),
            ],
            vec![
                Datum::Text("b".into()),
                Datum::Bool(false),
                Datum::Int4(5),
                num(2),
                Datum::Float8(2.0),
                Datum::Float8(2.0),
            ],
            vec![
                Datum::Null,
                Datum::Null,
                Datum::Null,
                num(3),
                Datum::Float8(3.0),
                Datum::Float8(4.0),
            ],
            vec![
                Datum::Text("c".into()),
                Datum::Bool(true),
                Datum::Int4(6),
                num(4),
                Datum::Null,
                Datum::Float8(5.0),
            ],
        ]
    }

    /// One row of text-format cells for a query over [`stats_rows`].
    fn stats_row(sql: &str) -> Vec<Option<String>> {
        let t = stats_table();
        let mut rows = agg_text(sql, Some(&t), stats_rows()).expect(sql);
        assert2::assert!(rows.len() == 1, "{sql}");
        rows.pop().expect("one row")
    }

    fn cells(values: &[&str]) -> Vec<Option<String>> {
        values
            .iter()
            .map(|v| {
                if *v == "<null>" {
                    None
                } else {
                    Some((*v).to_string())
                }
            })
            .collect()
    }

    /// Every expectation here is a PostgreSQL 18.4 value. `string_agg` skips
    /// NULLs. The boolean pair returns NULL only when no non-NULL row arrived.
    /// The bitwise trio folds in the argument's own width.
    #[test]
    fn collecting_boolean_and_bitwise_aggregates_match_postgres() {
        use assert2::assert;
        let cases: [(&str, &[&str]); 6] = [
            ("SELECT string_agg(s, ',') FROM t", &["a,b,c"]),
            ("SELECT string_agg(s, '') FROM t", &["abc"]),
            (
                "SELECT bool_and(b), bool_or(b), every(b) FROM t",
                &["f", "t", "f"],
            ),
            (
                "SELECT bit_and(i), bit_or(i), bit_xor(i) FROM t",
                &["0", "7", "0"],
            ),
            ("SELECT string_agg(DISTINCT s, ',') FROM t", &["a,b,c"]),
            ("SELECT upper(string_agg(s, ',')) FROM t", &["A,B,C"]),
        ];
        for (sql, expected) in cases {
            assert!(stats_row(sql) == cells(expected), "{sql}");
        }
    }

    /// Zero qualifying rows is SQL NULL for every one of these. It is not an
    /// empty string, not zero, and not an error.
    #[test]
    fn the_new_aggregates_are_null_over_zero_rows() {
        use assert2::assert;
        let t = stats_table();
        let sqls = [
            "SELECT string_agg(s, ',') FROM t",
            "SELECT bool_and(b) FROM t",
            "SELECT bool_or(b) FROM t",
            "SELECT bit_and(i) FROM t",
            "SELECT var_pop(i) FROM t",
            "SELECT stddev(i) FROM t",
            "SELECT corr(y, x) FROM t",
        ];
        for sql in sqls {
            assert!(
                agg_text(sql, Some(&t), Vec::new()).expect(sql) == vec![vec![None]],
                "{sql}"
            );
        }
        // regr_count is the exception: it counts, so zero rows is zero.
        assert!(
            agg_text("SELECT regr_count(y, x) FROM t", Some(&t), Vec::new()).expect("regr_count")
                == vec![vec![Some("0".to_string())]]
        );
    }

    /// The `numeric` variance/stddev display scale comes from PostgreSQL's
    /// `select_div_scale`, and the `float8` scale comes from its Youngs-Cramer
    /// transition. The printed digits, and not only the value, are the test.
    #[test]
    fn statistical_aggregates_match_postgres_digit_for_digit() {
        use assert2::assert;
        let cases: [(&str, &[&str]); 6] = [
            (
                "SELECT var_pop(i), var_samp(i), stddev_pop(i), stddev_samp(i) FROM t",
                &[
                    "1.5555555555555556",
                    "2.3333333333333333",
                    "1.2472191289246471",
                    "1.5275252316519467",
                ],
            ),
            (
                "SELECT var_pop(q), stddev_pop(q) FROM t",
                &["1.2500000000000000", "1.1180339887498948"],
            ),
            (
                "SELECT var_pop(y), var_samp(y), stddev_pop(y), stddev_samp(y) FROM t",
                &["0.6666666666666666", "1", "0.816496580927726", "1"],
            ),
            (
                "SELECT corr(y, x), covar_pop(y, x), covar_samp(y, x) FROM t",
                &["0.9819805060619659", "1", "1.5"],
            ),
            (
                "SELECT regr_count(y, x), regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM t",
                &["3", "4.666666666666666", "2", "3"],
            ),
            (
                "SELECT regr_avgx(y, x), regr_avgy(y, x), regr_slope(y, x), \
                 regr_intercept(y, x), regr_r2(y, x) FROM t",
                &[
                    "2.3333333333333335",
                    "2",
                    "0.6428571428571429",
                    "0.4999999999999997",
                    "0.9642857142857144",
                ],
            ),
        ];
        for (sql, expected) in cases {
            assert!(stats_row(sql) == cells(expected), "{sql}");
        }
    }

    /// `variance`/`stddev` are PostgreSQL's aliases for the SAMPLE forms, and
    /// `every` is its alias for `bool_and`. Each pair must agree exactly.
    #[test]
    fn the_aggregate_aliases_agree_with_what_they_alias() {
        use assert2::assert;
        let pairs = [
            ("variance(i)", "var_samp(i)"),
            ("stddev(i)", "stddev_samp(i)"),
            ("variance(y)", "var_samp(y)"),
            ("stddev(y)", "stddev_samp(y)"),
            ("every(b)", "bool_and(b)"),
        ];
        for (alias, canonical) in pairs {
            let sql = format!("SELECT {alias}, {canonical} FROM t");
            let row = stats_row(&sql);
            assert!(row[0] == row[1], "{sql}");
        }
    }

    /// The sample forms need two rows and the population forms one, so a
    /// single-row group is NULL for `var_samp` but zero for `var_pop`.
    #[test]
    fn the_sample_forms_need_two_rows() {
        use assert2::assert;
        let t = stats_table();
        let one = vec![stats_rows()[0].clone()];
        let row = agg_text(
            "SELECT var_pop(i), var_samp(i), stddev_pop(i), stddev_samp(i) FROM t",
            Some(&t),
            one.clone(),
        )
        .expect("one row");
        assert!(row == vec![cells(&["0", "<null>", "0", "<null>"])]);
        let row = agg_text(
            "SELECT covar_pop(y, x), covar_samp(y, x), corr(y, x) FROM t",
            Some(&t),
            one,
        )
        .expect("one row");
        assert!(row == vec![cells(&["0", "<null>", "<null>"])]);
    }

    /// One `(y, x)` row of the statistical table, with every other column NULL
    /// so that only the float pair reaches the accumulator.
    fn yx(y: f64, x: f64) -> Vec<Datum> {
        vec![
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Float8(y),
            Datum::Float8(x),
        ]
    }

    /// A NaN or ±Infinity anywhere in the input poisons PostgreSQL's
    /// Youngs–Cramer sums of squared deviations, so the whole variance and
    /// regression family answers NaN.
    ///
    /// An accumulator that drops the non-finite value reports a variance of
    /// zero instead. That is a wrong answer with nothing to signal it.
    #[test]
    fn non_finite_inputs_propagate_through_the_statistical_family() {
        use assert2::assert;
        let t = stats_table();
        let inf = f64::INFINITY;
        let nan = f64::NAN;
        let cases: Vec<StatCase<'_>> = vec![
            (
                "SELECT var_pop(y), var_samp(y), stddev_pop(y), stddev_samp(y) FROM t",
                vec![yx(inf, 0.0)],
                &["NaN", "<null>", "NaN", "<null>"],
            ),
            (
                "SELECT var_pop(y), var_samp(y), stddev_pop(y), stddev_samp(y) FROM t",
                vec![yx(nan, 0.0)],
                &["NaN", "<null>", "NaN", "<null>"],
            ),
            // A finite row first still has to end up NaN, which is the branch a
            // first-row-only guard would miss.
            (
                "SELECT sum(y), avg(y), var_pop(y) FROM t",
                vec![yx(1.0, 0.0), yx(inf, 0.0)],
                &["Infinity", "Infinity", "NaN"],
            ),
            (
                "SELECT sum(y), avg(y), var_pop(y) FROM t",
                vec![yx(inf, 0.0), yx(1.0, 0.0)],
                &["Infinity", "Infinity", "NaN"],
            ),
            (
                "SELECT sum(y), avg(y), var_pop(y) FROM t",
                vec![yx(-inf, 0.0), yx(inf, 0.0)],
                &["NaN", "NaN", "NaN"],
            ),
            (
                "SELECT covar_pop(y, x), covar_samp(y, x) FROM t",
                vec![yx(1.0, inf)],
                &["NaN", "<null>"],
            ),
            // Only the sums the non-finite argument feeds are poisoned: an
            // infinite x leaves regr_syy finite, and vice versa.
            (
                "SELECT corr(y, x), covar_pop(y, x), regr_sxx(y, x), regr_syy(y, x), \
                 regr_sxy(y, x), regr_avgx(y, x), regr_avgy(y, x), regr_count(y, x) FROM t",
                vec![yx(1.0, inf), yx(2.0, 3.0)],
                &["NaN", "NaN", "NaN", "0.5", "NaN", "Infinity", "1.5", "2"],
            ),
            (
                "SELECT corr(y, x), covar_pop(y, x), regr_sxx(y, x), regr_syy(y, x), \
                 regr_sxy(y, x) FROM t",
                vec![yx(nan, 1.0), yx(2.0, 3.0)],
                &["NaN", "NaN", "2", "NaN", "NaN"],
            ),
            // The finite path is unchanged: a zero spread is still zero, not NaN.
            (
                "SELECT var_pop(y), var_samp(y) FROM t",
                vec![yx(2.0, 0.0), yx(2.0, 0.0)],
                &["0", "0"],
            ),
        ];
        for (sql, rows, expected) in cases {
            assert!(
                agg_text(sql, Some(&t), rows).expect(sql) == vec![cells(expected)],
                "{sql}"
            );
        }
    }

    /// The same accumulator guard is 22003, not NaN, when every input was
    /// finite and the running sum overflowed. PostgreSQL reports the overflow
    /// only in that case.
    #[test]
    fn finite_inputs_that_overflow_the_variance_sums_are_22003() {
        use assert2::assert;
        let t = stats_table();
        let big = 1.0e308;
        let sqls = [
            "SELECT var_pop(y) FROM t",
            "SELECT stddev_samp(y) FROM t",
            "SELECT regr_sxx(y, x) FROM t",
            "SELECT corr(y, x) FROM t",
        ];
        for sql in sqls {
            let err = agg_text(
                sql,
                Some(&t),
                vec![yx(big, big), yx(-big, -big), yx(big, big)],
            )
            .expect_err(sql);
            assert!(err.into_pg().code == "22003", "{sql}");
        }
    }

    /// The argument types each family accepts, and what it reports back.
    ///
    /// `variance(int4)` is numeric, while `variance(float8)` stays float8.
    /// `bit_and` keeps the integer width it was given.
    #[test]
    fn new_aggregate_result_types_match_postgres() {
        use assert2::assert;
        let t = stats_table();
        let scope = scope_of(Some(&t));
        let cases = [
            ("string_agg(s, ',')", ColumnType::Text),
            ("bool_and(b)", ColumnType::Bool),
            ("every(b)", ColumnType::Bool),
            ("bit_and(i)", ColumnType::Int4),
            ("var_pop(i)", ColumnType::Numeric(None)),
            ("variance(q)", ColumnType::Numeric(None)),
            ("stddev(y)", ColumnType::Float8),
            ("var_samp(y)", ColumnType::Float8),
            ("corr(y, x)", ColumnType::Float8),
            ("regr_count(y, x)", ColumnType::Int8),
            ("regr_slope(y, x)", ColumnType::Float8),
        ];
        for (call, expected) in cases {
            let expr = crabka_pgparser::parser::parse_expr_for_test(call).expect("parse");
            let Expr::Func(fc) = &expr else {
                panic!("{call} is not a function call")
            };
            assert!(
                func_result_type(fc, &scope).expect(call) == expected,
                "{call}"
            );
        }
        // A type outside the family is 42883 rather than a silent coercion.
        for call in [
            "bool_and(i)",
            "bit_and(s)",
            "var_pop(s)",
            "string_agg(i, s)",
        ] {
            let expr = crabka_pgparser::parser::parse_expr_for_test(call).expect("parse");
            let Expr::Func(fc) = &expr else {
                panic!("{call} is not a function call")
            };
            assert!(
                func_result_type(fc, &scope).expect_err(call).into_pg().code == "42883",
                "{call}"
            );
        }
    }
}
