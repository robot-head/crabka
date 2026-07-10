//! SP29: scalar (row) functions + the `||` concatenation operator.
//!
//! Like SP27's aggregates and SP28's predicates, every function here is a pure,
//! deterministic transform over a *single row's* already-MVCC-resolved Datums.
//! A whole table lives on one range (`RangeMap::range_for_table`), so a scalar
//! function executes entirely inside one `execute_read`/`eval` on one engine —
//! no cross-range scatter, no new lock/visibility rule, no new interleaving. This
//! is exactly CLAUDE.md's "pure-data / single-node refactor" carve-out, so SP29
//! ships NO Stateright model (a model of a scalar fold would have an
//! interleaving-free state space and merely restate these unit tests).
//!
//! The dispatch mirrors SP28: the pure combinators are shared between scalar
//! `eval` and the grouped evaluator (`agg::eval_grouped`); only the child-eval
//! closure differs (`eval_scalar` takes it as `FnMut(&Expr) -> Result<Datum>`).
//!
//! Supported: string `length`/`char_length`/`character_length`, `upper`,
//! `lower`, `btrim`/`ltrim`/`rtrim`, `substr`/`substring` (the comma form),
//! `replace`, `concat`; math `abs`, `mod`; null/conditional `coalesce`,
//! `nullif`, `greatest`, `least`. `||` is a binary operator handled in `eval`.

use std::cmp::Ordering;

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{ColumnType, Datum, ops};

use crate::{clock::EvalCtx, error::ExecError, scope::Scope};

/// The scalar functions SP29 supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarFunc {
    Length,
    Upper,
    Lower,
    Btrim,
    Ltrim,
    Rtrim,
    Substr,
    Replace,
    Concat,
    Abs,
    Mod,
    Coalesce,
    NullIf,
    Greatest,
    Least,
    // SP33: rounding family (type-preserving).
    Floor,
    Ceil,
    Round,
    Trunc,
    Sign,
    // SP33: transcendental family (always float8).
    Sqrt,
    Power,
    Exp,
    Ln,
    Log,
    Pi,
    // SP33: string family.
    Lpad,
    Rpad,
    Left,
    Right,
    Repeat,
    Reverse,
    Strpos,
    Initcap,
    Ascii,
    Chr,
    CurrentSetting,
    SetConfig,
    CurrentDatabase,
    CurrentSchema,
    CurrentUser,
    SessionUser,
    Version,
    FormatType,
    PgTableIsVisible,
    NextVal,
    CurrVal,
    SetVal,
}

/// Classify a (lowercased — the lexer lowercases unquoted idents) function name.
/// `None` means "not a known scalar function"; the caller then tries the
/// aggregate path / reports an undefined function.
fn scalar_func(name: &str) -> Option<ScalarFunc> {
    Some(match name {
        "length" | "char_length" | "character_length" => ScalarFunc::Length,
        "upper" => ScalarFunc::Upper,
        "lower" => ScalarFunc::Lower,
        "btrim" => ScalarFunc::Btrim,
        "ltrim" => ScalarFunc::Ltrim,
        "rtrim" => ScalarFunc::Rtrim,
        "substr" | "substring" => ScalarFunc::Substr,
        "replace" => ScalarFunc::Replace,
        "concat" => ScalarFunc::Concat,
        "abs" => ScalarFunc::Abs,
        "mod" => ScalarFunc::Mod,
        "coalesce" => ScalarFunc::Coalesce,
        "nullif" => ScalarFunc::NullIf,
        "greatest" => ScalarFunc::Greatest,
        "least" => ScalarFunc::Least,
        "floor" => ScalarFunc::Floor,
        "ceil" | "ceiling" => ScalarFunc::Ceil,
        "round" => ScalarFunc::Round,
        "trunc" => ScalarFunc::Trunc,
        "sign" => ScalarFunc::Sign,
        "sqrt" => ScalarFunc::Sqrt,
        "power" | "pow" => ScalarFunc::Power,
        "exp" => ScalarFunc::Exp,
        "ln" => ScalarFunc::Ln,
        "log" => ScalarFunc::Log,
        "pi" => ScalarFunc::Pi,
        "lpad" => ScalarFunc::Lpad,
        "rpad" => ScalarFunc::Rpad,
        "left" => ScalarFunc::Left,
        "right" => ScalarFunc::Right,
        "repeat" => ScalarFunc::Repeat,
        "reverse" => ScalarFunc::Reverse,
        "strpos" => ScalarFunc::Strpos,
        "initcap" => ScalarFunc::Initcap,
        "ascii" => ScalarFunc::Ascii,
        "chr" => ScalarFunc::Chr,
        "current_setting" => ScalarFunc::CurrentSetting,
        "set_config" => ScalarFunc::SetConfig,
        "current_database" => ScalarFunc::CurrentDatabase,
        "current_schema" => ScalarFunc::CurrentSchema,
        "current_user" => ScalarFunc::CurrentUser,
        "session_user" => ScalarFunc::SessionUser,
        "version" => ScalarFunc::Version,
        "format_type" => ScalarFunc::FormatType,
        "pg_table_is_visible" => ScalarFunc::PgTableIsVisible,
        "nextval" => ScalarFunc::NextVal,
        "currval" => ScalarFunc::CurrVal,
        "setval" => ScalarFunc::SetVal,
        _ => return None,
    })
}

/// Is `name` a known scalar function? (The dispatch point in `eval`/`infer_type`.)
pub(crate) fn is_scalar(name: &str) -> bool {
    scalar_func(name).is_some()
}

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

/// `DISTINCT`/`ALL` is only meaningful for aggregates (PostgreSQL 42809). Our
/// parser discards `ALL`, so only an explicit `DISTINCT` reaches here.
fn distinct_not_aggregate(name: &str) -> ExecError {
    ExecError::WrongObjectType(format!(
        "DISTINCT specified, but {name} is not an aggregate function"
    ))
}

/// The positional argument list of a scalar call. `f(*)` is never valid for a
/// scalar function (only `count(*)`), so it is an undefined-function error.
fn exprs_of(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    match &fc.args {
        FuncArgs::Exprs(v) => Ok(v),
        FuncArgs::Star => Err(undefined_function(&fc.name)),
    }
}

/// Reject the `DISTINCT` modifier (42809) and return the call's argument list.
/// Shared front-door check for both `scalar_result_type` and `eval_scalar`.
fn checked_args(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    if fc.distinct {
        return Err(distinct_not_aggregate(&fc.name));
    }
    exprs_of(fc)
}

/// Statically infer a scalar call's result type (for RowDescription), validating
/// name, arity, and — where the result type depends on them or the function is
/// strictly typed — argument types. A bad name/arity/argument type is 42883.
///
/// NB: this runs for PROJECTED expressions (via `resolve_projection`). A scalar
/// function appearing only in `WHERE`/`HAVING`/`ORDER BY` is evaluated without a
/// separate type-resolution pass (a pre-existing trait of the engine — the same
/// is true of arithmetic), so an argument-type misuse THERE surfaces at runtime
/// as 42804 rather than here as 42883. This per-clause difference is documented.
pub(crate) fn scalar_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    let f = scalar_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let n = args.len();
    match f {
        ScalarFunc::Length => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Upper | ScalarFunc::Lower => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Btrim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            require_arity(fc, n == 1 || n == 2)?;
            for a in args {
                require_text(a, scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Substr => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], scope)?;
            for a in &args[1..] {
                require_int(a, scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Replace => {
            require_arity(fc, n == 3)?;
            for a in args {
                require_text(a, scope)?;
            }
            Ok(ColumnType::Text)
        }
        // concat takes any number of arguments of any (non-array) type.
        ScalarFunc::Concat => Ok(ColumnType::Text),
        ScalarFunc::Abs => {
            require_arity(fc, n == 1)?;
            // abs preserves the numeric type (int width, or SP30's float8).
            require_numeric(&args[0], scope)
        }
        ScalarFunc::Mod => {
            require_arity(fc, n == 2)?;
            // SP32: mod takes int OR numeric operands (PostgreSQL has no float8 mod);
            // a numeric operand makes the result numeric, else the int promotion.
            let lt = require_int_or_numeric(&args[0], scope)?;
            let rt = require_int_or_numeric(&args[1], scope)?;
            if lt.is_numeric() || rt.is_numeric() {
                Ok(ColumnType::Numeric(None))
            } else {
                Ok(promote(lt, rt))
            }
        }
        ScalarFunc::Coalesce | ScalarFunc::Greatest | ScalarFunc::Least => {
            require_arity(fc, n >= 1)?;
            unify_args(args, scope)
        }
        ScalarFunc::NullIf => {
            require_arity(fc, n == 2)?;
            // NULLIF's result is the first argument's type (a bare NULL → text).
            if matches!(args[0], Expr::NullLiteral) {
                Ok(ColumnType::Text)
            } else {
                crate::eval::infer_type(&args[0], scope)
            }
        }
        ScalarFunc::Floor | ScalarFunc::Ceil | ScalarFunc::Sign => {
            require_arity(fc, n == 1)?;
            // preserves the input numeric type (int4/int8/float8/numeric).
            require_numeric(&args[0], scope)
        }
        ScalarFunc::Round | ScalarFunc::Trunc => {
            require_arity(fc, n == 1 || n == 2)?;
            if n == 1 {
                require_numeric(&args[0], scope)
            } else {
                // two-arg: numeric (or int promoted to numeric) first arg, int
                // second arg, → numeric. A float8 first arg has no 2-arg form.
                let t0 = require_numeric(&args[0], scope)?;
                if t0 == ColumnType::Float8 {
                    return Err(no_matching_function());
                }
                require_int(&args[1], scope)?;
                Ok(ColumnType::Numeric(None))
            }
        }
        ScalarFunc::Sqrt | ScalarFunc::Exp | ScalarFunc::Ln | ScalarFunc::Log => {
            require_arity(fc, n == 1)?;
            let at = require_numeric(&args[0], scope)?;
            Ok(if at.is_numeric() {
                ColumnType::Numeric(None)
            } else {
                ColumnType::Float8
            })
        }
        ScalarFunc::Power => {
            require_arity(fc, n == 2)?;
            let a = require_numeric(&args[0], scope)?;
            let b = require_numeric(&args[1], scope)?;
            Ok(power_result_type(a, b))
        }
        ScalarFunc::Pi => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Float8)
        }
        ScalarFunc::Lpad | ScalarFunc::Rpad => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], scope)?;
            require_int(&args[1], scope)?;
            if n == 3 {
                require_text(&args[2], scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Left | ScalarFunc::Right | ScalarFunc::Repeat => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], scope)?;
            require_int(&args[1], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Reverse | ScalarFunc::Initcap => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Strpos => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Ascii => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Chr => {
            require_arity(fc, n == 1)?;
            require_int(&args[0], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::CurrentSetting => {
            require_arity(fc, n == 1 || n == 2)?;
            require_text(&args[0], scope)?;
            if n == 2 {
                require_bool(&args[1], scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::SetConfig => {
            require_arity(fc, n == 3)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            require_bool(&args[2], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::CurrentDatabase
        | ScalarFunc::CurrentSchema
        | ScalarFunc::CurrentUser
        | ScalarFunc::SessionUser
        | ScalarFunc::Version => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::FormatType => {
            require_arity(fc, n == 2)?;
            require_int(&args[0], scope)?;
            require_int(&args[1], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::PgTableIsVisible => {
            require_arity(fc, n == 1)?;
            require_int(&args[0], scope)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::NextVal | ScalarFunc::CurrVal => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Int8)
        }
        ScalarFunc::SetVal => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], scope)?;
            require_int(&args[1], scope)?;
            if n == 3 {
                require_bool(&args[2], scope)?;
            }
            Ok(ColumnType::Int8)
        }
    }
}

/// Evaluate a scalar call. `eval_child` evaluates each argument expression
/// against the current row — the SAME `eval` for scalar context and
/// `agg::eval_grouped` for a grouped context, so the combinators are shared and
/// only the closure differs. Short-circuiting functions (`coalesce`) and the
/// lazy ones evaluate arguments only as far as needed.
pub(crate) fn eval_scalar(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = scalar_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    match f {
        // coalesce returns the first non-NULL argument, NOT evaluating the rest
        // (so `coalesce(x, 1/0)` with x non-null never divides by zero).
        ScalarFunc::Coalesce => {
            require_arity(fc, !args.is_empty())?;
            for a in args {
                let v = eval_child(a)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Datum::Null)
        }
        ScalarFunc::Greatest | ScalarFunc::Least => {
            require_arity(fc, !args.is_empty())?;
            let want_greater = matches!(f, ScalarFunc::Greatest);
            let mut best: Option<Datum> = None;
            for a in args {
                let v = eval_child(a)?;
                if v.is_null() {
                    continue; // greatest/least ignore NULL arguments
                }
                best = Some(match best {
                    None => v,
                    Some(cur) => {
                        let replace = match ops::compare(&v, &cur)? {
                            Some(Ordering::Greater) => want_greater,
                            Some(Ordering::Less) => !want_greater,
                            _ => false, // Equal (both non-null, so never None)
                        };
                        if replace { v } else { cur }
                    }
                });
            }
            Ok(best.unwrap_or(Datum::Null))
        }
        ScalarFunc::NullIf => {
            require_arity(fc, args.len() == 2)?;
            let a = eval_child(&args[0])?;
            let b = eval_child(&args[1])?;
            // NULLIF(a, b) = NULL when a = b, else a. `compare` is None if either
            // is NULL (so a NULL `a` falls through to `Ok(a)` = NULL).
            match ops::compare(&a, &b)? {
                Some(Ordering::Equal) => Ok(Datum::Null),
                _ => Ok(a),
            }
        }
        // Eager, strict-or-concat functions: evaluate every argument first.
        _ => {
            let vals = args
                .iter()
                .map(&mut eval_child)
                .collect::<Result<Vec<_>, _>>()?;
            eval_eager(f, fc, &vals, ctx)
        }
    }
}

/// Apply an eager scalar function to its already-evaluated arguments. Every
/// function here except `concat` is strict (any NULL argument → NULL); `concat`
/// skips NULLs and never returns NULL.
fn eval_eager(
    f: ScalarFunc,
    fc: &FuncCall,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    if let ScalarFunc::Concat = f {
        // `concat` renders each argument via its canonical wire text encoding,
        // using the session zone from `ctx` (so `Timestamptz` agrees with DataRow).
        let tz = &ctx.time_zone;
        let mut s = String::new();
        for v in vals {
            if !v.is_null() {
                s.push_str(&text_render(v, tz));
            }
        }
        return Ok(Datum::Text(s));
    }
    // Strict: a NULL argument short-circuits to NULL.
    if vals.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    match f {
        ScalarFunc::Length => {
            require_arity(fc, vals.len() == 1)?;
            let n = text_arg(&vals[0])?.chars().count();
            i32::try_from(n)
                .map(Datum::Int4)
                .map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))
        }
        ScalarFunc::Upper => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(text_arg(&vals[0])?.to_uppercase()))
        }
        ScalarFunc::Lower => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(text_arg(&vals[0])?.to_lowercase()))
        }
        ScalarFunc::Btrim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            // The optional second argument is the set of characters to strip
            // (default: ASCII/Unicode whitespace).
            let trimmed = match vals.get(1) {
                None => trim_ws(f, s),
                Some(chars) => {
                    let set: Vec<char> = text_arg(chars)?.chars().collect();
                    trim_set(f, s, &set)
                }
            };
            Ok(Datum::Text(trimmed))
        }
        ScalarFunc::Substr => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let s = text_arg(&vals[0])?;
            let start = int_arg(&vals[1])?;
            let count = match vals.get(2) {
                None => None,
                Some(c) => Some(int_arg(c)?),
            };
            substr(s, start, count)
        }
        ScalarFunc::Replace => {
            require_arity(fc, vals.len() == 3)?;
            let (s, from, to) = (
                text_arg(&vals[0])?,
                text_arg(&vals[1])?,
                text_arg(&vals[2])?,
            );
            // PostgreSQL `replace` leaves the string unchanged when `from` is empty.
            let out = if from.is_empty() {
                s.to_string()
            } else {
                s.replace(from, to)
            };
            Ok(Datum::Text(out))
        }
        ScalarFunc::Abs => {
            require_arity(fc, vals.len() == 1)?;
            match &vals[0] {
                Datum::Int4(n) => n
                    .checked_abs()
                    .map(Datum::Int4)
                    .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow)),
                Datum::Int8(n) => n
                    .checked_abs()
                    .map(Datum::Int8)
                    .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow)),
                // SP30: abs over float8 (always representable, no overflow trap).
                Datum::Float8(f) => Ok(Datum::Float8(f.abs())),
                // SP32: abs over numeric.
                Datum::Numeric(d) => Ok(Datum::Numeric(crabka_pgtypes::numeric::abs(d))),
                other => Err(type_error("abs", other)),
            }
        }
        ScalarFunc::Mod => {
            require_arity(fc, vals.len() == 2)?;
            Ok(ops::rem(&vals[0], &vals[1])?)
        }
        ScalarFunc::Floor | ScalarFunc::Ceil | ScalarFunc::Sign => {
            require_arity(fc, vals.len() == 1)?;
            round_family(f, &vals[0], None)
        }
        ScalarFunc::Round | ScalarFunc::Trunc => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let scale = match vals.get(1) {
                None => None,
                Some(s) => Some(int_arg(s)?),
            };
            round_family(f, &vals[0], scale)
        }
        ScalarFunc::Sqrt => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_sqrt(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            if x < 0.0 {
                return Err(domain(
                    "2201F",
                    "cannot take square root of a negative number",
                ));
            }
            Ok(Datum::Float8(x.sqrt()))
        }
        ScalarFunc::Exp => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_exp(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            finite_or_overflow(as_f64(&vals[0])?.exp())
        }
        ScalarFunc::Ln => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_ln(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            if x <= 0.0 {
                return Err(domain(
                    "2201E",
                    "cannot take logarithm of a non-positive number",
                ));
            }
            Ok(Datum::Float8(x.ln()))
        }
        ScalarFunc::Log => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_log10(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            if x <= 0.0 {
                return Err(domain(
                    "2201E",
                    "cannot take logarithm of a non-positive number",
                ));
            }
            Ok(Datum::Float8(x.log10()))
        }
        ScalarFunc::Power => {
            require_arity(fc, vals.len() == 2)?;
            let any_num =
                matches!(&vals[0], Datum::Numeric(_)) || matches!(&vals[1], Datum::Numeric(_));
            let any_f64 =
                matches!(&vals[0], Datum::Float8(_)) || matches!(&vals[1], Datum::Float8(_));
            if any_num && !any_f64 {
                let b = to_numeric(&vals[0])?;
                let e = to_numeric(&vals[1])?;
                return crabka_pgtypes::numeric::num_power(&b, &e)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            power(as_f64(&vals[0])?, as_f64(&vals[1])?)
        }
        ScalarFunc::Pi => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Float8(std::f64::consts::PI))
        }
        ScalarFunc::Lpad | ScalarFunc::Rpad => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let s = text_arg(&vals[0])?;
            let width = int_arg(&vals[1])?;
            let fill = match vals.get(2) {
                None => " ",
                Some(d) => text_arg(d)?,
            };
            Ok(Datum::Text(pad(f, s, width, fill)?))
        }
        ScalarFunc::Left | ScalarFunc::Right => {
            require_arity(fc, vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            let n = int_arg(&vals[1])?;
            Ok(Datum::Text(left_right(f, s, n)))
        }
        ScalarFunc::Repeat => {
            require_arity(fc, vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            let n = int_arg(&vals[1])?;
            repeat_str(s, n)
        }
        ScalarFunc::Reverse => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(text_arg(&vals[0])?.chars().rev().collect()))
        }
        ScalarFunc::Initcap => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(initcap(text_arg(&vals[0])?)))
        }
        ScalarFunc::Strpos => {
            require_arity(fc, vals.len() == 2)?;
            Ok(Datum::Int4(strpos(
                text_arg(&vals[0])?,
                text_arg(&vals[1])?,
            )))
        }
        ScalarFunc::Ascii => {
            require_arity(fc, vals.len() == 1)?;
            let code = text_arg(&vals[0])?.chars().next().map_or(0, |c| c as i32);
            Ok(Datum::Int4(code))
        }
        ScalarFunc::Chr => {
            require_arity(fc, vals.len() == 1)?;
            chr(int_arg(&vals[0])?)
        }
        ScalarFunc::CurrentSetting => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let missing_ok = vals.get(1).map(bool_arg).transpose()?.unwrap_or(false);
            match crate::session::current_setting_runtime(text_arg(&vals[0])?, missing_ok)? {
                Some(value) => Ok(Datum::Text(value)),
                None => Ok(Datum::Null),
            }
        }
        ScalarFunc::SetConfig => {
            require_arity(fc, vals.len() == 3)?;
            let name = text_arg(&vals[0])?;
            let value = text_arg(&vals[1])?;
            let local = bool_arg(&vals[2])?;
            crate::session::set_config_runtime(name, value, local).map(Datum::Text)
        }
        ScalarFunc::NextVal => {
            require_arity(fc, vals.len() == 1)?;
            let name = text_arg(&vals[0])?.to_string();
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence functions require a SQL session".into())
            })?;
            let value = runtime.manager.nextval(&*runtime.kv, &name)?;
            runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .insert(name, value);
            Ok(Datum::Int8(value))
        }
        ScalarFunc::CurrVal => {
            require_arity(fc, vals.len() == 1)?;
            let name = text_arg(&vals[0])?;
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence functions require a SQL session".into())
            })?;
            let value = runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .get(name)
                .copied()
                .ok_or_else(|| {
                    ExecError::ObjectNotInPrerequisiteState(format!(
                        "currval of sequence \"{name}\" is not yet defined in this session"
                    ))
                })?;
            Ok(Datum::Int8(value))
        }
        ScalarFunc::SetVal => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let name = text_arg(&vals[0])?.to_string();
            let value = int_arg(&vals[1])?;
            let is_called = vals.get(2).map(bool_arg).transpose()?.unwrap_or(true);
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence functions require a SQL session".into())
            })?;
            let value = runtime
                .manager
                .setval(&*runtime.kv, &name, value, is_called)?;
            runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .insert(name, value);
            Ok(Datum::Int8(value))
        }
        ScalarFunc::CurrentDatabase => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text("postgres".into()))
        }
        ScalarFunc::CurrentSchema => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text("public".into()))
        }
        ScalarFunc::CurrentUser => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(ctx.current_user.clone()))
        }
        ScalarFunc::SessionUser => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(ctx.session_user.clone()))
        }
        ScalarFunc::Version => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text("PostgreSQL 18-compatible Crabka".into()))
        }
        ScalarFunc::FormatType => {
            require_arity(fc, vals.len() == 2)?;
            let oid = int_arg(&vals[0])?;
            let _typmod = int_arg(&vals[1])?;
            let name = i32::try_from(oid)
                .ok()
                .and_then(crate::exec::format_type_name)
                .unwrap_or("unknown");
            Ok(Datum::Text(name.into()))
        }
        ScalarFunc::PgTableIsVisible => {
            require_arity(fc, vals.len() == 1)?;
            let _oid = int_arg(&vals[0])?;
            Ok(Datum::Bool(true))
        }
        // concat / coalesce / nullif / greatest / least are handled before here.
        _ => unreachable!("non-eager scalar function reached eval_eager"),
    }
}

// ---- argument-type helpers ----

/// 42883 for an argument whose static type a function does not accept (PG's
/// "no function matches the given name and argument types").
fn no_matching_function() -> ExecError {
    ExecError::UndefinedFunction("no function matches the given name and argument types".into())
}

/// Require the argument to statically type as `text` (a bare `NULL` qualifies,
/// since it types as text); otherwise the function does not exist for it (42883).
fn require_text(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    match crate::eval::infer_type(arg, scope)? {
        ColumnType::Text => Ok(()),
        _ => Err(no_matching_function()),
    }
}

/// Require the argument to statically type as an integer; returns that width.
fn require_int(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    match crate::eval::infer_type(arg, scope)? {
        t @ (ColumnType::Int4 | ColumnType::Int8) => Ok(t),
        _ => Err(no_matching_function()),
    }
}

fn require_bool(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    match crate::eval::infer_type(arg, scope)? {
        ColumnType::Bool => Ok(()),
        _ => Err(no_matching_function()),
    }
}

/// SP32: require an int OR numeric argument (the `mod` operand types — PostgreSQL
/// has no `float8` modulo).
fn require_int_or_numeric(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    let t = crate::eval::infer_type(arg, scope)?;
    if matches!(t, ColumnType::Int4 | ColumnType::Int8) || t.is_numeric() {
        Ok(t)
    } else {
        Err(no_matching_function())
    }
}

/// SP30/SP32: require a numeric argument (int4/int8/float8/numeric); returns that
/// type so the caller (`abs`) can preserve it.
fn require_numeric(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    let t = crate::eval::infer_type(arg, scope)?;
    if matches!(t, ColumnType::Int4 | ColumnType::Int8 | ColumnType::Float8) || t.is_numeric() {
        Ok(t)
    } else {
        Err(no_matching_function())
    }
}

/// Unify every argument's type into one (for `coalesce`/`greatest`/`least`); an
/// all-NULL argument list types as text. Incompatible types are 42804.
fn unify_args(args: &[Expr], scope: &Scope) -> Result<ColumnType, ExecError> {
    let mut acc: Option<ColumnType> = None;
    for a in args {
        acc = crate::eval::unify_branch(acc, a, scope)?;
    }
    Ok(acc.unwrap_or(ColumnType::Text))
}

fn promote(a: ColumnType, b: ColumnType) -> ColumnType {
    if a == ColumnType::Int4 && b == ColumnType::Int4 {
        ColumnType::Int4
    } else {
        ColumnType::Int8
    }
}

fn require_arity(fc: &FuncCall, ok: bool) -> Result<(), ExecError> {
    if ok {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

/// A text argument at runtime. A non-text Datum here means the function was used
/// in a non-projected position (so `scalar_result_type` never type-checked it);
/// PostgreSQL rejects it at plan time (42883), we surface it at runtime (42804).
fn text_arg(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        other => Err(type_error("function", other)),
    }
}

/// An integer argument at runtime, promoted to i64.
fn int_arg(d: &Datum) -> Result<i64, ExecError> {
    match d {
        Datum::Int4(n) => Ok(i64::from(*n)),
        Datum::Int8(n) => Ok(*n),
        other => Err(type_error("function", other)),
    }
}

fn bool_arg(d: &Datum) -> Result<bool, ExecError> {
    match d {
        Datum::Bool(value) => Ok(*value),
        other => Err(type_error("function", other)),
    }
}

fn type_error(what: &str, got: &Datum) -> ExecError {
    ExecError::TypeMismatch(format!(
        "{what} does not accept an argument of type {}",
        got.column_type().map(|t| t.name()).unwrap_or("unknown")
    ))
}

/// The canonical text rendering of a non-NULL Datum (the wire text encoding), so
/// `concat` agrees with the DataRow output and with the `||` operator.
fn text_render(d: &Datum, tz: &jiff::tz::TimeZone) -> String {
    String::from_utf8(crabka_pgtypes::encoding::encode_text(d, tz))
        .expect("a Datum's text encoding is always valid UTF-8")
}

// ---- rounding helpers (SP33) ----

/// Rounding-family value transform. `scale` is `Some` only for the two-arg
/// `round`/`trunc` form, which always yields numeric (an int first arg is
/// promoted to numeric). The one-arg form preserves the input numeric type.
fn round_family(f: ScalarFunc, v: &Datum, scale: Option<i64>) -> Result<Datum, ExecError> {
    use crabka_pgtypes::numeric as num;
    if let Some(n) = scale {
        let bd = match v {
            Datum::Int4(i) => num::from_i64(i64::from(*i)),
            Datum::Int8(i) => num::from_i64(*i),
            Datum::Numeric(d) => d.clone(),
            other => return Err(type_error("function", other)),
        };
        return Ok(Datum::Numeric(match f {
            ScalarFunc::Round => num::round(&bd, n),
            ScalarFunc::Trunc => num::trunc(&bd, n),
            _ => unreachable!("scale is only set for round/trunc"),
        }));
    }
    match v {
        Datum::Int4(_) | Datum::Int8(_) => match f {
            ScalarFunc::Sign => sign_int(v),
            _ => Ok(v.clone()), // floor/ceil/round/trunc of an integer is itself
        },
        Datum::Float8(x) => Ok(Datum::Float8(match f {
            ScalarFunc::Floor => x.floor(),
            ScalarFunc::Ceil => x.ceil(),
            ScalarFunc::Round => x.round_ties_even(), // PG float8 round = half-to-even
            ScalarFunc::Trunc => x.trunc(),
            ScalarFunc::Sign => float_sign(*x),
            _ => unreachable!(),
        })),
        Datum::Numeric(d) => Ok(Datum::Numeric(match f {
            ScalarFunc::Floor => num::floor(d),
            ScalarFunc::Ceil => num::ceil(d),
            ScalarFunc::Round => num::round(d, 0),
            ScalarFunc::Trunc => num::trunc(d, 0),
            ScalarFunc::Sign => num::sign(d),
            _ => unreachable!(),
        })),
        other => Err(type_error("function", other)),
    }
}

/// `sign` of an integer, preserving its width.
fn sign_int(v: &Datum) -> Result<Datum, ExecError> {
    Ok(match v {
        Datum::Int4(n) => Datum::Int4(n.signum()),
        Datum::Int8(n) => Datum::Int8(n.signum()),
        other => return Err(type_error("sign", other)),
    })
}

// ---- transcendental helpers (SP33) ----

/// Coerce any numeric Datum (int4/int8/float8/numeric) to f64 for the
/// transcendental functions, which always compute in float8.
fn as_f64(d: &Datum) -> Result<f64, ExecError> {
    Ok(match d {
        Datum::Int4(n) => f64::from(*n),
        Datum::Int8(n) => *n as f64,
        Datum::Float8(x) => *x,
        Datum::Numeric(d) => crabka_pgtypes::numeric::to_f64(d),
        other => return Err(type_error("function", other)),
    })
}

/// Build a domain error carrying its PostgreSQL SQLSTATE.
fn domain(sqlstate: &'static str, message: &'static str) -> ExecError {
    ExecError::Type(crabka_pgtypes::TypeError::Domain { sqlstate, message })
}

/// Wrap an f64 result, mapping an overflow-to-infinity to 22003 (matching the
/// engine's float8 arithmetic, which treats a finite→∞ overflow as out-of-range).
fn finite_or_overflow(x: f64) -> Result<Datum, ExecError> {
    if x.is_infinite() {
        Err(ExecError::Type(crabka_pgtypes::TypeError::Overflow))
    } else {
        Ok(Datum::Float8(x))
    }
}

/// PostgreSQL power result type: float8 if any operand is float8; else numeric if
/// any operand is numeric; else float8 (all-int, PG's preferred type).
fn power_result_type(a: ColumnType, b: ColumnType) -> ColumnType {
    if a == ColumnType::Float8 || b == ColumnType::Float8 {
        ColumnType::Float8
    } else if a.is_numeric() || b.is_numeric() {
        ColumnType::Numeric(None)
    } else {
        ColumnType::Float8
    }
}

/// Promote an int4/int8/numeric Datum to a numeric BigDecimal (for the numeric
/// power path, where one operand may be an integer).
fn to_numeric(d: &Datum) -> Result<bigdecimal::BigDecimal, ExecError> {
    match d {
        Datum::Int4(n) => Ok(crabka_pgtypes::numeric::from_i64(i64::from(*n))),
        Datum::Int8(n) => Ok(crabka_pgtypes::numeric::from_i64(*n)),
        Datum::Numeric(d) => Ok(d.clone()),
        other => Err(type_error("power", other)),
    }
}

/// `power(base, exp)` with PostgreSQL's domain checks (2201F).
fn power(base: f64, exp: f64) -> Result<Datum, ExecError> {
    if base == 0.0 && exp < 0.0 {
        return Err(domain(
            "2201F",
            "zero raised to a negative power is undefined",
        ));
    }
    if base < 0.0 && exp.fract() != 0.0 {
        return Err(domain(
            "2201F",
            "a negative number raised to a non-integer power yields a complex result",
        ));
    }
    finite_or_overflow(base.powf(exp))
}

/// `sign` of a float8: −1 / 0 / 1, with `NaN` → `NaN` (PostgreSQL `dsign`).
fn float_sign(x: f64) -> f64 {
    if x.is_nan() {
        f64::NAN
    } else if x > 0.0 {
        1.0
    } else if x < 0.0 {
        -1.0
    } else {
        0.0
    }
}

// ---- string helpers ----

fn trim_ws(f: ScalarFunc, s: &str) -> String {
    match f {
        ScalarFunc::Ltrim => s.trim_start().to_string(),
        ScalarFunc::Rtrim => s.trim_end().to_string(),
        _ => s.trim().to_string(), // btrim
    }
}

fn trim_set(f: ScalarFunc, s: &str, set: &[char]) -> String {
    let in_set = |c: char| set.contains(&c);
    match f {
        ScalarFunc::Ltrim => s.trim_start_matches(in_set).to_string(),
        ScalarFunc::Rtrim => s.trim_end_matches(in_set).to_string(),
        _ => s.trim_matches(in_set).to_string(), // btrim
    }
}

/// PostgreSQL `substr(string, start [, count])`: 1-based `start`; characters
/// before position 1 count against `count`; a negative `count` is an error
/// (22011); a NULL argument already short-circuited to NULL in `eval_eager`.
fn substr(s: &str, start: i64, count: Option<i64>) -> Result<Datum, ExecError> {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    // The window is [start, end) in 1-based positions, clamped to [1, len+1).
    let end = match count {
        None => len + 1,
        Some(c) => {
            if c < 0 {
                return Err(ExecError::Type(crabka_pgtypes::TypeError::TypeMismatch {
                    message: "negative substring length not allowed".into(),
                }));
            }
            start.saturating_add(c)
        }
    };
    let lo = start.max(1);
    let hi = end.min(len + 1);
    if lo >= hi {
        return Ok(Datum::Text(String::new()));
    }
    let out: String = chars[(lo - 1) as usize..(hi - 1) as usize].iter().collect();
    Ok(Datum::Text(out))
}

// ---- string-family helpers (SP33) ----

/// PostgreSQL's ~1 GB field-size limit — guards `repeat`/`lpad`/`rpad` against
/// minting an adversarially huge string (raised as 54000, "requested length too
/// large", rather than aborting the process on an out-of-memory allocation).
const MAX_FIELD_SIZE: usize = 1 << 30;

/// 54000 (`program_limit_exceeded`) — a string function was asked to produce a
/// field larger than the engine permits.
fn length_too_large() -> ExecError {
    domain("54000", "requested length too large")
}

/// `lpad`/`rpad`: pad `s` to `width` chars with `fill`; when `s` is longer than
/// `width`, truncate to its first `width` chars (both forms). A `width <= 0`
/// yields the empty string; an empty `fill` that cannot pad leaves `s` unchanged.
/// A `width` beyond [`MAX_FIELD_SIZE`] is 54000 (rather than an OOM allocation).
fn pad(f: ScalarFunc, s: &str, width: i64, fill: &str) -> Result<String, ExecError> {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    if width <= 0 {
        return Ok(String::new());
    }
    if width as usize > MAX_FIELD_SIZE {
        return Err(length_too_large());
    }
    if len >= width {
        return Ok(chars[..width as usize].iter().collect());
    }
    let fill_chars: Vec<char> = fill.chars().collect();
    if fill_chars.is_empty() {
        return Ok(s.to_string());
    }
    let pad_len = (width - len) as usize;
    let padding: String = fill_chars.iter().cycle().take(pad_len).collect();
    Ok(match f {
        ScalarFunc::Lpad => format!("{padding}{s}"),
        _ => format!("{s}{padding}"), // Rpad
    })
}

/// `left`/`right`: first/last `n` chars; a negative `n` drops `|n|` chars from the
/// far end (PostgreSQL semantics).
fn left_right(f: ScalarFunc, s: &str, n: i64) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    let take = if n < 0 { (len + n).max(0) } else { n.min(len) };
    match f {
        ScalarFunc::Left => chars[..take as usize].iter().collect(),
        _ => chars[(len - take) as usize..].iter().collect(), // Right
    }
}

/// `repeat(s, n)`: `n <= 0` → empty; guarded against exceeding the field-size limit.
fn repeat_str(s: &str, n: i64) -> Result<Datum, ExecError> {
    if n <= 0 {
        return Ok(Datum::Text(String::new()));
    }
    let n = n as usize;
    match s.len().checked_mul(n) {
        Some(total) if total <= MAX_FIELD_SIZE => Ok(Datum::Text(s.repeat(n))),
        _ => Err(length_too_large()),
    }
}

/// `initcap(s)`: uppercase the first alphanumeric of each word, lowercase the rest.
fn initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_alnum = false;
    for c in s.chars() {
        if c.is_alphanumeric() {
            if prev_alnum {
                out.extend(c.to_lowercase());
            } else {
                out.extend(c.to_uppercase());
            }
            prev_alnum = true;
        } else {
            out.push(c);
            prev_alnum = false;
        }
    }
    out
}

/// `strpos(s, sub)`: 1-based char index of the first `sub` in `s`; `0` if absent;
/// an empty `sub` matches at position `1` (PostgreSQL).
fn strpos(s: &str, sub: &str) -> i32 {
    if sub.is_empty() {
        return 1;
    }
    match s.find(sub) {
        None => 0,
        Some(byte_idx) => (s[..byte_idx].chars().count() + 1) as i32,
    }
}

/// `chr(n)`: the one-character string for Unicode code point `n`. `0` or an
/// out-of-range / surrogate code point is 54000.
fn chr(n: i64) -> Result<Datum, ExecError> {
    if n == 0 {
        return Err(domain("54000", "null character not permitted"));
    }
    match u32::try_from(n).ok().and_then(char::from_u32) {
        Some(c) => Ok(Datum::Text(c.to_string())),
        None => Err(domain(
            "54000",
            "requested character too large for encoding",
        )),
    }
}

#[cfg(test)]
mod tests {
    use crabka_pgcatalog::{Column, Table};
    use crabka_pgparser::parser::parse_expr_for_test as pexpr;

    use super::*;

    fn table() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![
                Column::new("s", ColumnType::Text),
                Column::new("n", ColumnType::Int4),
            ],
            sharded: false,
            sharding: None,
            foreign: None,
        }
    }

    fn table_n() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![Column::new("qn", ColumnType::Numeric(None))],
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

    /// Evaluate a scalar-function expression with no row context.
    fn ev(sql: &str) -> Datum {
        let ctx = crate::clock::EvalCtx::test_default();
        crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx).expect("eval")
    }

    /// SQLSTATE of a runtime eval error (no row context).
    fn ec_eval(sql: &str) -> String {
        let ctx = crate::clock::EvalCtx::test_default();
        crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx)
            .expect_err("expected error")
            .into_pg()
            .code
    }

    fn err_code(sql: &str, t: Option<&Table>) -> String {
        // Drive both the static (projection) and runtime path: infer first (this
        // is what a projected expression hits), falling back to eval.
        let ctx = crate::clock::EvalCtx::test_default();
        let e = pexpr(sql).expect("parse");
        let scope = scope_of(t);
        crate::eval::infer_type(&e, &scope)
            .err()
            .or_else(|| crate::eval::eval(&e, &scope, &[Datum::Null, Datum::Null], &ctx).err())
            .expect("expected error")
            .into_pg()
            .code
    }

    #[test]
    fn string_length_upper_lower() {
        assert_eq!(ev("length('hello')"), Datum::Int4(5));
        assert_eq!(ev("char_length('abc')"), Datum::Int4(3));
        assert_eq!(ev("character_length('')"), Datum::Int4(0));
        assert_eq!(ev("upper('aBc')"), Datum::Text("ABC".into()));
        assert_eq!(ev("lower('aBc')"), Datum::Text("abc".into()));
        // strict: NULL argument → NULL.
        assert_eq!(ev("length(null)"), Datum::Null);
        assert_eq!(ev("upper(null)"), Datum::Null);
    }

    #[test]
    fn trims_default_and_with_set() {
        assert_eq!(ev("btrim('  hi  ')"), Datum::Text("hi".into()));
        assert_eq!(ev("ltrim('  hi  ')"), Datum::Text("hi  ".into()));
        assert_eq!(ev("rtrim('  hi  ')"), Datum::Text("  hi".into()));
        assert_eq!(ev("btrim('xxhixx', 'x')"), Datum::Text("hi".into()));
        assert_eq!(ev("ltrim('xyhi', 'xy')"), Datum::Text("hi".into()));
        // a NULL character-set argument → NULL (strict).
        assert_eq!(ev("btrim('hi', null)"), Datum::Null);
    }

    #[test]
    fn substr_and_replace() {
        assert_eq!(ev("substr('abcdef', 2, 3)"), Datum::Text("bcd".into()));
        assert_eq!(ev("substring('abcdef', 4)"), Datum::Text("def".into()));
        // start before position 1: the count is consumed from position 1.
        assert_eq!(ev("substr('abcdef', 0, 2)"), Datum::Text("a".into()));
        assert_eq!(ev("substr('abc', 5)"), Datum::Text("".into()));
        assert_eq!(
            ev("replace('a.b.c', '.', '-')"),
            Datum::Text("a-b-c".into())
        );
        // negative substring length is 22011-class (mapped to 42804 here).
        let ctx = crate::clock::EvalCtx::test_default();
        let err = crate::eval::eval(
            &pexpr("substr('abc', 1, -1)").expect("p"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect_err("neg len");
        assert_eq!(err.into_pg().code, "42804");
    }

    #[test]
    fn concat_skips_nulls_and_renders_each() {
        assert_eq!(ev("concat('a', 'b', 'c')"), Datum::Text("abc".into()));
        assert_eq!(ev("concat('x', null, 'y')"), Datum::Text("xy".into()));
        assert_eq!(ev("concat(1, '+', 2)"), Datum::Text("1+2".into()));
        // all-NULL (and zero-arg) concat is the empty string, never NULL.
        assert_eq!(ev("concat(null, null)"), Datum::Text("".into()));
        assert_eq!(ev("concat()"), Datum::Text("".into()));
    }

    #[test]
    fn abs_and_mod() {
        let ctx = crate::clock::EvalCtx::test_default();
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        assert_eq!(ev("abs(-5)"), Datum::Int4(5));
        assert_eq!(ev("abs(7)"), Datum::Int4(7));
        // SP32: a bare decimal is numeric, so abs over it is numeric (float8 abs
        // is reached via an explicit cast).
        assert_eq!(ev("abs(-2.5)"), num("2.5"));
        assert_eq!(ev("abs(2.5)"), num("2.5"));
        assert_eq!(ev("abs(-2.5::float8)"), Datum::Float8(2.5));
        assert_eq!(ev("mod(11, 3)"), Datum::Int4(2));
        assert_eq!(ev("mod(-11, 3)"), Datum::Int4(-2));
        // SP32: numeric mod (the remainder takes the dividend's sign).
        assert_eq!(ev("mod(7.5, 2)"), num("1.5"));
        assert_eq!(ev("abs(null)"), Datum::Null);
        // abs overflow at i32::MIN is 22003. A bare `-2147483648` literal is a
        // negated int8, so the overflow only arises on an actual int4 column
        // value; evaluate `abs(n)` against a row holding Int4(i32::MIN).
        let t = table();
        let err = crate::eval::eval(
            &pexpr("abs(n)").expect("p"),
            &scope_of(Some(&t)),
            &[Datum::Null, Datum::Int4(i32::MIN)],
            &ctx,
        )
        .expect_err("overflow");
        assert_eq!(err.into_pg().code, "22003");
        // mod by zero is 22012.
        let err = crate::eval::eval(&pexpr("mod(1, 0)").expect("p"), &Scope::empty(), &[], &ctx)
            .expect_err("div0");
        assert_eq!(err.into_pg().code, "22012");
    }

    #[test]
    fn coalesce_short_circuits_and_nullif() {
        assert_eq!(ev("coalesce(null, null, 3)"), Datum::Int4(3));
        assert_eq!(ev("coalesce(null, null)"), Datum::Null);
        // short-circuit: the un-taken `1/0` branch is never evaluated.
        assert_eq!(ev("coalesce(7, 1/0)"), Datum::Int4(7));
        assert_eq!(ev("nullif(5, 5)"), Datum::Null);
        assert_eq!(ev("nullif(5, 6)"), Datum::Int4(5));
        assert_eq!(ev("nullif(null, 1)"), Datum::Null);
    }

    #[test]
    fn greatest_least_ignore_nulls() {
        assert_eq!(ev("greatest(3, 7, 2)"), Datum::Int4(7));
        assert_eq!(ev("least(3, 7, 2)"), Datum::Int4(2));
        assert_eq!(ev("greatest(null, 4, null)"), Datum::Int4(4));
        assert_eq!(ev("least('b', 'a', 'c')"), Datum::Text("a".into()));
        assert_eq!(ev("greatest(null, null)"), Datum::Null);
    }

    #[test]
    fn result_types_for_row_description() {
        let t = table();
        let scope = scope_of(Some(&t));
        let ty = |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("p"), &scope).expect("ty");
        assert_eq!(ty("length(s)"), ColumnType::Int4);
        assert_eq!(ty("upper(s)"), ColumnType::Text);
        assert_eq!(ty("substr(s, 1, 2)"), ColumnType::Text);
        assert_eq!(ty("concat(s, n)"), ColumnType::Text);
        assert_eq!(ty("abs(n)"), ColumnType::Int4);
        assert_eq!(ty("mod(n, 2)"), ColumnType::Int4);
        // coalesce(int4, int8) unifies to int8.
        assert_eq!(ty("coalesce(n, 3000000000)"), ColumnType::Int8);
        assert_eq!(ty("nullif(s, 'x')"), ColumnType::Text);
        // `||` is text; one operand text is enough.
        assert_eq!(ty("'id=' || n"), ColumnType::Text);
    }

    #[test]
    fn error_surface() {
        let t = table();
        // unknown function → 42883.
        assert_eq!(err_code("frobnicate(s)", Some(&t)), "42883");
        // wrong arity → 42883.
        assert_eq!(err_code("length(s, s)", Some(&t)), "42883");
        // bad argument type in a projected position → 42883.
        assert_eq!(err_code("upper(n)", Some(&t)), "42883");
        assert_eq!(err_code("abs(s)", Some(&t)), "42883");
        // `int || int` (neither operand text) → 42883.
        assert_eq!(err_code("n || n", Some(&t)), "42883");
        // incompatible coalesce/greatest types → 42804.
        assert_eq!(err_code("coalesce(n, s)", Some(&t)), "42804");
        // DISTINCT on a scalar function → 42809.
        assert_eq!(err_code("upper(distinct s)", Some(&t)), "42809");
    }

    #[test]
    fn concat_operator_evaluates_and_propagates_null() {
        assert_eq!(ev("'a' || 'b' || 'c'"), Datum::Text("abc".into()));
        assert_eq!(ev("'id=' || 5"), Datum::Text("id=5".into()));
        assert_eq!(ev("'x' || null"), Datum::Null);
    }

    #[test]
    fn rounding_family_preserves_type() {
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        // int in → int out (unchanged)
        assert_eq!(ev("floor(5)"), Datum::Int4(5));
        assert_eq!(ev("ceil(5)"), Datum::Int4(5));
        assert_eq!(ev("trunc(5)"), Datum::Int4(5));
        assert_eq!(ev("round(5)"), Datum::Int4(5));
        assert_eq!(ev("sign(-7)"), Datum::Int4(-1));
        // numeric in → numeric out
        assert_eq!(ev("floor(2.9)"), num("2"));
        assert_eq!(ev("ceiling(2.1)"), num("3"));
        assert_eq!(ev("round(2.5)"), num("3"));
        assert_eq!(ev("round(2.567, 2)"), num("2.57"));
        assert_eq!(ev("trunc(2.99)"), num("2"));
        assert_eq!(ev("trunc(2.567, 1)"), num("2.5"));
        assert_eq!(ev("sign(-0.3)"), num("-1"));
        // float8 in → float8 out (round half-to-even)
        assert_eq!(ev("floor(2.9::float8)"), Datum::Float8(2.0));
        assert_eq!(ev("round(2.5::float8)"), Datum::Float8(2.0)); // half-to-even
        assert_eq!(ev("round(3.5::float8)"), Datum::Float8(4.0));
        assert_eq!(ev("sign(-3.0::float8)"), Datum::Float8(-1.0));
        // two-arg round/trunc on an int → numeric
        assert_eq!(ev("round(1234, -2)"), num("1200"));
        // strict NULL
        assert_eq!(ev("floor(null)"), Datum::Null);
    }

    #[test]
    fn rounding_family_types_and_errors() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("floor(n)"), ColumnType::Int4);
        assert_eq!(ty("round(2.5)"), ColumnType::Numeric(None));
        assert_eq!(ty("floor(2.5::float8)"), ColumnType::Float8);
        assert_eq!(ty("round(2.5, 1)"), ColumnType::Numeric(None));
        // two-arg round on a float8 first arg → 42883 (PG has no round(float8,int)).
        assert_eq!(err_code("round(2.5::float8, 1)", Some(&t)), "42883");
        // non-numeric arg → 42883.
        assert_eq!(err_code("floor(s)", Some(&t)), "42883");
    }

    #[test]
    fn transcendental_family_returns_float8() {
        assert_eq!(ev("sqrt(4)"), Datum::Float8(2.0));
        assert_eq!(ev("sqrt(2.25::float8)"), Datum::Float8(1.5));
        assert_eq!(ev("power(2, 10)"), Datum::Float8(1024.0));
        assert_eq!(ev("pow(2, 0.5::float8)"), Datum::Float8(2.0_f64.sqrt()));
        assert_eq!(ev("exp(0)"), Datum::Float8(1.0));
        assert_eq!(ev("ln(1)"), Datum::Float8(0.0));
        assert_eq!(ev("log(1000)"), Datum::Float8(3.0));
        assert_eq!(ev("pi()"), Datum::Float8(std::f64::consts::PI));
        // strict NULL
        assert_eq!(ev("sqrt(null)"), Datum::Null);
    }

    #[test]
    fn string_family_values() {
        assert_eq!(ev("lpad('hi', 5)"), Datum::Text("   hi".into()));
        assert_eq!(ev("lpad('hi', 5, '*')"), Datum::Text("***hi".into()));
        assert_eq!(ev("lpad('hello', 3)"), Datum::Text("hel".into()));
        assert_eq!(ev("rpad('hi', 5, 'ab')"), Datum::Text("hiaba".into()));
        assert_eq!(ev("rpad('hello', 3)"), Datum::Text("hel".into()));
        assert_eq!(ev("left('abcdef', 2)"), Datum::Text("ab".into()));
        assert_eq!(ev("left('abcdef', -2)"), Datum::Text("abcd".into()));
        assert_eq!(ev("right('abcdef', 2)"), Datum::Text("ef".into()));
        assert_eq!(ev("right('abcdef', -2)"), Datum::Text("cdef".into()));
        assert_eq!(ev("repeat('ab', 3)"), Datum::Text("ababab".into()));
        assert_eq!(ev("repeat('ab', 0)"), Datum::Text("".into()));
        assert_eq!(ev("reverse('abc')"), Datum::Text("cba".into()));
        assert_eq!(
            ev("initcap('hello WORLD')"),
            Datum::Text("Hello World".into())
        );
        assert_eq!(ev("strpos('abcde', 'cd')"), Datum::Int4(3));
        assert_eq!(ev("strpos('abcde', 'xy')"), Datum::Int4(0));
        assert_eq!(ev("strpos('abc', '')"), Datum::Int4(1));
        assert_eq!(ev("ascii('A')"), Datum::Int4(65));
        assert_eq!(ev("ascii('')"), Datum::Int4(0));
        assert_eq!(ev("chr(65)"), Datum::Text("A".into()));
        // strict NULL
        assert_eq!(ev("lpad(null, 5)"), Datum::Null);
        assert_eq!(ev("reverse(null)"), Datum::Null);
    }

    #[test]
    fn string_family_types_and_errors() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("lpad(s, 5)"), ColumnType::Text);
        assert_eq!(ty("strpos(s, 'x')"), ColumnType::Int4);
        assert_eq!(ty("ascii(s)"), ColumnType::Int4);
        assert_eq!(ty("chr(n)"), ColumnType::Text);
        // chr(0) and an out-of-range code point → 54000.
        assert_eq!(ec_eval("chr(0)"), "54000");
        assert_eq!(ec_eval("chr(99999999999)"), "54000");
        // wrong arg type → 42883.
        assert_eq!(err_code("left(n, 2)", Some(&t)), "42883");
        assert_eq!(err_code("ascii(n)", Some(&t)), "42883");
        // an adversarially huge lpad/rpad width or repeat count is 54000
        // ("requested length too large"), guarded against OOM — not a process abort.
        assert_eq!(ec_eval("lpad('x', 9999999999)"), "54000");
        assert_eq!(ec_eval("rpad('x', 9999999999)"), "54000");
        assert_eq!(ec_eval("repeat('x', 9999999999)"), "54000");
    }

    #[test]
    fn transcendental_domain_errors() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("sqrt(n)"), ColumnType::Float8);
        assert_eq!(ty("pi()"), ColumnType::Float8);
        // sqrt(negative) → 2201F
        assert_eq!(ec_eval("sqrt(-1)"), "2201F");
        // ln/log of a non-positive number → 2201E
        assert_eq!(ec_eval("ln(0)"), "2201E");
        assert_eq!(ec_eval("ln(-1)"), "2201E");
        assert_eq!(ec_eval("log(0)"), "2201E");
        // zero to a negative power → 2201F
        assert_eq!(ec_eval("power(0, -1)"), "2201F");
        // wrong arity → 42883
        assert_eq!(err_code("pi(1)", Some(&t)), "42883");
        assert_eq!(err_code("power(2)", Some(&t)), "42883");
    }

    #[test]
    fn transcendentals_are_numeric_for_numeric_input() {
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        // numeric in -> numeric out (oracle-validated exact values from pgtypes unit tests)
        assert_eq!(ev("sqrt(2.0)"), num("1.414213562373095"));
        assert_eq!(ev("exp(1.0)"), num("2.7182818284590452"));
        assert_eq!(ev("ln(2.0)"), num("0.6931471805599453"));
        assert_eq!(ev("power(2.0, 3.0)"), num("8.0000000000000000"));
        // int in -> float8 out (unchanged)
        assert_eq!(ev("sqrt(4)"), Datum::Float8(2.0));
        assert_eq!(ev("exp(0)"), Datum::Float8(1.0));
        // float8 in -> float8 out (unchanged)
        assert_eq!(ev("sqrt(4.0::float8)"), Datum::Float8(2.0));
        // strict NULL
        assert_eq!(ev("sqrt(null)"), Datum::Null);
        // numeric-path domain errors (2201E/2201F) and overflow (22003) surface
        // end-to-end from SQL — never a panic, hang, or silently-wrong value.
        assert_eq!(ec_eval("sqrt(-1::numeric)"), "2201F");
        assert_eq!(ec_eval("ln(0::numeric)"), "2201E");
        assert_eq!(ec_eval("power(0::numeric, -1::numeric)"), "2201F");
        assert_eq!(ec_eval("exp(6000::numeric)"), "22003");
        assert_eq!(ec_eval("power(10::numeric, 200000::numeric)"), "22003");
    }

    #[test]
    fn transcendental_result_types() {
        let t = table_n();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("sqrt(qn)"), ColumnType::Numeric(None)); // qn is numeric
        assert_eq!(ty("ln(qn)"), ColumnType::Numeric(None));
        assert_eq!(ty("sqrt(4)"), ColumnType::Float8); // int literal
        assert_eq!(ty("sqrt(4.0::float8)"), ColumnType::Float8);
        assert_eq!(ty("power(qn, 2)"), ColumnType::Numeric(None)); // numeric base
        assert_eq!(ty("power(2, 3)"), ColumnType::Float8); // all-int
    }
}
