//! The remaining PostgreSQL mathematical functions: integer/numeric number
//! theory (`gcd`, `lcm`, `factorial`, `div`), the `numeric` scale introspection
//! trio (`scale`, `min_scale`, `trim_scale`), `width_bucket`, the pseudo-random
//! pair (`random`, `setseed`), and the full trigonometric / hyperbolic /
//! logarithmic family.
//!
//! Like every other function family in this crate (`func`, `datetime_fn`,
//! `format_fn`, `json_fn`, `array_fn`), each entry is a pure, deterministic
//! transform over a single row's already-evaluated Datums — `random`/`setseed`
//! being the sole exception, and they touch only the session PRNG described at
//! [`Prng`]. `func::is_scalar` routes these names here, so the module needs
//! no separate dispatch point in `eval`.
//!
//! The degree-argument trigonometric functions reproduce PostgreSQL's
//! `sind_q1`/`cosd_q1`/`asind_q1` stitching, so that the exact-answer angles
//! come out exactly right rather than one ULP off. Those angles are 0, 30, 45,
//! 60 and 90, and their reflections. `cbrt` reproduces the C library's routine
//! rather than Rust's more accurate one, because PostgreSQL's answer is the C
//! library's.

use std::{cell::RefCell, cmp::Ordering};

use bigdecimal::{BigDecimal, num_bigint::Sign};
use crabka_pgparser::ast::{Expr, FuncCall};
use crabka_pgtypes::{ColumnType, Datum, numeric, numeric::NumericValue};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    func::{
        ambiguous_function, checked_args, domain, int_arg, is_unknown_arg, no_matching_function,
        require_arity, to_numeric, type_error, undefined_function,
    },
    scope::Scope,
};

/// The mathematical functions this module owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MathFunc {
    Gcd,
    Lcm,
    Factorial,
    Div,
    Scale,
    MinScale,
    TrimScale,
    WidthBucket,
    Random,
    SetSeed,
    // Radian trigonometry.
    Sin,
    Cos,
    Tan,
    Cot,
    Asin,
    Acos,
    Atan,
    Atan2,
    // Degree trigonometry.
    Sind,
    Cosd,
    Tand,
    Cotd,
    Asind,
    Acosd,
    Atand,
    Atan2d,
    // Hyperbolic.
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    // Angle conversion and the remaining roots/logarithms.
    Degrees,
    Radians,
    Log10,
    Cbrt,
}

/// Classify a lowercased function name. The lexer lowercases unquoted idents.
fn math_func(name: &str) -> Option<MathFunc> {
    Some(match name {
        "gcd" => MathFunc::Gcd,
        "lcm" => MathFunc::Lcm,
        "factorial" => MathFunc::Factorial,
        "div" => MathFunc::Div,
        "scale" => MathFunc::Scale,
        "min_scale" => MathFunc::MinScale,
        "trim_scale" => MathFunc::TrimScale,
        "width_bucket" => MathFunc::WidthBucket,
        "random" => MathFunc::Random,
        "setseed" => MathFunc::SetSeed,
        "sin" => MathFunc::Sin,
        "cos" => MathFunc::Cos,
        "tan" => MathFunc::Tan,
        "cot" => MathFunc::Cot,
        "asin" => MathFunc::Asin,
        "acos" => MathFunc::Acos,
        "atan" => MathFunc::Atan,
        "atan2" => MathFunc::Atan2,
        "sind" => MathFunc::Sind,
        "cosd" => MathFunc::Cosd,
        "tand" => MathFunc::Tand,
        "cotd" => MathFunc::Cotd,
        "asind" => MathFunc::Asind,
        "acosd" => MathFunc::Acosd,
        "atand" => MathFunc::Atand,
        "atan2d" => MathFunc::Atan2d,
        "sinh" => MathFunc::Sinh,
        "cosh" => MathFunc::Cosh,
        "tanh" => MathFunc::Tanh,
        "asinh" => MathFunc::Asinh,
        "acosh" => MathFunc::Acosh,
        "atanh" => MathFunc::Atanh,
        "degrees" => MathFunc::Degrees,
        "radians" => MathFunc::Radians,
        "log10" => MathFunc::Log10,
        "cbrt" => MathFunc::Cbrt,
        _ => return None,
    })
}

/// Is `name` one of this module's functions? `func::is_scalar` folds this in.
pub(crate) fn is_math_func(name: &str) -> bool {
    math_func(name).is_some()
}

/// The numeric family an argument resolves into. PostgreSQL overloads the
/// functions here on `int4`/`int8`/`numeric`/`float8`, and the *widest* argument
/// picks the overload. So this is the only argument classification the module
/// needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum NumKind {
    Int4,
    Int8,
    Numeric,
    Float8,
}

impl NumKind {
    fn column_type(self) -> ColumnType {
        match self {
            NumKind::Int4 => ColumnType::Int4,
            NumKind::Int8 => ColumnType::Int8,
            NumKind::Numeric => ColumnType::Numeric(None),
            NumKind::Float8 => ColumnType::Float8,
        }
    }
}

fn num_kind(t: ColumnType) -> Option<NumKind> {
    match t {
        // No function here has an `int2` or `float4` overload, so PostgreSQL
        // resolves those arguments through the implicit widening cast — `int2`
        // onto the `int4` overload, `float4` onto the `float8` one.
        ColumnType::Int2 | ColumnType::Int4 => Some(NumKind::Int4),
        ColumnType::Int8 => Some(NumKind::Int8),
        ColumnType::Float4 | ColumnType::Float8 => Some(NumKind::Float8),
        _ if t.is_numeric() => Some(NumKind::Numeric),
        _ => None,
    }
}

/// The numeric family of one argument expression, or `None` for an argument
/// PostgreSQL still calls `unknown`, that is a bare `NULL` or an unadorned
/// string literal. An `unknown` argument constrains nothing. The *other*
/// arguments pick the overload, and the literal is then coerced into it.
fn arg_kind(arg: &Expr, scope: &Scope) -> Result<Option<NumKind>, ExecError> {
    if is_unknown_arg(arg) {
        return Ok(None);
    }
    num_kind(crate::eval::infer_type(arg, scope)?)
        .map(Some)
        .ok_or_else(no_matching_function)
}

/// The widest family across `args`, which ignores the `unknown` ones. This is
/// the overload PostgreSQL picks. `default` is what an all-`unknown` list
/// resolves to, that is the candidate set's preferred type. The ambiguous
/// families reject that case through [`require_one_known`] before it reaches
/// here.
fn widest(args: &[Expr], scope: &Scope, default: NumKind) -> Result<NumKind, ExecError> {
    let mut resolved: Option<NumKind> = None;
    for a in args {
        if let Some(kind) = arg_kind(a, scope)? {
            resolved = Some(resolved.map_or(kind, |acc: NumKind| acc.max(kind)));
        }
    }
    Ok(resolved.unwrap_or(default))
}

/// Statically infer a math call's result type, for RowDescription. This function
/// validates the name, the arity and the argument types, and reports 42883 for
/// any mismatch.
pub(crate) fn math_func_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    let f = math_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let n = args.len();
    match f {
        // gcd/lcm keep the integer width, or stay numeric; there is no float8
        // overload, so a bare NULL resolves to numeric.
        MathFunc::Gcd | MathFunc::Lcm => {
            require_arity(fc, n == 2)?;
            require_one_known(fc, args)?;
            match widest(args, scope, NumKind::Numeric)? {
                NumKind::Int4 => Ok(ColumnType::Int4),
                NumKind::Int8 => Ok(ColumnType::Int8),
                _ => Ok(ColumnType::Numeric(None)),
            }
        }
        // factorial(bigint) and div(numeric, numeric) are numeric-only.
        MathFunc::Factorial => {
            require_arity(fc, n == 1)?;
            int_or_null(&args[0], scope)?;
            Ok(ColumnType::Numeric(None))
        }
        MathFunc::Div | MathFunc::TrimScale => {
            require_arity(fc, n == if f == MathFunc::Div { 2 } else { 1 })?;
            for a in args {
                numeric_castable(a, scope)?;
            }
            Ok(ColumnType::Numeric(None))
        }
        MathFunc::Scale | MathFunc::MinScale => {
            require_arity(fc, n == 1)?;
            numeric_castable(&args[0], scope)?;
            Ok(ColumnType::Int4)
        }
        // width_bucket(operand, low, high, count) -> int4, over numeric or float8.
        MathFunc::WidthBucket => {
            require_arity(fc, n == 4)?;
            widest(&args[..3], scope, NumKind::Numeric)?;
            arg_kind(&args[3], scope)?;
            Ok(ColumnType::Int4)
        }
        // random() -> float8; random(lo, hi) keeps the argument family.
        MathFunc::Random => {
            require_arity(fc, n == 0 || n == 2)?;
            if n == 0 {
                return Ok(ColumnType::Float8);
            }
            require_one_known(fc, args)?;
            match widest(args, scope, NumKind::Numeric)? {
                NumKind::Int4 => Ok(ColumnType::Int4),
                NumKind::Int8 => Ok(ColumnType::Int8),
                // There is no `random(float8, float8)`, so a float8 argument
                // (and an all-unknown pair) lands on the numeric overload.
                NumKind::Numeric | NumKind::Float8 => Ok(ColumnType::Numeric(None)),
            }
        }
        // setseed returns `void`; crabka has no void column type, so it reports
        // text and evaluates to the empty string — the same documented shape
        // `pg_notify` uses, and byte-identical on the wire.
        MathFunc::SetSeed => {
            require_arity(fc, n == 1)?;
            arg_kind(&args[0], scope)?;
            Ok(ColumnType::Text)
        }
        MathFunc::Atan2 | MathFunc::Atan2d => {
            require_arity(fc, n == 2)?;
            widest(args, scope, NumKind::Float8)?;
            Ok(ColumnType::Float8)
        }
        // log10 is the one member of the family with a numeric overload.
        MathFunc::Log10 => {
            require_arity(fc, n == 1)?;
            Ok(match arg_kind(&args[0], scope)? {
                Some(NumKind::Numeric) => ColumnType::Numeric(None),
                _ => ColumnType::Float8,
            })
        }
        // Everything else is float8-in / float8-out.
        _ => {
            require_arity(fc, n == 1)?;
            arg_kind(&args[0], scope)?;
            Ok(ColumnType::Float8)
        }
    }
}

/// PostgreSQL cannot choose between `gcd`/`lcm`/`random`'s int4, int8 and
/// numeric overloads when EVERY argument is still `unknown`, so it raises 42725
/// rather than choosing one. A single typed argument settles it.
fn require_one_known(fc: &FuncCall, args: &[Expr]) -> Result<(), ExecError> {
    if args.iter().all(is_unknown_arg) {
        return Err(ambiguous_function(&fc.name, args.len()));
    }
    Ok(())
}

/// Require an argument PostgreSQL can pass to a `numeric` parameter: any numeric
/// family member, or a bare `NULL`.
fn numeric_castable(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    arg_kind(arg, scope).map(|_| ())
}

/// Require an integer or `unknown` argument. `factorial`'s only parameter type
/// is `bigint`, which `numeric` does not implicitly reach.
fn int_or_null(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    match arg_kind(arg, scope)? {
        None | Some(NumKind::Int4 | NumKind::Int8 | NumKind::Float8) => Ok(()),
        Some(NumKind::Numeric) => Err(no_matching_function()),
    }
}

/// Evaluate a math call. Every function here is strict except `setseed`, which
/// has no NULL fast path to skip because it is declared strict too. So the
/// shared NULL short-circuit below covers the whole module.
pub(crate) fn eval_math(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = math_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let mut vals = args
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    if vals.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    coerce_unknown_args(f, args, &mut vals, ctx)?;
    eval_strict(f, fc, &vals, ctx)
}

/// Coerce every `unknown` literal argument into the parameter type the call
/// resolved to, so `gcd('8', 12)` and `sind('30')` compute rather than reporting
/// a text argument. PostgreSQL does this coercion at plan time. crabka's scalar
/// evaluator has no scope, so it re-derives the family from the values the
/// *typed* arguments produced.
fn coerce_unknown_args(
    f: MathFunc,
    args: &[Expr],
    vals: &mut [Datum],
    ctx: &EvalCtx,
) -> Result<(), ExecError> {
    if !args.iter().any(is_unknown_arg) {
        return Ok(());
    }
    // `width_bucket`'s bucket count is int4 whatever family the bounds resolve
    // to, so it is coerced separately from the first three arguments.
    let (family_args, count_arg) = if f == MathFunc::WidthBucket && args.len() == 4 {
        (3, true)
    } else {
        (args.len(), false)
    };
    let family = value_family(f, &args[..family_args], &vals[..family_args]);
    for (a, v) in args.iter().zip(vals.iter_mut()).take(family_args) {
        if is_unknown_arg(a) {
            *v = crabka_pgtypes::cast::cast(v, family.column_type(), &ctx.time_zone)?;
        }
    }
    if count_arg && is_unknown_arg(&args[3]) {
        vals[3] = crabka_pgtypes::cast::cast(&vals[3], ColumnType::Int4, &ctx.time_zone)?;
    }
    Ok(())
}

/// The parameter family a call resolved to, read off the already-evaluated typed
/// arguments. `default` is the preferred type each candidate set falls back to
/// when nothing else constrains it.
fn value_family(f: MathFunc, args: &[Expr], vals: &[Datum]) -> NumKind {
    // A function with ONE parameter type coerces every `unknown` argument to
    // it, whatever the typed arguments happen to be.
    match f {
        MathFunc::Factorial => return NumKind::Int8,
        MathFunc::Div | MathFunc::Scale | MathFunc::MinScale | MathFunc::TrimScale => {
            return NumKind::Numeric;
        }
        // The overloaded families fall through to the resolution below.
        MathFunc::Gcd
        | MathFunc::Lcm
        | MathFunc::Random
        | MathFunc::WidthBucket
        | MathFunc::Log10 => {}
        // Everything else — the trigonometric, hyperbolic and root families —
        // takes float8 only.
        _ => return NumKind::Float8,
    }
    let mut widest: Option<NumKind> = None;
    for (a, v) in args.iter().zip(vals) {
        if is_unknown_arg(a) {
            continue;
        }
        if let Some(kind) = v.column_type().and_then(num_kind) {
            widest = Some(widest.map_or(kind, |acc: NumKind| acc.max(kind)));
        }
    }
    // With nothing typed to go on, each candidate set falls back to its own
    // preferred type.
    widest.unwrap_or(match f {
        MathFunc::Log10 => NumKind::Float8,
        _ => NumKind::Numeric,
    })
}

fn eval_strict(
    f: MathFunc,
    fc: &FuncCall,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    match f {
        MathFunc::Gcd | MathFunc::Lcm => {
            require_arity(fc, vals.len() == 2)?;
            gcd_lcm(f, &vals[0], &vals[1])
        }
        MathFunc::Factorial => {
            require_arity(fc, vals.len() == 1)?;
            factorial(int_arg(&vals[0])?)
        }
        MathFunc::Div => {
            require_arity(fc, vals.len() == 2)?;
            let (a, b) = (to_numeric(&vals[0])?, to_numeric(&vals[1])?);
            numeric::div_trunc(&a, &b)
                .map(Datum::Numeric)
                .map_err(ExecError::Type)
        }
        // A special value has no display scale at all: PostgreSQL's `scale` and
        // `min_scale` return NULL for it, while `trim_scale` hands it back.
        MathFunc::Scale => {
            require_arity(fc, vals.len() == 1)?;
            Ok(match to_numeric(&vals[0])?.as_finite() {
                None => Datum::Null,
                Some(d) => Datum::Int4(display_scale(d)),
            })
        }
        MathFunc::MinScale => {
            require_arity(fc, vals.len() == 1)?;
            Ok(match to_numeric(&vals[0])?.as_finite() {
                None => Datum::Null,
                Some(d) => Datum::Int4(display_scale(&trimmed(d))),
            })
        }
        MathFunc::TrimScale => {
            require_arity(fc, vals.len() == 1)?;
            let value = to_numeric(&vals[0])?;
            Ok(Datum::Numeric(match value.as_finite() {
                None => value.clone(),
                Some(d) => NumericValue::from(trimmed(d)),
            }))
        }
        MathFunc::WidthBucket => {
            require_arity(fc, vals.len() == 4)?;
            width_bucket(&vals[0], &vals[1], &vals[2], int_arg(&vals[3])?)
        }
        MathFunc::Random => {
            require_arity(fc, vals.is_empty() || vals.len() == 2)?;
            match vals {
                [] => Ok(Datum::Float8(with_prng(ctx, Prng::next_double))),
                [lo, hi] => random_range(lo, hi, ctx),
                _ => Err(undefined_function(&fc.name)),
            }
        }
        MathFunc::SetSeed => {
            require_arity(fc, vals.len() == 1)?;
            let seed = as_f64(&vals[0])?;
            if !(-1.0..=1.0).contains(&seed) {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: format!("setseed parameter {seed} is out of allowed range [-1,1]"),
                });
            }
            with_prng(ctx, |prng| prng.seed_double(seed));
            Ok(Datum::Text(String::new()))
        }
        // The numeric-overloaded logarithm.
        MathFunc::Log10 => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return numeric::num_log10(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            float_log10(x)
        }
        MathFunc::Atan2 => {
            require_arity(fc, vals.len() == 2)?;
            Ok(Datum::Float8(as_f64(&vals[0])?.atan2(as_f64(&vals[1])?)))
        }
        MathFunc::Atan2d => {
            require_arity(fc, vals.len() == 2)?;
            let (y, x) = (as_f64(&vals[0])?, as_f64(&vals[1])?);
            Ok(Datum::Float8(if y.is_nan() || x.is_nan() {
                f64::NAN
            } else {
                y.atan2(x) / ATAN_1_0 * 45.0
            }))
        }
        _ => {
            require_arity(fc, vals.len() == 1)?;
            unary_float(f, as_f64(&vals[0])?)
        }
    }
}

/// PostgreSQL's `numeric` display scale: the number of digits kept after the
/// decimal point, which `numeric` preserves through parsing and arithmetic.
fn display_scale(d: &BigDecimal) -> i32 {
    i32::try_from(d.fractional_digit_count().max(0)).unwrap_or(i32::MAX)
}

/// The same value with every insignificant trailing zero dropped. This is the
/// smallest display scale that still represents it, which is what `min_scale`
/// reports and `trim_scale` returns.
fn trimmed(d: &BigDecimal) -> BigDecimal {
    numeric::canonical(d.normalized())
}

/// Coerce a numeric Datum to `f64` for the float8-domain functions.
fn as_f64(d: &Datum) -> Result<f64, ExecError> {
    Ok(match d {
        Datum::Int4(n) => f64::from(*n),
        Datum::Int8(n) => *n as f64,
        Datum::Float8(x) => *x,
        Datum::Numeric(d) => numeric::to_f64(d),
        other => return Err(type_error("function", other)),
    })
}

// ---- number theory ----

/// `gcd`/`lcm` over the integer widths and `numeric`, with PostgreSQL's `22003`
/// overflow behavior. The notable case is `gcd(-2147483648, 0)`, whose exact
/// answer `2147483648` does not fit back into `int4`.
fn gcd_lcm(f: MathFunc, a: &Datum, b: &Datum) -> Result<Datum, ExecError> {
    match (a, b) {
        (Datum::Int4(x), Datum::Int4(y)) => {
            let g = gcd_i64(i64::from(*x), i64::from(*y));
            let v = if f == MathFunc::Gcd {
                g
            } else {
                lcm_i64(i64::from(*x), i64::from(*y))?
            };
            i32::try_from(v)
                .map(Datum::Int4)
                .map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))
        }
        _ => {
            if let (Some(x), Some(y)) = (as_i64(a), as_i64(b)) {
                let v = if f == MathFunc::Gcd {
                    gcd_i128(i128::from(x), i128::from(y))
                } else {
                    lcm_i128(i128::from(x), i128::from(y))?
                };
                return i64::try_from(v).map(Datum::Int8).map_err(|_| {
                    ExecError::Type(crabka_pgtypes::TypeError::OutOfRange {
                        message: "bigint out of range".into(),
                    })
                });
            }
            let (x, y) = (to_numeric(a)?, to_numeric(b)?);
            // PostgreSQL's `numeric_gcd`/`numeric_lcm` yield NaN for any special
            // operand, infinities included.
            let (Some(x), Some(y)) = (x.as_finite(), y.as_finite()) else {
                return Ok(Datum::Numeric(NumericValue::NaN));
            };
            Ok(Datum::Numeric(NumericValue::from(if f == MathFunc::Gcd {
                gcd_numeric(x, y)
            } else {
                lcm_numeric(x, y)
            })))
        }
    }
}

fn as_i64(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        _ => None,
    }
}

fn gcd_i64(a: i64, b: i64) -> i64 {
    gcd_i128(i128::from(a), i128::from(b)) as i64
}

fn gcd_i128(mut a: i128, mut b: i128) -> i128 {
    a = a.abs();
    b = b.abs();
    while b != 0 {
        let t = a % b;
        a = b;
        b = t;
    }
    a
}

fn lcm_i64(a: i64, b: i64) -> Result<i64, ExecError> {
    lcm_i128(i128::from(a), i128::from(b)).map(|v| v as i64)
}

/// `lcm(a, b) = |a / gcd(a, b) * b|`, with `lcm(0, x) = 0`, as PostgreSQL does.
fn lcm_i128(a: i128, b: i128) -> Result<i128, ExecError> {
    if a == 0 || b == 0 {
        return Ok(0);
    }
    let g = gcd_i128(a, b);
    (a / g)
        .checked_mul(b)
        .map(i128::abs)
        .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow))
}

fn gcd_numeric(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    let zero = BigDecimal::from(0);
    let (mut a, mut b) = (a.abs(), b.abs());
    while b != zero {
        // `numeric_gcd` is Euclid's algorithm over exact decimals: the remainder
        // is exact because both operands have a finite decimal expansion.
        let t = numeric::canonical(&a % &b);
        a = b;
        b = t;
    }
    numeric::canonical(a)
}

fn lcm_numeric(a: &BigDecimal, b: &BigDecimal) -> BigDecimal {
    let zero = BigDecimal::from(0);
    if *a == zero || *b == zero {
        return zero;
    }
    let g = gcd_numeric(a, b);
    numeric::canonical((a / &g * b).abs())
}

/// PostgreSQL's ceiling on how many decimal digits a `numeric` may carry. A
/// `factorial` argument beyond the value that fits is 22003 rather than an
/// unbounded allocation.
const MAX_FACTORIAL: i64 = 32_177;

fn factorial(n: i64) -> Result<Datum, ExecError> {
    if n < 0 {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::Domain {
            sqlstate: "22003",
            message: "factorial of a negative number is undefined",
        }));
    }
    if n > MAX_FACTORIAL {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::Overflow));
    }
    let mut acc = bigdecimal::num_bigint::BigInt::from(1);
    for i in 2..=n {
        acc *= bigdecimal::num_bigint::BigInt::from(i);
    }
    Ok(Datum::Numeric(NumericValue::from(BigDecimal::from(acc))))
}

// ---- width_bucket ----

/// 2201G, that is `invalid_argument_for_width_bucket_function`.
fn width_bucket_error(message: &'static str) -> ExecError {
    domain("2201G", message)
}

/// `width_bucket(operand, low, high, count)`: which of `count` equal-width
/// buckets spanning `[low, high)` the operand falls in. The result is `0` below
/// the range and `count + 1` above it. A reversed `low`/`high` pair reverses the
/// numbering.
fn width_bucket(op: &Datum, low: &Datum, high: &Datum, count: i64) -> Result<Datum, ExecError> {
    if count <= 0 {
        return Err(width_bucket_error("count must be greater than zero"));
    }
    let count_i32 =
        i32::try_from(count).map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))?;
    let float_input = [op, low, high]
        .iter()
        .any(|d| matches!(d, Datum::Float8(_)));
    if float_input {
        let (op, low, high) = (as_f64(op)?, as_f64(low)?, as_f64(high)?);
        if op.is_nan() || low.is_nan() || high.is_nan() {
            return Err(width_bucket_error(
                "operand, lower bound, and upper bound cannot be NaN",
            ));
        }
        if low.is_infinite() || high.is_infinite() {
            return Err(width_bucket_error("lower and upper bounds must be finite"));
        }
        // PostgreSQL computes one fraction of the way from `low` toward `high`
        // — reversed bounds simply measure from the other end — then floors it
        // into a bucket and clamps to the underflow/overflow buckets.
        let fraction = match low.partial_cmp(&high) {
            Some(Ordering::Less) => (op - low) / (high - low),
            Some(Ordering::Greater) => (low - op) / (low - high),
            _ => return Err(width_bucket_error("lower bound cannot equal upper bound")),
        };
        let bucket = (fraction * count as f64).floor();
        return Ok(Datum::Int4(clamp_bucket(bucket as i64, count_i32)));
    }
    let (op, low, high) = (to_numeric(op)?, to_numeric(low)?, to_numeric(high)?);
    if op.is_nan() || low.is_nan() || high.is_nan() {
        return Err(width_bucket_error(
            "operand, lower bound, and upper bound cannot be NaN",
        ));
    }
    let (Some(low), Some(high)) = (low.as_finite(), high.as_finite()) else {
        return Err(width_bucket_error("lower and upper bounds must be finite"));
    };
    let ascending = match low.cmp(high) {
        Ordering::Equal => {
            return Err(width_bucket_error("lower bound cannot equal upper bound"));
        }
        Ordering::Less => true,
        Ordering::Greater => false,
    };
    // An infinite operand is entirely above or below any finite range.
    let op = match op {
        NumericValue::Infinity => {
            return Ok(Datum::Int4(clamp_bucket(
                if ascending { count + 1 } else { -1 },
                count_i32,
            )));
        }
        NumericValue::NegInfinity => {
            return Ok(Datum::Int4(clamp_bucket(
                if ascending { -1 } else { count + 1 },
                count_i32,
            )));
        }
        NumericValue::NaN => unreachable!("NaN operands returned above"),
        NumericValue::Finite(op) => op,
    };
    let fraction = if ascending {
        (&op - low) / (high - low)
    } else {
        (low - &op) / (low - high)
    };
    let scaled = numeric::floor(&NumericValue::from(fraction * BigDecimal::from(count)));
    // A quotient too large for i64 is far outside the bucket range either way,
    // so it saturates to the underflow or overflow bucket.
    let bucket = numeric::to_i64(&scaled).unwrap_or_else(|_| {
        if scaled.as_finite().is_some_and(|s| s.sign() == Sign::Minus) {
            -1
        } else {
            count + 1
        }
    });
    Ok(Datum::Int4(clamp_bucket(bucket, count_i32)))
}

/// Clamp a floored bucket offset into PostgreSQL's `[0, count + 1]` range. The
/// result is `0` for an operand below the range, and `count + 1` for one at or
/// above the top.
fn clamp_bucket(offset: i64, count: i32) -> i32 {
    offset.saturating_add(1).clamp(0, i64::from(count) + 1) as i32
}

// ---- pseudo-random numbers ----

/// The xoroshiro128\*\* generator behind `random()` and `setseed()`. It is the
/// same algorithm and the same seeding shape as PostgreSQL's `pg_prng`, so the
/// distribution, the range handling and the "same seed, same sequence" contract
/// all hold.
///
/// One documented divergence from PostgreSQL remains:
///
/// - The *stream* is not PostgreSQL's. Its `pg_prng_seed` mixes the 64-bit seed
///   into the two state words with an internal constant that is not part of any
///   documented interface, so a given `setseed(x)` produces a different (equally
///   uniform) sequence here. Distribution and session semantics match, but the
///   seeded sequence remains an upstream regression-suite compatibility gap.
///
/// A SQL session owns one locked generator, so `setseed(x)` survives across
/// statements and executor threads. The thread-local fallback is used only by
/// planning and unit-test contexts that have no SQL session.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Prng {
    s0: u64,
    s1: u64,
}

thread_local! {
    static PRNG: RefCell<Option<Prng>> = const { RefCell::new(None) };
}

impl Prng {
    /// A generator seeded from a 64-bit value. This function mixes the value
    /// across both state words and spins four times, so nearby seeds diverge
    /// immediately.
    pub(crate) fn seeded(seed: u64) -> Prng {
        let mut prng = Prng {
            s0: seed,
            s1: seed ^ 0x9E37_79B9_7F4A_7C15,
        };
        if prng.s0 == 0 && prng.s1 == 0 {
            prng.s1 = 1;
        }
        // pg_prng_seed spins the state four times before handing it out.
        for _ in 0..4 {
            prng.next_u64();
        }
        prng
    }

    /// `setseed`'s mapping from a `[-1, 1]` double onto the 64-bit seed space,
    /// the same scaling PostgreSQL's `pg_prng_fseed` applies.
    pub(crate) fn seed_double(&mut self, seed: f64) {
        let iseed = (seed * 0x7FFF_FFFF_FFFF_FFFF_u64 as f64) as i64;
        *self = Prng::seeded(iseed as u64);
    }

    /// One xoroshiro128\*\* step.
    pub(crate) fn next_u64(&mut self) -> u64 {
        let s0 = self.s0;
        let mut s1 = self.s1;
        let val = s0.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        s1 ^= s0;
        self.s0 = s0.rotate_left(24) ^ s1 ^ (s1 << 16);
        self.s1 = s1.rotate_left(37);
        val
    }

    /// A value in `[0, 1)` built from the top 52 bits, so every representable
    /// mantissa is equally likely. This is PostgreSQL's `pg_prng_double` rule.
    pub(crate) fn next_double(&mut self) -> f64 {
        (self.next_u64() >> 12) as f64 * f64::from_bits(0x3CB0_0000_0000_0000)
    }

    /// A uniform value in `[0, range]` by bitmask-with-rejection. This is the
    /// method PostgreSQL's `pg_prng_uint64_range` uses, so a modulo bias favours
    /// no value.
    pub(crate) fn next_below(&mut self, range: u64) -> u64 {
        if range == 0 {
            return 0;
        }
        let rshift = range.leading_zeros();
        loop {
            let val = self.next_u64() >> rshift;
            if val <= range {
                return val;
            }
        }
    }

    /// Run `body` against this thread's generator, and seed it from the system
    /// entropy source on first use. PostgreSQL seeds each backend the same way,
    /// so an unseeded `random()` is unpredictable in both.
    fn with<R>(body: impl FnOnce(&mut Prng) -> R) -> R {
        PRNG.with(|cell| {
            let mut slot = cell.borrow_mut();
            let prng = slot.get_or_insert_with(|| Prng::seeded(entropy_seed()));
            body(prng)
        })
    }
}

/// A one-off seed for a session (or a fallback thread's first `random()`). Uses
/// the wall clock and process entropy so independent streams do not start from
/// the same state. The `CRABKA_RANDOM_SEED` override makes integration tests
/// reproducible.
pub(crate) fn entropy_seed() -> u64 {
    use std::{
        hash::{BuildHasher, RandomState},
        time::{SystemTime, UNIX_EPOCH},
    };
    if let Some(seed) = configured_random_seed(std::env::var("CRABKA_RANDOM_SEED").ok().as_deref())
    {
        return seed;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    nanos ^ RandomState::new().hash_one(nanos)
}

fn configured_random_seed(value: Option<&str>) -> Option<u64> {
    value?.parse().ok()
}

fn with_prng<R>(ctx: &EvalCtx, body: impl FnOnce(&mut Prng) -> R) -> R {
    if let Some(random) = &ctx.random {
        body(&mut random.lock().expect("session random generator"))
    } else {
        Prng::with(body)
    }
}

/// `random(lo, hi)` over the integer widths and `numeric`.
fn random_range(lo: &Datum, hi: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let bound_error = || ExecError::FunctionError {
        sqlstate: "22023",
        message: "lower bound must be less than or equal to upper bound".into(),
    };
    match (lo, hi) {
        (Datum::Int4(a), Datum::Int4(b)) => {
            if a > b {
                return Err(bound_error());
            }
            let span = i64::from(*b) - i64::from(*a);
            let offset = with_prng(ctx, |p| p.next_below(span as u64));
            Ok(Datum::Int4((i64::from(*a) + offset as i64) as i32))
        }
        _ if as_i64(lo).is_some() && as_i64(hi).is_some() => {
            let (a, b) = (as_i64(lo).unwrap_or(0), as_i64(hi).unwrap_or(0));
            if a > b {
                return Err(bound_error());
            }
            let span = (b as u64).wrapping_sub(a as u64);
            let offset = with_prng(ctx, |p| p.next_below(span));
            Ok(Datum::Int8((a as u64).wrapping_add(offset) as i64))
        }
        _ => {
            let (a, b) = (to_numeric(lo)?, to_numeric(hi)?);
            if a > b {
                return Err(bound_error());
            }
            random_numeric(&a, &b, ctx)
        }
    }
}

/// `random(numeric, numeric)`: uniform over the values at the wider of the two
/// input scales. Crabka draws the offset from the 64-bit generator rather than
/// PostgreSQL's base-10000 digit-at-a-time walk. So the *distribution* and the
/// result scale match, but the seeded *sequence* does not. This is a documented
/// divergence confined to the numeric overload.
fn random_numeric(lo: &NumericValue, hi: &NumericValue, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    // PostgreSQL's `random(numeric, numeric)` rejects a special bound outright.
    if lo.is_nan() {
        return Err(domain("22023", "lower bound cannot be NaN"));
    }
    if hi.is_nan() {
        return Err(domain("22023", "upper bound cannot be NaN"));
    }
    let (Some(lo), Some(hi)) = (lo.as_finite(), hi.as_finite()) else {
        return Err(domain("22023", "lower bound cannot be infinity"));
    };
    let scale = lo
        .fractional_digit_count()
        .max(hi.fractional_digit_count())
        .max(0);
    let unit = BigDecimal::from(1).with_scale(scale);
    let steps = numeric::to_i64(&numeric::trunc(&NumericValue::from((hi - lo) / &unit), 0))
        .map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))?;
    let offset = with_prng(ctx, |p| p.next_below(steps as u64));
    let value = lo + BigDecimal::from(offset as i64) * &unit;
    Ok(Datum::Numeric(NumericValue::from(value.with_scale(scale))))
}

// ---- trigonometry, hyperbolics, roots and logarithms ----

/// PostgreSQL's `RADIANS_PER_DEGREE`. `degrees`/`radians` are defined against
/// this literal. A division by it is NOT the same double as a multiplication by
/// `180/π`, so the constant, and the direction of each operation, both matter.
const RADIANS_PER_DEGREE: f64 = 0.017_453_292_519_943_295;

/// `atan(1)`, the constant PostgreSQL divides by so `atand(1)` is exactly 45.
const ATAN_1_0: f64 = std::f64::consts::FRAC_PI_4;

/// 22003 for an argument outside a function's domain. PostgreSQL raises the
/// same "input is out of range" for `asin(2)`, `acosh(0.5)` and `sin(inf)`.
fn input_out_of_range() -> ExecError {
    domain("22003", "input is out of range")
}

fn unary_float(f: MathFunc, x: f64) -> Result<Datum, ExecError> {
    if x.is_nan() {
        return Ok(Datum::Float8(f64::NAN));
    }
    let v = match f {
        MathFunc::Sin | MathFunc::Cos | MathFunc::Tan | MathFunc::Cot if x.is_infinite() => {
            return Err(input_out_of_range());
        }
        MathFunc::Sin => x.sin(),
        MathFunc::Cos => x.cos(),
        MathFunc::Tan => x.tan(),
        MathFunc::Cot => 1.0 / x.tan(),
        MathFunc::Asin | MathFunc::Acos | MathFunc::Asind | MathFunc::Acosd
            if !(-1.0..=1.0).contains(&x) =>
        {
            return Err(input_out_of_range());
        }
        MathFunc::Asin => x.asin(),
        MathFunc::Acos => x.acos(),
        MathFunc::Atan => x.atan(),
        MathFunc::Sind | MathFunc::Cosd | MathFunc::Tand | MathFunc::Cotd if x.is_infinite() => {
            return Err(input_out_of_range());
        }
        MathFunc::Sind => sind(x),
        MathFunc::Cosd => cosd(x),
        MathFunc::Tand => tand(x),
        MathFunc::Cotd => cotd(x),
        MathFunc::Asind if x < 0.0 => -asind_q1(-x),
        MathFunc::Asind => asind_q1(x),
        MathFunc::Acosd if x < 0.0 => 90.0 + asind_q1(-x),
        MathFunc::Acosd => acosd_q1(x),
        MathFunc::Atand => (x.atan() / ATAN_1_0) * 45.0,
        MathFunc::Sinh => x.sinh(),
        MathFunc::Cosh => x.cosh(),
        MathFunc::Tanh => x.tanh(),
        MathFunc::Asinh => x.asinh(),
        // acosh is defined on [1, inf); atanh on [-1, 1] with infinite endpoints.
        MathFunc::Acosh if x < 1.0 => return Err(input_out_of_range()),
        MathFunc::Acosh => x.acosh(),
        // atanh is defined on [-1, 1], with an infinity of the argument's sign
        // at each endpoint.
        MathFunc::Atanh => match x.abs().partial_cmp(&1.0) {
            Some(Ordering::Greater) => return Err(input_out_of_range()),
            Some(Ordering::Equal) => f64::INFINITY.copysign(x),
            _ => x.atanh(),
        },
        MathFunc::Degrees => x / RADIANS_PER_DEGREE,
        MathFunc::Radians => x * RADIANS_PER_DEGREE,
        MathFunc::Cbrt => cbrt(x),
        _ => return Err(no_matching_function()),
    };
    // The only functions here that can grow a finite argument past float8 are
    // the two exponential hyperbolics and the linear angle conversions;
    // PostgreSQL reports that as 22003 instead of returning an infinity. The
    // rest reach infinity only at a pole (`cot(0)`, `tand(90)`, `atanh(1)`),
    // where PostgreSQL returns the infinity.
    let can_overflow = matches!(
        f,
        MathFunc::Sinh | MathFunc::Cosh | MathFunc::Degrees | MathFunc::Radians
    );
    if v.is_infinite() && x.is_finite() && can_overflow {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::Overflow));
    }
    Ok(Datum::Float8(v))
}

fn float_log10(x: f64) -> Result<Datum, ExecError> {
    if x == 0.0 {
        return Err(domain("2201E", "cannot take logarithm of zero"));
    }
    if x < 0.0 {
        return Err(domain(
            "2201E",
            "cannot take logarithm of a negative number",
        ));
    }
    Ok(Datum::Float8(x.log10()))
}

/// `sin(x°) / sin(30°) / 2`: exactly `0.5` at 30 degrees.
fn sind_0_to_30(x: f64) -> f64 {
    ((x * RADIANS_PER_DEGREE).sin() / (30.0 * RADIANS_PER_DEGREE).sin()) / 2.0
}

/// `1 − (1 − cos(x°)) / (1 − cos(60°)) / 2`: exactly `1` at 0 and `0.5` at 60.
fn cosd_0_to_60(x: f64) -> f64 {
    let one_minus_cos_60 = 1.0 - (60.0 * RADIANS_PER_DEGREE).cos();
    1.0 - ((1.0 - (x * RADIANS_PER_DEGREE).cos()) / one_minus_cos_60) / 2.0
}

/// `cbrt(x)` as the C library computes it. Reduce the exponent with `frexp`,
/// evaluate a sixth-degree minimax polynomial on the `[0.5, 1)` mantissa, refine
/// with one Newton step, then scale back by `2^(e/3)` times the cube root of the
/// leftover `e mod 3`.
///
/// PostgreSQL's `cbrt()` is the platform `cbrt(3)`, and Rust's `f64::cbrt` is a
/// *different*, more accurate implementation. `cbrt(27.0)` is `3` there and
/// `3.0000000000000004` in the C library. A copy of the C routine is what makes
/// crabka's answers identical to the oracle's rather than merely correct.
fn cbrt(x: f64) -> f64 {
    /// `2^(1/3)` and `2^(2/3)`, and their reciprocals. These are the correction
    /// for each possible `e mod 3`.
    const CBRT2: f64 = 1.259_921_049_894_873_2;
    const SQR_CBRT2: f64 = 1.587_401_051_968_199_6;
    const FACTOR: [f64; 5] = [1.0 / SQR_CBRT2, 1.0 / CBRT2, 1.0, CBRT2, SQR_CBRT2];
    if x == 0.0 || !x.is_finite() {
        return x;
    }
    let (xm, xe) = frexp(x.abs());
    let u = 0.354_895_765_043_919_86
        + (1.508_191_937_815_849
            + (-2.114_994_941_673_713
                + (2.446_931_225_635_344_4
                    + (-1.834_692_774_836_130_8
                        + (0.784_932_344_976_639_2 - 0.145_263_899_385_486_37 * xm) * xm)
                        * xm)
                    * xm)
                * xm)
            * xm;
    let t2 = u * u * u;
    let ym = u * (t2 + 2.0 * xm) / (2.0 * t2 + xm) * FACTOR[(2 + xe % 3) as usize];
    ldexp(if x > 0.0 { ym } else { -ym }, xe / 3)
}

/// Split `x` into a mantissa in `[0.5, 1)` and a power of two, as C's `frexp`.
fn frexp(x: f64) -> (f64, i32) {
    /// `2^54`, the scale that lifts a subnormal into the normal range.
    const LIFT: f64 = 18_014_398_509_481_984.0;
    let bits = x.to_bits();
    let raw = ((bits >> 52) & 0x7ff) as i32;
    if raw == 0 {
        let (m, e) = frexp(x * LIFT);
        return (m, e - 54);
    }
    let mantissa = f64::from_bits((bits & 0x800f_ffff_ffff_ffff) | 0x3fe0_0000_0000_0000);
    (mantissa, raw - 1022)
}

/// `y · 2^e`, as C's `ldexp`. Every `e` this module produces is a third of a
/// float8 exponent, so `2^e` is always exactly representable.
fn ldexp(y: f64, e: i32) -> f64 {
    y * 2.0_f64.powi(e)
}

/// The first-quadrant inverse sine, in degrees, stitched at `x = 0.5` so that
/// 0, 0.5 and 1 map to exactly 0, 30 and 90.
fn asind_q1(x: f64) -> f64 {
    if x <= 0.5 {
        (x.asin() / 0.5_f64.asin()) * 30.0
    } else {
        90.0 - (x.acos() / 0.5_f64.acos()) * 60.0
    }
}

/// The first-quadrant inverse cosine, in degrees, stitched at the same point so
/// that 0, 0.5 and 1 map to exactly 90, 60 and 0.
fn acosd_q1(x: f64) -> f64 {
    if x <= 0.5 {
        90.0 - (x.asin() / 0.5_f64.asin()) * 30.0
    } else {
        (x.acos() / 0.5_f64.acos()) * 60.0
    }
}

/// The first-quadrant sine, stitched at 30 degrees so 0/30/90 are exact.
fn sind_q1(x: f64) -> f64 {
    if x <= 30.0 {
        sind_0_to_30(x)
    } else {
        cosd_0_to_60(90.0 - x)
    }
}

/// The first-quadrant cosine, stitched at 60 degrees so 0/60/90 are exact.
fn cosd_q1(x: f64) -> f64 {
    if x <= 60.0 {
        cosd_0_to_60(x)
    } else {
        sind_0_to_30(90.0 - x)
    }
}

/// Fold an angle in degrees into the first quadrant. Returns the reduced angle,
/// and whether each of sine and cosine flips sign.
fn reduce_degrees(x: f64) -> (f64, i32, i32) {
    let mut a = x % 360.0;
    let (mut sin_sign, mut cos_sign) = (1, 1);
    if a < 0.0 {
        a = -a;
        sin_sign = -sin_sign;
    }
    if a > 180.0 {
        a = 360.0 - a;
        sin_sign = -sin_sign;
    }
    if a > 90.0 {
        a = 180.0 - a;
        cos_sign = -cos_sign;
    }
    (a, sin_sign, cos_sign)
}

fn sind(x: f64) -> f64 {
    let (a, sin_sign, _) = reduce_degrees(x);
    f64::from(sin_sign) * sind_q1(a)
}

fn cosd(x: f64) -> f64 {
    let (a, _, cos_sign) = reduce_degrees(x);
    f64::from(cos_sign) * cosd_q1(a)
}

fn tand(x: f64) -> f64 {
    let (a, sin_sign, cos_sign) = reduce_degrees(x);
    let sign = f64::from(sin_sign * cos_sign);
    let (s, c) = (sind_q1(a), cosd_q1(a));
    if c == 0.0 {
        sign * f64::INFINITY
    } else {
        sign * (s / c)
    }
}

fn cotd(x: f64) -> f64 {
    let (a, sin_sign, cos_sign) = reduce_degrees(x);
    let sign = f64::from(sin_sign * cos_sign);
    let (s, c) = (sind_q1(a), cosd_q1(a));
    if s == 0.0 {
        sign * f64::INFINITY
    } else {
        sign * (c / s)
    }
}

#[cfg(test)]
mod tests;
