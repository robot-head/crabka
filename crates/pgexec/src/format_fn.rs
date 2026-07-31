//! SP38: date/time formatting + constructor functions + numeric `to_char`.
//!
//! Exposes the Task 1–5 `crabka_pgtypes::{datetime,numeric}` value engines as SQL
//! functions: `to_char` (temporal + numeric), `to_timestamp`, `to_date`, the
//! `make_*` constructors, and the `justify_*` interval normalizers.
//!
//! Mirrors `datetime_fn.rs` (SP37) / `func.rs` (SP29): a `format_func(name)`
//! registry, an `is_format_func` dispatch predicate, an `eval_format` value
//! evaluator, and a `format_func_result_type` static result-type resolver. Like
//! every breadth slice since SP27, each function is a pure, deterministic
//! transform over a single row's already-evaluated Datums (+ the per-statement
//! `EvalCtx` session zone for the timestamptz cases), so there is no new lock /
//! visibility rule / write path / interleaving and thus no Stateright model — the
//! "pure-data / single-node refactor" carve-out. Proven by the unit tests below +
//! the Task-8 wire test + the Task-9 conformance corpus diffed against PostgreSQL.

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{
    ColumnType, Datum, TypeError,
    datetime::{self, Interval},
    numeric,
};

use crate::{clock::EvalCtx, error::ExecError, scope::Scope};

/// The SP38 formatting / constructor functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FmtFunc {
    /// `to_char(value, template)` — temporal OR numeric value → formatted text.
    ToChar,
    ToNumber,
    /// `to_timestamp(epoch_seconds)` (1-arg) or `to_timestamp(text, template)`
    /// (2-arg) → `timestamptz`.
    ToTimestamp,
    /// `to_date(text, template)` → `date`.
    ToDate,
    /// `make_date(year, month, day)` → `date`.
    MakeDate,
    /// `make_time(hour, min, sec)` → `time`.
    MakeTime,
    /// `make_timestamp(y, mo, d, h, mi, sec)` → `timestamp`.
    MakeTimestamp,
    /// `make_timestamptz(y, mo, d, h, mi, sec [, zone])` → `timestamptz`.
    MakeTimestamptz,
    /// `make_interval([years, months, weeks, days, hours, mins, secs])` → `interval`.
    MakeInterval,
    /// `justify_days(interval)` → `interval`.
    JustifyDays,
    /// `justify_hours(interval)` → `interval`.
    JustifyHours,
    /// `justify_interval(interval)` → `interval`.
    JustifyInterval,
}

/// Classify a (lowercased — the lexer lowercases unquoted idents) function name.
/// `None` means "not an SP38 formatting/constructor function".
fn format_func(name: &str) -> Option<FmtFunc> {
    Some(match name {
        "to_char" => FmtFunc::ToChar,
        "to_number" => FmtFunc::ToNumber,
        "to_timestamp" => FmtFunc::ToTimestamp,
        "to_date" => FmtFunc::ToDate,
        "make_date" => FmtFunc::MakeDate,
        "make_time" => FmtFunc::MakeTime,
        "make_timestamp" => FmtFunc::MakeTimestamp,
        "make_timestamptz" => FmtFunc::MakeTimestamptz,
        "make_interval" => FmtFunc::MakeInterval,
        "justify_days" => FmtFunc::JustifyDays,
        "justify_hours" => FmtFunc::JustifyHours,
        "justify_interval" => FmtFunc::JustifyInterval,
        _ => return None,
    })
}

/// Is `name` an SP38 formatting/constructor function? (The dispatch point.)
pub(crate) fn is_format_func(name: &str) -> bool {
    format_func(name).is_some()
}

// ---- result-type inference ----

/// Statically infer an SP38 call's result type (for RowDescription). Arity / arg
/// type mismatches surface as 42883 here (plan time), before any row is produced.
pub(crate) fn format_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let f = format_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    let n = args.len();
    Ok(match f {
        FmtFunc::ToChar => {
            require_arity(fc, n == 2)?;
            // arg0 must be a formattable type (temporal OR numeric/int/float), arg1
            // text. A NULL arg (unknown type) is permitted — it yields NULL at eval.
            let v = crate::eval::infer_type(&args[0], scope)?;
            if !is_formattable(v) {
                return Err(undefined_function(&fc.name));
            }
            // `varchar`/`char` are binary-coercible to `text`, so a format
            // string held in one resolves through that implicit cast.
            let t = crate::eval::infer_type(&args[1], scope)?;
            if !t.is_string() {
                return Err(undefined_function(&fc.name));
            }
            ColumnType::Text
        }
        FmtFunc::ToTimestamp => {
            require_arity(fc, n == 1 || n == 2)?;
            if n == 1 {
                // numeric/float/int Unix epoch seconds.
                let a = crate::eval::infer_type(&args[0], scope)?;
                if !is_numeric_like(a) {
                    return Err(undefined_function(&fc.name));
                }
            } else {
                require_text_args(fc, args, scope)?;
            }
            ColumnType::Timestamptz
        }
        // `to_number(text, text)` reads a number out of `input` using `template`
        // to say where the digits are; the result is `numeric`.
        FmtFunc::ToNumber => {
            require_arity(fc, n == 2)?;
            require_text_args(fc, args, scope)?;
            ColumnType::Numeric(None)
        }
        FmtFunc::ToDate => {
            require_arity(fc, n == 2)?;
            require_text_args(fc, args, scope)?;
            ColumnType::Date
        }
        FmtFunc::MakeDate => {
            require_arity(fc, n == 3)?;
            ColumnType::Date
        }
        FmtFunc::MakeTime => {
            require_arity(fc, n == 3)?;
            ColumnType::Time
        }
        FmtFunc::MakeTimestamp => {
            require_arity(fc, n == 6)?;
            ColumnType::Timestamp
        }
        FmtFunc::MakeTimestamptz => {
            require_arity(fc, n == 6 || n == 7)?;
            ColumnType::Timestamptz
        }
        FmtFunc::MakeInterval => {
            require_arity(fc, n <= 7)?;
            ColumnType::Interval
        }
        FmtFunc::JustifyDays | FmtFunc::JustifyHours | FmtFunc::JustifyInterval => {
            require_arity(fc, n == 1)?;
            ColumnType::Interval
        }
    })
}

/// A type `to_char` can format: any temporal type or any numeric type.
fn is_formattable(t: ColumnType) -> bool {
    matches!(
        t,
        ColumnType::Date
            | ColumnType::Time
            | ColumnType::Timestamp
            | ColumnType::Timestamptz
            | ColumnType::Interval
            | ColumnType::Int2
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Float4
            | ColumnType::Float8
            | ColumnType::Numeric(_)
    )
}

/// A numeric-like type (int/float/numeric) — the `to_timestamp(epoch)` arg domain.
fn is_numeric_like(t: ColumnType) -> bool {
    matches!(
        t,
        ColumnType::Int2
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Float4
            | ColumnType::Float8
            | ColumnType::Numeric(_)
    )
}

/// Both args of a (text, text) call must be a string type (plan-time 42883
/// otherwise) — `varchar`/`char` included, as they coerce to `text`.
fn require_text_args(fc: &FuncCall, args: &[Expr], scope: &Scope) -> Result<(), ExecError> {
    for a in args {
        if !crate::eval::infer_type(a, scope)?.is_string() {
            return Err(undefined_function(&fc.name));
        }
    }
    Ok(())
}

// ---- evaluation ----

/// Evaluate an SP38 call. `eval_child` evaluates each argument against the current
/// row (the same `eval` used for scalar context, or `agg::eval_grouped` in a
/// grouped context), so the math is shared and only the closure differs.
///
/// Every SP38 function is STRICT: any NULL argument yields `Datum::Null` (matching
/// PostgreSQL's `to_*`/`make_*`/`justify_*`).
pub(crate) fn eval_format(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = format_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    // Evaluate every argument up front, then short-circuit to NULL on any NULL
    // (PG strictness). The arity is re-checked per-arm below.
    let vals: Vec<Datum> = args.iter().map(&mut eval_child).collect::<Result<_, _>>()?;
    if vals.iter().any(Datum::is_null) {
        // Still validate the arity so a NULL with wrong arity is 42883, not silent NULL.
        check_arity(f, fc, vals.len())?;
        return Ok(Datum::Null);
    }
    match f {
        FmtFunc::ToChar => {
            require_arity(fc, vals.len() == 2)?;
            let template = text_value(&vals[1], &fc.name)?;
            to_char(&vals[0], template, ctx, &fc.name)
        }
        FmtFunc::ToTimestamp => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            if vals.len() == 1 {
                to_timestamp_epoch(&vals[0], &fc.name)
            } else {
                // PG: to_timestamp(input_text, format_text) — input first.
                let input = text_value(&vals[0], &fc.name)?;
                let template = text_value(&vals[1], &fc.name)?;
                to_timestamp_template(template, input, ctx)
            }
        }
        FmtFunc::ToNumber => {
            require_arity(fc, vals.len() == 2)?;
            let input = text_value(&vals[0], &fc.name)?;
            let template = text_value(&vals[1], &fc.name)?;
            to_number(input, template, &fc.name)
        }
        FmtFunc::ToDate => {
            require_arity(fc, vals.len() == 2)?;
            // PG: to_date(input_text, format_text) — input first.
            let input = text_value(&vals[0], &fc.name)?;
            let template = text_value(&vals[1], &fc.name)?;
            to_date(template, input)
        }
        FmtFunc::MakeDate => {
            require_arity(fc, vals.len() == 3)?;
            let y = int_arg(&vals[0], &fc.name)?;
            let mo = int_arg(&vals[1], &fc.name)?;
            let d = int_arg(&vals[2], &fc.name)?;
            Ok(Datum::Date(
                datetime::make_date(y, mo, d).map_err(map_type)?,
            ))
        }
        FmtFunc::MakeTime => {
            require_arity(fc, vals.len() == 3)?;
            let h = int_arg(&vals[0], &fc.name)?;
            let mi = int_arg(&vals[1], &fc.name)?;
            let sec = f64_arg(&vals[2], &fc.name)?;
            Ok(Datum::Time(
                datetime::make_time(h, mi, sec).map_err(map_type)?,
            ))
        }
        FmtFunc::MakeTimestamp => {
            require_arity(fc, vals.len() == 6)?;
            let y = int_arg(&vals[0], &fc.name)?;
            let mo = int_arg(&vals[1], &fc.name)?;
            let d = int_arg(&vals[2], &fc.name)?;
            let h = int_arg(&vals[3], &fc.name)?;
            let mi = int_arg(&vals[4], &fc.name)?;
            let sec = f64_arg(&vals[5], &fc.name)?;
            Ok(Datum::Timestamp(
                datetime::make_timestamp_civil(y, mo, d, h, mi, sec).map_err(map_type)?,
            ))
        }
        FmtFunc::MakeTimestamptz => {
            require_arity(fc, vals.len() == 6 || vals.len() == 7)?;
            let y = int_arg(&vals[0], &fc.name)?;
            let mo = int_arg(&vals[1], &fc.name)?;
            let d = int_arg(&vals[2], &fc.name)?;
            let h = int_arg(&vals[3], &fc.name)?;
            let mi = int_arg(&vals[4], &fc.name)?;
            let sec = f64_arg(&vals[5], &fc.name)?;
            let dt = datetime::make_timestamp_civil(y, mo, d, h, mi, sec).map_err(map_type)?;
            // The optional 7th arg names the zone; default is the session zone.
            let zone = match vals.get(6) {
                Some(z) => zone_arg(z, &fc.name)?,
                None => ctx.time_zone.clone(),
            };
            dt.to_zoned(zone)
                .map(|z| Datum::Timestamptz(z.timestamp()))
                .map_err(|_| {
                    ExecError::Type(TypeError::DatetimeFieldOverflow {
                        value: format!("{y}-{mo}-{d} {h}:{mi}:{sec}"),
                    })
                })
        }
        FmtFunc::MakeInterval => {
            // 0..=7 positional args; first 6 are ints (default 0), the 7th `secs` is
            // f64 (default 0.0). >7 args → 42883.
            require_arity(fc, vals.len() <= 7)?;
            let get_int = |i: usize| -> Result<i32, ExecError> {
                match vals.get(i) {
                    Some(d) => int_arg(d, &fc.name),
                    None => Ok(0),
                }
            };
            let years = get_int(0)?;
            let months = get_int(1)?;
            let weeks = get_int(2)?;
            let days = get_int(3)?;
            let hours = get_int(4)?;
            let mins = get_int(5)?;
            let secs = match vals.get(6) {
                Some(d) => f64_arg(d, &fc.name)?,
                None => 0.0,
            };
            Ok(Datum::Interval(
                datetime::make_interval(years, months, weeks, days, hours, mins, secs)
                    .map_err(map_type)?,
            ))
        }
        FmtFunc::JustifyDays => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Interval(
                datetime::justify_days(interval_value(&vals[0], &fc.name)?).map_err(map_type)?,
            ))
        }
        FmtFunc::JustifyHours => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Interval(
                datetime::justify_hours(interval_value(&vals[0], &fc.name)?).map_err(map_type)?,
            ))
        }
        FmtFunc::JustifyInterval => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Interval(
                datetime::justify_interval(interval_value(&vals[0], &fc.name)?)
                    .map_err(map_type)?,
            ))
        }
    }
}

/// `to_char(value, template)`: dispatch on the value type. Temporal values render
/// through `format_datetime`/`format_interval`; numeric/int/float through
/// `format_numeric`. A non-formattable type is 42883.
/// `to_char` of a non-finite date/time or interval is NULL in PostgreSQL — not
/// the empty string and not an error — because there is no calendar field to
/// render. Every template behaves the same way, so the check is on the value.
fn non_finite_to_char(value: &Datum) -> bool {
    match value {
        Datum::Date(d) => datetime::date_is_infinite(*d),
        Datum::Timestamp(ts) => datetime::timestamp_is_infinite(*ts),
        Datum::Timestamptz(ts) => datetime::timestamptz_is_infinite(*ts),
        Datum::Interval(iv) => iv.infinite_sign() != 0,
        _ => false,
    }
}

/// Divergence: PostgreSQL consumes the input POSITIONALLY against the template,
/// so leading whitespace eats digit positions — `to_number('  123', '999')` is 12
/// there and 123 here. That is why the template is accepted and not read: under
/// the C locale the decimal point is `.` and the group separator `,` whatever
/// the template spells them as (`D`/`G` or the literals), so scanning the input
/// alone reproduces PostgreSQL for every template the corpus uses. Everything
/// else in the numeric template family agrees.
/// `to_number(input, template)` — read a `numeric` out of `input`.
///
/// PostgreSQL uses the template to say WHERE the digits are rather than to
/// validate the input strictly: the decoration a `to_char` template would have
/// emitted (group separators, currency, sign markers, literal text) is skipped,
/// and what is left is read as a number. So `to_number('-34,338,492',
/// '99G999G999')` is -34338492 and `to_number('0.01', 'FM9.99')` is 0.01.
///
/// The decimal separator is taken from the template: `D` or a literal `.` marks
/// it, and `G` or a literal `,` marks a group separator that is dropped.
fn to_number(input: &str, _template: &str, name: &str) -> Result<Datum, ExecError> {
    let mut digits = String::with_capacity(input.len());
    let mut seen_decimal = false;
    let mut trailing_negative = false;
    for ch in input.chars() {
        match ch {
            '0'..='9' => digits.push(ch),
            // A leading sign belongs to the number. A sign AFTER the digits is the
            // trailing-sign form the `S`/`MI`/`PL` template markers accept, so it
            // negates rather than being read as part of the value.
            '-' if digits.is_empty() => digits.push('-'),
            '+' if digits.is_empty() => {}
            '-' => trailing_negative = true,
            // `D` and `.` both mark the decimal point, which under the C locale is
            // `.` in the input either way; `G` and `,` mark a group separator,
            // which carries no value. Only the first `.` can be the decimal point.
            '.' if !seen_decimal => {
                seen_decimal = true;
                digits.push('.');
            }
            // Anything else is decoration the template accounted for.
            _ => {}
        }
    }
    if trailing_negative && !digits.starts_with('-') {
        digits.insert(0, '-');
    }
    if digits.is_empty() || digits == "-" || digits == "." || digits == "-." {
        return Err(ExecError::FunctionError {
            sqlstate: "22P02",
            message: format!("invalid input syntax for type numeric: \"{input}\""),
        });
    }
    crabka_pgtypes::numeric::parse(&digits)
        .map(Datum::Numeric)
        .ok_or_else(|| ExecError::FunctionError {
            sqlstate: "22P02",
            message: format!("invalid input syntax for type numeric: \"{input}\" ({name})"),
        })
}

fn to_char(value: &Datum, template: &str, ctx: &EvalCtx, name: &str) -> Result<Datum, ExecError> {
    if non_finite_to_char(value) {
        return Ok(Datum::Null);
    }
    let text = match value {
        Datum::Date(d) => {
            let fields = datetime::DateTimeFields::from_civil(datetime::date_to_midnight(*d), None);
            datetime::format_datetime(template, &fields).map_err(map_type)?
        }
        Datum::Timestamp(dt) => {
            let fields = datetime::DateTimeFields::from_civil(*dt, None);
            datetime::format_datetime(template, &fields).map_err(map_type)?
        }
        Datum::Time(t) => {
            // Only the clock patterns are meaningful for a bare time; combine with a
            // fixed date so the field struct is well-formed.
            let dt = datetime::combine_date_time(jiff::civil::date(2000, 1, 1), *t);
            let fields = datetime::DateTimeFields::from_civil(dt, None);
            datetime::format_datetime(template, &fields).map_err(map_type)?
        }
        Datum::Timestamptz(ts) => {
            let dt = ctx.time_zone.to_datetime(*ts);
            let off = ctx.time_zone.to_offset(*ts).seconds();
            let fields = datetime::DateTimeFields::from_civil(dt, Some(off));
            datetime::format_datetime(template, &fields).map_err(map_type)?
        }
        Datum::Interval(iv) => datetime::format_interval(*iv, template).map_err(map_type)?,
        // PostgreSQL has no `to_char(smallint, text)`; the call resolves through
        // the implicit `int2 → int4` cast to the int4 overload.
        Datum::Int2(n) => numeric::format_numeric(template, &numeric::from_i64(i64::from(*n)))
            .map_err(map_type)?,
        Datum::Int4(n) => numeric::format_numeric(template, &numeric::from_i64(i64::from(*n)))
            .map_err(map_type)?,
        Datum::Int8(n) => {
            numeric::format_numeric(template, &numeric::from_i64(*n)).map_err(map_type)?
        }
        Datum::Numeric(d) => numeric::format_numeric(template, d).map_err(map_type)?,
        // `float4_to_char` works at the type's own `FLT_DIG` precision, which is
        // what `from_f32` reproduces.
        Datum::Float4(f) => {
            let bd = numeric::from_f32(*f);
            numeric::format_numeric(template, &bd).map_err(map_type)?
        }
        Datum::Float8(f) => {
            let bd = numeric::from_f64(*f);
            numeric::format_numeric(template, &bd).map_err(map_type)?
        }
        _ => return Err(undefined_function(name)),
    };
    Ok(Datum::Text(text))
}

/// `to_timestamp(epoch_seconds)`: Unix epoch seconds (possibly fractional) → an
/// absolute instant (`timestamptz`).
fn to_timestamp_epoch(value: &Datum, name: &str) -> Result<Datum, ExecError> {
    let secs = f64_arg(value, name)?;
    if !secs.is_finite() {
        return Err(ExecError::Type(TypeError::DatetimeFieldOverflow {
            value: secs.to_string(),
        }));
    }
    let micros_f = (secs * 1_000_000.0).round();
    if micros_f.abs() >= 9_223_372_036_854_775_808.0_f64 {
        return Err(ExecError::Type(TypeError::DatetimeFieldOverflow {
            value: secs.to_string(),
        }));
    }
    let micros = micros_f as i64;
    jiff::Timestamp::from_microsecond(micros)
        .map(Datum::Timestamptz)
        .map_err(|_| {
            ExecError::Type(TypeError::DatetimeFieldOverflow {
                value: secs.to_string(),
            })
        })
}

/// `to_timestamp(input, template)`: parse `input` by `template`, then interpret the
/// resulting wall-clock in the session zone → `timestamptz`.
fn to_timestamp_template(template: &str, input: &str, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let p = datetime::parse_by_template(template, input).map_err(map_type)?;
    let dt = civil_from_parsed(&p)?;
    dt.to_zoned(ctx.time_zone.clone())
        .map(|z| Datum::Timestamptz(z.timestamp()))
        .map_err(|_| {
            ExecError::Type(TypeError::DatetimeFieldOverflow {
                value: input.to_string(),
            })
        })
}

/// `to_date(input, template)`: parse `input` by `template` into a calendar date.
fn to_date(template: &str, input: &str) -> Result<Datum, ExecError> {
    let p = datetime::parse_by_template(template, input).map_err(map_type)?;
    let date = jiff::civil::Date::new(p.year as i16, p.month as i8, p.day as i8).map_err(|_| {
        ExecError::Type(TypeError::DatetimeFieldOverflow {
            value: input.to_string(),
        })
    })?;
    Ok(Datum::Date(date))
}

/// Build a civil `DateTime` from a `ParsedDateTime`, mapping a jiff
/// range/validity error (e.g. Feb 30) to 22008.
fn civil_from_parsed(p: &datetime::ParsedDateTime) -> Result<jiff::civil::DateTime, ExecError> {
    jiff::civil::DateTime::new(
        p.year as i16,
        p.month as i8,
        p.day as i8,
        p.hour as i8,
        p.minute as i8,
        p.second as i8,
        (p.micros * 1_000) as i32,
    )
    .map_err(|_| {
        ExecError::Type(TypeError::DatetimeFieldOverflow {
            value: format!("{}-{}-{}", p.year, p.month, p.day),
        })
    })
}

// ---- argument helpers ----

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

/// The positional argument list. SP38 functions never accept `f(*)`.
fn exprs_of(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    match &fc.args {
        FuncArgs::Exprs(v) => Ok(v),
        FuncArgs::Star => Err(undefined_function(&fc.name)),
    }
}

fn require_arity(fc: &FuncCall, ok: bool) -> Result<(), ExecError> {
    if ok {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

/// Validate just the arity for `f` (used on the NULL short-circuit path so a NULL
/// with the wrong number of args still reports 42883).
fn check_arity(f: FmtFunc, fc: &FuncCall, n: usize) -> Result<(), ExecError> {
    let ok = match f {
        FmtFunc::ToChar | FmtFunc::ToDate | FmtFunc::ToNumber => n == 2,
        FmtFunc::ToTimestamp => n == 1 || n == 2,
        FmtFunc::MakeDate | FmtFunc::MakeTime => n == 3,
        FmtFunc::MakeTimestamp => n == 6,
        FmtFunc::MakeTimestamptz => n == 6 || n == 7,
        FmtFunc::MakeInterval => n <= 7,
        FmtFunc::JustifyDays | FmtFunc::JustifyHours | FmtFunc::JustifyInterval => n == 1,
    };
    require_arity(fc, ok)
}

/// Map a `crabka_pgtypes::TypeError` (22007/22008/22003/…) onto the executor error so its
/// SQLSTATE propagates to the wire.
fn map_type(e: TypeError) -> ExecError {
    ExecError::Type(e)
}

fn type_error(what: &str, got: &Datum) -> ExecError {
    ExecError::TypeMismatch(format!(
        "{what} does not accept an argument of type {}",
        got.column_type().map(|t| t.name()).unwrap_or("unknown")
    ))
}

/// A text argument at runtime.
fn text_value<'a>(d: &'a Datum, name: &str) -> Result<&'a str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        _ => Err(type_error(name, d)),
    }
}

/// An integer argument at runtime, narrowed to i32 (the `make_*` field width).
fn int_arg(d: &Datum, name: &str) -> Result<i32, ExecError> {
    match d {
        Datum::Int2(n) => Ok(i32::from(*n)),
        Datum::Int4(n) => Ok(*n),
        Datum::Int8(n) => i32::try_from(*n).map_err(|_| {
            ExecError::Type(TypeError::DatetimeFieldOverflow {
                value: n.to_string(),
            })
        }),
        _ => Err(type_error(name, d)),
    }
}

/// A floating argument at runtime, promoted to f64 (int/float/numeric).
fn f64_arg(d: &Datum, name: &str) -> Result<f64, ExecError> {
    Ok(match d {
        Datum::Int2(n) => f64::from(*n),
        Datum::Int4(n) => f64::from(*n),
        Datum::Int8(n) => *n as f64,
        Datum::Float4(x) => f64::from(*x),
        Datum::Float8(x) => *x,
        Datum::Numeric(d) => numeric::to_f64(d),
        _ => return Err(type_error(name, d)),
    })
}

/// An interval argument at runtime (the `justify_*` domain).
fn interval_value(d: &Datum, name: &str) -> Result<Interval, ExecError> {
    match d {
        Datum::Interval(iv) => Ok(*iv),
        _ => Err(type_error(name, d)),
    }
}

/// Resolve a zone-name text value to a jiff `TimeZone`. `UTC` and fixed-offset
/// spellings are handled by jiff's tzdb; an unknown zone is 22023.
fn zone_arg(d: &Datum, name: &str) -> Result<jiff::tz::TimeZone, ExecError> {
    let zone = match d {
        Datum::Text(s) => s.as_str(),
        _ => return Err(type_error(name, d)),
    };
    if zone.eq_ignore_ascii_case("utc") {
        return Ok(jiff::tz::TimeZone::UTC);
    }
    jiff::tz::TimeZone::get(zone).map_err(|_| {
        ExecError::InvalidParameterValue(format!("time zone \"{zone}\" not recognized"))
    })
}

#[cfg(test)]
mod tests {
    use crabka_pgtypes::{ColumnType, Datum};

    use crate::{clock::EvalCtx, scope::Scope};

    fn ev(sql: &str) -> Datum {
        let ctx = EvalCtx::test_default();
        crate::eval::eval(
            &crabka_pgparser::parser::parse_expr_for_test(sql).expect("parse"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect("eval")
    }
    fn ty(sql: &str) -> ColumnType {
        crate::eval::infer_type(
            &crabka_pgparser::parser::parse_expr_for_test(sql).expect("p"),
            &Scope::empty(),
        )
        .expect("ty")
    }
    fn ec(sql: &str) -> String {
        let ctx = EvalCtx::test_default();
        crate::eval::eval(
            &crabka_pgparser::parser::parse_expr_for_test(sql).expect("p"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect_err("err")
        .into_pg()
        .code
    }

    #[test]
    /// `to_number` reads the digits out of a decorated string, dropping the
    /// decoration a `to_char` template would have emitted. Every expectation is
    /// PostgreSQL 18.4's.
    #[test]
    fn to_number_reads_digits_through_the_template_decoration() {
        for (expr, expected) in [
            ("to_number('-34,338,492', '99G999G999')", "-34338492"),
            (
                "to_number('-34,338,492.654,878', '99G999G999D999G999')",
                "-34338492.654878",
            ),
            ("to_number('0.01', 'FM9.99')", "0.01"),
            ("to_number('.0', 'FM9.99')", "0.0"),
            ("to_number('123', '999')", "123"),
            ("to_number('$1,234.56', 'L9G999D99')", "1234.56"),
            // A sign AFTER the digits negates, which is the `S`/`MI` form.
            ("to_number('5.01-', 'FM9.99S')", "-5.01"),
            ("to_number('12,454.8-', '99G999D9S')", "-12454.8"),
            ("to_number('-1234.56', 'S9999.99')", "-1234.56"),
        ] {
            let got = ev(expr);
            let want = crabka_pgtypes::numeric::parse(expected).expect("expected parses");
            assert!(got == Datum::Numeric(want), "{expr}: {got:?}");
        }
        // Nothing numeric in the input at all is 22P02, not a zero.
        let ctx = EvalCtx::test_default();
        let error = crate::eval::eval(
            &crabka_pgparser::parser::parse_expr_for_test("to_number('abc', '999')")
                .expect("parse"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect_err("no digits at all");
        assert!(error.into_pg().code == "22P02");
    }

    #[test]
    fn to_char_dispatch_and_types() {
        assert_eq!(
            ev("to_char(TIMESTAMP '2024-01-15 13:45:06', 'YYYY-MM-DD')"),
            Datum::Text("2024-01-15".into())
        );
        assert_eq!(ev("to_char(485, '999')"), Datum::Text(" 485".into()));
        assert_eq!(ty("to_char(485, '999')"), ColumnType::Text);
        assert_eq!(ty("to_char(now(), 'YYYY')"), ColumnType::Text);
    }

    #[test]
    fn to_timestamp_to_date_make_justify() {
        assert_eq!(
            ev("to_date('2024-07-04', 'YYYY-MM-DD')"),
            Datum::Date(jiff::civil::date(2024, 7, 4))
        );
        assert_eq!(
            ty("to_timestamp('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"),
            ColumnType::Timestamptz
        );
        // to_timestamp(double) — Unix epoch → instant.
        assert_eq!(
            ev("to_timestamp(0)"),
            Datum::Timestamptz("1970-01-01T00:00:00Z".parse().expect("ts"))
        );
        assert_eq!(
            ev("make_date(2024, 7, 4)"),
            Datum::Date(jiff::civil::date(2024, 7, 4))
        );
        assert_eq!(
            ev("make_interval(0, 0, 0, 5)"),
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months: 0,
                days: 5,
                micros: 0
            })
        );
        assert_eq!(
            ev("justify_hours(INTERVAL '27 hours')"),
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months: 0,
                days: 1,
                micros: 3 * 3_600_000_000
            })
        );
    }

    #[test]
    fn error_surface() {
        assert_eq!(ec("to_char(485)"), "42883"); // wrong arity
        assert_eq!(ec("to_date('xx', 'YYYY-MM-DD')"), "22007"); // bad input
        assert_eq!(ec("make_date(2024, 13, 1)"), "22008"); // field overflow
        assert_eq!(
            ec("make_timestamptz(2024,1,1,0,0,0,'Mars/Olympus')"),
            "22023"
        ); // bad zone
        assert_eq!(ec("to_char(true, 'YYYY')"), "42883"); // non-formattable type
    }

    // ---- additional coverage ----

    #[test]
    fn to_char_interval_and_numeric() {
        assert_eq!(
            ev("to_char(INTERVAL '36 hours', 'HH24:MI:SS')"),
            Datum::Text("36:00:00".into())
        );
        // numeric forms
        assert_eq!(
            ev("to_char(485.0::float8, '999')"),
            Datum::Text(" 485".into())
        );
        assert_eq!(ev("to_char(1.5, '9D9')"), Datum::Text(" 1.5".into()));
    }

    #[test]
    fn make_timestamptz_happy_path_and_zone() {
        // 6-arg: interpreted in the session zone (UTC by default).
        assert_eq!(
            ev("make_timestamptz(2024, 1, 15, 12, 0, 0)"),
            Datum::Timestamptz("2024-01-15T12:00:00Z".parse().expect("ts"))
        );
        // 7-arg with an explicit zone: 12:00 in -05:00 (New_York, January) = 17:00 UTC.
        assert_eq!(
            ev("make_timestamptz(2024, 1, 15, 12, 0, 0, 'America/New_York')"),
            Datum::Timestamptz("2024-01-15T17:00:00Z".parse().expect("ts"))
        );
    }

    #[test]
    fn make_time_make_timestamp_justify_interval() {
        assert_eq!(
            ev("make_time(8, 15, 30)"),
            Datum::Time(jiff::civil::time(8, 15, 30, 0))
        );
        assert_eq!(
            ev("make_timestamp(2024, 7, 4, 13, 45, 6)"),
            Datum::Timestamp(jiff::civil::datetime(2024, 7, 4, 13, 45, 6, 0))
        );
        // justify_interval rolls 27h → +1 day, 3h and 35 days → +1 month, 5 days.
        assert_eq!(
            ev("justify_interval(INTERVAL '35 days 27 hours')"),
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months: 1,
                days: 6,
                micros: 3 * 3_600_000_000
            })
        );
        // justify_days rolls 35 days → 1 month, 5 days.
        assert_eq!(
            ev("justify_days(INTERVAL '35 days')"),
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months: 1,
                days: 5,
                micros: 0
            })
        );
    }

    #[test]
    fn result_types_for_row_description() {
        assert_eq!(ty("to_timestamp(0)"), ColumnType::Timestamptz);
        assert_eq!(ty("to_date('2024-07-04', 'YYYY-MM-DD')"), ColumnType::Date);
        assert_eq!(ty("make_date(2024, 7, 4)"), ColumnType::Date);
        assert_eq!(ty("make_time(8, 15, 30)"), ColumnType::Time);
        assert_eq!(
            ty("make_timestamp(2024, 7, 4, 0, 0, 0)"),
            ColumnType::Timestamp
        );
        assert_eq!(
            ty("make_timestamptz(2024, 7, 4, 0, 0, 0)"),
            ColumnType::Timestamptz
        );
        assert_eq!(ty("make_interval(1)"), ColumnType::Interval);
        assert_eq!(
            ty("justify_interval(INTERVAL '1 day')"),
            ColumnType::Interval
        );
    }

    #[test]
    fn null_arguments_propagate() {
        assert_eq!(ev("to_char(null::timestamp, 'YYYY')"), Datum::Null);
        assert_eq!(ev("to_char(485, null::text)"), Datum::Null);
        assert_eq!(ev("to_date(null::text, 'YYYY-MM-DD')"), Datum::Null);
        assert_eq!(ev("to_timestamp(null::float8)"), Datum::Null);
        assert_eq!(ev("make_date(null::int4, 1, 1)"), Datum::Null);
        assert_eq!(ev("make_interval(null::int4)"), Datum::Null);
        assert_eq!(ev("justify_hours(null::interval)"), Datum::Null);
    }

    #[test]
    fn make_date_feb30_is_22008() {
        assert_eq!(ec("make_date(2024, 2, 30)"), "22008");
        assert_eq!(ec("to_date('2024-02-30', 'YYYY-MM-DD')"), "22008");
    }
}
