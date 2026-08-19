//! SP38: date/time formatting + constructor functions + numeric `to_char`.
//!
//! This module exposes the Task 1–5 `crabka_pgtypes::{datetime,numeric}` value
//! engines as SQL functions: `to_char` for temporal and numeric values,
//! `to_timestamp`, `to_date`, the `make_*` constructors, and the `justify_*`
//! interval normalizers.
//!
//! It follows `datetime_fn.rs` (SP37) and `func.rs` (SP29). It holds a
//! `format_func(name)` registry, an `is_format_func` dispatch predicate, an
//! `eval_format` value evaluator, and a `format_func_result_type` static
//! result-type resolver. Like every breadth slice since SP27, each function is a
//! pure, deterministic transform over a single row's already-evaluated Datums,
//! plus the per-statement `EvalCtx` session zone for the timestamptz cases. So
//! there is no new lock, no new visibility rule, no new write path and no new
//! interleaving, and so no Stateright model. That is the "pure-data /
//! single-node refactor" carve-out. The unit tests below prove it, together with
//! the Task-8 wire test and the Task-9 conformance corpus diffed against
//! PostgreSQL.

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
    /// `to_char(value, template)`: temporal OR numeric value → formatted text.
    ToChar,
    ToNumber,
    /// `to_timestamp(epoch_seconds)` with one argument, or
    /// `to_timestamp(text, template)` with two, → `timestamptz`.
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

/// Classify a lowercased function name. The lexer lowercases unquoted idents.
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

/// Is `name` an SP38 formatting/constructor function? This is the dispatch
/// point.
pub(crate) fn is_format_func(name: &str) -> bool {
    format_func(name).is_some()
}

// ---- result-type inference ----

/// Statically infer an SP38 call's result type, for RowDescription. An arity or
/// argument-type mismatch is 42883 here, at plan time, before any row exists.
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

/// A numeric-like type, that is int, float or numeric. This is the
/// `to_timestamp(epoch)` argument domain.
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

/// Both arguments of a (text, text) call must be a string type, and any other
/// type is a plan-time 42883. `varchar` and `char` count, because they coerce to
/// `text`.
fn require_text_args(fc: &FuncCall, args: &[Expr], scope: &Scope) -> Result<(), ExecError> {
    for a in args {
        if !crate::eval::infer_type(a, scope)?.is_string() {
            return Err(undefined_function(&fc.name));
        }
    }
    Ok(())
}

// ---- evaluation ----

/// Evaluate an SP38 call. `eval_child` evaluates each argument against the
/// current row. It is the same `eval` the scalar context uses, or
/// `agg::eval_grouped` in a grouped context. So the math is shared and only the
/// closure differs.
///
/// Every SP38 function is STRICT: any NULL argument yields `Datum::Null`, which
/// matches PostgreSQL's `to_*`/`make_*`/`justify_*`.
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
            // A reading on a daylight-saving boundary resolves by PostgreSQL's
            // rule, not jiff's default; see `datetime::zone_offset_for`.
            datetime::zoned_instant(dt, &zone)
                .map(Datum::Timestamptz)
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

/// `to_char(value, template)`: dispatch on the value type. Temporal values
/// render through `format_datetime`/`format_interval`, and numeric, int and
/// float values render through `format_numeric`. A non-formattable type is
/// 42883.
///
/// `to_char` of a non-finite date/time or interval is NULL in PostgreSQL. It is
/// not the empty string and not an error, because there is no calendar field to
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

/// `to_number(input, template)`: read a `numeric` out of `input`.
///
/// PostgreSQL uses the template to say WHERE the digits are, rather than to
/// validate the input strictly. This function skips the decoration a `to_char`
/// template would have emitted, that is group separators, currency, sign markers
/// and literal text, and reads what is left as a number. So
/// `to_number('-34,338,492', '99G999G999')` is -34338492 and
/// `to_number('0.01', 'FM9.99')` is 0.01.
///
/// The decimal separator comes from the template. `D` or a literal `.` marks it,
/// and `G` or a literal `,` marks a group separator that this function drops.
///
/// Divergence: PostgreSQL consumes the input POSITIONALLY against the template,
/// so leading whitespace eats digit positions. `to_number('  123', '999')` is 12
/// there and 123 here. That is why this function accepts the template and does
/// not read it. Under the C locale the decimal point is `.` and the group
/// separator is `,`, whatever the template spells them as, either `D`/`G` or the
/// literals. So a scan of the input alone reproduces PostgreSQL for every
/// template the corpus uses. Everything else in the numeric template family
/// agrees.
fn to_number(input: &str, template: &str, name: &str) -> Result<Datum, ExecError> {
    match numeric::number_template(template) {
        numeric::NumberTemplate::Digits => {}
        numeric::NumberTemplate::Refused(numeric::RomanRefusal::Twice) => {
            return Err(ExecError::FunctionError {
                sqlstate: "42601",
                message: "cannot use \"RN\" twice".to_string(),
            });
        }
        numeric::NumberTemplate::Refused(numeric::RomanRefusal::Incompatible) => {
            return Err(ExecError::FunctionErrorWithDetail {
                sqlstate: "42601",
                message: "\"RN\" is incompatible with other formats",
                detail: "\"RN\" may only be used together with \"FM\".",
            });
        }
        numeric::NumberTemplate::Roman => {
            // An empty input never reaches PostgreSQL's Roman decoder at all:
            // the processor stops on the exhausted input and `numeric_in` is
            // handed the bare sign space instead.
            if input.is_empty() {
                return Err(ExecError::FunctionError {
                    sqlstate: "22P02",
                    message: "invalid input syntax for type numeric: \" \"".to_string(),
                });
            }
            return match numeric::roman_to_int(input) {
                Some(value) => Ok(Datum::Numeric(numeric::from_i64(i64::from(value)))),
                None => Err(ExecError::FunctionError {
                    sqlstate: "22P02",
                    message: "invalid Roman numeral".to_string(),
                }),
            };
        }
    }
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
            // Only the clock patterns are meaningful for a bare time; the field
            // struct carries a fixed date so it is well-formed.
            let fields = datetime::DateTimeFields::from_time(*t, None);
            datetime::format_datetime(template, &fields).map_err(map_type)?
        }
        Datum::Timestamptz(ts) => {
            let zone = ctx.time_zone.to_offset_info(*ts);
            let fields = datetime::DateTimeFields::from_civil(
                zone.offset().to_datetime(*ts),
                Some(zone.offset().seconds()),
            )
            .with_tz_name(Some(zone.abbreviation().to_string()));
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
        // `float4_to_char` / `float8_to_char` clamp the template's fractional
        // positions to the type's own decimal digits, which `NumPrecision` carries.
        Datum::Float4(f) => {
            let bd = numeric::from_f32(*f);
            numeric::format_numeric_prec(template, &bd, numeric::NumPrecision::Float4)
                .map_err(map_type)?
        }
        Datum::Float8(f) => {
            let bd = numeric::from_f64(*f);
            numeric::format_numeric_prec(template, &bd, numeric::NumPrecision::Float8)
                .map_err(map_type)?
        }
        _ => return Err(undefined_function(name)),
    };
    Ok(Datum::Text(text))
}

/// `to_timestamp(epoch_seconds)`: Unix epoch seconds, which may be fractional, →
/// an absolute instant, that is a `timestamptz`.
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

/// `to_timestamp(input, template)`: parse `input` by `template`, then reduce the
/// resulting wall-clock to an instant → `timestamptz`.
///
/// A template that named a zone (`TZ`, `OF`, `TZH`/`TZM`) fixes the offset
/// itself; otherwise the reading is local to the session zone, resolved by the
/// same rule a bare `timestamp` cast uses.
fn to_timestamp_template(template: &str, input: &str, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let p = datetime::parse_by_template(template, input).map_err(map_type)?;
    let dt = civil_from_parsed(&p, input)?;
    let out_of_range = || {
        ExecError::Type(TypeError::DatetimeFieldOverflow {
            value: input.to_string(),
        })
    };
    let instant = match p.tz_offset_secs {
        Some(secs) => jiff::tz::Offset::from_seconds(secs)
            .and_then(|offset| offset.to_timestamp(dt))
            .map_err(|_| out_of_range())?,
        None => datetime::zoned_instant(dt, &ctx.time_zone).map_err(|_| out_of_range())?,
    };
    let instant = match p.fractional_precision {
        Some(precision) => round_to_precision(instant, precision).ok_or_else(out_of_range)?,
        None => instant,
    };
    Ok(Datum::Timestamptz(instant))
}

/// Round an instant to `precision` fractional-second digits.
///
/// `PostgreSQL`'s `AdjustTimestampForTypmod`, which is what an `FF`n pattern
/// ends up applying: the microsecond count is rounded half away from zero, taken
/// about PostgreSQL's own 2000-01-01 epoch. The epoch matters only for the
/// tie-breaking direction on instants before it, which is precisely where
/// rounding about the Unix epoch would disagree.
fn round_to_precision(instant: jiff::Timestamp, precision: u8) -> Option<jiff::Timestamp> {
    /// Microseconds between the Unix epoch and PostgreSQL's 2000-01-01 epoch.
    const PG_EPOCH_MICROS: i64 = 946_684_800 * 1_000_000;
    if precision >= 6 {
        return Some(instant);
    }
    let scale = 10_i64.checked_pow(6 - u32::from(precision))?;
    let half = scale / 2;
    let micros = instant.as_microsecond().checked_sub(PG_EPOCH_MICROS)?;
    let rounded = if micros >= 0 {
        micros.checked_add(half)? / scale * scale
    } else {
        -((micros.checked_neg()?.checked_add(half)?) / scale * scale)
    };
    jiff::Timestamp::from_microsecond(rounded.checked_add(PG_EPOCH_MICROS)?).ok()
}

/// `to_date(input, template)`: parse `input` by `template` into a calendar date.
/// Any zone the template named is parsed and discarded, as `PostgreSQL` does —
/// a `date` has no zone to carry it.
fn to_date(template: &str, input: &str) -> Result<Datum, ExecError> {
    let p = datetime::parse_by_template(template, input).map_err(map_type)?;
    let date = date_from_parsed(&p, input)?;
    Ok(Datum::Date(date.into()))
}

/// Build a civil `Date` from a `ParsedDateTime`.
///
/// The parse has already range-checked every field, so the only failure left is
/// a year outside the ±9999 jiff stores a date in — the storage limit that keeps
/// gres short of PostgreSQL's 294276 AD.
fn date_from_parsed(
    p: &datetime::ParsedDateTime,
    input: &str,
) -> Result<jiff::civil::Date, ExecError> {
    let out_of_range = || {
        ExecError::Type(TypeError::DatetimeFieldOverflow {
            value: input.to_string(),
        })
    };
    let year = i16::try_from(p.year).map_err(|_| out_of_range())?;
    let month = i8::try_from(p.month).map_err(|_| out_of_range())?;
    let day = i8::try_from(p.day).map_err(|_| out_of_range())?;
    jiff::civil::Date::new(year, month, day).map_err(|_| out_of_range())
}

/// Build a civil `DateTime` from a `ParsedDateTime`, on top of
/// [`date_from_parsed`].
fn civil_from_parsed(
    p: &datetime::ParsedDateTime,
    input: &str,
) -> Result<jiff::civil::DateTime, ExecError> {
    let out_of_range = || {
        ExecError::Type(TypeError::DatetimeFieldOverflow {
            value: input.to_string(),
        })
    };
    let date = date_from_parsed(p, input)?;
    let nanos = i32::try_from(p.micros).map_err(|_| out_of_range())? * 1_000;
    let time = jiff::civil::Time::new(
        i8::try_from(p.hour).map_err(|_| out_of_range())?,
        i8::try_from(p.minute).map_err(|_| out_of_range())?,
        i8::try_from(p.second).map_err(|_| out_of_range())?,
        nanos,
    )
    .map_err(|_| out_of_range())?;
    Ok(date.to_datetime(time))
}

// ---- argument helpers ----

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

/// The positional argument list. SP38 functions never accept `f(*)`.
fn exprs_of(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    match &fc.args {
        FuncArgs::Exprs(v) => Ok(v),
        FuncArgs::Star | FuncArgs::Named { .. } | FuncArgs::Variadic { .. } => {
            Err(undefined_function(&fc.name))
        }
    }
}

fn require_arity(fc: &FuncCall, ok: bool) -> Result<(), ExecError> {
    if ok {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

/// Validate just the arity for `f`. The NULL short-circuit path uses this, so a
/// NULL with the wrong number of arguments still reports 42883.
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

/// Map a `crabka_pgtypes::TypeError`, such as 22007, 22008 or 22003, onto the
/// executor error, so its SQLSTATE propagates to the wire.
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

/// An integer argument at runtime, narrowed to i32, the `make_*` field width.
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

/// A floating argument at runtime, promoted to f64, from int, float or numeric.
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

/// An interval argument at runtime, the `justify_*` domain.
fn interval_value(d: &Datum, name: &str) -> Result<Interval, ExecError> {
    match d {
        Datum::Interval(iv) => Ok(*iv),
        _ => Err(type_error(name, d)),
    }
}

/// Resolve `make_timestamptz`'s zone argument, which `PostgreSQL` reads by
/// rules of its own (`parse_sane_timezone`).
///
/// A numeric offset is tried first and keeps the ISO sign, so `'+2'` is two
/// hours *east* — the opposite of what the same text means to `AT TIME ZONE`.
/// To stop the `POSIX` grammar from quietly accepting a spelling the numeric
/// grammar has already rejected, a leading digit is refused outright rather
/// than falling through. Everything else resolves the way `AT TIME ZONE` does.
fn zone_arg(d: &Datum, name: &str) -> Result<jiff::tz::TimeZone, ExecError> {
    let zone = match d {
        Datum::Text(s) => s.as_str(),
        _ => return Err(type_error(name, d)),
    };
    if zone.eq_ignore_ascii_case("utc") {
        return Ok(jiff::tz::TimeZone::UTC);
    }
    if zone.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(ExecError::NumericTimeZoneSyntax(zone.to_string()));
    }
    match crabka_pgtypes::datetime::decode_numeric_time_zone(zone) {
        Ok(offset) => return Ok(jiff::tz::TimeZone::fixed(offset)),
        Err(crabka_pgtypes::datetime::DecodeError::TzDisplacement) => {
            return Err(ExecError::NumericTimeZoneOutOfRange(zone.to_string()));
        }
        Err(_) => {}
    }
    crabka_pgtypes::datetime::resolve_time_zone(zone)
        .ok_or_else(|| ExecError::UnknownTimeZone(zone.to_string()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
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

    fn pg_error(sql: &str) -> crabka_pgwire::error::PgError {
        let ctx = EvalCtx::test_default();
        crate::eval::eval(
            &crabka_pgparser::parser::parse_expr_for_test(sql).expect("parse"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect_err("error")
        .into_pg()
    }

    /// An `FF`n pattern does not truncate what it reads — every digit is parsed
    /// — it asks for the finished instant to be rounded to n fractional digits,
    /// half away from zero. Expectations are PostgreSQL 18.4's.
    #[test]
    fn to_timestamp_rounds_to_the_fractional_precision_the_template_asked_for() {
        let micros = |sql: &str| match ev(sql) {
            Datum::Timestamptz(ts) => ts.as_microsecond(),
            other => panic!("expected timestamptz, got {other:?}"),
        };
        for (precision, expected) in [
            (1, 1_541_162_096_100_000_i64),
            (2, 1_541_162_096_120_000),
            (3, 1_541_162_096_123_000),
            (4, 1_541_162_096_123_500),
            (5, 1_541_162_096_123_460),
            (6, 1_541_162_096_123_456),
        ] {
            let sql = format!(
                "to_timestamp('2018-11-02 12:34:56.123456', 'YYYY-MM-DD HH24:MI:SS.FF{precision}')"
            );
            assert!(micros(&sql) == expected, "FF{precision}");
        }
        // A template that names a zone fixes the offset itself instead of
        // reading the wall clock as local to the session.
        assert!(
            micros("to_timestamp('2011-12-18 11:38 -05', 'YYYY-MM-DD HH12:MI TZH')")
                == 1_324_226_280_000_000
        );
    }

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
    fn to_char_returns_null_for_infinite_temporal_values() {
        let ctx = EvalCtx::test_default();
        for value in [
            Datum::Date(crabka_pgtypes::datetime::DATE_INFINITY),
            Datum::Timestamp(crabka_pgtypes::datetime::TIMESTAMP_INFINITY),
            Datum::Timestamptz(jiff::Timestamp::MAX),
            Datum::Interval(crabka_pgtypes::datetime::Interval::INFINITY),
        ] {
            assert_eq!(
                super::to_char(&value, "YYYY", &ctx, "to_char").expect("to_char"),
                Datum::Null
            );
        }
        assert_eq!(
            super::to_char(
                &Datum::Timestamp(jiff::civil::datetime(2024, 1, 1, 0, 0, 0, 0)),
                "YYYY",
                &ctx,
                "to_char"
            )
            .expect("to_char"),
            Datum::Text("2024".into())
        );
    }

    #[test]
    fn to_char_uses_the_session_zone_abbreviation() {
        let instant = |text: &str| text.parse::<jiff::Timestamp>().expect("instant");
        let mut ctx = EvalCtx::test_default();
        for (zone, at, expected) in [
            ("America/Los_Angeles", "2012-12-12T20:00:00Z", "PST pst"),
            ("America/Los_Angeles", "1800-01-01T00:00:00Z", "LMT lmt"),
            ("America/Montevideo", "1912-01-01T03:30:00Z", "MMT mmt"),
            ("Europe/Moscow", "2012-01-01T00:00:00Z", "MSK msk"),
            ("-1.5", "2012-12-12T00:00:00Z", "-01:30 -01:30"),
            ("+2", "2012-12-12T00:00:00Z", "+02 +02"),
        ] {
            ctx.time_zone = crabka_pgtypes::datetime::resolve_guc_time_zone(zone)
                .unwrap_or_else(|| panic!("{zone} resolves"));
            assert_eq!(
                super::to_char(&Datum::Timestamptz(instant(at)), "TZ tz", &ctx, "to_char")
                    .expect("to_char"),
                Datum::Text(expected.into()),
                "{zone} at {at}"
            );
        }
    }

    #[test]
    fn to_timestamp_to_date_make_justify() {
        assert_eq!(
            ev("to_date('2024-07-04', 'YYYY-MM-DD')"),
            Datum::Date(jiff::civil::date(2024, 7, 4).into())
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
            Datum::Date(jiff::civil::date(2024, 7, 4).into())
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

    #[test]
    fn template_errors_keep_postgres_detail_and_hint() {
        let short = pg_error("to_timestamp('19971', 'YYYYMMDD')");
        assert!(short.code == "22007");
        assert!(short.message == "source string too short for \"MM\" formatting field");
        let diagnostics = short.diagnostics.expect("diagnostics");
        assert!(
            diagnostics.detail.as_deref()
                == Some("Field requires 2 characters, but only 1 remain.")
        );
        assert!(
            diagnostics.hint.as_deref()
                == Some("If your source string is not fixed-width, try using the \"FM\" modifier.")
        );

        let clobbered = pg_error("to_timestamp('1997-11-Jan-16', 'YYYY-MM-Mon-DD')");
        assert!(
            clobbered.diagnostics.and_then(|d| d.detail)
                == Some("This value contradicts a previous setting for the same field type.".into())
        );
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

    /// `make_timestamptz` reads its zone by `PostgreSQL`'s
    /// `parse_sane_timezone` rules, which are neither the setting's nor
    /// `AT TIME ZONE`'s. A numeric offset keeps the ISO sign — `'+2'` is two
    /// hours *east*, where `AT TIME ZONE '+2'` would be two hours west — and a
    /// specification the numeric grammar cannot read falls through to the
    /// `POSIX` one. Every expectation is `PostgreSQL` 18.4's.
    #[test]
    fn make_timestamptz_reads_a_numeric_zone_with_the_iso_sign() {
        for (expr, expected) in [
            // Numeric offsets: east.
            (
                "make_timestamptz(1973, 7, 15, 8, 15, 55, '+2')",
                "1973-07-15T06:15:55Z",
            ),
            (
                "make_timestamptz(1973, 7, 15, 8, 15, 55, '-2')",
                "1973-07-15T10:15:55Z",
            ),
            (
                "make_timestamptz(2014, 12, 10, 10, 10, 10, '-08:00')",
                "2014-12-10T18:10:10Z",
            ),
            (
                "make_timestamptz(2014, 12, 10, 10, 10, 10, '-08')",
                "2014-12-10T18:10:10Z",
            ),
            // POSIX specifications: west, and daylight-saving aware.
            (
                "make_timestamptz(2014, 12, 10, 10, 10, 10, 'UTC-2')",
                "2014-12-10T08:10:10Z",
            ),
            (
                "make_timestamptz(2014, 12, 10, 10, 10, 10, 'PST8PDT,M3.2.0,M11.1.0')",
                "2014-12-10T18:10:10Z",
            ),
            (
                "make_timestamptz(2014, 12, 10, 10, 10, 10, 'FOO8BAR')",
                "2014-12-10T18:10:10Z",
            ),
            // An abbreviation.
            (
                "make_timestamptz(2008, 12, 10, 10, 10, 10, 'EST')",
                "2008-12-10T15:10:10Z",
            ),
        ] {
            let want = Datum::Timestamptz(expected.parse().expect("expected instant"));
            assert!(ev(expr) == want, "{expr}");
        }
    }

    /// The three ways `make_timestamptz` rejects a zone, each with
    /// `PostgreSQL`'s own wording. A leading digit is refused outright so the
    /// `POSIX` grammar cannot accept a spelling the numeric grammar rejected.
    #[test]
    fn make_timestamptz_rejects_a_bad_zone_the_way_postgresql_words_it() {
        for (expr, message, hint) in [
            (
                "make_timestamptz(1973, 7, 15, 8, 15, 55, '2')",
                "invalid input syntax for type numeric time zone: \"2\"",
                Some("Numeric time zones must have \"-\" or \"+\" as first character."),
            ),
            (
                "make_timestamptz(2014, 12, 10, 10, 10, 10, '+16')",
                "numeric time zone \"+16\" out of range",
                None,
            ),
            (
                "make_timestamptz(2014, 12, 10, 10, 10, 10, '-16')",
                "numeric time zone \"-16\" out of range",
                None,
            ),
            (
                "make_timestamptz(1910, 12, 24, 0, 0, 0, 'Nehwon/Lankhmar')",
                "time zone \"Nehwon/Lankhmar\" not recognized",
                None,
            ),
        ] {
            let error = crate::eval::eval(
                &crabka_pgparser::parser::parse_expr_for_test(expr).expect("parse"),
                &Scope::empty(),
                &[],
                &EvalCtx::test_default(),
            )
            .expect_err("zone should be rejected")
            .into_pg();
            assert!(error.code == "22023", "{expr}");
            assert!(error.message == message, "{expr}: {}", error.message);
            let got_hint = error.diagnostics.as_ref().and_then(|f| f.hint.clone());
            assert!(got_hint.as_deref() == hint, "{expr}: {got_hint:?}");
        }
    }

    #[test]
    fn make_time_make_timestamp_justify_interval() {
        assert_eq!(
            ev("make_time(8, 15, 30)"),
            Datum::Time(jiff::civil::time(8, 15, 30, 0).into())
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
