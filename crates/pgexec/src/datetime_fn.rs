//! SP37: date/time *functions*. The clock family, `extract`/`date_part`,
//! `date_trunc`, `age`, and the `timezone` function (the `AT TIME ZONE` target).
//!
//! This mirrors `func.rs` (SP29 scalar functions): a `datetime_func(name)`
//! registry, an `is_datetime_func` dispatch predicate, an `eval_datetime` value
//! evaluator, and a `datetime_func_result_type` static result-type resolver.
//! Like every breadth slice since SP27, each function is a pure, deterministic
//! transform over a single row's already-evaluated Datums, and the clock family
//! reads the per-statement `EvalCtx`. So there is no new lock, visibility rule,
//! write path, or interleaving, and no Stateright model.
//! The unit tests below, the Task-14 wire test, and the Task-15 conformance
//! corpus diffed against PostgreSQL prove it instead.
//!
//! The field math (extract/date_part/date_trunc) happens here in jiff. Only
//! value-pure, reusable computations live in `crabka_pgtypes::datetime`.

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{ColumnType, Datum, datetime::Interval};
use jiff::{
    Unit,
    civil::{Date, DateTime, Time},
    tz::TimeZone,
};

use crate::{clock::EvalCtx, error::ExecError, scope::Scope};

/// The date/time functions SP37 supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DtFunc {
    /// `now()` / `current_timestamp` / `transaction_timestamp()`: the transaction
    /// instant, as `timestamptz` (transaction-stable, like PostgreSQL).
    TransactionTimestamp,
    /// `statement_timestamp()`: the statement instant, as `timestamptz`.
    StatementTimestamp,
    /// `clock_timestamp()`: the real-time clock, as `timestamptz`.
    ClockTimestamp,
    /// `current_date`: the transaction date in the session zone.
    CurrentDate,
    /// `current_time` / `localtime`: the transaction time-of-day in the session
    /// zone.
    CurrentTime,
    /// `localtimestamp`: the transaction wall-clock as a (civil) `timestamp`.
    LocalTimestamp,
    /// `extract(field, source)` → `numeric` (PG 14+).
    Extract,
    /// `date_part(text, source)` → `float8` (the historical double-precision form).
    DatePart,
    /// `date_trunc(field, source [, tz])`: truncate to a unit.
    DateTrunc,
    /// `age(ts1, ts2)` / `age(ts1)`: a symbolic interval with month borrowing.
    Age,
    /// `timezone(zone, value)`: the `AT TIME ZONE` target.
    Timezone,
    /// `isfinite(value)`: false for the non-finite date/timestamp/interval
    /// values.
    IsFinite,
    /// `date_bin(stride, source, origin)`: snap `source` to a stride grid.
    DateBin,
}

/// Classify a lowercased function name. The lexer lowercases unquoted idents.
/// `None` means "not a date/time function". The caller then reports a misplaced
/// aggregate or an undefined function.
fn datetime_func(name: &str) -> Option<DtFunc> {
    Some(match name {
        "now" | "current_timestamp" | "transaction_timestamp" => DtFunc::TransactionTimestamp,
        "statement_timestamp" => DtFunc::StatementTimestamp,
        "clock_timestamp" => DtFunc::ClockTimestamp,
        "current_date" => DtFunc::CurrentDate,
        "current_time" | "localtime" => DtFunc::CurrentTime,
        "localtimestamp" => DtFunc::LocalTimestamp,
        "extract" => DtFunc::Extract,
        "date_part" => DtFunc::DatePart,
        "date_trunc" => DtFunc::DateTrunc,
        "age" => DtFunc::Age,
        "timezone" => DtFunc::Timezone,
        "isfinite" => DtFunc::IsFinite,
        "date_bin" => DtFunc::DateBin,
        _ => return None,
    })
}

/// Is `name` a known date/time function? This is the dispatch point in
/// `eval`/`infer_type`.
pub(crate) fn is_datetime_func(name: &str) -> bool {
    datetime_func(name).is_some()
}

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

/// The positional argument list. The clock family is niladic (`Exprs([])`), and
/// `f(*)` is never valid for a date/time function.
fn exprs_of(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    match &fc.args {
        FuncArgs::Exprs(v) => Ok(v),
        FuncArgs::Star => Err(undefined_function(&fc.name)),
    }
}

/// 22023 (`InvalidParameterValue`) for an unknown extract/date_part field or an
/// unknown `date_trunc`/`timezone` unit/zone. That is PostgreSQL's SQLSTATE for
/// these.
fn invalid_param(msg: impl Into<String>) -> ExecError {
    ExecError::InvalidParameterValue(msg.into())
}

/// A runtime type error for a function applied to an unsupported argument type.
fn type_error(what: &str, got: &Datum) -> ExecError {
    ExecError::TypeMismatch(format!(
        "{what} does not accept an argument of type {}",
        got.column_type().map(|t| t.name()).unwrap_or("unknown")
    ))
}

/// Statically infer a date/time call's result type, for RowDescription.
pub(crate) fn datetime_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let f = datetime_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    let n = args.len();
    Ok(match f {
        DtFunc::TransactionTimestamp | DtFunc::StatementTimestamp | DtFunc::ClockTimestamp => {
            require_arity(fc, n == 0)?;
            ColumnType::Timestamptz
        }
        DtFunc::CurrentDate => {
            require_arity(fc, n == 0)?;
            ColumnType::Date
        }
        DtFunc::CurrentTime => {
            require_arity(fc, n == 0)?;
            ColumnType::Time
        }
        DtFunc::LocalTimestamp => {
            require_arity(fc, n == 0)?;
            ColumnType::Timestamp
        }
        // extract → numeric; date_part → float8 (the PG quirk — replicate BOTH).
        DtFunc::Extract => {
            require_arity(fc, n == 2)?;
            ColumnType::Numeric(None)
        }
        DtFunc::DatePart => {
            require_arity(fc, n == 2)?;
            ColumnType::Float8
        }
        // date_trunc's result type matches the (promoted) source type. A date source
        // promotes to timestamp; everything else preserves its type.
        DtFunc::DateTrunc => {
            require_arity(fc, n == 2 || n == 3)?;
            match crate::eval::infer_type(&args[1], scope)? {
                // A `date` argument resolves through `timestamptz`, so the
                // result carries a zone (PostgreSQL has no date overload).
                ColumnType::Date => ColumnType::Timestamptz,
                t @ (ColumnType::Timestamp | ColumnType::Timestamptz | ColumnType::Interval) => t,
                // A non-temporal / unknown source surfaces the real error at eval.
                other => other,
            }
        }
        DtFunc::Age => {
            require_arity(fc, n == 1 || n == 2)?;
            ColumnType::Interval
        }
        // timezone(zone, value) and the one-argument `AT LOCAL` overload alike:
        // timestamp → timestamptz, timestamptz → timestamp, timetz → timetz.
        DtFunc::Timezone => {
            require_arity(fc, n == 1 || n == 2)?;
            match crate::eval::infer_type(&args[n - 1], scope)? {
                ColumnType::Timestamp => ColumnType::Timestamptz,
                ColumnType::Timestamptz => ColumnType::Timestamp,
                other => other,
            }
        }
        DtFunc::IsFinite => {
            require_arity(fc, n == 1)?;
            ColumnType::Bool
        }
        // date_bin keeps the source's type, as its stride grid is in that type.
        DtFunc::DateBin => {
            require_arity(fc, n == 3)?;
            crate::eval::infer_type(&args[1], scope)?
        }
    })
}

/// Evaluate a date/time call. `eval_child` evaluates each argument against the
/// current row. It is the same `eval` for a scalar context, or
/// `agg::eval_grouped` for a grouped context, so the two share the math and only
/// the closure differs.
pub(crate) fn eval_datetime(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = datetime_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = exprs_of(fc)?;
    match f {
        // ---- clock family (niladic; read the per-statement EvalCtx) ----
        DtFunc::TransactionTimestamp => {
            require_arity(fc, args.is_empty())?;
            Ok(Datum::Timestamptz(ctx.now))
        }
        DtFunc::StatementTimestamp => {
            require_arity(fc, args.is_empty())?;
            Ok(Datum::Timestamptz(ctx.stmt_now))
        }
        DtFunc::ClockTimestamp => {
            require_arity(fc, args.is_empty())?;
            Ok(Datum::Timestamptz(ctx.clock.now()))
        }
        DtFunc::CurrentDate => {
            require_arity(fc, args.is_empty())?;
            Ok(Datum::Date(ctx.time_zone.to_datetime(ctx.now).date()))
        }
        DtFunc::CurrentTime => {
            require_arity(fc, args.is_empty())?;
            Ok(Datum::Time(
                ctx.time_zone.to_datetime(ctx.now).time().into(),
            ))
        }
        DtFunc::LocalTimestamp => {
            require_arity(fc, args.is_empty())?;
            Ok(Datum::Timestamp(ctx.time_zone.to_datetime(ctx.now)))
        }
        // ---- extract / date_part ----
        DtFunc::Extract => {
            require_arity(fc, args.len() == 2)?;
            let field = literal_field(&args[0])?;
            let source = eval_child(&args[1])?;
            if source.is_null() {
                return Ok(Datum::Null);
            }
            let Some(v) = extract_field(&field, &source, &ctx.time_zone)? else {
                return Ok(Datum::Null);
            };
            // numeric result (PG 14+). The value is exact for every field; build it
            // from text so fractional seconds/epoch keep full precision.
            Ok(Datum::Numeric(
                crabka_pgtypes::numeric::parse(&v).ok_or_else(|| {
                    ExecError::Type(crabka_pgtypes::TypeError::InvalidText {
                        type_name: "numeric",
                        value: v.clone(),
                    })
                })?,
            ))
        }
        DtFunc::DatePart => {
            require_arity(fc, args.len() == 2)?;
            // date_part's first argument is a runtime text value (not a literal).
            let field_d = eval_child(&args[0])?;
            let source = eval_child(&args[1])?;
            if field_d.is_null() || source.is_null() {
                return Ok(Datum::Null);
            }
            let field = match &field_d {
                Datum::Text(s) => s.to_ascii_lowercase(),
                other => return Err(type_error("date_part", other)),
            };
            let Some(v) = extract_field(&field, &source, &ctx.time_zone)? else {
                return Ok(Datum::Null);
            };
            // float8 result (the historical double-precision form).
            Ok(Datum::Float8(v.parse::<f64>().map_err(|_| {
                ExecError::Type(crabka_pgtypes::TypeError::InvalidText {
                    type_name: "double precision",
                    value: v.clone(),
                })
            })?))
        }
        // ---- date_trunc ----
        DtFunc::DateTrunc => {
            require_arity(fc, args.len() == 2 || args.len() == 3)?;
            let unit_d = eval_child(&args[0])?;
            let source = eval_child(&args[1])?;
            // The 3-arg form's zone is taken from arg 3; the 2-arg form uses the
            // session zone for the `timestamptz` case.
            let zone = match args.get(2) {
                Some(a) => Some(zone_arg(&eval_child(a)?)?),
                None => None,
            };
            if unit_d.is_null() || source.is_null() {
                return Ok(Datum::Null);
            }
            let unit = match &unit_d {
                Datum::Text(s) => s.to_ascii_lowercase(),
                other => return Err(type_error("date_trunc", other)),
            };
            let type_name =
                date_trunc_type_name(&source).ok_or_else(|| type_error("date_trunc", &source))?;
            let unit = canonical_unit(&unit).ok_or_else(|| unknown_field(&unit, type_name))?;
            date_trunc(unit, &source, zone.as_ref(), &ctx.time_zone)
        }
        // ---- age ----
        DtFunc::Age => {
            require_arity(fc, args.len() == 1 || args.len() == 2)?;
            let a = eval_child(&args[0])?;
            if a.is_null() {
                return Ok(Datum::Null);
            }
            // Two-arg: `age(end, start)` = end − start. One-arg: `age(ts)` =
            // current_date(at midnight, session zone) − ts (PG semantics), and
            // the transaction date is always finite, so only `a` can be
            // infinite there.
            let b = match args.get(1) {
                Some(e) => {
                    let b = eval_child(e)?;
                    if b.is_null() {
                        return Ok(Datum::Null);
                    }
                    Some(b)
                }
                None => None,
            };
            // Settle the non-finite cases on the Datums, BEFORE `as_datetime`.
            // A `timestamptz` infinity is the extreme *instant*, and rendering
            // it as a wall-clock in a session zone off UTC moves it off the
            // reserved value — the sentinel would be gone by the time the field
            // arithmetic saw it.
            let (end_sign, start_sign) = match &b {
                Some(b) => (temporal_infinite_sign(&a), temporal_infinite_sign(b)),
                None => (0, temporal_infinite_sign(&a)),
            };
            if let Some(result) =
                crabka_pgtypes::datetime::infinite_interval_difference(end_sign, start_sign)
            {
                return Ok(Datum::Interval(result?));
            }
            let (end, start) = match &b {
                Some(b) => (
                    as_datetime(&a, &ctx.time_zone)?,
                    as_datetime(b, &ctx.time_zone)?,
                ),
                None => {
                    let today = ctx.time_zone.to_datetime(ctx.now).date();
                    (
                        crabka_pgtypes::datetime::date_to_midnight(today),
                        as_datetime(&a, &ctx.time_zone)?,
                    )
                }
            };
            Ok(Datum::Interval(age(end, start)))
        }
        // ---- timezone (AT TIME ZONE) ----
        DtFunc::Timezone => {
            require_arity(fc, (1..=2).contains(&args.len()))?;
            // `x AT LOCAL` is the one-argument overload — `timetz_at_local` and
            // its two `timestamp` siblings — which converts against the
            // *session's* zone. It is a distinct `pg_proc` entry rather than
            // sugar for `AT TIME ZONE current_setting('TimeZone')`, which is
            // why the session zone is read here instead of being planted as an
            // argument at parse time.
            let [zone, value] = match args {
                [value] => [None, Some(eval_child(value)?)],
                // Zone FIRST: that is the argument order `AT TIME ZONE` lowers
                // to, and `pg_proc` declares.
                [zone, value] => [Some(eval_child(zone)?), Some(eval_child(value)?)],
                _ => unreachable!("arity checked above"),
            };
            let Some(value) = value else {
                unreachable!("arity checked above")
            };
            if zone.as_ref().is_some_and(Datum::is_null) || value.is_null() {
                return Ok(Datum::Null);
            }
            let tz = match &zone {
                Some(zone) => zone_arg(zone)?,
                None => ctx.time_zone.clone(),
            };
            timezone_convert(&tz, &value)
        }
        DtFunc::IsFinite => {
            require_arity(fc, args.len() == 1)?;
            let value = eval_child(&args[0])?;
            if value.is_null() {
                return Ok(Datum::Null);
            }
            Ok(Datum::Bool(match &value {
                Datum::Date(d) => !crabka_pgtypes::datetime::date_is_infinite(*d),
                Datum::Timestamp(ts) => !crabka_pgtypes::datetime::timestamp_is_infinite(*ts),
                Datum::Timestamptz(ts) => !crabka_pgtypes::datetime::timestamptz_is_infinite(*ts),
                Datum::Interval(iv) => !iv.is_infinite(),
                other => return Err(type_error("isfinite", other)),
            }))
        }
        DtFunc::DateBin => {
            require_arity(fc, args.len() == 3)?;
            let stride = eval_child(&args[0])?;
            let source = eval_child(&args[1])?;
            let origin = eval_child(&args[2])?;
            if stride.is_null() || source.is_null() || origin.is_null() {
                return Ok(Datum::Null);
            }
            date_bin(&stride, &source, &origin, &ctx.time_zone)
        }
    }
}

/// `date_bin(stride, source, origin)`: snap `source` down to the start of the
/// `stride`-wide bin whose grid is anchored at `origin`.
///
/// The stride must be a pure day-and-time interval. Months are not a fixed
/// number of microseconds, so PostgreSQL refuses them and does not guess.
fn date_bin(
    stride: &Datum,
    source: &Datum,
    origin: &Datum,
    tz: &TimeZone,
) -> Result<Datum, ExecError> {
    let Datum::Interval(stride) = stride else {
        return Err(type_error("date_bin", stride));
    };
    let out_of_range = |message: &str| {
        ExecError::Type(crabka_pgtypes::TypeError::DatetimeOutOfRange {
            message: message.to_string(),
        })
    };
    if stride.is_infinite() {
        return Err(out_of_range(
            "timestamps cannot be binned into infinite intervals",
        ));
    }
    if stride.months != 0 {
        return Err(ExecError::Unsupported(
            "timestamps cannot be binned into intervals containing months or years".to_string(),
        ));
    }
    let Some(width) = i64::from(stride.days)
        .checked_mul(86_400_000_000)
        .and_then(|days| days.checked_add(stride.micros))
    else {
        return Err(out_of_range("stride is too large"));
    };
    if width <= 0 {
        return Err(out_of_range("stride must be greater than zero"));
    }
    // Both endpoints are compared as microsecond counts on the same axis, so the
    // arithmetic is identical for `timestamp` and `timestamptz`.
    let micros_of = |d: &Datum| -> Result<i64, ExecError> {
        match d {
            Datum::Timestamp(ts) => Ok(i64::from_be_bytes(
                crabka_pgtypes::datetime::timestamp_to_binary(*ts),
            )),
            Datum::Timestamptz(ts) => Ok(i64::from_be_bytes(
                crabka_pgtypes::datetime::timestamptz_to_binary(*ts),
            )),
            other => Err(type_error("date_bin", other)),
        }
    };
    let infinite = match source {
        Datum::Timestamp(ts) => crabka_pgtypes::datetime::timestamp_is_infinite(*ts),
        Datum::Timestamptz(ts) => crabka_pgtypes::datetime::timestamptz_is_infinite(*ts),
        other => return Err(type_error("date_bin", other)),
    };
    if infinite {
        return Ok(source.clone());
    }
    let source_micros = micros_of(source)?;
    let origin_micros = micros_of(origin)?;
    let delta = i128::from(source_micros) - i128::from(origin_micros);
    let binned =
        i128::from(origin_micros) + delta.div_euclid(i128::from(width)) * i128::from(width);
    let binned = i64::try_from(binned)
        .map_err(|_| out_of_range("interval out of range"))?
        .to_be_bytes();
    let _ = tz;
    match source {
        Datum::Timestamp(_) => crabka_pgtypes::datetime::timestamp_from_binary(&binned)
            .map(Datum::Timestamp)
            .map_err(ExecError::from),
        _ => crabka_pgtypes::datetime::timestamptz_from_binary(&binned)
            .map(Datum::Timestamptz)
            .map_err(ExecError::from),
    }
}

// ---- argument helpers ----

fn require_arity(fc: &FuncCall, ok: bool) -> Result<(), ExecError> {
    if ok {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

/// The `extract` field, which the parser lowered to a `StringLiteral` that is
/// already lowercased. Anything else is a misuse, and gives an undefined
/// function.
fn literal_field(e: &Expr) -> Result<String, ExecError> {
    match e {
        Expr::StringLiteral(s) => Ok(s.to_ascii_lowercase()),
        _ => Err(ExecError::UndefinedFunction(
            "extract requires a field name".into(),
        )),
    }
}

/// Resolve a zone-specification text value to a jiff `TimeZone`, the way
/// `AT TIME ZONE` and `date_trunc`'s third argument do: the default
/// abbreviation set, then the bundled zone database, then a `POSIX` `TZ`
/// specification whose offset counts *west* of Greenwich. An unresolvable
/// specification is 22023.
fn zone_arg(d: &Datum) -> Result<TimeZone, ExecError> {
    let name = match d {
        Datum::Text(s) => s.as_str(),
        // `AT TIME ZONE INTERVAL '-08:00'` names a fixed offset rather than a
        // zone — `timestamp_izone` and its `timestamptz`/`timetz` siblings.
        // Months and days have no fixed length in seconds, so an interval that
        // carries either names no offset at all and PostgreSQL refuses it
        // rather than picking one.
        Datum::Interval(interval) => return interval_zone(*interval),
        other => return Err(type_error("timezone", other)),
    };
    if name.eq_ignore_ascii_case("utc") {
        return Ok(TimeZone::UTC);
    }
    crabka_pgtypes::datetime::resolve_time_zone(name)
        .ok_or_else(|| ExecError::UnknownTimeZone(name.to_string()))
}

/// The fixed zone an `interval` names as a `timezone()` argument.
///
/// `PostgreSQL` reads the interval's microseconds as a signed offset *east*
/// (`timestamp_izone` shifts by `zone->time`, and `timetz_izone` stores its
/// negation because a `TimeTzADT` counts west), so one conversion serves all
/// three value types. Months and days are refused rather than approximated:
/// neither is a fixed number of seconds, so an interval carrying one names no
/// offset at all.
fn interval_zone(interval: crabka_pgtypes::datetime::Interval) -> Result<TimeZone, ExecError> {
    let spelled = || crate::func::text_render(&Datum::Interval(interval), &TimeZone::UTC);
    // 22023 with PostgreSQL's own sentence, not the `SET`-shaped one
    // [`invalid_param`] wraps a parameter name in.
    let refuse = |reason: &str| {
        ExecError::InvalidParameterValueMessage(format!(
            "interval time zone \"{}\" {reason}",
            spelled()
        ))
    };
    if interval.is_infinite() {
        return Err(refuse("must be finite"));
    }
    if interval.months != 0 || interval.days != 0 {
        return Err(refuse("must not include months or days"));
    }
    let seconds = i32::try_from(interval.micros / 1_000_000)
        .ok()
        .and_then(|seconds| jiff::tz::Offset::from_seconds(seconds).ok())
        .ok_or_else(|| invalid_param("time zone displacement out of range"))?;
    Ok(TimeZone::fixed(seconds))
}

/// `1` for a `+infinity` temporal Datum, `-1` for `-infinity`, `0` for a finite
/// one. A Datum of a type that has no infinities — or none at all — is `0`, and
/// the caller's own coercion then reports the type error.
///
/// This has to be read off the Datum in its own type. [`as_datetime`] renders a
/// `timestamptz` as a wall-clock, which for the reserved extreme instant lands
/// on an ordinary civil datetime in any session zone off UTC, so a check made
/// after the coercion would miss half the infinities.
fn temporal_infinite_sign(d: &Datum) -> i32 {
    match d {
        Datum::Date(v) => crabka_pgtypes::datetime::date_infinite_sign(*v),
        Datum::Timestamp(v) => crabka_pgtypes::datetime::timestamp_infinite_sign(*v),
        Datum::Timestamptz(v) => crabka_pgtypes::datetime::timestamptz_infinite_sign(*v),
        _ => 0,
    }
}

/// Coerce a temporal Datum to a civil `DateTime` for `age` arithmetic. A
/// `timestamptz` renders in the session zone, and a `date` promotes to
/// midnight.
fn as_datetime(d: &Datum, tz: &TimeZone) -> Result<DateTime, ExecError> {
    match d {
        Datum::Timestamp(dt) => Ok(*dt),
        Datum::Date(dd) => Ok(crabka_pgtypes::datetime::date_to_midnight(*dd)),
        Datum::Timestamptz(ts) => Ok(tz.to_datetime(*ts)),
        other => Err(type_error("age", other)),
    }
}

// ---- extract / date_part field math ----

/// The Unix epoch as a civil datetime, for `epoch` of timestamp/date ("as if
/// UTC").
fn unix_epoch_civil() -> DateTime {
    DateTime::constant(1970, 1, 1, 0, 0, 0, 0)
}

/// Canonicalize an extract/date_part unit, and accept every spelling PostgreSQL
/// accepts. `None` means the word is not a unit at all, which is 22023. A unit
/// the source type has no value for is a different error (0A000).
fn canonical_unit(field: &str) -> Option<&'static str> {
    Some(match field {
        "microsecond" | "microseconds" | "usecond" | "useconds" | "usec" | "usecs" | "us" => {
            "microseconds"
        }
        "millisecond" | "milliseconds" | "msecond" | "mseconds" | "msec" | "msecs" | "ms" => {
            "milliseconds"
        }
        "second" | "seconds" | "sec" | "secs" | "s" => "second",
        "minute" | "minutes" | "min" | "mins" | "m" => "minute",
        "hour" | "hours" | "hr" | "hrs" | "h" => "hour",
        "day" | "days" | "d" => "day",
        "week" | "weeks" | "w" => "week",
        "month" | "months" | "mon" | "mons" => "month",
        "quarter" | "quarters" | "qtr" => "quarter",
        "year" | "years" | "yr" | "yrs" | "y" => "year",
        "decade" | "decades" | "dec" | "decs" => "decade",
        "century" | "centuries" | "cent" | "c" => "century",
        "millennium" | "millennia" | "mil" | "mils" => "millennium",
        "isoyear" => "isoyear",
        "isodow" => "isodow",
        "dow" => "dow",
        "doy" => "doy",
        "epoch" => "epoch",
        "julian" | "jd" => "julian",
        "timezone" | "tz" => "timezone",
        "timezone_hour" | "timezone_h" => "timezone_hour",
        "timezone_minute" | "timezone_m" => "timezone_minute",
        _ => return None,
    })
}

/// 0A000: a real unit the source type has no value for (`day` of a `time`).
fn unsupported_field(field: &str, type_name: &str) -> ExecError {
    ExecError::Unsupported(format!(
        "unit \"{field}\" not supported for type {type_name}"
    ))
}

/// Compute one extract/date_part field of `source` and return it as a decimal
/// STRING. The numeric form then keeps full precision, and the float form only
/// parses it. `tz` is the session zone, and only the timezone-* fields of a
/// timestamptz use it.
fn extract_field(field: &str, source: &Datum, tz: &TimeZone) -> Result<Option<String>, ExecError> {
    let type_name = extract_type_name(source).ok_or_else(|| type_error("extract", source))?;
    let unit = canonical_unit(field).ok_or_else(|| unknown_field(field, type_name))?;
    match source {
        Datum::Date(d) => {
            // A `date` has no clock or zone, so PostgreSQL refuses those units,
            // and returns its whole-second epoch as an integer rather than at the
            // scale-6 the timestamp forms carry.
            if matches!(
                unit,
                "second"
                    | "minute"
                    | "hour"
                    | "milliseconds"
                    | "microseconds"
                    | "timezone"
                    | "timezone_hour"
                    | "timezone_minute"
            ) {
                return Err(unsupported_field(field, "date"));
            }
            match non_finite_field(unit, crabka_pgtypes::datetime::date_infinite_sign(*d)) {
                NonFinite::Value(text) => return Ok(Some(text)),
                NonFinite::Null => return Ok(None),
                NonFinite::Finite => {}
            }
            let dt = crabka_pgtypes::datetime::date_to_midnight(*d);
            if unit == "epoch" {
                let micros = dt
                    .since((Unit::Microsecond, unix_epoch_civil()))
                    .map(|s| s.get_microseconds())
                    .map_err(|_| invalid_param("date out of range for epoch"))?;
                return Ok(Some((micros / 1_000_000).to_string()));
            }
            if unit == "julian" {
                return Ok(Some(julian_day(dt.date()).to_string()));
            }
            extract_from_datetime(unit, dt, None, "date").map(Some)
        }
        Datum::Timestamp(dt) => {
            match non_finite_field(unit, crabka_pgtypes::datetime::timestamp_infinite_sign(*dt)) {
                NonFinite::Value(text) => return Ok(Some(text)),
                NonFinite::Null => return Ok(None),
                NonFinite::Finite => {}
            }
            extract_from_datetime(unit, *dt, None, "timestamp without time zone").map(Some)
        }
        Datum::Timestamptz(ts) => {
            match non_finite_field(
                unit,
                crabka_pgtypes::datetime::timestamptz_infinite_sign(*ts),
            ) {
                NonFinite::Value(text) => return Ok(Some(text)),
                NonFinite::Null => return Ok(None),
                NonFinite::Finite => {}
            }
            // Render the instant in the session zone; the offset enables timezone_*.
            let dt = tz.to_datetime(*ts);
            let off_secs = i64::from(tz.to_offset(*ts).seconds());
            // epoch for timestamptz is the ABSOLUTE instant (independent of zone).
            if unit == "epoch" {
                return Ok(Some(epoch_string_micros(ts.as_microsecond())));
            }
            extract_from_datetime(unit, dt, Some(off_secs), "timestamp with time zone").map(Some)
        }
        Datum::Interval(iv) => {
            match non_finite_field(unit, iv.infinite_sign()) {
                NonFinite::Value(text) => return Ok(Some(text)),
                NonFinite::Null => return Ok(None),
                NonFinite::Finite => {}
            }
            extract_from_interval(unit, field, *iv).map(Some)
        }
        Datum::Time(t) => extract_from_time(unit, field, *t, None).map(Some),
        Datum::Timetz(t) => {
            extract_from_time(unit, field, t.time, Some(i64::from(t.offset.seconds()))).map(Some)
        }
        other => Err(type_error("extract", other)),
    }
}

/// The value of `unit` for a non-finite source, or `None` when the source is
/// finite. PostgreSQL splits the units in two. The *monotonic* ones, which only
/// grow with the value, become `Infinity`/`-Infinity`. The *oscillating* ones
/// (month, day, hour, …) have no limit and become NULL. A unit with no meaning
/// at all for the type is still an error.
fn non_finite_field(unit: &str, sign: i32) -> NonFinite {
    if sign == 0 {
        return NonFinite::Finite;
    }
    let infinity = if sign > 0 { "Infinity" } else { "-Infinity" };
    match unit {
        "year" | "isoyear" | "decade" | "century" | "millennium" | "julian" | "epoch" => {
            NonFinite::Value(infinity.to_string())
        }
        _ => NonFinite::Null,
    }
}

/// What [`non_finite_field`] found: either the source is finite and the caller
/// must compute the field itself, or it is not and the answer is already known.
enum NonFinite {
    /// The source is finite. Compute the field normally.
    Finite,
    /// A monotonic field of a non-finite source: `Infinity` / `-Infinity`.
    Value(String),
    /// An oscillating field of a non-finite source, which has no value.
    Null,
}

/// The Julian Day Number of a calendar date, PostgreSQL's `date2j`.
fn julian_day(date: Date) -> i64 {
    /// The Julian Day Number of 2000-01-01, the anchor the offset is taken from.
    const JD_2000_01_01: i64 = 2_451_545;
    date.since((Unit::Day, Date::constant(2000, 1, 1)))
        .map_or(JD_2000_01_01, |span| {
            JD_2000_01_01 + i64::from(span.get_days())
        })
}

/// The Julian date of a timestamp: the day number plus the elapsed fraction of
/// the day, at the twenty fractional digits PostgreSQL's numeric division
/// gives.
fn julian_datetime_str(dt: DateTime) -> String {
    let day = julian_day(dt.date());
    let micros = i128::from(crabka_pgtypes::datetime::time_to_micros_of_day(
        dt.time().into(),
    ));
    let scale = 10_i128.pow(20);
    let per_day = 86_400_000_000_i128;
    let frac = (micros * scale + per_day / 2) / per_day;
    format!("{day}.{frac:020}")
}

/// Fields of a civil datetime. `tz_offset_secs` is `Some` only for a
/// `timestamptz` source, so the `timezone*` fields resolve. For a plain
/// timestamp they are undefined and give 22023, which matches PostgreSQL.
fn extract_from_datetime(
    field: &str,
    dt: DateTime,
    tz_offset_secs: Option<i64>,
    type_name: &str,
) -> Result<String, ExecError> {
    let date = dt.date();
    let time = dt.time();
    let year = i64::from(date.year());
    Ok(match field {
        "century" => {
            // PG: years 1..100 → century 1; the century of year Y is ceil(Y/100) for
            // positive years (and floor((Y-1)/100)+1 generally for AD).
            int_str(century_of(year))
        }
        "decade" => int_str(year.div_euclid(10)),
        "millennium" => int_str(millennium_of(year)),
        "year" => int_str(year),
        "isoyear" => int_str(i64::from(date.iso_week_date().year())),
        "quarter" => int_str(((i64::from(date.month()) - 1) / 3) + 1),
        "month" => int_str(i64::from(date.month())),
        "day" => int_str(i64::from(date.day())),
        "hour" => int_str(i64::from(time.hour())),
        "minute" => int_str(i64::from(time.minute())),
        "second" => seconds_str(i64::from(time.second()), time.subsec_nanosecond()),
        "milliseconds" => millis_str(i64::from(time.second()), time.subsec_nanosecond()),
        "microseconds" => micros_str(i64::from(time.second()), time.subsec_nanosecond()),
        // dow: Sunday = 0 .. Saturday = 6; isodow: Monday = 1 .. Sunday = 7.
        "dow" => int_str(i64::from(date.weekday().to_sunday_zero_offset())),
        "isodow" => int_str(i64::from(date.weekday().to_monday_one_offset())),
        "doy" => int_str(i64::from(date.day_of_year())),
        "week" => int_str(i64::from(date.iso_week_date().week())),
        "epoch" => {
            // For a plain timestamp/date: seconds since 1970-01-01 "as if UTC".
            let micros = dt
                .since((Unit::Microsecond, unix_epoch_civil()))
                .map(|s| s.get_microseconds())
                .map_err(|_| invalid_param("timestamp out of range for epoch"))?;
            epoch_string_micros(micros)
        }
        "julian" => julian_datetime_str(dt),
        "timezone" => match tz_offset_secs {
            Some(secs) => int_str(secs),
            None => return Err(unsupported_field(field, type_name)),
        },
        "timezone_hour" => match tz_offset_secs {
            Some(secs) => int_str(secs / 3600),
            None => return Err(unsupported_field(field, type_name)),
        },
        "timezone_minute" => match tz_offset_secs {
            Some(secs) => int_str((secs % 3600) / 60),
            None => return Err(unsupported_field(field, type_name)),
        },
        _ => return Err(unsupported_field(field, type_name)),
    })
}

/// Fields of a `time` value: only the clock fields, plus the zone fields when
/// the value is a `timetz` and so carries `tz_offset_secs`.
fn extract_from_time(
    unit: &str,
    field: &str,
    t: crabka_pgtypes::datetime::PgTime,
    tz_offset_secs: Option<i64>,
) -> Result<String, ExecError> {
    let type_name = if tz_offset_secs.is_some() {
        "time with time zone"
    } else {
        "time without time zone"
    };
    Ok(match unit {
        "hour" => int_str(i64::from(t.hour())),
        "minute" => int_str(i64::from(t.minute())),
        "second" => seconds_str(i64::from(t.second()), t.subsec_nanosecond()),
        "milliseconds" => millis_str(i64::from(t.second()), t.subsec_nanosecond()),
        "microseconds" => micros_str(i64::from(t.second()), t.subsec_nanosecond()),
        // A `timetz` epoch is seconds since midnight UTC, so the offset counts.
        "epoch" => epoch_string_micros(
            crabka_pgtypes::datetime::time_to_micros_of_day(t)
                - tz_offset_secs.unwrap_or(0) * 1_000_000,
        ),
        "timezone" => match tz_offset_secs {
            Some(secs) => int_str(secs),
            None => return Err(unsupported_field(field, type_name)),
        },
        "timezone_hour" => match tz_offset_secs {
            Some(secs) => int_str(secs / 3600),
            None => return Err(unsupported_field(field, type_name)),
        },
        "timezone_minute" => match tz_offset_secs {
            Some(secs) => int_str((secs % 3600) / 60),
            None => return Err(unsupported_field(field, type_name)),
        },
        _ => return Err(unsupported_field(field, type_name)),
    })
}

/// Fields of an `interval`. PostgreSQL extracts the stored months/days/micros
/// fields directly and normalizes nothing: year = months/12, month = months%12,
/// and so on.
fn extract_from_interval(unit: &str, field: &str, iv: Interval) -> Result<String, ExecError> {
    let months = i64::from(iv.months);
    let secs_whole = iv.micros / 1_000_000;
    let subsec = (iv.micros % 1_000_000) as i32; // microseconds within the second
    Ok(match unit {
        "millennium" => int_str(months / 12000),
        "century" => int_str(months / 1200),
        "decade" => int_str(months / 120),
        "year" => int_str(months / 12),
        "quarter" => int_str((months % 12) / 3 + 1),
        "month" => int_str(months % 12),
        "day" => int_str(i64::from(iv.days)),
        "hour" => int_str(secs_whole / 3600),
        "minute" => int_str((secs_whole % 3600) / 60),
        "second" => seconds_str(secs_whole % 60, subsec * 1_000),
        "milliseconds" => millis_str(secs_whole % 60, subsec * 1_000),
        "microseconds" => micros_str(secs_whole % 60, subsec * 1_000),
        "epoch" => {
            // PG: total seconds, treating a month as 30 days and a day as 86400 s.
            // PostgreSQL's `extract(epoch …)` returns `numeric`, so a wide
            // interval has an answer rather than an overflow; compute in i128
            // so this one does too.
            let total_micros = (i128::from(months) * 30 + i128::from(iv.days)) * 86_400_000_000
                + i128::from(iv.micros);
            epoch_string_micros_wide(total_micros)
        }
        _ => return Err(unsupported_field(field, "interval")),
    })
}

/// 22023: a unit no datetime function has ever heard of (`fortnight`).
///
/// `PostgreSQL` names the source type as `format_type_be` spells it, so the
/// same unrecognised unit reads differently over a `time` and over a `timetz`.
/// The wording is the whole message, not a value inside the `SET`/`RESET`
/// sentence [`ExecError::InvalidParameterValue`] builds.
fn unknown_field(field: &str, type_name: &str) -> ExecError {
    ExecError::InvalidParameterValueMessage(format!(
        "unit \"{field}\" not recognized for type {type_name}"
    ))
}

/// The type an `extract` diagnostic names, for the six source types the
/// function accepts. `None` for anything else, which `PostgreSQL` turns away
/// during function resolution and never reports as a unit failure.
fn extract_type_name(source: &Datum) -> Option<&'static str> {
    match source {
        Datum::Date(_)
        | Datum::Time(_)
        | Datum::Timetz(_)
        | Datum::Timestamp(_)
        | Datum::Timestamptz(_)
        | Datum::Interval(_) => source.column_type().map(ColumnType::name),
        _ => None,
    }
}

/// The type a `date_trunc` diagnostic names.
///
/// `PostgreSQL` has no `date_trunc(text, date)`: a `date` argument is coerced
/// to `timestamptz` before the call, so a `date` reports the type it was
/// coerced to and never its own.
fn date_trunc_type_name(source: &Datum) -> Option<&'static str> {
    match source {
        Datum::Date(_) => Some(ColumnType::Timestamptz.name()),
        Datum::Timestamp(_) | Datum::Timestamptz(_) | Datum::Interval(_) => {
            source.column_type().map(ColumnType::name)
        }
        _ => None,
    }
}

/// century containing AD year `y` (PG: 2000 is in century 20, 2001 starts century 21).
fn century_of(y: i64) -> i64 {
    if y > 0 {
        (y + 99) / 100
    } else {
        // Astronomical year 0 is 1 BC, which PostgreSQL counts in century -1.
        -((99 - (y - 1)) / 100)
    }
}

/// millennium containing AD year `y` (PG: 2000 is millennium 2, 2001 starts 3).
fn millennium_of(y: i64) -> i64 {
    if y > 0 {
        (y + 999) / 1000
    } else {
        -((999 - (y - 1)) / 1000)
    }
}

fn int_str(n: i64) -> String {
    n.to_string()
}

/// `second` (and the interval/time second) as a decimal. `extract` returns
/// `numeric`, and PostgreSQL fixes this field's scale at 6, so the trailing
/// zeros are significant. `3` prints as `3.000000`.
fn seconds_str(secs: i64, subsec_nanos: i32) -> String {
    let total_micros = secs * 1_000_000 + i64::from(subsec_nanos / 1_000);
    decimal_micros_scaled(total_micros, 1_000_000)
}

/// `milliseconds` = whole_seconds*1000 + subsecond-as-millis, at PostgreSQL's
/// fixed scale of 3.
fn millis_str(secs: i64, subsec_nanos: i32) -> String {
    let total_micros = secs * 1_000_000 + i64::from(subsec_nanos / 1_000);
    decimal_micros_scaled(total_micros, 1_000)
}

/// `microseconds` = whole_seconds*1_000_000 + subsecond-micros (an integer).
fn micros_str(secs: i64, subsec_nanos: i32) -> String {
    let total_micros = secs * 1_000_000 + i64::from(subsec_nanos / 1_000);
    total_micros.to_string()
}

/// Render `total_micros / divisor` at the fixed scale `divisor` implies: 1000
/// gives 3 fractional digits, and 1_000_000 gives 6. `extract` yields `numeric`,
/// whose text form keeps every digit of the declared scale, so trailing zeros
/// stay.
fn decimal_micros_scaled(total_micros: i64, divisor: i64) -> String {
    let neg = total_micros < 0;
    let abs = total_micros.unsigned_abs();
    let divisor = divisor.unsigned_abs();
    let whole = abs / divisor;
    let frac = abs % divisor;
    let width = divisor.ilog10() as usize;
    format!("{}{whole}.{frac:0width$}", if neg { "-" } else { "" })
}

/// `epoch` = seconds, from a signed microsecond count, at PostgreSQL's fixed
/// scale of 6.
fn epoch_string_micros(total_micros: i64) -> String {
    decimal_micros_scaled(total_micros, 1_000_000)
}

/// `epoch` from a microsecond count too wide for `i64`.
///
/// `extract(epoch FROM interval)` returns `numeric` in PostgreSQL, so a month
/// count near the type's limits still has an answer. A render from `i128` keeps
/// it.
fn epoch_string_micros_wide(total_micros: i128) -> String {
    let seconds = total_micros / 1_000_000;
    let fraction = (total_micros % 1_000_000).abs();
    let sign = if total_micros < 0 && seconds == 0 {
        "-"
    } else {
        ""
    };
    format!("{sign}{seconds}.{fraction:06}")
}

// ---- date_trunc ----

/// Truncate `source` to `unit`. timestamp, date (to timestamp), and interval
/// truncate directly. timestamptz truncates in `zone` (3-arg) or `session_tz`
/// (2-arg) and returns a timestamptz.
fn date_trunc(
    unit: &str,
    source: &Datum,
    zone: Option<&TimeZone>,
    session_tz: &TimeZone,
) -> Result<Datum, ExecError> {
    match source {
        Datum::Timestamp(dt) => {
            // A non-finite timestamp truncates to itself, but the unit is still
            // validated first.
            if crabka_pgtypes::datetime::timestamp_is_infinite(*dt) {
                trunc_datetime(unit, ColumnType::Timestamp.name(), DateTime::default())?;
                return Ok(Datum::Timestamp(*dt));
            }
            Ok(Datum::Timestamp(trunc_datetime(
                unit,
                ColumnType::Timestamp.name(),
                *dt,
            )?))
        }
        // PostgreSQL has no `date_trunc(text, date)`: the date is coerced to
        // `timestamptz`, so the result carries the session zone.
        Datum::Date(d) => date_trunc(
            unit,
            &crabka_pgtypes::cast::cast(
                source,
                crabka_pgtypes::ColumnType::Timestamptz,
                session_tz,
            )
            .map_err(|_| invalid_param(format!("date out of range for date_trunc: {d}")))?,
            zone,
            session_tz,
        ),
        Datum::Timestamptz(ts) => {
            if crabka_pgtypes::datetime::timestamptz_is_infinite(*ts) {
                trunc_datetime(unit, ColumnType::Timestamptz.name(), DateTime::default())?;
                return Ok(Datum::Timestamptz(*ts));
            }
            let tz = zone.unwrap_or(session_tz);
            // Render in the zone, truncate the wall-clock, then put it back on
            // the timeline.
            let offset = tz.to_offset(*ts);
            let dt = tz.to_datetime(*ts);
            let truncated = trunc_datetime(unit, ColumnType::Timestamptz.name(), dt)?;
            let instant = if unit_redoes_zone(unit) {
                // Truncating to a day or coarser moves the reading far enough
                // that its offset has to be worked out afresh, and the midnight
                // it lands on may itself be a DST gap or fold.
                crabka_pgtypes::datetime::zoned_instant(truncated, tz)
            } else {
                // Truncating within the day keeps the offset the source instant
                // already had. That is what makes `date_trunc('hour', …)` inside
                // a fall-back fold stay on the pre-transition side, where
                // re-deriving the offset would move it an hour.
                offset.to_timestamp(truncated)
            };
            instant
                .map(Datum::Timestamptz)
                .map_err(|_| invalid_param("timestamp out of range for date_trunc"))
        }
        Datum::Interval(iv) => Ok(Datum::Interval(trunc_interval(unit, *iv)?)),
        other => Err(type_error("date_trunc", other)),
    }
}

/// Whether truncating to `unit` makes `date_trunc` re-derive the zone offset.
///
/// `PostgreSQL`'s `redotz`: set for `day` and every coarser unit, left clear for
/// `hour` and finer. A sub-day truncation therefore carries the source instant's
/// own offset forward, which only shows up on a DST boundary — and is exactly
/// where it shows up that the two rules disagree.
fn unit_redoes_zone(unit: &str) -> bool {
    matches!(
        unit,
        "day" | "week" | "month" | "quarter" | "year" | "decade" | "century" | "millennium"
    )
}

/// Zero out every field below `unit` in a civil datetime. `type_name` is the
/// source type the caller reports, since the same civil datetime stands in for
/// a `timestamp` and for a `timestamptz` rendered in its zone.
fn trunc_datetime(unit: &str, type_name: &str, dt: DateTime) -> Result<DateTime, ExecError> {
    let d = dt.date();
    let (y, m, day) = (d.year(), d.month(), d.day());
    let t = dt.time();
    let mk = |y: i16, m: i8, d: i8, h: i8, mi: i8, s: i8| {
        DateTime::new(y, m, d, h, mi, s, 0).map_err(|_| invalid_param("date_trunc out of range"))
    };
    Ok(match unit {
        "microseconds" => dt,
        "milliseconds" => {
            // Keep whole milliseconds (drop sub-millisecond nanos).
            let nanos = (t.subsec_nanosecond() / 1_000_000) * 1_000_000;
            DateTime::new(y, m, day, t.hour(), t.minute(), t.second(), nanos)
                .map_err(|_| invalid_param("date_trunc out of range"))?
        }
        "second" => mk(y, m, day, t.hour(), t.minute(), t.second())?,
        "minute" => mk(y, m, day, t.hour(), t.minute(), 0)?,
        "hour" => mk(y, m, day, t.hour(), 0, 0)?,
        "day" => mk(y, m, day, 0, 0, 0)?,
        "week" => {
            // PG truncates to the most recent Monday (ISO week start), midnight.
            let back = i64::from(d.weekday().to_monday_one_offset()) - 1;
            let monday = crabka_pgtypes::datetime::date_plus_days(d, -back)
                .map_err(|_| invalid_param("date_trunc week out of range"))?;
            crabka_pgtypes::datetime::date_to_midnight(monday)
        }
        "month" => mk(y, m, 1, 0, 0, 0)?,
        "quarter" => {
            let qm = ((m - 1) / 3) * 3 + 1;
            mk(y, qm, 1, 0, 0, 0)?
        }
        "year" => mk(y, 1, 1, 0, 0, 0)?,
        "decade" => mk((y / 10) * 10, 1, 1, 0, 0, 0)?,
        "century" => {
            // First year of the century containing y (1901, 2001, …).
            let cy = ((century_of(i64::from(y)) - 1) * 100 + 1) as i16;
            mk(cy, 1, 1, 0, 0, 0)?
        }
        "millennium" => {
            let my = ((millennium_of(i64::from(y)) - 1) * 1000 + 1) as i16;
            mk(my, 1, 1, 0, 0, 0)?
        }
        _ => return Err(unsupported_field(unit, type_name)),
    })
}

/// Truncate an interval to `unit`, and zero out the finer stored fields.
fn trunc_interval(unit: &str, iv: Interval) -> Result<Interval, ExecError> {
    let months = iv.months;
    let days = iv.days;
    let micros = iv.micros;
    let secs = micros / 1_000_000;
    Ok(match unit {
        "microseconds" => iv,
        "milliseconds" => Interval {
            months,
            days,
            micros: (micros / 1_000) * 1_000,
        },
        "second" => Interval {
            months,
            days,
            micros: secs * 1_000_000,
        },
        "minute" => Interval {
            months,
            days,
            micros: (secs / 60) * 60_000_000,
        },
        "hour" => Interval {
            months,
            days,
            micros: (secs / 3600) * 3_600_000_000,
        },
        "day" => Interval {
            months,
            days,
            micros: 0,
        },
        "month" => Interval {
            months,
            days: 0,
            micros: 0,
        },
        "quarter" => Interval {
            months: (months / 3) * 3,
            days: 0,
            micros: 0,
        },
        "year" => Interval {
            months: (months / 12) * 12,
            days: 0,
            micros: 0,
        },
        "decade" => Interval {
            months: (months / 120) * 120,
            days: 0,
            micros: 0,
        },
        "century" => Interval {
            months: (months / 1200) * 1200,
            days: 0,
            micros: 0,
        },
        "millennium" => Interval {
            months: (months / 12000) * 12000,
            days: 0,
            micros: 0,
        },
        // `week` is the one unit PostgreSQL refuses for `interval` with an
        // explanation: months are stored apart from days, and a month is not a
        // whole number of weeks, so there is no week boundary to truncate to.
        "week" => {
            return Err(ExecError::UnsupportedWithDetail {
                message: format!("unit \"{unit}\" not supported for type interval"),
                detail: "Months usually have fractional weeks.".into(),
            });
        }
        _ => return Err(unsupported_field(unit, "interval")),
    })
}

// ---- age ----

/// PostgreSQL `age(end, start)`: a SYMBOLIC interval. It subtracts each field
/// and borrows from the next-larger field when a smaller field goes negative,
/// with the actual days in the borrowed month. Returns months/days/micros.
fn age(end: DateTime, start: DateTime) -> Interval {
    let mut years = i64::from(end.date().year()) - i64::from(start.date().year());
    let mut months = i64::from(end.date().month()) - i64::from(start.date().month());
    let mut days = i64::from(end.date().day()) - i64::from(start.date().day());

    // Time-of-day difference in microseconds.
    let mut micros = time_micros(end.time()) - time_micros(start.time());

    // Borrow up from micros → days, days → months, months → years.
    const DAY_US: i64 = 86_400_000_000;
    if micros < 0 {
        micros += DAY_US;
        days -= 1;
    }
    if days < 0 {
        // Borrow the number of days in the month BEFORE the end month (PG uses the
        // month preceding `end`'s month, in `end`'s year, adjusting for January).
        let (by, bm) = prev_month(end.date().year(), end.date().month());
        // A month outside the representable calendar cannot precede a valid
        // `end`, so falling back to 30 is unreachable in practice and keeps
        // this arithmetic total rather than panicking.
        days += i64::from(days_in_month(by, bm).unwrap_or(30));
        months -= 1;
    }
    if months < 0 {
        months += 12;
        years -= 1;
    }

    let total_months = years * 12 + months;
    Interval {
        months: total_months as i32,
        days: days as i32,
        micros,
    }
}

fn time_micros(t: Time) -> i64 {
    i64::from(t.hour()) * 3_600_000_000
        + i64::from(t.minute()) * 60_000_000
        + i64::from(t.second()) * 1_000_000
        + i64::from(t.subsec_nanosecond() / 1_000)
}

/// The (year, month) immediately before `(year, month)`.
fn prev_month(year: i16, month: i8) -> (i16, i8) {
    if month == 1 {
        (year - 1, 12)
    } else {
        (year, month - 1)
    }
}

/// Days in the given month (1..=12), leap-aware through jiff.
/// The length of `year`-`month`, or `None` when that month is outside the
/// representable calendar. A caller that walks months can step past the end.
fn days_in_month(year: i16, month: i8) -> Option<i8> {
    Date::new(year, month, 1).ok().map(Date::days_in_month)
}

// ---- timezone (AT TIME ZONE) ----

/// `timezone(zone, value)`:
///   * `timestamp` → `timestamptz`: interpret the wall-clock as local to `zone`.
///   * `timestamptz` → `timestamp`: render the instant in `zone` (wall-clock).
fn timezone_convert(tz: &TimeZone, value: &Datum) -> Result<Datum, ExecError> {
    match value {
        // A non-finite reading is the same non-finite reading in every zone: it
        // is not a wall-clock a zone can move. Rendering it would land on an
        // ordinary value in any zone off UTC, and re-deriving an instant from
        // one can leave the calendar altogether, so only the type changes.
        Datum::Timestamp(dt) if crabka_pgtypes::datetime::timestamp_is_infinite(*dt) => {
            let sign = crabka_pgtypes::datetime::timestamp_infinite_sign(*dt);
            Ok(Datum::Timestamptz(
                crabka_pgtypes::datetime::timestamptz_infinity_of_sign(sign),
            ))
        }
        Datum::Timestamptz(ts) if crabka_pgtypes::datetime::timestamptz_is_infinite(*ts) => {
            let sign = crabka_pgtypes::datetime::timestamptz_infinite_sign(*ts);
            Ok(Datum::Timestamp(
                crabka_pgtypes::datetime::timestamp_infinity_of_sign(sign),
            ))
        }
        // A wall-clock on a DST boundary has two readings or none; PostgreSQL's
        // `DetermineTimeZoneOffset` picks the later instant either way, which is
        // what `zoned_instant` applies. jiff's own `Compatible` strategy takes
        // the *earlier* instant in a fold, so it cannot stand in here.
        Datum::Timestamp(dt) => crabka_pgtypes::datetime::zoned_instant(*dt, tz)
            .map(Datum::Timestamptz)
            .map_err(|_| invalid_param("timestamp out of range for time zone conversion")),
        Datum::Timestamptz(ts) => Ok(Datum::Timestamp(tz.to_datetime(*ts))),
        // `timetz AT TIME ZONE zone` re-expresses the same instant-of-day at the
        // target zone's offset, keeping the type.
        Datum::Timetz(t) => {
            let offset = tz.to_offset(jiff::Timestamp::now());
            let utc_micros = t.utc_micros().rem_euclid(86_400_000_000);
            let shifted =
                (utc_micros + i64::from(offset.seconds()) * 1_000_000).rem_euclid(86_400_000_000);
            Ok(Datum::Timetz(crabka_pgtypes::datetime::TimeTz {
                time: crabka_pgtypes::datetime::time_from_micros_of_day_public(shifted),
                offset,
            }))
        }
        other => Err(type_error("timezone", other)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_pgtypes::{ColumnType, Datum};

    use crate::{
        clock::{EvalCtx, FixedClock},
        scope::Scope,
    };

    fn ctx_at(rfc3339: &str) -> EvalCtx {
        ctx_at_zone(rfc3339, jiff::tz::TimeZone::UTC)
    }
    fn ctx_at_zone(rfc3339: &str, time_zone: jiff::tz::TimeZone) -> EvalCtx {
        let now: jiff::Timestamp = rfc3339.parse().expect("ts");
        EvalCtx {
            now,
            stmt_now: now,
            time_zone,
            date_order: crabka_pgtypes::datetime::DateOrder::default(),
            date_style: crabka_pgtypes::datetime::DateStyle::default(),
            interval_style: crabka_pgtypes::datetime::IntervalStyle::default(),
            extra_float_digits: 1,
            current_user: "public".into(),
            session_user: "public".into(),
            backend_pid: 0,
            trigger_depth: 0,
            clock: Arc::new(FixedClock(now)),
            random: None,
            sequence: None,
            catalog: None,
            resolution: None,
            notify: None,
            transition_relations: None,
            event_trigger: None,
        }
    }
    fn ev(sql: &str, ctx: &EvalCtx) -> Datum {
        crate::eval::eval(
            &crabka_pgparser::parser::parse_expr_for_test(sql).expect("parse"),
            &Scope::empty(),
            &[],
            ctx,
        )
        .expect("eval")
    }
    fn num(s: &str) -> Datum {
        Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"))
    }

    /// `AT TIME ZONE` and `date_trunc`'s zone argument read their text as a
    /// POSIX specification, whose offset counts *west* of Greenwich — so
    /// `'-08:00'` names UTC+8, the opposite of what the same text means inside
    /// a literal. Every expectation is `PostgreSQL` 18.4's.
    #[test]
    fn at_time_zone_reads_a_specification_west_of_greenwich() {
        use assert2::assert;

        let ctx = ctx_at("2024-01-15T12:00:00Z");
        for (expr, expected) in [
            (
                "timestamptz '1970-01-01 00:00:00+00' AT TIME ZONE '-08:00'",
                "1970-01-01T08:00:00",
            ),
            (
                "timestamptz '1970-01-01 00:00:00+00' AT TIME ZONE '+08:00'",
                "1969-12-31T16:00:00",
            ),
            (
                "timestamptz '1970-01-01 00:00:00+00' AT TIME ZONE '08:00'",
                "1969-12-31T16:00:00",
            ),
            (
                "timestamptz '1970-01-01 00:00:00+00' AT TIME ZONE '-08'",
                "1970-01-01T08:00:00",
            ),
            (
                "timestamptz '1970-01-01 00:00:00+00' AT TIME ZONE 'UTC-2'",
                "1970-01-01T02:00:00",
            ),
            (
                "timestamptz '1970-01-01 00:00:00+00' AT TIME ZONE 'UTC+10'",
                "1969-12-31T14:00:00",
            ),
            // A specification with a rule, either side of its spring transition.
            (
                "timestamptz '2014-03-09 09:59:00+00' AT TIME ZONE 'PST8PDT,M3.2.0,M11.1.0'",
                "2014-03-09T01:59:00",
            ),
            (
                "timestamptz '2014-03-09 10:01:00+00' AT TIME ZONE 'PST8PDT,M3.2.0,M11.1.0'",
                "2014-03-09T03:01:00",
            ),
            // An abbreviation the `TimeZone` setting would refuse.
            (
                "timestamptz '1970-01-01 00:00:00+00' AT TIME ZONE 'PST'",
                "1969-12-31T16:00:00",
            ),
        ] {
            let want = Datum::Timestamp(expected.parse().expect("expected civil datetime"));
            assert!(ev(expr, &ctx) == want, "{expr}");
        }
    }

    /// An unresolvable zone is `PostgreSQL`'s own one-line 22023, not a message
    /// nested inside another message.
    #[test]
    fn at_time_zone_reports_an_unknown_zone_the_way_postgresql_words_it() {
        use assert2::assert;

        let error = crate::eval::eval(
            &crabka_pgparser::parser::parse_expr_for_test(
                "timestamptz '1970-01-01 00:00:00+00' AT TIME ZONE 'America/Does_not_exist'",
            )
            .expect("parse"),
            &Scope::empty(),
            &[],
            &ctx_at("2024-01-15T12:00:00Z"),
        )
        .expect_err("unknown zone")
        .into_pg();
        assert!(error.code == "22023");
        assert!(error.message == "time zone \"America/Does_not_exist\" not recognized");
    }

    #[test]
    fn now_is_transaction_stable_and_typed_timestamptz() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        assert_eq!(ev("now()", &ctx), ev("current_timestamp", &ctx));
        assert_eq!(
            crate::eval::infer_type(
                &crabka_pgparser::parser::parse_expr_for_test("now()").expect("p"),
                &Scope::empty()
            )
            .expect("inf"),
            ColumnType::Timestamptz
        );
        assert_eq!(
            ev("current_date", &ctx),
            Datum::Date("2024-01-15".parse().expect("d"))
        );
    }

    #[test]
    fn extract_returns_numeric_date_part_returns_float8() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        assert_eq!(
            ev("extract(year from TIMESTAMP '2024-01-15 13:45:06')", &ctx),
            num("2024")
        );
        assert_eq!(ev("extract(month from DATE '2024-07-01')", &ctx), num("7"));
        assert_eq!(
            ev("date_part('hour', TIMESTAMP '2024-01-15 13:45:06')", &ctx),
            Datum::Float8(13.0)
        );
    }

    #[test]
    fn date_trunc_and_age() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        assert_eq!(
            ev("date_trunc('month', TIMESTAMP '2024-07-15 13:45:06')", &ctx),
            Datum::Timestamp(
                crabka_pgtypes::datetime::parse_timestamp("2024-07-01 00:00:00").expect("ts")
            )
        );
        assert_eq!(
            ev(
                "age(TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')",
                &ctx
            ),
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months: 2,
                days: 0,
                micros: 0
            })
        );
    }

    // ---- additional coverage ----

    #[test]
    fn clock_family_variants() {
        let ctx = ctx_at("2024-06-01T08:30:45Z");
        let now = ctx.now;
        assert_eq!(ev("now()", &ctx), Datum::Timestamptz(now));
        assert_eq!(ev("transaction_timestamp()", &ctx), Datum::Timestamptz(now));
        assert_eq!(ev("statement_timestamp()", &ctx), Datum::Timestamptz(now));
        assert_eq!(ev("clock_timestamp()", &ctx), Datum::Timestamptz(now));
        // current_time / localtime / localtimestamp render in the session zone (UTC).
        assert_eq!(
            ev("current_time", &ctx),
            Datum::Time(crabka_pgtypes::datetime::parse_time("08:30:45").expect("t"))
        );
        assert_eq!(
            ev("localtime", &ctx),
            Datum::Time(crabka_pgtypes::datetime::parse_time("08:30:45").expect("t"))
        );
        assert_eq!(
            ev("localtimestamp", &ctx),
            Datum::Timestamp(
                crabka_pgtypes::datetime::parse_timestamp("2024-06-01 08:30:45").expect("ts")
            )
        );
    }

    #[test]
    fn current_date_and_clock_respect_session_zone() {
        // 2024-01-15 02:00 UTC is 2024-01-14 21:00 in New_York → current_date is the 14th.
        let ny: jiff::Timestamp = "2024-01-15T02:00:00Z".parse().expect("ts");
        let ctx = EvalCtx {
            now: ny,
            stmt_now: ny,
            time_zone: jiff::tz::TimeZone::get("America/New_York").expect("ny"),
            date_order: crabka_pgtypes::datetime::DateOrder::default(),
            date_style: crabka_pgtypes::datetime::DateStyle::default(),
            interval_style: crabka_pgtypes::datetime::IntervalStyle::default(),
            extra_float_digits: 1,
            current_user: "public".into(),
            session_user: "public".into(),
            backend_pid: 0,
            trigger_depth: 0,
            clock: Arc::new(FixedClock(ny)),
            random: None,
            sequence: None,
            catalog: None,
            resolution: None,
            notify: None,
            transition_relations: None,
            event_trigger: None,
        };
        assert_eq!(
            ev("current_date", &ctx),
            Datum::Date("2024-01-14".parse().expect("d"))
        );
        // now() is still the absolute instant (timestamptz), zone-independent.
        assert_eq!(ev("now()", &ctx), Datum::Timestamptz(ny));
    }

    #[test]
    fn extract_many_fields() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        let ex = |f: &str| {
            ev(
                &format!("extract({f} from TIMESTAMP '2024-02-29 13:45:06.5')"),
                &ctx,
            )
        };
        assert_eq!(ex("year"), num("2024"));
        assert_eq!(ex("month"), num("2"));
        assert_eq!(ex("day"), num("29"));
        assert_eq!(ex("hour"), num("13"));
        assert_eq!(ex("minute"), num("45"));
        assert_eq!(ex("second"), num("6.5"));
        assert_eq!(ex("quarter"), num("1"));
        assert_eq!(ex("decade"), num("202"));
        assert_eq!(ex("century"), num("21"));
        assert_eq!(ex("millennium"), num("3"));
        // 2024-02-29 is a Thursday → dow=4, isodow=4.
        assert_eq!(ex("dow"), num("4"));
        assert_eq!(ex("isodow"), num("4"));
        // day-of-year for a leap year: Jan(31)+Feb(29) = 60.
        assert_eq!(ex("doy"), num("60"));
        // milliseconds = 6*1000 + 500 = 6500; microseconds = 6_500_000.
        assert_eq!(ex("milliseconds"), num("6500"));
        assert_eq!(ex("microseconds"), num("6500000"));
    }

    #[test]
    fn extract_epoch_timestamp_and_timestamptz() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        // timestamp epoch is "as if UTC": 2024-01-01 00:00:00 → 1704067200.
        assert_eq!(
            ev("extract(epoch from TIMESTAMP '2024-01-01 00:00:00')", &ctx),
            num("1704067200")
        );
        // timestamptz epoch is the absolute instant (same value when UTC).
        assert_eq!(
            ev(
                "extract(epoch from TIMESTAMPTZ '2024-01-01 00:00:00+00')",
                &ctx
            ),
            num("1704067200")
        );
        // sub-second epoch is exact.
        assert_eq!(
            ev(
                "extract(epoch from TIMESTAMP '1970-01-01 00:00:00.5')",
                &ctx
            ),
            num("0.5")
        );
    }

    #[test]
    fn extract_timezone_fields_on_timestamptz() {
        // Render in New_York (EST = -05:00 in January) → timezone = -18000s.
        let ts: jiff::Timestamp = "2024-01-15T17:00:00Z".parse().expect("ts");
        let ctx = EvalCtx {
            now: ts,
            stmt_now: ts,
            time_zone: jiff::tz::TimeZone::get("America/New_York").expect("ny"),
            date_order: crabka_pgtypes::datetime::DateOrder::default(),
            date_style: crabka_pgtypes::datetime::DateStyle::default(),
            interval_style: crabka_pgtypes::datetime::IntervalStyle::default(),
            extra_float_digits: 1,
            current_user: "public".into(),
            session_user: "public".into(),
            backend_pid: 0,
            trigger_depth: 0,
            clock: Arc::new(FixedClock(ts)),
            random: None,
            sequence: None,
            catalog: None,
            resolution: None,
            notify: None,
            transition_relations: None,
            event_trigger: None,
        };
        assert_eq!(
            ev(
                "extract(timezone from TIMESTAMPTZ '2024-01-15 12:00:00-05')",
                &ctx
            ),
            num("-18000")
        );
        assert_eq!(
            ev(
                "extract(timezone_hour from TIMESTAMPTZ '2024-01-15 12:00:00-05')",
                &ctx
            ),
            num("-5")
        );
    }

    #[test]
    fn extract_from_interval_and_time() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        // INTERVAL '14 months 5 days 06:00:00' → year 1, month 2, day 5, hour 6.
        assert_eq!(
            ev("extract(year from INTERVAL '14 months')", &ctx),
            num("1")
        );
        assert_eq!(
            ev("extract(month from INTERVAL '14 months')", &ctx),
            num("2")
        );
        assert_eq!(
            ev("extract(hour from INTERVAL '6 hours 30 minutes')", &ctx),
            num("6")
        );
        assert_eq!(
            ev("extract(minute from INTERVAL '6 hours 30 minutes')", &ctx),
            num("30")
        );
        // bare time
        assert_eq!(ev("extract(hour from TIME '08:15:30')", &ctx), num("8"));
        assert_eq!(ev("extract(minute from TIME '08:15:30')", &ctx), num("15"));
    }

    #[test]
    fn extract_field_errors_split_unknown_units_from_unsupported_ones() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        let err = crate::eval::eval(
            &crabka_pgparser::parser::parse_expr_for_test(
                "extract(nonsense from TIMESTAMP '2024-01-15 00:00:00')",
            )
            .expect("p"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect_err("unknown field");
        assert_eq!(err.into_pg().code, "22023");
        // `timezone` is a real unit, but a plain timestamp has no value for it,
        // which PostgreSQL reports as 0A000 rather than an unknown unit.
        let err2 = crate::eval::eval(
            &crabka_pgparser::parser::parse_expr_for_test(
                "extract(timezone from TIMESTAMP '2024-01-15 00:00:00')",
            )
            .expect("p"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect_err("no tz on plain ts");
        assert2::assert!(err2.into_pg().code == "0A000");
    }

    #[test]
    fn date_trunc_units() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        let ts =
            |s: &str| Datum::Timestamp(crabka_pgtypes::datetime::parse_timestamp(s).expect("ts"));
        let dt = "TIMESTAMP '2024-07-15 13:45:06.789'";
        assert_eq!(
            ev(&format!("date_trunc('hour', {dt})"), &ctx),
            ts("2024-07-15 13:00:00")
        );
        assert_eq!(
            ev(&format!("date_trunc('day', {dt})"), &ctx),
            ts("2024-07-15 00:00:00")
        );
        assert_eq!(
            ev(&format!("date_trunc('month', {dt})"), &ctx),
            ts("2024-07-01 00:00:00")
        );
        assert_eq!(
            ev(&format!("date_trunc('year', {dt})"), &ctx),
            ts("2024-01-01 00:00:00")
        );
        assert_eq!(
            ev(&format!("date_trunc('quarter', {dt})"), &ctx),
            ts("2024-07-01 00:00:00")
        );
        // week: 2024-07-15 is a Monday → unchanged at midnight.
        assert_eq!(
            ev(&format!("date_trunc('week', {dt})"), &ctx),
            ts("2024-07-15 00:00:00")
        );
        // PostgreSQL has no `date_trunc(text, date)`: the date resolves through
        // `timestamptz`, so the result carries the session zone.
        assert2::assert!(
            ev("date_trunc('month', DATE '2024-07-15')", &ctx)
                == Datum::Timestamptz(
                    crabka_pgtypes::datetime::parse_timestamptz(
                        "2024-07-01 00:00:00+00",
                        &jiff::tz::TimeZone::UTC
                    )
                    .expect("tstz")
                )
        );
    }

    /// The rules `PostgreSQL` applies when a wall-clock reading falls on a DST
    /// boundary, all read off a PostgreSQL 18.4 oracle.
    ///
    /// `AT TIME ZONE` resolves a gap or a fold with `DetermineTimeZoneOffset`,
    /// which takes the smaller of the two candidate offsets — the later instant
    /// — where jiff's own `Compatible` strategy takes the earlier one in a fold.
    /// `date_trunc` uses that same rule only for `day` and coarser units; a
    /// sub-day truncation keeps whatever offset the source instant already had,
    /// which is `PostgreSQL`'s `redotz` flag.
    #[test]
    fn dst_boundaries_resolve_the_way_postgres_resolves_them() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        let epoch = |d: &Datum| match d {
            Datum::Timestamptz(ts) => ts.as_second(),
            other => panic!("expected timestamptz, got {other:?}"),
        };
        let at = |secs: i64| secs;
        // 2011-11-06 01:30 in Los Angeles is a fold: 08:30 UTC at PDT (-7) or
        // 09:30 UTC at PST (-8). PostgreSQL names the later instant.
        assert2::assert!(
            epoch(&ev(
                "TIMESTAMP '2011-11-06 01:30:00' AT TIME ZONE 'America/Los_Angeles'",
                &ctx
            )) == at(1_320_571_800)
        );
        // 2011-03-13 02:30 does not exist there; the same rule gives 10:30 UTC.
        assert2::assert!(
            epoch(&ev(
                "TIMESTAMP '2011-03-13 02:30:00' AT TIME ZONE 'America/Los_Angeles'",
                &ctx
            )) == at(1_300_012_200)
        );
        // Truncating to the hour inside that fold keeps the source offset, so
        // 08:30 UTC (PDT) becomes 08:00 UTC and stays on the pre-transition
        // side; re-deriving the offset would move it an hour to 09:00 UTC.
        assert2::assert!(
            epoch(&ev(
                "date_trunc('hour', TIMESTAMPTZ '2011-11-06 08:30:00+00', 'America/Los_Angeles')",
                &ctx
            )) == at(1_320_566_400)
        );
        // A day truncation does re-derive it. Havana began DST at midnight on
        // 2011-03-13, so the midnight it lands on is a gap.
        assert2::assert!(
            epoch(&ev(
                "date_trunc('day', TIMESTAMPTZ '2011-03-13 10:00:00+00', 'America/Havana')",
                &ctx
            )) == at(1_299_992_400)
        );
        // And the same day truncation inside the fold lands on a midnight that
        // is unambiguous, one PDT day earlier.
        assert2::assert!(
            epoch(&ev(
                "date_trunc('day', TIMESTAMPTZ '2011-11-06 08:30:00+00', 'America/Los_Angeles')",
                &ctx
            )) == at(1_320_562_800)
        );
    }

    #[test]
    fn date_trunc_result_type_matches_source() {
        let infer = |sql: &str| {
            crate::eval::infer_type(
                &crabka_pgparser::parser::parse_expr_for_test(sql).expect("p"),
                &Scope::empty(),
            )
            .expect("inf")
        };
        assert_eq!(
            infer("date_trunc('month', TIMESTAMP '2024-07-15 00:00:00')"),
            ColumnType::Timestamp
        );
        // A date source resolves through `timestamptz`, so the result does too.
        assert2::assert!(
            infer("date_trunc('month', DATE '2024-07-15')") == ColumnType::Timestamptz
        );
        assert_eq!(
            infer("date_trunc('hour', INTERVAL '5 days 03:30:00')"),
            ColumnType::Interval
        );
    }

    #[test]
    fn age_two_arg_borrowing() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        let iv = |months: i32, days: i32, micros: i64| {
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months,
                days,
                micros,
            })
        };
        // Simple two-month difference.
        assert_eq!(
            ev(
                "age(TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')",
                &ctx
            ),
            iv(2, 0, 0)
        );
        // Borrow across a month boundary: 2024-03-01 minus 2024-01-15.
        // months = 3-1 = 2, days = 1-15 = -14 → borrow Feb (the month before March,
        // 2024 is a leap year so Feb=29): days = -14+29 = 15, months = 1.
        assert_eq!(
            ev(
                "age(TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-01-15 00:00:00')",
                &ctx
            ),
            iv(1, 15, 0)
        );
        // Time-of-day borrow into days.
        assert_eq!(
            ev(
                "age(TIMESTAMP '2024-03-02 06:00:00', TIMESTAMP '2024-03-01 18:00:00')",
                &ctx
            ),
            iv(0, 0, 12 * 3_600_000_000)
        );
    }

    #[test]
    fn age_one_arg_uses_transaction_date_midnight() {
        // ctx.now = 2024-03-15 → current_date midnight = 2024-03-15 00:00:00.
        let ctx = ctx_at("2024-03-15T10:00:00Z");
        // age(2024-01-15) = (2024-03-15 - 2024-01-15) = 2 months exactly.
        assert_eq!(
            ev("age(TIMESTAMP '2024-01-15 00:00:00')", &ctx),
            Datum::Interval(crabka_pgtypes::datetime::Interval {
                months: 2,
                days: 0,
                micros: 0
            })
        );
    }

    /// `PostgreSQL` 18.4, measured. `age` CARRIES a non-finite endpoint; it does
    /// not compute with it. Before this, the reserved sentinels went into the
    /// field arithmetic as ordinary years 9999 and −9999, so `age(timestamp
    /// 'infinity')` answered a finite interval measured from the transaction
    /// date — a wrong answer that also changed every day.
    ///
    /// The finite rows share the table on purpose: they run the same path and
    /// must not move.
    #[test]
    fn age_carries_a_non_finite_endpoint_rather_than_computing_with_it() {
        use assert2::assert;

        // A session zone OFF UTC is the interesting one for `timestamptz`: the
        // reserved instant renders as an ordinary wall-clock there, so a check
        // made after the coercion to civil time would see a finite value.
        for zone in [
            jiff::tz::TimeZone::UTC,
            jiff::tz::TimeZone::fixed(jiff::tz::Offset::constant(-8)),
            jiff::tz::TimeZone::fixed(jiff::tz::Offset::constant(11)),
        ] {
            let ctx = ctx_at_zone("2024-03-15T10:00:00Z", zone.clone());
            for (expr, expected) in [
                // One argument: `current_date − x`, so the sign flips.
                ("age(timestamp 'infinity')", "-infinity"),
                ("age(timestamp '-infinity')", "infinity"),
                ("age(timestamptz 'infinity')", "-infinity"),
                ("age(timestamptz '-infinity')", "infinity"),
                ("age(date 'infinity')", "-infinity"),
                ("age(date '-infinity')", "infinity"),
                // Two arguments: `end − start`, so `end`'s sign wins, and a
                // negated `-infinity` start is a `+infinity` contribution.
                (
                    "age(timestamp 'infinity', timestamp '-infinity')",
                    "infinity",
                ),
                (
                    "age(timestamp '-infinity', timestamp 'infinity')",
                    "-infinity",
                ),
                (
                    "age(timestamp 'infinity', timestamp '2000-01-01')",
                    "infinity",
                ),
                (
                    "age(timestamp '2000-01-01', timestamp 'infinity')",
                    "-infinity",
                ),
                (
                    "age(timestamp '2000-01-01', timestamp '-infinity')",
                    "infinity",
                ),
                (
                    "age(timestamptz 'infinity', timestamptz '2000-01-01')",
                    "infinity",
                ),
                ("age(date 'infinity', timestamp '2000-01-01')", "infinity"),
                // A NULL argument still wins over a non-finite one.
                ("age(timestamp 'infinity', NULL::timestamp)", "NULL"),
                ("age(NULL::timestamp, timestamp 'infinity')", "NULL"),
                // Finite endpoints, through the same path, unmoved.
                (
                    "age(TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')",
                    "2 mons",
                ),
                (
                    "age(TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-01-15 00:00:00')",
                    "1 mon 15 days",
                ),
                ("age(TIMESTAMP '2024-01-15 00:00:00')", "2 mons"),
            ] {
                let got = match ev(expr, &ctx) {
                    Datum::Null => "NULL".to_string(),
                    Datum::Interval(iv) => crabka_pgtypes::datetime::interval_to_text(iv),
                    other => panic!("{expr} gave {other:?}"),
                };
                assert!(got == expected, "{expr} in {zone:?}");
            }
        }
    }

    /// Two infinities of the SAME sign would have to cancel, which `PostgreSQL`
    /// refuses with 22008 rather than answering zero. Gres answered `@ 0`.
    #[test]
    fn age_of_two_like_signed_infinities_is_22008() {
        use assert2::assert;

        let ctx = ctx_at("2024-03-15T10:00:00Z");
        for expr in [
            "age(timestamp 'infinity', timestamp 'infinity')",
            "age(timestamp '-infinity', timestamp '-infinity')",
            "age(timestamptz 'infinity', timestamptz 'infinity')",
            "age(timestamptz '-infinity', timestamptz '-infinity')",
            "age(date 'infinity', date 'infinity')",
        ] {
            let error = crate::eval::eval(
                &crabka_pgparser::parser::parse_expr_for_test(expr).expect("parse"),
                &Scope::empty(),
                &[],
                &ctx,
            )
            .expect_err(expr)
            .into_pg();
            assert!(error.code == "22008", "{expr}");
            assert!(error.message == "interval out of range", "{expr}");
        }
    }

    /// A non-finite reading is the same reading in every zone, so `AT TIME ZONE`
    /// only switches its type. Rendering the reserved instant as a wall-clock
    /// used to lose the sentinel in any zone off UTC.
    #[test]
    fn a_non_finite_reading_survives_at_time_zone() {
        use assert2::assert;

        let ctx = ctx_at("2024-03-15T10:00:00Z");
        let civil_high = Datum::Timestamp(crabka_pgtypes::datetime::TIMESTAMP_INFINITY);
        let civil_low = Datum::Timestamp(crabka_pgtypes::datetime::TIMESTAMP_NEG_INFINITY);
        let zoned_high = Datum::Timestamptz(crabka_pgtypes::datetime::timestamptz_infinity());
        let zoned_low = Datum::Timestamptz(crabka_pgtypes::datetime::timestamptz_neg_infinity());
        for zone in ["UTC", "-08:00", "+11:00"] {
            for (expr, expected) in [
                (
                    format!("timestamp 'infinity' AT TIME ZONE '{zone}'"),
                    zoned_high.clone(),
                ),
                (
                    format!("timestamp '-infinity' AT TIME ZONE '{zone}'"),
                    zoned_low.clone(),
                ),
                (
                    format!("timestamptz 'infinity' AT TIME ZONE '{zone}'"),
                    civil_high.clone(),
                ),
                (
                    format!("timestamptz '-infinity' AT TIME ZONE '{zone}'"),
                    civil_low.clone(),
                ),
            ] {
                assert!(ev(&expr, &ctx) == expected, "{expr}");
            }
        }
    }

    #[test]
    fn timezone_function_both_directions() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        // timestamp → timestamptz: interpret 12:00 wall-clock in +05:00 → 07:00 UTC.
        // Verify by rendering back in UTC.
        let v = ev("timezone('UTC', TIMESTAMP '2024-01-15 12:00:00')", &ctx);
        assert_eq!(
            v,
            Datum::Timestamptz("2024-01-15T12:00:00Z".parse().expect("ts"))
        );
        // timestamptz → timestamp: render the instant in UTC as a wall-clock.
        assert_eq!(
            ev(
                "timezone('UTC', TIMESTAMPTZ '2024-01-15 12:00:00+00')",
                &ctx
            ),
            Datum::Timestamp(
                crabka_pgtypes::datetime::parse_timestamp("2024-01-15 12:00:00").expect("ts")
            )
        );
        // AT TIME ZONE lowers to timezone(zone, value) — same result.
        assert_eq!(
            ev("TIMESTAMP '2024-01-15 12:00:00' AT TIME ZONE 'UTC'", &ctx),
            Datum::Timestamptz("2024-01-15T12:00:00Z".parse().expect("ts"))
        );
    }

    #[test]
    fn timezone_unknown_zone_is_22023() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        let err = crate::eval::eval(
            &crabka_pgparser::parser::parse_expr_for_test(
                "timezone('Mars/Olympus', TIMESTAMP '2024-01-15 12:00:00')",
            )
            .expect("p"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect_err("unknown zone");
        assert_eq!(err.into_pg().code, "22023");
    }

    #[test]
    fn null_arguments_propagate() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        assert_eq!(ev("extract(year from null::timestamp)", &ctx), Datum::Null);
        assert_eq!(ev("date_part('hour', null::timestamp)", &ctx), Datum::Null);
        assert_eq!(ev("date_trunc('day', null::timestamp)", &ctx), Datum::Null);
        assert_eq!(ev("age(null::timestamp)", &ctx), Datum::Null);
        assert_eq!(ev("timezone('UTC', null::timestamp)", &ctx), Datum::Null);
    }

    #[test]
    fn result_types_for_row_description() {
        let infer = |sql: &str| {
            crate::eval::infer_type(
                &crabka_pgparser::parser::parse_expr_for_test(sql).expect("p"),
                &Scope::empty(),
            )
            .expect("inf")
        };
        assert_eq!(infer("now()"), ColumnType::Timestamptz);
        assert_eq!(infer("statement_timestamp()"), ColumnType::Timestamptz);
        assert_eq!(infer("clock_timestamp()"), ColumnType::Timestamptz);
        assert_eq!(infer("current_date"), ColumnType::Date);
        assert_eq!(infer("current_time"), ColumnType::Time);
        assert_eq!(infer("localtimestamp"), ColumnType::Timestamp);
        assert_eq!(
            infer("extract(year from TIMESTAMP '2024-01-01 00:00:00')"),
            ColumnType::Numeric(None)
        );
        assert_eq!(
            infer("date_part('year', TIMESTAMP '2024-01-01 00:00:00')"),
            ColumnType::Float8
        );
        assert_eq!(
            infer("age(TIMESTAMP '2024-01-01 00:00:00')"),
            ColumnType::Interval
        );
        assert_eq!(
            infer("timezone('UTC', TIMESTAMP '2024-01-01 00:00:00')"),
            ColumnType::Timestamptz
        );
        assert_eq!(
            infer("timezone('UTC', TIMESTAMPTZ '2024-01-01 00:00:00+00')"),
            ColumnType::Timestamp
        );
    }
}
