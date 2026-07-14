//! SP37: date/time *functions* — the clock family, `extract`/`date_part`,
//! `date_trunc`, `age`, and the `timezone` function (the `AT TIME ZONE` target).
//!
//! Mirrors `func.rs` (SP29 scalar functions): a `datetime_func(name)` registry, an
//! `is_datetime_func` dispatch predicate, an `eval_datetime` value evaluator, and a
//! `datetime_func_result_type` static result-type resolver. Like every breadth
//! slice since SP27, each function is a pure, deterministic transform over a single
//! row's already-evaluated Datums (the clock family reads the per-statement
//! `EvalCtx`), so there is no new lock / visibility rule / write path / interleaving
//! and thus no Stateright model — proven instead by the unit tests below + the
//! Task-14 wire test + the Task-15 conformance corpus diffed against PostgreSQL.
//!
//! Field math (extract/date_part/date_trunc) is done here in jiff; only value-pure,
//! reusable computations live in `crabka_pgtypes::datetime`.

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
    /// `now()` / `current_timestamp` / `transaction_timestamp()` — the transaction
    /// instant, as `timestamptz` (transaction-stable, like PostgreSQL).
    TransactionTimestamp,
    /// `statement_timestamp()` — the statement instant, as `timestamptz`.
    StatementTimestamp,
    /// `clock_timestamp()` — the real-time clock, as `timestamptz`.
    ClockTimestamp,
    /// `current_date` — the transaction date in the session zone.
    CurrentDate,
    /// `current_time` / `localtime` — the transaction time-of-day in the session zone.
    CurrentTime,
    /// `localtimestamp` — the transaction wall-clock as a (civil) `timestamp`.
    LocalTimestamp,
    /// `extract(field, source)` → `numeric` (PG 14+).
    Extract,
    /// `date_part(text, source)` → `float8` (the historical double-precision form).
    DatePart,
    /// `date_trunc(field, source [, tz])` — truncate to a unit.
    DateTrunc,
    /// `age(ts1, ts2)` / `age(ts1)` — a symbolic interval with month borrowing.
    Age,
    /// `timezone(zone, value)` — the `AT TIME ZONE` target.
    Timezone,
}

/// Classify a (lowercased — the lexer lowercases unquoted idents) function name.
/// `None` means "not a date/time function"; the caller then reports a misplaced
/// aggregate / undefined function.
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
        _ => return None,
    })
}

/// Is `name` a known date/time function? (The dispatch point in `eval`/`infer_type`.)
pub(crate) fn is_datetime_func(name: &str) -> bool {
    datetime_func(name).is_some()
}

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

/// The positional argument list. The clock family is niladic (`Exprs([])`) — `f(*)`
/// is never valid for a date/time function.
fn exprs_of(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    match &fc.args {
        FuncArgs::Exprs(v) => Ok(v),
        FuncArgs::Star => Err(undefined_function(&fc.name)),
    }
}

/// 22023 (`InvalidParameterValue`) for an unknown extract/date_part field or an
/// unknown `date_trunc`/`timezone` unit/zone — PostgreSQL's SQLSTATE for these.
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

/// Statically infer a date/time call's result type (for RowDescription).
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
                ColumnType::Date => ColumnType::Timestamp,
                t @ (ColumnType::Timestamp | ColumnType::Timestamptz | ColumnType::Interval) => t,
                // A non-temporal / unknown source surfaces the real error at eval.
                other => other,
            }
        }
        DtFunc::Age => {
            require_arity(fc, n == 1 || n == 2)?;
            ColumnType::Interval
        }
        // timezone(zone, value): timestamp → timestamptz, timestamptz → timestamp.
        DtFunc::Timezone => {
            require_arity(fc, n == 2)?;
            match crate::eval::infer_type(&args[1], scope)? {
                ColumnType::Timestamp => ColumnType::Timestamptz,
                ColumnType::Timestamptz => ColumnType::Timestamp,
                other => other,
            }
        }
    })
}

/// Evaluate a date/time call. `eval_child` evaluates each argument against the
/// current row (the same `eval` used for scalar context, or `agg::eval_grouped` in
/// a grouped context), so the math is shared and only the closure differs.
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
            Ok(Datum::Time(ctx.time_zone.to_datetime(ctx.now).time()))
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
            let v = extract_field(&field, &source, &ctx.time_zone)?;
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
            let v = extract_field(&field, &source, &ctx.time_zone)?;
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
            date_trunc(&unit, &source, zone.as_ref(), &ctx.time_zone)
        }
        // ---- age ----
        DtFunc::Age => {
            require_arity(fc, args.len() == 1 || args.len() == 2)?;
            let a = eval_child(&args[0])?;
            if a.is_null() {
                return Ok(Datum::Null);
            }
            // Two-arg: `age(end, start)` = end − start. One-arg: `age(ts)` =
            // current_date(at midnight, session zone) − ts (PG semantics).
            let (end, start) = match args.get(1) {
                Some(e) => {
                    let b = eval_child(e)?;
                    if b.is_null() {
                        return Ok(Datum::Null);
                    }
                    (
                        as_datetime(&a, &ctx.time_zone)?,
                        as_datetime(&b, &ctx.time_zone)?,
                    )
                }
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
            require_arity(fc, args.len() == 2)?;
            let zone_d = eval_child(&args[0])?; // zone FIRST (parser lowering)
            let value = eval_child(&args[1])?;
            if zone_d.is_null() || value.is_null() {
                return Ok(Datum::Null);
            }
            let tz = zone_arg(&zone_d)?;
            timezone_convert(&tz, &value)
        }
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

/// The `extract` field, which the parser lowered to a `StringLiteral` (already
/// lowercased). Anything else is a misuse → undefined function.
fn literal_field(e: &Expr) -> Result<String, ExecError> {
    match e {
        Expr::StringLiteral(s) => Ok(s.to_ascii_lowercase()),
        _ => Err(ExecError::UndefinedFunction(
            "extract requires a field name".into(),
        )),
    }
}

/// Resolve a zone-name text value to a jiff `TimeZone`. `UTC` and fixed-offset
/// spellings are handled by jiff's tzdb; an unknown zone is 22023.
fn zone_arg(d: &Datum) -> Result<TimeZone, ExecError> {
    let name = match d {
        Datum::Text(s) => s.as_str(),
        other => return Err(type_error("timezone", other)),
    };
    if name.eq_ignore_ascii_case("utc") {
        return Ok(TimeZone::UTC);
    }
    TimeZone::get(name).map_err(|_| invalid_param(format!("time zone \"{name}\" not recognized")))
}

/// Coerce a temporal Datum to a civil `DateTime` for `age` arithmetic. A
/// `timestamptz` is rendered in the session zone; a `date` promotes to midnight.
fn as_datetime(d: &Datum, tz: &TimeZone) -> Result<DateTime, ExecError> {
    match d {
        Datum::Timestamp(dt) => Ok(*dt),
        Datum::Date(dd) => Ok(crabka_pgtypes::datetime::date_to_midnight(*dd)),
        Datum::Timestamptz(ts) => Ok(tz.to_datetime(*ts)),
        other => Err(type_error("age", other)),
    }
}

// ---- extract / date_part field math ----

/// The Unix epoch as a civil datetime (for `epoch` of timestamp/date — "as if UTC").
fn unix_epoch_civil() -> DateTime {
    DateTime::constant(1970, 1, 1, 0, 0, 0, 0)
}

/// Compute one extract/date_part field of `source`, returned as a decimal STRING
/// (so the numeric form keeps full precision and the float form just parses it).
/// `tz` is the session zone, used only for the timezone-* fields of a timestamptz.
fn extract_field(field: &str, source: &Datum, tz: &TimeZone) -> Result<String, ExecError> {
    match source {
        Datum::Date(d) => {
            extract_from_datetime(field, crabka_pgtypes::datetime::date_to_midnight(*d), None)
        }
        Datum::Timestamp(dt) => extract_from_datetime(field, *dt, None),
        Datum::Timestamptz(ts) => {
            // Render the instant in the session zone; the offset enables timezone_*.
            let dt = tz.to_datetime(*ts);
            let off_secs = i64::from(tz.to_offset(*ts).seconds());
            // epoch for timestamptz is the ABSOLUTE instant (independent of zone).
            if field == "epoch" {
                return Ok(epoch_string_micros(ts.as_microsecond()));
            }
            extract_from_datetime(field, dt, Some(off_secs))
        }
        Datum::Interval(iv) => extract_from_interval(field, *iv),
        Datum::Time(t) => extract_from_time(field, *t),
        other => Err(type_error("extract", other)),
    }
}

/// Fields of a civil datetime. `tz_offset_secs` is `Some` only for a `timestamptz`
/// source (so the `timezone*` fields resolve; for a plain timestamp they are
/// undefined → 22023, matching PostgreSQL).
fn extract_from_datetime(
    field: &str,
    dt: DateTime,
    tz_offset_secs: Option<i64>,
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
        "timezone" => match tz_offset_secs {
            Some(secs) => int_str(secs),
            None => return Err(unknown_field(field)),
        },
        "timezone_hour" => match tz_offset_secs {
            Some(secs) => int_str(secs / 3600),
            None => return Err(unknown_field(field)),
        },
        "timezone_minute" => match tz_offset_secs {
            Some(secs) => int_str((secs % 3600) / 60),
            None => return Err(unknown_field(field)),
        },
        _ => return Err(unknown_field(field)),
    })
}

/// Fields of a bare `time` value (only the clock fields are defined).
fn extract_from_time(field: &str, t: Time) -> Result<String, ExecError> {
    Ok(match field {
        "hour" => int_str(i64::from(t.hour())),
        "minute" => int_str(i64::from(t.minute())),
        "second" => seconds_str(i64::from(t.second()), t.subsec_nanosecond()),
        "milliseconds" => millis_str(i64::from(t.second()), t.subsec_nanosecond()),
        "microseconds" => micros_str(i64::from(t.second()), t.subsec_nanosecond()),
        "epoch" => {
            let micros = i64::from(t.hour()) * 3_600_000_000
                + i64::from(t.minute()) * 60_000_000
                + i64::from(t.second()) * 1_000_000
                + i64::from(t.subsec_nanosecond() / 1_000);
            epoch_string_micros(micros)
        }
        _ => return Err(unknown_field(field)),
    })
}

/// Fields of an `interval`. PostgreSQL extracts the stored months/days/micros
/// fields directly (no normalization): year = months/12, month = months%12, etc.
fn extract_from_interval(field: &str, iv: Interval) -> Result<String, ExecError> {
    let months = i64::from(iv.months);
    let secs_whole = iv.micros / 1_000_000;
    let subsec = (iv.micros % 1_000_000) as i32; // microseconds within the second
    Ok(match field {
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
            let total_micros = (months * 30 + i64::from(iv.days)) * 86_400_000_000 + iv.micros;
            epoch_string_micros(total_micros)
        }
        _ => return Err(unknown_field(field)),
    })
}

fn unknown_field(field: &str) -> ExecError {
    invalid_param(format!("unit \"{field}\" not recognized"))
}

/// century containing AD year `y` (PG: 2000 is in century 20, 2001 starts century 21).
fn century_of(y: i64) -> i64 {
    if y > 0 {
        (y + 99) / 100
    } else {
        // BC handling (PG: year 0 doesn't exist) — floor division.
        (y - 99) / 100
    }
}

/// millennium containing AD year `y` (PG: 2000 is millennium 2, 2001 starts 3).
fn millennium_of(y: i64) -> i64 {
    if y > 0 {
        (y + 999) / 1000
    } else {
        (y - 999) / 1000
    }
}

fn int_str(n: i64) -> String {
    n.to_string()
}

/// `second` (and the interval/time second) as a decimal: whole seconds plus the
/// microsecond fraction, trailing zeros trimmed (`6` / `6.5` / `6.123456`).
fn seconds_str(secs: i64, subsec_nanos: i32) -> String {
    let micros = (subsec_nanos / 1_000).unsigned_abs() as u64;
    if micros == 0 {
        return secs.to_string();
    }
    let mut frac = format!("{micros:06}");
    while frac.ends_with('0') {
        frac.pop();
    }
    format!("{secs}.{frac}")
}

/// `milliseconds` = whole_seconds*1000 + subsecond-as-millis (3 implied decimals
/// of microsecond precision), trailing zeros trimmed.
fn millis_str(secs: i64, subsec_nanos: i32) -> String {
    let total_micros = secs * 1_000_000 + i64::from(subsec_nanos / 1_000);
    decimal_micros_scaled(total_micros, 1_000)
}

/// `microseconds` = whole_seconds*1_000_000 + subsecond-micros (an integer).
fn micros_str(secs: i64, subsec_nanos: i32) -> String {
    let total_micros = secs * 1_000_000 + i64::from(subsec_nanos / 1_000);
    total_micros.to_string()
}

/// Render `total_micros / divisor` as an exact decimal string (for `milliseconds`,
/// divisor 1000 → 3 fractional digits, trailing zeros trimmed).
fn decimal_micros_scaled(total_micros: i64, divisor: i64) -> String {
    if total_micros % divisor == 0 {
        return (total_micros / divisor).to_string();
    }
    let neg = total_micros < 0;
    let abs = total_micros.unsigned_abs();
    let whole = abs / divisor as u64;
    let frac = abs % divisor as u64;
    // divisor 1000 → 3 digits.
    let width = (divisor as f64).log10().round() as usize;
    let mut frac_s = format!("{frac:0width$}");
    while frac_s.ends_with('0') {
        frac_s.pop();
    }
    format!("{}{whole}.{frac_s}", if neg { "-" } else { "" })
}

/// `epoch` = seconds, from a signed microsecond count. Renders the exact decimal
/// (six fractional digits, trailing zeros trimmed) so sub-second epochs are exact.
fn epoch_string_micros(total_micros: i64) -> String {
    decimal_micros_scaled(total_micros, 1_000_000)
}

// ---- date_trunc ----

/// Truncate `source` to `unit`. timestamp/date(→timestamp)/interval truncate
/// directly; timestamptz truncates in `zone` (3-arg) or `session_tz` (2-arg) and
/// returns a timestamptz.
fn date_trunc(
    unit: &str,
    source: &Datum,
    zone: Option<&TimeZone>,
    session_tz: &TimeZone,
) -> Result<Datum, ExecError> {
    match source {
        Datum::Timestamp(dt) => Ok(Datum::Timestamp(trunc_datetime(unit, *dt)?)),
        Datum::Date(d) => Ok(Datum::Timestamp(trunc_datetime(
            unit,
            crabka_pgtypes::datetime::date_to_midnight(*d),
        )?)),
        Datum::Timestamptz(ts) => {
            let tz = zone.unwrap_or(session_tz);
            // Render in the zone, truncate the wall-clock, re-interpret in the zone.
            let dt = tz.to_datetime(*ts);
            let truncated = trunc_datetime(unit, dt)?;
            truncated
                .to_zoned(tz.clone())
                .map(|z| Datum::Timestamptz(z.timestamp()))
                .map_err(|_| invalid_param("timestamp out of range for date_trunc"))
        }
        Datum::Interval(iv) => Ok(Datum::Interval(trunc_interval(unit, *iv)?)),
        other => Err(type_error("date_trunc", other)),
    }
}

/// Zero out every field below `unit` in a civil datetime.
fn trunc_datetime(unit: &str, dt: DateTime) -> Result<DateTime, ExecError> {
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
        _ => return Err(unknown_field(unit)),
    })
}

/// Truncate an interval to `unit` (zero out finer fields, on the stored fields).
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
        _ => return Err(unknown_field(unit)),
    })
}

// ---- age ----

/// PostgreSQL `age(end, start)`: a SYMBOLIC interval computed by subtracting each
/// field and borrowing from the next-larger field (using the actual days in the
/// borrowed month) when a smaller field goes negative. Returns months/days/micros.
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
        days += i64::from(days_in_month(by, bm));
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

/// Days in the given month (1..=12), leap-aware via jiff.
fn days_in_month(year: i16, month: i8) -> i8 {
    Date::new(year, month, 1)
        .expect("a valid first-of-month date")
        .days_in_month()
}

// ---- timezone (AT TIME ZONE) ----

/// `timezone(zone, value)`:
///   * `timestamp` → `timestamptz`: interpret the wall-clock as local to `zone`.
///   * `timestamptz` → `timestamp`: render the instant in `zone` (wall-clock).
fn timezone_convert(tz: &TimeZone, value: &Datum) -> Result<Datum, ExecError> {
    match value {
        Datum::Timestamp(dt) => dt
            .to_zoned(tz.clone())
            .map(|z| Datum::Timestamptz(z.timestamp()))
            .map_err(|_| invalid_param("timestamp out of range for time zone conversion")),
        Datum::Timestamptz(ts) => Ok(Datum::Timestamp(tz.to_datetime(*ts))),
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
        let now: jiff::Timestamp = rfc3339.parse().expect("ts");
        EvalCtx {
            now,
            stmt_now: now,
            time_zone: jiff::tz::TimeZone::UTC,
            current_user: "public".into(),
            session_user: "public".into(),
            clock: Arc::new(FixedClock(now)),
            sequence: None,
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
            current_user: "public".into(),
            session_user: "public".into(),
            clock: Arc::new(FixedClock(ny)),
            sequence: None,
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
            current_user: "public".into(),
            session_user: "public".into(),
            clock: Arc::new(FixedClock(ts)),
            sequence: None,
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
    fn extract_unknown_field_is_22023() {
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
        // a plain timestamp has no timezone field → 22023.
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
        assert_eq!(err2.into_pg().code, "22023");
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
        // date source promotes to timestamp.
        assert_eq!(
            ev("date_trunc('month', DATE '2024-07-15')", &ctx),
            ts("2024-07-01 00:00:00")
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
        // date source → timestamp.
        assert_eq!(
            infer("date_trunc('month', DATE '2024-07-15')"),
            ColumnType::Timestamp
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
