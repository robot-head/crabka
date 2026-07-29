//! SP37: date/time *values* — the `Interval` type plus parsing, formatting,
//! binary encodings, and value arithmetic. PostgreSQL semantics; `jiff` does the
//! calendar/timezone math. This is the single source of truth for date/time
//! values (the SP32 `numeric` module pattern).

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible date/time semantics kept structurally close to donor"
)]

use jiff::{
    Timestamp, ToSpan,
    civil::{Date, DateTime, Time},
    tz::{Offset, TimeZone},
};

use crate::TypeError;

// ---------------------------------------------------------------------------
// Value-level arithmetic helpers (called from crabka_pgtypes::ops)
// ---------------------------------------------------------------------------

/// Add two intervals field-wise with overflow checking.
pub fn add_interval(a: Interval, b: Interval) -> Result<Interval, TypeError> {
    let months = a.months.checked_add(b.months).ok_or(TypeError::Overflow)?;
    let days = a.days.checked_add(b.days).ok_or(TypeError::Overflow)?;
    let micros = a.micros.checked_add(b.micros).ok_or(TypeError::Overflow)?;
    Ok(Interval {
        months,
        days,
        micros,
    })
}

/// Subtract two intervals field-wise with overflow checking.
pub fn sub_interval(a: Interval, b: Interval) -> Result<Interval, TypeError> {
    let neg = neg_interval(b)?;
    add_interval(a, neg)
}

/// Negate an interval field-wise with overflow checking.
pub fn neg_interval(a: Interval) -> Result<Interval, TypeError> {
    let months = a.months.checked_neg().ok_or(TypeError::Overflow)?;
    let days = a.days.checked_neg().ok_or(TypeError::Overflow)?;
    let micros = a.micros.checked_neg().ok_or(TypeError::Overflow)?;
    Ok(Interval {
        months,
        days,
        micros,
    })
}

/// Multiply an interval by a scalar factor. PostgreSQL distributes the factor
/// over each field and spills any fractional months into days (30-day month)
/// and fractional days into microseconds (86400000000 µs/day), matching PG's
/// `interval_mul` behaviour.
pub fn mul_interval(a: Interval, factor: f64) -> Result<Interval, TypeError> {
    if !factor.is_finite() {
        return Err(TypeError::Overflow);
    }
    // Scale months; carry the fraction down to days.
    let months_f = f64::from(a.months) * factor;
    let months_whole = months_f.trunc();
    let months_frac = months_f.fract();
    let months = months_whole as i64;

    // Spill fractional months → days (PG uses 30-day month).
    let days_from_months = months_frac * 30.0;
    let days_f = f64::from(a.days) * factor + days_from_months;
    let days_whole = days_f.trunc();
    let days_frac = days_f.fract();
    let days = days_whole as i64;

    // Spill fractional days → micros.
    let micros_from_days = days_frac * USECS_PER_DAY_I64 as f64;
    let micros_f = a.micros as f64 * factor + micros_from_days;

    // Range-check the fields (interval fields are i32/i64).
    let months = i32::try_from(months).map_err(|_| TypeError::Overflow)?;
    let days = i32::try_from(days).map_err(|_| TypeError::Overflow)?;
    // Guard micros: a finite f64 larger than i64::MAX (= 2^63 = 9.22e18) would
    // silently saturate to i64::MAX on `as i64` cast; reject it explicitly.
    // `i64::MAX as f64` rounds up to exactly 2^63, so `>= 2^63` is the right bound.
    if !micros_f.is_finite() || micros_f.abs() >= 9_223_372_036_854_775_808.0_f64 {
        return Err(TypeError::Overflow);
    }
    let micros = micros_f.round() as i64;
    Ok(Interval {
        months,
        days,
        micros,
    })
}

/// Divide an interval by a scalar divisor (zero → 22012).
pub fn div_interval(a: Interval, divisor: f64) -> Result<Interval, TypeError> {
    if divisor == 0.0 {
        return Err(TypeError::DivisionByZero);
    }
    mul_interval(a, 1.0 / divisor)
}

/// Add `days` to a `Date`, returning the new `Date` (overflow → 22008).
pub fn date_plus_days(d: Date, days: i64) -> Result<Date, TypeError> {
    d.checked_add(days.days())
        .map_err(|_| TypeError::DatetimeFieldOverflow {
            value: days.to_string(),
        })
}

/// Subtract two dates, returning the number of days between them (a - b).
pub fn date_diff_days(a: Date, b: Date) -> i32 {
    a.since((jiff::Unit::Day, b))
        .map(|span| span.get_days())
        .expect("difference of in-range date values always fits in a Span")
}

/// Promote a `Date` to a civil `DateTime` at midnight.
pub fn date_to_midnight(d: Date) -> DateTime {
    d.to_datetime(Time::midnight())
}

/// Add an `Interval` to a `Date` (PG: promotes date→midnight timestamp first),
/// returning a `DateTime`.  Applies months, then days, then micros in order
/// (calendar-aware via jiff `Span`).
pub fn date_plus_interval(d: Date, iv: Interval) -> Result<DateTime, TypeError> {
    timestamp_plus_interval(date_to_midnight(d), iv)
}

/// Add an `Interval` to a `DateTime`. Applies months, days, and micros in
/// sequence so that `+1 month` lands on the correct calendar date and only then
/// the time offset is applied.
pub fn timestamp_plus_interval(ts: DateTime, iv: Interval) -> Result<DateTime, TypeError> {
    let overflow = |_| TypeError::DatetimeFieldOverflow {
        value: "interval arithmetic".into(),
    };
    // Apply months (calendar-aware, e.g. Jan 31 + 1 month → Feb 28/29).
    let after_months = if iv.months != 0 {
        ts.checked_add(iv.months.months()).map_err(overflow)?
    } else {
        ts
    };
    // Apply days (calendar-aware, skips DST ambiguity for civil datetimes).
    let after_days = if iv.days != 0 {
        after_months.checked_add(iv.days.days()).map_err(overflow)?
    } else {
        after_months
    };
    // Apply microseconds.
    let result = if iv.micros != 0 {
        after_days
            .checked_add(iv.micros.microseconds())
            .map_err(overflow)?
    } else {
        after_days
    };
    Ok(result)
}

/// Compute `a - b` for two `DateTime` values, returning an `Interval` with
/// months = 0 (PG's `timestamp - timestamp` result: total micros, stored in
/// the days + micros fields — days for full 86400 µs days, remainder in micros).
pub fn timestamp_diff(a: DateTime, b: DateTime) -> Interval {
    let total_micros = a
        .since((jiff::Unit::Microsecond, b))
        .map(|span| span.get_microseconds())
        .expect("difference of in-range timestamp values always fits in a Span");
    // Split into whole days + remaining micros (matching PG's interval storage).
    let days = (total_micros / USECS_PER_DAY_I64) as i32;
    let micros = total_micros % USECS_PER_DAY_I64;
    Interval {
        months: 0,
        days,
        micros,
    }
}

/// Add an `Interval` to a `Time`, returning the new `Time`. PostgreSQL's
/// `time + interval` uses ONLY the interval's microseconds component — a `time`
/// has no date, so the interval's `months`/`days` are ignored — and wraps the
/// result modulo 24 h (`time '23:00' + interval '2 hours'` → `01:00:00`,
/// `time '12:00' + interval '1 day'` → `12:00:00`).
pub fn time_plus_interval(t: Time, iv: Interval) -> Time {
    // Micros-of-day of the input time.
    let base = i64::from(t.hour()) * 3_600_000_000
        + i64::from(t.minute()) * 60_000_000
        + i64::from(t.second()) * 1_000_000
        + i64::from(t.subsec_nanosecond() / 1_000);
    // Add the interval micros and wrap into [0, 86_400_000_000) (the `.rem_euclid`
    // keeps a negative shift positive, so `time '00:30' - interval '1 hour'`
    // wraps to `23:30:00`).
    let micros = (base + iv.micros).rem_euclid(USECS_PER_DAY_I64);
    let hour = (micros / 3_600_000_000) as i8;
    let rem = micros % 3_600_000_000;
    let minute = (rem / 60_000_000) as i8;
    let rem = rem % 60_000_000;
    let second = (rem / 1_000_000) as i8;
    let nanos = ((rem % 1_000_000) * 1_000) as i32;
    Time::new(hour, minute, second, nanos)
        .expect("a micros-of-day in [0, 86_400_000_000) is always a valid Time")
}

/// Combine a `Date` and a `Time` into a `DateTime` (PostgreSQL's `date + time`
/// and `time + date` → `timestamp`).
pub fn combine_date_time(d: Date, t: Time) -> DateTime {
    d.to_datetime(t)
}

/// Add an `Interval` to a `timestamptz` instant, calendar-aware in `tz`. The
/// months and days are applied to the WALL-CLOCK time in the session zone (so a
/// `+1 day` across a DST boundary lands on the same wall-clock time the next day,
/// not exactly 24 h later), while the microseconds are an absolute (instant)
/// shift. This is tz-aware, so it lives here (used from the executor's
/// `apply_binary`, which has the session zone) rather than in `crabka_pgtypes::ops`.
pub fn timestamptz_plus_interval(
    ts: Timestamp,
    iv: Interval,
    tz: &TimeZone,
) -> Result<Timestamp, TypeError> {
    let overflow = |_| TypeError::DatetimeFieldOverflow {
        value: "interval arithmetic".into(),
    };
    // Apply the calendar (months, then days) to the zoned wall-clock time.
    let zoned = ts.to_zoned(tz.clone());
    let after_cal = if iv.months != 0 || iv.days != 0 {
        zoned
            .checked_add(iv.months.months())
            .and_then(|z| z.checked_add(iv.days.days()))
            .map_err(overflow)?
    } else {
        zoned
    };
    // Apply the microseconds as an absolute (instant) shift.
    let after_micros = if iv.micros != 0 {
        after_cal
            .checked_add(iv.micros.microseconds())
            .map_err(overflow)?
    } else {
        after_cal
    };
    Ok(after_micros.timestamp())
}

/// Compute `a - b` for two `timestamptz` instants, returning an `Interval` of pure
/// micros (split into whole days + remainder, matching PG's interval storage). The
/// subtraction is on absolute instants, so no time zone is needed.
pub fn timestamptz_diff(a: Timestamp, b: Timestamp) -> Interval {
    let total_micros = a.as_microsecond() - b.as_microsecond();
    let days = (total_micros / USECS_PER_DAY_I64) as i32;
    let micros = total_micros % USECS_PER_DAY_I64;
    Interval {
        months: 0,
        days,
        micros,
    }
}

/// A PostgreSQL `interval`: months, days, and microseconds kept SEPARATE (PG does
/// not fold `1 month` into `30 days` for storage/arithmetic — only for ordering).
#[derive(Debug, Clone, Copy)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

const USECS_PER_DAY: i128 = 86_400_000_000;

impl Interval {
    /// PostgreSQL's `interval_cmp` canonical value: a 30-day month and 24-hour
    /// day estimate, in microseconds, as `i128` to avoid overflow.
    pub fn canonical_micros(&self) -> i128 {
        (i128::from(self.months) * 30 + i128::from(self.days)) * USECS_PER_DAY
            + i128::from(self.micros)
    }
}

impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        self.canonical_micros() == other.canonical_micros()
    }
}
impl Eq for Interval {}
impl std::hash::Hash for Interval {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.canonical_micros().hash(state);
    }
}
impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Interval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.canonical_micros().cmp(&other.canonical_micros())
    }
}

// ---------------------------------------------------------------------------
// Epoch constants. PostgreSQL stores `date`/`timestamp` relative to 2000-01-01
// (the PostgreSQL epoch), NOT the Unix epoch. `timestamptz` is an absolute
// instant; its binary wire form is µs since the PG epoch in UTC.
// ---------------------------------------------------------------------------

/// The PostgreSQL epoch (`2000-01-01`) as a `jiff` civil date.
fn pg_epoch_date() -> Date {
    Date::constant(2000, 1, 1)
}

/// The PostgreSQL epoch (`2000-01-01 00:00:00`) as a civil datetime.
fn pg_epoch_datetime() -> DateTime {
    DateTime::constant(2000, 1, 1, 0, 0, 0, 0)
}

/// Seconds from the Unix epoch (1970-01-01) to the PostgreSQL epoch (2000-01-01).
const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;

/// Microseconds in one calendar day (24h estimate — civil days are always 24h).
const USECS_PER_DAY_I64: i64 = 86_400_000_000;

// ---------------------------------------------------------------------------
// Sub-second rendering. PostgreSQL prints the fractional seconds only when
// non-zero, trimming trailing zeros (so `.450000` → `.45`, `.500000` → `.5`).
// jiff exposes the sub-second component as nanoseconds; PG's resolution is
// microseconds, so we render up to six fractional digits.
// ---------------------------------------------------------------------------

/// Append `.ffffff` (trailing zeros trimmed) for a non-zero sub-second part.
/// `subsec_nanos` is the time's nanosecond-of-second. Values reach here already
/// quantized to µs, so dividing the tail away is exact rather than lossy.
fn push_subsecond(out: &mut String, subsec_nanos: i32) {
    let micros = subsec_nanos / 1_000;
    if micros == 0 {
        return;
    }
    // Six zero-padded digits, then strip trailing zeros (always leaves ≥1).
    let mut frac = format!("{micros:06}");
    while frac.ends_with('0') {
        frac.pop();
    }
    out.push('.');
    out.push_str(&frac);
}

// ---------------------------------------------------------------------------
// date
// ---------------------------------------------------------------------------

/// Parse a `date` literal in `YYYY-MM-DD` form. A well-formed shape whose fields
/// jiff rejects (e.g. `2023-02-29`) is a field overflow (22008); anything that
/// does not even look like a date is a format error (22007).
pub fn parse_date(s: &str) -> Result<Date, TypeError> {
    let t = s.trim();
    // Shape check: exactly three `-`-separated all-digit fields (optionally a
    // leading `-` for a BC-ish negative year is NOT supported here — PG uses an
    // `AD`/`BC` suffix; out of scope for this slice).
    let parts: Vec<&str> = t.split('-').collect();
    let well_shaped = parts.len() == 3
        && !parts[0].is_empty()
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    match t.parse::<Date>() {
        Ok(d) => Ok(d),
        Err(_) if well_shaped => Err(TypeError::DatetimeFieldOverflow {
            value: s.to_string(),
        }),
        Err(_) => Err(TypeError::InvalidDatetimeFormat {
            type_name: "date",
            value: s.to_string(),
        }),
    }
}

/// Render a `date` as ISO `YYYY-MM-DD` (PostgreSQL `date_out`, ISO datestyle).
pub fn date_to_text(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year(), d.month(), d.day())
}

/// `date_send`: i32 big-endian days since the PostgreSQL epoch (2000-01-01).
pub fn date_to_binary(d: Date) -> [u8; 4] {
    // `since` with largest unit Day yields a Span carrying only `days`.
    let days = d
        .since((jiff::Unit::Day, pg_epoch_date()))
        .map(|span| span.get_days())
        .expect("difference from a valid date to the PG epoch always fits");
    days.to_be_bytes()
}

/// `date_recv`: i32 big-endian days since the PostgreSQL epoch.
pub fn date_from_binary(b: &[u8]) -> Result<Date, TypeError> {
    let arr: [u8; 4] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "date",
        value: format!("{b:?}"),
    })?;
    let days = i64::from(i32::from_be_bytes(arr));
    // Route through a non-panicking `Timestamp` — `ToSpan::days()` PANICS when the
    // value is outside jiff's Span range, and these bytes are arbitrary (storage /
    // fuzz). An i32 day count · 86_400 + the epoch offset always fits i64, so the
    // only failure is an out-of-range instant, reported as 22008.
    let unix_secs = days * 86_400 + PG_EPOCH_UNIX_SECS;
    Timestamp::from_second(unix_secs)
        .map(|ts| ts.to_zoned(jiff::tz::TimeZone::UTC).date())
        .map_err(|_| TypeError::DatetimeFieldOverflow {
            value: days.to_string(),
        })
}

// ---------------------------------------------------------------------------
// time without time zone
// ---------------------------------------------------------------------------

/// Why a civil date/time literal could not be turned into a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClockParseError {
    /// The text is not a well-formed literal.
    Syntax,
    /// The text is well formed, but quantizing it to microseconds rounded it
    /// past the largest value crabka can represent.
    Overflow,
}

impl ClockParseError {
    /// Label this failure with the SQL type and literal that produced it.
    fn into_type_error(self, type_name: &'static str, value: &str) -> TypeError {
        match self {
            Self::Syntax => TypeError::InvalidDatetimeFormat {
                type_name,
                value: value.to_string(),
            },
            Self::Overflow => TypeError::DatetimeFieldOverflow {
                value: value.to_string(),
            },
        }
    }
}

/// Parse a `time` literal in `HH:MM[:SS[.ffffff]]` form.
pub fn parse_time(s: &str) -> Result<Time, TypeError> {
    let t = s.trim();
    let parsed = parse_time_inner(t).ok_or_else(|| TypeError::InvalidDatetimeFormat {
        type_name: "time without time zone",
        value: s.to_string(),
    })?;
    if parsed.day_carry {
        // PostgreSQL's `time` domain is closed at `24:00:00`, so it renders
        // `23:59:59.9999995` as `24:00:00`. jiff's `Time` has no such value —
        // it rejects the literal `24:00:00` for the same reason — and midnight
        // is a different time, not a rounding of this one, so this fails
        // instead of wrapping.
        return Err(TypeError::DatetimeFieldOverflow {
            value: s.to_string(),
        });
    }
    Ok(parsed.time)
}

/// A clock reading quantized to PostgreSQL's microsecond resolution, plus the
/// day carry that rounding the sub-microsecond tail may have produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoundedTime {
    /// The quantized reading. `00:00:00` whenever `day_carry` is set.
    time: Time,
    /// Rounding pushed the value past `23:59:59.999999` into the next day.
    day_carry: bool,
}

/// Best-effort `HH:MM[:SS[.ffffff]]` parse (returns `None` on any malformation).
/// jiff's `Time` FromStr requires `HH:MM:SS`, so we normalize `HH:MM` ourselves.
///
/// This is the single ingestion point for `time`, `timestamp` and `timestamptz`
/// text, so it is also where the value is quantized to PostgreSQL's microsecond
/// resolution (see [`round_fraction_to_micros`]). Every crabka encoding —
/// `time_send`, `timestamp_send`, the row encoder, index keys — carries
/// microseconds and nothing finer, so a value parsed at jiff's nanosecond
/// resolution would (a) not survive a storage round trip and (b) compare UNEQUAL
/// to its stored form while encoding to byte-identical storage, which an index,
/// being equality-by-bytes, would silently conflate.
fn parse_time_inner(t: &str) -> Option<RoundedTime> {
    let mut fields = t.split(':');
    let hours = fields.next()?;
    let minutes = fields.next()?;
    let seconds = fields.next();
    if fields.next().is_some() {
        return None;
    }

    // The fraction is handled here rather than by jiff: jiff stops at nine
    // digits and would round to nanoseconds first, so rounding to microseconds
    // afterwards would round twice. Only the seconds field may carry one —
    // PostgreSQL reads `12:34.5` as `MM:SS.f`, a form crabka does not accept,
    // so a fraction on any other field stays the syntax error it is today.
    if hours.contains('.') || minutes.contains('.') {
        return None;
    }
    let (seconds, fraction) = match seconds {
        Some(seconds) => match seconds.split_once('.') {
            Some((seconds, fraction)) => (seconds, Some(fraction)),
            None => (seconds, None),
        },
        // jiff parses `HH:MM:SS`; supply `:00` seconds when omitted.
        None => ("00", None),
    };
    let extra_micros = match fraction {
        Some(fraction) => round_fraction_to_micros(fraction)?,
        None => 0,
    };

    // jiff validates the whole-number fields. It never sees a sub-second one, so
    // `whole` always lands exactly on a second and the only carry that can leave
    // the day is the one `extra_micros` contributes.
    let whole = format!("{hours}:{minutes}:{seconds}")
        .parse::<Time>()
        .ok()?;
    let micros_of_day = i64::from(whole.hour()) * 3_600_000_000
        + i64::from(whole.minute()) * 60_000_000
        + i64::from(whole.second()) * 1_000_000
        + i64::from(extra_micros);
    let day_carry = micros_of_day >= MICROS_PER_DAY;
    Some(RoundedTime {
        time: time_from_micros_of_day(micros_of_day - i64::from(day_carry) * MICROS_PER_DAY),
        day_carry,
    })
}

/// Microseconds in one calendar day, the modulus of a clock reading.
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Rebuild a clock reading from microseconds since midnight.
///
/// # Panics
///
/// Panics unless `micros` is in `0..MICROS_PER_DAY`.
fn time_from_micros_of_day(mut micros: i64) -> Time {
    let hour = (micros / 3_600_000_000) as i8;
    micros %= 3_600_000_000;
    let minute = (micros / 60_000_000) as i8;
    micros %= 60_000_000;
    let second = (micros / 1_000_000) as i8;
    micros %= 1_000_000;
    Time::new(hour, minute, second, (micros * 1_000) as i32)
        .expect("microseconds within a day are a valid clock reading")
}

/// Round a fractional-seconds digit string to PostgreSQL's microsecond
/// resolution.
///
/// PostgreSQL reads the fraction as a `double` and applies `rint`, which rounds
/// half to even: `time '00:00:00.0000005'` is `00:00:00` while
/// `time '00:00:00.0000015'` is `00:00:00.000002`. The same rule is applied here
/// in decimal, so it holds for fractions of any length instead of only those a
/// `double` happens to represent exactly.
///
/// Returns microseconds in `0..=1_000_000`, the top value being a carry of one
/// whole second that the caller must apply. `None` when the text is not a
/// non-empty run of ASCII digits.
fn round_fraction_to_micros(digits: &str) -> Option<u32> {
    let digits = digits.as_bytes();
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }

    // The fraction is left-aligned, so short inputs are padded, not shifted.
    let (kept, discarded) = digits.split_at(digits.len().min(6));
    let mut micros: u32 = 0;
    for digit in kept {
        micros = micros * 10 + u32::from(digit - b'0');
    }
    for _ in kept.len()..6 {
        micros *= 10;
    }

    let round_up = match discarded.split_first() {
        None | Some((b'0'..=b'4', _)) => false,
        // Exactly one half goes to the even neighbour; anything beyond it goes up.
        Some((b'5', rest)) => rest.iter().any(|digit| *digit != b'0') || micros % 2 == 1,
        Some(_) => true,
    };
    Some(micros + u32::from(round_up))
}

/// Render a `time` as `HH:MM:SS[.ffffff]` (PostgreSQL `time_out`).
pub fn time_to_text(t: Time) -> String {
    let mut out = format!("{:02}:{:02}:{:02}", t.hour(), t.minute(), t.second());
    push_subsecond(&mut out, t.subsec_nanosecond());
    out
}

/// `time_send`: i64 big-endian microseconds since midnight.
pub fn time_to_binary(t: Time) -> [u8; 8] {
    let micros = i64::from(t.hour()) * 3_600_000_000
        + i64::from(t.minute()) * 60_000_000
        + i64::from(t.second()) * 1_000_000
        + i64::from(t.subsec_nanosecond() / 1_000);
    micros.to_be_bytes()
}

/// `time_recv`: i64 big-endian microseconds since midnight.
pub fn time_from_binary(b: &[u8]) -> Result<Time, TypeError> {
    let arr: [u8; 8] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "time without time zone",
        value: format!("{b:?}"),
    })?;
    let mut micros = i64::from_be_bytes(arr);
    let hour = (micros / 3_600_000_000) as i8;
    micros %= 3_600_000_000;
    let minute = (micros / 60_000_000) as i8;
    micros %= 60_000_000;
    let second = (micros / 1_000_000) as i8;
    micros %= 1_000_000;
    let nanos = (micros * 1_000) as i32;
    Time::new(hour, minute, second, nanos).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: i64::from_be_bytes(arr).to_string(),
    })
}

// ---------------------------------------------------------------------------
// timestamp without time zone
// ---------------------------------------------------------------------------

/// Parse a `timestamp` literal: `YYYY-MM-DD{ |T}HH:MM[:SS[.ffffff]]`. jiff accepts
/// a space or `T`/`t` separator natively, but requires `HH:MM:SS` for the time,
/// so we split on the separator and reuse the `time` normalization.
pub fn parse_timestamp(s: &str) -> Result<DateTime, TypeError> {
    let t = s.trim();
    parse_timestamp_inner(t).map_err(|err| err.into_type_error("timestamp without time zone", s))
}

/// Split a civil datetime into its date and time around a space/`T` separator,
/// parsing each part (normalizing the time's optional seconds).
fn parse_timestamp_inner(t: &str) -> Result<DateTime, ClockParseError> {
    // Find the date/time separator: the first ` `, `T`, or `t`.
    let sep = t.find([' ', 'T', 't']).ok_or(ClockParseError::Syntax)?;
    let date_part = &t[..sep];
    let time_part = &t[sep + 1..];
    let date = date_part
        .parse::<Date>()
        .map_err(|_| ClockParseError::Syntax)?;
    let RoundedTime { time, day_carry } =
        parse_time_inner(time_part).ok_or(ClockParseError::Syntax)?;
    let date = if day_carry {
        // Rounding crossed midnight, so the day owes a carry. PostgreSQL's
        // timestamp range runs to 294276 AD and simply carries; jiff's `Date`
        // stops at 9999-12-31, where there is no next day and the value must
        // fail rather than wrap back to the start of the one it came from.
        date.tomorrow().map_err(|_| ClockParseError::Overflow)?
    } else {
        date
    };
    Ok(date.to_datetime(time))
}

/// Render a `timestamp` as `YYYY-MM-DD HH:MM:SS[.ffffff]` (SPACE separator —
/// PostgreSQL `timestamp_out`, ISO datestyle).
pub fn timestamp_to_text(ts: DateTime) -> String {
    let d = ts.date();
    let tm = ts.time();
    let mut out = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        d.year(),
        d.month(),
        d.day(),
        tm.hour(),
        tm.minute(),
        tm.second()
    );
    push_subsecond(&mut out, tm.subsec_nanosecond());
    out
}

/// `timestamp_send`: i64 big-endian microseconds since the PG epoch.
pub fn timestamp_to_binary(ts: DateTime) -> [u8; 8] {
    let micros = ts
        .since((jiff::Unit::Microsecond, pg_epoch_datetime()))
        .map(|span| span.get_microseconds())
        .expect("difference from a valid timestamp to the PG epoch always fits");
    micros.to_be_bytes()
}

/// `timestamp_recv`: i64 big-endian microseconds since the PG epoch.
pub fn timestamp_from_binary(b: &[u8]) -> Result<DateTime, TypeError> {
    let arr: [u8; 8] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "timestamp without time zone",
        value: format!("{b:?}"),
    })?;
    let pg_micros = i64::from_be_bytes(arr);
    // Route through a non-panicking UTC `Timestamp` — `ToSpan::microseconds()`
    // PANICS outside jiff's Span range and these bytes are arbitrary. The civil
    // timestamp is µs since 2000-01-01 read as UTC, so the round trip is exact
    // (UTC has no DST). Overflow on either step → 22008.
    let unix_micros = pg_micros
        .checked_add(PG_EPOCH_UNIX_SECS * 1_000_000)
        .ok_or_else(|| TypeError::DatetimeFieldOverflow {
            value: pg_micros.to_string(),
        })?;
    Timestamp::from_microsecond(unix_micros)
        .map(|ts| ts.to_zoned(jiff::tz::TimeZone::UTC).datetime())
        .map_err(|_| TypeError::DatetimeFieldOverflow {
            value: pg_micros.to_string(),
        })
}

// ---------------------------------------------------------------------------
// timestamp with time zone
// ---------------------------------------------------------------------------

/// Parse a `timestamptz` literal into an absolute instant. If the text carries an
/// explicit offset (`Z`/`z` or `±HH[:MM[:SS]]`), that offset fixes the instant;
/// otherwise the wall-clock time is interpreted as local to the session `tz`.
pub fn parse_timestamptz(s: &str, tz: &TimeZone) -> Result<Timestamp, TypeError> {
    let t = s.trim();
    let (civil_str, offset) = split_offset(t);
    let dt = parse_timestamp_inner(civil_str)
        .map_err(|err| err.into_type_error("timestamp with time zone", s))?;
    match offset {
        // Explicit offset: the instant is the civil time minus the offset.
        Some(off) => off
            .to_timestamp(dt)
            .map_err(|_| TypeError::DatetimeFieldOverflow {
                value: s.to_string(),
            }),
        // No offset: interpret the wall clock as local to `tz`.
        None => dt.to_zoned(tz.clone()).map(|z| z.timestamp()).map_err(|_| {
            TypeError::DatetimeFieldOverflow {
                value: s.to_string(),
            }
        }),
    }
}

/// Split a trailing UTC-offset designator off a civil-datetime string. Returns
/// the civil portion and the parsed offset (if any). Recognizes `Z`/`z` (UTC) and
/// `±HH`, `±HH:MM`, `±HH:MM:SS` (and the colon-less `±HHMM` form).
fn split_offset(t: &str) -> (&str, Option<Offset>) {
    if let Some(stripped) = t.strip_suffix(['Z', 'z']) {
        return (stripped, Some(Offset::UTC));
    }
    // Scan for the offset sign AFTER the time portion. The date uses `-` as a
    // field separator, so only a `+`/`-` at/after the time can begin an offset;
    // we find the date/time separator first and search the time portion only.
    let sep = match t.find([' ', 'T', 't']) {
        Some(i) => i,
        None => return (t, None),
    };
    let time_region = &t[sep + 1..];
    // The offset sign is the first `+` or `-` in the time region.
    if let Some(rel) = time_region.find(['+', '-']) {
        let abs = sep + 1 + rel;
        let civil = &t[..abs];
        let off_str = &t[abs..];
        if let Some(off) = parse_offset_str(off_str) {
            return (civil, Some(off));
        }
    }
    (t, None)
}

/// Parse a `±HH[:MM[:SS]]` (or `±HHMM`/`±HHMMSS`) UTC offset into seconds.
fn parse_offset_str(s: &str) -> Option<Offset> {
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1i32, &s[1..]),
        b'-' => (-1i32, &s[1..]),
        _ => return None,
    };
    let (h, m, sec) = if rest.contains(':') {
        let mut it = rest.split(':');
        let h = it.next()?;
        let m = it.next().unwrap_or("0");
        let sec = it.next().unwrap_or("0");
        if it.next().is_some() {
            return None;
        }
        (h, m, sec)
    } else {
        // Colon-less: HH, HHMM, or HHMMSS.
        match rest.len() {
            1 | 2 => (rest, "0", "0"),
            4 => (&rest[..2], &rest[2..4], "0"),
            6 => (&rest[..2], &rest[2..4], &rest[4..6]),
            _ => return None,
        }
    };
    let hours: i32 = h.parse().ok()?;
    let mins: i32 = m.parse().ok()?;
    let secs: i32 = sec.parse().ok()?;
    let total = sign * (hours * 3600 + mins * 60 + secs);
    Offset::from_seconds(total).ok()
}

/// Render a `timestamptz` instant in `tz`: `YYYY-MM-DD HH:MM:SS[.ffffff]±HH[:MM[:SS]]`
/// (PostgreSQL `timestamptz_out`, ISO datestyle). The offset suffix shows `:MM`/`:SS`
/// only when non-zero.
pub fn timestamptz_to_text(ts: Timestamp, tz: &TimeZone) -> String {
    let dt = tz.to_datetime(ts);
    let off = tz.to_offset(ts);
    let mut out = timestamp_to_text(dt);
    push_offset(&mut out, off);
    out
}

/// Append a PostgreSQL-style offset suffix `±HH`, with `:MM` and `:SS` added only
/// when those finer components are non-zero.
fn push_offset(out: &mut String, off: Offset) {
    let total = off.seconds();
    let sign = if total < 0 { '-' } else { '+' };
    let abs = total.unsigned_abs();
    let hours = abs / 3600;
    let mins = (abs % 3600) / 60;
    let secs = abs % 60;
    out.push(sign);
    out.push_str(&format!("{hours:02}"));
    if mins != 0 || secs != 0 {
        out.push_str(&format!(":{mins:02}"));
        if secs != 0 {
            out.push_str(&format!(":{secs:02}"));
        }
    }
}

/// `timestamptz_send`: i64 big-endian microseconds since the PG epoch (UTC).
pub fn timestamptz_to_binary(ts: Timestamp) -> [u8; 8] {
    // Unix-epoch µs, then rebase to the PG epoch (2000-01-01 is 946684800s after
    // the Unix epoch).
    let unix_micros = ts.as_microsecond();
    let micros = unix_micros - PG_EPOCH_UNIX_SECS * 1_000_000;
    micros.to_be_bytes()
}

/// `timestamptz_recv`: i64 big-endian microseconds since the PG epoch (UTC).
pub fn timestamptz_from_binary(b: &[u8]) -> Result<Timestamp, TypeError> {
    let arr: [u8; 8] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "timestamp with time zone",
        value: format!("{b:?}"),
    })?;
    let pg_micros = i64::from_be_bytes(arr);
    // Rebase to the Unix epoch with a CHECKED add: `pg_micros` comes from
    // arbitrary bytes (storage/fuzz), so an unchecked `+` overflows i64 near the
    // boundary and panics under overflow-checks. Overflow → out of range (22008).
    let unix_micros = pg_micros
        .checked_add(PG_EPOCH_UNIX_SECS * 1_000_000)
        .ok_or_else(|| TypeError::DatetimeFieldOverflow {
            value: pg_micros.to_string(),
        })?;
    Timestamp::from_microsecond(unix_micros).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: pg_micros.to_string(),
    })
}

// ---------------------------------------------------------------------------
// interval
// ---------------------------------------------------------------------------

/// Parse a PostgreSQL verbose `interval`: a sequence of signed `<qty> <unit>`
/// terms and/or a `[-]HH:MM[:SS[.ffffff]]` clock term. Fractional quantities spill
/// into the next-smaller unit (PG rule); weeks fold to days, years to months.
pub fn parse_interval(s: &str) -> Result<Interval, TypeError> {
    let t = s.trim();
    if t.is_empty() {
        return Err(TypeError::InvalidDatetimeFormat {
            type_name: "interval",
            value: s.to_string(),
        });
    }
    let err = || TypeError::InvalidDatetimeFormat {
        type_name: "interval",
        value: s.to_string(),
    };

    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i128 = 0;

    let tokens: Vec<&str> = t.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i];
        // A clock term `[-]HH:MM[:SS[.ffffff]]` stands alone (no trailing unit).
        if tok.contains(':') {
            micros += parse_clock_term(tok).ok_or_else(err)? as i128;
            i += 1;
            continue;
        }
        // Otherwise a `<qty> <unit>` pair.
        let qty: f64 = tok.parse().map_err(|_| err())?;
        let unit = tokens.get(i + 1).ok_or_else(err)?;
        accumulate_unit(qty, unit, &mut months, &mut days, &mut micros).ok_or_else(err)?;
        i += 2;
    }

    let months = i32::try_from(months).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    })?;
    let days = i32::try_from(days).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    })?;
    let micros = i64::try_from(micros).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    })?;
    Ok(Interval {
        months,
        days,
        micros,
    })
}

/// Parse a `[-]HH:MM[:SS[.ffffff]]` clock term into signed microseconds.
fn parse_clock_term(tok: &str) -> Option<i64> {
    let (sign, rest) = match tok.strip_prefix('-') {
        Some(r) => (-1i64, r),
        None => (1i64, tok.strip_prefix('+').unwrap_or(tok)),
    };
    let mut parts = rest.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let (s_whole, s_frac_micros) = match parts.next() {
        Some(sec) => {
            if let Some((whole, frac)) = sec.split_once('.') {
                let whole: i64 = whole.parse().ok()?;
                // Pad/truncate the fraction to six µs digits.
                let mut frac_digits = frac.to_string();
                while frac_digits.len() < 6 {
                    frac_digits.push('0');
                }
                let micros: i64 = frac_digits[..6].parse().ok()?;
                (whole, micros)
            } else {
                (sec.parse().ok()?, 0)
            }
        }
        None => (0, 0),
    };
    if parts.next().is_some() {
        return None;
    }
    let total = h * 3_600_000_000 + m * 60_000_000 + s_whole * 1_000_000 + s_frac_micros;
    Some(sign * total)
}

/// Add one `<qty> <unit>` term, spilling a fractional quantity into the next
/// smaller field (PG semantics). Returns `None` for an unknown unit.
fn accumulate_unit(
    qty: f64,
    unit: &str,
    months: &mut i64,
    days: &mut i64,
    micros: &mut i128,
) -> Option<()> {
    let u = unit.trim_end_matches('s').to_ascii_lowercase();
    // The whole part of `qty`; the fractional part spills down.
    let whole = qty.trunc() as i64;
    let frac = qty.fract();
    match u.as_str() {
        "year" | "yr" => {
            *months += whole * 12;
            // Fractional years → months.
            *micros += 0; // (no µs contribution)
            *months += (frac * 12.0).round() as i64;
        }
        "month" | "mon" => {
            *months += whole;
            // Fractional months → days (PG uses a 30-day month).
            *days += (frac * 30.0).trunc() as i64;
            let day_frac = (frac * 30.0).fract();
            *micros += (day_frac * USECS_PER_DAY_I64 as f64).round() as i128;
        }
        "week" | "wk" => {
            *days += whole * 7;
            *micros += (frac * 7.0 * USECS_PER_DAY_I64 as f64).round() as i128;
        }
        "day" => {
            *days += whole;
            *micros += (frac * USECS_PER_DAY_I64 as f64).round() as i128;
        }
        "hour" | "hr" | "h" => {
            *micros += whole as i128 * 3_600_000_000;
            *micros += (frac * 3_600_000_000.0).round() as i128;
        }
        "minute" | "min" | "m" => {
            *micros += whole as i128 * 60_000_000;
            *micros += (frac * 60_000_000.0).round() as i128;
        }
        "second" | "sec" | "s" => {
            *micros += whole as i128 * 1_000_000;
            *micros += (frac * 1_000_000.0).round() as i128;
        }
        "millisecond" | "msec" | "ms" => {
            *micros += (qty * 1_000.0).round() as i128;
        }
        "microsecond" | "usec" | "us" => {
            *micros += qty.round() as i128;
        }
        _ => return None,
    }
    Some(())
}

/// Render an `interval` in PostgreSQL's `postgres` IntervalStyle (the default):
/// `[<y> year[s]] [<m> mons] [<d> days] [±HH:MM:SS[.ffffff]]`; a fully-zero
/// interval prints `00:00:00`.
pub fn interval_to_text(iv: Interval) -> String {
    let mut parts: Vec<String> = Vec::new();

    // Year/month component, derived from total months. PostgreSQL pluralizes the
    // unit name unless the value is *exactly* 1 (so `-1` and `2` are plural, only
    // `1` is singular — `1 year`, `-1 days`, `2 mons`).
    let years = iv.months / 12;
    let mons = iv.months % 12;
    if years != 0 {
        parts.push(format!("{years} year{}", if years == 1 { "" } else { "s" }));
    }
    if mons != 0 {
        parts.push(format!("{mons} mon{}", if mons == 1 { "" } else { "s" }));
    }
    // Day component.
    if iv.days != 0 {
        parts.push(format!(
            "{} day{}",
            iv.days,
            if iv.days == 1 { "" } else { "s" }
        ));
    }
    // Clock component (only when non-zero, OR when the whole interval is zero so
    // we have something to print).
    let has_clock = iv.micros != 0;
    if has_clock {
        parts.push(format_clock(iv.micros));
    }
    if parts.is_empty() {
        // A fully-zero interval prints the clock zero.
        return "00:00:00".to_string();
    }
    parts.join(" ")
}

/// Format the µs component of an interval as a signed `HH:MM:SS[.ffffff]` clock.
/// The sign applies to the whole clock (PG prints `-01:00:00`, not `01:-00:00`).
fn format_clock(total_micros: i64) -> String {
    let neg = total_micros < 0;
    let abs = total_micros.unsigned_abs();
    let hours = abs / 3_600_000_000;
    let rem = abs % 3_600_000_000;
    let mins = rem / 60_000_000;
    let rem = rem % 60_000_000;
    let secs = rem / 1_000_000;
    let micros = (rem % 1_000_000) as i32;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    out.push_str(&format!("{hours:02}:{mins:02}:{secs:02}"));
    push_subsecond(&mut out, micros * 1_000);
    out
}

/// `interval_send`: i64 µs ++ i32 days ++ i32 months, all big-endian (16 bytes).
pub fn interval_to_binary(iv: Interval) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&iv.micros.to_be_bytes());
    out[8..12].copy_from_slice(&iv.days.to_be_bytes());
    out[12..16].copy_from_slice(&iv.months.to_be_bytes());
    out
}

/// `interval_recv`: i64 µs ++ i32 days ++ i32 months, all big-endian.
pub fn interval_from_binary(b: &[u8]) -> Result<Interval, TypeError> {
    let arr: [u8; 16] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "interval",
        value: format!("{b:?}"),
    })?;
    let micros = i64::from_be_bytes(arr[0..8].try_into().expect("8-byte slice"));
    let days = i32::from_be_bytes(arr[8..12].try_into().expect("4-byte slice"));
    let months = i32::from_be_bytes(arr[12..16].try_into().expect("4-byte slice"));
    Ok(Interval {
        months,
        days,
        micros,
    })
}

// ---------------------------------------------------------------------------
// SP38: the date/time `to_char` template engine.
//
// PostgreSQL's `to_char(timestamp, fmt)` walks the template left-to-right,
// matching the LONGEST pattern keyword at each point (so `HH24` wins over `HH`,
// `YYYY` over `YY`), emitting a `"..."`-quoted run or any non-pattern character
// verbatim, and honoring the `FM` (fill-mode: drop padding/leading-zeros for the
// next field) and `TH`/`th` (ordinal suffix on the preceding number) modifiers.
// This module is pure value logic: the executor fills `DateTimeFields` from a
// `Datum` and calls `format_datetime`.
// ---------------------------------------------------------------------------

/// Pre-extracted civil fields for the `to_char` date/time engine. The executor
/// fills this from a `Datum`; a `timestamptz` supplies `tz_offset_secs`, a plain
/// `timestamp`/`date`/`time` leaves it `None` (so TZ patterns render empty, the
/// PostgreSQL behavior).
#[derive(Debug, Clone, Copy)]
pub struct DateTimeFields {
    pub year: i32,
    pub month: u32,         // 1..=12
    pub day: u32,           // 1..=31
    pub hour: u32,          // 0..=23
    pub minute: u32,        // 0..=59
    pub second: u32,        // 0..=59
    pub micros: u32,        // 0..=999_999
    pub iso_dow: u32,       // Mon=1 .. Sun=7   (PG `ID`)
    pub dow: u32,           // Sun=1 .. Sat=7   (PG `D`)
    pub doy: u32,           // 1..=366          (PG `DDD`)
    pub iso_week: u32,      // 1..=53           (PG `IW`)
    pub iso_year: i32,      // ISO week-numbering year (PG `IYYY`)
    pub week_of_year: u32,  // (doy-1)/7 + 1    (PG `WW`)
    pub week_of_month: u32, // (day-1)/7 + 1    (PG `W`)
    pub tz_offset_secs: Option<i32>,
}

impl DateTimeFields {
    /// Build the field struct from a `jiff` civil `DateTime`. `tz_offset_secs` is
    /// `Some` only when the source value is a `timestamptz` rendered in a zone.
    pub fn from_civil(dt: DateTime, tz_offset_secs: Option<i32>) -> Self {
        let date = dt.date();
        let time = dt.time();
        let iso = date.iso_week_date();
        // jiff returns signed civil components (i8/i16); all are non-negative for
        // a valid in-range datetime, so the `as u32` casts are exact.
        let doy = date.day_of_year() as u32;
        let day = date.day() as u32;
        DateTimeFields {
            year: i32::from(date.year()),
            month: date.month() as u32,
            day,
            hour: time.hour() as u32,
            minute: time.minute() as u32,
            second: time.second() as u32,
            micros: (time.subsec_nanosecond() / 1_000) as u32,
            iso_dow: date.weekday().to_monday_one_offset() as u32,
            dow: date.weekday().to_sunday_zero_offset() as u32 + 1,
            doy,
            iso_week: iso.week() as u32,
            iso_year: i32::from(iso.year()),
            week_of_year: (doy - 1) / 7 + 1,
            week_of_month: (day - 1) / 7 + 1,
            tz_offset_secs,
        }
    }
}

/// The field source the `to_char` renderer reads. Both `DateTimeFields` (a
/// civil date/time, fields already normalized to `0..=23` hours etc.) and an
/// interval field-set (PG `interval2tm`: hours may be `≥ 24` or negative, no
/// meaningful dow/doy/ISO fields) implement it, so `format_datetime` and
/// `format_interval` share ONE tokenizer/renderer (`match_pattern` + the
/// `render_tokens` body).
///
/// Every numeric getter returns `i64` so an interval's un-normalized hour count
/// (e.g. `36`, or a negative offset) is representable; `DateTimeFields` widens
/// its narrow civil fields losslessly.
trait FieldSource {
    fn year(&self) -> i64;
    fn month(&self) -> i64; // 1..=12 for a datetime; 0..=11 (months % 12) for an interval
    fn day(&self) -> i64;
    fn hour(&self) -> i64; // 0..=23 for a datetime; may be ≥ 24 / negative for an interval
    fn minute(&self) -> i64;
    fn second(&self) -> i64;
    fn micros(&self) -> i64; // 0..=999_999 sub-second microseconds
    fn iso_dow(&self) -> i64;
    fn dow(&self) -> i64;
    fn doy(&self) -> i64;
    fn iso_week(&self) -> i64;
    fn iso_year(&self) -> i64;
    fn week_of_year(&self) -> i64;
    fn week_of_month(&self) -> i64;
    fn tz_offset_secs(&self) -> Option<i32>;

    /// Index into the 12-entry month-name/Roman tables. A datetime's `month` is
    /// always `1..=12`; an interval's `months % 12` can be `0..=11` (or negative),
    /// so this maps the raw value into `0..=11` rather than panicking on an
    /// out-of-range subscript. (Month NAMES on an interval are not a corpus case;
    /// this just keeps the shared renderer total — see Task 9.)
    fn month_name_index(&self) -> usize {
        (self.month().rem_euclid(12)) as usize
    }

    /// Index into the 7-entry day-name table (`DAY_NAMES`, 0 = Sunday). A datetime's
    /// `dow` is `1..=7`; an interval has no day-of-week, so this clamps into range
    /// to keep the renderer total (day NAMES on an interval are not a corpus case).
    fn day_name_index(&self) -> usize {
        ((self.dow() - 1).rem_euclid(7)) as usize
    }
}

impl FieldSource for DateTimeFields {
    fn year(&self) -> i64 {
        i64::from(self.year)
    }
    fn month(&self) -> i64 {
        i64::from(self.month)
    }
    fn day(&self) -> i64 {
        i64::from(self.day)
    }
    fn hour(&self) -> i64 {
        i64::from(self.hour)
    }
    fn minute(&self) -> i64 {
        i64::from(self.minute)
    }
    fn second(&self) -> i64 {
        i64::from(self.second)
    }
    fn micros(&self) -> i64 {
        i64::from(self.micros)
    }
    fn iso_dow(&self) -> i64 {
        i64::from(self.iso_dow)
    }
    fn dow(&self) -> i64 {
        i64::from(self.dow)
    }
    fn doy(&self) -> i64 {
        i64::from(self.doy)
    }
    fn iso_week(&self) -> i64 {
        i64::from(self.iso_week)
    }
    fn iso_year(&self) -> i64 {
        i64::from(self.iso_year)
    }
    fn week_of_year(&self) -> i64 {
        i64::from(self.week_of_year)
    }
    fn week_of_month(&self) -> i64 {
        i64::from(self.week_of_month)
    }
    fn tz_offset_secs(&self) -> Option<i32> {
        self.tz_offset_secs
    }
    // For a datetime, `month`/`dow` are in range, so the default index maps are
    // exact (`month - 1 == month.rem_euclid(12)` for 1..=12, etc.); we override
    // to make that obvious and avoid relying on the wrap path.
    fn month_name_index(&self) -> usize {
        (self.month - 1) as usize
    }
    fn day_name_index(&self) -> usize {
        (self.dow - 1) as usize
    }
}

/// The interval field-set for the `to_char(interval, fmt)` renderer, mirroring
/// PostgreSQL `interval2tm`: the stored `months`/`days`/`micros` are read
/// component-wise WITHOUT normalizing across the day/month boundary —
/// `year = months / 12`, `month = months % 12`, `day = days`, and from `micros`
/// `hour = micros / 3_600_000_000` (which may be `≥ 24` or negative), then
/// minute/second/sub-second from the remainder.
struct IntervalFields {
    months: i64,
    days: i64,
    micros: i64,
}

impl IntervalFields {
    fn new(iv: Interval) -> Self {
        IntervalFields {
            months: i64::from(iv.months),
            days: i64::from(iv.days),
            micros: iv.micros,
        }
    }
}

impl FieldSource for IntervalFields {
    fn year(&self) -> i64 {
        self.months / 12
    }
    fn month(&self) -> i64 {
        self.months % 12
    }
    fn day(&self) -> i64 {
        self.days
    }
    fn hour(&self) -> i64 {
        // PG `interval2tm`: hours are NOT folded into days — `36 h` stays `36`.
        self.micros / 3_600_000_000
    }
    fn minute(&self) -> i64 {
        (self.micros / 60_000_000) % 60
    }
    fn second(&self) -> i64 {
        (self.micros / 1_000_000) % 60
    }
    fn micros(&self) -> i64 {
        // Sub-second microseconds; the sign rides along on negative intervals.
        self.micros % 1_000_000
    }
    // An interval has no calendar day-of-week / day-of-year / ISO week fields.
    // PG's `to_char(interval, …)` leaves these as the raw `tm` defaults (0); we
    // return 0 so a numeric ISO/dow/doy pattern renders its zero rather than
    // panicking — these patterns are not part of the interval corpus (Task 9).
    fn iso_dow(&self) -> i64 {
        0
    }
    fn dow(&self) -> i64 {
        0
    }
    fn doy(&self) -> i64 {
        0
    }
    fn iso_week(&self) -> i64 {
        0
    }
    fn iso_year(&self) -> i64 {
        0
    }
    fn week_of_year(&self) -> i64 {
        0
    }
    fn week_of_month(&self) -> i64 {
        0
    }
    fn tz_offset_secs(&self) -> Option<i32> {
        None
    }
}

/// The Roman-numeral month table (1-indexed: `ROMAN_MONTHS[m-1]`).
const ROMAN_MONTHS: [&str; 12] = [
    "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII",
];

/// Full English month names (1-indexed). C/English locale only — `TM` (locale
/// translation) is out of scope for this slice.
const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

/// Full English day names (index 0 = Sunday .. 6 = Saturday).
const DAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

/// The date/time `to_char` engine: render `template` from the pre-extracted
/// `fields`. Returns `Err(TypeError)` only on an internal range failure; an
/// unrecognized character is emitted literally (PostgreSQL behavior), never an
/// error.
pub fn format_datetime(template: &str, fields: &DateTimeFields) -> Result<String, TypeError> {
    render_tokens(template, fields)
}

/// The `to_char(interval, fmt)` engine: render `template` from an interval's
/// STORED `months`/`days`/`micros` (PG `interval2tm` — clock fields are NOT
/// normalized across the day/month boundary, so e.g. `HH24` of a `36 hour`
/// interval is `36`). Shares the exact tokenizer/renderer as `format_datetime`
/// via the `FieldSource` indirection.
pub fn format_interval(iv: Interval, template: &str) -> Result<String, TypeError> {
    render_tokens(template, &IntervalFields::new(iv))
}

/// The shared `to_char` tokenizer/renderer: walk `template` left-to-right,
/// longest-pattern-match at each point, honoring quoted runs, `FM`, and `TH`/`th`.
/// The field VALUES come from `src` (a civil datetime or an interval), so the same
/// engine serves `to_char(timestamp, …)` and `to_char(interval, …)`.
fn render_tokens(template: &str, src: &dyn FieldSource) -> Result<String, TypeError> {
    let chars: Vec<char> = template.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    // `fill_mode` is the one-shot FM flag: it suppresses padding/leading-zeros for
    // the NEXT pattern, then resets.
    let mut fill_mode = false;
    // The value of the most-recently-rendered numeric pattern, for a following
    // `TH`/`th` ordinal suffix. `None` if the previous token was not numeric.
    let mut last_number: Option<i64> = None;

    while i < chars.len() {
        // A `"`-quoted literal run: emit verbatim, honoring `\"` and `\\`.
        if chars[i] == '"' {
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    out.push(chars[i + 1]);
                    i += 2;
                } else {
                    out.push(chars[i]);
                    i += 1;
                }
            }
            // Skip the closing quote — but only an ACTUAL `"` (an unterminated
            // run stops at end-of-input, where there is nothing to skip).
            if chars.get(i) == Some(&'"') {
                i += 1;
            }
            last_number = None;
            continue;
        }

        // `FM`: set the one-shot fill-mode flag (it modifies the NEXT pattern).
        if matches_at(&chars, i, "FM") {
            fill_mode = true;
            i += 2;
            continue;
        }

        // `TH`/`th`: ordinal suffix on the preceding number (no-op otherwise).
        if let Some(n) = last_number
            && (matches_at(&chars, i, "TH") || matches_at(&chars, i, "th"))
        {
            let upper = chars[i] == 'T';
            out.push_str(&ordinal_suffix(n, upper));
            i += 2;
            last_number = None;
            continue;
        }

        // Try the longest matching pattern keyword.
        if let Some((kw, rendered, number)) = match_pattern(&chars, i, src, fill_mode)? {
            out.push_str(&rendered);
            last_number = number;
            fill_mode = false;
            i += kw;
            continue;
        }

        // No pattern matched: emit the character literally.
        out.push(chars[i]);
        last_number = None;
        i += 1;
    }
    Ok(out)
}

/// Does `chars[i..]` start with the ASCII keyword `kw` (exact, case-sensitive)?
fn matches_at(chars: &[char], i: usize, kw: &str) -> bool {
    let kw: Vec<char> = kw.chars().collect();
    if i + kw.len() > chars.len() {
        return false;
    }
    chars[i..i + kw.len()] == kw[..]
}

/// The English ordinal suffix (`st`/`nd`/`rd`/`th`) for `n`, upper- or
/// lower-cased per the `TH` vs `th` spelling. PostgreSQL keys the suffix off the
/// last two decimal digits (so 11/12/13 → `th`).
fn ordinal_suffix(n: i64, upper: bool) -> String {
    let abs = n.unsigned_abs() % 100;
    let s = if (11..=13).contains(&abs) {
        "th"
    } else {
        match abs % 10 {
            1 => "st",
            2 => "nd",
            3 => "rd",
            _ => "th",
        }
    };
    if upper {
        s.to_ascii_uppercase()
    } else {
        s.to_string()
    }
}

/// Zero-pad `value` to `width` unless `fm` (fill-mode) is set, in which case the
/// natural (un-padded) decimal is returned. Negative values keep their sign.
fn pad_num(value: i64, width: usize, fm: bool) -> String {
    if fm {
        value.to_string()
    } else if value < 0 {
        format!("-{:0width$}", value.unsigned_abs(), width = width)
    } else {
        format!("{value:0width$}")
    }
}

/// Blank-pad `name` to `width` on the RIGHT unless `fm` is set (PG pads month/day
/// names to a fixed 9-char field). Always returns the trimmed name under FM.
fn pad_name(name: &str, width: usize, fm: bool) -> String {
    if fm {
        name.to_string()
    } else {
        format!("{name:<width$}")
    }
}

/// Blank-pad a Roman-numeral month on the RIGHT to width 4 (PG LEFT-justifies
/// `RM`/`rm` in a field as wide as the widest numeral, "VIII" — oracle-confirmed,
/// PG 18: `RM` for January → `'I   '`, for March → `'III '`); `fm` strips it.
fn pad_roman(numeral: &str, fm: bool) -> String {
    if fm {
        numeral.to_string()
    } else {
        format!("{numeral:<4}")
    }
}

/// Render the meridiem string variants. `lower` lowercases; `dotted` inserts the
/// dots (`A.M.`/`P.M.`). `hour` is `i64` so the shared renderer also serves an
/// interval source (where AM/PM has no clock meaning — not a corpus case); for a
/// civil `0..=23` hour the `>= 12` test is unchanged.
fn meridiem(hour: i64, lower: bool, dotted: bool) -> String {
    let pm = hour >= 12;
    let s = match (pm, dotted) {
        (false, false) => "AM",
        (true, false) => "PM",
        (false, true) => "A.M.",
        (true, true) => "P.M.",
    };
    if lower {
        s.to_ascii_lowercase()
    } else {
        s.to_string()
    }
}

/// Render the era string variants (`AD`/`BC`, dotted, lowercase). PostgreSQL uses
/// `AD` for year > 0 and `BC` for year ≤ 0.
fn era(year: i32, lower: bool, dotted: bool) -> String {
    let bc = year <= 0;
    let s = match (bc, dotted) {
        (false, false) => "AD",
        (true, false) => "BC",
        (false, true) => "A.D.",
        (true, true) => "B.C.",
    };
    if lower {
        s.to_ascii_lowercase()
    } else {
        s.to_string()
    }
}

/// Case-fold a name per the pattern's casing: `Title` (first upper), `UPPER`, or
/// `lower`. `style` is the matched keyword's casing template.
#[derive(Clone, Copy)]
enum NameCase {
    Title,
    Upper,
    Lower,
}

fn cased(name: &str, case: NameCase) -> String {
    match case {
        NameCase::Title => name.to_string(), // table entries are already Title-case
        NameCase::Upper => name.to_ascii_uppercase(),
        NameCase::Lower => name.to_ascii_lowercase(),
    }
}

/// 12-hour clock hour for `HH`/`HH12`: `((h + 11) % 12) + 1` (so 0→12, 13→1).
/// `hour` is `i64` so the shared renderer also serves an interval source (where
/// `HH`/`HH12` has no clock meaning — not a corpus case); `rem_euclid` keeps the
/// result in `1..=12` for any input, matching the civil `0..=23` mapping exactly.
fn hour12(hour: i64) -> i64 {
    (hour + 11).rem_euclid(12) + 1
}

/// Render the `±HH` / `±HH:MM` etc. timezone forms from an offset in seconds.
/// `secs` is the signed UTC offset. The returned string carries the sign.
fn offset_hh(secs: i32) -> String {
    let sign = if secs < 0 { '-' } else { '+' };
    let h = secs.unsigned_abs() / 3600;
    format!("{sign}{h:02}")
}

/// Try to match the longest pattern keyword at `chars[i..]`. On a match, returns
/// `Some((consumed_len, rendered_text, numeric_value_for_TH))`. A non-match is
/// `Ok(None)`. `fm` is the one-shot fill-mode flag for the keyword being matched.
fn match_pattern(
    chars: &[char],
    i: usize,
    f: &dyn FieldSource,
    fm: bool,
) -> Result<Option<(usize, String, Option<i64>)>, TypeError> {
    // -- year (longest first) --
    if matches_at(chars, i, "YYYY") {
        return Ok(Some((4, pad_num(f.year(), 4, fm), Some(f.year()))));
    }
    if matches_at(chars, i, "YYY") {
        let v = f.year().rem_euclid(1000);
        return Ok(Some((3, pad_num(v, 3, fm), Some(v))));
    }
    // `Y,YYY`: comma-grouped 4-digit year (the comma grouping is kept even under
    // FM; PG's FM only suppresses leading zeros, not the group separator).
    if matches_at(chars, i, "Y,YYY") {
        let y = f.year();
        let s = format!("{},{:03}", y / 1000, (y % 1000).abs());
        return Ok(Some((5, s, Some(y))));
    }
    if matches_at(chars, i, "YY") {
        let v = f.year().rem_euclid(100);
        return Ok(Some((2, pad_num(v, 2, fm), Some(v))));
    }
    if matches_at(chars, i, "Y") {
        let v = f.year().rem_euclid(10);
        return Ok(Some((1, pad_num(v, 1, fm), Some(v))));
    }
    // -- ISO patterns (longest first so `IDDD`/`IYYY` win over `IY`/`IW`/`ID`/`I`) --
    if matches_at(chars, i, "IDDD") {
        // ISO day-of-year: (iso_week - 1) * 7 + iso_dow.
        let v = (f.iso_week() - 1) * 7 + f.iso_dow();
        return Ok(Some((4, pad_num(v, 3, fm), Some(v))));
    }
    if matches_at(chars, i, "IYYY") {
        return Ok(Some((4, pad_num(f.iso_year(), 4, fm), Some(f.iso_year()))));
    }
    if matches_at(chars, i, "IYY") {
        let v = f.iso_year().rem_euclid(1000);
        return Ok(Some((3, pad_num(v, 3, fm), Some(v))));
    }
    if matches_at(chars, i, "IW") {
        return Ok(Some((2, pad_num(f.iso_week(), 2, fm), Some(f.iso_week()))));
    }
    if matches_at(chars, i, "IY") {
        let v = f.iso_year().rem_euclid(100);
        return Ok(Some((2, pad_num(v, 2, fm), Some(v))));
    }
    if matches_at(chars, i, "ID") {
        let v = f.iso_dow();
        return Ok(Some((2, v.to_string(), Some(v))));
    }
    if matches_at(chars, i, "I") {
        let v = f.iso_year().rem_euclid(10);
        return Ok(Some((1, pad_num(v, 1, fm), Some(v))));
    }
    // -- century --
    if matches_at(chars, i, "CC") {
        // Century of year Y: `ceil(Y/100)` for AD years (Y ≥ 1 → `(Y+99)/100`),
        // and the floor form `(Y-99)/100` for Y ≤ 0 (BC / proleptic year 0). The
        // test is written `y < 1` (not `y > 0`) so the boundary year 1 — which the
        // two branches map to 1 vs 0 — makes the comparison observable (a year-1
        // unit test pins it).
        let y = f.year();
        let c = if y < 1 {
            (y - 99) / 100
        } else {
            (y + 99) / 100
        };
        return Ok(Some((2, pad_num(c, 2, fm), Some(c))));
    }
    // -- era (dotted forms first, then plain; upper before lower) --
    for (kw, lower, dotted) in [
        ("A.D.", false, true),
        ("B.C.", false, true),
        ("a.d.", true, true),
        ("b.c.", true, true),
        ("AD", false, false),
        ("BC", false, false),
        ("ad", true, false),
        ("bc", true, false),
    ] {
        if matches_at(chars, i, kw) {
            return Ok(Some((
                kw.chars().count(),
                era(f.year() as i32, lower, dotted),
                None,
            )));
        }
    }
    // -- month --
    if matches_at(chars, i, "MM") {
        return Ok(Some((2, pad_num(f.month(), 2, fm), Some(f.month()))));
    }
    for (kw, case) in [
        ("Month", NameCase::Title),
        ("MONTH", NameCase::Upper),
        ("month", NameCase::Lower),
    ] {
        if matches_at(chars, i, kw) {
            let name = cased(MONTH_NAMES[f.month_name_index()], case);
            return Ok(Some((5, pad_name(&name, 9, fm), None)));
        }
    }
    for (kw, case) in [
        ("Mon", NameCase::Title),
        ("MON", NameCase::Upper),
        ("mon", NameCase::Lower),
    ] {
        if matches_at(chars, i, kw) {
            let name = cased(&MONTH_NAMES[f.month_name_index()][..3], case);
            return Ok(Some((3, name, None)));
        }
    }
    // `RM`/`rm`: PostgreSQL left-justifies the Roman month numeral in a width-4
    // field (the widest is "VIII"); `FM` strips that padding.
    if matches_at(chars, i, "RM") {
        return Ok(Some((
            2,
            pad_roman(ROMAN_MONTHS[f.month_name_index()], fm),
            None,
        )));
    }
    if matches_at(chars, i, "rm") {
        let lower = ROMAN_MONTHS[f.month_name_index()].to_ascii_lowercase();
        return Ok(Some((2, pad_roman(&lower, fm), None)));
    }
    // -- day (DDD before DD before D; the ISO `IDDD`/`ID` are handled above) --
    if matches_at(chars, i, "DDD") {
        return Ok(Some((3, pad_num(f.doy(), 3, fm), Some(f.doy()))));
    }
    if matches_at(chars, i, "DD") {
        return Ok(Some((2, pad_num(f.day(), 2, fm), Some(f.day()))));
    }
    for (kw, case) in [
        ("Day", NameCase::Title),
        ("DAY", NameCase::Upper),
        ("day", NameCase::Lower),
    ] {
        if matches_at(chars, i, kw) {
            let name = cased(DAY_NAMES[f.day_name_index()], case);
            return Ok(Some((3, pad_name(&name, 9, fm), None)));
        }
    }
    for (kw, case) in [
        ("Dy", NameCase::Title),
        ("DY", NameCase::Upper),
        ("dy", NameCase::Lower),
    ] {
        if matches_at(chars, i, kw) {
            let name = cased(&DAY_NAMES[f.day_name_index()][..3], case);
            return Ok(Some((2, name, None)));
        }
    }
    if matches_at(chars, i, "D") {
        let v = f.dow();
        return Ok(Some((1, v.to_string(), Some(v))));
    }
    // -- week / quarter (the ISO `IW` is handled in the ISO group above) --
    if matches_at(chars, i, "WW") {
        return Ok(Some((
            2,
            pad_num(f.week_of_year(), 2, fm),
            Some(f.week_of_year()),
        )));
    }
    if matches_at(chars, i, "W") {
        let v = f.week_of_month();
        return Ok(Some((1, v.to_string(), Some(v))));
    }
    if matches_at(chars, i, "Q") {
        let v = (f.month() - 1) / 3 + 1;
        return Ok(Some((1, v.to_string(), Some(v))));
    }
    // -- time (HH24 before HH12/HH; SSSSS before SSSS before SS) --
    if matches_at(chars, i, "HH24") {
        return Ok(Some((4, pad_num(f.hour(), 2, fm), Some(f.hour()))));
    }
    if matches_at(chars, i, "HH12") {
        let v = hour12(f.hour());
        return Ok(Some((4, pad_num(v, 2, fm), Some(v))));
    }
    if matches_at(chars, i, "HH") {
        let v = hour12(f.hour());
        return Ok(Some((2, pad_num(v, 2, fm), Some(v))));
    }
    if matches_at(chars, i, "MI") {
        return Ok(Some((2, pad_num(f.minute(), 2, fm), Some(f.minute()))));
    }
    // `SSSS`/`SSSSS` (seconds past midnight): PostgreSQL does NOT zero-pad these
    // (e.g. `00:00:05` → `5`, not `0005`); they render as a bare decimal.
    if matches_at(chars, i, "SSSSS") {
        let v = f.hour() * 3600 + f.minute() * 60 + f.second();
        return Ok(Some((5, v.to_string(), Some(v))));
    }
    if matches_at(chars, i, "SSSS") {
        let v = f.hour() * 3600 + f.minute() * 60 + f.second();
        return Ok(Some((4, v.to_string(), Some(v))));
    }
    if matches_at(chars, i, "SS") {
        return Ok(Some((2, pad_num(f.second(), 2, fm), Some(f.second()))));
    }
    if matches_at(chars, i, "MS") {
        // Milliseconds: micros / 1000, 3 digits.
        let v = f.micros() / 1000;
        return Ok(Some((2, pad_num(v, 3, fm), Some(v))));
    }
    if matches_at(chars, i, "US") {
        let v = f.micros();
        return Ok(Some((2, pad_num(v, 6, fm), Some(v))));
    }
    // FF1..FF6: fractional seconds to N digits.
    if matches_at(chars, i, "FF") && i + 2 < chars.len() && chars[i + 2].is_ascii_digit() {
        let n = (chars[i + 2] as u8 - b'0') as usize;
        if (1..=6).contains(&n) {
            // Six-digit micros, take the first `n` digits.
            let full = format!("{:06}", f.micros());
            return Ok(Some((3, full[..n].to_string(), None)));
        }
    }
    // -- meridiem (dotted forms before plain; upper before lower) --
    for (kw, lower, dotted) in [
        ("A.M.", false, true),
        ("P.M.", false, true),
        ("a.m.", true, true),
        ("p.m.", true, true),
        ("AM", false, false),
        ("PM", false, false),
        ("am", true, false),
        ("pm", true, false),
    ] {
        if matches_at(chars, i, kw) {
            return Ok(Some((
                kw.chars().count(),
                meridiem(f.hour(), lower, dotted),
                None,
            )));
        }
    }
    // -- timezone (only with an offset present; else empty) --
    if matches_at(chars, i, "TZH") {
        let s = match f.tz_offset_secs() {
            Some(secs) => offset_hh(secs),
            None => String::new(),
        };
        return Ok(Some((3, s, None)));
    }
    if matches_at(chars, i, "TZM") {
        let s = match f.tz_offset_secs() {
            Some(secs) => format!("{:02}", (secs.unsigned_abs() % 3600) / 60),
            None => String::new(),
        };
        return Ok(Some((3, s, None)));
    }
    if matches_at(chars, i, "OF") {
        let s = match f.tz_offset_secs() {
            Some(secs) => {
                let mins = (secs.unsigned_abs() % 3600) / 60;
                if mins == 0 {
                    offset_hh(secs)
                } else {
                    format!("{}:{:02}", offset_hh(secs), mins)
                }
            }
            None => String::new(),
        };
        return Ok(Some((2, s, None)));
    }
    if matches_at(chars, i, "TZ") || matches_at(chars, i, "tz") {
        let s = match f.tz_offset_secs() {
            Some(secs) => offset_hh(secs),
            None => String::new(),
        };
        return Ok(Some((2, s, None)));
    }
    Ok(None)
}

/// The fields extracted by a template-driven parse (`to_timestamp`/`to_date`).
/// Separate from `DateTimeFields` (which is for FORMATTING): this is the OUTPUT of
/// parsing, holding whatever fields the template/input supplied, with PostgreSQL's
/// defaults filled in for the rest. The caller (the executor) builds a jiff
/// `Date`/`DateTime` from these fields, where the final civil-validity check (e.g.
/// Feb 30) is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub micros: u32,
    pub tz_offset_secs: Option<i32>,
}

impl Default for ParsedDateTime {
    /// PostgreSQL's defaults for fields no template pattern supplies: year 1,
    /// month 1, day 1, all clock fields 0, no timezone.
    fn default() -> Self {
        ParsedDateTime {
            year: 1,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
            tz_offset_secs: None,
        }
    }
}

/// Which half-of-day a meridiem pattern (`AM`/`PM`, dotted/lower) selected, so the
/// 12-hour `HH12`/`HH` value can be converted to 24-hour AFTER the whole input is
/// scanned (the meridiem may appear before or after the hour in the template).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Meridiem {
    Am,
    Pm,
}

/// Template-driven parse for `to_timestamp`/`to_date`. Tokenizes `template` with the
/// SAME longest-match pattern recognition as the format engine (`matches_at`), then
/// for each pattern consumes the corresponding piece of `input`: a numeric pattern
/// consumes up to its max width of leading ASCII digits; a name pattern
/// (`Mon`/`Month`) matches a month name case-insensitively; literal template chars
/// are matched leniently (PostgreSQL largely ignores separators — a non-alphanumeric
/// template char skips a run of non-alphanumeric input chars). Returns a
/// `ParsedDateTime` with PG defaults for absent fields (year 1, month/day 1, time 0).
/// Bad shape (a non-digit where a number is required, an unrecognized name) → 22007
/// (`InvalidDatetimeFormat`); an out-of-range field (month 13, hour 24, …) → 22008
/// (`DatetimeFieldOverflow`).
pub fn parse_by_template(template: &str, input: &str) -> Result<ParsedDateTime, TypeError> {
    let tchars: Vec<char> = template.chars().collect();
    let ichars: Vec<char> = input.chars().collect();
    let mut ti = 0usize; // cursor into the template
    let mut ii = 0usize; // cursor into the input

    let mut out = ParsedDateTime::default();
    // Track whether a meridiem pattern was present and which half it selected, so we
    // can fold the 12-hour clock to 24-hour after the full scan.
    let mut meridiem: Option<Meridiem> = None;

    let bad_shape = || TypeError::InvalidDatetimeFormat {
        type_name: "timestamp",
        value: input.to_string(),
    };
    let out_of_range = |what: &str, v: i64| TypeError::DatetimeFieldOverflow {
        value: format!("{what}={v}"),
    };

    while ti < tchars.len() {
        // A `"`-quoted literal run in the template: each char inside the quotes is a
        // literal matched against the input the same way a bare literal char is — an
        // alphanumeric must match (case-insensitively, else tolerated), a separator
        // skips a run of input separators (PG's lenient literal matching).
        if tchars[ti] == '"' {
            ti += 1;
            while ti < tchars.len() && tchars[ti] != '"' {
                let lit = if tchars[ti] == '\\' && ti + 1 < tchars.len() {
                    ti += 2;
                    tchars[ti - 1]
                } else {
                    let c = tchars[ti];
                    ti += 1;
                    c
                };
                match_literal(lit, &ichars, &mut ii);
            }
            if tchars.get(ti) == Some(&'"') {
                ti += 1;
            }
            continue;
        }
        // `FM` is a no-op for parsing (it only affects formatting fill).
        if matches_at(&tchars, ti, "FM") {
            ti += 2;
            continue;
        }

        if let Some((consumed, field)) = match_parse_pattern(&tchars, ti) {
            match field {
                ParseField::Num { max, set } => {
                    let v = consume_number(&ichars, &mut ii, max).ok_or_else(bad_shape)?;
                    set(&mut out, v);
                }
                ParseField::MonthAbbrev => {
                    let m = consume_month_name(&ichars, &mut ii, true).ok_or_else(bad_shape)?;
                    out.month = m;
                }
                ParseField::MonthFull => {
                    let m = consume_month_name(&ichars, &mut ii, false).ok_or_else(bad_shape)?;
                    out.month = m;
                }
                ParseField::DayNameSkip { len } => {
                    // A day-of-week NAME pattern (`Day`/`Dy`) does not set a value;
                    // skip a run of input letters (PG accepts and ignores it).
                    consume_day_name(&ichars, &mut ii, len);
                }
                ParseField::Meridiem => {
                    meridiem = Some(consume_meridiem(&ichars, &mut ii).ok_or_else(bad_shape)?);
                }
            }
            ti += consumed;
            continue;
        }

        // A bare literal template char (matched leniently — see `match_literal`).
        match_literal(tchars[ti], &ichars, &mut ii);
        ti += 1;
    }

    // Fold the 12-hour clock to 24-hour if a meridiem pattern was present.
    if let Some(m) = meridiem {
        // PG only treats the hour as a 12-hour value when an HH12/HH pattern fed it;
        // 12 AM → 0, 12 PM → 12, otherwise +12 for PM. (If no HH12 pattern set the
        // hour, a stray AM/PM still applies the standard conversion to whatever hour
        // value is present, matching PG's `tm` post-processing.)
        let h = out.hour % 12; // 12 → 0
        out.hour = match m {
            Meridiem::Am => h,
            Meridiem::Pm => h + 12,
        };
    }

    // Range-validate the assembled fields. Full civil validity (Feb 30, etc.) is the
    // caller's job; here we reject a clearly out-of-range single field.
    if !(1..=12).contains(&out.month) {
        return Err(out_of_range("month", out.month as i64));
    }
    if !(1..=31).contains(&out.day) {
        return Err(out_of_range("day", out.day as i64));
    }
    if out.hour > 23 {
        return Err(out_of_range("hour", out.hour as i64));
    }
    if out.minute > 59 {
        return Err(out_of_range("minute", out.minute as i64));
    }
    if out.second > 59 {
        return Err(out_of_range("second", out.second as i64));
    }

    Ok(out)
}

/// A parse-time pattern: what kind of input piece to consume and how to store it.
enum ParseField {
    /// A run of up to `max` leading digits; `set` records it into the right field.
    Num {
        max: usize,
        set: fn(&mut ParsedDateTime, i64),
    },
    /// A 3-letter month abbreviation.
    MonthAbbrev,
    /// A full month name (longest match).
    MonthFull,
    /// A day-of-week name pattern that is accepted but sets no value.
    DayNameSkip { len: usize },
    /// An `AM`/`PM` meridiem marker (dotted/lower forms accepted).
    Meridiem,
}

/// Recognize the parse pattern at `tchars[ti..]` (longest match), returning the
/// number of TEMPLATE chars it spans and the field to consume. Mirrors the
/// formatter's longest-first ordering for the patterns `to_timestamp`/`to_date`
/// commonly use; unrecognized template text falls through to literal handling.
fn match_parse_pattern(tchars: &[char], ti: usize) -> Option<(usize, ParseField)> {
    // Numeric patterns, longest first within each family so `YYYY` beats `YY`, etc.
    // The `max` is the max digits to consume; PG accepts fewer if a non-digit follows.
    let num = |max: usize, set: fn(&mut ParsedDateTime, i64)| ParseField::Num { max, set };

    // -- year --
    if matches_at(tchars, ti, "YYYY") {
        return Some((4, num(4, |p, v| p.year = v as i32)));
    }
    if matches_at(tchars, ti, "YYY") {
        return Some((3, num(3, |p, v| p.year = v as i32)));
    }
    if matches_at(tchars, ti, "YY") {
        return Some((2, num(2, |p, v| p.year = v as i32)));
    }
    if matches_at(tchars, ti, "Y") {
        return Some((1, num(1, |p, v| p.year = v as i32)));
    }
    // -- month (numeric, then names; `Month` before `Mon`) --
    if matches_at(tchars, ti, "MM") {
        return Some((2, num(2, |p, v| p.month = v as u32)));
    }
    if matches_at(tchars, ti, "Month")
        || matches_at(tchars, ti, "MONTH")
        || matches_at(tchars, ti, "month")
    {
        return Some((5, ParseField::MonthFull));
    }
    if matches_at(tchars, ti, "Mon")
        || matches_at(tchars, ti, "MON")
        || matches_at(tchars, ti, "mon")
    {
        return Some((3, ParseField::MonthAbbrev));
    }
    // -- day-of-month / day-of-week name (accepted, sets nothing) --
    if matches_at(tchars, ti, "DD") {
        return Some((2, num(2, |p, v| p.day = v as u32)));
    }
    if matches_at(tchars, ti, "Day")
        || matches_at(tchars, ti, "DAY")
        || matches_at(tchars, ti, "day")
    {
        return Some((3, ParseField::DayNameSkip { len: 9 }));
    }
    if matches_at(tchars, ti, "Dy") || matches_at(tchars, ti, "DY") || matches_at(tchars, ti, "dy")
    {
        return Some((2, ParseField::DayNameSkip { len: 3 }));
    }
    // -- time (HH24 before HH12/HH; SS before nothing shorter here) --
    if matches_at(tchars, ti, "HH24") {
        return Some((4, num(2, |p, v| p.hour = v as u32)));
    }
    if matches_at(tchars, ti, "HH12") {
        return Some((4, num(2, |p, v| p.hour = v as u32)));
    }
    if matches_at(tchars, ti, "HH") {
        return Some((2, num(2, |p, v| p.hour = v as u32)));
    }
    if matches_at(tchars, ti, "MI") {
        return Some((2, num(2, |p, v| p.minute = v as u32)));
    }
    if matches_at(tchars, ti, "SS") {
        return Some((2, num(2, |p, v| p.second = v as u32)));
    }
    if matches_at(tchars, ti, "US") {
        // Microseconds: up to 6 digits.
        return Some((2, num(6, |p, v| p.micros = v as u32)));
    }
    if matches_at(tchars, ti, "MS") {
        // Milliseconds: up to 3 digits, scaled to micros.
        return Some((2, num(3, |p, v| p.micros = (v as u32) * 1000)));
    }
    // -- meridiem (dotted forms before plain; either case) --
    for kw in ["A.M.", "P.M.", "a.m.", "p.m.", "AM", "PM", "am", "pm"] {
        if matches_at(tchars, ti, kw) {
            return Some((kw.chars().count(), ParseField::Meridiem));
        }
    }
    None
}

/// Consume up to `max` leading ASCII digits from `chars` at `*i`, returning the
/// value. Requires at least one digit (PG: a number-expecting pattern with a
/// non-digit there is an error) — returns `None` otherwise.
fn consume_number(chars: &[char], i: &mut usize, max: usize) -> Option<i64> {
    let start = *i;
    let mut v: i64 = 0;
    let mut n = 0usize;
    while *i < chars.len() && n < max && chars[*i].is_ascii_digit() {
        v = v * 10 + (chars[*i] as u8 - b'0') as i64;
        *i += 1;
        n += 1;
    }
    if *i == start { None } else { Some(v) }
}

/// Consume a month name from `chars` at `*i`, case-insensitively. When `abbrev`,
/// match a 3-letter abbreviation (the first 3 chars of a `MONTH_NAMES` entry);
/// otherwise match a full month name (longest match — the input must begin with the
/// full name). Returns the 1-based month, or `None` if no name matches.
fn consume_month_name(chars: &[char], i: &mut usize, abbrev: bool) -> Option<u32> {
    for (idx, name) in MONTH_NAMES.iter().enumerate() {
        let needle: Vec<char> = if abbrev {
            name.chars().take(3).collect()
        } else {
            name.chars().collect()
        };
        if input_starts_with_ci(chars, *i, &needle) {
            *i += needle.len();
            return Some(idx as u32 + 1);
        }
    }
    None
}

/// Skip a day-of-week NAME in the input (accepted but value-less). Matches a known
/// day name (full or 3-letter abbrev) case-insensitively; if none matches, skips a
/// run of up to `len` leading letters as a lenient fallback.
fn consume_day_name(chars: &[char], i: &mut usize, len: usize) {
    for name in DAY_NAMES.iter() {
        let full: Vec<char> = name.chars().collect();
        if input_starts_with_ci(chars, *i, &full) {
            *i += full.len();
            return;
        }
        let abbrev: Vec<char> = name.chars().take(3).collect();
        if input_starts_with_ci(chars, *i, &abbrev) {
            *i += abbrev.len();
            return;
        }
    }
    // Lenient fallback: skip up to `len` leading alphabetic chars.
    let mut n = 0;
    while *i < chars.len() && n < len && chars[*i].is_alphabetic() {
        *i += 1;
        n += 1;
    }
}

/// Consume an `AM`/`PM` meridiem at `*i` (dotted `A.M.`/`P.M.` and either case
/// accepted). Returns the half-of-day, or `None` if neither matches.
fn consume_meridiem(chars: &[char], i: &mut usize) -> Option<Meridiem> {
    // Dotted forms first (longest match), then plain.
    for (needle, m) in [
        ("a.m.", Meridiem::Am),
        ("p.m.", Meridiem::Pm),
        ("am", Meridiem::Am),
        ("pm", Meridiem::Pm),
    ] {
        let nchars: Vec<char> = needle.chars().collect();
        if input_starts_with_ci(chars, *i, &nchars) {
            *i += nchars.len();
            return Some(m);
        }
    }
    None
}

/// Does `chars[i..]` begin with `needle` (already a `&[char]`), ASCII-case-insensitive?
fn input_starts_with_ci(chars: &[char], i: usize, needle: &[char]) -> bool {
    if i + needle.len() > chars.len() {
        return false;
    }
    chars[i..i + needle.len()]
        .iter()
        .zip(needle)
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// Advance `*i` over a run of leading non-alphanumeric (separator/whitespace/punct)
/// input chars. PostgreSQL is lenient about separators between fields, so a literal
/// template separator matches zero-or-more input separators.
fn skip_separators(chars: &[char], i: &mut usize) {
    while *i < chars.len() && !chars[*i].is_alphanumeric() {
        *i += 1;
    }
}

/// Match a single literal template char `lit` against the input at `*i`. An
/// alphanumeric literal consumes one matching input char (case-insensitive; a
/// mismatch is tolerated — PG does not hard-fail a literal mismatch — leaving the
/// cursor in place). A separator/punctuation literal matches leniently: it skips a
/// run of leading separator chars in the input (PG largely ignores separators, so
/// e.g. an input `-` matches a template `/`).
fn match_literal(lit: char, chars: &[char], i: &mut usize) {
    if lit.is_alphanumeric() {
        if *i < chars.len() && chars[*i].eq_ignore_ascii_case(&lit) {
            *i += 1;
        }
    } else {
        skip_separators(chars, i);
    }
}

// ---------------------------------------------------------------------------
// SP38: `make_*` constructors and `justify_*` normalization.
//
// PostgreSQL's `make_date`/`make_time`/`make_timestamp`/`make_interval` build a
// value from positional numeric fields; an out-of-range field is a
// `DatetimeFieldOverflow` (22008). The `interval_justify_{days,hours,interval}`
// functions re-balance an interval's months/days/micros into PG's canonical
// 30-day-month / 24-hour-day buckets, then sign-normalize so no field's sign
// disagrees with the whole (`justify_interval`). Pure value helpers — the
// executor wires them into `make_*`/`justify_*` SQL functions in a later task.
// ---------------------------------------------------------------------------

/// Map a jiff civil-constructor error (an out-of-range field) to a
/// `DatetimeFieldOverflow` (22008), labelling the offending field set.
fn field_overflow(value: impl Into<String>) -> TypeError {
    TypeError::DatetimeFieldOverflow {
        value: value.into(),
    }
}

/// Split a fractional-seconds `f64` into whole seconds + nanoseconds at µs
/// resolution (PG stores microseconds, so the nanos are always a multiple of
/// 1000). Returns `None` when the whole-second part does not fit an `i8` (the
/// jiff `Time`/`DateTime` second field), which a civil time would reject anyway.
fn split_seconds(sec: f64) -> Option<(i8, i32)> {
    if !sec.is_finite() {
        return None;
    }
    let whole = sec.trunc();
    if !(-128.0..=127.0).contains(&whole) {
        return None;
    }
    let whole = whole as i8;
    // Fractional part → microseconds, then ×1000 for jiff's nanosecond field.
    // `.round()` stands in for PG's `rint` on the µs value.
    let micros = (sec.fract() * 1_000_000.0).round() as i32;
    let nanos = micros * 1_000;
    Some((whole, nanos))
}

/// `make_date(year, month, day)`. An out-of-range field (month 13, day 0, …) →
/// 22008 (`DatetimeFieldOverflow`).
pub fn make_date(year: i32, month: i32, day: i32) -> Result<Date, TypeError> {
    let label = || format!("{year}-{month}-{day}");
    let y = i16::try_from(year).map_err(|_| field_overflow(label()))?;
    let mo = i8::try_from(month).map_err(|_| field_overflow(label()))?;
    let d = i8::try_from(day).map_err(|_| field_overflow(label()))?;
    Date::new(y, mo, d).map_err(|_| field_overflow(label()))
}

/// `make_time(hour, min, sec)`; the fractional part of `sec` becomes microseconds
/// (PG resolution). An out-of-range field (hour 24, minute 60, …) → 22008.
pub fn make_time(hour: i32, min: i32, sec: f64) -> Result<Time, TypeError> {
    let label = || format!("{hour}:{min}:{sec}");
    let h = i8::try_from(hour).map_err(|_| field_overflow(label()))?;
    let mi = i8::try_from(min).map_err(|_| field_overflow(label()))?;
    let (s, nanos) = split_seconds(sec).ok_or_else(|| field_overflow(label()))?;
    Time::new(h, mi, s, nanos).map_err(|_| field_overflow(label()))
}

/// Civil-`DateTime` builder shared by `make_timestamp` / `make_timestamptz` (the
/// executor wraps the time-zone step for the latter). An out-of-range field →
/// 22008.
pub fn make_timestamp_civil(
    y: i32,
    mo: i32,
    d: i32,
    h: i32,
    mi: i32,
    sec: f64,
) -> Result<DateTime, TypeError> {
    let date = make_date(y, mo, d)?;
    let time = make_time(h, mi, sec)?;
    Ok(date.to_datetime(time))
}

/// `make_interval(years, months, weeks, days, hours, mins, secs)`: weeks fold into
/// days, years into months, and the clock fields (hours/mins/secs, fractional secs
/// included) into microseconds. All arithmetic is checked; any field overflow →
/// 22008.
pub fn make_interval(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    mins: i32,
    secs: f64,
) -> Result<Interval, TypeError> {
    let label = "make_interval";
    // months = years*12 + months (checked, i32).
    let months = years
        .checked_mul(12)
        .and_then(|m| m.checked_add(months))
        .ok_or_else(|| field_overflow(label))?;
    // days = weeks*7 + days (checked, i32).
    let days = weeks
        .checked_mul(7)
        .and_then(|d| d.checked_add(days))
        .ok_or_else(|| field_overflow(label))?;
    // micros = (((hours*60 + mins)*60) * 1e6) + round(secs*1e6) (checked, i64).
    if !secs.is_finite() {
        return Err(field_overflow(label));
    }
    let sec_micros_f = (secs * 1_000_000.0).round();
    if sec_micros_f.abs() >= 9_223_372_036_854_775_808.0_f64 {
        return Err(field_overflow(label));
    }
    let sec_micros = sec_micros_f as i64;
    let micros = (i64::from(hours) * 60 + i64::from(mins))
        .checked_mul(60)
        .and_then(|s| s.checked_mul(1_000_000))
        .and_then(|us| us.checked_add(sec_micros))
        .ok_or_else(|| field_overflow(label))?;
    Ok(Interval {
        months,
        days,
        micros,
    })
}

/// PostgreSQL `interval_justify_days`: roll whole 30-day groups of `days` into
/// `months`, leaving `days` in `(-30, 30)` (truncating division keeps the sign).
/// The `months` sum is done in i64 and narrowed back, so a near-`i32::MAX` input
/// raises 22008 (PG 15+ `ERROR: interval out of range`) rather than panicking
/// (debug, overflow-checks on) or wrapping (release).
pub fn justify_days(iv: Interval) -> Result<Interval, TypeError> {
    let whole_months = i64::from(iv.days) / 30;
    let months = i64::from(iv.months) + whole_months;
    Ok(Interval {
        months: i32::try_from(months).map_err(|_| field_overflow("justify_days"))?,
        days: iv.days % 30,
        micros: iv.micros,
    })
}

/// PostgreSQL `interval_justify_hours`: roll whole 24-hour groups of `micros` into
/// `days`, leaving `micros` in `(-1 day, 1 day)` (truncating division keeps the
/// sign). The `days` sum is done in i64 and narrowed back, so a near-`i32::MAX`
/// input raises 22008 (PG 15+ `ERROR: interval out of range`) rather than
/// panicking (debug, overflow-checks on) or wrapping (release).
pub fn justify_hours(iv: Interval) -> Result<Interval, TypeError> {
    let whole_days = iv.micros / USECS_PER_DAY_I64;
    let days = i64::from(iv.days) + whole_days;
    Ok(Interval {
        months: iv.months,
        days: i32::try_from(days).map_err(|_| field_overflow("justify_hours"))?,
        micros: iv.micros % USECS_PER_DAY_I64,
    })
}

/// PostgreSQL `interval_justify_interval` (src/backend/utils/adt/timestamp.c):
/// pre-justify micros→days (24h) and days→months (30d), then sign-normalize so no
/// field's sign disagrees with a larger non-zero field. The result is PG's
/// canonical form, e.g. `'1 mon -1 hour'` → `'29 days 23:00:00'`.
pub fn justify_interval(iv: Interval) -> Result<Interval, TypeError> {
    const DAYS_PER_MONTH: i32 = 30;
    // Pre-justify on widened fields (the rolls can briefly push `days` past i32).
    let mut months = i64::from(iv.months);
    let mut days = i64::from(iv.days);
    let mut micros = iv.micros;

    // micros → days (24h), then days → months (30d).
    days += micros / USECS_PER_DAY_I64;
    micros %= USECS_PER_DAY_I64;
    months += days / i64::from(DAYS_PER_MONTH);
    days %= i64::from(DAYS_PER_MONTH);

    // Sign-normalize months↔days: if months and days disagree (or days==0 and
    // micros disagrees with months), borrow/carry a whole 30-day month.
    if months > 0 && (days < 0 || (days == 0 && micros < 0)) {
        days += i64::from(DAYS_PER_MONTH);
        months -= 1;
    } else if months < 0 && (days > 0 || (days == 0 && micros > 0)) {
        days -= i64::from(DAYS_PER_MONTH);
        months += 1;
    }
    // Sign-normalize days↔micros: borrow/carry a whole 24-hour day.
    if days > 0 && micros < 0 {
        micros += USECS_PER_DAY_I64;
        days -= 1;
    } else if days < 0 && micros > 0 {
        micros -= USECS_PER_DAY_I64;
        days += 1;
    }

    // Normalization keeps each field within a month/day of its pre-justify value,
    // but `months` (and transiently `days`) can already exceed i32 after rolling
    // the micros/days carries up — so the narrowing back to i32 is NOT lossless
    // in general. Check it: an out-of-i32 result raises 22008 (PG 15+ `ERROR:
    // interval out of range`) rather than silently wrapping.
    Ok(Interval {
        months: i32::try_from(months).map_err(|_| field_overflow("justify_interval"))?,
        days: i32::try_from(days).map_err(|_| field_overflow("justify_interval"))?,
        micros,
    })
}

#[cfg(test)]
mod format_tests {
    use super::{DateTimeFields, format_datetime};

    fn fields_monday() -> DateTimeFields {
        // 2024-01-15 13:45:06.5, a Monday.
        DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 15, 13, 45, 6, 500_000_000),
            None,
        )
    }

    #[test]
    fn format_datetime_core_patterns() {
        let f = fields_monday();
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        assert_eq!(fmt("YYYY-MM-DD HH24:MI:SS"), "2024-01-15 13:45:06");
        assert_eq!(fmt("HH12:MI:SS PM"), "01:45:06 PM");
        assert_eq!(fmt("HH12:MI am"), "01:45 pm");
        assert_eq!(fmt("Mon Month"), "Jan January  "); // Month blank-padded to 9
        assert_eq!(fmt("FMMonth DD, YYYY"), "January 15, 2024"); // FM suppresses padding
        assert_eq!(fmt("Dy Day"), "Mon Monday   "); // Day padded to 9
        assert_eq!(fmt("Q"), "1");
        assert_eq!(fmt("MS US"), "500 500000");
        assert_eq!(fmt(r#""year:" YYYY"#), "year: 2024"); // quoted literal
        assert_eq!(fmt("DDth"), "15th"); // ordinal suffix
        assert_eq!(fmt("FF3"), "500");
    }

    #[test]
    fn format_datetime_timezone_patterns() {
        // timestamptz rendered at -05:00 (offset present).
        let f = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 15, 12, 0, 0, 0),
            Some(-5 * 3600),
        );
        assert_eq!(format_datetime("OF", &f).expect("OF"), "-05");
        assert_eq!(format_datetime("TZH:TZM", &f).expect("tz"), "-05:00");
        // A plain timestamp (no offset) renders TZ patterns as empty (PG behavior).
        let g = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 15, 12, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("HH24OF", &g).expect("notz"), "12");
    }

    #[test]
    fn format_datetime_year_patterns() {
        let f = fields_monday();
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        assert_eq!(fmt("YYYY"), "2024");
        assert_eq!(fmt("YYY"), "024");
        assert_eq!(fmt("YY"), "24");
        assert_eq!(fmt("Y"), "4");
        assert_eq!(fmt("Y,YYY"), "2,024");
        assert_eq!(fmt("CC"), "21"); // 2024 → century 21
        // ISO year: 2024-01-15 is in ISO year 2024.
        assert_eq!(fmt("IYYY"), "2024");
        assert_eq!(fmt("IYY"), "024");
        assert_eq!(fmt("IY"), "24");
        assert_eq!(fmt("I"), "4");
    }

    #[test]
    fn format_datetime_era_patterns() {
        let f = fields_monday();
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        assert_eq!(fmt("AD"), "AD");
        assert_eq!(fmt("BC"), "AD"); // both spellings render the era for the value
        assert_eq!(fmt("ad"), "ad");
        assert_eq!(fmt("A.D."), "A.D.");
        assert_eq!(fmt("a.d."), "a.d.");
    }

    #[test]
    fn format_datetime_month_patterns() {
        let f = fields_monday();
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        assert_eq!(fmt("MM"), "01");
        assert_eq!(fmt("Mon"), "Jan");
        assert_eq!(fmt("MON"), "JAN");
        assert_eq!(fmt("mon"), "jan");
        assert_eq!(fmt("Month"), "January  ");
        assert_eq!(fmt("MONTH"), "JANUARY  ");
        assert_eq!(fmt("month"), "january  ");
        // PG LEFT-justifies the Roman numeral in a width-4 field ("VIII" is widest).
        assert_eq!(fmt("RM"), "I   ");
        assert_eq!(fmt("rm"), "i   ");
        assert_eq!(fmt("FMRM"), "I"); // FM strips the left-justify padding
        assert_eq!(fmt("FMMonth"), "January");
        assert_eq!(fmt("FMMM"), "1");
    }

    #[test]
    fn format_datetime_day_and_week_patterns() {
        let f = fields_monday();
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        assert_eq!(fmt("DD"), "15");
        assert_eq!(fmt("DDD"), "015"); // day-of-year 15
        assert_eq!(fmt("IDDD"), "015"); // ISO day-of-year 15 (week 3, dow 1: 2*7+1=15)
        assert_eq!(fmt("D"), "2"); // Monday → Sun=1 scheme → 2
        assert_eq!(fmt("ID"), "1"); // Monday → ISO dow 1
        assert_eq!(fmt("Day"), "Monday   ");
        assert_eq!(fmt("DAY"), "MONDAY   ");
        assert_eq!(fmt("day"), "monday   ");
        assert_eq!(fmt("Dy"), "Mon");
        assert_eq!(fmt("DY"), "MON");
        assert_eq!(fmt("dy"), "mon");
        assert_eq!(fmt("W"), "3"); // (15-1)/7 + 1 = 3
        assert_eq!(fmt("WW"), "03"); // (15-1)/7 + 1 = 3
        assert_eq!(fmt("IW"), "03"); // ISO week 3
        assert_eq!(fmt("FMDDD"), "15"); // FM drops the leading zero
    }

    #[test]
    fn format_datetime_time_patterns() {
        let f = fields_monday();
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        assert_eq!(fmt("HH24"), "13");
        assert_eq!(fmt("HH12"), "01");
        assert_eq!(fmt("HH"), "01");
        assert_eq!(fmt("MI"), "45");
        assert_eq!(fmt("SS"), "06");
        // seconds past midnight: 13*3600 + 45*60 + 6 = 49506.
        assert_eq!(fmt("SSSS"), "49506");
        assert_eq!(fmt("SSSSS"), "49506");
        assert_eq!(fmt("MS"), "500");
        assert_eq!(fmt("US"), "500000");
        assert_eq!(fmt("FF1"), "5");
        assert_eq!(fmt("FF2"), "50");
        assert_eq!(fmt("FF3"), "500");
        assert_eq!(fmt("FF6"), "500000");
        assert_eq!(fmt("FMHH24"), "13");
        assert_eq!(fmt("FMSS"), "6"); // FM drops leading zero
    }

    #[test]
    fn format_datetime_meridiem_and_midnight() {
        // 00:30 → AM, 12-hour 12.
        let f = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 15, 0, 30, 0, 0),
            None,
        );
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        assert_eq!(fmt("HH12 AM"), "12 AM");
        assert_eq!(fmt("HH12 PM"), "12 AM"); // both spellings render the value's meridiem
        assert_eq!(fmt("HH12 am"), "12 am");
        assert_eq!(fmt("A.M."), "A.M.");
        assert_eq!(fmt("p.m."), "a.m.");
        // SSSS/SSSSS are NOT zero-padded (PG): 00:30:00 → 1800, not 1800/01800.
        assert_eq!(fmt("SSSS"), "1800");
        assert_eq!(fmt("SSSSS"), "1800");
        // noon → PM, 12-hour 12.
        let g = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 15, 12, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("HH12 PM", &g).expect("noon"), "12 PM");
    }

    #[test]
    fn format_datetime_ordinal_th_variants() {
        let f = fields_monday();
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        assert_eq!(fmt("DDth"), "15th");
        assert_eq!(fmt("DDTH"), "15TH");
        // DD=15 → th. Use a day that ends in 1/2/3 for st/nd/rd.
        let d1 = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 1, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("DDth", &d1).expect("1"), "01st");
        let d2 = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 2, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("DDth", &d2).expect("2"), "02nd");
        let d3 = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 3, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("DDth", &d3).expect("3"), "03rd");
        // 11/12/13 are all `th`.
        let d11 = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 11, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("DDth", &d11).expect("11"), "11th");
        // FMDDth drops the leading zero AND keeps the suffix: "1st".
        assert_eq!(format_datetime("FMDDth", &d1).expect("fm"), "1st");
    }

    #[test]
    fn format_datetime_quoted_and_passthrough() {
        let f = fields_monday();
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        // Quoted literal with an embedded pattern char (Y) emitted verbatim.
        assert_eq!(fmt(r#""Year " YYYY"#), "Year  2024");
        // Escaped quote inside a quoted run.
        assert_eq!(fmt(r#""a\"b""#), "a\"b");
        // A non-pattern char (e.g. `/`) passes through literally.
        assert_eq!(fmt("YYYY/MM/DD"), "2024/01/15");
        // A bare letter that begins no pattern is emitted literally.
        assert_eq!(fmt("Q!"), "1!");
    }

    #[test]
    fn format_datetime_offset_minutes() {
        // +05:30 offset (e.g. India): OF shows the colon-minutes; TZH/TZM split.
        let f = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 15, 12, 0, 0, 0),
            Some(5 * 3600 + 30 * 60),
        );
        assert_eq!(format_datetime("OF", &f).expect("of"), "+05:30");
        assert_eq!(format_datetime("TZH", &f).expect("tzh"), "+05");
        assert_eq!(format_datetime("TZM", &f).expect("tzm"), "30");
        assert_eq!(format_datetime("TZ", &f).expect("tz"), "+05");
    }

    // -----------------------------------------------------------------------
    // Mutation-killing tests (cargo-mutants on `datetime.rs`): each pins a
    // boundary the broad pattern tests above leave ambiguous.
    // -----------------------------------------------------------------------

    /// `WW`/`W` use `(x - 1) / 7 + 1`; a day-of-year / day-of-month at a 7-boundary
    /// (the 7th) is the value where `(x-1)/7` differs from `(x+1)/7` and `x/7`, so it
    /// kills the `from_civil` week mutants (`- → +`, `- → /`).
    #[test]
    fn format_datetime_week_off_by_one_boundary() {
        // 2024-01-07: day-of-month 7, day-of-year 7.
        let f = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 7, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("W", &f).expect("W"), "1"); // (7-1)/7+1 = 1
        assert_eq!(format_datetime("WW", &f).expect("WW"), "01"); // (7-1)/7+1 = 1
        // Day 8 would give week 2 only if the `-1` is correct (8th is the start of
        // the 2nd 7-day group).
        let g = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 8, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("W", &g).expect("W8"), "2"); // (8-1)/7+1 = 2
    }

    /// `pad_num`'s `value < 0` sign branch: a ZERO-valued numeric field must render
    /// without a spurious sign (kills `< → <=` and `< → ==`, which would add `-` at
    /// zero). A NEGATIVE year exercises the sign branch itself.
    #[test]
    fn format_datetime_zero_and_negative_numbers() {
        // Midnight, minute/second zero → "00", never "-00".
        let f = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 15, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("HH24:MI:SS", &f).expect("zero"), "00:00:00");
        // A BC-ish negative year renders with a leading sign, then zero-padded to
        // the field width (`-{:04}` of 100 → "-0100") — the `value < 0` arm.
        let bc = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(-100, 6, 15, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("YYYY", &bc).expect("neg"), "-0100");
        // `CC` of year -100 exercises the `y < 1` (BC / year ≤ 0) century branch
        // `(y - 99) / 100` → -1, killing the century arithmetic mutants
        // (`- → +`, `- → /`, `/ → %`, `/ → *`).
        assert_eq!(format_datetime("CC", &bc).expect("cc"), "-01");
        // Year 1 is the `y < 1` boundary: it takes the AD branch `(1+99)/100 = 1`.
        // If the test were `<=` / `==` it would wrongly take the BC branch
        // `(1-99)/100 = 0`, so this pins the comparison.
        let ad1 =
            DateTimeFields::from_civil(jiff::civil::DateTime::constant(1, 6, 15, 0, 0, 0, 0), None);
        assert_eq!(format_datetime("CC", &ad1).expect("cc1"), "01");
        // Year 2024 stays in the AD branch (century 21) — covered above, repeated
        // here so the `else` branch's `+ 99` / `/ 100` are exercised on a positive.
        assert_eq!(
            format_datetime("CC", &fields_monday()).expect("cc2024"),
            "21"
        );
    }

    /// `offset_hh`'s `secs < 0` sign: a ZERO offset must render `+00`, not `-00`
    /// (kills `< → <=`).
    #[test]
    fn format_datetime_zero_offset_is_plus() {
        let f = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 15, 12, 0, 0, 0),
            Some(0),
        );
        assert_eq!(format_datetime("TZH", &f).expect("z"), "+00");
        assert_eq!(format_datetime("OF", &f).expect("of"), "+00");
        assert_eq!(format_datetime("TZ", &f).expect("tz"), "+00");
        // A NEGATIVE offset still renders the minus (the sign branch itself).
        let n = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 1, 15, 12, 0, 0, 0),
            Some(-3 * 3600),
        );
        assert_eq!(format_datetime("TZH", &n).expect("neg"), "-03");
    }

    /// The quoted-literal loop boundaries: an UNTERMINATED quote must not over-read
    /// (kills the `i < len` / `i + 1 < len` `< → <=` mutants, which would index past
    /// the end and panic), and a trailing `\` inside an unterminated quote is emitted
    /// literally (kills the escape-lookahead `+`/`<` mutants).
    #[test]
    fn format_datetime_unterminated_quote_does_not_overrun() {
        let f = fields_monday();
        let fmt = |t: &str| format_datetime(t, &f).expect(t);
        // Unterminated quote: everything after the opening quote is emitted verbatim.
        assert_eq!(fmt(r#""abc"#), "abc");
        // A trailing backslash with no following char (the escape lookahead's false
        // branch): the `\` is emitted literally.
        assert_eq!(fmt(r#""x\"#), "x\\");
        // A bare `FF` at end-of-string (no digit) must not over-read past the buffer
        // and falls through to two literal `F`s.
        assert_eq!(fmt("FF"), "FF");
    }

    /// The escape `i += 2` advance: an escaped quote NOT at index 2 (so `i*2 ≠ i+2`)
    /// followed by more content proves the index advances by exactly 2 (kills
    /// `+= → *=`).
    #[test]
    fn format_datetime_escaped_quote_advances_by_two() {
        let f = fields_monday();
        // `"ab\"c"`: the backslash is at index 3; after the escaped `"` the engine
        // must land on `c` (i += 2 → 5), not skip it (i *= 2 → 6).
        assert_eq!(format_datetime(r#""ab\"c""#, &f).expect("esc"), "ab\"c");
    }

    /// The `TH` `i += 2` advance: a `th` NOT at index 2 with trailing content proves
    /// the suffix advances the cursor by exactly 2 (kills `+= → *=`). `MMDDthMM`:
    /// after the `th` at index 4, the trailing `MM` must still render.
    #[test]
    fn format_datetime_th_advances_by_two() {
        let f = fields_monday(); // month 01, day 15
        // MM=01, DD=15, th (ordinal of 15) = "th", trailing MM=01.
        assert_eq!(format_datetime("MMDDthMM", &f).expect("th"), "0115th01");
    }

    /// `Q` uses `(month - 1) / 3 + 1`; month 3 is the value where the correct quarter
    /// (1) differs from every arithmetic mutant of that expression, killing the four
    /// `match_pattern` 1399 mutants (`- → +`, `- → /`, `/ → %`, `/ → *`).
    #[test]
    fn format_datetime_quarter_boundary() {
        let m3 = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 3, 15, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("Q", &m3).expect("q1"), "1"); // (3-1)/3+1 = 1
        let m7 = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(2024, 7, 15, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("Q", &m7).expect("q3"), "3"); // (7-1)/3+1 = 3
    }

    /// The `FF` bounds check `matches_at(.,"FF") && i + 2 < len && ...`: a non-`FF`
    /// 3-char run ending in a digit must NOT be rendered as fractional seconds
    /// (kills `&& → ||`), and an `FF` whose digit index is just past a `i * 2`
    /// boundary must still render (kills `+ → *` in the bounds check).
    #[test]
    fn format_datetime_ff_bounds_check() {
        let f = fields_monday(); // micros 500000
        // `&& → ||`: with OR, `AB3` (no FF) would wrongly trigger the FF render. The
        // `A`/`B`/`3` are literal passthrough.
        assert_eq!(format_datetime("AB3", &f).expect("ab3"), "AB3");
        // `+ → *`: place FF at index 4 (four literal dots), where `4 + 2 = 6 < 7` but
        // `4 * 2 = 8 ≥ 7`, so the `*` mutant would skip the FF render.
        assert_eq!(format_datetime("....FF3", &f).expect("ff"), "....500");
    }

    #[test]
    fn format_interval_uses_stored_fields() {
        use super::{Interval, format_interval};
        let fmt = |iv: Interval, t: &str| format_interval(iv, t).expect(t);
        // 36 hours: HH24 reads the micros component → 36 (not normalized to 1 day 12h).
        let h36 = Interval {
            months: 0,
            days: 0,
            micros: 36 * 3_600_000_000,
        };
        assert_eq!(fmt(h36, "HH24:MI:SS"), "36:00:00");
        // 1 day 02:03:04 → DD=01, HH24=02 (days stay separate from the clock).
        let d1 = Interval {
            months: 0,
            days: 1,
            micros: (2 * 3600 + 3 * 60 + 4) * 1_000_000,
        };
        assert_eq!(fmt(d1, "DD HH24:MI:SS"), "01 02:03:04");
    }

    #[test]
    fn format_interval_year_month_and_remainder() {
        use super::{Interval, format_interval};
        let fmt = |iv: Interval, t: &str| format_interval(iv, t).expect(t);
        // 14 months → YYYY = months/12 = 1, MM = months%12 = 02 (NOT carried as a year).
        let m14 = Interval {
            months: 14,
            days: 0,
            micros: 0,
        };
        assert_eq!(fmt(m14, "YYYY-MM"), "0001-02");
        // The full clock remainder past an over-24h hour: 25:30:45.123456.
        let clock = Interval {
            months: 0,
            days: 0,
            micros: 25 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000 + 123_456,
        };
        assert_eq!(fmt(clock, "HH24:MI:SS.US"), "25:30:45.123456");
        // Sub-second millis read from the micros remainder.
        assert_eq!(fmt(clock, "MS"), "123");
    }

    #[test]
    fn format_interval_negative_clock_decomposes_component_wise() {
        use super::{Interval, format_interval};
        // A wholly-negative clock interval (-02:03:04). PG `interval2tm` splits the
        // signed `micros` component-wise (`tm_hour`/`tm_min`/`tm_sec` each negative),
        // so each numeric clock field renders with its OWN sign — there is no single
        // factored leading minus. HH24 = micros/3_600_000_000 = -2; MI = -3; SS = -4;
        // `pad_num` keeps each sign and zero-pads the magnitude.
        // NOTE: the exact per-component-sign rendering is flagged for Task 9's PG
        // oracle pass; this pins the documented `interval2tm` decomposition contract.
        let neg = Interval {
            months: 0,
            days: 0,
            micros: -((2 * 3600 + 3 * 60 + 4) * 1_000_000),
        };
        assert_eq!(
            format_interval(neg, "HH24:MI:SS").expect("neg"),
            "-02:-03:-04"
        );
    }
}

#[cfg(test)]
mod interval_tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use super::*;

    fn h(i: &Interval) -> u64 {
        let mut s = DefaultHasher::new();
        i.hash(&mut s);
        s.finish()
    }

    #[test]
    fn interval_grouping_equality_uses_canonical_estimate() {
        let one_month = Interval {
            months: 1,
            days: 0,
            micros: 0,
        };
        let thirty_days = Interval {
            months: 0,
            days: 30,
            micros: 0,
        };
        assert_eq!(one_month, thirty_days);
        assert_eq!(h(&one_month), h(&thirty_days));
        let one_day = Interval {
            months: 0,
            days: 1,
            micros: 0,
        };
        let day_us = 86_400_000_000i64;
        let twentyfour_h = Interval {
            months: 0,
            days: 0,
            micros: day_us,
        };
        assert_eq!(one_day, twentyfour_h);
        assert_eq!(h(&one_day), h(&twentyfour_h));
        assert_ne!(one_month, one_day);
    }

    #[test]
    fn interval_ordering_is_by_canonical_estimate() {
        use std::cmp::Ordering;
        let a = Interval {
            months: 0,
            days: 1,
            micros: 0,
        };
        let b = Interval {
            months: 1,
            days: 0,
            micros: 0,
        };
        assert_eq!(a.cmp(&b), Ordering::Less);
        assert_eq!(a.canonical_micros(), 86_400_000_000i128);
        assert_eq!(b.canonical_micros(), 30 * 86_400_000_000i128);
    }
}

#[cfg(test)]
mod io_tests {
    use super::*;

    #[test]
    fn parse_and_format_date() {
        let d = parse_date("2024-02-29").expect("leap day");
        assert_eq!(date_to_text(d), "2024-02-29");
        assert!(matches!(
            parse_date("2023-02-29"),
            Err(crate::TypeError::DatetimeFieldOverflow { .. })
        ));
        assert!(matches!(
            parse_date("not-a-date"),
            Err(crate::TypeError::InvalidDatetimeFormat { .. })
        ));
    }

    #[test]
    fn parse_and_format_time_trims_subseconds() {
        assert_eq!(
            time_to_text(parse_time("12:34:56").expect("valid time")),
            "12:34:56"
        );
        assert_eq!(
            time_to_text(parse_time("12:34").expect("valid time")),
            "12:34:00"
        );
        assert_eq!(
            time_to_text(parse_time("01:02:03.450000").expect("valid time")),
            "01:02:03.45"
        );
    }

    /// Parsed date/time values must land on a microsecond, because that is all
    /// the wire and storage encodings carry: a sub-microsecond value would
    /// otherwise compare UNEQUAL to its stored form while encoding to identical
    /// bytes, which an equality-by-bytes index would conflate.
    #[test]
    fn parsed_clock_values_are_quantized_to_microseconds() {
        use assert2::assert;

        let tz = jiff::tz::TimeZone::UTC;
        let time = parse_time("00:00:00.0000001").expect("valid time");
        assert!(time == parse_time("00:00:00").expect("valid time"));
        assert!(time_to_binary(time) == time_to_binary(parse_time("00:00:00").expect("time")));
        assert!(
            time_from_binary(&time_to_binary(time)).expect("round trip") == time,
            "storage round trip is lossless"
        );
        // The last microsecond digit still survives.
        assert!(time_to_text(parse_time("00:00:00.999999").expect("time")) == "00:00:00.999999");

        let ts = parse_timestamp("2024-01-15 13:45:00.1234567").expect("valid timestamp");
        assert!(ts == parse_timestamp("2024-01-15 13:45:00.123457").expect("valid timestamp"));
        assert!(
            timestamp_from_binary(&timestamp_to_binary(ts)).expect("round trip") == ts,
            "storage round trip is lossless"
        );

        let tstz = parse_timestamptz("2024-01-15 13:45:00.1234567+00", &tz).expect("valid tstz");
        assert!(
            tstz == parse_timestamptz("2024-01-15 13:45:00.123457+00", &tz).expect("valid tstz")
        );
        assert!(
            timestamptz_from_binary(&timestamptz_to_binary(tstz)).expect("round trip") == tstz,
            "storage round trip is lossless"
        );
    }

    /// Fractional seconds beyond the sixth digit round the way PostgreSQL's
    /// `rint` on the microsecond value does — half to even, not truncated and
    /// not half-up. Every row was measured against PostgreSQL 18.4.
    #[test]
    fn fractional_seconds_round_half_to_even_like_postgres() {
        use assert2::assert;

        // (fraction digits, `time_out` of `time '00:00:00.<digits>'`)
        let cases = [
            ("0000001", "00:00:00"),
            ("0000004", "00:00:00"),
            // Exactly half: 0 is even, so it stays.
            ("0000005", "00:00:00"),
            ("0000006", "00:00:00.000001"),
            ("0000014", "00:00:00.000001"),
            // Exactly half: 1 is odd, so it climbs to 2.
            ("0000015", "00:00:00.000002"),
            ("0000016", "00:00:00.000002"),
            ("0000025", "00:00:00.000002"),
            ("0000035", "00:00:00.000004"),
            ("0000045", "00:00:00.000004"),
            ("0000055", "00:00:00.000006"),
            ("1234565", "00:00:00.123456"),
            ("1234575", "00:00:00.123458"),
            ("5000005", "00:00:00.5"),
            ("5000015", "00:00:00.500002"),
            // Just short of and just past half: the tie rule does not apply.
            ("00000049999", "00:00:00"),
            ("00000050001", "00:00:00.000001"),
            // Fractions longer than a double can hold digits.
            ("12345678901234", "00:00:00.123457"),
            ("0000000000001", "00:00:00"),
            ("999999", "00:00:00.999999"),
            ("9999994", "00:00:00.999999"),
            // Rounding up carries a whole second.
            ("9999995", "00:00:01"),
        ];

        for (digits, expected) in cases {
            let literal = format!("00:00:00.{digits}");
            let parsed = parse_time(&literal).expect("valid time");
            assert!(
                time_to_text(parsed) == expected,
                "time '{literal}' should render as {expected}"
            );
        }
    }

    /// Rounding can carry past the end of the day. PostgreSQL's `time` domain is
    /// closed at `24:00:00` and its `timestamp` range reaches 294276 AD, so it
    /// carries in both cases; crabka's representations stop one value earlier, so
    /// a timestamp carries the date and a `time` fails. Neither may wrap back to
    /// midnight of the day it started in — that is a different instant.
    #[test]
    fn rounding_across_midnight_carries_the_date_and_never_wraps() {
        use assert2::assert;

        let tz = jiff::tz::TimeZone::UTC;

        // Below the carry, the value is untouched.
        assert!(time_to_text(parse_time("23:59:59.9999994").expect("time")) == "23:59:59.999999");
        // PostgreSQL 18.4 answers `24:00:00` here; jiff's `Time` cannot hold it.
        assert!(let Err(TypeError::DatetimeFieldOverflow { .. }) = parse_time("23:59:59.9999995"));
        assert!(let Err(TypeError::DatetimeFieldOverflow { .. }) = parse_time("23:59:59.9999999"));

        // A timestamp has a date to carry into, exactly as PostgreSQL does.
        for literal in ["2024-01-01 23:59:59.9999995", "2024-01-01 23:59:59.9999999"] {
            let ts = parse_timestamp(literal).expect("valid timestamp");
            assert!(
                timestamp_to_text(ts) == "2024-01-02 00:00:00",
                "timestamp '{literal}' should carry into the next day"
            );
        }
        assert!(
            timestamp_to_text(parse_timestamp("2024-01-01 23:59:59.9999985").expect("timestamp"))
                == "2024-01-01 23:59:59.999998",
            "the tie below the carry still goes to the even neighbour"
        );

        // PostgreSQL 18.4 answers `10000-01-01 00:00:00`; jiff's `Date` ends at
        // 9999-12-31, so the carry has nowhere to go and must fail.
        assert!(
            timestamp_to_text(parse_timestamp("9999-12-31 23:59:59.9999994").expect("timestamp"))
                == "9999-12-31 23:59:59.999999"
        );
        assert!(
            let Err(TypeError::DatetimeFieldOverflow { .. }) =
                parse_timestamp("9999-12-31 23:59:59.9999999")
        );

        // `timestamptz` shares the parser, so it rounds identically.
        let tstz = parse_timestamptz("2024-06-01 12:00:00.9999995+00", &tz).expect("valid tstz");
        assert!(timestamptz_to_text(tstz, &tz) == "2024-06-01 12:00:01+00");
    }

    /// A fraction is only ever part of the seconds field. PostgreSQL reads
    /// `12:34.5` as `MM:SS.f`, which crabka does not implement, so it stays a
    /// rejection rather than silently becoming `12:34:00.5`.
    #[test]
    fn a_fraction_outside_the_seconds_field_is_rejected() {
        use assert2::assert;

        assert!(let Err(TypeError::InvalidDatetimeFormat { .. }) = parse_time("12:34.5"));
        assert!(let Err(TypeError::InvalidDatetimeFormat { .. }) = parse_time("12.5:34:56"));
        assert!(let Err(TypeError::InvalidDatetimeFormat { .. }) = parse_time("12:34:56."));
        assert!(let Err(TypeError::InvalidDatetimeFormat { .. }) = parse_time("12:34:56.5x"));
    }

    #[test]
    fn parse_and_format_timestamp() {
        let ts = parse_timestamp("2024-01-15 13:45:00").expect("valid timestamp");
        assert_eq!(timestamp_to_text(ts), "2024-01-15 13:45:00");
        let ts2 = parse_timestamp("2024-01-15T13:45:00.5").expect("valid timestamp");
        assert_eq!(timestamp_to_text(ts2), "2024-01-15 13:45:00.5");
    }

    #[test]
    fn parse_and_format_timestamptz_uses_session_zone() {
        let tz = jiff::tz::TimeZone::get("America/New_York").expect("tzdb has NY");
        let ts = parse_timestamptz("2024-01-15 12:00:00", &tz).expect("valid tstz");
        assert_eq!(timestamptz_to_text(ts, &tz), "2024-01-15 12:00:00-05");
        assert_eq!(
            timestamptz_to_text(ts, &jiff::tz::TimeZone::UTC),
            "2024-01-15 17:00:00+00"
        );
        let ts3 = parse_timestamptz("2024-01-15 12:00:00+02", &tz).expect("valid tstz");
        assert_eq!(
            timestamptz_to_text(ts3, &jiff::tz::TimeZone::UTC),
            "2024-01-15 10:00:00+00"
        );
    }

    #[test]
    fn parse_and_format_interval_postgres_style() {
        assert_eq!(
            interval_to_text(parse_interval("1 day").expect("valid interval")),
            "1 day"
        );
        assert_eq!(
            interval_to_text(parse_interval("1 year 2 months").expect("valid interval")),
            "1 year 2 mons"
        );
        assert_eq!(
            interval_to_text(parse_interval("3 days 04:05:06").expect("valid interval")),
            "3 days 04:05:06"
        );
        assert_eq!(
            interval_to_text(parse_interval("2 hours 30 minutes").expect("valid interval")),
            "02:30:00"
        );
        assert_eq!(
            interval_to_text(parse_interval("0 days").expect("valid interval")),
            "00:00:00"
        );
        assert_eq!(
            interval_to_text(parse_interval("-1 day").expect("valid interval")),
            "-1 days"
        );
    }

    // -----------------------------------------------------------------------
    // Teeth test: proves the CURRENT (unfixed) code saturates instead of
    // returning Err(Overflow).  After the fix this test must PASS.
    // -----------------------------------------------------------------------
    #[test]
    fn mul_interval_micros_overflow_is_caught() {
        // micros = i64::MAX, factor = 1000.0 → product ≈ 9.22e21, far above
        // i64::MAX; the fixed code must return Err(Overflow), not Ok with a
        // saturated i64::MAX value.
        let big = Interval {
            months: 0,
            days: 0,
            micros: i64::MAX,
        };
        assert!(
            matches!(mul_interval(big, 1000.0), Err(crate::TypeError::Overflow)),
            "expected Overflow but got a saturated Ok — fix the finite-range guard"
        );
    }

    #[test]
    fn binary_round_trips_through_pg_epoch() {
        let d = parse_date("2000-01-02").expect("valid date");
        assert_eq!(date_to_binary(d), 1i32.to_be_bytes());
        assert_eq!(date_from_binary(&date_to_binary(d)).expect("round-trip"), d);
        let i = Interval {
            months: 14,
            days: 3,
            micros: 4_000_000,
        };
        assert_eq!(
            interval_from_binary(&interval_to_binary(i)).expect("round-trip"),
            i
        );
    }

    /// Fuzz regression: every `*_from_binary` takes ARBITRARY bytes (from storage
    /// or a fuzzer) and must return `Ok`/`Err`, NEVER panic. The bug: a previous
    /// `timestamptz_from_binary` added the PG-epoch offset to `pg_micros` with an
    /// unchecked `+`, which overflowed i64 (panicking under overflow-checks) for
    /// boundary inputs like `i64::MAX`.
    #[test]
    fn from_binary_never_panics_on_adversarial_bytes() {
        let eights: [[u8; 8]; 5] = [
            [0xFF; 8],
            i64::MAX.to_be_bytes(),
            i64::MIN.to_be_bytes(),
            [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            [0; 8],
        ];
        for b in &eights {
            let _ = time_from_binary(b);
            let _ = timestamp_from_binary(b);
            let _ = timestamptz_from_binary(b);
        }
        for b in &[[0xFF; 4], [0x7F, 0xFF, 0xFF, 0xFF], [0x80, 0, 0, 0], [0; 4]] {
            let _ = date_from_binary(b);
        }
        for b in &[[0xFF; 16], [0; 16]] {
            let _ = interval_from_binary(b);
        }
        // The specific overflow boundary must be a clean Err, not a panic.
        assert!(timestamptz_from_binary(&i64::MAX.to_be_bytes()).is_err());
        assert!(timestamptz_from_binary(&i64::MIN.to_be_bytes()).is_err());
    }
}

// ---------------------------------------------------------------------------
// SP37: mutation-killing tests — every arithmetic operator, match arm, binary
// encode/decode, parse/format internal, and the Interval/Datum eq/hash impls is
// pinned to its exact PG-faithful value so a cargo-mutants edit (`* → /`,
// `+ → -`, `|| → &&`, a deleted match arm, a `[0;8]`/`Default` body) breaks an
// assertion. Values cross-checked against PostgreSQL semantics.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod mutation_tests {
    use super::*;

    fn iv(months: i32, days: i32, micros: i64) -> Interval {
        Interval {
            months,
            days,
            micros,
        }
    }

    // -- mul_interval / div_interval -------------------------------------

    #[test]
    fn mul_interval_scales_each_field_exactly() {
        // A clean ×2 with no spill: each field doubles.
        assert_eq!(
            mul_interval(iv(1, 2, 3_000_000), 2.0).expect("mul"),
            iv(2, 4, 6_000_000)
        );
    }

    #[test]
    fn mul_interval_spills_fractional_months_into_days_and_micros() {
        // months 3 × 1.5 = 4.5 → 4 months, 0.5*30 = 15 days carried down (line 61
        // `months_frac * 30.0`); days 4*1.5 = 6, +15 = 21 (line 62 the `+`);
        // micros 6_000_000 × 1.5 = 9_000_000.
        assert_eq!(
            mul_interval(iv(3, 4, 6_000_000), 1.5).expect("mul"),
            iv(4, 21, 9_000_000)
        );
    }

    #[test]
    fn mul_interval_spills_fractional_days_into_micros() {
        // months 0; days 1 × 0.5 = 0.5 → 0 days, 0.5 day = 43_200_000_000 µs spilt
        // down (lines 68/69 `days_frac * USECS_PER_DAY` + `micros*factor`).
        assert_eq!(
            mul_interval(iv(0, 1, 0), 0.5).expect("mul"),
            iv(0, 0, 43_200_000_000)
        );
    }

    #[test]
    fn div_interval_by_nonzero_divides_each_field() {
        // /4 = ×0.25: months 2 → 0.5 → 0 mon + 15 days; days 4×0.25 = 1, +15 = 16;
        // micros 6_000_000 × 0.25 = 1_500_000.
        assert_eq!(
            div_interval(iv(2, 4, 6_000_000), 4.0).expect("div"),
            iv(0, 16, 1_500_000)
        );
        // A pure-micros interval /2 halves the micros.
        assert_eq!(
            div_interval(iv(0, 0, 7_000_000), 2.0).expect("div"),
            iv(0, 0, 3_500_000)
        );
    }

    #[test]
    fn div_interval_by_zero_is_division_by_zero() {
        // Line 90 `divisor == 0.0`: a zero divisor is 22012, NOT a pass-through.
        assert!(matches!(
            div_interval(iv(1, 1, 1), 0.0),
            Err(TypeError::DivisionByZero)
        ));
        // A non-zero divisor must NOT error (guards `== → !=`).
        assert!(div_interval(iv(1, 1, 1), 2.0).is_ok());
    }

    // -- timestamp_plus_interval ----------------------------------------

    #[test]
    fn timestamp_plus_interval_applies_months_then_micros() {
        // Non-zero months AND micros so both `iv.months != 0` (line 131) and
        // `iv.micros != 0` (line 143) branches are taken.
        let base = parse_timestamp("2024-01-31 10:00:00").expect("ts");
        // +1 month lands on Feb 29 (2024 leap), + 90 min → 11:30.
        let got = timestamp_plus_interval(base, iv(1, 0, 90 * 60_000_000)).expect("ok");
        assert_eq!(
            timestamp_to_text(got),
            "2024-02-29 11:30:00",
            "months applied calendar-aware before the micros offset"
        );
        // Days branch too (line 137 already covered by other tests, pin here).
        let got2 = timestamp_plus_interval(
            parse_timestamp("2024-01-01 00:00:00").expect("ts"),
            iv(0, 3, 0),
        )
        .expect("ok");
        assert_eq!(timestamp_to_text(got2), "2024-01-04 00:00:00");
    }

    // -- time_plus_interval ---------------------------------------------

    #[test]
    fn time_plus_interval_micros_of_day_math_is_exact() {
        // Construct base micros-of-day (lines 178-181: the `+` chain and the
        // `* / 1000` subsec) and the wrap (lines 185/190/191).
        let t = parse_time("23:59:59.500000").expect("t");
        // + 2 s wraps past midnight to 00:00:01.5.
        let got = time_plus_interval(t, iv(0, 0, 2_000_000));
        assert_eq!(time_to_text(got), "00:00:01.5");
        // A mid-day shift that exercises the hour/min/sec split (lines 186-191).
        let t2 = parse_time("01:02:03").expect("t");
        let got2 = time_plus_interval(t2, iv(0, 0, 3_600_000_000 + 60_000_000 + 1_000_000));
        assert_eq!(time_to_text(got2), "02:03:04");
        // The interval's days/months are ignored (only micros matter): adding the
        // micros for "12:00" with months/days set still wraps on micros alone.
        let t3 = parse_time("12:00:00").expect("t");
        let got3 = time_plus_interval(t3, iv(5, 9, 0));
        assert_eq!(time_to_text(got3), "12:00:00");
    }

    // -- timestamptz_plus_interval --------------------------------------

    #[test]
    fn timestamptz_plus_interval_applies_calendar_and_micros() {
        let tz = TimeZone::UTC;
        let base = parse_timestamptz("2024-01-15 12:00:00", &tz).expect("base");
        // months != 0 (line 218 left), days != 0 (line 218 right), micros != 0
        // (line 227): +1 month +2 days +30 min.
        let got = timestamptz_plus_interval(base, iv(1, 2, 30 * 60_000_000), &tz).expect("ok");
        assert_eq!(timestamptz_to_text(got, &tz), "2024-02-17 12:30:00+00");
        // A pure-micros shift (months == 0 && days == 0, so the cal branch is the
        // `else`): +1 h.
        let got2 = timestamptz_plus_interval(base, iv(0, 0, 3_600_000_000), &tz).expect("ok");
        assert_eq!(timestamptz_to_text(got2, &tz), "2024-01-15 13:00:00+00");
        // months only (days == 0) — proves `||` is OR not AND (line 218): a value
        // with months but zero days must still apply the month.
        let got3 = timestamptz_plus_interval(base, iv(1, 0, 0), &tz).expect("ok");
        assert_eq!(timestamptz_to_text(got3, &tz), "2024-02-15 12:00:00+00");
        // days only (months == 0) — the other `||` operand.
        let got4 = timestamptz_plus_interval(base, iv(0, 5, 0), &tz).expect("ok");
        assert_eq!(timestamptz_to_text(got4, &tz), "2024-01-20 12:00:00+00");
    }

    // -- timestamptz_diff -----------------------------------------------

    #[test]
    fn timestamptz_diff_splits_days_and_remaining_micros() {
        // a − b a non-round number of µs apart: a = 2024-01-15 12:00:00.000000Z,
        // b = 2024-01-13 09:30:00.250000Z → 181_799_750_000 µs = 2 days +
        // 8_999_750_000 µs remainder (lines 241 `-`, 242 `/`, 243 `%`).
        let tz = TimeZone::UTC;
        let a = parse_timestamptz("2024-01-15 12:00:00", &tz).expect("a");
        let b = parse_timestamptz("2024-01-13 09:30:00.250000", &tz).expect("b");
        assert_eq!(timestamptz_diff(a, b), iv(0, 2, 8_999_750_000));
        // The reverse is the negation (proves the `-` is a real subtraction).
        assert_eq!(timestamptz_diff(b, a), iv(0, -2, -8_999_750_000));
    }

    // -- Interval Hash / PartialOrd -------------------------------------

    #[test]
    fn interval_hash_and_partial_cmp_use_canonical_estimate() {
        use std::{
            cmp::Ordering,
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };
        fn h(i: &Interval) -> u64 {
            let mut s = DefaultHasher::new();
            i.hash(&mut s);
            s.finish()
        }
        // Canonically-equal intervals (1 month == 30 days) hash equally — kills
        // the `hash with ()` mutant (line 279) only if UNEQUAL intervals hash
        // DIFFERENTLY, so also assert that.
        assert_eq!(h(&iv(1, 0, 0)), h(&iv(0, 30, 0)));
        assert_ne!(h(&iv(1, 0, 0)), h(&iv(0, 1, 0)));
        // partial_cmp returns a real ordering (kills `partial_cmp -> None`, line 284).
        assert_eq!(iv(0, 1, 0).partial_cmp(&iv(1, 0, 0)), Some(Ordering::Less));
        assert_eq!(
            iv(1, 0, 0).partial_cmp(&iv(0, 1, 0)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            iv(0, 30, 0).partial_cmp(&iv(1, 0, 0)),
            Some(Ordering::Equal)
        );
        assert!(iv(0, 1, 0) < iv(1, 0, 0));
        assert!(iv(1, 0, 0) > iv(0, 1, 0));
    }

    // -- pg_epoch_datetime ----------------------------------------------

    #[test]
    fn pg_epoch_datetime_is_2000_01_01_midnight() {
        // Kills `pg_epoch_datetime -> Default::default()` (line 306): the PG epoch
        // is 2000-01-01, not jiff's default (0001-01-01 wall-clock zero). Observed
        // via timestamp_to_binary, whose reference point IS pg_epoch_datetime.
        let one_sec_after =
            parse_timestamp("2000-01-01 00:00:01").expect("ts one second after epoch");
        assert_eq!(
            timestamp_to_binary(one_sec_after),
            1_000_000i64.to_be_bytes(),
            "1 s after the PG epoch is 1_000_000 µs"
        );
    }

    // -- time_to_binary / time_from_binary ------------------------------

    #[test]
    fn time_to_binary_exact_bytes_and_round_trip() {
        // Known case: 00:00:01 → 1_000_000 µs (sanity-checked PG-correct).
        assert_eq!(
            time_to_binary(parse_time("00:00:01").expect("t")),
            1_000_000i64.to_be_bytes()
        );
        // Non-trivial: 13:45:06.123456 → 49_506_123_456 µs (lines 428-431 each
        // `*`/`+`/`/`). Wrong arithmetic ⇒ wrong bytes.
        let t = parse_time("13:45:06.123456").expect("t");
        assert_eq!(time_to_binary(t), 49_506_123_456i64.to_be_bytes());
        // Round-trip a non-trivial value (lines 442-448).
        assert_eq!(time_from_binary(&time_to_binary(t)).expect("round-trip"), t);
        // And from a known byte vector back to the exact time.
        assert_eq!(
            time_from_binary(&49_506_123_456i64.to_be_bytes()).expect("from"),
            t
        );
    }

    // -- timestamp_to_binary / timestamp_from_binary --------------------

    #[test]
    fn timestamp_to_binary_exact_bytes_and_round_trip() {
        // 2024-07-15 13:45:06.5 → 774_366_306_500_000 µs since the PG epoch.
        let ts = parse_timestamp("2024-07-15 13:45:06.5").expect("ts");
        assert_eq!(
            timestamp_to_binary(ts),
            774_366_306_500_000i64.to_be_bytes()
        );
        assert_eq!(
            timestamp_from_binary(&timestamp_to_binary(ts)).expect("round-trip"),
            ts
        );
        // A known byte vector decodes to the exact timestamp.
        assert_eq!(
            timestamp_from_binary(&774_366_306_500_000i64.to_be_bytes()).expect("from"),
            ts
        );
    }

    // -- timestamptz_to_binary / timestamptz_from_binary ----------------

    #[test]
    fn timestamptz_to_binary_exact_bytes_and_round_trip() {
        // Instant 2024-01-15 12:00:00 UTC → 758_635_200_000_000 µs since the PG
        // epoch (lines 649 `-`/`*`, 661 `+`/`*`).
        let tz = TimeZone::UTC;
        let ts = parse_timestamptz("2024-01-15 12:00:00", &tz).expect("tstz");
        assert_eq!(
            timestamptz_to_binary(ts),
            758_635_200_000_000i64.to_be_bytes()
        );
        assert_eq!(
            timestamptz_from_binary(&timestamptz_to_binary(ts)).expect("round-trip"),
            ts
        );
        assert_eq!(
            timestamptz_from_binary(&758_635_200_000_000i64.to_be_bytes()).expect("from"),
            ts
        );
        // An off-UTC instant also round-trips (the offset is absorbed into the
        // absolute instant, so the bytes match the equivalent UTC instant).
        let tz_ny = TimeZone::get("America/New_York").expect("NY");
        let ts2 = parse_timestamptz("2024-01-15 07:00:00", &tz_ny).expect("NY 07:00 = 12:00 UTC");
        assert_eq!(timestamptz_to_binary(ts2), timestamptz_to_binary(ts));
    }

    // -- parse_offset_str -----------------------------------------------

    #[test]
    fn parse_offset_str_spellings_and_sign() {
        let tz = TimeZone::UTC;
        // Helper: parse a tstz with an explicit offset and read back the instant
        // via UTC text, so the offset's effect on the instant is observable.
        let inst = |lit: &str| {
            timestamptz_to_text(
                parse_timestamptz(&format!("2024-01-15 12:00:00{lit}"), &tz).expect("parse"),
                &tz,
            )
        };
        // +05 → instant is 07:00 UTC (subtract the offset).
        assert_eq!(inst("+05"), "2024-01-15 07:00:00+00");
        // +0530 (colon-less HHMM, the `4 =>` arm) → 06:30 UTC.
        assert_eq!(inst("+0530"), "2024-01-15 06:30:00+00");
        // +053045 (colon-less HHMMSS, the `6 =>` arm) → 06:29:15 UTC.
        assert_eq!(inst("+053045"), "2024-01-15 06:29:15+00");
        // +05:30 (colon path) → 06:30 UTC.
        assert_eq!(inst("+05:30"), "2024-01-15 06:30:00+00");
        // +05:30:45 (HH:MM:SS colon path) → 06:29:15 UTC.
        assert_eq!(inst("+05:30:45"), "2024-01-15 06:29:15+00");
        // -08 → 20:00 UTC (the `b'-'` arm + the negative sign, lines 585/609).
        assert_eq!(inst("-08"), "2024-01-15 20:00:00+00");
        // Z → UTC, unchanged.
        assert_eq!(inst("Z"), "2024-01-15 12:00:00+00");
    }

    // -- push_offset ----------------------------------------------------

    #[test]
    fn push_offset_renders_hh_mm_ss_only_when_nonzero() {
        let mut s = String::new();
        push_offset(
            &mut s,
            Offset::from_seconds(5 * 3600 + 30 * 60).expect("off"),
        );
        assert_eq!(s, "+05:30");
        let mut s = String::new();
        push_offset(
            &mut s,
            Offset::from_seconds(5 * 3600 + 30 * 60 + 45).expect("off"),
        );
        assert_eq!(s, "+05:30:45");
        // Whole-hour offset prints only `±HH` (mins == 0 && secs == 0 → the `||`
        // is false, line 635).
        let mut s = String::new();
        push_offset(&mut s, Offset::from_seconds(5 * 3600).expect("off"));
        assert_eq!(s, "+05");
        // Negative offset.
        let mut s = String::new();
        push_offset(&mut s, Offset::from_seconds(-8 * 3600).expect("off"));
        assert_eq!(s, "-08");
        // Seconds-only-nonzero exercises the inner `secs != 0` branch (line 637)
        // and the `mins` div/rem (line 631).
        let mut s = String::new();
        push_offset(&mut s, Offset::from_seconds(45).expect("off"));
        assert_eq!(s, "+00:00:45");
    }

    // -- parse_clock_term -----------------------------------------------

    #[test]
    fn parse_clock_term_negative_and_fraction() {
        // A clock-only interval observed via parse_interval / interval_to_text.
        // Negative clock term (line 727 `-` prefix): -1:02:03 → -3_723_000_000 µs.
        assert_eq!(
            parse_interval("-1:02:03").expect("iv"),
            iv(0, 0, -3_723_000_000)
        );
        // Positive with fraction (the frac-pad loop, line 739 `< 6`): 1:02:03.5 →
        // 3_723_500_000 µs (the `.5` pads to 500000 µs).
        assert_eq!(
            parse_interval("1:02:03.5").expect("iv"),
            iv(0, 0, 3_723_500_000)
        );
        // The total combines h/m/s/frac additively (line 753 `+`).
        assert_eq!(
            parse_interval("00:00:01.000001").expect("iv"),
            iv(0, 0, 1_000_001)
        );
    }

    // -- accumulate_unit (every unit + fraction spill) -------------------

    #[test]
    fn accumulate_unit_every_term() {
        // year / yr (lines 771-775): whole×12 + round(frac×12).
        assert_eq!(parse_interval("2 years").expect("iv"), iv(24, 0, 0));
        assert_eq!(parse_interval("0.5 year").expect("iv"), iv(6, 0, 0)); // frac→months
        // month / mon (lines 777-782): whole + frac×30 days + sub-day µs.
        assert_eq!(parse_interval("3 months").expect("iv"), iv(3, 0, 0));
        assert_eq!(parse_interval("1.5 months").expect("iv"), iv(1, 15, 0)); // .5*30=15 days
        // week / wk (line 784-786): whole×7 days; fractional → µs.
        assert_eq!(parse_interval("2 weeks").expect("iv"), iv(0, 14, 0));
        assert_eq!(
            parse_interval("1.5 wk").expect("iv"),
            iv(0, 7, 302_400_000_000) // .5 wk = 3.5 days = 302_400_000_000 µs
        );
        // day (line 788-790).
        assert_eq!(parse_interval("4 days").expect("iv"), iv(0, 4, 0));
        assert_eq!(
            parse_interval("0.5 day").expect("iv"),
            iv(0, 0, 43_200_000_000)
        );
        // hour / hr / h (line 792-794).
        assert_eq!(
            parse_interval("2 hours").expect("iv"),
            iv(0, 0, 7_200_000_000)
        );
        assert_eq!(
            parse_interval("1.5 hr").expect("iv"),
            iv(0, 0, 5_400_000_000)
        );
        // minute / min / m (line 796-798).
        assert_eq!(
            parse_interval("90 minutes").expect("iv"),
            iv(0, 0, 5_400_000_000)
        );
        assert_eq!(
            parse_interval("2.5 min").expect("iv"),
            iv(0, 0, 150_000_000)
        );
        // second / sec / s (line 800-802).
        assert_eq!(
            parse_interval("3 seconds").expect("iv"),
            iv(0, 0, 3_000_000)
        );
        assert_eq!(parse_interval("2.5 sec").expect("iv"), iv(0, 0, 2_500_000));
        // millisecond / msec (line 804-805). NOTE: the literal "ms" trims to "m"
        // (minute) under trim_end_matches('s'), so the millisecond arm is reached
        // via "millisecond"/"msec" — those spellings pin the arm.
        assert_eq!(
            parse_interval("500 milliseconds").expect("iv"),
            iv(0, 0, 500_000)
        );
        assert_eq!(parse_interval("2.5 msec").expect("iv"), iv(0, 0, 2_500));
        // microsecond / usec (line 807-808). Likewise "us" trims to "u" (unknown);
        // the arm is reached via "microsecond"/"usec".
        assert_eq!(
            parse_interval("123 microseconds").expect("iv"),
            iv(0, 0, 123)
        );
        assert_eq!(parse_interval("7 usec").expect("iv"), iv(0, 0, 7));
        // An unknown unit is rejected (the `_ => None` arm).
        assert!(parse_interval("3 fortnights").is_err());
    }

    #[test]
    fn accumulate_unit_year_term_does_not_clobber_prior_micros() {
        // Line 774 is `*micros += 0` in the year arm. A prior term sets micros
        // non-zero; the `year` term must LEAVE it (`+= 0`), not `*= 0` (wipe it).
        // "2 hours 1 year": hours set 7_200_000_000 µs, the year adds 12 months
        // and must NOT zero the µs.
        assert_eq!(
            parse_interval("2 hours 1 year").expect("iv"),
            iv(12, 0, 7_200_000_000),
            "the year arm's `*micros += 0` must not wipe accumulated micros"
        );
    }

    #[test]
    fn accumulate_unit_fractional_month_spills_subday_into_micros() {
        // Line 780-782: a fractional month whose 30-day spill has a SUB-DAY
        // remainder exercises the micros line (line 782). 1.05 months → frac 0.05
        // × 30 = 1.5 days → 1 day + 0.5 day = 43_200_000_000 µs. A zero-remainder
        // fraction (like 1.5 months → exactly 15 days) leaves line 782 adding 0,
        // so use a remainder-bearing fraction to give that line teeth.
        assert_eq!(
            parse_interval("1.05 months").expect("iv"),
            iv(1, 1, 43_200_000_000)
        );
    }

    #[test]
    fn parse_interval_clock_then_pair_advances_index() {
        // A clock term FOLLOWED by a `<qty> <unit>` pair: the clock advances `i`
        // (line 698) to 1, then the pair's `i += 2` (line 705) lands on len. Under
        // `i *= 2` the pair would jump to `1*2 = 2`, leaving "day" to be parsed as a
        // quantity → an error; the real `i += 2` reaches len cleanly.
        assert_eq!(
            parse_interval("01:00:00 1 day").expect("iv"),
            iv(0, 1, 3_600_000_000)
        );
    }

    // -- parse_interval multi-term (also converts the two += timeouts) ---

    #[test]
    fn parse_interval_multi_term_accumulates_each_field() {
        // Each term ADDS to its field (lines 698/705 `+=`, NOT `*=`): a multi-term
        // interval sums months (1y2mo = 14), days (3), micros (4h5m6s).
        let got = parse_interval("1 year 2 months 3 days 4 hours 5 minutes 6 seconds").expect("iv");
        assert_eq!(got, iv(14, 3, 14_706_000_000));
        // Two clock terms accumulate via the `micros += parse_clock_term` path
        // (line 698): 01:00:00 + 00:30:00.
        assert_eq!(
            parse_interval("01:00:00 00:30:00").expect("iv"),
            iv(0, 0, 5_400_000_000)
        );
    }

    // -- format_clock ---------------------------------------------------

    #[test]
    fn format_clock_signs_and_subsecond() {
        // hours ≥ 10 (kills the `* / 1000` subsec mutant at line 869, and the
        // `< 0` sign test at line 856): 10:00:00.5.
        assert_eq!(
            interval_to_text(iv(0, 0, 10 * 3_600_000_000 + 500_000)),
            "10:00:00.5"
        );
        // Negative whole-hour clock prints `-01:00:00` (line 856 `< 0`).
        assert_eq!(interval_to_text(iv(0, 0, -3_600_000_000)), "-01:00:00");
        // Negative mixed clock.
        assert_eq!(
            interval_to_text(iv(0, 0, -(3_600_000_000 + 2 * 60_000_000 + 3 * 1_000_000))),
            "-01:02:03"
        );
        // A positive sub-second-only clock (the subsec multiply, line 869).
        assert_eq!(interval_to_text(iv(0, 0, 250_000)), "00:00:00.25");
    }
}

#[cfg(test)]
mod parse_template_tests {
    #[test]
    fn parse_by_template_extracts_fields() {
        use super::parse_by_template;
        let p = parse_by_template("YYYY-MM-DD HH24:MI:SS", "2024-01-15 13:45:06").expect("p");
        assert_eq!((p.year, p.month, p.day), (2024, 1, 15));
        assert_eq!((p.hour, p.minute, p.second), (13, 45, 6));
        // month name + 12-hour + meridiem
        let q = parse_by_template("Mon DD YYYY HH12:MI PM", "Jul 04 2024 01:30 PM").expect("q");
        assert_eq!(
            (q.year, q.month, q.day, q.hour, q.minute),
            (2024, 7, 4, 13, 30)
        );
        // absent fields default (PG): year→1, month→1, day→1, time→0.
        let d = parse_by_template("YYYY", "2030").expect("d");
        assert_eq!((d.year, d.month, d.day, d.hour), (2030, 1, 1, 0));
    }

    #[test]
    fn parse_by_template_errors() {
        use super::parse_by_template;
        // non-digit where a digit is required → 22007.
        assert_eq!(
            parse_by_template("YYYY-MM-DD", "abcd-01-01")
                .expect_err("non-digit")
                .sqlstate(),
            "22007"
        );
        // out-of-range field → 22008.
        assert_eq!(
            parse_by_template("YYYY-MM-DD", "2024-13-01")
                .expect_err("month 13")
                .sqlstate(),
            "22008"
        );
    }

    #[test]
    fn parse_by_template_meridiem_conversions() {
        use super::parse_by_template;
        // 12 AM → 0 (midnight).
        let mid = parse_by_template("HH12:MI AM", "12:00 AM").expect("mid");
        assert_eq!((mid.hour, mid.minute), (0, 0));
        // 12 PM → 12 (noon).
        let noon = parse_by_template("HH12:MI PM", "12:00 PM").expect("noon");
        assert_eq!(noon.hour, 12);
        // 11 PM → 23.
        let eve = parse_by_template("HH12 PM", "11 PM").expect("eve");
        assert_eq!(eve.hour, 23);
        // lowercase meridiem accepted.
        let low = parse_by_template("HH12:MI am", "07:15 am").expect("am");
        assert_eq!((low.hour, low.minute), (7, 15));
        // dotted meridiem accepted.
        let dot = parse_by_template("HH12 P.M.", "03 P.M.").expect("dot");
        assert_eq!(dot.hour, 15);
        // No meridiem: HH12 value used as-is (PG: 13 stays 13 in HH12 w/o AM/PM).
        let raw = parse_by_template("HH12:MI:SS", "13:05:09").expect("raw");
        assert_eq!((raw.hour, raw.minute, raw.second), (13, 5, 9));
    }

    #[test]
    fn parse_by_template_full_month_name_and_us() {
        use super::parse_by_template;
        // Full month name (longest match, case-insensitive).
        let m = parse_by_template("Month DD, YYYY", "September 09, 1999").expect("m");
        assert_eq!((m.year, m.month, m.day), (1999, 9, 9));
        let m2 = parse_by_template("MONTH", "DECEMBER").expect("m2");
        assert_eq!(m2.month, 12);
        // Microseconds.
        let us = parse_by_template("HH24:MI:SS.US", "01:02:03.123456").expect("us");
        assert_eq!(
            (us.hour, us.minute, us.second, us.micros),
            (1, 2, 3, 123456)
        );
    }

    #[test]
    fn parse_by_template_leniency() {
        use super::parse_by_template;
        // PG is lenient about separators: a slash template against dashes still parses.
        let p = parse_by_template("YYYY/MM/DD", "2024-01-15").expect("p");
        assert_eq!((p.year, p.month, p.day), (2024, 1, 15));
        // Fewer digits than the field width are accepted when a non-digit follows.
        let q = parse_by_template("YYYY-MM-DD", "2024-1-5").expect("q");
        assert_eq!((q.month, q.day), (1, 5));
        // A quoted literal run in the template is skipped over the matching input.
        let r = parse_by_template("YYYY\"-the-\"MM", "2024-the-07").expect("r");
        assert_eq!((r.year, r.month), (2024, 7));
    }

    #[test]
    fn parse_by_template_range_errors() {
        use super::parse_by_template;
        // hour 24 (after no meridiem) is out of range → 22008.
        assert_eq!(
            parse_by_template("HH24:MI", "24:00")
                .expect_err("hour 24")
                .sqlstate(),
            "22008"
        );
        // minute 60 → 22008.
        assert_eq!(
            parse_by_template("MI", "60")
                .expect_err("minute 60")
                .sqlstate(),
            "22008"
        );
        // day 0 → 22008.
        assert_eq!(
            parse_by_template("DD", "00").expect_err("day 0").sqlstate(),
            "22008"
        );
        // An unrecognized month name → 22007 (bad shape, no digits to consume).
        assert_eq!(
            parse_by_template("Mon", "Xyz")
                .expect_err("bad month name")
                .sqlstate(),
            "22007"
        );
    }

    #[test]
    fn parse_by_template_milliseconds_scale_to_micros() {
        use super::parse_by_template;
        // The `MS` (milliseconds) pattern consumes up to 3 digits and scales them to
        // microseconds (×1000): 123 ms → 123_000 µs.
        let p = parse_by_template("HH24:MI:SS.MS", "01:02:03.123").expect("ms");
        assert_eq!((p.hour, p.minute, p.second, p.micros), (1, 2, 3, 123_000));
    }

    #[test]
    fn parse_by_template_day_name_is_skipped() {
        use super::parse_by_template;
        // A `Day`/`Dy` day-of-week NAME pattern is accepted and skipped without setting
        // any field; the remaining month/day/year fields are still extracted correctly.
        let p =
            parse_by_template("Day, Month DD, YYYY", "Monday, July 04, 2024").expect("day name");
        assert_eq!((p.year, p.month, p.day), (2024, 7, 4));
        // Defaults for the unset time fields are unchanged (no field corruption).
        assert_eq!((p.hour, p.minute, p.second, p.micros), (0, 0, 0, 0));
    }
}

#[cfg(test)]
mod make_justify_tests {
    #[test]
    fn make_constructors() {
        use super::{Interval, make_date, make_interval, make_time, make_timestamp_civil};
        assert_eq!(
            make_date(2024, 7, 4).expect("d"),
            jiff::civil::date(2024, 7, 4)
        );
        // make_time(hour, min, sec) — fractional seconds → micros.
        assert_eq!(
            make_time(13, 45, 6.5).expect("t"),
            jiff::civil::time(13, 45, 6, 500_000_000)
        );
        assert_eq!(
            make_timestamp_civil(2024, 7, 4, 13, 45, 6.0).expect("ts"),
            jiff::civil::datetime(2024, 7, 4, 13, 45, 6, 0)
        );
        // make_interval positional: 1 year, 2 months, 0 weeks, 3 days.
        assert_eq!(
            make_interval(1, 2, 0, 3, 0, 0, 0.0).expect("iv"),
            Interval {
                months: 14,
                days: 3,
                micros: 0
            }
        );
        // weeks fold into days; fractional secs into micros.
        assert_eq!(
            make_interval(0, 0, 2, 0, 0, 0, 1.5).expect("iv"),
            Interval {
                months: 0,
                days: 14,
                micros: 1_500_000
            }
        );
        // out-of-range field → 22008.
        assert_eq!(
            make_date(2024, 13, 1).expect_err("month 13").sqlstate(),
            "22008"
        );
    }

    #[test]
    fn make_boundary_and_overflow() {
        use super::{Interval, make_interval, make_time};
        // make_time(24, 0, 0) is out of range → 22008.
        assert_eq!(
            make_time(24, 0, 0.0).expect_err("hour 24").sqlstate(),
            "22008"
        );
        // A negative interval field is representable (PG allows negative make_interval).
        assert_eq!(
            make_interval(0, -1, 0, 0, -2, 0, 0.0).expect("neg"),
            Interval {
                months: -1,
                days: 0,
                micros: -2 * 3_600_000_000
            }
        );
        // years*12 overflowing i32 → 22008.
        assert_eq!(
            make_interval(i32::MAX, 1, 0, 0, 0, 0, 0.0)
                .expect_err("months overflow")
                .sqlstate(),
            "22008"
        );
    }

    #[test]
    fn justify_helpers() {
        use super::{Interval, justify_days, justify_hours, justify_interval};
        // 35 days → 1 month 5 days.
        assert_eq!(
            justify_days(Interval {
                months: 0,
                days: 35,
                micros: 0
            })
            .expect("35 days justifies in range"),
            Interval {
                months: 1,
                days: 5,
                micros: 0
            }
        );
        // 27 hours → 1 day 3 hours.
        assert_eq!(
            justify_hours(Interval {
                months: 0,
                days: 0,
                micros: 27 * 3_600_000_000
            })
            .expect("27 hours justifies in range"),
            Interval {
                months: 0,
                days: 1,
                micros: 3 * 3_600_000_000
            }
        );
        // PG: justify_interval('1 mon -1 hour') = '29 days 23:00:00'.
        assert_eq!(
            justify_interval(Interval {
                months: 1,
                days: 0,
                micros: -3_600_000_000
            })
            .expect("'1 mon -1 hour' justifies in range"),
            Interval {
                months: 0,
                days: 29,
                micros: 23 * 3_600_000_000
            }
        );
        // The symmetric mixed-sign case: '-1 mon +1 hour' → '-29 days -23:00:00'.
        assert_eq!(
            justify_interval(Interval {
                months: -1,
                days: 0,
                micros: 3_600_000_000
            })
            .expect("'-1 mon +1 hour' justifies in range"),
            Interval {
                months: 0,
                days: -29,
                micros: -23 * 3_600_000_000
            }
        );
    }

    // Each `justify_*` narrows an i64 month/day roll back to an i32 field; an
    // input whose fields sit near i32::MAX overflows that narrowing (the old
    // unchecked code panicked in debug / wrapped in release / silently truncated
    // the i64→i32 cast). PG 15+ raises `ERROR: interval out of range` (22008).
    #[test]
    fn justify_days_overflow_is_22008() {
        use super::{Interval, justify_days};
        // months + days/30 ≈ i32::MAX + 71_582_788 overflows the i32 narrowing.
        let err = justify_days(Interval {
            months: i32::MAX,
            days: i32::MAX,
            micros: 0,
        })
        .expect_err("near-i32::MAX months+days overflows justify_days");
        assert_eq!(err.sqlstate(), "22008");
    }

    #[test]
    fn justify_hours_overflow_is_22008() {
        use super::{Interval, justify_hours};
        // days + micros/USECS_PER_DAY ≈ i32::MAX + 106_751_991 overflows the i32
        // narrowing.
        let err = justify_hours(Interval {
            months: 0,
            days: i32::MAX,
            micros: i64::MAX,
        })
        .expect_err("near-i32::MAX days plus a full i64 of micros overflows justify_hours");
        assert_eq!(err.sqlstate(), "22008");
    }

    #[test]
    fn justify_interval_overflow_is_22008() {
        use super::{Interval, justify_interval};
        // After rolling micros→days→months the month total exceeds i32::MAX; the
        // old `months as i32` silently wrapped, the checked narrowing now errors.
        let err = justify_interval(Interval {
            months: i32::MAX,
            days: i32::MAX,
            micros: i64::MAX,
        })
        .expect_err("rolled month total exceeds i32 in justify_interval");
        assert_eq!(err.sqlstate(), "22008");
    }
}
