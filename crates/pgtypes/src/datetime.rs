//! SP37: date/time *values*: the `Interval` type plus parsing, formatting,
//! binary encodings, and value arithmetic. PostgreSQL semantics; `jiff` does the
//! calendar/timezone math. This is the single source of truth for date/time
//! values (the SP32 `numeric` module pattern).

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible date/time semantics kept structurally close to donor"
)]

use jiff::{
    Span, Timestamp, ToSpan,
    civil::{Date, DateTime, Time},
    tz::{Offset, TimeZone},
};

use crate::TypeError;

mod parse;

pub use self::parse::{
    DateOrder, DecodeError, Decoded, Parts, Special, Zone, decode, resolve_time_zone,
};

// ---------------------------------------------------------------------------
// Non-finite values. `date`, `timestamp`, `timestamptz` and `interval` each have
// a `+infinity` and a `-infinity` that sort outside every finite value and are
// carried through arithmetic rather than computed with. PostgreSQL reserves the
// extreme representable value of each type's storage for them; crabka does the
// same, so ordering, equality, grouping and index keys all come out right with
// no extra case in the comparison paths.
//
// The reserved civil values sit outside PostgreSQL's own finite range
// (4713-11-24 BC .. 5874897-12-31) at the low end and at the very top of jiff's
// range at the high end, so a finite literal can never land on one.
// ---------------------------------------------------------------------------

/// `date 'infinity'`.
pub const DATE_INFINITY: Date = Date::MAX;
/// `date '-infinity'`.
pub const DATE_NEG_INFINITY: Date = Date::MIN;
/// `timestamp 'infinity'`.
pub const TIMESTAMP_INFINITY: DateTime = DateTime::MAX;
/// `timestamp '-infinity'`.
pub const TIMESTAMP_NEG_INFINITY: DateTime = DateTime::MIN;

/// Whether a `date` is one of the two non-finite values.
#[must_use]
pub fn date_is_infinite(d: Date) -> bool {
    d == DATE_INFINITY || d == DATE_NEG_INFINITY
}

/// Whether a `timestamp` is one of the two non-finite values.
#[must_use]
pub fn timestamp_is_infinite(ts: DateTime) -> bool {
    ts == TIMESTAMP_INFINITY || ts == TIMESTAMP_NEG_INFINITY
}

/// `timestamptz 'infinity'`.
#[must_use]
pub fn timestamptz_infinity() -> Timestamp {
    Timestamp::MAX
}

/// `timestamptz '-infinity'`.
#[must_use]
pub fn timestamptz_neg_infinity() -> Timestamp {
    Timestamp::MIN
}

/// Whether a `timestamptz` is one of the two non-finite values.
#[must_use]
pub fn timestamptz_is_infinite(ts: Timestamp) -> bool {
    ts == Timestamp::MAX || ts == Timestamp::MIN
}

/// The sign of a non-finite value: `1` for `infinity`, `-1` for `-infinity`,
/// `0` for anything finite. Lets the arithmetic paths branch once.
#[must_use]
pub fn date_infinite_sign(d: Date) -> i32 {
    if d == DATE_INFINITY {
        1
    } else if d == DATE_NEG_INFINITY {
        -1
    } else {
        0
    }
}

/// [`date_infinite_sign`] for `timestamp`.
#[must_use]
pub fn timestamp_infinite_sign(ts: DateTime) -> i32 {
    if ts == TIMESTAMP_INFINITY {
        1
    } else if ts == TIMESTAMP_NEG_INFINITY {
        -1
    } else {
        0
    }
}

/// [`date_infinite_sign`] for `timestamptz`.
#[must_use]
pub fn timestamptz_infinite_sign(ts: Timestamp) -> i32 {
    if ts == Timestamp::MAX {
        1
    } else if ts == Timestamp::MIN {
        -1
    } else {
        0
    }
}

/// The `timestamp` of the given sign: `+1` → `infinity`, `-1` → `-infinity`.
#[must_use]
pub fn timestamp_infinity_of_sign(sign: i32) -> DateTime {
    if sign >= 0 {
        TIMESTAMP_INFINITY
    } else {
        TIMESTAMP_NEG_INFINITY
    }
}

/// The `date` of the given sign.
#[must_use]
pub fn date_infinity_of_sign(sign: i32) -> Date {
    if sign >= 0 {
        DATE_INFINITY
    } else {
        DATE_NEG_INFINITY
    }
}

/// The `timestamptz` of the given sign.
#[must_use]
pub fn timestamptz_infinity_of_sign(sign: i32) -> Timestamp {
    if sign >= 0 {
        Timestamp::MAX
    } else {
        Timestamp::MIN
    }
}

// ---------------------------------------------------------------------------
// Value-level arithmetic helpers (called from crabka_pgtypes::ops)
// ---------------------------------------------------------------------------

/// `interval out of range`: the 22008 PostgreSQL raises when two opposite
/// infinities would have to cancel.
fn interval_out_of_range() -> TypeError {
    TypeError::DatetimeOutOfRange {
        message: "interval out of range".to_string(),
    }
}

/// `timestamp out of range`: the 22008 for the timestamp equivalent.
fn timestamp_out_of_range() -> TypeError {
    TypeError::DatetimeOutOfRange {
        message: "timestamp out of range".to_string(),
    }
}

/// Combine two non-finite signs, where one or both operands is infinite.
/// Opposite infinities have no defined result; otherwise the infinite side wins.
fn combine_infinite(a: i32, b: i32) -> Option<i32> {
    match (a, b) {
        (0, 0) => None,
        (x, 0) | (0, x) => Some(x),
        (x, y) if x == y => Some(x),
        _ => Some(0),
    }
}

/// Add two intervals field-wise with overflow checking.
pub fn add_interval(a: Interval, b: Interval) -> Result<Interval, TypeError> {
    if let Some(sign) = combine_infinite(a.infinite_sign(), b.infinite_sign()) {
        if sign == 0 {
            return Err(interval_out_of_range());
        }
        return Ok(Interval::infinity_of_sign(sign));
    }
    // A field that will not hold the sum is `interval out of range`, the same
    // 22008 PostgreSQL raises — not the generic integer overflow.
    let months = a
        .months
        .checked_add(b.months)
        .ok_or_else(interval_out_of_range)?;
    let days = a
        .days
        .checked_add(b.days)
        .ok_or_else(interval_out_of_range)?;
    let micros = a
        .micros
        .checked_add(b.micros)
        .ok_or_else(interval_out_of_range)?;
    finite_interval(Interval {
        months,
        days,
        micros,
    })
}

/// A computed interval, refusing the encoding reserved for the non-finite pair.
///
/// Two finite operands that land exactly on it have not produced an infinity.
/// They have run out of range, which is what PostgreSQL reports.
fn finite_interval(value: Interval) -> Result<Interval, TypeError> {
    if value.is_infinite() {
        return Err(interval_out_of_range());
    }
    Ok(value)
}

/// Subtract two intervals field-wise with overflow checking.
pub fn sub_interval(a: Interval, b: Interval) -> Result<Interval, TypeError> {
    let neg = neg_interval(b)?;
    add_interval(a, neg)
}

/// Negate an interval field-wise with overflow checking.
pub fn neg_interval(a: Interval) -> Result<Interval, TypeError> {
    if a.is_infinite() {
        return Ok(Interval::infinity_of_sign(-a.infinite_sign()));
    }
    let months = a.months.checked_neg().ok_or_else(interval_out_of_range)?;
    let days = a.days.checked_neg().ok_or_else(interval_out_of_range)?;
    let micros = a.micros.checked_neg().ok_or_else(interval_out_of_range)?;
    finite_interval(Interval {
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
    if a.is_infinite() {
        if factor == 0.0 {
            return Err(interval_out_of_range());
        }
        let sign = if factor < 0.0 { -1 } else { 1 };
        return Ok(Interval::infinity_of_sign(a.infinite_sign() * sign));
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

/// Add `days` to a `Date`, returning the new `Date` (overflow → 22008). Adding
/// to a non-finite date leaves it unchanged.
pub fn date_plus_days(d: Date, days: i64) -> Result<Date, TypeError> {
    if date_is_infinite(d) {
        return Ok(d);
    }
    let overflow = |_| TypeError::DatetimeFieldOverflow {
        value: days.to_string(),
    };
    let span = Span::new().try_days(days).map_err(overflow)?;
    d.checked_add(span).map_err(overflow)
}

/// Subtract two dates, returning the number of days between them (a - b).
/// Subtracting infinite dates has no defined answer (22008).
pub fn date_diff_days(a: Date, b: Date) -> Result<i32, TypeError> {
    if date_is_infinite(a) || date_is_infinite(b) {
        return Err(TypeError::DatetimeOutOfRange {
            message: "cannot subtract infinite dates".to_string(),
        });
    }
    Ok(a.since((jiff::Unit::Day, b))
        .map(|span| span.get_days())
        .expect("difference of in-range date values always fits in a Span"))
}

/// Promote a `Date` to a civil `DateTime` at midnight.
pub fn date_to_midnight(d: Date) -> DateTime {
    d.to_datetime(Time::midnight())
}

/// Add an `Interval` to a `Date` (PG: promotes date→midnight timestamp first)
/// and return a `DateTime`. This function applies months, then days, then micros
/// in order (calendar-aware, with a jiff `Span`).
pub fn date_plus_interval(d: Date, iv: Interval) -> Result<DateTime, TypeError> {
    match date_infinite_sign(d) {
        0 => timestamp_plus_interval(date_to_midnight(d), iv),
        sign => timestamp_plus_interval(timestamp_infinity_of_sign(sign), iv),
    }
}

/// Add an `Interval` to a `DateTime`. Applies months, days, and micros in
/// sequence so that `+1 month` lands on the correct calendar date and only then
/// the time offset is applied.
pub fn timestamp_plus_interval(ts: DateTime, iv: Interval) -> Result<DateTime, TypeError> {
    if let Some(sign) = combine_infinite(timestamp_infinite_sign(ts), iv.infinite_sign()) {
        if sign == 0 {
            return Err(timestamp_out_of_range());
        }
        return Ok(timestamp_infinity_of_sign(sign));
    }
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
        let days = Span::new().try_days(iv.days).map_err(overflow)?;
        after_months.checked_add(days).map_err(overflow)?
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

/// Compute `a - b` for two `DateTime` values and return an `Interval` with
/// months = 0 (PG's `timestamp - timestamp` result: total micros, stored in
/// the days + micros fields, days for full 86400 µs days and the remainder in
/// micros).
pub fn timestamp_diff(a: DateTime, b: DateTime) -> Result<Interval, TypeError> {
    // Two infinities of the same sign cancel to nothing definable; one infinite
    // operand gives the infinite interval of the difference's sign.
    if let Some(sign) = combine_infinite(timestamp_infinite_sign(a), -timestamp_infinite_sign(b)) {
        if sign == 0 {
            return Err(interval_out_of_range());
        }
        return Ok(Interval::infinity_of_sign(sign));
    }
    let total_micros = a
        .since((jiff::Unit::Microsecond, b))
        .map(|span| span.get_microseconds())
        .expect("difference of in-range timestamp values always fits in a Span");
    // Split into whole days + remaining micros (matching PG's interval storage).
    let days = (total_micros / USECS_PER_DAY_I64) as i32;
    let micros = total_micros % USECS_PER_DAY_I64;
    Ok(Interval {
        months: 0,
        days,
        micros,
    })
}

/// Add an `Interval` to a `Time` and return the new `Time`. PostgreSQL's
/// `time + interval` uses ONLY the interval's microseconds component, because a
/// `time` has no date, so it ignores the interval's `months`/`days`. It also
/// wraps the result modulo 24 h (`time '23:00' + interval '2 hours'` →
/// `01:00:00`, `time '12:00' + interval '1 day'` → `12:00:00`).
pub fn time_plus_interval(t: Time, iv: Interval) -> Time {
    // Micros-of-day of the input time.
    let base = i64::from(t.hour()) * 3_600_000_000
        + i64::from(t.minute()) * 60_000_000
        + i64::from(t.second()) * 1_000_000
        + i64::from(t.subsec_nanosecond() / 1_000);
    // Add the interval micros and wrap into [0, 86_400_000_000) (the `.rem_euclid`
    // keeps a negative shift positive, so `time '00:30' - interval '1 hour'`
    // wraps to `23:30:00`).
    // `iv.micros` comes from a user-supplied interval, so the sum can leave
    // `i64` for an extreme one; wrapping into the day is the same answer
    // whichever multiple of a day the shift is, so reduce first.
    let micros = base
        .wrapping_add(iv.micros.rem_euclid(USECS_PER_DAY_I64))
        .rem_euclid(USECS_PER_DAY_I64);
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
    if let Some(sign) = combine_infinite(timestamptz_infinite_sign(ts), iv.infinite_sign()) {
        if sign == 0 {
            return Err(timestamp_out_of_range());
        }
        return Ok(timestamptz_infinity_of_sign(sign));
    }
    let overflow = |_| TypeError::DatetimeFieldOverflow {
        value: "interval arithmetic".into(),
    };
    // Apply the calendar (months, then days) to the zoned wall-clock time.
    let zoned = ts.to_zoned(tz.clone());
    let after_cal = if iv.months != 0 || iv.days != 0 {
        let days = Span::new().try_days(iv.days).map_err(overflow)?;
        zoned
            .checked_add(iv.months.months())
            .and_then(|z| z.checked_add(days))
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
pub fn timestamptz_diff(a: Timestamp, b: Timestamp) -> Result<Interval, TypeError> {
    if let Some(sign) =
        combine_infinite(timestamptz_infinite_sign(a), -timestamptz_infinite_sign(b))
    {
        if sign == 0 {
            return Err(interval_out_of_range());
        }
        return Ok(Interval::infinity_of_sign(sign));
    }
    let total_micros = a.as_microsecond() - b.as_microsecond();
    let days = (total_micros / USECS_PER_DAY_I64) as i32;
    let micros = total_micros % USECS_PER_DAY_I64;
    Ok(Interval {
        months: 0,
        days,
        micros,
    })
}

/// A PostgreSQL `interval`: months, days, and microseconds kept SEPARATE (PG does
/// not fold `1 month` into `30 days` for storage/arithmetic, only for
/// ordering).
#[derive(Debug, Clone, Copy)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

const USECS_PER_DAY: i128 = 86_400_000_000;

impl Interval {
    /// `interval 'infinity'`. PostgreSQL reserves the triple of field extremes,
    /// ALL THREE at once, for the non-finite values, which is what leaves
    /// `2562047788:00:54.775807` (`i64::MAX` microseconds on its own) a perfectly
    /// ordinary finite interval, and makes infinity sort outside every finite one
    /// for free.
    pub const INFINITY: Interval = Interval {
        months: i32::MAX,
        days: i32::MAX,
        micros: i64::MAX,
    };

    /// `interval '-infinity'`.
    pub const NEG_INFINITY: Interval = Interval {
        months: i32::MIN,
        days: i32::MIN,
        micros: i64::MIN,
    };

    /// Whether this is one of the two non-finite intervals.
    #[must_use]
    pub fn is_infinite(&self) -> bool {
        self.infinite_sign() != 0
    }

    /// `1` for `infinity`, `-1` for `-infinity`, `0` when finite.
    #[must_use]
    pub fn infinite_sign(&self) -> i32 {
        if self.months == i32::MAX && self.days == i32::MAX && self.micros == i64::MAX {
            1
        } else if self.months == i32::MIN && self.days == i32::MIN && self.micros == i64::MIN {
            -1
        } else {
            0
        }
    }

    /// The non-finite interval of the given sign.
    #[must_use]
    pub fn infinity_of_sign(sign: i32) -> Interval {
        if sign >= 0 {
            Interval::INFINITY
        } else {
            Interval::NEG_INFINITY
        }
    }

    /// PostgreSQL's `interval_cmp` canonical value: a 30-day month and 24-hour
    /// day estimate, in microseconds, as `i128` to avoid overflow. The reserved
    /// non-finite encodings hold every field at its extreme, so their canonical
    /// value is already beyond any finite interval's.
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

/// Microseconds in one calendar day (24h estimate; civil days are always 24h).
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

/// Turn a decoder failure into the `TypeError` carrying `PostgreSQL`'s SQLSTATE
/// for that class: 22007 for malformed text, 22008 for a field or value out of
/// range, 22009 for an unusable UTC offset, 22023 for an unknown zone.
fn decode_error(err: DecodeError, type_name: &'static str, value: &str) -> TypeError {
    match err {
        DecodeError::Syntax => TypeError::InvalidDatetimeFormat {
            type_name,
            value: value.to_string(),
        },
        DecodeError::FieldOverflow => TypeError::DatetimeFieldOverflow {
            value: value.to_string(),
        },
        DecodeError::TzDisplacement => TypeError::TimezoneDisplacementOverflow {
            value: value.to_string(),
        },
        DecodeError::UnknownZone(name) => TypeError::UnknownTimeZone { name },
    }
}

/// The value layer's clock, used to resolve `now` / `today` / `tomorrow` /
/// `yesterday`. `PostgreSQL` freezes these at the transaction timestamp; crabka
/// reads the system clock, which agrees except across a transaction boundary.
fn clock_now() -> Timestamp {
    Timestamp::now()
}

/// Resolve a reserved date spelling against the session zone.
fn special_to_date(special: Special, tz: &TimeZone) -> Result<Date, TypeError> {
    let today = || tz.to_datetime(clock_now()).date();
    Ok(match special {
        Special::Infinity => DATE_INFINITY,
        Special::NegInfinity => DATE_NEG_INFINITY,
        Special::Epoch => Date::constant(1970, 1, 1),
        Special::Now | Special::Today => today(),
        Special::Tomorrow => today()
            .tomorrow()
            .map_err(|_| TypeError::DatetimeFieldOverflow {
                value: "tomorrow".to_string(),
            })?,
        Special::Yesterday => {
            today()
                .yesterday()
                .map_err(|_| TypeError::DatetimeFieldOverflow {
                    value: "yesterday".to_string(),
                })?
        }
    })
}

/// Resolve a reserved timestamp spelling against the session zone.
fn special_to_datetime(special: Special, tz: &TimeZone) -> Result<DateTime, TypeError> {
    Ok(match special {
        Special::Infinity => TIMESTAMP_INFINITY,
        Special::NegInfinity => TIMESTAMP_NEG_INFINITY,
        Special::Epoch => DateTime::constant(1970, 1, 1, 0, 0, 0, 0),
        Special::Now => tz.to_datetime(clock_now()),
        Special::Today | Special::Tomorrow | Special::Yesterday => {
            special_to_date(special, tz)?.to_datetime(Time::midnight())
        }
    })
}

/// Parse a `date` literal in every spelling `PostgreSQL` accepts, reading an
/// ambiguous all-numeric date in `MDY` order (the default `DateStyle`).
pub fn parse_date(s: &str) -> Result<Date, TypeError> {
    parse_date_in(s, DateOrder::default(), &TimeZone::UTC)
}

/// [`parse_date`] with the session's `DateStyle` field order and zone.
pub fn parse_date_in(s: &str, order: DateOrder, tz: &TimeZone) -> Result<Date, TypeError> {
    match decode(s.trim(), order).map_err(|e| decode_error(e, "date", s))? {
        Decoded::Special(special) => special_to_date(special, tz),
        Decoded::Parts(parts) => {
            let date = parts.date.ok_or_else(|| TypeError::InvalidDatetimeFormat {
                type_name: "date",
                value: s.to_string(),
            })?;
            // Rounding a sub-microsecond tail up to `24:00:00` rolls into the
            // next day before the time is discarded.
            let date = if parts.micros_of_day >= MICROS_PER_DAY {
                date.tomorrow()
                    .map_err(|_| TypeError::DatetimeFieldOverflow {
                        value: s.to_string(),
                    })?
            } else {
                date
            };
            check_finite_date(date, s)?;
            Ok(date)
        }
    }
}

/// The earliest finite date PostgreSQL represents, 4714-11-24 BC, in the
/// astronomical year numbering both PostgreSQL and jiff use.
const MIN_FINITE_DATE: Date = Date::constant(-4713, 11, 24);

/// Reject a literal outside the finite range: below PostgreSQL's own lower bound,
/// or on a value reserved for `infinity`.
fn check_finite_date(d: Date, s: &str) -> Result<(), TypeError> {
    if date_is_infinite(d) || d < MIN_FINITE_DATE {
        return Err(TypeError::DatetimeOutOfRange {
            message: format!("date out of range: \"{s}\""),
        });
    }
    Ok(())
}

/// The *output-format* half of PostgreSQL's `DateStyle` GUC, which decides how
/// `date_out`, `timestamp_out` and `timestamptz_out` spell a value. The other
/// half, the field ordering, is [`DateOrder`], and the two are independent:
/// `SQL, DMY` prints `11/07/2001` while `SQL, MDY` prints `07/11/2001`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DateStyle {
    /// `2001-07-11 10:51:14.123+00`: the default, and the only style the field
    /// ordering does not reach.
    #[default]
    Iso,
    /// `Wed Jul 11 10:51:14.123 2001 UTC`.
    Postgres,
    /// `07/11/2001 10:51:14.123 UTC`.
    Sql,
    /// `11.07.2001 10:51:14.123 UTC`: always day-first, whatever the ordering.
    German,
}

impl DateStyle {
    /// Read the output format out of a `DateStyle` setting (`ISO, DMY`),
    /// ignoring the ordering component. Text naming no format keeps the default.
    #[must_use]
    pub fn from_datestyle(style: &str) -> Self {
        for part in style.split(',') {
            let part = part.trim();
            if part.eq_ignore_ascii_case("iso") {
                return DateStyle::Iso;
            }
            if part.eq_ignore_ascii_case("postgres") {
                return DateStyle::Postgres;
            }
            if part.eq_ignore_ascii_case("sql") {
                return DateStyle::Sql;
            }
            if part.eq_ignore_ascii_case("german") {
                return DateStyle::German;
            }
        }
        DateStyle::Iso
    }

    /// Whether the calendar date leads with the day. `German` always does;
    /// `Postgres` and `SQL` follow the ordering, and only `DMY` is day-first
    /// (PostgreSQL treats `YMD` as month-first on output).
    fn day_first(self, order: DateOrder) -> bool {
        match self {
            DateStyle::German => true,
            DateStyle::Postgres | DateStyle::Sql => order == DateOrder::Dmy,
            DateStyle::Iso => false,
        }
    }
}

/// The calendar-date half of a non-ISO rendering, without the era suffix.
fn styled_date(d: Date, style: DateStyle, order: DateOrder) -> String {
    let (year, _) = era_year(d.year());
    let (month, day) = (d.month(), d.day());
    let separator = if style == DateStyle::German { '.' } else { '/' };
    match style {
        DateStyle::Iso => format!("{year:04}-{month:02}-{day:02}"),
        DateStyle::Postgres => {
            if style.day_first(order) {
                format!("{day:02}-{month:02}-{year:04}")
            } else {
                format!("{month:02}-{day:02}-{year:04}")
            }
        }
        DateStyle::Sql | DateStyle::German => {
            if style.day_first(order) {
                format!("{day:02}{separator}{month:02}{separator}{year:04}")
            } else {
                format!("{month:02}{separator}{day:02}{separator}{year:04}")
            }
        }
    }
}

/// The `HH:MM:SS[.ffffff]` clock every style shares.
fn styled_clock(t: Time) -> String {
    let mut out = format!("{:02}:{:02}:{:02}", t.hour(), t.minute(), t.second());
    push_subsecond(&mut out, t.subsec_nanosecond());
    out
}

/// The `Postgres` style's `Dow Mon DD HH:MM:SS` / `Dow DD Mon HH:MM:SS` prefix,
/// which puts the year *after* the clock rather than in the date.
fn postgres_style_datetime(dt: DateTime, order: DateOrder) -> String {
    let date = dt.date();
    let dow = &DAY_NAMES[date.weekday().to_sunday_zero_offset() as usize][..3];
    let month = &MONTH_NAMES[(date.month() as usize) - 1][..3];
    let (year, _) = era_year(date.year());
    let day = date.day();
    let clock = styled_clock(dt.time());
    if DateStyle::Postgres.day_first(order) {
        format!("{dow} {day:02} {month} {clock} {year:04}")
    } else {
        format!("{dow} {month} {day:02} {clock} {year:04}")
    }
}

/// Render a `date` in the session's `DateStyle`.
#[must_use]
pub fn date_to_text_in(d: Date, style: DateStyle, order: DateOrder) -> String {
    if style == DateStyle::Iso {
        return date_to_text(d);
    }
    if d == DATE_INFINITY {
        return "infinity".to_string();
    }
    if d == DATE_NEG_INFINITY {
        return "-infinity".to_string();
    }
    let (_, era) = era_year(d.year());
    format!("{}{era}", styled_date(d, style, order))
}

/// Render a `timestamp` in the session's `DateStyle`.
#[must_use]
pub fn timestamp_to_text_in(ts: DateTime, style: DateStyle, order: DateOrder) -> String {
    if style == DateStyle::Iso {
        return timestamp_to_text(ts);
    }
    if ts == TIMESTAMP_INFINITY {
        return "infinity".to_string();
    }
    if ts == TIMESTAMP_NEG_INFINITY {
        return "-infinity".to_string();
    }
    let (_, era) = era_year(ts.date().year());
    let body = if style == DateStyle::Postgres {
        postgres_style_datetime(ts, order)
    } else {
        format!(
            "{} {}",
            styled_date(ts.date(), style, order),
            styled_clock(ts.time())
        )
    };
    format!("{body}{era}")
}

/// Render a `timestamptz` instant in `tz` in the session's `DateStyle`. Outside
/// `ISO` the zone is spelled as its *abbreviation* at that instant (`EDT`, `MSK`,
/// `+0545`) rather than as a numeric offset, and the era suffix follows it.
#[must_use]
pub fn timestamptz_to_text_in(
    ts: Timestamp,
    tz: &TimeZone,
    style: DateStyle,
    order: DateOrder,
) -> String {
    if style == DateStyle::Iso {
        return timestamptz_to_text(ts, tz);
    }
    if ts == Timestamp::MAX {
        return "infinity".to_string();
    }
    if ts == Timestamp::MIN {
        return "-infinity".to_string();
    }
    let dt = tz.to_datetime(ts);
    let (_, era) = era_year(dt.date().year());
    let body = if style == DateStyle::Postgres {
        postgres_style_datetime(dt, order)
    } else {
        format!(
            "{} {}",
            styled_date(dt.date(), style, order),
            styled_clock(dt.time())
        )
    };
    let zone = tz.to_offset_info(ts).abbreviation().to_string();
    format!("{body} {zone}{era}")
}

/// Render a `date` as ISO `YYYY-MM-DD` (PostgreSQL `date_out`, ISO datestyle),
/// with the `BC` era suffix for years at or before the astronomical year 0.
pub fn date_to_text(d: Date) -> String {
    if d == DATE_INFINITY {
        return "infinity".to_string();
    }
    if d == DATE_NEG_INFINITY {
        return "-infinity".to_string();
    }
    let (year, era) = era_year(d.year());
    format!("{year:04}-{:02}-{:02}{era}", d.month(), d.day())
}

/// Split an astronomical year into the printed year number and era suffix.
/// Astronomical year 0 is 1 BC, so a non-positive year prints as `1 - year BC`.
fn era_year(year: i16) -> (i32, &'static str) {
    if year <= 0 {
        (1 - i32::from(year), " BC")
    } else {
        (i32::from(year), "")
    }
}

/// `date_send`: i32 big-endian days since the PostgreSQL epoch (2000-01-01).
/// The two non-finite values use PostgreSQL's reserved `INT32_MIN`/`INT32_MAX`.
pub fn date_to_binary(d: Date) -> [u8; 4] {
    if d == DATE_INFINITY {
        return i32::MAX.to_be_bytes();
    }
    if d == DATE_NEG_INFINITY {
        return i32::MIN.to_be_bytes();
    }
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
    let raw = i32::from_be_bytes(arr);
    if raw == i32::MAX {
        return Ok(DATE_INFINITY);
    }
    if raw == i32::MIN {
        return Ok(DATE_NEG_INFINITY);
    }
    let days = i64::from(raw);
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

/// Parse a `time` literal. `PostgreSQL` accepts a leading date and a trailing
/// zone here and discards both, so `'2003-03-07 15:36:39 America/New_York'` is a
/// legal `time`. A zone *name* still has to be resolvable, which is why the
/// same text without its date is a syntax error.
pub fn parse_time(s: &str) -> Result<Time, TypeError> {
    parse_time_in(s, DateOrder::default(), &TimeZone::UTC)
}

/// [`parse_time`] with the session's `DateStyle` field order and zone.
pub fn parse_time_in(s: &str, order: DateOrder, tz: &TimeZone) -> Result<Time, TypeError> {
    let type_name = "time without time zone";
    let micros = match decode(s.trim(), order).map_err(|e| decode_error(e, type_name, s))? {
        Decoded::Special(special) => match special {
            // `allballs` decodes as a plain clock reading, so the only reserved
            // spellings that reach here are the ones a bare clock cannot express.
            Special::Now => tz
                .to_datetime(clock_now())
                .time()
                .duration_since(Time::midnight())
                .as_micros() as i64,
            Special::Epoch | Special::Today | Special::Tomorrow | Special::Yesterday => 0,
            Special::Infinity | Special::NegInfinity => {
                return Err(TypeError::InvalidDatetimeFormat {
                    type_name,
                    value: s.to_string(),
                });
            }
        },
        Decoded::Parts(parts) => parts.micros_of_day,
    };
    // `24:00:00` is a legal `time` in PostgreSQL but has no jiff representation;
    // crabka reports it as out of range rather than silently folding it onto
    // midnight, which is a different value.
    if micros >= MICROS_PER_DAY {
        return Err(TypeError::DatetimeFieldOverflow {
            value: s.to_string(),
        });
    }
    Ok(time_from_micros_of_day(micros))
}

// ---------------------------------------------------------------------------
// time with time zone
// ---------------------------------------------------------------------------

/// A PostgreSQL `time with time zone`: a clock reading plus the UTC offset it
/// was read at.
///
/// The two parts stay separate because both are observable: the value prints as
/// `15:36:39-05`, not as its UTC equivalent. But *ordering* is by the
/// UTC-equivalent instant, which is what [`TimeTz::utc_micros`] computes and what
/// the `Ord` impl compares. Equality follows ordering, so `12:00-05` and
/// `17:00+00` are the same value even though they print differently, exactly as
/// in PostgreSQL.
#[derive(Debug, Clone, Copy)]
pub struct TimeTz {
    /// The wall-clock reading, as written.
    pub time: Time,
    /// The UTC offset the reading was taken at.
    pub offset: Offset,
}

impl TimeTz {
    /// Microseconds since midnight UTC, the value PostgreSQL orders `timetz` by.
    #[must_use]
    pub fn utc_micros(&self) -> i64 {
        time_to_micros_of_day(self.time) - i64::from(self.offset.seconds()) * 1_000_000
    }
}

impl TimeTz {
    /// The sort key: the UTC-equivalent instant first, then the zone as seconds
    /// *west* of UTC. The tiebreak is what makes `12:00-05` and `17:00+00`
    /// distinct values despite naming the same instant.
    fn sort_key(&self) -> (i64, i32) {
        (self.utc_micros(), -self.offset.seconds())
    }
}

impl PartialEq for TimeTz {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key() == other.sort_key()
    }
}
impl Eq for TimeTz {}
impl std::hash::Hash for TimeTz {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sort_key().hash(state);
    }
}
impl PartialOrd for TimeTz {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TimeTz {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_key().cmp(&other.sort_key())
    }
}

/// Parse a `timetz` literal. A zone is required. With none in the text the
/// session zone supplies one, but that needs a date to resolve a *named* zone,
/// which is why `'15:36:39 America/New_York'` is a syntax error while
/// `'2003-03-07 15:36:39 America/New_York'` is not.
pub fn parse_timetz(s: &str, tz: &TimeZone) -> Result<TimeTz, TypeError> {
    parse_timetz_in(s, DateOrder::default(), tz)
}

/// [`parse_timetz`] with the session's `DateStyle` field order.
pub fn parse_timetz_in(s: &str, order: DateOrder, tz: &TimeZone) -> Result<TimeTz, TypeError> {
    let type_name = "time with time zone";
    let syntax = || TypeError::InvalidDatetimeFormat {
        type_name,
        value: s.to_string(),
    };
    let parts = match decode(s.trim(), order).map_err(|e| decode_error(e, type_name, s))? {
        Decoded::Special(Special::Now) => {
            let zoned = clock_now().to_zoned(tz.clone());
            return Ok(TimeTz {
                time: zoned.datetime().time(),
                offset: zoned.offset(),
            });
        }
        Decoded::Special(_) => return Err(syntax()),
        Decoded::Parts(parts) => parts,
    };
    if parts.micros_of_day >= MICROS_PER_DAY {
        return Err(TypeError::DatetimeFieldOverflow {
            value: s.to_string(),
        });
    }
    let time = time_from_micros_of_day(parts.micros_of_day);
    // A named zone's offset depends on the date, so a `timetz` may only use one
    // when the literal also carried a date.
    let offset = match parts.zone {
        Some(Zone::Offset(offset)) => offset,
        Some(Zone::Named(zone)) => {
            let date = parts.date.ok_or_else(syntax)?;
            zone.to_offset(
                date.to_datetime(time)
                    .to_zoned(zone.clone())
                    .map_err(|_| syntax())?
                    .timestamp(),
            )
        }
        None => {
            let date = parts
                .date
                .unwrap_or_else(|| tz.to_datetime(clock_now()).date());
            tz.to_offset(
                date.to_datetime(time)
                    .to_zoned(tz.clone())
                    .map_err(|_| syntax())?
                    .timestamp(),
            )
        }
    };
    Ok(TimeTz { time, offset })
}

/// Render a `timetz` as `HH:MM:SS[.ffffff]±HH[:MM[:SS]]` (PostgreSQL `timetz_out`).
#[must_use]
pub fn timetz_to_text(value: TimeTz) -> String {
    let mut out = time_to_text(value.time);
    push_offset(&mut out, value.offset);
    out
}

/// `timetz_send`: i64 big-endian microseconds since midnight, then i32
/// big-endian seconds *west* of UTC, the sign PostgreSQL stores, which is the
/// negation of the offset the value prints.
#[must_use]
pub fn timetz_to_binary(value: TimeTz) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[0..8].copy_from_slice(&time_to_micros_of_day(value.time).to_be_bytes());
    out[8..12].copy_from_slice(&(-value.offset.seconds()).to_be_bytes());
    out
}

/// `timetz_recv`: the inverse of [`timetz_to_binary`].
pub fn timetz_from_binary(b: &[u8]) -> Result<TimeTz, TypeError> {
    let arr: [u8; 12] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "time with time zone",
        value: format!("{b:?}"),
    })?;
    let micros = i64::from_be_bytes(arr[0..8].try_into().expect("eight bytes"));
    let west = i32::from_be_bytes(arr[8..12].try_into().expect("four bytes"));
    if !(0..MICROS_PER_DAY).contains(&micros) {
        return Err(TypeError::DatetimeFieldOverflow {
            value: micros.to_string(),
        });
    }
    let offset = Offset::from_seconds(-west).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: west.to_string(),
    })?;
    Ok(TimeTz {
        time: time_from_micros_of_day(micros),
        offset,
    })
}

/// Rebuild a clock reading from microseconds since midnight, the inverse of
/// [`time_to_micros_of_day`].
///
/// # Panics
///
/// Panics unless `micros` is in `0..86_400_000_000`.
#[must_use]
pub fn time_from_micros_of_day_public(micros: i64) -> Time {
    time_from_micros_of_day(micros)
}

/// Microseconds since midnight for a clock reading.
#[must_use]
pub fn time_to_micros_of_day(t: Time) -> i64 {
    i64::from(t.hour()) * 3_600_000_000
        + i64::from(t.minute()) * 60_000_000
        + i64::from(t.second()) * 1_000_000
        + i64::from(t.subsec_nanosecond() / 1_000)
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

/// Parse a `timestamp` literal in every spelling `PostgreSQL` accepts. A zone in
/// the text is decoded (so an unresolvable one is still an error) and then
/// discarded, because this type has no zone.
pub fn parse_timestamp(s: &str) -> Result<DateTime, TypeError> {
    parse_timestamp_in(s, DateOrder::default(), &TimeZone::UTC)
}

/// [`parse_timestamp`] with the session's `DateStyle` field order and zone.
pub fn parse_timestamp_in(s: &str, order: DateOrder, tz: &TimeZone) -> Result<DateTime, TypeError> {
    let type_name = "timestamp without time zone";
    match decode(s.trim(), order).map_err(|e| decode_error(e, type_name, s))? {
        Decoded::Special(special) => special_to_datetime(special, tz),
        Decoded::Parts(parts) => {
            let date = parts.date.ok_or_else(|| TypeError::InvalidDatetimeFormat {
                type_name,
                value: s.to_string(),
            })?;
            let dt = combine_parts(date, parts.micros_of_day, s)?;
            check_finite_timestamp(dt, s)?;
            Ok(dt)
        }
    }
}

/// Join a date and a microsecond-of-day into a civil timestamp, carrying the
/// `24:00:00` reading into the following day the way `PostgreSQL` does.
fn combine_parts(date: Date, micros_of_day: i64, s: &str) -> Result<DateTime, TypeError> {
    let overflow = || TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    };
    if micros_of_day >= MICROS_PER_DAY {
        let next = date.tomorrow().map_err(|_| overflow())?;
        return Ok(next.to_datetime(time_from_micros_of_day(micros_of_day - MICROS_PER_DAY)));
    }
    Ok(date.to_datetime(time_from_micros_of_day(micros_of_day)))
}

/// Reject a literal outside the finite range, the timestamp counterpart of
/// [`check_finite_date`].
fn check_finite_timestamp(ts: DateTime, s: &str) -> Result<(), TypeError> {
    if timestamp_is_infinite(ts) || ts.date() < MIN_FINITE_DATE {
        return Err(TypeError::DatetimeOutOfRange {
            message: format!("timestamp out of range: \"{s}\""),
        });
    }
    Ok(())
}

/// Render a `timestamp` as `YYYY-MM-DD HH:MM:SS[.ffffff]` (SPACE separator,
/// PostgreSQL `timestamp_out`, ISO datestyle), with the `BC` era suffix.
pub fn timestamp_to_text(ts: DateTime) -> String {
    if ts == TIMESTAMP_INFINITY {
        return "infinity".to_string();
    }
    if ts == TIMESTAMP_NEG_INFINITY {
        return "-infinity".to_string();
    }
    let d = ts.date();
    let tm = ts.time();
    let (year, era) = era_year(d.year());
    let mut out = format!(
        "{year:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        d.month(),
        d.day(),
        tm.hour(),
        tm.minute(),
        tm.second()
    );
    push_subsecond(&mut out, tm.subsec_nanosecond());
    out.push_str(era);
    out
}

/// `timestamp_send`: i64 big-endian microseconds since the PG epoch. The two
/// non-finite values use PostgreSQL's reserved `INT64_MIN`/`INT64_MAX`.
pub fn timestamp_to_binary(ts: DateTime) -> [u8; 8] {
    if ts == TIMESTAMP_INFINITY {
        return i64::MAX.to_be_bytes();
    }
    if ts == TIMESTAMP_NEG_INFINITY {
        return i64::MIN.to_be_bytes();
    }
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
    if pg_micros == i64::MAX {
        return Ok(TIMESTAMP_INFINITY);
    }
    if pg_micros == i64::MIN {
        return Ok(TIMESTAMP_NEG_INFINITY);
    }
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

/// Parse a `timestamptz` literal into an absolute instant. A zone in the text
/// fixes the instant, whether it is an offset, an abbreviation or a
/// zone-database name. Otherwise the wall clock reads as local to the session
/// `tz`.
pub fn parse_timestamptz(s: &str, tz: &TimeZone) -> Result<Timestamp, TypeError> {
    parse_timestamptz_in(s, DateOrder::default(), tz)
}

/// [`parse_timestamptz`] with the session's `DateStyle` field order.
pub fn parse_timestamptz_in(
    s: &str,
    order: DateOrder,
    tz: &TimeZone,
) -> Result<Timestamp, TypeError> {
    let type_name = "timestamp with time zone";
    let overflow = || TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    };
    match decode(s.trim(), order).map_err(|e| decode_error(e, type_name, s))? {
        Decoded::Special(special) => match special {
            Special::Infinity => Ok(timestamptz_infinity()),
            Special::NegInfinity => Ok(timestamptz_neg_infinity()),
            Special::Now => Ok(clock_now()),
            other => special_to_datetime(other, tz)?
                .to_zoned(tz.clone())
                .map(|z| z.timestamp())
                .map_err(|_| overflow()),
        },
        Decoded::Parts(parts) => {
            let date = parts.date.ok_or_else(|| TypeError::InvalidDatetimeFormat {
                type_name,
                value: s.to_string(),
            })?;
            let dt = combine_parts(date, parts.micros_of_day, s)?;
            let instant = match parts.zone {
                Some(Zone::Offset(off)) => off.to_timestamp(dt).map_err(|_| overflow())?,
                Some(Zone::Named(zone)) => zone
                    .to_zoned(dt)
                    .map(|z| z.timestamp())
                    .map_err(|_| overflow())?,
                None => dt
                    .to_zoned(tz.clone())
                    .map(|z| z.timestamp())
                    .map_err(|_| overflow())?,
            };
            check_finite_timestamp(dt, s)?;
            if timestamptz_is_infinite(instant) {
                return Err(overflow());
            }
            Ok(instant)
        }
    }
}

/// Render a `timestamptz` instant in `tz`: `YYYY-MM-DD HH:MM:SS[.ffffff]±HH[:MM[:SS]]`
/// (PostgreSQL `timestamptz_out`, ISO datestyle). The offset suffix shows `:MM`/`:SS`
/// only when non-zero, and the `BC` era suffix comes after it.
pub fn timestamptz_to_text(ts: Timestamp, tz: &TimeZone) -> String {
    if ts == Timestamp::MAX {
        return "infinity".to_string();
    }
    if ts == Timestamp::MIN {
        return "-infinity".to_string();
    }
    let dt = tz.to_datetime(ts);
    let off = tz.to_offset(ts);
    let (year, era) = era_year(dt.date().year());
    let time = dt.time();
    let mut out = format!(
        "{year:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        dt.date().month(),
        dt.date().day(),
        time.hour(),
        time.minute(),
        time.second()
    );
    push_subsecond(&mut out, time.subsec_nanosecond());
    push_offset(&mut out, off);
    out.push_str(era);
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
/// The two non-finite values use PostgreSQL's reserved `INT64_MIN`/`INT64_MAX`.
pub fn timestamptz_to_binary(ts: Timestamp) -> [u8; 8] {
    if ts == Timestamp::MAX {
        return i64::MAX.to_be_bytes();
    }
    if ts == Timestamp::MIN {
        return i64::MIN.to_be_bytes();
    }
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
    if pg_micros == i64::MAX {
        return Ok(timestamptz_infinity());
    }
    if pg_micros == i64::MIN {
        return Ok(timestamptz_neg_infinity());
    }
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

/// The interval field a bare quantity is measured in, used both as the unit an
/// `INTERVAL '…' <field>` qualifier supplies and as the step in the coarsening
/// chain a unit-less field list walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IntervalField {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
    Decade,
    Century,
    Millennium,
}

impl IntervalField {
    /// The field name as `PostgreSQL` spells it in a qualifier.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            IntervalField::Microsecond => "microsecond",
            IntervalField::Millisecond => "millisecond",
            IntervalField::Second => "second",
            IntervalField::Minute => "minute",
            IntervalField::Hour => "hour",
            IntervalField::Day => "day",
            IntervalField::Week => "week",
            IntervalField::Month => "month",
            IntervalField::Year => "year",
            IntervalField::Decade => "decade",
            IntervalField::Century => "century",
            IntervalField::Millennium => "millennium",
        }
    }

    /// Parse a unit word, in every spelling `PostgreSQL` accepts.
    #[must_use]
    pub fn parse(word: &str) -> Option<Self> {
        Some(match word.trim().to_ascii_lowercase().as_str() {
            "microsecond" | "microseconds" | "microsecon" | "usecond" | "useconds" | "usec"
            | "usecs" | "us" => IntervalField::Microsecond,
            "millisecond" | "milliseconds" | "millisecon" | "msecond" | "mseconds" | "msec"
            | "msecs" | "ms" => IntervalField::Millisecond,
            "second" | "seconds" | "sec" | "secs" | "s" => IntervalField::Second,
            "minute" | "minutes" | "min" | "mins" | "m" => IntervalField::Minute,
            "hour" | "hours" | "hr" | "hrs" | "h" => IntervalField::Hour,
            "day" | "days" | "d" => IntervalField::Day,
            "week" | "weeks" | "w" | "wk" | "wks" => IntervalField::Week,
            "month" | "months" | "mon" | "mons" => IntervalField::Month,
            "year" | "years" | "yr" | "yrs" | "y" => IntervalField::Year,
            "decade" | "decades" | "dec" | "decs" => IntervalField::Decade,
            "century" | "centuries" | "cent" | "c" => IntervalField::Century,
            "millennium" | "millennia" | "mil" | "mils" => IntervalField::Millennium,
            _ => return None,
        })
    }

    /// This field's bit in the "already supplied" mask PostgreSQL's interval
    /// decoder carries (its `fmask`), which is what makes a repeated field a
    /// syntax error rather than a second addition.
    const fn mask_bit(self) -> u32 {
        1 << (self as u32)
    }

    /// The unit the bare quantity to the LEFT of one just consumed takes.
    ///
    /// PostgreSQL keeps the current unit for the next bare quantity, which is
    /// why `'1 2' MINUTE` is a duplicate-field error rather than an hour and a
    /// minute. It steps to `DAY` in exactly one place, after an hour.
    fn next_bare_unit(self) -> Self {
        match self {
            IntervalField::Hour => IntervalField::Day,
            other => other,
        }
    }
}

/// The sub-second fields together. A *fractional* second quantity spills into
/// all three, so PostgreSQL marks all three as supplied and a following
/// millisecond or microsecond term is a duplicate.
const SUBSECOND_FIELDS: u32 = IntervalField::Second.mask_bit()
    | IntervalField::Millisecond.mask_bit()
    | IntervalField::Microsecond.mask_bit();

/// Everything a `HH:MM:SS.ffffff` clock term supplies: PostgreSQL's
/// `DTK_TIME_M`, which is why `'1:20:05 5 microseconds'` is rejected.
const CLOCK_FIELDS: u32 =
    IntervalField::Hour.mask_bit() | IntervalField::Minute.mask_bit() | SUBSECOND_FIELDS;

/// Parse a PostgreSQL `interval` literal: a sequence of signed `<qty> <unit>`
/// terms, `[-]HH:MM[:SS[.ffffff]]` clock terms, the `Y-M` and `D HH:MM:SS`
/// shorthands, an `@ … ago` verbose form, or an ISO-8601 duration. Fractional
/// quantities spill into the next-smaller unit; weeks fold to days, years to
/// months.
pub fn parse_interval(s: &str) -> Result<Interval, TypeError> {
    parse_interval_ranged(s, None)
}

/// [`parse_interval`] with the field range an `INTERVAL '…' <field> [TO <field>]`
/// qualifier supplies. `range` is `(start, end)`; a bare quantity with no unit of
/// its own takes `end`, and each bare quantity to its left takes the next coarser
/// field, so `'4 5' DAY TO HOUR` is four days and five hours.
pub fn parse_interval_ranged(
    s: &str,
    range: Option<(IntervalField, IntervalField)>,
) -> Result<Interval, TypeError> {
    let t = s.trim();
    let err = || TypeError::InvalidDatetimeFormat {
        type_name: "interval",
        value: s.to_string(),
    };
    if t.is_empty() {
        return Err(err());
    }
    // The two non-finite intervals, spelled exactly as PostgreSQL accepts them.
    let lower = t.to_ascii_lowercase();
    match lower.as_str() {
        "infinity" | "+infinity" => return Ok(Interval::INFINITY),
        "-infinity" => return Ok(Interval::NEG_INFINITY),
        _ => {}
    }
    if lower.starts_with('p') && !lower.starts_with("p ") {
        return parse_iso8601_interval(t).ok_or_else(err);
    }

    // The verbose form brackets the terms with `@` and may end with `ago`, which
    // negates the whole interval.
    let body = t.strip_prefix('@').unwrap_or(t).trim();
    let (body, negate) = match body.to_ascii_lowercase().strip_suffix("ago") {
        Some(prefix) if prefix.is_empty() || prefix.ends_with(char::is_whitespace) => {
            (&body[..prefix.len()], true)
        }
        _ => (body, false),
    };

    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i128 = 0;

    // Terms are read right to left so an unqualified quantity can take its unit
    // from the field range and pass it on to its neighbour.
    let tokens: Vec<&str> = body.split_whitespace().collect();
    // With no qualifier the rightmost bare quantity is seconds, PostgreSQL's
    // `INTERVAL_FULL_RANGE` default.
    let mut implied = range.map_or(IntervalField::Second, |(_, end)| end);
    // Which fields a term has already supplied. Supplying one twice is
    // PostgreSQL's `DTERR_BAD_FORMAT`, not a second addition.
    let mut supplied: u32 = 0;
    let claim = |bits: u32, supplied: &mut u32| {
        if bits & *supplied != 0 {
            return Err(err());
        }
        *supplied |= bits;
        Ok(())
    };
    let mut i = tokens.len();
    while i > 0 {
        let tok = tokens[i - 1];
        // A clock term stands alone; the quantity to its left is a day count.
        if tok.contains(':') {
            claim(CLOCK_FIELDS, &mut supplied)?;
            micros += i128::from(parse_clock_term(tok, range).ok_or_else(err)?);
            implied = IntervalField::Day;
            i -= 1;
            continue;
        }
        // A `Y-M` term is the year-month shorthand, which PostgreSQL reads as a
        // month count and leaves months as the unit for the quantity to its left.
        if let Some(shorthand) = parse_year_month_term(tok) {
            claim(IntervalField::Month.mask_bit(), &mut supplied)?;
            months += shorthand;
            implied = IntervalField::Month;
            i -= 1;
            continue;
        }
        let unit = match IntervalField::parse(tok) {
            Some(unit) if i >= 2 => {
                i -= 1;
                unit
            }
            // A trailing word that is not a unit, or a unit with no quantity.
            Some(_) | None if Quantity::parse(tok).is_none() => return Err(err()),
            _ => implied,
        };
        let qty = Quantity::parse(tokens.get(i - 1).ok_or_else(err)?).ok_or_else(err)?;
        // A fraction of a second reaches the millisecond and microsecond fields,
        // so it supplies all three; a fraction of any coarser unit does not.
        let bits = if unit == IntervalField::Second && qty.frac != 0.0 {
            SUBSECOND_FIELDS
        } else {
            unit.mask_bit()
        };
        claim(bits, &mut supplied)?;
        accumulate_unit(qty, unit, &mut months, &mut days, &mut micros).ok_or_else(err)?;
        implied = unit.next_bare_unit();
        i -= 1;
    }

    let overflow = || TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    };
    let sign = if negate { -1 } else { 1 };
    let months = i32::try_from(months * i64::from(sign)).map_err(|_| overflow())?;
    let days = i32::try_from(days * i64::from(sign)).map_err(|_| overflow())?;
    let micros = i64::try_from(micros * i128::from(sign)).map_err(|_| overflow())?;
    let value = Interval {
        months,
        days,
        micros,
    };
    if value.is_infinite() {
        return Err(overflow());
    }
    Ok(truncate_to_range(value, range))
}

/// Drop everything finer than the range's end field, the way a qualified
/// `INTERVAL '…' <field>` literal truncates. `SECOND` keeps its fraction.
fn truncate_to_range(iv: Interval, range: Option<(IntervalField, IntervalField)>) -> Interval {
    let Some((_, end)) = range else {
        return iv;
    };
    let step = match end {
        IntervalField::Microsecond | IntervalField::Millisecond | IntervalField::Second => {
            return iv;
        }
        IntervalField::Minute => 60_000_000,
        IntervalField::Hour => 3_600_000_000,
        IntervalField::Day | IntervalField::Week => return Interval { micros: 0, ..iv },
        IntervalField::Month
        | IntervalField::Year
        | IntervalField::Decade
        | IntervalField::Century
        | IntervalField::Millennium => {
            return Interval {
                days: 0,
                micros: 0,
                ..iv
            };
        }
    };
    Interval {
        micros: iv.micros - iv.micros.rem_euclid(step),
        ..iv
    }
}

/// Parse the `Y-M` year-month shorthand into a signed month count.
fn parse_year_month_term(tok: &str) -> Option<i64> {
    let (sign, rest) = match tok.strip_prefix('-') {
        Some(rest) => (-1i64, rest),
        None => (1i64, tok.strip_prefix('+').unwrap_or(tok)),
    };
    let (years, months) = rest.split_once('-')?;
    let years: i64 = years.parse().ok()?;
    let months: i64 = months.parse().ok()?;
    Some(sign * (years * 12 + months))
}

/// Parse an ISO-8601 duration, in both the designator form (`P1Y2M3DT4H5M6S`)
/// and the alternative all-numeric form (`P0001-02-03T04:05:06`).
fn parse_iso8601_interval(text: &str) -> Option<Interval> {
    let body = text.get(1..)?;
    let (date_part, time_part) = match body.split_once(['T', 't']) {
        Some((date, time)) => (date, Some(time)),
        None => (body, None),
    };
    if date_part.contains('-') || time_part.is_some_and(|t| t.contains(':')) {
        return parse_iso8601_alternative(date_part, time_part);
    }
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i128 = 0;
    for (part, in_time) in [(date_part, false), (time_part.unwrap_or(""), true)] {
        let mut number = String::new();
        for c in part.chars() {
            if c.is_ascii_digit() || c == '.' || c == '-' || c == '+' {
                number.push(c);
                continue;
            }
            let qty = Quantity::parse(&number)?;
            number.clear();
            // Each designator belongs to ONE of the two halves — `PT1D` and
            // `P1H` are as malformed as an unknown letter would be.
            let unit = match (c.to_ascii_uppercase(), in_time) {
                ('Y', false) => IntervalField::Year,
                ('M', false) => IntervalField::Month,
                ('W', false) => IntervalField::Week,
                ('D', false) => IntervalField::Day,
                ('H', true) => IntervalField::Hour,
                ('M', true) => IntervalField::Minute,
                ('S', true) => IntervalField::Second,
                _ => return None,
            };
            accumulate_unit(qty, unit, &mut months, &mut days, &mut micros)?;
        }
        if !number.is_empty() {
            return None;
        }
    }
    Some(Interval {
        months: i32::try_from(months).ok()?,
        days: i32::try_from(days).ok()?,
        micros: i64::try_from(micros).ok()?,
    })
}

/// The alternative ISO-8601 form, whose fields are positional rather than
/// designated: `P<years>-<months>-<days>T<hours>:<minutes>:<seconds>`.
fn parse_iso8601_alternative(date_part: &str, time_part: Option<&str>) -> Option<Interval> {
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i128 = 0;
    let date_fields: Vec<&str> = date_part.split('-').collect();
    if date_fields.len() != 3 {
        return None;
    }
    for (text, unit) in date_fields.iter().zip([
        IntervalField::Year,
        IntervalField::Month,
        IntervalField::Day,
    ]) {
        accumulate_unit(
            Quantity::parse(text)?,
            unit,
            &mut months,
            &mut days,
            &mut micros,
        )?;
    }
    if let Some(time_part) = time_part {
        let time_fields: Vec<&str> = time_part.split(':').collect();
        if time_fields.len() != 3 {
            return None;
        }
        for (text, unit) in time_fields.iter().zip([
            IntervalField::Hour,
            IntervalField::Minute,
            IntervalField::Second,
        ]) {
            accumulate_unit(
                Quantity::parse(text)?,
                unit,
                &mut months,
                &mut days,
                &mut micros,
            )?;
        }
    }
    Some(Interval {
        months: i32::try_from(months).ok()?,
        days: i32::try_from(days).ok()?,
        micros: i64::try_from(micros).ok()?,
    })
}

/// Parse a `[-]HH:MM[:SS[.ffffff]]` clock term into signed microseconds. A
/// two-field term is hours and minutes unless the field range says the reading
/// ends at `SECOND`, in which case it is minutes and seconds.
fn parse_clock_term(tok: &str, range: Option<(IntervalField, IntervalField)>) -> Option<i64> {
    let (sign, rest) = match tok.strip_prefix('-') {
        Some(r) => (-1i64, r),
        None => (1i64, tok.strip_prefix('+').unwrap_or(tok)),
    };
    let mut fields: Vec<&str> = rest.split(':').collect();
    if fields.len() < 2 || fields.len() > 3 {
        return None;
    }
    // `MINUTE TO SECOND` re-reads a two-field term one place down the clock.
    if fields.len() == 2 && range == Some((IntervalField::Minute, IntervalField::Second)) {
        fields.insert(0, "0");
    }
    let hours: i64 = fields[0].parse().ok()?;
    let minutes: i64 = fields[1].parse().ok()?;
    let (whole_seconds, frac_micros): (i64, i64) = match fields.get(2) {
        Some(sec) => match sec.split_once('.') {
            Some((whole, frac)) => {
                // Pad/truncate the fraction to six µs digits.
                let mut digits = frac.to_string();
                while digits.len() < 6 {
                    digits.push('0');
                }
                (whole.parse().ok()?, digits[..6].parse().ok()?)
            }
            None => (sec.parse().ok()?, 0),
        },
        None => (0, 0),
    };
    // A field can be spelled with arbitrarily many digits, so every step here
    // is checked: an unrepresentable literal is a rejected literal, never a
    // wrapped one.
    let total = hours
        .checked_mul(3_600_000_000)?
        .checked_add(minutes.checked_mul(60_000_000)?)?
        .checked_add(whole_seconds.checked_mul(1_000_000)?)?
        .checked_add(frac_micros)?;
    total.checked_mul(sign)
}

/// Add `whole` plus the rounded fraction of `qty` to a month accumulator, in
/// units of `per_unit` months, refusing to wrap.
fn add_months(months: &mut i64, whole: i64, frac: f64, per_unit: i64) -> Option<()> {
    let fraction = (frac * per_unit as f64).round() as i64;
    *months = months
        .checked_add(whole.checked_mul(per_unit)?)?
        .checked_add(fraction)?;
    Some(())
}

/// One interval quantity, split the way PostgreSQL's decoder reads it: the whole
/// part exactly, the fraction as a `double`.
///
/// A read of the whole token as an `f64` instead costs microseconds at the top
/// of the range. `2562047788.01521550194 hours` has more significant digits than
/// a `double` carries, and the rounding lands on a different microsecond.
#[derive(Clone, Copy)]
struct Quantity {
    whole: i64,
    frac: f64,
}

impl Quantity {
    /// Parse a signed decimal quantity. `None` for anything that is not one,
    /// including the exponent forms `f64::from_str` would otherwise accept,
    /// which PostgreSQL's interval decoder rejects.
    fn parse(text: &str) -> Option<Quantity> {
        let (negative, digits) = match text.as_bytes().first() {
            Some(b'-') => (true, &text[1..]),
            Some(b'+') => (false, &text[1..]),
            _ => (false, text),
        };
        let (int_text, frac_text) = digits.split_once('.').unwrap_or((digits, ""));
        if int_text.is_empty() && frac_text.is_empty() {
            return None;
        }
        // `.5` is fine but `-.5` is not: PostgreSQL reads the integer part first,
        // and a sign with no digit behind it leaves the sign unconsumed.
        if int_text.is_empty() && digits.len() != text.len() {
            return None;
        }
        if !int_text
            .bytes()
            .chain(frac_text.bytes())
            .all(|b| b.is_ascii_digit())
        {
            return None;
        }
        // Parse the sign WITH the digits, so `-9223372036854775808` is readable
        // (its magnitude alone is not).
        let whole: i64 = if int_text.is_empty() {
            0
        } else if negative {
            format!("-{int_text}").parse().ok()?
        } else {
            int_text.parse().ok()?
        };
        let frac: f64 = if frac_text.is_empty() {
            0.0
        } else {
            format!("0.{frac_text}").parse().ok()?
        };
        Some(Quantity {
            whole,
            frac: if negative { -frac } else { frac },
        })
    }
}

/// Add one `<qty> <unit>` term, spilling a fractional quantity into the next
/// smaller field (PG semantics). Returns `None` on arithmetic that cannot be
/// represented.
fn accumulate_unit(
    qty: Quantity,
    unit: IntervalField,
    months: &mut i64,
    days: &mut i64,
    micros: &mut i128,
) -> Option<()> {
    // The whole part of `qty`; the fractional part spills down.
    let Quantity { whole, frac } = qty;
    match unit {
        IntervalField::Millennium => {
            add_months(months, whole, frac, 12_000)?;
        }
        IntervalField::Century => {
            add_months(months, whole, frac, 1_200)?;
        }
        IntervalField::Decade => {
            add_months(months, whole, frac, 120)?;
        }
        IntervalField::Year => {
            // Fractional years → months.
            add_months(months, whole, frac, 12)?;
        }
        IntervalField::Month => {
            *months = months.checked_add(whole)?;
            // Fractional months → days (PG uses a 30-day month).
            *days = days.checked_add((frac * 30.0).trunc() as i64)?;
            let day_frac = (frac * 30.0).fract();
            *micros += (day_frac * USECS_PER_DAY_I64 as f64).round() as i128;
        }
        IntervalField::Week => {
            *days = days.checked_add(whole.checked_mul(7)?)?;
            // A fractional week spills into whole days first, then the leftover
            // part of a day into microseconds.
            *days = days.checked_add((frac * 7.0).trunc() as i64)?;
            let day_frac = (frac * 7.0).fract();
            *micros += (day_frac * USECS_PER_DAY_I64 as f64).round() as i128;
        }
        IntervalField::Day => {
            *days = days.checked_add(whole)?;
            *micros += (frac * USECS_PER_DAY_I64 as f64).round() as i128;
        }
        IntervalField::Hour => {
            *micros = micros.checked_add(i128::from(whole).checked_mul(3_600_000_000)?)?;
            *micros += (frac * 3_600_000_000.0).round() as i128;
        }
        IntervalField::Minute => {
            *micros = micros.checked_add(i128::from(whole).checked_mul(60_000_000)?)?;
            *micros += (frac * 60_000_000.0).round() as i128;
        }
        IntervalField::Second => {
            *micros = micros.checked_add(i128::from(whole).checked_mul(1_000_000)?)?;
            *micros += (frac * 1_000_000.0).round() as i128;
        }
        IntervalField::Millisecond => {
            *micros = micros.checked_add(i128::from(whole).checked_mul(1_000)?)?;
            *micros += (frac * 1_000.0).round() as i128;
        }
        IntervalField::Microsecond => {
            *micros = micros.checked_add(i128::from(whole))?;
            *micros += frac.round() as i128;
        }
    }
    Some(())
}

/// PostgreSQL's `IntervalStyle` GUC: the four spellings `interval_out` produces.
///
/// The setting is read at output time, so one stored value renders four
/// different ways.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IntervalStyle {
    /// `1 year 2 mons 3 days 04:05:06`: the default.
    #[default]
    Postgres,
    /// `@ 1 year 2 mons 3 days 4 hours 5 mins 6 secs [ago]`.
    PostgresVerbose,
    /// `+1-2 +3 +4:05:06`: SQL's year-month / day-time split.
    SqlStandard,
    /// `P1Y2M3DT4H5M6S`.
    Iso8601,
}

impl IntervalStyle {
    /// Read the style out of an `IntervalStyle` GUC value, and fall back to the
    /// default for anything unrecognized. The GUC layer rejects those, so this
    /// only guards a caller with no session behind it.
    #[must_use]
    pub fn from_setting(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "postgres_verbose" => Self::PostgresVerbose,
            "sql_standard" => Self::SqlStandard,
            "iso_8601" => Self::Iso8601,
            _ => Self::Postgres,
        }
    }
}

/// The `interval` components PostgreSQL's `interval2itm` produces: total months
/// split into years plus residual months, days on their own, and the microsecond
/// field split into a clock.
///
/// Every component is widened to `i64` because rendering negates them, and both
/// `i32::MIN` days and `i64::MIN` microseconds are ordinary interval values whose
/// magnitude PostgreSQL prints (`-2147483648 days` is `@ 2147483648 days ago`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IntervalParts {
    year: i64,
    mon: i64,
    mday: i64,
    hour: i64,
    min: i64,
    sec: i64,
    /// Microseconds, carrying the same sign as the rest of the clock.
    fsec: i64,
}

impl IntervalParts {
    fn of(iv: Interval) -> Self {
        let months = i64::from(iv.months);
        let mut rest = iv.micros;
        let hour = rest / 3_600_000_000;
        rest %= 3_600_000_000;
        let min = rest / 60_000_000;
        rest %= 60_000_000;
        let sec = rest / 1_000_000;
        let fsec = rest % 1_000_000;
        Self {
            year: months / 12,
            mon: months % 12,
            mday: i64::from(iv.days),
            hour,
            min,
            sec,
            fsec,
        }
    }

    fn negated(self) -> Self {
        Self {
            year: -self.year,
            mon: -self.mon,
            mday: -self.mday,
            hour: -self.hour,
            min: -self.min,
            sec: -self.sec,
            fsec: -self.fsec,
        }
    }

    /// The components in PostgreSQL's own order, which is the order the sign
    /// rules scan them in.
    fn all(self) -> [i64; 7] {
        [
            self.year, self.mon, self.mday, self.hour, self.min, self.sec, self.fsec,
        ]
    }

    fn has_year_month(self) -> bool {
        self.year != 0 || self.mon != 0
    }

    fn has_clock(self) -> bool {
        self.hour != 0 || self.min != 0 || self.sec != 0 || self.fsec != 0
    }

    fn clock_is_negative(self) -> bool {
        self.hour < 0 || self.min < 0 || self.sec < 0 || self.fsec < 0
    }
}

/// PostgreSQL's `AppendSeconds`: the seconds magnitude (the sign is always the
/// caller's to emit), optionally zero-padded to two digits, with a fractional
/// tail whose trailing zeros are trimmed.
fn append_seconds(out: &mut String, sec: i64, fsec: i64, fill_zeros: bool) {
    let whole = sec.unsigned_abs();
    if fill_zeros {
        out.push_str(&format!("{whole:02}"));
    } else {
        out.push_str(&format!("{whole}"));
    }
    if fsec != 0 {
        let mut digits = format!("{:06}", fsec.unsigned_abs());
        while digits.ends_with('0') {
            digits.pop();
        }
        out.push('.');
        out.push_str(&digits);
    }
}

/// Render an `interval` in the session's `IntervalStyle`.
#[must_use]
pub fn interval_to_text_in(iv: Interval, style: IntervalStyle) -> String {
    match iv.infinite_sign() {
        1 => return "infinity".to_string(),
        -1 => return "-infinity".to_string(),
        _ => {}
    }
    match style {
        IntervalStyle::Postgres => interval_to_text(iv),
        IntervalStyle::PostgresVerbose => interval_postgres_verbose(IntervalParts::of(iv)),
        IntervalStyle::SqlStandard => interval_sql_standard(IntervalParts::of(iv)),
        IntervalStyle::Iso8601 => interval_iso_8601(IntervalParts::of(iv)),
    }
}

/// `postgres_verbose`: `@ 1 year 2 mons 3 days 4 hours 5 mins 6 secs [ago]`.
///
/// The sign of the *first* non-zero component decides the `ago` suffix; when it
/// is negative every component is negated before printing, so a mixed-sign value
/// keeps inner minus signs: `@ 10 mons 3 days -3 hours -55 mins -6 secs ago`.
fn interval_postgres_verbose(parts: IntervalParts) -> String {
    let is_before = parts
        .all()
        .into_iter()
        .find(|value| *value != 0)
        .is_some_and(|value| value < 0);
    let parts = if is_before { parts.negated() } else { parts };
    let mut out = String::from("@");
    for (value, unit) in [
        (parts.year, "year"),
        (parts.mon, "mon"),
        (parts.mday, "day"),
        (parts.hour, "hour"),
        (parts.min, "min"),
    ] {
        if value == 0 {
            continue;
        }
        // PostgreSQL pluralizes on the value, not its magnitude, so a negative
        // one is plural (`-1 days`) while the seconds field below is not.
        let plural = if value == 1 { "" } else { "s" };
        out.push_str(&format!(" {value} {unit}{plural}"));
    }
    if parts.sec != 0 || parts.fsec != 0 {
        out.push(' ');
        if parts.sec < 0 || (parts.sec == 0 && parts.fsec < 0) {
            out.push('-');
        }
        append_seconds(&mut out, parts.sec, parts.fsec, false);
        let plural = if parts.sec.unsigned_abs() == 1 && parts.fsec == 0 {
            ""
        } else {
            "s"
        };
        out.push_str(&format!(" sec{plural}"));
    }
    if out == "@" {
        out.push_str(" 0");
    }
    if is_before {
        out.push_str(" ago");
    }
    out
}

/// `sql_standard`: one leading sign and either a `Y-M` year-month literal or a
/// `[D ]H:MM:SS` day-time literal when the value fits the SQL standard's shape.
///
/// A value that mixes signs, or that carries both a year-month and a day-time
/// part, cannot be spelled that way, so every component gets an explicit sign
/// instead and the result is deliberately non-standard: `+0-1 -1 +0:00:00`.
fn interval_sql_standard(parts: IntervalParts) -> String {
    let has_negative = parts.all().iter().any(|value| *value < 0);
    let has_positive = parts.all().iter().any(|value| *value > 0);
    let has_year_month = parts.has_year_month();
    let has_day_time = parts.mday != 0 || parts.has_clock();
    let standard = !(has_negative && has_positive) && !(has_year_month && has_day_time);

    let mut out = String::new();
    let parts = if has_negative && standard {
        out.push('-');
        parts.negated()
    } else {
        parts
    };

    if !has_negative && !has_positive {
        out.push('0');
    } else if !standard {
        let year_sign = if parts.year < 0 || parts.mon < 0 {
            '-'
        } else {
            '+'
        };
        let day_sign = if parts.mday < 0 { '-' } else { '+' };
        let sec_sign = if parts.clock_is_negative() { '-' } else { '+' };
        out.push_str(&format!(
            "{year_sign}{}-{} {day_sign}{} {sec_sign}{}:{:02}:",
            parts.year.unsigned_abs(),
            parts.mon.unsigned_abs(),
            parts.mday.unsigned_abs(),
            parts.hour.unsigned_abs(),
            parts.min.unsigned_abs()
        ));
        append_seconds(&mut out, parts.sec, parts.fsec, true);
    } else if has_year_month {
        out.push_str(&format!("{}-{}", parts.year, parts.mon));
    } else {
        if parts.mday != 0 {
            out.push_str(&format!("{} ", parts.mday));
        }
        out.push_str(&format!("{}:{:02}:", parts.hour, parts.min));
        append_seconds(&mut out, parts.sec, parts.fsec, true);
    }
    out
}

/// `iso_8601`: `P[n]Y[n]M[n]DT[n]H[n]M[n]S` with zero components omitted, and
/// the all-zero interval spelled `PT0S`.
fn interval_iso_8601(parts: IntervalParts) -> String {
    if parts.all().iter().all(|value| *value == 0) {
        return "PT0S".to_string();
    }
    let mut out = String::from("P");
    for (value, designator) in [(parts.year, 'Y'), (parts.mon, 'M'), (parts.mday, 'D')] {
        if value != 0 {
            out.push_str(&format!("{value}{designator}"));
        }
    }
    if parts.has_clock() {
        out.push('T');
    }
    for (value, designator) in [(parts.hour, 'H'), (parts.min, 'M')] {
        if value != 0 {
            out.push_str(&format!("{value}{designator}"));
        }
    }
    if parts.sec != 0 || parts.fsec != 0 {
        if parts.sec < 0 || parts.fsec < 0 {
            out.push('-');
        }
        append_seconds(&mut out, parts.sec, parts.fsec, false);
        out.push('S');
    }
    out
}

/// Render an `interval` in PostgreSQL's `postgres` IntervalStyle (the default):
/// `[<y> year[s]] [<m> mons] [<d> days] [±HH:MM:SS[.ffffff]]`; a fully-zero
/// interval prints `00:00:00`.
///
/// The sign placement is PostgreSQL's: each field carries its own sign, and a
/// *positive* field gets an explicit `+` when the last non-zero field before it
/// was negative, hence `-3 days +04:05:06`.
pub fn interval_to_text(iv: Interval) -> String {
    match iv.infinite_sign() {
        1 => return "infinity".to_string(),
        -1 => return "-infinity".to_string(),
        _ => {}
    }
    let mut out = String::new();
    let mut after_negative = false;

    // Year/month component, derived from total months. PostgreSQL pluralizes the
    // unit name unless the value is *exactly* 1 (so `-1` and `2` are plural, only
    // `1` is singular — `1 year`, `-1 days`, `2 mons`).
    for (value, unit) in [
        (i64::from(iv.months) / 12, "year"),
        (i64::from(iv.months) % 12, "mon"),
        (i64::from(iv.days), "day"),
    ] {
        if value == 0 {
            continue;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        if after_negative && value > 0 {
            out.push('+');
        }
        out.push_str(&format!(
            "{value} {unit}{}",
            if value == 1 { "" } else { "s" }
        ));
        after_negative = value < 0;
    }

    if iv.micros != 0 {
        if !out.is_empty() {
            out.push(' ');
        }
        if after_negative && iv.micros > 0 {
            out.push('+');
        }
        out.push_str(&format_clock(iv.micros));
    }
    if out.is_empty() {
        // A fully-zero interval prints the clock zero.
        return "00:00:00".to_string();
    }
    out
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

    /// The year `to_char` PRINTS. PostgreSQL never prints a negative year for a
    /// date/time: the astronomical year `0` is `1 BC` and prints as `1`, so a
    /// non-positive year is folded to `1 - year` and the era is left to the
    /// `AD`/`BC` patterns. An interval has no era, and PostgreSQL prints its
    /// year field signed, so `IntervalFields` overrides this.
    fn display_year(&self) -> i64 {
        let year = self.year();
        if year <= 0 { 1 - year } else { year }
    }

    /// PostgreSQL `date2j`: the Julian Day Number of the source's calendar date,
    /// backing the `J` pattern. The year is the astronomical one, so this is
    /// continuous across the BC/AD boundary.
    fn julian_day(&self) -> i64 {
        let (mut year, mut month) = (self.year(), self.month());
        if month > 2 {
            month += 1;
            year += 4800;
        } else {
            month += 13;
            year += 4799;
        }
        let century = year / 100;
        let mut julian = year * 365 - 32167;
        julian += year / 4 - century + century / 4;
        julian + 7834 * month / 256 + self.day()
    }

    /// Index into the 12-entry month-name/Roman tables. A datetime's `month` is
    /// always `1..=12`; an interval's `months % 12` can be `0..=11` (or negative),
    /// so this maps the raw value into `0..=11` rather than a panic on an
    /// out-of-range subscript. (Month NAMES on an interval are not a corpus
    /// case; this only keeps the shared renderer total. See Task 9.)
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
/// PostgreSQL `interval2tm`: this code reads the stored `months`/`days`/`micros`
/// component-wise and does NOT normalize across the day/month boundary.
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
    /// An interval has no era, so PostgreSQL prints its year field as it stands,
    /// sign included (`make_interval(months => -12)` renders `YYYY` as `-0001`).
    fn display_year(&self) -> i64 {
        self.year()
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

/// Full English month names (1-indexed). C/English locale only; `TM` (locale
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
/// STORED `months`/`days`/`micros` (PG `interval2tm`; clock fields are NOT
/// normalized across the day/month boundary, so e.g. `HH24` of a `36 hour`
/// interval is `36`). It shares the exact tokenizer/renderer with
/// `format_datetime` through the `FieldSource` indirection.
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
        if matches_ci(&chars, i, "FM") {
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

/// Does `chars[i..]` start with the ASCII keyword `kw`, ignoring case?
///
/// PostgreSQL's template lexer is case-insensitive for every pattern whose case
/// does not decide the output, so `yyyy`, `hh24`, `of` and `ff3` all work. The
/// patterns that DO carry their casing into the result (`MONTH`/`Month`/`month`,
/// `DY`/`Dy`/`dy`, `RM`/`rm`, `AM`/`am`, `TH`/`th`, the era words) keep using
/// [`matches_at`] so each spelling stays a distinct keyword.
fn matches_ci(chars: &[char], i: usize, kw: &str) -> bool {
    let kw: Vec<char> = kw.chars().collect();
    if i + kw.len() > chars.len() {
        return false;
    }
    chars[i..i + kw.len()]
        .iter()
        .zip(kw.iter())
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
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
/// `RM`/`rm` in a field as wide as the widest numeral, "VIII"; oracle-confirmed,
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
/// interval source (where AM/PM has no clock meaning, not a corpus case); for a
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
/// `HH`/`HH12` has no clock meaning, not a corpus case); `rem_euclid` keeps the
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
    if matches_ci(chars, i, "YYYY") {
        let v = f.display_year();
        return Ok(Some((4, pad_num(v, 4, fm), Some(v))));
    }
    if matches_ci(chars, i, "YYY") {
        let v = f.display_year().rem_euclid(1000);
        return Ok(Some((3, pad_num(v, 3, fm), Some(v))));
    }
    // `Y,YYY`: comma-grouped 4-digit year (the comma grouping is kept even under
    // FM; PG's FM only suppresses leading zeros, not the group separator).
    if matches_ci(chars, i, "Y,YYY") {
        let y = f.display_year();
        let s = format!("{},{:03}", y / 1000, (y % 1000).abs());
        return Ok(Some((5, s, Some(y))));
    }
    if matches_ci(chars, i, "YY") {
        let v = f.display_year().rem_euclid(100);
        return Ok(Some((2, pad_num(v, 2, fm), Some(v))));
    }
    if matches_ci(chars, i, "Y") {
        let v = f.display_year().rem_euclid(10);
        return Ok(Some((1, pad_num(v, 1, fm), Some(v))));
    }
    // -- ISO patterns (longest first so `IDDD`/`IYYY` win over `IY`/`IW`/`ID`/`I`) --
    if matches_ci(chars, i, "IDDD") {
        // ISO day-of-year: (iso_week - 1) * 7 + iso_dow.
        let v = (f.iso_week() - 1) * 7 + f.iso_dow();
        return Ok(Some((4, pad_num(v, 3, fm), Some(v))));
    }
    if matches_ci(chars, i, "IYYY") {
        return Ok(Some((4, pad_num(f.iso_year(), 4, fm), Some(f.iso_year()))));
    }
    if matches_ci(chars, i, "IYY") {
        let v = f.iso_year().rem_euclid(1000);
        return Ok(Some((3, pad_num(v, 3, fm), Some(v))));
    }
    if matches_ci(chars, i, "IW") {
        return Ok(Some((2, pad_num(f.iso_week(), 2, fm), Some(f.iso_week()))));
    }
    if matches_ci(chars, i, "IY") {
        let v = f.iso_year().rem_euclid(100);
        return Ok(Some((2, pad_num(v, 2, fm), Some(v))));
    }
    if matches_ci(chars, i, "ID") {
        let v = f.iso_dow();
        return Ok(Some((2, v.to_string(), Some(v))));
    }
    if matches_ci(chars, i, "I") {
        let v = f.iso_year().rem_euclid(10);
        return Ok(Some((1, pad_num(v, 1, fm), Some(v))));
    }
    // -- century --
    if matches_ci(chars, i, "CC") {
        // Century of year Y: `ceil(Y/100)` for AD years (Y ≥ 1 → `(Y+99)/100`),
        // and the floor form `(Y-99)/100` for Y ≤ 0 (BC / proleptic year 0). The
        // test is written `y < 1` (not `y > 0`) so the boundary year 1 — which the
        // two branches map to 1 vs 0 — makes the comparison observable (a year-1
        // unit test pins it).
        let y = f.year();
        let c = if y < 1 { y / 100 - 1 } else { (y + 99) / 100 };
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
    if matches_ci(chars, i, "MM") {
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
    if matches_ci(chars, i, "DDD") {
        return Ok(Some((3, pad_num(f.doy(), 3, fm), Some(f.doy()))));
    }
    if matches_ci(chars, i, "DD") {
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
    if matches_ci(chars, i, "D") {
        let v = f.dow();
        return Ok(Some((1, v.to_string(), Some(v))));
    }
    // -- week / quarter (the ISO `IW` is handled in the ISO group above) --
    if matches_ci(chars, i, "WW") {
        return Ok(Some((
            2,
            pad_num(f.week_of_year(), 2, fm),
            Some(f.week_of_year()),
        )));
    }
    if matches_ci(chars, i, "W") {
        let v = f.week_of_month();
        return Ok(Some((1, v.to_string(), Some(v))));
    }
    if matches_ci(chars, i, "Q") {
        let v = (f.month() - 1) / 3 + 1;
        return Ok(Some((1, v.to_string(), Some(v))));
    }
    // `J`: Julian Day Number, never padded.
    if matches_ci(chars, i, "J") {
        let v = f.julian_day();
        return Ok(Some((1, v.to_string(), Some(v))));
    }
    // -- time (HH24 before HH12/HH; SSSSS before SSSS before SS) --
    if matches_ci(chars, i, "HH24") {
        return Ok(Some((4, pad_num(f.hour(), 2, fm), Some(f.hour()))));
    }
    if matches_ci(chars, i, "HH12") {
        let v = hour12(f.hour());
        return Ok(Some((4, pad_num(v, 2, fm), Some(v))));
    }
    if matches_ci(chars, i, "HH") {
        let v = hour12(f.hour());
        return Ok(Some((2, pad_num(v, 2, fm), Some(v))));
    }
    if matches_ci(chars, i, "MI") {
        return Ok(Some((2, pad_num(f.minute(), 2, fm), Some(f.minute()))));
    }
    // `SSSS`/`SSSSS` (seconds past midnight): PostgreSQL does NOT zero-pad these
    // (e.g. `00:00:05` → `5`, not `0005`); they render as a bare decimal.
    if matches_ci(chars, i, "SSSSS") {
        let v = f.hour() * 3600 + f.minute() * 60 + f.second();
        return Ok(Some((5, v.to_string(), Some(v))));
    }
    if matches_ci(chars, i, "SSSS") {
        let v = f.hour() * 3600 + f.minute() * 60 + f.second();
        return Ok(Some((4, v.to_string(), Some(v))));
    }
    if matches_ci(chars, i, "SS") {
        return Ok(Some((2, pad_num(f.second(), 2, fm), Some(f.second()))));
    }
    if matches_ci(chars, i, "MS") {
        // Milliseconds: micros / 1000, 3 digits.
        let v = f.micros() / 1000;
        return Ok(Some((2, pad_num(v, 3, fm), Some(v))));
    }
    if matches_ci(chars, i, "US") {
        let v = f.micros();
        return Ok(Some((2, pad_num(v, 6, fm), Some(v))));
    }
    // FF1..FF6: fractional seconds to N digits.
    if matches_ci(chars, i, "FF") && i + 2 < chars.len() && chars[i + 2].is_ascii_digit() {
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
    if matches_ci(chars, i, "TZH") {
        let s = match f.tz_offset_secs() {
            Some(secs) => offset_hh(secs),
            None => String::new(),
        };
        return Ok(Some((3, s, None)));
    }
    if matches_ci(chars, i, "TZM") {
        let s = match f.tz_offset_secs() {
            Some(secs) => format!("{:02}", (secs.unsigned_abs() % 3600) / 60),
            None => String::new(),
        };
        return Ok(Some((3, s, None)));
    }
    if matches_ci(chars, i, "OF") {
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
/// are matched leniently (PostgreSQL largely ignores separators, so a non-alphanumeric
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

/// Consume up to `max` leading ASCII digits from `chars` at `*i` and return the
/// value. This function needs at least one digit (PG: a number-expecting pattern
/// with a non-digit there is an error), and it returns `None` otherwise.
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
/// otherwise match a full month name (longest match; the input must begin with
/// the full name). Returns the 1-based month, or `None` if no name matches.
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
/// mismatch is tolerated, because PG does not hard-fail a literal mismatch, and
/// the cursor stays in place). A separator/punctuation literal matches
/// leniently: it skips a run of leading separator chars in the input (PG largely
/// ignores separators, so e.g. an input `-` matches a template `/`).
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
    if iv.is_infinite() {
        return Ok(iv);
    }
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
    if iv.is_infinite() {
        return Ok(iv);
    }
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
    if iv.is_infinite() {
        return Ok(iv);
    }
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
        // A BC year renders UNSIGNED, as PostgreSQL does — the era is carried by
        // `BC`/`AD`, never by a minus sign, so `to_char(…, 'YYYY')` of a BC date
        // is `0101` and only `'YYYY BC'` shows the era.
        //
        // The value is off by one between the two calendars and that is not a
        // bug: jiff counts proleptic ISO years, which include a year 0, so ISO
        // -100 is 101 BC. PostgreSQL agrees — `extract(year from timestamp
        // '0101-01-01 BC')` is -101 while ISO calls the same day -100 — and
        // `to_char(timestamp '0101-01-01 BC', 'YYYY')` is `0101`.
        let bc = DateTimeFields::from_civil(
            jiff::civil::DateTime::constant(-100, 6, 15, 0, 0, 0, 0),
            None,
        );
        assert_eq!(format_datetime("YYYY", &bc).expect("neg"), "0101");
        // `CC` of the same day is `-02`, matching
        // `to_char(timestamp '0101-01-01 BC', 'CC')` on the oracle, and exercises
        // the BC (year ≤ 0) century branch — killing the century arithmetic
        // mutants (`- → +`, `- → /`, `/ → %`, `/ → *`).
        assert_eq!(format_datetime("CC", &bc).expect("cc"), "-02");
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

    fn iv(months: i32, days: i32, micros: i64) -> Interval {
        Interval {
            months,
            days,
            micros,
        }
    }

    /// PostgreSQL's interval decoder records which fields a literal has already
    /// supplied and rejects a second one outright, so `'1 day 1 day'` is a
    /// syntax error, not two days. A decoder that only adds each term instead
    /// answers a plausible-looking wrong interval with nothing to signal it.
    #[test]
    fn a_repeated_interval_field_is_rejected() {
        use assert2::assert;

        let rejected = [
            "1 second 2 seconds",
            "10 milliseconds 20 milliseconds",
            // A FRACTIONAL second reaches the millisecond and microsecond fields,
            // so it supplies all three and collides with either of them.
            "5.5 seconds 3 milliseconds",
            "3 milliseconds 5.5 seconds",
            // A clock term supplies hours through microseconds.
            "1:20:05 5 microseconds",
            "1:00 2:00",
            "1 day 1 day",
            "1 day 2 hours 3 hours",
            "1 year 1 month 1 year",
            "1 week 1 week",
            "1 mon 1 month",
            "1 decade 1 decade",
            "1-2 3-4",
            "@ 1 day 1 day ago",
            // A bare quantity keeps the unit of the one to its right, so a second
            // bare quantity repeats that field.
            "123 11",
            "1 2 3",
            // Each ISO-8601 designator belongs to one half of the duration.
            "PT1Y",
            "PT1W",
            "PT1D",
            "P1H",
            "P1S",
            "P1DT1D",
            // A sign with no digit behind it never reaches the fraction.
            "-.5 seconds",
            "+.5 seconds",
        ];
        for literal in rejected {
            let error = parse_interval(literal).expect_err(literal);
            assert!(error.sqlstate() == "22007", "{literal}");
        }
    }

    /// The distinct-field literals the rule above must NOT reject, with the value
    /// each one carries.
    #[test]
    fn distinct_interval_fields_still_accumulate() {
        use assert2::assert;

        let accepted = [
            ("1 week 2 days", iv(0, 9, 0)),
            ("1 month 1 week", iv(1, 7, 0)),
            // A WHOLE second supplies only the second field, so a microsecond
            // term beside it is fine.
            ("5 seconds 3 microseconds", iv(0, 0, 5_000_003)),
            ("5.5 milliseconds 3 microseconds", iv(0, 0, 5_503)),
            ("1 minute 30 seconds", iv(0, 0, 90_000_000)),
            ("1 decade 1 year", iv(132, 0, 0)),
            ("1 century 1 decade", iv(1_320, 0, 0)),
            ("1-2 3", iv(14, 0, 3_000_000)),
            ("1 day 2", iv(0, 1, 2_000_000)),
            (".5 seconds", iv(0, 0, 500_000)),
        ];
        for (literal, expected) in accepted {
            assert!(
                parse_interval(literal).expect(literal) == expected,
                "{literal}"
            );
        }
    }

    /// A bare quantity takes the unit of the quantity to its right, and steps to
    /// DAY in exactly one place, after an hour. `'1 2' MINUTE` is therefore a
    /// repeated field rather than an hour and a minute.
    #[test]
    fn a_bare_quantity_keeps_its_neighbours_unit_except_after_an_hour() {
        use assert2::assert;

        let range = |start, end| Some((start, end));
        let accepted = [
            (
                "4 5",
                range(IntervalField::Day, IntervalField::Hour),
                iv(0, 4, 18_000_000_000),
            ),
            (
                "1 2",
                range(IntervalField::Hour, IntervalField::Hour),
                iv(0, 1, 7_200_000_000),
            ),
            (
                "1 2:03",
                range(IntervalField::Day, IntervalField::Hour),
                iv(0, 1, 7_200_000_000),
            ),
        ];
        for (literal, range, expected) in accepted {
            assert!(
                parse_interval_ranged(literal, range).expect(literal) == expected,
                "{literal}"
            );
        }
        let rejected = [
            ("1 2", range(IntervalField::Day, IntervalField::Minute)),
            ("1 2", range(IntervalField::Hour, IntervalField::Minute)),
            ("1 2", range(IntervalField::Minute, IntervalField::Second)),
            ("1 2", range(IntervalField::Day, IntervalField::Day)),
            ("1 2", range(IntervalField::Year, IntervalField::Month)),
            ("1 2 3", range(IntervalField::Day, IntervalField::Hour)),
        ];
        for (literal, range) in rejected {
            assert!(parse_interval_ranged(literal, range).is_err(), "{literal}");
        }
    }

    /// The whole part of a quantity is read exactly. Reading the whole token as a
    /// `f64` loses microseconds at the top of the range, which showed up as a
    /// maximal interval reading back several hundred microseconds short.
    #[test]
    fn a_wide_quantity_keeps_every_microsecond() {
        use assert2::assert;

        let max = iv(0, 0, i64::MAX);
        let cases = [
            ("2562047788.01521550194 hours", max),
            ("9223372036854.775807 seconds", max),
            ("PT2562047788H54.775807S", max),
            ("-2562047788.01521550222 hours", iv(0, 0, i64::MIN)),
            ("-9223372036854.775808 seconds", iv(0, 0, i64::MIN)),
        ];
        for (literal, expected) in cases {
            let parsed = parse_interval(literal).expect(literal);
            assert!(parsed.micros == expected.micros, "{literal}");
            // ... and such an interval is FINITE: only the whole triple of field
            // extremes is the reserved infinity encoding.
            assert!(!parsed.is_infinite(), "{literal}");
        }
        assert!(Interval::INFINITY.is_infinite());
        assert!(Interval::NEG_INFINITY.is_infinite());
        assert!(interval_to_text(Interval::INFINITY) == "infinity");
        assert!(interval_to_text(Interval::NEG_INFINITY) == "-infinity");
        assert!(Interval::INFINITY > iv(0, 0, i64::MAX));
        assert!(Interval::NEG_INFINITY < iv(0, 0, i64::MIN));
    }

    /// Two FINITE operands landing exactly on the reserved encoding have run out
    /// of range; answering `infinity` there would be a wrong answer that returns
    /// cleanly. An operand that is ALREADY infinite still propagates.
    #[test]
    fn arithmetic_that_lands_on_the_infinity_encoding_is_22008() {
        use assert2::assert;

        let extreme = iv(-2_147_483_647, -2_147_483_647, -9_223_372_036_854_775_807);
        let one = iv(1, 1, 1);
        let errors = [
            neg_interval(extreme).err(),
            add_interval(extreme, neg_interval(one).expect("negate one")).err(),
            sub_interval(extreme, one).err(),
        ];
        for error in errors {
            let error = error.expect("refused");
            assert!(error.sqlstate() == "22008");
            assert!(error.to_string() == "interval out of range");
        }
        // An already-infinite operand propagates rather than erroring.
        assert!(neg_interval(Interval::INFINITY).expect("negate") == Interval::NEG_INFINITY);
        assert!(add_interval(Interval::INFINITY, iv(0, 1, 0)).expect("add") == Interval::INFINITY);
    }

    /// An interval field can be spelled with arbitrarily many digits. Before
    /// these were checked, a wide one wrapped in release and aborted the whole
    /// process in debug; PostgreSQL rejects them.
    #[test]
    fn interval_fields_too_wide_to_represent_are_rejected_not_wrapped() {
        let cases = [
            "9223372036854775807 years",
            "-9223372036854775807 years",
            "9223372036854775807 months",
            "9223372036854775807 weeks",
            "9223372036854775807 days",
            "9223372036854775807 hours",
            "100000000000000:00:00",
            "9223372036854775807 millennium",
            "9223372036854775807 centuries",
            "9223372036854775807 decades",
        ];

        for case in cases {
            assert!(
                parse_interval(case).is_err(),
                "`{case}` must be rejected, not wrapped"
            );
        }
    }

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
    /// `rint` on the microsecond value does: half to even, not truncated and
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
    /// midnight of the day it started in, because that is a different instant.
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

    /// A fraction on a two-field clock reading means the fields were minutes and
    /// seconds, so PostgreSQL shifts them down an hour rather than reading the
    /// leading field as hours; anywhere else a fraction is malformed.
    #[test]
    fn a_fraction_on_a_two_field_clock_shifts_to_minutes_and_seconds() {
        use assert2::assert;

        assert!(time_to_text(parse_time("12:34.5").expect("mm:ss.f")) == "00:12:34.5");
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
        // The largest FINITE micros (`i64::MAX` itself is the `infinity`
        // sentinel) times 1000 is ≈ 9.22e21, far above i64::MAX; the fixed code
        // must return Err(Overflow), not Ok with a saturated i64::MAX value.
        let big = Interval {
            months: 0,
            days: 0,
            micros: i64::MAX - 1,
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
        // The two i64 extremes are PostgreSQL's reserved non-finite encodings,
        // so they decode to infinity rather than overflowing the epoch rebase.
        assert!(timestamptz_from_binary(&i64::MAX.to_be_bytes()) == Ok(timestamptz_infinity()));
        assert!(timestamptz_from_binary(&i64::MIN.to_be_bytes()) == Ok(timestamptz_neg_infinity()));
        assert!(timestamp_from_binary(&i64::MAX.to_be_bytes()) == Ok(TIMESTAMP_INFINITY));
        assert!(date_from_binary(&i32::MIN.to_be_bytes()) == Ok(DATE_NEG_INFINITY));
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

    #[test]
    fn timestamp_plus_interval_reports_oversized_day_span() {
        let base = parse_timestamp("2024-01-01 00:00:00").expect("ts");
        assert!(matches!(
            timestamp_plus_interval(base, iv(0, 106_000_000, 0)),
            Err(TypeError::DatetimeFieldOverflow { .. })
        ));
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
        assert2::assert!(timestamptz_diff(a, b) == Ok(iv(0, 2, 8_999_750_000)));
        // The reverse is the negation (proves the `-` is a real subtraction).
        assert2::assert!(timestamptz_diff(b, a) == Ok(iv(0, -2, -8_999_750_000)));
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
        // A clock term ADDS to whatever microseconds an earlier term already
        // spilled there (the `micros += parse_clock_term` path): 1.5 days is a
        // day and twelve hours, and the clock term puts another hour on top.
        assert2::assert!(
            parse_interval("1.5 days 01:00:00").expect("iv") == iv(0, 1, 13 * 3_600_000_000)
        );
        // Two clock terms would supply the hour/minute/second fields twice, which
        // PostgreSQL rejects outright rather than summing.
        assert2::assert!(parse_interval("01:00:00 00:30:00").is_err());
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
            micros: i64::MAX - 1,
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
            micros: i64::MAX - 1,
        })
        .expect_err("rolled month total exceeds i32 in justify_interval");
        assert_eq!(err.sqlstate(), "22008");
    }
}
