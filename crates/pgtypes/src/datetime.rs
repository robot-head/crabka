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
    tz::{AmbiguousOffset, Offset, TimeZone},
};

use crate::TypeError;

mod parse;
mod tzdb;

pub use self::{
    parse::{
        DateOrder, DecodeError, DecodeMode, Decoded, Parts, Special, Zone, decode,
        decode_numeric_time_zone, resolve_guc_time_zone, resolve_time_zone,
    },
    tzdb::zone_by_name,
};

// ---------------------------------------------------------------------------
// Non-finite values. `date`, `timestamp`, `timestamptz` and `interval` each have
// a `+infinity` and a `-infinity` that sort outside every finite value and are
// carried through arithmetic rather than computed with. PostgreSQL reserves the
// extreme representable value of each type's storage for them; crabka does the
// same where the storage has room to spare, so ordering, equality, grouping and
// index keys all come out right with no extra case in the comparison paths.
//
// `date` has no room to spare. PostgreSQL stores a `date` as a day count, and
// the two values it reserves are millions of days outside the calendar it can
// spell, so no literal reaches them. jiff stores a civil date instead, and its
// extremes ARE the civil dates 9999-12-31 and -9999-01-01. A user can write the
// top one. So `date` keeps its two non-finite values OUT of band, in `PgDate`,
// and reserves no civil date at all.
// ---------------------------------------------------------------------------

/// A PostgreSQL `date`: one civil date, or one of the two non-finite values.
///
/// The variants are declared in sort order. The derived [`Ord`] is therefore
/// PostgreSQL's date ordering, with `-infinity` below every civil date and
/// `infinity` above every civil date. No comparison path needs a case for the
/// non-finite values, and the derived [`Hash`] and [`Eq`] agree with that order.
///
/// The wire and on-disk form does not change. [`date_to_binary`] still reserves
/// `i32::MIN` and `i32::MAX` for the two non-finite values, as PostgreSQL's
/// `date_send` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PgDate {
    /// `date '-infinity'`.
    NegInfinity,
    /// A civil date on the calendar.
    Finite(Date),
    /// `date 'infinity'`.
    Infinity,
}

impl PgDate {
    /// The civil date, or `None` for a non-finite value.
    ///
    /// Call this only where the calendar fields are necessary. The arithmetic
    /// and formatting helpers in this module take a whole [`PgDate`] and carry
    /// the non-finite values themselves.
    #[must_use]
    pub fn finite(self) -> Option<Date> {
        match self {
            PgDate::Finite(d) => Some(d),
            _ => None,
        }
    }

    /// `1` for `infinity`, `-1` for `-infinity`, `0` for a civil date.
    #[must_use]
    pub fn sign(self) -> i32 {
        match self {
            PgDate::NegInfinity => -1,
            PgDate::Finite(_) => 0,
            PgDate::Infinity => 1,
        }
    }
}

impl From<Date> for PgDate {
    fn from(d: Date) -> Self {
        PgDate::Finite(d)
    }
}

/// `date 'infinity'`.
pub const DATE_INFINITY: PgDate = PgDate::Infinity;
/// `date '-infinity'`.
pub const DATE_NEG_INFINITY: PgDate = PgDate::NegInfinity;
/// `timestamp 'infinity'`.
pub const TIMESTAMP_INFINITY: DateTime = DateTime::MAX;
/// `timestamp '-infinity'`.
pub const TIMESTAMP_NEG_INFINITY: DateTime = DateTime::MIN;

/// Whether a `date` is one of the two non-finite values.
#[must_use]
pub fn date_is_infinite(d: PgDate) -> bool {
    d.finite().is_none()
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
pub fn date_infinite_sign(d: PgDate) -> i32 {
    d.sign()
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
pub fn date_infinity_of_sign(sign: i32) -> PgDate {
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

/// The interval `end - start` when either endpoint is non-finite, given the two
/// endpoints' [`timestamp_infinite_sign`]-style signs. `None` means both
/// endpoints are finite and the caller must do the arithmetic itself.
///
/// One rule serves every timestamp-difference operator, because PostgreSQL gives
/// them all the same answer: two infinities of the SAME sign would have to
/// cancel, which is `interval out of range`; otherwise the infinite endpoint
/// decides, and `end`'s sign wins over a negated `start`'s. `age` needs it too —
/// it is a difference with month borrowing, and the borrowing has nothing to
/// borrow from once an endpoint is infinite.
#[must_use]
pub fn infinite_interval_difference(
    end_sign: i32,
    start_sign: i32,
) -> Option<Result<Interval, TypeError>> {
    let sign = combine_infinite(end_sign, -start_sign)?;
    Some(if sign == 0 {
        Err(interval_out_of_range())
    } else {
        Ok(Interval::infinity_of_sign(sign))
    })
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
///
/// The subtraction is done on the fields themselves, not as `a + (-b)`. PG's
/// `interval_mi` does the same, and the difference is observable: `-b` alone
/// leaves the range whenever a field of `b` is `i32::MIN`/`i64::MIN`, so
/// negate-then-add would refuse `f1 - f1` for the very intervals that sit at the
/// bottom of the range, where the answer is plainly zero.
pub fn sub_interval(a: Interval, b: Interval) -> Result<Interval, TypeError> {
    if let Some(sign) = combine_infinite(a.infinite_sign(), -b.infinite_sign()) {
        if sign == 0 {
            return Err(interval_out_of_range());
        }
        return Ok(Interval::infinity_of_sign(sign));
    }
    let months = a
        .months
        .checked_sub(b.months)
        .ok_or_else(interval_out_of_range)?;
    let days = a
        .days
        .checked_sub(b.days)
        .ok_or_else(interval_out_of_range)?;
    let micros = a
        .micros
        .checked_sub(b.micros)
        .ok_or_else(interval_out_of_range)?;
    finite_interval(Interval {
        months,
        days,
        micros,
    })
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

/// Seconds in a day, as PostgreSQL's `SECS_PER_DAY`.
const SECS_PER_DAY_F64: f64 = 86_400.0;
/// Days in an interval month, as PostgreSQL's `DAYS_PER_MONTH`.
const DAYS_PER_MONTH_F64: f64 = 30.0;
/// Microseconds in a second, as PostgreSQL's `USECS_PER_SEC`.
const USECS_PER_SEC_F64: f64 = 1_000_000.0;

/// `PostgreSQL`'s `TSROUND`: round to microsecond resolution. The cascade needs
/// it because a factor such as `1/3` leaves a tail far below a microsecond that
/// would otherwise decide which side of a whole day the remainder falls on.
fn ts_round(value: f64) -> f64 {
    (value * USECS_PER_SEC_F64).round_ties_even() / USECS_PER_SEC_F64
}

/// `PostgreSQL`'s `FLOAT8_FITS_IN_INT32`. `NaN` fails both comparisons, so this
/// screens it out too.
fn fits_in_i32(value: f64) -> bool {
    (-2_147_483_648.0_f64..2_147_483_648.0_f64).contains(&value)
}

/// `PostgreSQL`'s `FLOAT8_FITS_IN_INT64`.
fn fits_in_i64(value: f64) -> bool {
    (-9_223_372_036_854_775_808.0_f64..9_223_372_036_854_775_808.0_f64).contains(&value)
}

/// The sign of an interval as a whole (`interval_sign`): the sign of its
/// canonical 30-day-month, 24-hour-day microsecond value.
fn interval_sign(a: Interval) -> i32 {
    match a.canonical_micros().cmp(&0) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/// Scale every field of a FINITE interval by `scale`, cascading the fractions
/// down. This is the body `interval_mul` and `interval_div` share: the two
/// differ only in whether `scale` multiplies or divides, and doing the division
/// directly rather than as multiplication by the reciprocal is what keeps
/// `interval '4 mons 4 days 40:48:00' / 10` on `PostgreSQL`'s answer.
///
/// Fractions cascade DOWN only — months into days at 30 days a month, days into
/// microseconds at 86400 seconds a day. PostgreSQL does not cascade back up,
/// because the representation does not force it to; `justify_hours` is how a
/// caller asks for that.
fn scale_interval(a: Interval, scale: impl Fn(f64) -> f64) -> Result<Interval, TypeError> {
    let scaled_months = scale(f64::from(a.months));
    if !fits_in_i32(scaled_months) {
        return Err(interval_out_of_range());
    }
    let months = scaled_months as i32;

    let scaled_days = scale(f64::from(a.days));
    if !fits_in_i32(scaled_days) {
        return Err(interval_out_of_range());
    }
    let mut days = scaled_days as i32;

    // The whole-number parts are settled; what is left of the months and days
    // products has to go somewhere lower down.
    let month_remainder_days = ts_round((scaled_months - f64::from(months)) * DAYS_PER_MONTH_F64);
    let mut sec_remainder = ts_round(
        (scaled_days - f64::from(days) + month_remainder_days - month_remainder_days.trunc())
            * SECS_PER_DAY_F64,
    );

    // Rounding, or a cascade from the months, can leave a whole day or more in
    // the seconds remainder. Lift those days back out before they reach the
    // microsecond field.
    if sec_remainder.abs() >= SECS_PER_DAY_F64 {
        let whole = sec_remainder / SECS_PER_DAY_F64;
        if !fits_in_i32(whole) {
            return Err(interval_out_of_range());
        }
        let whole = whole as i32;
        days = days.checked_add(whole).ok_or_else(interval_out_of_range)?;
        sec_remainder -= f64::from(whole) * SECS_PER_DAY_F64;
    }

    if !fits_in_i32(month_remainder_days) {
        return Err(interval_out_of_range());
    }
    days = days
        .checked_add(month_remainder_days as i32)
        .ok_or_else(interval_out_of_range)?;

    let scaled_micros =
        (scale(a.micros as f64) + sec_remainder * USECS_PER_SEC_F64).round_ties_even();
    if !fits_in_i64(scaled_micros) {
        return Err(interval_out_of_range());
    }
    finite_interval(Interval {
        months,
        days,
        micros: scaled_micros as i64,
    })
}

/// Multiply an interval by a scalar factor (`interval_mul`).
///
/// An infinite operand on either side carries through with the product's sign.
/// The two combinations that would have to name a quantity the type cannot hold
/// — an infinite interval times zero, and a zero-length interval times infinity
/// — are `interval out of range`, because `interval` has no `NaN`.
pub fn mul_interval(a: Interval, factor: f64) -> Result<Interval, TypeError> {
    if factor.is_nan() {
        return Err(interval_out_of_range());
    }
    if a.is_infinite() {
        if factor == 0.0 {
            return Err(interval_out_of_range());
        }
        return Ok(Interval::infinity_of_sign(if factor < 0.0 {
            -a.infinite_sign()
        } else {
            a.infinite_sign()
        }));
    }
    if factor.is_infinite() {
        let sign = interval_sign(a);
        if sign == 0 {
            return Err(interval_out_of_range());
        }
        let negative = (factor < 0.0) != (sign < 0);
        return Ok(Interval::infinity_of_sign(if negative { -1 } else { 1 }));
    }
    scale_interval(a, |field| field * factor)
}

/// Divide an interval by a scalar divisor (`interval_div`; zero → 22012).
///
/// Dividing a FINITE interval by an infinity is not an error: every field goes
/// to zero, which the ordinary division already gives. Only `infinity/infinity`
/// has no answer.
pub fn div_interval(a: Interval, divisor: f64) -> Result<Interval, TypeError> {
    if divisor == 0.0 {
        return Err(TypeError::DivisionByZero);
    }
    if divisor.is_nan() {
        return Err(interval_out_of_range());
    }
    if a.is_infinite() {
        if divisor.is_infinite() {
            return Err(interval_out_of_range());
        }
        return Ok(Interval::infinity_of_sign(if divisor < 0.0 {
            -a.infinite_sign()
        } else {
            a.infinite_sign()
        }));
    }
    scale_interval(a, |field| field / divisor)
}

/// Add `days` to a `date`, returning the new `date` (overflow → 22008). Adding
/// to a non-finite date leaves it unchanged.
pub fn date_plus_days(d: PgDate, days: i64) -> Result<PgDate, TypeError> {
    let Some(civil) = d.finite() else {
        return Ok(d);
    };
    let overflow = |_| TypeError::DatetimeFieldOverflow {
        value: days.to_string(),
    };
    let span = Span::new().try_days(days).map_err(overflow)?;
    civil
        .checked_add(span)
        .map(PgDate::Finite)
        .map_err(overflow)
}

/// Subtract two dates, returning the number of days between them (a - b).
/// Subtracting infinite dates has no defined answer (22008).
pub fn date_diff_days(a: PgDate, b: PgDate) -> Result<i32, TypeError> {
    let out_of_range = || TypeError::DatetimeOutOfRange {
        message: "cannot subtract infinite dates".to_string(),
    };
    let (a, b) = (
        a.finite().ok_or_else(out_of_range)?,
        b.finite().ok_or_else(out_of_range)?,
    );
    Ok(a.since((jiff::Unit::Day, b))
        .map(|span| span.get_days())
        .expect("difference of in-range date values always fits in a Span"))
}

/// Promote a `date` to a civil `DateTime` at midnight (`date2timestamp`).
///
/// A non-finite date becomes the `timestamp` of the same sign. That is what
/// keeps `date 'infinity' = timestamp 'infinity'` true. The two types hold the
/// same value in different storage, and the cross-type comparisons meet here.
pub fn date_to_midnight(d: PgDate) -> DateTime {
    match d {
        PgDate::Finite(civil) => civil.to_datetime(Time::midnight()),
        PgDate::Infinity => TIMESTAMP_INFINITY,
        PgDate::NegInfinity => TIMESTAMP_NEG_INFINITY,
    }
}

/// Combine a `Date` and a `timetz` into the instant they name
/// (`datetimetz_timestamptz`).
///
/// The offset the `timetz` carries is the whole of the zone information here:
/// the reading was taken at that offset, so the instant is the date's midnight
/// plus the reading less the offset. The session zone is never consulted, which
/// is why `date + timetz` is the one `timestamptz`-producing operator that needs
/// no session at all.
pub fn date_plus_timetz(d: PgDate, t: TimeTz) -> Result<Timestamp, TypeError> {
    match d.sign() {
        0 => {}
        sign => return Ok(timestamptz_infinity_of_sign(sign)),
    }
    // A `date` spans more years than a `timestamp` does, and the offset can push
    // the instant out of range on its own, so both steps can fail.
    date_to_midnight(d)
        .to_zoned(TimeZone::UTC)
        .map_err(|_| date_out_of_range_for_timestamp())?
        .timestamp()
        .checked_add(t.utc_micros().microseconds())
        .map_err(|_| date_out_of_range_for_timestamp())
}

/// `date out of range for timestamp`: the 22008 the `date`-to-instant
/// conversions raise, worded as `datetimetz_timestamptz` words it.
fn date_out_of_range_for_timestamp() -> TypeError {
    TypeError::DatetimeOutOfRange {
        message: "date out of range for timestamp".to_string(),
    }
}

/// Subtract two `time` readings (`time_mi_time`).
///
/// The answer is a signed microsecond count with no months and no days, so
/// `time '00:30' - time '01:00'` is `-00:30:00`. It does not wrap the way
/// `time - interval` does, because the result is no longer a clock reading.
pub fn time_diff(a: PgTime, b: PgTime) -> Interval {
    Interval {
        months: 0,
        days: 0,
        // Both readings are in `0..=86_400_000_000`, so the difference cannot
        // overflow.
        micros: a.micros_of_day() - b.micros_of_day(),
    }
}

/// Read a `time` as an `interval` (`interval(time)`, `pg_cast`'s one implicit
/// route out of the date/time category).
///
/// The reading becomes that many microseconds. `time '24:00:00'` is a legal
/// reading and becomes the interval `24:00:00`, not one day — `interval` keeps
/// days and microseconds apart, and this conversion sets no days.
pub fn time_to_interval(t: PgTime) -> Interval {
    Interval {
        months: 0,
        days: 0,
        micros: t.micros_of_day(),
    }
}

/// Read an `interval` as a `time` (`interval_time`), the reverse conversion.
///
/// Only the microseconds field takes part: neither a month nor a day has a
/// fixed length in microseconds, so PostgreSQL ignores both rather than guess.
/// What is left is reduced into one day by taking the FLOOR, which is why
/// `interval '-2 hours'` reads `22:00:00` and not `-02:00:00`. An infinite
/// interval names no reading at all (22008).
pub fn interval_to_time(iv: Interval) -> Result<PgTime, TypeError> {
    if iv.is_infinite() {
        return Err(TypeError::DatetimeOutOfRange {
            message: "cannot convert infinite interval to time".to_string(),
        });
    }
    let micros = iv.micros.rem_euclid(USECS_PER_DAY_I64);
    PgTime::from_micros_of_day(micros).ok_or_else(|| TypeError::DatetimeFieldOverflow {
        value: "interval".to_string(),
    })
}

/// Add an `Interval` to a `Date` (PG: promotes date→midnight timestamp first)
/// and return a `DateTime`. This function applies months, then days, then micros
/// in order (calendar-aware, with a jiff `Span`).
pub fn date_plus_interval(d: PgDate, iv: Interval) -> Result<DateTime, TypeError> {
    // `date_to_midnight` already promotes a non-finite date to the `timestamp`
    // of the same sign, and `timestamp_plus_interval` carries that through.
    timestamp_plus_interval(date_to_midnight(d), iv)
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
    if let Some(result) =
        infinite_interval_difference(timestamp_infinite_sign(a), timestamp_infinite_sign(b))
    {
        return result;
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
/// The result is reduced into `[0, 24:00:00)`, so it never lands on the
/// `24:00:00` boundary: PostgreSQL's `time_pl_interval` subtracts a whole day
/// from a sum that reaches one, which makes `time '24:00:00' + interval '0'`
/// come out as `00:00:00`.
pub fn time_plus_interval(t: PgTime, iv: Interval) -> PgTime {
    // Add the interval micros and wrap into [0, 86_400_000_000) (the `.rem_euclid`
    // keeps a negative shift positive, so `time '00:30' - interval '1 hour'`
    // wraps to `23:30:00`).
    // `iv.micros` comes from a user-supplied interval, so the sum can leave
    // `i64` for an extreme one; wrapping into the day is the same answer
    // whichever multiple of a day the shift is, so reduce first.
    let micros = t
        .micros_of_day()
        .wrapping_add(iv.micros.rem_euclid(USECS_PER_DAY_I64))
        .rem_euclid(USECS_PER_DAY_I64);
    PgTime(micros)
}

/// Combine a `Date` and a `time` into a `DateTime` (PostgreSQL's `date + time`
/// and `time + date` → `timestamp`). A `24:00:00` reading lands at midnight on
/// the following day, so `date '2020-01-01' + time '24:00:00'` is
/// `2020-01-02 00:00:00`. `None` when that day is past the end of the calendar.
///
/// A non-finite date swallows the reading, exactly as `datetime_timestamp` does:
/// it promotes the date to a timestamp first and adds the time only to a finite
/// one, so `date 'infinity' + time '01:00'` is `infinity` and not a clock
/// reading on the last representable day.
#[must_use]
pub fn combine_date_time(d: PgDate, t: PgTime) -> Option<DateTime> {
    match d.finite() {
        Some(civil) => t.on_date(civil),
        None => Some(timestamp_infinity_of_sign(d.sign())),
    }
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
    if let Some(result) =
        infinite_interval_difference(timestamptz_infinite_sign(a), timestamptz_infinite_sign(b))
    {
        return result;
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

/// Apply PostgreSQL's fractional-seconds typmod. Values are stored at
/// microsecond resolution, so a precision below six rounds the microsecond
/// count half away from zero.
fn round_typmod_micros(micros: i64, precision: Option<u8>) -> Option<i64> {
    let precision = precision?;
    if precision >= 6 {
        return Some(micros);
    }
    let scale = 10_i64.checked_pow(6 - u32::from(precision))?;
    let half = scale / 2;
    if micros >= 0 {
        micros.checked_add(half).map(|value| value / scale * scale)
    } else {
        micros
            .checked_neg()?
            .checked_add(half)
            .map(|value| -(value / scale * scale))
    }
}

/// Round a `time(p)` value to its declared fractional-second precision.
pub fn apply_time_typmod(value: PgTime, precision: Option<u8>) -> Result<PgTime, TypeError> {
    let micros = round_typmod_micros(value.micros_of_day(), precision)
        .ok_or_else(|| TypeError::DatetimeFieldOverflow {
            value: time_to_text(value),
        })?;
    PgTime::from_micros_of_day(micros).ok_or_else(|| TypeError::DatetimeFieldOverflow {
        value: time_to_text(value),
    })
}

/// Round a `timetz(p)` value without changing its stored UTC offset.
pub fn apply_timetz_typmod(value: TimeTz, precision: Option<u8>) -> Result<TimeTz, TypeError> {
    Ok(TimeTz {
        time: apply_time_typmod(value.time, precision)?,
        offset: value.offset,
    })
}

/// Round a `timestamp(p)` value about PostgreSQL's 2000-01-01 epoch.
pub fn apply_timestamp_typmod(value: DateTime, precision: Option<u8>) -> Result<DateTime, TypeError> {
    if timestamp_is_infinite(value) {
        return Ok(value);
    }
    let micros = i64::from_be_bytes(timestamp_to_binary(value));
    let rounded = round_typmod_micros(micros, precision).ok_or_else(|| TypeError::DatetimeFieldOverflow {
        value: timestamp_to_text(value),
    })?;
    timestamp_from_binary(&rounded.to_be_bytes())
}

/// Round a `timestamptz(p)` value about PostgreSQL's 2000-01-01 epoch.
pub fn apply_timestamptz_typmod(value: Timestamp, precision: Option<u8>) -> Result<Timestamp, TypeError> {
    if timestamptz_is_infinite(value) {
        return Ok(value);
    }
    let micros = i64::from_be_bytes(timestamptz_to_binary(value));
    let rounded = round_typmod_micros(micros, precision).ok_or_else(|| TypeError::DatetimeFieldOverflow {
        value: timestamptz_to_text(value, &TimeZone::UTC),
    })?;
    timestamptz_from_binary(&rounded.to_be_bytes())
}

/// Round the microsecond field of `interval(p)`. Field masks are applied while
/// parsing interval literals; a plain type modifier always has the full range.
pub fn apply_interval_typmod(value: Interval, precision: Option<u8>) -> Result<Interval, TypeError> {
    if value.is_infinite() {
        return Ok(value);
    }
    let micros = round_typmod_micros(value.micros, precision).ok_or_else(interval_out_of_range)?;
    Ok(Interval { micros, ..value })
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
        // Same 22008 and same message as `FieldOverflow`; the month and the day
        // are the fields a mis-read `DateStyle` swaps, so PostgreSQL names the
        // setting that would read the literal the way it was meant.
        DecodeError::MdFieldOverflow => TypeError::CodedWithHint {
            sqlstate: "22008",
            message: format!("date/time field value out of range: \"{value}\""),
            hint: "Perhaps you need a different \"DateStyle\" setting.",
        },
        // Named by the type, not by a field, because no single field is at
        // fault. `timestamptz` borrows `timestamp`'s wording, as PostgreSQL
        // does — the offset plays no part in the day being unreachable.
        DecodeError::ValueOutOfRange => TypeError::DatetimeOutOfRange {
            message: format!(
                "{} out of range: \"{value}\"",
                if type_name == "date" {
                    "date"
                } else {
                    "timestamp"
                }
            ),
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

/// The UTC offset a zone gives a civil reading, resolved the way `PostgreSQL`'s
/// `DetermineTimeZoneOffset` resolves it.
///
/// A reading on a daylight-saving boundary has two candidate offsets, or none at
/// all. `PostgreSQL` forms both interpretations and takes whichever lands on the
/// *later* instant — its `beforetime > aftertime` test picks the offset in force
/// before the transition, and otherwise the one after. Both branches come to the
/// same thing: the *smaller* of the two offsets, since a smaller offset means a
/// later instant for the same wall clock. So a spring-forward gap reads at the
/// pre-transition offset and a fall-back fold reads at the post-transition one,
/// which is the rule `Europe/Moscow` in October 2014 was added to the suite to
/// pin down.
///
/// jiff's own strategies do not include this pairing — `Compatible` takes the
/// earlier instant in a fold — so the choice is made here rather than by picking
/// a [`jiff::tz::Disambiguation`].
#[must_use]
pub fn zone_offset_for(dt: DateTime, tz: &TimeZone) -> Offset {
    match tz.to_ambiguous_timestamp(dt).offset() {
        AmbiguousOffset::Unambiguous { offset } => offset,
        AmbiguousOffset::Gap { before, after } | AmbiguousOffset::Fold { before, after } => {
            before.min(after)
        }
    }
}

/// The instant a civil reading names in `tz`, under [`zone_offset_for`]'s rule.
///
/// # Errors
///
/// [`jiff::Error`] when the instant falls outside the representable range.
pub fn zoned_instant(dt: DateTime, tz: &TimeZone) -> Result<Timestamp, jiff::Error> {
    zone_offset_for(dt, tz).to_timestamp(dt)
}

/// Resolve a whole-value reserved date spelling.
///
/// The clock-relative spellings never reach here: they fill calendar fields
/// inside the decoder and arrive as ordinary [`Parts`].
fn special_to_date(special: Special) -> PgDate {
    match special {
        Special::Infinity => DATE_INFINITY,
        Special::NegInfinity => DATE_NEG_INFINITY,
        Special::Epoch => PgDate::Finite(Date::constant(1970, 1, 1)),
    }
}

/// Resolve a whole-value reserved timestamp spelling.
fn special_to_datetime(special: Special) -> DateTime {
    match special {
        Special::Infinity => TIMESTAMP_INFINITY,
        Special::NegInfinity => TIMESTAMP_NEG_INFINITY,
        Special::Epoch => DateTime::constant(1970, 1, 1, 0, 0, 0, 0),
    }
}

/// Parse a `date` literal in every spelling `PostgreSQL` accepts, reading an
/// ambiguous all-numeric date in `MDY` order (the default `DateStyle`).
pub fn parse_date(s: &str) -> Result<PgDate, TypeError> {
    parse_date_in(s, DateOrder::default(), &TimeZone::UTC)
}

/// [`parse_date`] with the session's `DateStyle` field order and zone.
pub fn parse_date_in(s: &str, order: DateOrder, tz: &TimeZone) -> Result<PgDate, TypeError> {
    match decode(s.trim(), order, DecodeMode::DateTime, tz)
        .map_err(|e| decode_error(e, "date", s))?
    {
        Decoded::Special(special) => Ok(special_to_date(special)),
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
            Ok(PgDate::Finite(date))
        }
    }
}

/// The earliest finite date PostgreSQL represents, 4714-11-24 BC, in the
/// astronomical year numbering both PostgreSQL and jiff use.
const MIN_FINITE_DATE: Date = Date::constant(-4713, 11, 24);

/// Reject a literal below PostgreSQL's own lower bound.
///
/// There is no upper check here. jiff's last civil date, 9999-12-31, is an
/// ordinary date that PostgreSQL accepts, and [`PgDate`] holds `infinity`
/// out of band, so nothing above this bound is reserved.
fn check_finite_date(d: Date, s: &str) -> Result<(), TypeError> {
    if d < MIN_FINITE_DATE {
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
pub fn date_to_text_in(d: PgDate, style: DateStyle, order: DateOrder) -> String {
    let Some(civil) = d.finite() else {
        // The two non-finite values spell the same in every style.
        return date_to_text(d);
    };
    if style == DateStyle::Iso {
        return date_to_text(d);
    }
    let (_, era) = era_year(civil.year());
    format!("{}{era}", styled_date(civil, style, order))
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
pub fn date_to_text(d: PgDate) -> String {
    let civil = match d {
        PgDate::Infinity => return "infinity".to_string(),
        PgDate::NegInfinity => return "-infinity".to_string(),
        PgDate::Finite(civil) => civil,
    };
    let (year, era) = era_year(civil.year());
    format!("{year:04}-{:02}-{:02}{era}", civil.month(), civil.day())
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
pub fn date_to_binary(d: PgDate) -> [u8; 4] {
    let civil = match d {
        PgDate::Infinity => return i32::MAX.to_be_bytes(),
        PgDate::NegInfinity => return i32::MIN.to_be_bytes(),
        PgDate::Finite(civil) => civil,
    };
    // `since` with largest unit Day yields a Span carrying only `days`.
    let days = civil
        .since((jiff::Unit::Day, pg_epoch_date()))
        .map(|span| span.get_days())
        .expect("difference from a valid date to the PG epoch always fits");
    days.to_be_bytes()
}

/// `date_recv`: i32 big-endian days since the PostgreSQL epoch.
pub fn date_from_binary(b: &[u8]) -> Result<PgDate, TypeError> {
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
    // Count the days off the epoch DATE, never off an instant. jiff's
    // `Timestamp` stops short of the calendar it can spell, because it holds
    // back enough of the last day for every zone offset. An instant therefore
    // cannot carry 9999-12-31, which is an ordinary date here.
    //
    // `date_plus_days` is also the non-panicking route. `ToSpan::days()` PANICS
    // outside jiff's Span range, and these bytes are arbitrary (storage, fuzz),
    // so a day count the calendar cannot reach must come back as a 22008.
    date_plus_days(PgDate::Finite(pg_epoch_date()), i64::from(raw))
}

// ---------------------------------------------------------------------------
// time without time zone
// ---------------------------------------------------------------------------

/// A PostgreSQL `time`: microseconds since midnight, `0..=86_400_000_000`.
///
/// This is not a `jiff::civil::Time`, and it cannot be. PostgreSQL's `time`
/// reaches one microsecond past the last civil reading of the day — `24:00:00`
/// is a legal value, distinct from `00:00:00`, that `'23:59:60'` and
/// `'23:59:59.9999999'` round up to. jiff's civil clock stops at
/// `23:59:59.999999999`, so the boundary has no civil spelling and the type has
/// to be an offset from midnight instead, which is exactly how PostgreSQL's own
/// `TimeADT` stores it.
///
/// The ordering is the ordering of the microsecond count, so `24:00:00` sorts
/// above every other reading and does not collide with midnight, and `Hash`
/// agrees with `Eq` because both come from that one integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgTime(i64);

impl PgTime {
    /// `00:00:00`, the low end of the range.
    pub const MIDNIGHT: Self = Self(0);
    /// `24:00:00`, the high end. One microsecond more is out of range.
    pub const END_OF_DAY: Self = Self(MICROS_PER_DAY);

    /// A `time` from microseconds since midnight, or `None` outside
    /// `0..=86_400_000_000`.
    #[must_use]
    pub fn from_micros_of_day(micros: i64) -> Option<Self> {
        (0..=MICROS_PER_DAY)
            .contains(&micros)
            .then_some(Self(micros))
    }

    /// Microseconds since midnight, `0..=86_400_000_000`.
    #[must_use]
    pub const fn micros_of_day(self) -> i64 {
        self.0
    }

    /// The hour field, `0..=24`. It is `24` only for [`PgTime::END_OF_DAY`].
    #[must_use]
    pub const fn hour(self) -> i8 {
        (self.0 / 3_600_000_000) as i8
    }

    /// The minute field, `0..=59`.
    #[must_use]
    pub const fn minute(self) -> i8 {
        (self.0 % 3_600_000_000 / 60_000_000) as i8
    }

    /// The whole-second field, `0..=59`.
    #[must_use]
    pub const fn second(self) -> i8 {
        (self.0 % 60_000_000 / 1_000_000) as i8
    }

    /// The sub-second field in nanoseconds. Always a whole number of
    /// microseconds, because that is the resolution PostgreSQL stores.
    #[must_use]
    pub const fn subsec_nanosecond(self) -> i32 {
        (self.0 % 1_000_000 * 1_000) as i32
    }

    /// The equivalent civil clock reading, or `None` for `24:00:00`, which has
    /// none.
    #[must_use]
    pub fn to_civil(self) -> Option<Time> {
        (self != Self::END_OF_DAY).then(|| {
            Time::new(
                self.hour(),
                self.minute(),
                self.second(),
                self.subsec_nanosecond(),
            )
            .expect("microseconds below a day are a valid clock reading")
        })
    }

    /// The civil date-and-time this reading names on `date`, carrying
    /// `24:00:00` into the following day the way PostgreSQL's `date + time`
    /// does. `None` when that day is past the end of the calendar.
    fn on_date(self, date: Date) -> Option<DateTime> {
        match self.to_civil() {
            Some(t) => Some(date.to_datetime(t)),
            None => date
                .tomorrow()
                .ok()
                .map(|next| date_to_midnight(next.into())),
        }
    }
}

impl From<Time> for PgTime {
    fn from(t: Time) -> Self {
        Self(
            i64::from(t.hour()) * 3_600_000_000
                + i64::from(t.minute()) * 60_000_000
                + i64::from(t.second()) * 1_000_000
                + i64::from(t.subsec_nanosecond() / 1_000),
        )
    }
}

/// Parse a `time` literal. `PostgreSQL` accepts a leading date and a trailing
/// zone here and discards both, so `'2003-03-07 15:36:39 America/New_York'` is a
/// legal `time`. A zone *name* still has to be resolvable, which is why the
/// same text without its date is a syntax error.
pub fn parse_time(s: &str) -> Result<PgTime, TypeError> {
    parse_time_in(s, DateOrder::default(), &TimeZone::UTC)
}

/// [`parse_time`] with the session's `DateStyle` field order and zone.
pub fn parse_time_in(s: &str, order: DateOrder, tz: &TimeZone) -> Result<PgTime, TypeError> {
    // `time_in` names the type `time` in its errors, not the canonical
    // `time without time zone` that `pg_typeof` and `format_type` report.
    let type_name = "time";
    let micros = match decode(s.trim(), order, DecodeMode::TimeOnly, tz)
        .map_err(|e| decode_error(e, type_name, s))?
    {
        // `DecodeTimeOnly` takes `now` (which fills the clock and arrives as
        // parts) and nothing else reserved, so every whole-value spelling that
        // reaches here is malformed for this type.
        Decoded::Special(_) => {
            return Err(TypeError::InvalidDatetimeFormat {
                type_name,
                value: s.to_string(),
            });
        }
        Decoded::Parts(parts) => parts.micros_of_day,
    };
    // `24:00:00` is the top of the range, not past it, and `'23:59:60'` and
    // `'23:59:59.9999999'` round up onto it. One microsecond further is out of
    // range.
    PgTime::from_micros_of_day(micros).ok_or_else(|| TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    })
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
    pub time: PgTime,
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
    let parts = match decode(s.trim(), order, DecodeMode::TimeOnly, tz)
        .map_err(|e| decode_error(e, type_name, s))?
    {
        Decoded::Special(_) => return Err(syntax()),
        Decoded::Parts(parts) => parts,
    };
    let time = PgTime::from_micros_of_day(parts.micros_of_day).ok_or_else(|| {
        TypeError::DatetimeFieldOverflow {
            value: s.to_string(),
        }
    })?;
    // A moving zone is resolved against the instant the reading names, which for
    // `24:00:00` is midnight on the following day.
    let instant_on = |date: Date| {
        time.on_date(date)
            .unwrap_or_else(|| date_to_midnight(date.into()))
    };
    let offset = match parts.zone {
        Some(Zone::Offset(offset)) => offset,
        // A zone whose offset moves needs a date to be resolved against; the
        // decoder has already refused one that has neither a date nor a single
        // offset for all time, so a dateless zone here resolves without one.
        Some(Zone::Named(zone)) => match parts.date {
            Some(date) => zone_offset_for(instant_on(date), &zone),
            None => zone.to_fixed_offset().map_err(|_| syntax())?,
        },
        None => {
            let date = parts
                .date
                .unwrap_or_else(|| tz.to_datetime(clock_now()).date());
            zone_offset_for(instant_on(date), tz)
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
    let time =
        PgTime::from_micros_of_day(micros).ok_or_else(|| TypeError::DatetimeFieldOverflow {
            value: micros.to_string(),
        })?;
    let offset = Offset::from_seconds(-west).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: west.to_string(),
    })?;
    Ok(TimeTz { time, offset })
}

/// Rebuild a `time` from microseconds since midnight, the inverse of
/// [`time_to_micros_of_day`].
///
/// # Panics
///
/// Panics unless `micros` is in `0..=86_400_000_000`.
#[must_use]
pub fn time_from_micros_of_day_public(micros: i64) -> PgTime {
    PgTime::from_micros_of_day(micros).expect("microseconds within a day")
}

/// Microseconds since midnight for a `time`.
#[must_use]
pub fn time_to_micros_of_day(t: PgTime) -> i64 {
    t.micros_of_day()
}

/// Microseconds in one calendar day, the modulus of a clock reading.
const MICROS_PER_DAY: i64 = 86_400_000_000;

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

/// Render a `time` as `HH:MM:SS[.ffffff]` (PostgreSQL `time_out`). The hour is
/// `24` for [`PgTime::END_OF_DAY`].
pub fn time_to_text(t: PgTime) -> String {
    let mut out = format!("{:02}:{:02}:{:02}", t.hour(), t.minute(), t.second());
    push_subsecond(&mut out, t.subsec_nanosecond());
    out
}

/// `time_send`: i64 big-endian microseconds since midnight.
pub fn time_to_binary(t: PgTime) -> [u8; 8] {
    t.micros_of_day().to_be_bytes()
}

/// `time_recv`: i64 big-endian microseconds since midnight.
pub fn time_from_binary(b: &[u8]) -> Result<PgTime, TypeError> {
    let arr: [u8; 8] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "time without time zone",
        value: format!("{b:?}"),
    })?;
    let micros = i64::from_be_bytes(arr);
    PgTime::from_micros_of_day(micros).ok_or_else(|| TypeError::DatetimeFieldOverflow {
        value: micros.to_string(),
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
    // `timestamp_in` names the type `timestamp`, as `time_in` names `time`.
    let type_name = "timestamp";
    match decode(s.trim(), order, DecodeMode::DateTime, tz)
        .map_err(|e| decode_error(e, type_name, s))?
    {
        Decoded::Special(special) => Ok(special_to_datetime(special)),
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
    let (date, micros_of_day) = if micros_of_day >= MICROS_PER_DAY {
        (
            date.tomorrow().map_err(|_| overflow())?,
            micros_of_day - MICROS_PER_DAY,
        )
    } else {
        (date, micros_of_day)
    };
    PgTime::from_micros_of_day(micros_of_day)
        .and_then(|t| t.on_date(date))
        .ok_or_else(overflow)
}

/// Reject a literal outside the finite range, the timestamp counterpart of
/// [`check_finite_date`].
fn check_finite_timestamp(ts: DateTime, s: &str) -> Result<(), TypeError> {
    if !timestamp_is_in_range(ts) {
        return Err(TypeError::DatetimeOutOfRange {
            message: format!("timestamp out of range: \"{s}\""),
        });
    }
    Ok(())
}

/// Whether a timestamp is a finite value in PostgreSQL's supported range.
#[must_use]
pub fn timestamp_is_in_range(ts: DateTime) -> bool {
    !timestamp_is_infinite(ts) && ts.date() >= MIN_FINITE_DATE
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
    match decode(s.trim(), order, DecodeMode::DateTime, tz)
        .map_err(|e| decode_error(e, type_name, s))?
    {
        Decoded::Special(special) => match special {
            Special::Infinity => Ok(timestamptz_infinity()),
            Special::NegInfinity => Ok(timestamptz_neg_infinity()),
            // Unlike a bare civil timestamp, the `epoch` timestamptz spelling
            // names the fixed Unix instant, independent of the session zone.
            Special::Epoch => Ok(Timestamp::UNIX_EPOCH),
        },
        Decoded::Parts(parts) => {
            let date = parts.date.ok_or_else(|| TypeError::InvalidDatetimeFormat {
                type_name,
                value: s.to_string(),
            })?;
            let dt = combine_parts(date, parts.micros_of_day, s)?;
            let instant = match parts.zone {
                Some(Zone::Offset(off)) => off.to_timestamp(dt).map_err(|_| overflow())?,
                Some(Zone::Named(zone)) => zoned_instant(dt, &zone).map_err(|_| overflow())?,
                None => zoned_instant(dt, tz).map_err(|_| overflow())?,
            };
            // The range check belongs on the INSTANT, not on the local reading:
            // `4714-11-23 16:00:00-08 BC` is a legal `timestamptz` precisely
            // because its UTC equivalent is the first representable moment, even
            // though the wall clock it was written at falls a day earlier.
            if timestamptz_is_infinite(instant)
                || TimeZone::UTC.to_datetime(instant).date() < MIN_FINITE_DATE
            {
                return Err(TypeError::DatetimeOutOfRange {
                    message: format!("timestamp out of range: \"{s}\""),
                });
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
    ///
    /// The lookup truncates at ten characters first, exactly as `DecodeUnits`
    /// does against its token table. That truncation is not a size limit but
    /// part of the accepted grammar: it is the only reason `millenniums` and
    /// `microseconds` are units at all, since neither spelling is in the table.
    #[must_use]
    pub fn parse(word: &str) -> Option<Self> {
        let word = word.trim().to_ascii_lowercase();
        let word = word.get(..10).unwrap_or(&word);
        Some(match word {
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
    // The two non-finite intervals, spelled exactly as PostgreSQL accepts them.
    let lower = t.to_ascii_lowercase();
    match lower.as_str() {
        "infinity" | "+infinity" => return Ok(Interval::INFINITY),
        "-infinity" => return Ok(Interval::NEG_INFINITY),
        _ => {}
    }
    // `interval_in` runs the decoder, then folds the accumulator into the stored
    // three-field value. The two stages fail differently: a field that leaves
    // its accumulator is `interval field value out of range` (22015 — SQL99
    // gives interval its own code, so `interval_in` promotes the shared
    // decoder's 22008 field overflow to it), while a decode that only fails when
    // years and months are combined is `interval out of range` (22008).
    let itm = decode_interval(t, range).map_err(|e| match e {
        IntervalError::Format => TypeError::InvalidDatetimeFormat {
            type_name: "interval",
            value: s.to_string(),
        },
        IntervalError::FieldOverflow => TypeError::IntervalFieldOverflow {
            value: s.to_string(),
        },
    })?;
    let value = itm
        .into_interval()
        .ok_or_else(|| TypeError::DatetimeOutOfRange {
            message: "interval out of range".to_string(),
        })?;
    Ok(truncate_to_range(value, range))
}

/// Which of the two failures `PostgreSQL`'s interval input path keeps apart.
/// Collapsing them would be invisible in the value but wrong on the wire: they
/// carry different SQLSTATEs and different messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntervalError {
    /// `DTERR_BAD_FORMAT` — the text is not an interval literal.
    Format,
    /// `DTERR_FIELD_OVERFLOW` — a field, or the accumulator it feeds, is out of
    /// range.
    FieldOverflow,
}

/// `PostgreSQL`'s `struct pg_itm_in`: the accumulator `DecodeInterval` fills
/// before the parts become an `Interval`.
///
/// The field WIDTHS are load-bearing rather than an implementation detail. They
/// place the boundary between the two failures above: `'2147483647 years'`
/// accumulates fine (the years field is `i32`) and fails only when the years and
/// months are folded into one month count, so it is `interval out of range`,
/// while `'2147483648 years'` overflows the field itself and is `interval field
/// value out of range`. Accumulating into wider types would move that boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ItmIn {
    usec: i64,
    mday: i32,
    mon: i32,
    year: i32,
}

impl ItmIn {
    /// `AdjustFractMicroseconds`: scale a fraction to microseconds and add it.
    /// The half-microsecond tail rounds away from zero only past the halfway
    /// point, which is what `PostgreSQL` does and what `f64::round` does not.
    fn add_fract_micros(&mut self, frac: f64, scale: i64) -> Result<(), IntervalError> {
        if frac == 0.0 {
            return Ok(());
        }
        let scaled = frac * scale as f64;
        let mut usec = scaled as i64;
        let tail = scaled - usec as f64;
        if tail > 0.5 {
            usec += 1;
        } else if tail < -0.5 {
            usec -= 1;
        }
        self.usec = self
            .usec
            .checked_add(usec)
            .ok_or(IntervalError::FieldOverflow)?;
        Ok(())
    }

    /// `AdjustFractDays`: the whole part of the scaled fraction is days, the
    /// leftover part of a day is microseconds.
    fn add_fract_days(&mut self, frac: f64, scale: i32) -> Result<(), IntervalError> {
        if frac == 0.0 {
            return Ok(());
        }
        let scaled = frac * f64::from(scale);
        let extra_days = scaled as i32;
        self.mday = self
            .mday
            .checked_add(extra_days)
            .ok_or(IntervalError::FieldOverflow)?;
        self.add_fract_micros(scaled - f64::from(extra_days), USECS_PER_DAY_I64)
    }

    /// `AdjustFractYears`: a fraction of a year reaches months and stops — there
    /// is no spill from a fractional year into days.
    fn add_fract_years(&mut self, frac: f64, scale: i32) -> Result<(), IntervalError> {
        let extra_months = (frac * f64::from(scale) * 12.0).round_ties_even() as i32;
        self.mon = self
            .mon
            .checked_add(extra_months)
            .ok_or(IntervalError::FieldOverflow)?;
        Ok(())
    }

    /// `AdjustMicroseconds`: `(val + fval) * scale` microseconds.
    fn add_micros(&mut self, val: i64, fval: f64, scale: i64) -> Result<(), IntervalError> {
        self.usec = val
            .checked_mul(scale)
            .and_then(|product| self.usec.checked_add(product))
            .ok_or(IntervalError::FieldOverflow)?;
        self.add_fract_micros(fval, scale)
    }

    /// `AdjustDays`: `val * scale` days.
    fn add_days(&mut self, val: i64, scale: i32) -> Result<(), IntervalError> {
        let val = i32::try_from(val).map_err(|_| IntervalError::FieldOverflow)?;
        self.mday = val
            .checked_mul(scale)
            .and_then(|days| self.mday.checked_add(days))
            .ok_or(IntervalError::FieldOverflow)?;
        Ok(())
    }

    /// `AdjustMonths`: `val` is already a month count.
    fn add_months(&mut self, val: i64) -> Result<(), IntervalError> {
        let val = i32::try_from(val).map_err(|_| IntervalError::FieldOverflow)?;
        self.mon = self
            .mon
            .checked_add(val)
            .ok_or(IntervalError::FieldOverflow)?;
        Ok(())
    }

    /// `AdjustYears`: `val * scale` years.
    fn add_years(&mut self, val: i64, scale: i32) -> Result<(), IntervalError> {
        let val = i32::try_from(val).map_err(|_| IntervalError::FieldOverflow)?;
        self.year = val
            .checked_mul(scale)
            .and_then(|years| self.year.checked_add(years))
            .ok_or(IntervalError::FieldOverflow)?;
        Ok(())
    }

    /// The `ago` suffix negates every field. A field already at its minimum has
    /// no negation, so `'-2147483648 months ago'` is a field overflow.
    fn negate(&mut self) -> Result<(), IntervalError> {
        self.usec = self
            .usec
            .checked_neg()
            .ok_or(IntervalError::FieldOverflow)?;
        self.mday = self
            .mday
            .checked_neg()
            .ok_or(IntervalError::FieldOverflow)?;
        self.mon = self.mon.checked_neg().ok_or(IntervalError::FieldOverflow)?;
        self.year = self
            .year
            .checked_neg()
            .ok_or(IntervalError::FieldOverflow)?;
        Ok(())
    }

    /// `itmin2interval`: fold years into months and build the stored value.
    /// `None` when the combined month count leaves `i32` — the one failure that
    /// is `interval out of range` rather than a field overflow.
    fn into_interval(self) -> Option<Interval> {
        let total_months = i64::from(self.year)
            .checked_mul(12)?
            .checked_add(i64::from(self.mon))?;
        Some(Interval {
            months: i32::try_from(total_months).ok()?,
            days: self.mday,
            micros: self.usec,
        })
    }
}

/// Split an interval literal into `ParseDateTime`'s fields.
///
/// Whitespace is not what separates them — punctuation does most of the work,
/// and a letter run ends a number without any gap. That is why `1mon` is one
/// month, `1 month - 1 second` subtracts rather than failing on a lone `-`, and
/// `2562047788.1:0:54.775807` is a number followed by a clock reading rather
/// than one unreadable clock reading. A whitespace split gets all three wrong.
fn split_interval_fields(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut fields = Vec::new();
    let mut i = 0;
    let soak = |chars: &[char], i: &mut usize, accept: &dyn Fn(char) -> bool, out: &mut String| {
        while let Some(&c) = chars.get(*i)
            && accept(c)
        {
            out.push(c);
            *i += 1;
        }
    };
    while let Some(&c) = chars.get(i) {
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        let mut field = String::new();
        if c.is_ascii_digit() || c == '.' {
            field.push(c);
            i += 1;
            soak(&chars, &mut i, &|c| c.is_ascii_digit(), &mut field);
            match chars.get(i) {
                // A clock reading runs to the end of its digits and separators.
                Some(':') => soak(
                    &chars,
                    &mut i,
                    &|c| c.is_ascii_digit() || c == ':' || c == '.',
                    &mut field,
                ),
                // A delimiter joins a second group only when a third group
                // repeats the SAME delimiter, which is what keeps `1.5` a number
                // and makes `1-2-3` one field.
                Some(&delim @ ('-' | '/' | '.'))
                    if chars.get(i + 1).is_some_and(char::is_ascii_digit) =>
                {
                    field.push(delim);
                    i += 1;
                    soak(&chars, &mut i, &|c| c.is_ascii_digit(), &mut field);
                    if chars.get(i) == Some(&delim) {
                        soak(
                            &chars,
                            &mut i,
                            &|c| c.is_ascii_digit() || c == delim,
                            &mut field,
                        );
                    }
                }
                _ => {}
            }
        } else if c.is_ascii_alphabetic() {
            while let Some(&c) = chars.get(i)
                && c.is_ascii_alphabetic()
            {
                field.push(c.to_ascii_lowercase());
                i += 1;
            }
        } else if c == '+' || c == '-' {
            field.push(c);
            i += 1;
            while chars.get(i).is_some_and(|c| c.is_whitespace()) {
                i += 1;
            }
            match chars.get(i) {
                Some(c) if c.is_ascii_digit() => soak(
                    &chars,
                    &mut i,
                    &|c| c.is_ascii_digit() || matches!(c, ':' | '.' | '-'),
                    &mut field,
                ),
                Some(c) if c.is_ascii_alphabetic() => {
                    while let Some(&c) = chars.get(i)
                        && c.is_ascii_alphabetic()
                    {
                        field.push(c.to_ascii_lowercase());
                        i += 1;
                    }
                }
                // A sign with nothing behind it is its own (unreadable) field.
                _ => {}
            }
        } else {
            // Any other punctuation — `@`, a stray `,` — separates and is dropped.
            i += 1;
            continue;
        }
        fields.push(field);
    }
    fields
}

/// `DecodeInterval` plus the ISO-8601 fallback: read a literal into the
/// `pg_itm_in` accumulator without deciding what its failure means.
fn decode_interval(
    t: &str,
    range: Option<(IntervalField, IntervalField)>,
) -> Result<ItmIn, IntervalError> {
    if t.is_empty() {
        return Err(IntervalError::Format);
    }
    if t.starts_with(['p', 'P']) && !t[1..].starts_with(' ') {
        return parse_iso8601_interval(t);
    }

    let mut itm = ItmIn::default();

    // Terms are read right to left so an unqualified quantity can take its unit
    // from the field range and pass it on to its neighbour.
    let mut tokens = split_interval_fields(t);
    // `ago` negates the whole interval, and only at the very end.
    let negate = tokens.last().is_some_and(|last| last == "ago");
    if negate {
        tokens.pop();
    }
    let tokens: Vec<&str> = tokens.iter().map(String::as_str).collect();
    // With no qualifier the rightmost bare quantity is seconds, PostgreSQL's
    // `INTERVAL_FULL_RANGE` default.
    let mut implied = range.map_or(IntervalField::Second, |(_, end)| end);
    // Which fields a term has already supplied. Supplying one twice is
    // PostgreSQL's `DTERR_BAD_FORMAT`, not a second addition.
    let mut supplied: u32 = 0;
    let claim = |bits: u32, supplied: &mut u32| {
        if bits & *supplied != 0 {
            return Err(IntervalError::Format);
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
            let clock = parse_clock_term(tok, range).map_err(|e| clock_term_failure(tok, e))?;
            itm.usec = itm
                .usec
                .checked_add(clock)
                .ok_or(IntervalError::FieldOverflow)?;
            implied = IntervalField::Day;
            i -= 1;
            continue;
        }
        // A `Y-M` term is the year-month shorthand, which PostgreSQL reads as a
        // month count and leaves months as the unit for the quantity to its left.
        if let Some(shorthand) = parse_year_month_term(tok) {
            claim(IntervalField::Month.mask_bit(), &mut supplied)?;
            itm.add_months(shorthand?)?;
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
            Some(_) | None if Quantity::parse(tok).is_err() => return Err(IntervalError::Format),
            _ => implied,
        };
        let qty = Quantity::parse(tokens.get(i - 1).ok_or(IntervalError::Format)?)?;
        // A fraction of a second reaches the millisecond and microsecond fields,
        // so it supplies all three; a fraction of any coarser unit does not.
        let bits = if unit == IntervalField::Second && qty.frac != 0.0 {
            SUBSECOND_FIELDS
        } else {
            unit.mask_bit()
        };
        claim(bits, &mut supplied)?;
        accumulate_unit(qty, unit, &mut itm)?;
        implied = unit.next_bare_unit();
        i -= 1;
    }

    if negate {
        itm.negate()?;
    }
    Ok(itm)
}

/// Drop everything finer than the range's end field, the way a qualified
/// `INTERVAL '…' <field>` literal truncates. `SECOND` keeps its fraction.
fn truncate_to_range(iv: Interval, range: Option<(IntervalField, IntervalField)>) -> Interval {
    let Some((_, end)) = range else {
        return iv;
    };
    // The non-finite values have no fields to truncate.
    if iv.is_infinite() {
        return iv;
    }
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

/// Parse the `Y-M` year-month shorthand into a signed month count. The outer
/// `Option` says whether the token is that shape at all; the inner `Result`
/// reports a year or month field that will not fit, which `PostgreSQL` range-checks
/// here rather than at the accumulator (`0-12` is already out of range, and a
/// year whose month product leaves `i64` is a field overflow).
fn parse_year_month_term(tok: &str) -> Option<Result<i64, IntervalError>> {
    let (negative, rest) = match tok.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, tok.strip_prefix('+').unwrap_or(tok)),
    };
    let (years, months) = rest.split_once('-')?;
    if years.is_empty() || !years.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if months.is_empty() || !months.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(year_month_months(negative, years, months))
}

/// The arithmetic half of [`parse_year_month_term`], split out so the shape test
/// and the range test give different answers.
fn year_month_months(negative: bool, years: &str, months: &str) -> Result<i64, IntervalError> {
    let overflow = |_| IntervalError::FieldOverflow;
    let years: i64 = if negative {
        format!("-{years}").parse().map_err(overflow)?
    } else {
        years.parse().map_err(overflow)?
    };
    let months: i32 = months.parse().map_err(overflow)?;
    if !(0..12).contains(&months) {
        return Err(IntervalError::FieldOverflow);
    }
    let months = if negative { -months } else { months };
    years
        .checked_mul(12)
        .and_then(|y| y.checked_add(i64::from(months)))
        .ok_or(IntervalError::FieldOverflow)
}

/// Read the leading C `strtod` number, returning its value and the unconsumed
/// tail. `PostgreSQL`'s ISO-8601 interval reader delegates to `strtod`, so the
/// grammar here is `strtod`'s — exponent notation and the spelled-out
/// infinities included — not the stricter one [`Quantity::parse`] uses.
fn strtod_prefix(s: &str) -> Option<(f64, &str)> {
    let bytes = s.as_bytes();
    let mut i = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let negative = bytes.first() == Some(&b'-');
    for (word, value) in [
        ("infinity", f64::INFINITY),
        ("inf", f64::INFINITY),
        ("nan", f64::NAN),
    ] {
        if s.len() >= i + word.len() && s[i..i + word.len()].eq_ignore_ascii_case(word) {
            let value = if negative { -value } else { value };
            return Some((value, &s[i + word.len()..]));
        }
    }
    let mut digits = 0usize;
    while matches!(bytes.get(i), Some(b) if b.is_ascii_digit()) {
        i += 1;
        digits += 1;
    }
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        while matches!(bytes.get(i), Some(b) if b.is_ascii_digit()) {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    // An `e` only joins the number when real exponent digits follow it, so
    // `P1E` keeps the `E` as a (rejected) unit designator rather than eating it.
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        if matches!(bytes.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        let exponent_start = j;
        while matches!(bytes.get(j), Some(b) if b.is_ascii_digit()) {
            j += 1;
        }
        if j > exponent_start {
            i = j;
        }
    }
    s[..i].parse().ok().map(|value| (value, &s[i..]))
}

/// `ParseISO8601Number`: the number plus its truncated integer and fractional
/// halves. The 1e15 cap is PostgreSQL's, and it is what makes the integer part
/// exact and the fraction's magnitude strictly below one.
fn parse_iso8601_number(s: &str) -> Result<(i64, f64, &str), IntervalError> {
    if !matches!(s.as_bytes().first(), Some(b'0'..=b'9' | b'-' | b'.')) {
        return Err(IntervalError::Format);
    }
    let (value, rest) = strtod_prefix(s).ok_or(IntervalError::Format)?;
    if value.is_nan() || !(-1.0e15..=1.0e15).contains(&value) {
        return Err(IntervalError::FieldOverflow);
    }
    let integral = value.trunc();
    Ok((integral as i64, value - integral, rest))
}

/// `ISO8601IntegerWidth`: how many integral digits a field was written with,
/// which is what tells `P00021015` (the eight-digit basic date) from `P2015`.
fn iso8601_integer_width(field: &str) -> usize {
    let field = field.strip_prefix('-').unwrap_or(field);
    field.bytes().take_while(u8::is_ascii_digit).count()
}

/// Parse an ISO-8601 duration into the accumulator: the designator form
/// (`P1Y2M3DT4H5M6S`), the basic alternative form (`P00021015T103020`) and the
/// extended alternative form (`P0002-10-15T10:30:20`).
///
/// This is `DecodeISO8601Interval` transcribed. Its shape looks redundant — the
/// alternative formats are reached by *falling through* the designator switch —
/// but that fallthrough is what lets `P0002` mean two years while `P0002-10`
/// means two years and ten months, so it is kept.
fn parse_iso8601_interval(text: &str) -> Result<ItmIn, IntervalError> {
    let mut itm = ItmIn::default();
    let mut datepart = true;
    let mut havefield = false;
    if text.len() < 2 || !text.starts_with('P') {
        return Err(IntervalError::Format);
    }
    let mut s = &text[1..];
    while !s.is_empty() {
        // `T` opens the time half and supplies no field of its own.
        if let Some(rest) = s.strip_prefix('T') {
            datepart = false;
            havefield = false;
            s = rest;
            continue;
        }
        let fieldstart = s;
        let (val, fval, rest) = parse_iso8601_number(s)?;
        // The designator is the character right after the number; at the end of
        // the string it is the terminator, which several branches accept.
        let unit = rest.chars().next().unwrap_or('\0');
        s = rest.get(unit.len_utf8()..).unwrap_or("");
        if datepart {
            match unit {
                'Y' => {
                    itm.add_years(val, 1)?;
                    itm.add_fract_years(fval, 1)?;
                }
                'M' => {
                    itm.add_months(val)?;
                    itm.add_fract_days(fval, 30)?;
                }
                'W' => {
                    itm.add_days(val, 7)?;
                    itm.add_fract_days(fval, 7)?;
                }
                'D' => {
                    itm.add_days(val, 1)?;
                    itm.add_fract_micros(fval, USECS_PER_DAY_I64)?;
                }
                'T' | '\0' | '-' => {
                    // `PyyyymmddThhmmss` — eight digits with nothing before them.
                    if unit != '-' && iso8601_integer_width(fieldstart) == 8 && !havefield {
                        itm.add_years(val / 10_000, 1)?;
                        itm.add_months((val / 100) % 100)?;
                        itm.add_days(val % 100, 1)?;
                        itm.add_fract_micros(fval, USECS_PER_DAY_I64)?;
                        if unit == '\0' {
                            return Ok(itm);
                        }
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    if havefield {
                        return Err(IntervalError::Format);
                    }
                    itm.add_years(val, 1)?;
                    itm.add_fract_years(fval, 1)?;
                    if unit == '\0' {
                        return Ok(itm);
                    }
                    if unit == 'T' {
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    let (val, fval, rest) = parse_iso8601_number(s)?;
                    itm.add_months(val)?;
                    itm.add_fract_days(fval, 30)?;
                    s = rest;
                    if s.is_empty() {
                        return Ok(itm);
                    }
                    // A `T` here is left for the loop head to consume.
                    if s.starts_with('T') {
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    let Some(rest) = s.strip_prefix('-') else {
                        return Err(IntervalError::Format);
                    };
                    let (val, fval, rest) = parse_iso8601_number(rest)?;
                    itm.add_days(val, 1)?;
                    itm.add_fract_micros(fval, USECS_PER_DAY_I64)?;
                    s = rest;
                    if s.is_empty() {
                        return Ok(itm);
                    }
                    if s.starts_with('T') {
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    return Err(IntervalError::Format);
                }
                _ => return Err(IntervalError::Format),
            }
        } else {
            match unit {
                'H' => itm.add_micros(val, fval, 3_600_000_000)?,
                'M' => itm.add_micros(val, fval, 60_000_000)?,
                'S' => itm.add_micros(val, fval, 1_000_000)?,
                '\0' | ':' => {
                    // `PThhmmss` — six digits with nothing before them.
                    if unit == '\0' && iso8601_integer_width(fieldstart) == 6 && !havefield {
                        itm.add_micros(val / 10_000, 0.0, 3_600_000_000)?;
                        itm.add_micros((val / 100) % 100, 0.0, 60_000_000)?;
                        itm.add_micros(val % 100, 0.0, 1_000_000)?;
                        itm.add_fract_micros(fval, 1)?;
                        return Ok(itm);
                    }
                    if havefield {
                        return Err(IntervalError::Format);
                    }
                    itm.add_micros(val, fval, 3_600_000_000)?;
                    if unit == '\0' {
                        return Ok(itm);
                    }
                    let (val, fval, rest) = parse_iso8601_number(s)?;
                    itm.add_micros(val, fval, 60_000_000)?;
                    s = rest;
                    if s.is_empty() {
                        return Ok(itm);
                    }
                    let Some(rest) = s.strip_prefix(':') else {
                        return Err(IntervalError::Format);
                    };
                    let (val, fval, rest) = parse_iso8601_number(rest)?;
                    itm.add_micros(val, fval, 1_000_000)?;
                    s = rest;
                    if s.is_empty() {
                        return Ok(itm);
                    }
                    return Err(IntervalError::Format);
                }
                _ => return Err(IntervalError::Format),
            }
        }
        havefield = true;
    }
    Ok(itm)
}

/// Read the leading run of digits as an `i64`, with the unconsumed tail —
/// C `strtoi64` over the clock fields. A run too long for the type is a field
/// overflow; no run at all leaves the text untouched for the caller's delimiter
/// check to reject.
fn take_int64(s: &str) -> Result<(i64, &str), IntervalError> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    let width = digits.bytes().take_while(u8::is_ascii_digit).count();
    if width == 0 {
        return Ok((0, s));
    }
    let end = s.len() - digits.len() + width;
    let value = s[..end].parse().map_err(|_| IntervalError::FieldOverflow)?;
    Ok((value, &s[end..]))
}

/// [`take_int64`] into an `i32` — the width `PostgreSQL` reads the minute and
/// second fields at.
fn take_int32(s: &str) -> Result<(i32, &str), IntervalError> {
    let (value, rest) = take_int64(s)?;
    let value = i32::try_from(value).map_err(|_| IntervalError::FieldOverflow)?;
    Ok((value, rest))
}

/// `ParseFractionalSecond`: a `.` and its digits, and nothing after them,
/// rounded to microseconds.
fn parse_fractional_second(s: &str) -> Result<i64, IntervalError> {
    let digits = s.strip_prefix('.').ok_or(IntervalError::Format)?;
    // `1.` is a whole second; `1.x` is malformed.
    if digits.is_empty() {
        return Ok(0);
    }
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(IntervalError::Format);
    }
    let frac: f64 = format!("0.{digits}")
        .parse()
        .map_err(|_| IntervalError::Format)?;
    Ok((frac * 1_000_000.0).round_ties_even() as i64)
}

/// Parse a `[-]HH:MM[:SS[.ffffff]]` clock term into signed microseconds
/// (`DecodeTimeForInterval`).
///
/// A two-field term is hours and minutes — except that a *fraction* on the
/// second field re-reads the whole term one place down the clock, so `2:03.4567`
/// is two minutes and three and a bit seconds, not two hours. The field range
/// `MINUTE TO SECOND` shifts it the same way.
fn parse_clock_term(
    tok: &str,
    range: Option<(IntervalField, IntervalField)>,
) -> Result<i64, IntervalError> {
    let (negative, body) = match tok.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, tok.strip_prefix('+').unwrap_or(tok)),
    };
    let (mut hour, rest) = take_int64(body)?;
    let Some(rest) = rest.strip_prefix(':') else {
        return Err(IntervalError::Format);
    };
    let (mut minute, rest) = take_int32(rest)?;
    let mut second: i32 = 0;
    let mut fsec: i64 = 0;
    // Shift a two-field reading down to `mm:ss`.
    let shift_down = |hour: &mut i64, minute: &mut i32, second: &mut i32| {
        *second = *minute;
        *minute = i32::try_from(*hour).map_err(|_| IntervalError::FieldOverflow)?;
        *hour = 0;
        Ok::<(), IntervalError>(())
    };
    if rest.is_empty() {
        if range == Some((IntervalField::Minute, IntervalField::Second)) {
            shift_down(&mut hour, &mut minute, &mut second)?;
        }
    } else if rest.starts_with('.') {
        fsec = parse_fractional_second(rest)?;
        shift_down(&mut hour, &mut minute, &mut second)?;
    } else if let Some(rest) = rest.strip_prefix(':') {
        let (parsed, rest) = take_int32(rest)?;
        second = parsed;
        if rest.starts_with('.') {
            fsec = parse_fractional_second(rest)?;
        } else if !rest.is_empty() {
            return Err(IntervalError::Format);
        }
    } else {
        return Err(IntervalError::Format);
    }
    if hour < 0 || !(0..60).contains(&minute) || !(0..=60).contains(&second) {
        return Err(IntervalError::FieldOverflow);
    }
    let total = [
        (hour, 3_600_000_000i64),
        (i64::from(minute), 60_000_000),
        (i64::from(second), 1_000_000),
    ]
    .into_iter()
    .try_fold(fsec, |acc, (value, scale)| {
        value
            .checked_mul(scale)
            .and_then(|product| acc.checked_add(product))
    })
    .ok_or(IntervalError::FieldOverflow)?;
    if negative {
        return total.checked_neg().ok_or(IntervalError::FieldOverflow);
    }
    Ok(total)
}

/// What a rejected clock term actually means, which depends on whether it
/// carried an explicit sign.
///
/// `ParseDateTime` types a term with a leading `+`/`-` and at least one digit as
/// `DTK_TZ`, not `DTK_TIME`. `DecodeInterval` reads that term as a clock only
/// while the read succeeds (`… && DecodeTimeForInterval(field[i] + 1, …) == 0`);
/// a failed read is not an error yet, because control falls through to the
/// plain-number case, which re-reads the token as one integer and stops at the
/// `:`. So a signed term the clock reader rejects ends as `DTERR_BAD_FORMAT`
/// (22007) where its unsigned twin ends as `DTERR_FIELD_OVERFLOW` (22015):
/// `'-12:99:00'` and `'-2562047788:00:54.775808'` are invalid input syntax,
/// `'12:99:00'` and `'2562047788:00:54.775808'` are field overflows.
///
/// One failure survives the fallthrough. An integer too wide for `i64` overflows
/// the number case exactly as it overflowed the clock reader, so
/// `'-99999999999999999999999:00:00'` stays a field overflow.
fn clock_term_failure(tok: &str, error: IntervalError) -> IntervalError {
    if !tok.starts_with(['+', '-']) {
        return error;
    }
    match take_int64(tok) {
        Ok(_) => IntervalError::Format,
        Err(overflow) => overflow,
    }
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
    /// Parse a signed decimal quantity. [`IntervalError::Format`] for anything
    /// that is not one — including the exponent forms `f64::from_str` would
    /// otherwise accept, which PostgreSQL's interval decoder rejects — and
    /// [`IntervalError::FieldOverflow`] for a well-formed run of digits too long
    /// for the `i64` `strtoi64` reads it into.
    fn parse(text: &str) -> Result<Quantity, IntervalError> {
        let (negative, digits) = match text.as_bytes().first() {
            Some(b'-') => (true, &text[1..]),
            Some(b'+') => (false, &text[1..]),
            _ => (false, text),
        };
        let (int_text, frac_text) = digits.split_once('.').unwrap_or((digits, ""));
        if int_text.is_empty() && frac_text.is_empty() {
            return Err(IntervalError::Format);
        }
        // `.5` is fine but `-.5` is not: PostgreSQL reads the integer part first,
        // and a sign with no digit behind it leaves the sign unconsumed.
        if int_text.is_empty() && digits.len() != text.len() {
            return Err(IntervalError::Format);
        }
        if !int_text
            .bytes()
            .chain(frac_text.bytes())
            .all(|b| b.is_ascii_digit())
        {
            return Err(IntervalError::Format);
        }
        // Parse the sign WITH the digits, so `-9223372036854775808` is readable
        // (its magnitude alone is not).
        let whole: i64 = if int_text.is_empty() {
            0
        } else if negative {
            format!("-{int_text}")
                .parse()
                .map_err(|_| IntervalError::FieldOverflow)?
        } else {
            int_text.parse().map_err(|_| IntervalError::FieldOverflow)?
        };
        let frac: f64 = if frac_text.is_empty() {
            0.0
        } else {
            format!("0.{frac_text}")
                .parse()
                .map_err(|_| IntervalError::Format)?
        };
        Ok(Quantity {
            whole,
            frac: if negative { -frac } else { frac },
        })
    }
}

/// Add one `<qty> <unit>` term to the accumulator, spilling a fractional
/// quantity into the next smaller field. This is the unit switch at the heart of
/// `DecodeInterval`, term for term.
fn accumulate_unit(
    qty: Quantity,
    unit: IntervalField,
    itm: &mut ItmIn,
) -> Result<(), IntervalError> {
    // The whole part of `qty`; the fractional part spills down.
    let Quantity { whole, frac } = qty;
    match unit {
        IntervalField::Millennium => {
            itm.add_years(whole, 1_000)?;
            itm.add_fract_years(frac, 1_000)?;
        }
        IntervalField::Century => {
            itm.add_years(whole, 100)?;
            itm.add_fract_years(frac, 100)?;
        }
        IntervalField::Decade => {
            itm.add_years(whole, 10)?;
            itm.add_fract_years(frac, 10)?;
        }
        IntervalField::Year => {
            itm.add_years(whole, 1)?;
            itm.add_fract_years(frac, 1)?;
        }
        IntervalField::Month => {
            itm.add_months(whole)?;
            // Fractional months → days (PG uses a 30-day month).
            itm.add_fract_days(frac, 30)?;
        }
        IntervalField::Week => {
            itm.add_days(whole, 7)?;
            itm.add_fract_days(frac, 7)?;
        }
        IntervalField::Day => {
            itm.add_days(whole, 1)?;
            itm.add_fract_micros(frac, USECS_PER_DAY_I64)?;
        }
        IntervalField::Hour => itm.add_micros(whole, frac, 3_600_000_000)?,
        IntervalField::Minute => itm.add_micros(whole, frac, 60_000_000)?,
        IntervalField::Second => itm.add_micros(whole, frac, 1_000_000)?,
        IntervalField::Millisecond => itm.add_micros(whole, frac, 1_000)?,
        IntervalField::Microsecond => itm.add_micros(whole, frac, 1)?,
    }
    Ok(())
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

    /// Build the field struct for a bare `time`, whose date patterns render
    /// against a fixed 2000-01-01 the way PostgreSQL's `to_char(time, …)` does.
    /// The hour is taken from the reading itself rather than from a combined
    /// datetime, so `24:00:00` renders as hour 24 instead of rolling into the
    /// next day.
    #[must_use]
    pub fn from_time(t: PgTime, tz_offset_secs: Option<i32>) -> Self {
        let mut fields = Self::from_civil(
            date_to_midnight(Date::constant(2000, 1, 1).into()),
            tz_offset_secs,
        );
        fields.hour = t.hour() as u32;
        fields.minute = t.minute() as u32;
        fields.second = t.second() as u32;
        fields.micros = (t.subsec_nanosecond() / 1_000) as u32;
        fields
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
    matches_with(chars, i, kw, |a, b| a.eq_ignore_ascii_case(&b))
}

/// Does `chars[i..]` start with the ASCII keyword `kw` (exact, case-sensitive)?
fn matches_at(chars: &[char], i: usize, kw: &str) -> bool {
    matches_with(chars, i, kw, |a, b| a == b)
}

/// The comparison both keyword matchers share.
///
/// It walks `kw`'s characters rather than collecting them, because the template
/// tokenizer runs this against every entry of a 118-row keyword table for every
/// character of every template it sees. Collecting a `Vec<char>` per comparison
/// put an allocation on that path and dominated the cost of parsing a row.
fn matches_with(chars: &[char], i: usize, kw: &str, eq: fn(char, char) -> bool) -> bool {
    let mut n = 0usize;
    for expected in kw.chars() {
        match chars.get(i + n) {
            Some(&actual) if eq(actual, expected) => n += 1,
            _ => return false,
        }
    }
    true
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
/// interval source, and a `time` reaches `24`, so the hour is folded into the
/// day first: PostgreSQL tests `tm_hour % HOURS_PER_DAY >= HOURS_PER_DAY / 2`,
/// which puts `24:00:00` and `interval '24 hours'` in AM rather than PM. The
/// remainder truncates toward zero, as C's does, so a negative interval hour
/// stays negative and reads AM.
fn meridiem(hour: i64, lower: bool, dotted: bool) -> String {
    let pm = hour % 24 >= 12;
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
///
/// Separate from `DateTimeFields` (which is for FORMATTING): this is the OUTPUT
/// of parsing. Every field is already resolved and range-checked the way
/// `PostgreSQL`'s `do_to_timestamp` resolves it, so the caller only has to build
/// a jiff `Date`/`DateTime` and, for `to_timestamp`, reduce the reading to an
/// instant.
///
/// `year` is astronomical: 1 BC is year 0, 44 BC is year -43. That is jiff's own
/// convention and `PostgreSQL`'s internal `tm_year`, so neither side converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub micros: u32,
    /// The UTC offset the input named, east-positive (jiff's convention, the
    /// opposite of `PostgreSQL`'s internal `tz->gmtoffset`). `None` when the
    /// template named no zone, in which case the caller interprets the reading
    /// in the session zone.
    pub tz_offset_secs: Option<i32>,
    /// The fractional-second precision an `FF1`..`FF6` pattern asked for, or
    /// `None` when the template used none.
    ///
    /// It is not a width limit on what was parsed — `FF1` still reads every
    /// digit there is — but a precision the finished value is rounded to, which
    /// is why it has to travel out to the caller rather than being applied here:
    /// rounding can carry into the second, and the second belongs to an instant
    /// the caller has not built yet.
    pub fractional_precision: Option<u8>,
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
            fractional_precision: None,
        }
    }
}

// ---------------------------------------------------------------------------
// SP38: `to_timestamp`/`to_date` template parsing.
//
// A port of `PostgreSQL`'s `DCH_from_char` (scan) plus the field-assembly half
// of `do_to_timestamp`. The two halves stay apart for the same reason they do
// there: the scan fills a `TmFromChar` whose zero value means "field absent",
// and only once the whole input has been read can the fields be reconciled.
// Several rules are order-independent — `AM` may precede or follow the hour it
// applies to, `CC` may precede or follow the `YY` it scales, and `DDD` fills the
// month and day only when `MM`/`DD` did not — so a single pass cannot do it.
// ---------------------------------------------------------------------------

/// Which calendar a template's fields commit to. Mixing the two is an error:
/// `IYYY` (ISO week year) and `YYYY` (Gregorian year) do not name the same thing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum DateMode {
    /// The pattern says nothing about which calendar is in use.
    #[default]
    None,
    /// Gregorian year/month/day.
    Gregorian,
    /// ISO 8601 week date.
    IsoWeek,
}

/// A template pattern `to_timestamp`/`to_date` recognizes.
///
/// One variant per distinct *behaviour*, not per spelling: `Mon`, `MON` and `mon`
/// all parse a month abbreviation case-insensitively, so they share one variant.
/// Case survives only where `PostgreSQL` keeps it, and for parsing it never does
/// — the spelling matters solely because error messages quote it, and that is
/// carried alongside as the node's name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Key {
    /// `AD`/`BC` and their lowercase spellings.
    Era,
    /// `A.D.`/`B.C.` and their lowercase spellings.
    EraDotted,
    /// `AM`/`PM` and their lowercase spellings.
    Meridiem,
    /// `A.M.`/`P.M.` and their lowercase spellings.
    MeridiemDotted,
    Cc,
    /// A day-of-week name; `full` picks `Day` over `Dy`.
    DayName {
        full: bool,
    },
    Ddd,
    Iddd,
    Dd,
    D,
    Id,
    /// `FF1`..`FF6`: fractional seconds to N digits.
    Ff(u8),
    Fx,
    Hh24,
    /// `HH`/`HH12`: a 12-hour clock reading.
    Hh12,
    Iw,
    Ww,
    Iyyy,
    Iyy,
    Iy,
    I,
    J,
    Mi,
    Mm,
    /// A month name; `full` picks `Month` over `Mon`.
    MonthName {
        full: bool,
    },
    Ms,
    Of,
    Q,
    Rm,
    Ssss,
    Ss,
    Tzh,
    Tzm,
    Tz,
    Us,
    W,
    /// `Y,YYY`: a year written with a thousands separator.
    YComma,
    Yyyy,
    Yyy,
    Yy,
    Y,
}

impl Key {
    /// Whether the pattern consumes digits. [`is_next_separator`] uses this to
    /// decide whether a fixed-width field may slurp past its nominal width.
    fn is_digit(self) -> bool {
        !matches!(
            self,
            Key::Era
                | Key::EraDotted
                | Key::Meridiem
                | Key::MeridiemDotted
                | Key::DayName { .. }
                | Key::Fx
                | Key::MonthName { .. }
                | Key::Of
                | Key::Rm
                | Key::Tz
                | Key::Tzh
        )
    }

    /// Which calendar this pattern commits the template to.
    fn date_mode(self) -> DateMode {
        match self {
            Key::Ddd
            | Key::Dd
            | Key::D
            | Key::Mm
            | Key::MonthName { .. }
            | Key::Rm
            | Key::Ww
            | Key::W
            | Key::YComma
            | Key::Yyyy
            | Key::Yyy
            | Key::Yy
            | Key::Y => DateMode::Gregorian,
            Key::Iddd | Key::Id | Key::Iw | Key::Iyyy | Key::Iyy | Key::Iy | Key::I => {
                DateMode::IsoWeek
            }
            _ => DateMode::None,
        }
    }

    /// The number of input characters a numeric pattern nominally consumes.
    /// Usually the keyword's own length, but `HH24` reads two digits, not four,
    /// and `Ff(n)` reads exactly `n`.
    fn field_width(self, name: &str) -> usize {
        match self {
            Key::Hh24 | Key::Hh12 | Key::Tzh | Key::Tzm | Key::Of => 2,
            Key::Ff(n) => usize::from(n),
            Key::Us => 6,
            Key::Ms | Key::Iddd => 3,
            Key::Id => 1,
            _ => name.chars().count(),
        }
    }
}
/// The template keyword table, in `PostgreSQL`'s own order.
///
/// The order is load-bearing twice over. Within a first-letter group the longer
/// spelling comes first (`MONTH` before `MON`, `SSSSS` before `SS`), which is
/// what makes recognition longest-match; and the groups themselves are what
/// `PostgreSQL`'s first-character index iterates, so scanning the whole table in
/// this order and taking the first prefix match reproduces `index_seq_search`
/// exactly. Matching is case-SENSITIVE per entry — that is why every spelling
/// `PostgreSQL` accepts appears here rather than being folded.
const KEYWORDS: &[(&str, Key)] = &[
    ("A.D.", Key::EraDotted),
    ("A.M.", Key::MeridiemDotted),
    ("AD", Key::Era),
    ("AM", Key::Meridiem),
    ("B.C.", Key::EraDotted),
    ("BC", Key::Era),
    ("CC", Key::Cc),
    ("DAY", Key::DayName { full: true }),
    ("DDD", Key::Ddd),
    ("DD", Key::Dd),
    ("DY", Key::DayName { full: false }),
    ("Day", Key::DayName { full: true }),
    ("Dy", Key::DayName { full: false }),
    ("D", Key::D),
    ("FF1", Key::Ff(1)),
    ("FF2", Key::Ff(2)),
    ("FF3", Key::Ff(3)),
    ("FF4", Key::Ff(4)),
    ("FF5", Key::Ff(5)),
    ("FF6", Key::Ff(6)),
    ("FX", Key::Fx),
    ("HH24", Key::Hh24),
    ("HH12", Key::Hh12),
    ("HH", Key::Hh12),
    ("IDDD", Key::Iddd),
    ("ID", Key::Id),
    ("IW", Key::Iw),
    ("IYYY", Key::Iyyy),
    ("IYY", Key::Iyy),
    ("IY", Key::Iy),
    ("I", Key::I),
    ("J", Key::J),
    ("MI", Key::Mi),
    ("MM", Key::Mm),
    ("MONTH", Key::MonthName { full: true }),
    ("MON", Key::MonthName { full: false }),
    ("MS", Key::Ms),
    ("Month", Key::MonthName { full: true }),
    ("Mon", Key::MonthName { full: false }),
    ("OF", Key::Of),
    ("P.M.", Key::MeridiemDotted),
    ("PM", Key::Meridiem),
    ("Q", Key::Q),
    ("RM", Key::Rm),
    ("SSSSS", Key::Ssss),
    ("SSSS", Key::Ssss),
    ("SS", Key::Ss),
    ("TZH", Key::Tzh),
    ("TZM", Key::Tzm),
    ("TZ", Key::Tz),
    ("US", Key::Us),
    ("WW", Key::Ww),
    ("W", Key::W),
    ("Y,YYY", Key::YComma),
    ("YYYY", Key::Yyyy),
    ("YYY", Key::Yyy),
    ("YY", Key::Yy),
    ("Y", Key::Y),
    ("a.d.", Key::EraDotted),
    ("a.m.", Key::MeridiemDotted),
    ("ad", Key::Era),
    ("am", Key::Meridiem),
    ("b.c.", Key::EraDotted),
    ("bc", Key::Era),
    ("cc", Key::Cc),
    ("day", Key::DayName { full: true }),
    ("ddd", Key::Ddd),
    ("dd", Key::Dd),
    ("dy", Key::DayName { full: false }),
    ("d", Key::D),
    ("ff1", Key::Ff(1)),
    ("ff2", Key::Ff(2)),
    ("ff3", Key::Ff(3)),
    ("ff4", Key::Ff(4)),
    ("ff5", Key::Ff(5)),
    ("ff6", Key::Ff(6)),
    ("fx", Key::Fx),
    ("hh24", Key::Hh24),
    ("hh12", Key::Hh12),
    ("hh", Key::Hh12),
    ("iddd", Key::Iddd),
    ("id", Key::Id),
    ("iw", Key::Iw),
    ("iyyy", Key::Iyyy),
    ("iyy", Key::Iyy),
    ("iy", Key::Iy),
    ("i", Key::I),
    ("j", Key::J),
    ("mi", Key::Mi),
    ("mm", Key::Mm),
    ("month", Key::MonthName { full: true }),
    ("mon", Key::MonthName { full: false }),
    ("ms", Key::Ms),
    ("of", Key::Of),
    ("p.m.", Key::MeridiemDotted),
    ("pm", Key::Meridiem),
    ("q", Key::Q),
    ("rm", Key::Rm),
    ("sssss", Key::Ssss),
    ("ssss", Key::Ssss),
    ("ss", Key::Ss),
    ("tzh", Key::Tzh),
    ("tzm", Key::Tzm),
    ("tz", Key::Tz),
    ("us", Key::Us),
    ("ww", Key::Ww),
    ("w", Key::W),
    ("y,yyy", Key::YComma),
    ("yyyy", Key::Yyyy),
    ("yyy", Key::Yyy),
    ("yy", Key::Yy),
    ("y", Key::Y),
];

/// One tokenized template element.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    /// A recognized pattern, with the spelling it was written as (which is what
    /// error messages quote) and its suffixes.
    Action {
        key: Key,
        name: &'static str,
        /// `FM`: fill mode, which for parsing means "slurp digits without a
        /// width limit".
        fm: bool,
        /// `TH`/`th`: an ordinal suffix in the input, skipped after the value.
        thth: bool,
    },
    /// A whitespace character in the template.
    Space,
    /// A printable non-alphanumeric character in the template.
    Separator,
    /// Any other literal character, including everything inside `"` quotes.
    /// The character is kept because [`is_next_separator`] has to know whether
    /// a digit follows a field.
    Char(char),
}

impl Node {
    fn key(&self) -> Option<Key> {
        match self {
            Node::Action { key, .. } => Some(*key),
            Node::Space | Node::Separator | Node::Char(_) => None,
        }
    }
}

/// `PostgreSQL`'s `is_separator_char`: a printable ASCII character that is
/// neither a letter nor a digit.
fn is_separator_char(c: char) -> bool {
    c > '\u{20}' && c < '\u{7f}' && !c.is_ascii_alphanumeric()
}

/// Tokenize a template into nodes, the way `parse_format` does with `DCH_FLAG`.
///
/// Prefix suffixes (`FM`, `TM`) bind to the pattern that follows, postfix
/// suffixes (`TH`, `th`, `SP`) to the pattern before. `TM` (localized names) and
/// `SP` (spelled-out numbers) are recognized so they do not fall through to
/// literal handling, but neither changes what is parsed: gres has no
/// locale-specific name tables, and `PostgreSQL` ignores `SP` when parsing too.
fn tokenize_template(template: &str) -> Vec<Node> {
    let chars: Vec<char> = template.chars().collect();
    let mut nodes = Vec::with_capacity(chars.len());
    let mut i = 0usize;
    while i < chars.len() {
        let mut fm = false;
        for prefix in ["FM", "fm", "TM", "tm"] {
            if matches_at(&chars, i, prefix) {
                fm |= prefix.eq_ignore_ascii_case("FM");
                i += 2;
                break;
            }
        }
        if let Some((name, key)) = KEYWORDS
            .iter()
            .find(|(name, _)| matches_at(&chars, i, name))
        {
            i += name.chars().count();
            let mut thth = false;
            for postfix in ["TH", "th", "SP"] {
                if matches_at(&chars, i, postfix) {
                    thth |= !postfix.eq_ignore_ascii_case("SP");
                    i += 2;
                    break;
                }
            }
            nodes.push(Node::Action {
                key: *key,
                name,
                fm,
                thth,
            });
            continue;
        }
        let Some(&c) = chars.get(i) else { break };
        if c == '"' {
            // A quoted run contributes one literal node per character; a
            // backslash quotes the character after it.
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                }
                nodes.push(Node::Char(chars[i]));
                i += 1;
            }
            if chars.get(i) == Some(&'"') {
                i += 1;
            }
            continue;
        }
        // Outside quotes a backslash is special only before a double quote.
        let c = if c == '\\' && chars.get(i + 1) == Some(&'"') {
            i += 1;
            '"'
        } else {
            c
        };
        nodes.push(if is_separator_char(c) {
            Node::Separator
        } else if c.is_whitespace() {
            Node::Space
        } else {
            Node::Char(c)
        });
        i += 1;
    }
    nodes
}

/// `PostgreSQL`'s `is_next_separator`: whether the field at `idx` may slurp
/// digits past its nominal width because nothing digit-like follows it.
fn is_next_separator(nodes: &[Node], idx: usize) -> bool {
    if let Some(Node::Action { thth: true, .. }) = nodes.get(idx) {
        return true;
    }
    match nodes.get(idx + 1) {
        // The end of the template counts as a non-digit separator.
        None => true,
        Some(Node::Action { key, .. }) => !key.is_digit(),
        // A literal template character counts as digit-like only when it is a
        // digit; everything else, separators and spaces included, does not.
        Some(Node::Char(c)) => !c.is_ascii_digit(),
        Some(Node::Space | Node::Separator) => true,
    }
}

/// Everything the scan collected — `PostgreSQL`'s `TmFromChar`.
///
/// Zero means "not supplied" for every numeric field, which is `PostgreSQL`'s
/// own convention and the reason [`set_int`] treats a zero destination as
/// writable. Several documented quirks fall out of that — `AM` followed by `PM`
/// is accepted, because `AM` stores 0 — and are reproduced rather than tidied,
/// since the regression suite pins them.
#[derive(Debug, Default)]
struct TmFromChar {
    mode: DateMode,
    hh: i32,
    pm: i32,
    mi: i32,
    ss: i32,
    ssss: i32,
    d: i32,
    dd: i32,
    ddd: i32,
    mm: i32,
    ms: i32,
    year: i32,
    bc: i32,
    ww: i32,
    w: i32,
    cc: i32,
    j: i32,
    us: i32,
    /// How many digits the year pattern nominally has, which decides whether a
    /// `CC` in the same template scales it.
    yysz: i32,
    /// Set by `HH`/`HH12` and by any meridiem marker.
    clock12: bool,
    /// The fractional-second precision an `FF`n pattern asked for, which the
    /// finished value is rounded to.
    ff: i32,
    tzsign: i32,
    tzh: i32,
    tzm: i32,
    /// A `TZ` pattern matched a zone abbreviation.
    has_tz: bool,
    /// The east-positive offset a fixed abbreviation resolved to.
    gmtoffset: i32,
    /// The zone a dynamic abbreviation resolved to, whose offset depends on the
    /// reading and so cannot be taken until assembly.
    tzp: Option<TimeZone>,
}

/// A template scan or assembly failure, carrying `PostgreSQL`'s message verbatim.
///
/// The DETAIL and HINT lines `PostgreSQL` attaches are not reproduced: gres's
/// error channel carries one message, so only the primary line survives.
fn template_error(message: String) -> TypeError {
    TypeError::InvalidDatetimeTemplate { message }
}

/// `from_char_set_int`: store `value`, rejecting a second, different value.
///
/// A destination still holding zero is writable, so a field genuinely set to
/// zero can be overwritten without complaint. That is `PostgreSQL`'s behaviour,
/// not an oversight to correct here.
fn set_int(dest: &mut i32, value: i32, name: &str) -> Result<(), TypeError> {
    if *dest != 0 && *dest != value {
        return Err(template_error(format!(
            "conflicting values for \"{name}\" field in formatting string"
        )));
    }
    *dest = value;
    Ok(())
}

/// `adjust_partial_year_to_2020`: widen a one-to-three-digit year into the
/// window around the current century, so `97` is 1997 and `20` is 2020.
fn adjust_partial_year_to_2020(year: i32) -> i32 {
    if year < 70 {
        year + 2000
    } else if year < 100 {
        year + 1900
    } else if year < 520 {
        year + 2000
    } else if year < 1000 {
        year + 1000
    } else {
        year
    }
}

/// A cursor over the input string, counted in characters.
struct Input<'a> {
    chars: &'a [char],
    pos: usize,
}

impl Input<'_> {
    fn at_end(&self) -> bool {
        self.pos >= self.chars.len()
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    /// The remainder, which is what `PostgreSQL` quotes in a `TZ` error.
    fn rest(&self) -> String {
        self.chars[self.pos.min(self.chars.len())..]
            .iter()
            .collect()
    }

    /// The remainder truncated at the first whitespace, which is what
    /// `from_char_seq_search` quotes.
    fn rest_to_space(&self) -> String {
        self.chars[self.pos.min(self.chars.len())..]
            .iter()
            .take_while(|c| !c.is_whitespace())
            .collect()
    }

    /// Skip a run of whitespace, returning how many characters went by.
    fn skip_space(&mut self) -> usize {
        let start = self.pos;
        while self.peek().is_some_and(char::is_whitespace) {
            self.pos += 1;
        }
        self.pos - start
    }

    /// `SKIP_THth`: step over an ordinal suffix in the input.
    fn skip_thth(&mut self, thth: bool) {
        if thth {
            for _ in 0..2 {
                if !self.at_end() {
                    self.pos += 1;
                }
            }
        }
    }

    /// Read an optional sign and a run of digits starting at `from`, stopping at
    /// `limit`. Returns the value, the position after it, and whether the digits
    /// overflowed `i32`.
    fn read_signed(&self, from: usize, limit: usize) -> (i64, usize, bool) {
        let mut p = from;
        let negative = self.chars.get(p) == Some(&'-');
        if p < limit && (negative || self.chars.get(p) == Some(&'+')) {
            p += 1;
        }
        let digits_start = p;
        let mut value: i64 = 0;
        let mut overflow = false;
        while p < limit
            && let Some(c) = self.chars.get(p).copied()
            && c.is_ascii_digit()
        {
            value = value * 10 + i64::from(c as u8 - b'0');
            if value > i64::from(i32::MAX) + 1 {
                overflow = true;
                value = i64::from(i32::MAX) + 1;
            }
            p += 1;
        }
        if p == digits_start {
            // No digits: PostgreSQL's `strtol` consumes nothing at all, sign
            // included.
            return (0, from, false);
        }
        (if negative { -value } else { value }, p, overflow)
    }
}

/// `from_char_parse_int_len`: read one integer, honouring fixed-width rules.
///
/// Returns the value and the number of input characters consumed. That count,
/// not the digit count, is what `PostgreSQL` feeds back into the `YY`/`YYY` year
/// widening and the `MS`/`US`/`FF` fraction scaling, so it is what comes back
/// here too.
fn parse_int(
    input: &mut Input<'_>,
    len: usize,
    slurp: bool,
    name: &str,
) -> Result<(i32, usize), TypeError> {
    let init = input.pos;
    input.skip_space();
    // `used` is how much input remains, which is what PostgreSQL's `strlcpy`
    // return value measures — not how many digits are there.
    let used = input.chars.len().saturating_sub(input.pos);
    // The field-width window PostgreSQL copies out and quotes back in its
    // "invalid value" messages. Built only when a message needs it: this
    // function runs once per numeric field of every parsed row.
    let start = input.pos.min(input.chars.len());
    let copy = || -> String {
        input.chars[start..(start + len).min(input.chars.len())]
            .iter()
            .collect()
    };

    let value = if slurp {
        // Fill mode, or nothing digit-like follows: take a sign and as many
        // digits as there are. A sign is in reach here but not in fixed-width
        // mode, which is how `'-44-02-01'` under `'YYYY-MM-DD'` reads a
        // negative year while `'-05'` under `'MM'` does not.
        let (value, end, overflow) = input.read_signed(input.pos, input.chars.len());
        if overflow {
            return Err(out_of_range_value(name));
        }
        input.pos = end.max(input.pos);
        value
    } else {
        if used < len {
            return Err(template_error(format!(
                "source string too short for \"{name}\" formatting field"
            )));
        }
        let limit = (input.pos + len).min(input.chars.len());
        let (value, end, overflow) = input.read_signed(input.pos, limit);
        if overflow {
            return Err(out_of_range_value(name));
        }
        let consumed = end - input.pos;
        if consumed > 0 && consumed < len {
            return Err(template_error(format!(
                "invalid value \"{}\" for \"{name}\"",
                copy()
            )));
        }
        input.pos = end;
        value
    };

    if input.pos == init {
        return Err(template_error(format!(
            "invalid value \"{}\" for \"{name}\"",
            copy()
        )));
    }
    let value = i32::try_from(value).map_err(|_| out_of_range_value(name))?;
    Ok((value, input.pos - init))
}

/// The 22008 a source number outside `i32` raises.
fn out_of_range_value(name: &str) -> TypeError {
    TypeError::DatetimeOutOfRange {
        message: format!("value for \"{name}\" in source string is out of range"),
    }
}

/// `from_char_seq_search`: match one of `array` case-insensitively at the
/// cursor, returning its index.
fn seq_search(input: &mut Input<'_>, array: &[&str], name: &str) -> Result<i32, TypeError> {
    for (idx, candidate) in array.iter().enumerate() {
        let needle: Vec<char> = candidate.chars().collect();
        if input_starts_with_ci(input.chars, input.pos, &needle) {
            input.pos += needle.len();
            return i32::try_from(idx).map_err(|_| out_of_range_value(name));
        }
    }
    Err(template_error(format!(
        "invalid value \"{}\" for \"{name}\"",
        input.rest_to_space()
    )))
}

/// `Y,YYY`: a millennia count, a comma, then up to three more digits.
///
/// `PostgreSQL` reads this with `sscanf("%d,%03d")`, so the second group is a
/// magnitude that is added, never negated: `-1,500` is -1000 + 500 = -500.
fn parse_y_comma_yyy(input: &mut Input<'_>) -> Result<i32, TypeError> {
    let bad = |input: &Input<'_>| {
        template_error(format!("invalid value \"{}\" for \"Y,YYY\"", input.rest()))
    };
    let start = input.pos;
    // `%d` skips leading whitespace before the sign.
    let mut scan = Input {
        chars: input.chars,
        pos: start,
    };
    scan.skip_space();
    let (millennia, after, overflow) = scan.read_signed(scan.pos, scan.chars.len());
    if after == scan.pos || input.chars.get(after) != Some(&',') {
        return Err(bad(input));
    }
    let mut p = after + 1;
    let mut years: i64 = 0;
    let mut n = 0;
    while n < 3
        && let Some(c) = input.chars.get(p).copied()
        && c.is_ascii_digit()
    {
        years = years * 10 + i64::from(c as u8 - b'0');
        p += 1;
        n += 1;
    }
    if n == 0 {
        return Err(bad(input));
    }
    input.pos = p;
    let total = if overflow {
        None
    } else {
        millennia
            .checked_mul(1000)
            .and_then(|m| m.checked_add(years))
    };
    total
        .and_then(|t| i32::try_from(t).ok())
        .ok_or_else(|| out_of_range_value("Y,YYY"))
}

/// `DecodeTimezoneAbbrevPrefix`: match the longest zone abbreviation that
/// prefixes the input.
///
/// Returns the east-positive offset and, for an abbreviation whose meaning
/// depends on the date (`MSK`), the zone to resolve it against once the fields
/// are known. The table is the one literal parsing uses, so `to_timestamp` and a
/// zone-bearing literal accept exactly the same set of spellings.
fn consume_zone_abbrev(input: &mut Input<'_>) -> Option<(i32, Option<TimeZone>)> {
    /// `PostgreSQL`'s `TOKMAXLEN`, the longest abbreviation it will consider.
    const TOKMAXLEN: usize = 10;
    let mut len = 0usize;
    while len < TOKMAXLEN
        && input
            .chars
            .get(input.pos + len)
            .is_some_and(|c| c.is_alphabetic())
    {
        len += 1;
    }
    while len > 0 {
        let word: String = input.chars[input.pos..input.pos + len]
            .iter()
            .map(char::to_ascii_lowercase)
            .collect();
        if let Some(found) = parse::abbrev_offset(&word) {
            input.pos += len;
            return Some(found);
        }
        len -= 1;
    }
    None
}

/// The Roman month numerals in `PostgreSQL`'s `rm_months_lower` order — longest
/// first, so `viii` wins over `vi` and `v`. The index maps to a month as
/// `12 - index`.
const ROMAN_MONTHS_DESC: [&str; 12] = [
    "xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i",
];

/// `DCH_from_char`: walk the tokenized template, consuming input as it goes.
struct Scanner<'a> {
    nodes: &'a [Node],
    input: Input<'a>,
    out: TmFromChar,
    fx_mode: bool,
    /// How many input characters were skipped beyond what the template asked
    /// for. It is what lets a literal node stand aside when a run of spaces has
    /// already moved the cursor, and what lets `TZH` reclaim a minus sign a
    /// separator node ate.
    extra_skip: i32,
}

impl<'a> Scanner<'a> {
    fn new(nodes: &'a [Node], chars: &'a [char]) -> Self {
        Scanner {
            nodes,
            input: Input { chars, pos: 0 },
            out: TmFromChar::default(),
            fx_mode: false,
            extra_skip: 0,
        }
    }

    /// Run the scan. It stops when the input runs out, leaving any remaining
    /// template nodes unconsumed — `PostgreSQL` does the same, which is why a
    /// template may name more fields than the input supplies.
    fn run(mut self) -> Result<TmFromChar, TypeError> {
        for idx in 0..self.nodes.len() {
            if self.input.at_end() {
                break;
            }
            let node = &self.nodes[idx];
            if !self.fx_mode
                && node.key() != Some(Key::Fx)
                && (matches!(node, Node::Action { .. }) || idx == 0)
            {
                self.extra_skip += skip_count(self.input.skip_space());
            }
            match node {
                Node::Space | Node::Separator => self.literal_separator(),
                Node::Char(_) => self.literal_char(),
                Node::Action {
                    key,
                    name,
                    fm,
                    thth,
                } => {
                    let action = Action {
                        key: *key,
                        name,
                        thth: *thth,
                        slurp: *fm || is_next_separator(self.nodes, idx),
                    };
                    self.action(action)?;
                    if !self.fx_mode {
                        // Spaces after a field are free, and reset the budget a
                        // following literal node may draw on.
                        self.extra_skip = skip_count(self.input.skip_space());
                    }
                }
            }
        }
        Ok(self.out)
    }

    /// A space or separator in the template: outside FX mode it matches one
    /// space or separator in the input, or nothing at all.
    fn literal_separator(&mut self) {
        if self.fx_mode {
            self.input.pos += 1;
            return;
        }
        self.extra_skip -= 1;
        if self
            .input
            .peek()
            .is_some_and(|c| c.is_whitespace() || is_separator_char(c))
        {
            self.input.pos += 1;
            self.extra_skip += 1;
        }
    }

    /// Any other literal character in the template. The input character it lines
    /// up against never has to match it.
    fn literal_char(&mut self) {
        if !self.fx_mode && self.extra_skip > 0 {
            // Characters already skipped stand in for this literal, so the
            // cursor holds still — it may be sitting on a field.
            self.extra_skip -= 1;
        } else {
            self.input.pos += 1;
        }
    }

    /// Read one integer for `action`, at its nominal field width.
    fn num(&mut self, action: &Action<'_>) -> Result<(i32, usize), TypeError> {
        parse_int(
            &mut self.input,
            action.key.field_width(action.name),
            action.slurp,
            action.name,
        )
    }

    fn action(&mut self, action: Action<'_>) -> Result<(), TypeError> {
        self.out.set_mode(action.key.date_mode())?;
        if self.time_action(&action)? || self.date_action(&action)? {
            return Ok(());
        }
        self.zone_action(&action)
    }

    /// Clock fields, the meridiem markers and `FX`.
    fn time_action(&mut self, action: &Action<'_>) -> Result<bool, TypeError> {
        let name = action.name;
        match action.key {
            Key::Fx => self.fx_mode = true,
            Key::MeridiemDotted | Key::Meridiem => {
                let table: &[&str] = if action.key == Key::MeridiemDotted {
                    &["a.m.", "p.m."]
                } else {
                    &["am", "pm"]
                };
                let v = seq_search(&mut self.input, table, name)?;
                set_int(&mut self.out.pm, v, name)?;
                self.out.clock12 = true;
            }
            Key::Hh12 | Key::Hh24 => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.hh, v, name)?;
                self.out.clock12 |= action.key == Key::Hh12;
                self.input.skip_thth(action.thth);
            }
            Key::Mi => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.mi, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::Ss => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.ss, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::Ssss => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.ssss, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::Ms => {
                let (v, used) = self.num(action)?;
                set_int(&mut self.out.ms, v, name)?;
                // `25` is 0.25 and `250` is 0.25 too; `025` is 0.025.
                self.out.ms *= match used {
                    1 => 100,
                    2 => 10,
                    _ => 1,
                };
                self.input.skip_thth(action.thth);
            }
            Key::Us | Key::Ff(_) => {
                if let Key::Ff(n) = action.key {
                    self.out.ff = i32::from(n);
                }
                let (v, used) = self.num(action)?;
                set_int(&mut self.out.us, v, name)?;
                self.out.us *= match used {
                    1 => 100_000,
                    2 => 10_000,
                    3 => 1_000,
                    4 => 100,
                    5 => 10,
                    _ => 1,
                };
                self.input.skip_thth(action.thth);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Calendar fields: era, year, month, day, week and Julian day.
    fn date_action(&mut self, action: &Action<'_>) -> Result<bool, TypeError> {
        let name = action.name;
        match action.key {
            Key::EraDotted | Key::Era => {
                let table: &[&str] = if action.key == Key::EraDotted {
                    &["a.d.", "b.c."]
                } else {
                    &["ad", "bc"]
                };
                let v = seq_search(&mut self.input, table, name)?;
                set_int(&mut self.out.bc, v, name)?;
            }
            Key::MonthName { full } => {
                let v = seq_search(&mut self.input, month_names(full), name)?;
                set_int(&mut self.out.mm, v + 1, name)?;
            }
            Key::Rm => {
                let v = seq_search(&mut self.input, &ROMAN_MONTHS_DESC, name)?;
                set_int(&mut self.out.mm, 12 - v, name)?;
            }
            Key::DayName { full } => {
                let v = seq_search(&mut self.input, day_names(full), name)?;
                set_int(&mut self.out.d, v, name)?;
                self.out.d += 1;
            }
            Key::Mm => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.mm, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::Ddd | Key::Iddd => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.ddd, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::Dd => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.dd, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::D => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.d, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::Id => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.d, v, name)?;
                // Shift ISO numbering (Monday = 1) onto Gregorian's (Sunday = 1).
                self.out.d += 1;
                if self.out.d > 7 {
                    self.out.d = 1;
                }
                self.input.skip_thth(action.thth);
            }
            Key::Ww | Key::Iw => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.ww, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::W => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.w, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::Q => {
                // The quarter is read and discarded: it does not pin a date, and
                // honouring it could contradict a month given alongside it.
                self.num(action)?;
                self.input.skip_thth(action.thth);
            }
            Key::J => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.j, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::Cc => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.cc, v, name)?;
                self.input.skip_thth(action.thth);
            }
            Key::YComma => {
                let v = parse_y_comma_yyy(&mut self.input)?;
                set_int(&mut self.out.year, v, name)?;
                self.out.yysz = 4;
                self.input.skip_thth(action.thth);
            }
            Key::Yyyy | Key::Iyyy => {
                let (v, _) = self.num(action)?;
                set_int(&mut self.out.year, v, name)?;
                self.out.yysz = 4;
                self.input.skip_thth(action.thth);
            }
            Key::Yyy | Key::Iyy | Key::Yy | Key::Iy | Key::Y | Key::I => {
                let (v, used) = self.num(action)?;
                set_int(&mut self.out.year, v, name)?;
                if used < 4 {
                    self.out.year = adjust_partial_year_to_2020(self.out.year);
                }
                self.out.yysz = match action.key {
                    Key::Yyy | Key::Iyy => 3,
                    Key::Yy | Key::Iy => 2,
                    _ => 1,
                };
                self.input.skip_thth(action.thth);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// `TZ`, `OF`, `TZH` and `TZM`.
    fn zone_action(&mut self, action: &Action<'_>) -> Result<(), TypeError> {
        let name = action.name;
        match action.key {
            Key::Tz | Key::Of | Key::Tzh => {
                if action.key == Key::Tz {
                    match consume_zone_abbrev(&mut self.input) {
                        Some((offset, zone)) => {
                            self.out.has_tz = true;
                            self.out.gmtoffset = offset;
                            self.out.tzp = zone;
                            // A zone abbreviation supersedes any earlier TZH/TZM.
                            self.out.tzsign = 0;
                            return Ok(());
                        }
                        None if self.input.peek().is_some_and(char::is_alphabetic) => {
                            // It starts with a letter, so it was meant to be an
                            // abbreviation; reading it as an offset cannot help.
                            return Err(template_error(format!(
                                "invalid value \"{}\" for \"{name}\"",
                                self.input.rest()
                            )));
                        }
                        None => {}
                    }
                }
                self.out.tzsign = zone_sign(&mut self.input, self.extra_skip);
                let (v, _) = parse_int(&mut self.input, 2, action.slurp, name)?;
                set_int(&mut self.out.tzh, v, name)?;
                // `OF`, and `TZ` read as an offset, also take an optional
                // `:MM`; a bare `TZH` does not.
                if action.key != Key::Tzh && self.input.peek() == Some(':') {
                    self.input.pos += 1;
                    let (v, _) = parse_int(&mut self.input, 2, action.slurp, name)?;
                    set_int(&mut self.out.tzm, v, name)?;
                }
            }
            Key::Tzm => {
                // A `TZM` with no `TZH` before it is taken as positive.
                if self.out.tzsign == 0 {
                    self.out.tzsign = 1;
                }
                let (v, _) = parse_int(&mut self.input, 2, action.slurp, name)?;
                set_int(&mut self.out.tzm, v, name)?;
            }
            _ => unreachable!("every key is handled by one of the three action groups"),
        }
        Ok(())
    }
}

/// One template pattern and the decisions already made about it.
struct Action<'a> {
    key: Key,
    name: &'a str,
    thth: bool,
    /// Whether the field may read digits past its nominal width.
    slurp: bool,
}

/// Clamp a character count into the `i32` the skip budget is kept in.
fn skip_count(n: usize) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

/// Three-letter month abbreviations, spelled out rather than sliced off
/// [`MONTH_NAMES`] so a name lookup borrows a table instead of building one.
const MONTH_ABBREVS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Three-letter day abbreviations, index 0 = Sunday, as [`DAY_NAMES`].
const DAY_ABBREVS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

/// The month names `Month`/`Mon` match against.
fn month_names(full: bool) -> &'static [&'static str] {
    if full { &MONTH_NAMES } else { &MONTH_ABBREVS }
}

/// The day names `Day`/`Dy` match against.
fn day_names(full: bool) -> &'static [&'static str] {
    if full { &DAY_NAMES } else { &DAY_ABBREVS }
}

/// The sign of a `TZH`/`OF` offset.
///
/// An explicit sign wins. Without one, a minus that a preceding separator node
/// already consumed still counts — that is the only way `'2000 -10'` under
/// `'YYYY TZH'` can come out negative, since the space node ate the `-`.
fn zone_sign(input: &mut Input<'_>, extra_skip: i32) -> i32 {
    match input.peek() {
        Some('+' | ' ') => {
            input.pos += 1;
            1
        }
        Some('-') => {
            input.pos += 1;
            -1
        }
        _ => {
            if extra_skip > 0 && input.pos > 0 && input.chars[input.pos - 1] == '-' {
                -1
            } else {
                1
            }
        }
    }
}

impl TmFromChar {
    /// `from_char_set_mode`: commit the template to a calendar, rejecting a mix.
    fn set_mode(&mut self, mode: DateMode) -> Result<(), TypeError> {
        if mode == DateMode::None {
            return Ok(());
        }
        if self.mode == DateMode::None {
            self.mode = mode;
        } else if self.mode != mode {
            return Err(template_error(
                "invalid combination of date conventions".to_string(),
            ));
        }
        Ok(())
    }
}

/// Template-driven parse for `to_timestamp`/`to_date`.
///
/// Tokenizes `template` into nodes exactly as `PostgreSQL`'s `parse_format`
/// does, scans `input` against them (`DCH_from_char`), then reconciles and
/// range-checks the fields (`do_to_timestamp`).
///
/// # Errors
///
/// [`TypeError::InvalidDatetimeTemplate`] (22007) when the input does not fit
/// the template, [`TypeError::DatetimeFieldOverflow`] or
/// [`TypeError::DatetimeOutOfRange`] (22008) for an out-of-range field, and
/// [`TypeError::TimezoneDisplacementOverflow`] (22009) for a zone offset past
/// ±15:59.
pub fn parse_by_template(template: &str, input: &str) -> Result<ParsedDateTime, TypeError> {
    let nodes = tokenize_template(template);
    let ichars: Vec<char> = input.chars().collect();
    let tm = Scanner::new(&nodes, &ichars).run()?;
    Assembly::new(tm, input).finish()
}

/// `do_to_timestamp`'s second half: reconcile the scanned fields into one
/// calendar reading, then range-check it.
struct Assembly<'a> {
    tm: TmFromChar,
    input: &'a str,
    year: i32,
    mon: i32,
    mday: i32,
    hour: i32,
    minute: i32,
    second: i32,
    /// Which of year/month/day the template actually supplied, so the validity
    /// check judges only what was given — `PostgreSQL`'s `fmask`.
    has_year: bool,
    has_mon: bool,
    has_day: bool,
}

impl<'a> Assembly<'a> {
    fn new(tm: TmFromChar, input: &'a str) -> Self {
        Assembly {
            tm,
            input,
            year: 0,
            mon: 0,
            mday: 0,
            hour: 0,
            minute: 0,
            second: 0,
            has_year: false,
            has_mon: false,
            has_day: false,
        }
    }

    /// The 22008 every out-of-range field raises, quoting the whole input the
    /// way `PostgreSQL`'s `DateTimeParseError` does.
    fn overflow(&self) -> TypeError {
        TypeError::DatetimeFieldOverflow {
            value: self.input.to_string(),
        }
    }

    fn finish(mut self) -> Result<ParsedDateTime, TypeError> {
        self.clock()?;
        self.calendar_year()?;
        self.calendar_day()?;
        let micros = i64::from(self.tm.ms) * 1000 + i64::from(self.tm.us);
        self.validate(micros)?;
        self.normalize_date();
        let tz_offset_secs = self.zone_offset()?;
        Ok(ParsedDateTime {
            year: self.year,
            month: u32::try_from(self.mon).map_err(|_| self.overflow())?,
            day: u32::try_from(self.mday).map_err(|_| self.overflow())?,
            hour: u32::try_from(self.hour).map_err(|_| self.overflow())?,
            minute: u32::try_from(self.minute).map_err(|_| self.overflow())?,
            second: u32::try_from(self.second).map_err(|_| self.overflow())?,
            micros: u32::try_from(micros).map_err(|_| self.overflow())?,
            tz_offset_secs,
            fractional_precision: u8::try_from(self.tm.ff).ok().filter(|ff| *ff != 0),
        })
    }

    /// Fill in the defaults for a month or day the template never supplied, and
    /// settle the result through a Julian round-trip.
    ///
    /// `PostgreSQL` builds its date with `date2j`, which carries an over-long
    /// day into the next month instead of rejecting it. That only ever shows
    /// with no year in hand, because the leap-aware day check needs the year and
    /// is skipped without one — which is how `to_timestamp('02-30', 'MM-DD')`
    /// comes out as March 1 rather than an error. Every date that passed the
    /// check comes back from the round-trip unchanged.
    fn normalize_date(&mut self) {
        if !self.has_mon {
            self.mon = 1;
        }
        if !self.has_day {
            self.mday = 1;
        }
        let julian = ymd_to_julian(
            i64::from(self.year),
            i64::from(self.mon),
            i64::from(self.mday),
        );
        let (year, mon, mday) = julian_to_ymd(julian);
        self.year = year;
        self.mon = mon;
        self.mday = mday;
    }

    /// Seconds-past-midnight, then the individual clock fields, then the
    /// 12-hour fold.
    fn clock(&mut self) -> Result<(), TypeError> {
        if self.tm.ssss != 0 {
            let mut x = self.tm.ssss;
            self.hour = x / 3600;
            x %= 3600;
            self.minute = x / 60;
            self.second = x % 60;
        }
        if self.tm.ss != 0 {
            self.second = self.tm.ss;
        }
        if self.tm.mi != 0 {
            self.minute = self.tm.mi;
        }
        if self.tm.hh != 0 {
            self.hour = self.tm.hh;
        }
        if self.tm.clock12 {
            if !(1..=12).contains(&self.hour) {
                return Err(template_error(format!(
                    "hour \"{}\" is invalid for the 12-hour clock",
                    self.hour
                )));
            }
            if self.tm.pm != 0 && self.hour < 12 {
                self.hour += 12;
            } else if self.tm.pm == 0 && self.hour == 12 {
                self.hour = 0;
            }
        }
        Ok(())
    }

    /// The year, from `YYYY`-family fields and/or `CC`.
    fn calendar_year(&mut self) -> Result<(), TypeError> {
        if self.tm.year != 0 {
            if self.tm.cc != 0 && self.tm.yysz <= 2 {
                // `CC` supplies the century for a one- or two-digit year. The
                // 21st century AD runs 2001-2100, and the 6th century BC runs
                // 600 BC to 501 BC, so neither end is a plain multiple of 100.
                if self.tm.bc != 0 {
                    self.tm.cc = -self.tm.cc;
                }
                let low = self.tm.year % 100;
                self.year = if low == 0 {
                    // A year ending in "00" is the century's own first year.
                    self.tm
                        .cc
                        .checked_mul(100)
                        .and_then(|y| y.checked_add(i32::from(self.tm.cc < 0)))
                } else if self.tm.cc >= 0 {
                    self.tm
                        .cc
                        .checked_sub(1)
                        .and_then(|c| c.checked_mul(100))
                        .and_then(|c| low.checked_add(c))
                } else {
                    self.tm
                        .cc
                        .checked_add(1)
                        .and_then(|c| c.checked_mul(100))
                        .and_then(|c| c.checked_sub(low))
                        .and_then(|c| c.checked_add(1))
                }
                .ok_or_else(|| self.overflow())?;
            } else {
                // A four-digit year stands on its own and `CC` is ignored.
                self.year = self.tm.year;
                if self.tm.bc != 0 {
                    self.year = -self.year;
                }
                // 1 BC is stored as year 0, 2 BC as -1, and so on.
                if self.year < 0 {
                    self.year += 1;
                }
            }
            self.has_year = true;
        } else if self.tm.cc != 0 {
            if self.tm.bc != 0 {
                self.tm.cc = -self.tm.cc;
            }
            self.year = if self.tm.cc >= 0 {
                // +1 because the 21st century began in 2001.
                self.tm
                    .cc
                    .checked_sub(1)
                    .and_then(|c| c.checked_mul(100))
                    .and_then(|c| c.checked_add(1))
            } else {
                // +1 because year -599 is 600 BC.
                self.tm.cc.checked_mul(100).and_then(|c| c.checked_add(1))
            }
            .ok_or_else(|| self.overflow())?;
            self.has_year = true;
        }
        Ok(())
    }

    /// The month and day, from a Julian day, an ISO week, a week-of-year, a
    /// day-of-year, or plain `MM`/`DD`.
    fn calendar_day(&mut self) -> Result<(), TypeError> {
        if self.tm.j != 0 {
            let (y, m, d) = julian_to_ymd(i64::from(self.tm.j));
            self.set_ymd(y, m, d);
        }
        if self.tm.ww != 0 {
            if self.tm.mode == DateMode::IsoWeek {
                // Without a weekday the date sits on the Monday the week starts.
                let jday = if self.tm.d != 0 {
                    iso_weekdate_to_julian(self.year, self.tm.ww, self.tm.d)
                } else {
                    iso_week_to_julian(self.year, self.tm.ww)
                };
                let (y, m, d) = julian_to_ymd(jday);
                self.set_ymd(y, m, d);
            } else {
                self.tm.ddd = week_to_day_of(self.tm.ww).ok_or_else(|| self.overflow())?;
            }
        }
        if self.tm.w != 0 {
            self.tm.dd = week_to_day_of(self.tm.w).ok_or_else(|| self.overflow())?;
        }
        if self.tm.dd != 0 {
            self.mday = self.tm.dd;
            self.has_day = true;
        }
        if self.tm.mm != 0 {
            self.mon = self.tm.mm;
            self.has_mon = true;
        }
        if self.tm.ddd != 0 && (self.mon <= 1 || self.mday <= 1) {
            self.day_of_year()?;
        }
        Ok(())
    }

    fn set_ymd(&mut self, year: i32, mon: i32, mday: i32) {
        self.year = year;
        self.mon = mon;
        self.mday = mday;
        self.has_year = true;
        self.has_mon = true;
        self.has_day = true;
    }

    /// Fill the month and day from a day-of-year, which is only reached when
    /// neither was really given.
    fn day_of_year(&mut self) -> Result<(), TypeError> {
        if self.year == 0 && self.tm.bc == 0 {
            return Err(template_error(
                "cannot calculate day of year without year information".to_string(),
            ));
        }
        if self.tm.mode == DateMode::IsoWeek {
            let j0 = iso_week_to_julian(self.year, 1) - 1;
            let (y, m, d) = julian_to_ymd(j0 + i64::from(self.tm.ddd));
            self.set_ymd(y, m, d);
            return Ok(());
        }
        /// Days elapsed before the start of each month, common year then leap.
        const YSUM: [[i32; 13]; 2] = [
            [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
            [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
        ];
        let cumulative = &YSUM[usize::from(is_leap_year(self.year))];
        let mut i = 1usize;
        while i <= 12 {
            if self.tm.ddd <= cumulative[i] {
                break;
            }
            i += 1;
        }
        // A day-of-year past the end of the year leaves `i` at 13, which the
        // month range check then rejects — the same way PostgreSQL's does.
        if self.mon <= 1 {
            self.mon = i32::try_from(i).unwrap_or(i32::MAX);
        }
        if self.mday <= 1 {
            self.mday = self.tm.ddd - cumulative[i - 1];
        }
        self.has_mon = true;
        self.has_day = true;
        Ok(())
    }

    /// `ValidateDate` plus the clock-field range check.
    fn validate(&self, micros: i64) -> Result<(), TypeError> {
        if self.has_mon && !(1..=12).contains(&self.mon) {
            return Err(self.overflow());
        }
        if self.has_day && !(1..=31).contains(&self.mday) {
            return Err(self.overflow());
        }
        if self.has_year
            && self.has_mon
            && self.has_day
            && self.mday > days_in_civil_month(self.year, self.mon)
        {
            return Err(self.overflow());
        }
        if !(0..24).contains(&self.hour)
            || !(0..60).contains(&self.minute)
            || !(0..60).contains(&self.second)
            || !(0..1_000_000).contains(&micros)
        {
            return Err(self.overflow());
        }
        Ok(())
    }

    /// Reduce whatever zone information the template carried to a UTC offset.
    fn zone_offset(&self) -> Result<Option<i32>, TypeError> {
        if self.tm.tzsign != 0 {
            if !(0..=15).contains(&self.tm.tzh) || !(0..60).contains(&self.tm.tzm) {
                return Err(TypeError::TimezoneDisplacementOverflow {
                    value: self.input.to_string(),
                });
            }
            return Ok(Some(
                self.tm.tzsign * (self.tm.tzh * 3600 + self.tm.tzm * 60),
            ));
        }
        if !self.tm.has_tz {
            return Ok(None);
        }
        let Some(zone) = self.tm.tzp.as_ref() else {
            return Ok(Some(self.tm.gmtoffset));
        };
        // A dynamic abbreviation means whatever the zone meant at this reading,
        // so it cannot be resolved until every field is known.
        let dt = DateTime::new(
            i16::try_from(self.year).map_err(|_| self.overflow())?,
            i8::try_from(self.mon.max(1)).map_err(|_| self.overflow())?,
            i8::try_from(self.mday.max(1)).map_err(|_| self.overflow())?,
            i8::try_from(self.hour).map_err(|_| self.overflow())?,
            i8::try_from(self.minute).map_err(|_| self.overflow())?,
            i8::try_from(self.second).map_err(|_| self.overflow())?,
            0,
        )
        .map_err(|_| self.overflow())?;
        Ok(Some(zone_offset_for(dt, zone).seconds()))
    }
}

/// The first day of week `week`, as a day-of-year (`PostgreSQL` derives both
/// `WW` and `W` this way).
fn week_to_day_of(week: i32) -> Option<i32> {
    week.checked_sub(1)?.checked_mul(7)?.checked_add(1)
}

/// `PostgreSQL`'s `isleap`.
fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// Days in a proleptic-Gregorian month, computed here rather than through jiff
/// because the range check has to run before a jiff `Date` can be built.
fn days_in_civil_month(year: i32, month: i32) -> i32 {
    const DAY_TAB: [[i32; 12]; 2] = [
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    ];
    let idx = usize::try_from(month - 1).unwrap_or(0).min(11);
    DAY_TAB[usize::from(is_leap_year(year))][idx]
}

/// `PostgreSQL`'s `date2j`: a calendar date as a Julian Day Number.
///
/// Kept in `i64` where `PostgreSQL` uses `int`, so an absurd year produces a
/// large Julian day instead of wrapping. Every such day is outside the
/// representable calendar and is rejected downstream either way.
fn ymd_to_julian(year: i64, month: i64, day: i64) -> i64 {
    let (y, m) = if month > 2 {
        (year + 4800, month + 1)
    } else {
        (year + 4799, month + 13)
    };
    let century = y / 100;
    y * 365 - 32_167 + y / 4 - century + century / 4 + 7_834 * m / 256 + day
}

/// `PostgreSQL`'s `j2date`: a Julian Day Number as a calendar date.
fn julian_to_ymd(jd: i64) -> (i32, i32, i32) {
    let mut julian = jd + 32_044;
    let mut quad = julian.div_euclid(146_097);
    let extra = (julian - quad * 146_097) * 4 + 3;
    julian += 60 + quad * 3 + extra.div_euclid(146_097);
    quad = julian.div_euclid(1_461);
    julian -= quad * 1_461;
    let mut y = (julian * 4).div_euclid(1_461);
    julian = (if y == 0 {
        (julian + 306).rem_euclid(366)
    } else {
        (julian + 305).rem_euclid(365)
    }) + 123;
    y += quad * 4;
    let year = y - 4_800;
    let quad = (julian * 2_141).div_euclid(65_536);
    let day = julian - (7_834 * quad).div_euclid(256);
    let month = (quad + 10).rem_euclid(12) + 1;
    (
        i32::try_from(year).unwrap_or(i32::MAX),
        i32::try_from(month).unwrap_or(i32::MAX),
        i32::try_from(day).unwrap_or(i32::MAX),
    )
}

/// `PostgreSQL`'s `j2day`: 0 = Sunday.
fn julian_to_weekday(jd: i64) -> i64 {
    (jd + 1).rem_euclid(7)
}

/// `PostgreSQL`'s `isoweek2j`: the Julian day the given ISO week starts on.
fn iso_week_to_julian(year: i32, week: i32) -> i64 {
    let day4 = ymd_to_julian(i64::from(year), 1, 4);
    let day0 = julian_to_weekday(day4 - 1);
    i64::from(week - 1) * 7 + (day4 - day0)
}

/// `PostgreSQL`'s `isoweekdate2date`, as a Julian day. `wday` is Gregorian
/// (Sunday = 1), which is what `ID` was already shifted to.
fn iso_weekdate_to_julian(year: i32, week: i32, wday: i32) -> i64 {
    let jday = iso_week_to_julian(year, week);
    if wday > 1 {
        jday + i64::from(wday - 2)
    } else {
        jday + 6
    }
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

/// The seconds argument of a `make_*` complaint, spelled as C's `%02g` spells
/// it: six significant digits, trailing zeros trimmed, exponent notation once
/// the value leaves `1e-5 .. 1e6`, and a minimum field width of two.
///
/// The width is what turns a whole `0` into `00`, so `make_time(-1, 0, 0)`
/// reports `-1:00:00` rather than `-1:00:0`. PostgreSQL's own `snprintf` spells
/// the non-finite values `Infinity` and `NaN`, not C's `inf` and `nan`.
fn format_seconds_g(sec: f64) -> String {
    let mut rendered = if sec.is_nan() {
        "NaN".to_string()
    } else if sec.is_infinite() {
        if sec < 0.0 { "-Infinity" } else { "Infinity" }.to_string()
    } else {
        // `%g` picks its style from the exponent a `%e` conversion of the same
        // precision would carry, so read that exponent off such a conversion
        // rather than off a logarithm, which disagrees on the rounding
        // boundaries.
        let scientific = format!("{sec:.5e}");
        let exponent: i32 = scientific
            .split_once('e')
            .and_then(|(_, exponent)| exponent.parse().ok())
            .unwrap_or(0);
        if (-4..6).contains(&exponent) {
            let precision = usize::try_from(5 - exponent).unwrap_or(0);
            trim_g_zeros(&format!("{sec:.precision$}"))
        } else {
            let (mantissa, _) = scientific.split_once('e').unwrap_or((&scientific, "0"));
            let sign = if exponent < 0 { '-' } else { '+' };
            format!("{}e{sign}{:02}", trim_g_zeros(mantissa), exponent.abs())
        }
    };
    // The field width counts the sign, and the `0` flag pads behind it, so only
    // a bare single digit grows.
    while rendered.len() < 2 {
        rendered.insert(usize::from(rendered.starts_with('-')), '0');
    }
    rendered
}

/// Drop the trailing zeros of a fixed-point rendering, and the decimal point
/// with them when nothing is left after it. This is `%g`'s trailing-zero rule.
fn trim_g_zeros(rendered: &str) -> String {
    if rendered.contains('.') {
        rendered.trim_end_matches('0').trim_end_matches('.')
    } else {
        rendered
    }
    .to_string()
}

/// `date field value out of range: 2013-02-30`: the 22008 the `make_*`
/// constructors raise for a field `ValidateDate` refuses.
///
/// The three fields are spelled `%d-%02d-%02d`, so the month and the day are
/// zero-padded to two characters and the sign counts towards the width — a
/// negative day comes out as `2013-11--1`, exactly as PostgreSQL prints it.
fn date_field_out_of_range(year: i32, month: i32, day: i32) -> TypeError {
    TypeError::DatetimeOutOfRange {
        message: format!("date field value out of range: {year}-{month:02}-{day:02}"),
    }
}

/// `date out of range: 5874898-01-01`: the 22008 for a field set that passes
/// validation but names a day outside the type's range.
fn make_date_out_of_range(year: i32, month: i32, day: i32) -> TypeError {
    TypeError::DatetimeOutOfRange {
        message: format!("date out of range: {year}-{month:02}-{day:02}"),
    }
}

/// `time field value out of range: 10:55:100.1`: the 22008 for a clock field
/// `float_time_overflows` refuses. The seconds are spelled `%02g`.
fn time_field_out_of_range(hour: i32, min: i32, sec: f64) -> TypeError {
    TypeError::DatetimeOutOfRange {
        message: format!(
            "time field value out of range: {hour}:{min:02}:{}",
            format_seconds_g(sec)
        ),
    }
}

/// `make_date(year, month, day)` (`ValidateDate` on positional fields).
///
/// A NEGATIVE year is the BC era rather than an astronomical year, so
/// `make_date(-44, 3, 15)` is 44 BC and there is no year zero on either side of
/// the boundary. A field the validation refuses is `date field value out of
/// range`; a validated field set that names a day the type cannot hold is `date
/// out of range`. Both are 22008.
pub fn make_date(year: i32, month: i32, day: i32) -> Result<PgDate, TypeError> {
    make_date_civil(year, month, day).map(PgDate::Finite)
}

/// [`make_date`]'s calendar half, for the callers that go straight on to build a
/// `timestamp` and so need the civil fields back.
fn make_date_civil(year: i32, month: i32, day: i32) -> Result<Date, TypeError> {
    let is_bc = year < 0;
    // PostgreSQL negates in place and then reports whatever the field holds at
    // the moment it fails, so the year in the message is the era-corrected one
    // everywhere except on the negation itself.
    let magnitude = if is_bc {
        year.checked_neg()
            .ok_or_else(|| date_field_out_of_range(year, month, day))?
    } else {
        year
    };
    if magnitude <= 0 {
        return Err(date_field_out_of_range(magnitude, month, day));
    }
    // BC years are stored astronomically: 1 BC is year 0, so `n BC` is `-(n-1)`.
    let astronomical = if is_bc { -(magnitude - 1) } else { magnitude };
    // The coarse checks come before the month-length one, which is why
    // `make_date(2013, 13, 1)` complains about the month and not the day.
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(date_field_out_of_range(astronomical, month, day));
    }
    let range = || make_date_out_of_range(astronomical, month, day);
    let y = i16::try_from(astronomical).map_err(|_| range())?;
    let mo = i8::try_from(month).map_err(|_| range())?;
    let d = i8::try_from(day).map_err(|_| range())?;
    let first = Date::new(y, mo, 1).map_err(|_| range())?;
    if d > first.days_in_month() {
        return Err(date_field_out_of_range(astronomical, month, day));
    }
    let built = Date::new(y, mo, d).map_err(|_| range())?;
    // Nothing below PostgreSQL's own lower bound is a date a constructor may
    // return. There is no upper case: the non-finite values are out of band, so
    // the last civil date is an ordinary result.
    if built < MIN_FINITE_DATE {
        return Err(range());
    }
    Ok(built)
}

/// `make_time(hour, min, sec)` (`float_time_overflows` then `tm2time`); the
/// fractional part of `sec` becomes microseconds, PostgreSQL's resolution.
///
/// The fields are summed rather than assembled, so a seconds argument of `60` is
/// legal and rolls into the next minute: `make_time(1, 2, 60)` is `01:03:00`.
/// Only the total is bounded, at `24:00:00`, which is why `make_time(24, 0, 0)`
/// is the one hour-24 call that succeeds.
pub fn make_time(hour: i32, min: i32, sec: f64) -> Result<PgTime, TypeError> {
    let refuse = || time_field_out_of_range(hour, min, sec);
    if !(0..=24).contains(&hour) || !(0..60).contains(&min) || sec.is_nan() {
        return Err(refuse());
    }
    // Round to microseconds BEFORE the range check, so a seconds argument that
    // only reaches 60 by rounding is not refused for having got there.
    let sec_micros = (sec * USECS_PER_SEC_F64).round_ties_even();
    if !(0.0..=60.0 * USECS_PER_SEC_F64).contains(&sec_micros) {
        return Err(refuse());
    }
    let total = (i64::from(hour) * 60 + i64::from(min)) * 60 * 1_000_000 + sec_micros as i64;
    PgTime::from_micros_of_day(total).ok_or_else(refuse)
}

/// Civil-`DateTime` builder shared by `make_timestamp` / `make_timestamptz` (the
/// executor wraps the time-zone step for the latter). An out-of-range field →
/// 22008, worded by whichever half refused it; a field set both halves accept
/// that still names no `timestamp` is `timestamp out of range`.
pub fn make_timestamp_civil(
    y: i32,
    mo: i32,
    d: i32,
    h: i32,
    mi: i32,
    sec: f64,
) -> Result<DateTime, TypeError> {
    let date = make_date_civil(y, mo, d)?;
    let time = make_time(h, mi, sec)?;
    combine_date_time(PgDate::Finite(date), time).ok_or_else(|| TypeError::DatetimeOutOfRange {
        message: format!(
            "timestamp out of range: {}-{mo:02}-{d:02} {h}:{mi:02}:{}",
            date.year(),
            format_seconds_g(sec)
        ),
    })
}

/// `make_interval(years, months, weeks, days, hours, mins, secs)`: weeks fold into
/// days, years into months, and the clock fields (hours/mins/secs, fractional secs
/// included) into microseconds. All arithmetic is checked; a field that will not
/// hold its sum is `interval out of range` (22008), the wording every `interval`
/// overflow carries.
///
/// The one exception is `secs` so large that scaling it to microseconds leaves
/// `f64` altogether. That is the multiplication overflowing, not the interval,
/// and PostgreSQL reports it as `float8_mul` does: `value out of range: overflow`
/// (22003).
pub fn make_interval(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    mins: i32,
    secs: f64,
) -> Result<Interval, TypeError> {
    // Checked before anything else, as PostgreSQL checks it: a non-finite
    // seconds argument decides the error even when a whole-number field would
    // also have overflowed.
    if !secs.is_finite() {
        return Err(interval_out_of_range());
    }
    // months = years*12 + months (checked, i32).
    let months = years
        .checked_mul(12)
        .and_then(|m| m.checked_add(months))
        .ok_or_else(interval_out_of_range)?;
    // days = weeks*7 + days (checked, i32).
    let days = weeks
        .checked_mul(7)
        .and_then(|d| d.checked_add(days))
        .ok_or_else(interval_out_of_range)?;
    // micros = (((hours*60 + mins)*60) * 1e6) + rint(secs*1e6) (checked, i64).
    let sec_micros_f = secs * 1_000_000.0;
    if sec_micros_f.is_infinite() {
        return Err(TypeError::OutOfRange {
            message: "value out of range: overflow".to_string(),
        });
    }
    let sec_micros_f = sec_micros_f.round_ties_even();
    if !fits_in_i64(sec_micros_f) {
        return Err(interval_out_of_range());
    }
    let sec_micros = sec_micros_f as i64;
    let micros = (i64::from(hours) * 60 + i64::from(mins))
        .checked_mul(60)
        .and_then(|s| s.checked_mul(1_000_000))
        .and_then(|us| us.checked_add(sec_micros))
        .ok_or_else(interval_out_of_range)?;
    finite_interval(Interval {
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
        months: i32::try_from(months).map_err(|_| interval_out_of_range())?,
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
        days: i32::try_from(days).map_err(|_| interval_out_of_range())?,
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
        months: i32::try_from(months).map_err(|_| interval_out_of_range())?,
        days: i32::try_from(days).map_err(|_| interval_out_of_range())?,
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
        // `24:00:00` folds into the day before the meridiem test, so it reads
        // AM like the midnight it sits one day above, not PM.
        let h = DateTimeFields::from_time(super::PgTime::END_OF_DAY, None);
        use assert2::assert;
        assert!(format_datetime("HH24:MI:SS", &h).expect("end") == "24:00:00");
        assert!(format_datetime("HH12 AM", &h).expect("end") == "12 AM");
        assert!(format_datetime("SSSS", &h).expect("end") == "86400");
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

    /// The one rule every timestamp-difference operator shares, against
    /// `PostgreSQL` 18.4. Two infinities of the SAME sign have to cancel and are
    /// 22008; otherwise the infinite endpoint decides, with `end`'s sign winning
    /// over a negated `start`'s. Two finite endpoints are the caller's problem.
    #[test]
    fn a_difference_with_a_non_finite_endpoint_follows_one_rule() {
        use assert2::assert;

        // (end sign, start sign) → the text of the answer, or the error.
        let cases = [
            ((1, 1), Err("interval out of range")),
            ((-1, -1), Err("interval out of range")),
            ((1, -1), Ok("infinity")),
            ((-1, 1), Ok("-infinity")),
            ((1, 0), Ok("infinity")),
            ((0, 1), Ok("-infinity")),
            ((-1, 0), Ok("-infinity")),
            ((0, -1), Ok("infinity")),
        ];
        for ((end, start), expected) in cases {
            let outcome = infinite_interval_difference(end, start)
                .unwrap_or_else(|| panic!("({end}, {start}) is non-finite"));
            match (outcome, expected) {
                (Ok(interval), Ok(text)) => assert!(interval_to_text(interval) == text),
                (Err(error), Err(text)) => {
                    assert!(error.sqlstate() == "22008");
                    assert!(error.to_string() == text);
                }
                (got, want) => panic!("({end}, {start}): {got:?} is not {want:?}"),
            }
        }
        // Two finite endpoints are nobody's infinity.
        assert!(infinite_interval_difference(0, 0).is_none());
        // The three difference operators agree with the rule they now share.
        assert!(
            timestamp_diff(TIMESTAMP_INFINITY, TIMESTAMP_NEG_INFINITY).expect("difference")
                == Interval::INFINITY
        );
        assert!(timestamp_diff(TIMESTAMP_INFINITY, TIMESTAMP_INFINITY).is_err());
        assert!(
            timestamptz_diff(timestamptz_neg_infinity(), timestamptz_infinity())
                .expect("difference")
                == Interval::NEG_INFINITY
        );
        assert!(timestamptz_diff(timestamptz_infinity(), timestamptz_infinity()).is_err());
    }

    /// Each non-finite value still sorts outside every finite one, which is the
    /// whole reason no comparison path needs a case for them: they group,
    /// aggregate and key an index the way `PostgreSQL` does, for free. For
    /// `date` the ordering now comes from [`PgDate`]'s variant order; for the
    /// other three it still comes from the extreme representable value.
    #[test]
    fn the_reserved_values_still_sort_outside_every_finite_one() {
        use assert2::assert;

        let dates = [
            Date::constant(-4713, 11, 24),
            Date::constant(1, 1, 1),
            Date::constant(2000, 1, 1),
            Date::constant(9999, 12, 30),
        ];
        for d in dates.map(PgDate::Finite) {
            assert!(DATE_NEG_INFINITY < d && d < DATE_INFINITY);
            assert!(!date_is_infinite(d));
            let ts = date_to_midnight(d);
            assert!(TIMESTAMP_NEG_INFINITY < ts && ts < TIMESTAMP_INFINITY);
            assert!(!timestamp_is_infinite(ts));
        }
        for instant in [
            Timestamp::UNIX_EPOCH,
            Timestamp::from_second(1).expect("ts"),
        ] {
            assert!(timestamptz_neg_infinity() < instant && instant < timestamptz_infinity());
            assert!(!timestamptz_is_infinite(instant));
        }
        // An `age` answer of `infinity` is the same reserved interval every
        // other operator produces, so it aggregates and orders with them.
        let extremes = [iv(0, 0, i64::MAX), iv(i32::MAX, 0, 0), iv(0, i32::MAX, 0)];
        for finite in extremes {
            assert!(!finite.is_infinite());
            assert!(Interval::NEG_INFINITY < finite && finite < Interval::INFINITY);
        }
        assert!(
            infinite_interval_difference(1, 0)
                .expect("non-finite")
                .expect("interval")
                == Interval::INFINITY
        );
        assert!(
            infinite_interval_difference(-1, 0)
                .expect("non-finite")
                .expect("interval")
                == Interval::NEG_INFINITY
        );
    }

    /// The last civil date is a date, not a sentinel.
    ///
    /// jiff's `Date::MAX` IS 9999-12-31. A `date` that reserved the extreme
    /// representable value therefore took a real date away from the user.
    /// `date '9999-12-31'` and `make_date(9999, 12, 31)` were both refused as
    /// out of range, and a value that got in printed as `infinity`. PostgreSQL
    /// 18.4 accepts both spellings. [`PgDate`] holds the two non-finite values
    /// out of band, so the top of the calendar is free again.
    ///
    /// The wire and on-disk form must not move with it. The storage still
    /// reserves `i32::MIN` and `i32::MAX`, and `crabka_pgkv`'s row encoding
    /// round-trips through this pair of functions.
    #[test]
    fn the_last_civil_date_is_a_date_and_not_the_infinity_sentinel() {
        use assert2::assert;

        // literal, `date_out` text, `date_send` bytes.
        let cases = [
            ("-infinity", "-infinity", i32::MIN),
            ("4713-11-24 BC", "4713-11-24 BC", -2_451_179),
            ("2000-01-01", "2000-01-01", 0),
            ("9999-12-30", "9999-12-30", 2_921_938),
            ("9999-12-31", "9999-12-31", 2_921_939),
            ("infinity", "infinity", i32::MAX),
        ];
        for (literal, text, days) in cases {
            let parsed = parse_date(literal).unwrap_or_else(|e| panic!("{literal}: {e}"));
            assert!(date_to_text(parsed) == text, "{literal}");
            assert!(date_to_binary(parsed) == days.to_be_bytes(), "{literal}");
            assert!(
                date_from_binary(&days.to_be_bytes()).expect("recv") == parsed,
                "{literal}"
            );
        }

        // `make_date` reaches the same day the literal does.
        let top = make_date(9999, 12, 31).expect("the last civil date");
        assert!(top == parse_date("9999-12-31").expect("literal"));
        assert!(date_to_text(top) == "9999-12-31");
        assert!(!date_is_infinite(top));
        assert!(top.finite() == Some(Date::constant(9999, 12, 31)));

        // And it still sorts BELOW `infinity`, which is the property the
        // reserved civil value used to buy.
        assert!(DATE_NEG_INFINITY < top && top < DATE_INFINITY);
        assert!(date_to_midnight(DATE_INFINITY) == TIMESTAMP_INFINITY);
        assert!(date_to_midnight(top) < TIMESTAMP_INFINITY);
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

    /// A clock term that carries an explicit sign fails differently from the
    /// same term without one: `PostgreSQL` reads the signed form through its
    /// timezone-shaped token, and a failed read falls through to the
    /// plain-number case rather than reporting the clock reader's complaint.
    #[test]
    fn a_signed_clock_term_fails_as_bad_syntax_not_field_overflow() {
        use assert2::assert;

        let cases = [
            // One microsecond past the widest interval, which is the value
            // PostgreSQL reserves for `-infinity`.
            ("-2562047788:00:54.775808", "22007"),
            ("+2562047788:00:54.775808", "22007"),
            ("2562047788:00:54.775808", "22015"),
            // A field outside its own range, not the accumulator's.
            ("-12:99:00", "22007"),
            ("12:99:00", "22015"),
            ("-12:00:99", "22007"),
            ("12:00:99", "22015"),
            // The hour count alone leaves the accumulator.
            ("-100000000000:00:00", "22007"),
            ("100000000000:00:00", "22015"),
            // The plain-number case reads the sign, so the widest negative
            // `i64` still parses there and the failure stays bad syntax.
            ("-9223372036854775808:00:00", "22007"),
            // An integer no wider type can hold overflows the number case too.
            ("-99999999999999999999999:00:00", "22015"),
            ("99999999999999999999999:00:00", "22015"),
        ];
        for (literal, sqlstate) in cases {
            let error = parse_interval(literal).expect_err(literal);
            assert!(error.sqlstate() == sqlstate, "{literal}: {error}");
        }

        // The widest interval either sign can spell is still accepted.
        let accepted = [
            ("-2562047788:00:54.775807", -i64::MAX),
            ("+2562047788:00:54.775807", i64::MAX),
        ];
        for (literal, micros) in accepted {
            let parsed = parse_interval(literal).expect(literal);
            assert!(
                parsed
                    == Interval {
                        months: 0,
                        days: 0,
                        micros
                    },
                "{literal}"
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
    /// closed at `24:00:00`, which [`PgTime`] holds, so the carry lands on the
    /// boundary. Its `timestamp` range reaches 294276 AD where jiff's stops at
    /// 9999, so a timestamp carries the date until the calendar runs out and
    /// then fails. Neither may wrap back to midnight of the day it started in,
    /// because that is a different instant.
    #[test]
    fn rounding_across_midnight_carries_the_date_and_never_wraps() {
        use assert2::assert;

        let tz = jiff::tz::TimeZone::UTC;

        // Below the carry, the value is untouched.
        assert!(time_to_text(parse_time("23:59:59.9999994").expect("time")) == "23:59:59.999999");
        // At and above it, the carry lands on `24:00:00`, never back on midnight.
        assert!(time_to_text(parse_time("23:59:59.9999995").expect("time")) == "24:00:00");
        assert!(time_to_text(parse_time("23:59:59.9999999").expect("time")) == "24:00:00");

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
        // `epoch` is the Unix instant for timestamptz, not local midnight in
        // the session zone. PostgreSQL displays that same instant in each zone.
        let epoch = parse_timestamptz("epoch", &tz).expect("epoch");
        assert_eq!(epoch, Timestamp::UNIX_EPOCH);
        assert_eq!(timestamptz_to_text(epoch, &tz), "1969-12-31 19:00:00-05");
    }

    #[test]
    fn timestamptz_range_is_checked_after_the_offset_is_applied() {
        let utc = TimeZone::UTC;
        let err = parse_timestamptz("4714-11-23 23:59:59+00 BC", &utc)
            .expect_err("one second before the finite timestamptz range");
        assert_eq!(err.sqlstate(), "22008");

        // The same instant is valid through a local clock reading one day before
        // the minimum civil date: the range belongs to the UTC instant.
        let first = parse_timestamptz("4714-11-23 16:00:00-08 BC", &utc).expect("first instant");
        assert_eq!(
            timestamptz_to_text(first, &utc),
            "4714-11-24 00:00:00+00 BC"
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

    #[test]
    fn mul_interval_micros_overflow_is_caught() {
        // The largest FINITE micros (`i64::MAX` itself is the `infinity`
        // encoding) times 1000 is ≈ 9.22e21, far above `i64::MAX`. The product
        // must be refused rather than saturated, and refused as an `interval`
        // overflow — `integer out of range` names a type that is not involved.
        let big = Interval {
            months: 0,
            days: 0,
            micros: i64::MAX - 1,
        };
        let refused = mul_interval(big, 1000.0).expect_err("9.22e21 µs is out of range");
        assert!(refused.to_string() == "interval out of range");
        assert!(refused.sqlstate() == "22008");
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
        assert!(inst("+05") == "2024-01-15 07:00:00+00");
        // +0530 (colon-less HHMM) → 06:30 UTC: run together, the last two digits
        // are the minutes.
        assert!(inst("+0530") == "2024-01-15 06:30:00+00");
        // There is no run-together HHMMSS spelling — PostgreSQL reads +053045 as
        // 5304 hours and rejects it. Seconds need the colons.
        let err = parse_timestamptz("2024-01-15 12:00:00+053045", &tz)
            .expect_err("no run-together HHMMSS offset");
        assert!(err.sqlstate() == "22009", "got {err}");
        // +05:30 (colon path) → 06:30 UTC.
        assert!(inst("+05:30") == "2024-01-15 06:30:00+00");
        // +05:30:45 (HH:MM:SS colon path) → 06:29:15 UTC.
        assert!(inst("+05:30:45") == "2024-01-15 06:29:15+00");
        // -08 → 20:00 UTC.
        assert!(inst("-08") == "2024-01-15 20:00:00+00");
        // Z → UTC, unchanged.
        assert!(inst("Z") == "2024-01-15 12:00:00+00");
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
mod make_justify_tests {
    #[test]
    fn make_constructors() {
        use super::{Interval, PgDate, make_date, make_interval, make_time, make_timestamp_civil};
        assert_eq!(
            make_date(2024, 7, 4).expect("d"),
            PgDate::Finite(jiff::civil::date(2024, 7, 4))
        );
        // make_time(hour, min, sec) — fractional seconds → micros.
        assert_eq!(
            make_time(13, 45, 6.5).expect("t"),
            super::PgTime::from(jiff::civil::time(13, 45, 6, 500_000_000))
        );
        // `24:00:00` is the one hour-24 reading PostgreSQL's `make_time` builds.
        {
            use assert2::assert;
            assert!(make_time(24, 0, 0.0).expect("t") == super::PgTime::END_OF_DAY);
        }
        assert!(make_time(24, 0, 1.0).is_err());
        assert!(make_time(24, 1, 0.0).is_err());
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
        // make_time(25, 0, 0) is out of range → 22008.
        assert_eq!(
            make_time(25, 0, 0.0).expect_err("hour 25").sqlstate(),
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

    #[test]
    fn temporal_typmods_round_half_away_from_zero() {
        use super::{
            Interval, apply_interval_typmod, apply_time_typmod, apply_timestamp_typmod,
            apply_timestamptz_typmod, parse_time, parse_timestamp, parse_timestamptz,
            time_to_text, timestamp_to_text, timestamptz_to_text,
        };
        use jiff::tz::TimeZone;

        let time = parse_time("12:34:56.500001").expect("time");
        assert_eq!(time_to_text(apply_time_typmod(time, Some(0)).expect("round")), "12:34:57");
        assert_eq!(
            time_to_text(apply_time_typmod(time, Some(2)).expect("round")),
            "12:34:56.5"
        );

        let timestamp = parse_timestamp("1999-12-31 23:59:59.500000").expect("timestamp");
        assert_eq!(
            timestamp_to_text(apply_timestamp_typmod(timestamp, Some(0)).expect("round")),
            "1999-12-31 23:59:59"
        );

        let timestamptz = parse_timestamptz("2000-01-01 00:00:00.500000", &TimeZone::UTC)
            .expect("timestamptz");
        assert_eq!(
            timestamptz_to_text(
                apply_timestamptz_typmod(timestamptz, Some(0)).expect("round"),
                &TimeZone::UTC,
            ),
            "2000-01-01 00:00:01+00"
        );

        assert_eq!(
            apply_interval_typmod(
                Interval {
                    months: 0,
                    days: 0,
                    micros: -1_500_000,
                },
                Some(0),
            )
            .expect("round")
            .micros,
            -2_000_000
        );
    }

    #[test]
    fn interval_field_ranges_discard_finer_fields() {
        use super::{Interval, IntervalField, parse_interval_ranged};

        assert_eq!(
            parse_interval_ranged(
                "1 day 02:03:04.5",
                Some((IntervalField::Day, IntervalField::Minute)),
            )
            .expect("minute range"),
            Interval {
                months: 0,
                days: 1,
                micros: 7_380_000_000,
            }
        );
        assert_eq!(
            parse_interval_ranged(
                "2 months 3 days 04:05:06",
                Some((IntervalField::Year, IntervalField::Month)),
            )
            .expect("month range"),
            Interval {
                months: 2,
                days: 0,
                micros: 0,
            }
        );
        assert_eq!(
            parse_interval_ranged(
                "2 days 03:04:05",
                Some((IntervalField::Year, IntervalField::Day)),
            )
            .expect("day range"),
            Interval {
                months: 0,
                days: 2,
                micros: 0,
            }
        );
    }
}
