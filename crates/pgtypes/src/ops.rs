//! Operator semantics matching PostgreSQL: integer type promotion, checked
//! overflow (22003), division by zero (22012), NULL propagation, and
//! three-valued boolean logic.

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible operator semantics kept structurally close to donor"
)]

use std::cmp::Ordering;

use crate::{Datum, TypeError, numeric::NumericValue};

/// Type an integer literal: narrowest of int4, then int8; overflow -> 22003.
pub fn int_literal(s: &str) -> Result<Datum, TypeError> {
    if let Ok(n) = s.parse::<i32>() {
        return Ok(Datum::Int4(n));
    }
    match s.parse::<i64>() {
        Ok(n) => Ok(Datum::Int8(n)),
        Err(_) => Err(TypeError::Overflow),
    }
}

/// SP30: type a decimal/exponent literal as `float8` (crabgresql has no `numeric`,
/// so a bare `1.5`/`1e3` is `double precision`, not `numeric`). A literal that
/// overflows to infinity (e.g. `1e400`) is out of range (22003).
pub fn float_literal(s: &str) -> Result<Datum, TypeError> {
    match s.parse::<f64>() {
        Ok(v) if v.is_infinite() => Err(TypeError::Overflow),
        Ok(v) => Ok(Datum::Float8(v)),
        Err(_) => Err(TypeError::InvalidText {
            type_name: "double precision",
            value: s.to_string(),
        }),
    }
}

/// Promote an integer Datum to i32. `Some` only for the two widths PostgreSQL's
/// `int4` operators accept after the implicit `int2 → int4` cast, so an `int8`
/// operand falls through to the i64 rung.
fn as_i32(d: &Datum) -> Option<i32> {
    match d {
        Datum::Int2(n) => Some(i32::from(*n)),
        Datum::Int4(n) => Some(*n),
        _ => None,
    }
}

/// Promote an integer Datum to i64 for mixed-width arithmetic.
fn as_i64(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int2(n) => Some(i64::from(*n)),
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        // `regclass` is binary-coercible to `oid`, so it compares and orders as
        // its oid — `WHERE conrelid = 'pp'::regclass` is an integer comparison.
        Datum::Regclass(r) => Some(i64::from(r.oid)),
        _ => None,
    }
}

/// Promote a numeric Datum (int, numeric, or float) to f64 for mixed-type
/// arithmetic. (SP32: a `numeric` operand mixed with a `float8` promotes to
/// `float8`, since `float8` is the preferred type — `numeric ⊕ float8 → float8`.)
fn as_f64(d: &Datum) -> Option<f64> {
    match d {
        Datum::Int2(n) => Some(f64::from(*n)),
        Datum::Int4(n) => Some(f64::from(*n)),
        Datum::Int8(n) => Some(*n as f64),
        // `float4 → float8` is exact, so widening here never changes an ordering
        // or a sum that PostgreSQL would compute the same way.
        Datum::Float4(f) => Some(f64::from(*f)),
        Datum::Float8(f) => Some(*f),
        Datum::Numeric(d) => Some(crate::numeric::to_f64(d)),
        _ => None,
    }
}

/// SP32: promote an int/`numeric` Datum to a [`NumericValue`] (used when an
/// operand is `numeric` but neither is `float8`).
fn as_numeric(d: &Datum) -> Option<NumericValue> {
    match d {
        Datum::Int2(n) => Some(NumericValue::from(*n)),
        Datum::Int4(n) => Some(NumericValue::from(*n)),
        Datum::Int8(n) => Some(NumericValue::from(*n)),
        Datum::Numeric(d) => Some(d.clone()),
        _ => None,
    }
}

fn is_float(d: &Datum) -> bool {
    matches!(d, Datum::Float4(_) | Datum::Float8(_))
}

fn is_numeric(d: &Datum) -> bool {
    matches!(d, Datum::Numeric(_))
}

/// True if this Datum is a temporal (date/time/interval) value.  Used to
/// detect temporal operands early in `add`/`sub`/`mul`/`div`/`compare` so
/// they are handled before the numeric fast-paths.
fn is_temporal(d: &Datum) -> bool {
    matches!(
        d,
        Datum::Date(_)
            | Datum::Time(_)
            | Datum::Timestamp(_)
            | Datum::Timestamptz(_)
            | Datum::Interval(_)
    )
}

/// Convert a numeric Datum to f64 for use as an interval scalar factor.
/// Returns None for non-numeric types (temporal, text, bool, null).
fn numeric_as_f64(d: &Datum) -> Option<f64> {
    as_f64(d)
}

/// Apply a float op with PostgreSQL's finite-overflow rule: a `finite ⊕ finite`
/// result that becomes infinite is out of range (22003); an infinite *operand*
/// just propagates Infinity (no error). Underflow to 0 is silent, as in PG.
fn float_arith(x: f64, y: f64, op: fn(f64, f64) -> f64) -> Result<Datum, TypeError> {
    let r = op(x, y);
    if r.is_infinite() && x.is_finite() && y.is_finite() {
        return Err(TypeError::Overflow);
    }
    Ok(Datum::Float8(r))
}

/// [`float_arith`] one width down. `float4 ⊕ float4` is computed in `f32`, not
/// in `f64` and rounded, because that is what `float4pl` and friends do.
fn float4_arith(x: f32, y: f32, op: fn(f32, f32) -> f32) -> Result<Datum, TypeError> {
    let r = op(x, y);
    if r.is_infinite() && x.is_finite() && y.is_finite() {
        return Err(TypeError::float_overflow());
    }
    Ok(Datum::Float4(r))
}

/// One arithmetic operator, spelled once per numeric width.
///
/// PostgreSQL has a separate function per type (`int2pl`, `int4pl`, `int8pl`,
/// `float4pl`, `float8pl`, `numeric_add`) and the *result type follows the
/// operand widths*, so the dispatcher needs every width's version at once
/// rather than one widened implementation.
#[derive(Clone, Copy)]
struct NumericOp {
    i2: fn(i16, i16) -> Option<i16>,
    i4: fn(i32, i32) -> Option<i32>,
    i8: fn(i64, i64) -> Option<i64>,
    f4: fn(f32, f32) -> f32,
    f8: fn(f64, f64) -> f64,
    num: fn(&NumericValue, &NumericValue) -> NumericValue,
}

/// PostgreSQL's numeric promotion ladder, most specific rung first:
/// `float4 ⊕ float4 → float4`, any other float pairing → `float8` (there is no
/// `float4 ⊕ int` operator, so both sides implicitly widen to the preferred
/// type), then `numeric`, then `int2`/`int4`/`int8` by operand width.
fn arith(a: &Datum, b: &Datum, op: NumericOp) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    if let (Datum::Float4(x), Datum::Float4(y)) = (a, b) {
        return float4_arith(*x, *y, op.f4);
    }
    // SP30: if either operand is float, promote both to f64 (float8 is the
    // preferred numeric type, so it wins over numeric and int).
    if is_float(a) || is_float(b) {
        return match (as_f64(a), as_f64(b)) {
            (Some(x), Some(y)) => float_arith(x, y, op.f8),
            _ => Err(TypeError::TypeMismatch {
                message: "operator requires numeric operands".into(),
            }),
        };
    }
    // SP32: else if either operand is numeric, promote both to numeric.
    if is_numeric(a) || is_numeric(b) {
        return match (as_numeric(a), as_numeric(b)) {
            (Some(x), Some(y)) => Ok(Datum::Numeric((op.num)(&x, &y))),
            _ => Err(TypeError::TypeMismatch {
                message: "operator requires numeric operands".into(),
            }),
        };
    }
    match (a, b) {
        (Datum::Int2(x), Datum::Int2(y)) => (op.i2)(*x, *y)
            .map(Datum::Int2)
            .ok_or_else(|| TypeError::out_of_range_for("smallint")),
        _ => match (as_i32(a), as_i32(b)) {
            (Some(x), Some(y)) => (op.i4)(x, y).map(Datum::Int4).ok_or(TypeError::Overflow),
            _ => match (as_i64(a), as_i64(b)) {
                (Some(x), Some(y)) => (op.i8)(x, y).map(Datum::Int8).ok_or(TypeError::Overflow),
                _ => Err(TypeError::TypeMismatch {
                    message: "operator requires integer operands".into(),
                }),
            },
        },
    }
}

// ---------------------------------------------------------------------------
// Temporal arithmetic dispatch helpers
// ---------------------------------------------------------------------------

/// Which side of `time ± interval` a rejected infinite interval was on, so the
/// message names the operation the way PostgreSQL's does.
#[derive(Clone, Copy)]
enum TimeShift {
    Add,
    Subtract,
}

/// `time` and `timetz` have no representation for infinity, and shifting them by
/// an infinite interval would silently wrap the clock instead, so PostgreSQL
/// refuses with 22008 rather than answering.
fn reject_infinite_interval_on_time(
    iv: crate::datetime::Interval,
    shift: TimeShift,
) -> Result<(), TypeError> {
    if !iv.is_infinite() {
        return Ok(());
    }
    Err(TypeError::DatetimeOutOfRange {
        message: match shift {
            TimeShift::Add => "cannot add infinite interval to time".into(),
            TimeShift::Subtract => "cannot subtract infinite interval from time".into(),
        },
    })
}

/// `add` for temporal operand pairs. Called when at least one operand is
/// temporal. `Timestamptz` operands fall through to `TypeMismatch` (deferred;
/// needs the session tz, which is only available in the executor).
fn temporal_add(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    use crate::datetime::{
        add_interval, combine_date_time, date_plus_days, date_plus_interval, time_plus_interval,
        timestamp_plus_interval,
    };
    match (a, b) {
        // date + int4 / int8 → date (add days)
        (Datum::Date(d), Datum::Int4(n)) => date_plus_days(*d, i64::from(*n)).map(Datum::Date),
        (Datum::Date(d), Datum::Int8(n)) => date_plus_days(*d, *n).map(Datum::Date),
        // int4 / int8 + date → date
        (Datum::Int4(n), Datum::Date(d)) => date_plus_days(*d, i64::from(*n)).map(Datum::Date),
        (Datum::Int8(n), Datum::Date(d)) => date_plus_days(*d, *n).map(Datum::Date),
        // date + interval → timestamp
        (Datum::Date(d), Datum::Interval(iv)) => date_plus_interval(*d, *iv).map(Datum::Timestamp),
        (Datum::Interval(iv), Datum::Date(d)) => date_plus_interval(*d, *iv).map(Datum::Timestamp),
        // date + time / time + date → timestamp (combine the calendar date and
        // the wall-clock time; the time's days/months are irrelevant — a Time has
        // no date component).
        (Datum::Date(d), Datum::Time(t)) => Ok(Datum::Timestamp(combine_date_time(*d, *t))),
        (Datum::Time(t), Datum::Date(d)) => Ok(Datum::Timestamp(combine_date_time(*d, *t))),
        // time + interval / interval + time → time (uses ONLY the interval micros,
        // wrapping mod 24 h; the interval's days/months are ignored — a Time has no
        // date).
        (Datum::Time(t), Datum::Interval(iv)) | (Datum::Interval(iv), Datum::Time(t)) => {
            reject_infinite_interval_on_time(*iv, TimeShift::Add)?;
            Ok(Datum::Time(time_plus_interval(*t, *iv)))
        }
        // `timetz + interval` shifts the clock and keeps the offset, so the
        // result names the same zone the operand did.
        (Datum::Timetz(t), Datum::Interval(iv)) | (Datum::Interval(iv), Datum::Timetz(t)) => {
            reject_infinite_interval_on_time(*iv, TimeShift::Add)?;
            Ok(Datum::Timetz(crate::datetime::TimeTz {
                time: time_plus_interval(t.time, *iv),
                offset: t.offset,
            }))
        }
        // timestamp + interval → timestamp
        (Datum::Timestamp(ts), Datum::Interval(iv)) => {
            timestamp_plus_interval(*ts, *iv).map(Datum::Timestamp)
        }
        (Datum::Interval(iv), Datum::Timestamp(ts)) => {
            timestamp_plus_interval(*ts, *iv).map(Datum::Timestamp)
        }
        // interval + interval → interval
        (Datum::Interval(x), Datum::Interval(y)) => add_interval(*x, *y).map(Datum::Interval),
        // Everything else (including Timestamptz, which is tz-aware and handled in
        // the executor's `apply_binary` where the session zone is available) is a
        // type mismatch.
        _ => Err(TypeError::TypeMismatch {
            message: "operator does not exist for these temporal types".into(),
        }),
    }
}

/// `sub` for temporal operand pairs.
fn temporal_sub(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    use crate::datetime::{
        date_diff_days, date_plus_days, neg_interval, sub_interval, timestamp_diff,
        timestamp_plus_interval,
    };
    match (a, b) {
        // date - int4 / int8 → date (subtract days)
        (Datum::Date(d), Datum::Int4(n)) => {
            date_plus_days(*d, i64::from(n.checked_neg().ok_or(TypeError::Overflow)?))
                .map(Datum::Date)
        }
        (Datum::Date(d), Datum::Int8(n)) => {
            date_plus_days(*d, n.checked_neg().ok_or(TypeError::Overflow)?).map(Datum::Date)
        }
        // date - date → int4 (number of days)
        (Datum::Date(a), Datum::Date(b)) => date_diff_days(*a, *b).map(Datum::Int4),
        // date - interval → timestamp (negate interval, then add)
        (Datum::Date(d), Datum::Interval(iv)) => {
            let neg = neg_interval(*iv)?;
            crate::datetime::date_plus_interval(*d, neg).map(Datum::Timestamp)
        }
        // time - interval → time (negate the interval, then add — only the micros
        // matter; the result wraps mod 24 h).
        (Datum::Time(t), Datum::Interval(iv)) => {
            reject_infinite_interval_on_time(*iv, TimeShift::Subtract)?;
            let neg = neg_interval(*iv)?;
            Ok(Datum::Time(crate::datetime::time_plus_interval(*t, neg)))
        }
        (Datum::Timetz(t), Datum::Interval(iv)) => {
            reject_infinite_interval_on_time(*iv, TimeShift::Subtract)?;
            let neg = neg_interval(*iv)?;
            Ok(Datum::Timetz(crate::datetime::TimeTz {
                time: crate::datetime::time_plus_interval(t.time, neg),
                offset: t.offset,
            }))
        }
        // timestamp - interval → timestamp
        (Datum::Timestamp(ts), Datum::Interval(iv)) => {
            let neg = neg_interval(*iv)?;
            timestamp_plus_interval(*ts, neg).map(Datum::Timestamp)
        }
        // timestamp - timestamp → interval
        (Datum::Timestamp(a), Datum::Timestamp(b)) => timestamp_diff(*a, *b).map(Datum::Interval),
        // interval - interval → interval
        (Datum::Interval(x), Datum::Interval(y)) => sub_interval(*x, *y).map(Datum::Interval),
        // Everything else (including Timestamptz, which is tz-aware and handled in
        // the executor's `apply_binary`) is a type mismatch.
        _ => Err(TypeError::TypeMismatch {
            message: "operator does not exist for these temporal types".into(),
        }),
    }
}

/// `mul` for temporal operand pairs: only `interval * number` and
/// `number * interval` are defined.
fn temporal_mul(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    use crate::datetime::mul_interval;
    match (a, b) {
        (Datum::Interval(iv), num) => {
            let f = numeric_as_f64(num).ok_or_else(|| TypeError::TypeMismatch {
                message: "interval multiplier must be numeric".into(),
            })?;
            mul_interval(*iv, f).map(Datum::Interval)
        }
        (num, Datum::Interval(iv)) => {
            let f = numeric_as_f64(num).ok_or_else(|| TypeError::TypeMismatch {
                message: "interval multiplier must be numeric".into(),
            })?;
            mul_interval(*iv, f).map(Datum::Interval)
        }
        _ => Err(TypeError::TypeMismatch {
            message: "operator does not exist for these temporal types".into(),
        }),
    }
}

/// `div` for temporal operand pairs: only `interval / number` is defined.
fn temporal_div(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    use crate::datetime::div_interval;
    match (a, b) {
        (Datum::Interval(iv), num) => {
            let f = numeric_as_f64(num).ok_or_else(|| TypeError::TypeMismatch {
                message: "interval divisor must be numeric".into(),
            })?;
            div_interval(*iv, f).map(Datum::Interval)
        }
        _ => Err(TypeError::TypeMismatch {
            message: "operator does not exist for these temporal types".into(),
        }),
    }
}

/// `int2` widens to `int4` before the temporal matrix is consulted: PostgreSQL
/// resolves `date + int2` through the implicit `int2 → int4` cast, so the
/// matrix itself only carries `int4`/`int8` arms.
fn temporal_operand(d: &Datum) -> std::borrow::Cow<'_, Datum> {
    match d {
        Datum::Int2(n) => std::borrow::Cow::Owned(Datum::Int4(i32::from(*n))),
        _ => std::borrow::Cow::Borrowed(d),
    }
}

pub fn add(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    // Temporal dispatch — handled before the numeric fast-paths.
    if is_temporal(a) || is_temporal(b) {
        return temporal_add(&temporal_operand(a), &temporal_operand(b));
    }
    arith(
        a,
        b,
        NumericOp {
            i2: i16::checked_add,
            i4: i32::checked_add,
            i8: i64::checked_add,
            f4: |x, y| x + y,
            f8: |x, y| x + y,
            num: crate::numeric::add,
        },
    )
}
pub fn sub(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    // Temporal dispatch — handled before the numeric fast-paths.
    if is_temporal(a) || is_temporal(b) {
        return temporal_sub(&temporal_operand(a), &temporal_operand(b));
    }
    arith(
        a,
        b,
        NumericOp {
            i2: i16::checked_sub,
            i4: i32::checked_sub,
            i8: i64::checked_sub,
            f4: |x, y| x - y,
            f8: |x, y| x - y,
            num: crate::numeric::sub,
        },
    )
}
pub fn mul(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    // Temporal dispatch: interval * number or number * interval.
    if is_temporal(a) || is_temporal(b) {
        return temporal_mul(&temporal_operand(a), &temporal_operand(b));
    }
    arith(
        a,
        b,
        NumericOp {
            i2: i16::checked_mul,
            i4: i32::checked_mul,
            i8: i64::checked_mul,
            f4: |x, y| x * y,
            f8: |x, y| x * y,
            num: crate::numeric::mul,
        },
    )
}
pub fn div(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    // Temporal dispatch: interval / number.
    if is_temporal(a) || is_temporal(b) {
        return temporal_div(&temporal_operand(a), &temporal_operand(b));
    }
    // `float4 / float4` stays single-precision; a zero divisor is 22012 here
    // exactly as it is one rung down.
    if let (Datum::Float4(x), Datum::Float4(y)) = (a, b) {
        if *y == 0.0 {
            return Err(TypeError::DivisionByZero);
        }
        return float4_arith(*x, *y, |x, y| x / y);
    }
    // SP30: float division — a zero divisor (incl. `-0.0`) is 22012, like PG.
    if is_float(a) || is_float(b) {
        let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) else {
            return Err(TypeError::TypeMismatch {
                message: "operator requires numeric operands".into(),
            });
        };
        if y == 0.0 {
            return Err(TypeError::DivisionByZero);
        }
        return float_arith(x, y, |x, y| x / y);
    }
    // SP32: numeric division uses PostgreSQL's display-scale rule (a zero divisor
    // is 22012, handled inside `numeric::div`).
    if is_numeric(a) || is_numeric(b) {
        let (Some(x), Some(y)) = (as_numeric(a), as_numeric(b)) else {
            return Err(TypeError::TypeMismatch {
                message: "operator requires numeric operands".into(),
            });
        };
        return crate::numeric::div(&x, &y).map(Datum::Numeric);
    }
    if matches!(b, Datum::Int2(0) | Datum::Int4(0) | Datum::Int8(0)) {
        return Err(TypeError::DivisionByZero);
    }
    // Only integer operands reach here (float/numeric returned above), so the
    // float/numeric `op` members of `NumericOp` are never exercised on this path.
    arith(
        a,
        b,
        NumericOp {
            i2: i16::checked_div,
            i4: i32::checked_div,
            i8: i64::checked_div,
            f4: |x, y| x / y,
            f8: |x, y| x / y,
            num: |_, _| unreachable!("numeric division is handled before arith"),
        },
    )
}

/// SQL `mod(a, b)` / the `%` remainder (SP29, exposed as the `mod` function).
/// NULL propagates; a zero divisor is 22012; otherwise the remainder takes the
/// sign of the dividend (truncated division, like PostgreSQL). `wrapping_rem`
/// makes `i32::MIN % -1` the mathematically-correct `0` rather than an overflow
/// trap, so a remainder never raises 22003.
pub fn rem(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    // SP32: numeric `mod` (a zero divisor is 22012, handled in `numeric::rem`).
    if is_numeric(a) || is_numeric(b) {
        let (Some(x), Some(y)) = (as_numeric(a), as_numeric(b)) else {
            return Err(TypeError::TypeMismatch {
                message: "mod requires numeric operands".into(),
            });
        };
        return crate::numeric::rem(&x, &y).map(Datum::Numeric);
    }
    if matches!(b, Datum::Int2(0) | Datum::Int4(0) | Datum::Int8(0)) {
        return Err(TypeError::DivisionByZero);
    }
    match (a, b) {
        (Datum::Int2(x), Datum::Int2(y)) => Ok(Datum::Int2(x.wrapping_rem(*y))),
        _ => match (as_i32(a), as_i32(b)) {
            (Some(x), Some(y)) => Ok(Datum::Int4(x.wrapping_rem(y))),
            _ => match (as_i64(a), as_i64(b)) {
                (Some(x), Some(y)) => Ok(Datum::Int8(x.wrapping_rem(y))),
                _ => Err(TypeError::TypeMismatch {
                    message: "mod requires integer operands".into(),
                }),
            },
        },
    }
}

/// SQL `||` string concatenation (SP29). A NULL operand yields NULL; otherwise
/// each operand is rendered via its canonical text encoding (the same encoding
/// the wire layer uses — `true`→`t`, `5`→`5`) and the two are joined into a
/// `text`. The "at least one operand must be text" operator-resolution rule is a
/// static (plan-time) concern enforced by the executor's `infer_type`; this
/// value-level op is permissive so a `||` reached at runtime always has a result.
///
/// `style` carries the session zone plus the `DateStyle`/`IntervalStyle` GUCs
/// the temporal output functions read; every other type ignores it.
pub fn concat(
    a: &Datum,
    b: &Datum,
    style: crate::encoding::OutputStyle<'_>,
) -> Result<Datum, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    let mut s = text_of(a, style);
    s.push_str(&text_of(b, style));
    Ok(Datum::Text(s))
}

/// The text `||` renders a non-NULL Datum as, reusing the wire text encoder so
/// the operator and the DataRow encoding never disagree.
///
/// `boolean` is the one exception, and it is PostgreSQL's own: `||` resolves
/// through the `text` cast (`booltext`, which spells `true`/`false`) rather than
/// the output function (`boolout`, which spells `t`/`f`), so `'flag=' || true`
/// is `flag=true` even though `SELECT true` prints `t`. `concat()` keeps the
/// output function and therefore keeps `t`.
fn text_of(d: &Datum, style: crate::encoding::OutputStyle<'_>) -> String {
    if let Datum::Bool(b) = d {
        return if *b { "true".into() } else { "false".into() };
    }
    String::from_utf8(crate::encoding::encode_text_in(d, style))
        .expect("a Datum's text encoding is always valid UTF-8")
}

/// SQL comparison. Returns Ok(None) if either operand is NULL (so the caller
/// yields NULL / excludes the row). Cross-type integer comparison is allowed;
/// text compares lexicographically; bool compares false < true.
pub fn compare(a: &Datum, b: &Datum) -> Result<Option<Ordering>, TypeError> {
    if a.is_null() || b.is_null() {
        return Ok(None);
    }
    let ord = match (a, b) {
        (Datum::Text(x), Datum::Text(y)) => x.cmp(y),
        (Datum::Bool(x), Datum::Bool(y)) => x.cmp(y),
        // PostgreSQL's `byteacmp`: memcmp over the common prefix, then length.
        // That is exactly `Ord` for a byte slice. Without this arm `ORDER BY`,
        // `min`/`max` and `DISTINCT` over a bytea column all raise 42804.
        (Datum::Bytea(x), Datum::Bytea(y)) => x.cmp(y),
        // Temporal comparisons (same-type + date↔timestamp promotion).
        (Datum::Date(x), Datum::Date(y)) => x.cmp(y),
        (Datum::Time(x), Datum::Time(y)) => x.cmp(y),
        // `timetz` orders by the UTC-equivalent instant, not the printed clock,
        // so `12:00-05` sorts after `16:00+00`.
        (Datum::Timetz(x), Datum::Timetz(y)) => x.cmp(y),
        (Datum::Timestamp(x), Datum::Timestamp(y)) => x.cmp(y),
        // SP37: timestamptz comparison — absolute instant order (UTC µs).
        (Datum::Timestamptz(x), Datum::Timestamptz(y)) => x.cmp(y),
        (Datum::Interval(x), Datum::Interval(y)) => x.cmp(y),
        // date ↔ timestamp: promote the date to midnight and compare.
        (Datum::Date(d), Datum::Timestamp(ts)) => crate::datetime::date_to_midnight(*d).cmp(ts),
        (Datum::Timestamp(ts), Datum::Date(d)) => ts.cmp(&crate::datetime::date_to_midnight(*d)),
        // jsonb btree order (Object > Array > Bool > Number > String > Null).
        // Placed before the numeric fall-throughs, which would otherwise try to
        // promote these to f64 and fail.
        (Datum::Jsonb(x), Datum::Jsonb(y)) => x.cmp(y),
        (Datum::TsVector(x), Datum::TsVector(y)) => x.cmp(y),
        (Datum::TsQuery(x), Datum::TsQuery(y)) => x.cmp(y),
        // SQL arrays compare element-wise, shorter first on a common prefix.
        (Datum::Array(x), Datum::Array(y)) => compare_arrays(x, y)?,
        // `record_cmp`: field by field, left to right.
        (Datum::Record(x), Datum::Record(y)) => compare_records(x, y)?,
        // An enum orders by its labels' declared positions, which is what
        // `pg_enum.enumsortorder` records.
        (Datum::Enum(x), Datum::Enum(y)) => compare_enums(x, y)?,
        (Datum::Range(x), Datum::Range(y)) => compare_ranges(x, y)?,
        (Datum::Multirange(x), Datum::Multirange(y)) => compare_multiranges(x, y)?,
        // SP30: any numeric pair with a float promotes to float comparison (NaN is
        // the largest value and equals itself; `-0.0 == +0.0` — PG's float ordering).
        _ if is_float(a) || is_float(b) => match (as_f64(a), as_f64(b)) {
            (Some(x), Some(y)) => float_cmp(x, y),
            _ => return Err(cannot_compare(a, b)),
        },
        // SP32: a numeric pair (no float) compares exactly, by value (ignoring scale).
        _ if is_numeric(a) || is_numeric(b) => match (as_numeric(a), as_numeric(b)) {
            (Some(x), Some(y)) => x.cmp(&y),
            _ => return Err(cannot_compare(a, b)),
        },
        _ => match (as_i64(a), as_i64(b)) {
            (Some(x), Some(y)) => x.cmp(&y),
            _ => return Err(cannot_compare(a, b)),
        },
    };
    Ok(Some(ord))
}

fn compare_ranges(a: &crate::RangeValue, b: &crate::RangeValue) -> Result<Ordering, TypeError> {
    if a.ty != b.ty {
        return Err(TypeError::TypeMismatch {
            message: "cannot compare ranges of different types".into(),
        });
    }
    match (a.empty, b.empty) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => return Ok(Ordering::Less),
        (false, true) => return Ok(Ordering::Greater),
        (false, false) => {}
    }
    let lower = compare_range_bounds(
        a.lower.as_deref(),
        a.lower_inclusive,
        b.lower.as_deref(),
        b.lower_inclusive,
        false,
    )?;
    if lower != Ordering::Equal {
        return Ok(lower);
    }
    compare_range_bounds(
        a.upper.as_deref(),
        a.upper_inclusive,
        b.upper.as_deref(),
        b.upper_inclusive,
        true,
    )
}

fn compare_multiranges(
    a: &crate::MultirangeValue,
    b: &crate::MultirangeValue,
) -> Result<Ordering, TypeError> {
    if a.ty != b.ty {
        return Err(TypeError::TypeMismatch {
            message: "cannot compare multiranges of different types".into(),
        });
    }
    for (left, right) in a.ranges.iter().zip(&b.ranges) {
        let order = compare_ranges(left, right)?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(a.ranges.len().cmp(&b.ranges.len()))
}

fn compare_range_bounds(
    a: Option<&Datum>,
    a_inclusive: bool,
    b: Option<&Datum>,
    b_inclusive: bool,
    upper: bool,
) -> Result<Ordering, TypeError> {
    match (a, b) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(if upper {
            Ordering::Greater
        } else {
            Ordering::Less
        }),
        (Some(_), None) => Ok(if upper {
            Ordering::Less
        } else {
            Ordering::Greater
        }),
        (Some(a), Some(b)) => match compare(a, b)?.expect("finite range bounds are non-null") {
            Ordering::Equal => Ok(match (a_inclusive, b_inclusive) {
                (true, false) => {
                    if upper {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (false, true) => {
                    if upper {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                _ => Ordering::Equal,
            }),
            ordering => Ok(ordering),
        },
    }
}

/// PostgreSQL's array btree order (`array_cmp`): element-wise over the common
/// prefix, then the shorter array first, then fewer dimensions first, then the
/// dimension lengths, then the lower bounds. A NULL element sorts greater than
/// any non-NULL one (PostgreSQL's `btarraycmp` treats NULLs as largest), and two
/// NULLs are equal — the *comparison* is never NULL, unlike a scalar `=`.
fn compare_arrays(
    a: &crate::datum::ArrayValue,
    b: &crate::datum::ArrayValue,
) -> Result<Ordering, TypeError> {
    for (x, y) in a.elems.iter().zip(b.elems.iter()) {
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => compare(x, y)?.expect("non-NULL operands compare"),
        };
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    let by_shape = a
        .elems
        .len()
        .cmp(&b.elems.len())
        .then_with(|| a.dims.len().cmp(&b.dims.len()))
        .then_with(|| {
            a.dims
                .iter()
                .map(|d| d.len)
                .cmp(b.dims.iter().map(|d| d.len))
        })
        .then_with(|| {
            a.dims
                .iter()
                .map(|d| d.lower)
                .cmp(b.dims.iter().map(|d| d.lower))
        });
    Ok(by_shape)
}

/// PostgreSQL's `record_cmp`: field by field, left to right, with a NULL field
/// sorting after every non-NULL one and two NULLs equal. Unlike the *row
/// comparison operator* (`ROW(1,NULL) < ROW(1,2)`, which is NULL), comparing two
/// composite **values** never yields NULL — `ROW(1,NULL)::t < ROW(1,'a')::t` is
/// `false` on PostgreSQL 18.4, and `ORDER BY` over a composite column puts the
/// NULL-field row last. A record with fewer fields sorts first on a common
/// prefix.
fn compare_records(
    a: &crate::datum::RecordValue,
    b: &crate::datum::RecordValue,
) -> Result<Ordering, TypeError> {
    for (x, y) in a.values.iter().zip(b.values.iter()) {
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => compare(x, y)?.expect("non-NULL operands compare"),
        };
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(a.values.len().cmp(&b.values.len()))
}

/// Enum ordering: the labels' positions in the type's declared label list,
/// which is what `pg_enum.enumsortorder` holds. Two values of *different* enum
/// types have no comparison operator, matching PostgreSQL's 42883.
fn compare_enums(
    a: &crate::datum::EnumValue,
    b: &crate::datum::EnumValue,
) -> Result<Ordering, TypeError> {
    if a.ty != b.ty {
        return Err(TypeError::TypeMismatch {
            message: format!("operator does not exist: {} = {}", a.ty.name, b.ty.name),
        });
    }
    match (a.sort_order(), b.sort_order()) {
        (Some(x), Some(y)) => Ok(x.cmp(&y)),
        // A label that is no longer in the type has no sort order; comparing it
        // would invent an answer, so refuse instead.
        _ => Err(TypeError::Coded {
            sqlstate: "22P02",
            message: format!(
                "invalid input value for enum {}: \"{}\"",
                a.ty.name,
                if a.sort_order().is_none() {
                    &a.label
                } else {
                    &b.label
                }
            ),
        }),
    }
}

/// PostgreSQL's `float8` total order: NaN sorts greater than every non-NaN and is
/// equal to itself; `-0.0` and `+0.0` are equal.
fn float_cmp(x: f64, y: f64) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => x
            .partial_cmp(&y)
            .expect("non-NaN floats are totally ordered"),
    }
}

fn cannot_compare(a: &Datum, b: &Datum) -> TypeError {
    TypeError::TypeMismatch {
        message: format!(
            "cannot compare {} and {}",
            a.column_type().map(|t| t.name()).unwrap_or("unknown"),
            b.column_type().map(|t| t.name()).unwrap_or("unknown"),
        ),
    }
}

fn as_bool(d: &Datum) -> Result<Option<bool>, TypeError> {
    match d {
        Datum::Null => Ok(None),
        Datum::Bool(b) => Ok(Some(*b)),
        _ => Err(TypeError::TypeMismatch {
            message: "argument of boolean operator must be boolean".into(),
        }),
    }
}

/// Three-valued AND: NULL AND false = false, else NULL propagates.
pub fn and(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    let (x, y) = (as_bool(a)?, as_bool(b)?);
    Ok(match (x, y) {
        (Some(false), _) | (_, Some(false)) => Datum::Bool(false),
        (Some(true), Some(true)) => Datum::Bool(true),
        _ => Datum::Null,
    })
}

/// Three-valued OR: NULL OR true = true, else NULL propagates.
pub fn or(a: &Datum, b: &Datum) -> Result<Datum, TypeError> {
    let (x, y) = (as_bool(a)?, as_bool(b)?);
    Ok(match (x, y) {
        (Some(true), _) | (_, Some(true)) => Datum::Bool(true),
        (Some(false), Some(false)) => Datum::Bool(false),
        _ => Datum::Null,
    })
}

pub fn not(a: &Datum) -> Result<Datum, TypeError> {
    Ok(match as_bool(a)? {
        Some(b) => Datum::Bool(!b),
        None => Datum::Null,
    })
}

/// Build a Bool Datum from a comparison result and the operator.
pub fn cmp_to_bool(op_holds: bool, ord: Option<Ordering>) -> Datum {
    match ord {
        None => Datum::Null,
        Some(_) => Datum::Bool(op_holds),
    }
}

#[cfg(test)]
mod tests {
    /// `time` and `timetz` have no infinity, so PostgreSQL refuses to shift one
    /// by an infinite interval (22008) rather than wrapping the clock by the
    /// sentinel's microseconds — which is what silently produced a wrong
    /// time-of-day before.
    #[test]
    fn shifting_a_time_by_an_infinite_interval_is_22008() {
        use assert2::assert;

        use super::{Datum, add, sub};

        let time = Datum::Time(crate::datetime::parse_time("11:27:42").expect("time"));
        let timetz = Datum::Timetz(crate::datetime::TimeTz {
            time: crate::datetime::parse_time("11:27:42").expect("time"),
            offset: jiff::tz::Offset::UTC,
        });
        let inf = Datum::Interval(crate::datetime::Interval::INFINITY);
        let neg_inf = Datum::Interval(crate::datetime::Interval::NEG_INFINITY);
        let cases: [(&Datum, &Datum, bool, &str); 6] = [
            (&time, &inf, true, "cannot add infinite interval to time"),
            (
                &time,
                &neg_inf,
                true,
                "cannot add infinite interval to time",
            ),
            (&inf, &time, true, "cannot add infinite interval to time"),
            (
                &time,
                &inf,
                false,
                "cannot subtract infinite interval from time",
            ),
            (&timetz, &inf, true, "cannot add infinite interval to time"),
            (
                &timetz,
                &neg_inf,
                false,
                "cannot subtract infinite interval from time",
            ),
        ];
        for (left, right, adding, message) in cases {
            let error = if adding {
                add(left, right).expect_err("refused")
            } else {
                sub(left, right).expect_err("refused")
            };
            assert!(error.sqlstate() == "22008", "{message}");
            assert!(error.to_string() == message);
        }
        // A finite interval still shifts the clock.
        let hour = Datum::Interval(crate::datetime::Interval {
            months: 0,
            days: 0,
            micros: 3_600_000_000,
        });
        assert!(
            add(&time, &hour).expect("finite shift")
                == Datum::Time(crate::datetime::parse_time("12:27:42").expect("time"))
        );
    }

    /// `ORDER BY`, `min`/`max` and `DISTINCT` over a bytea column all route
    /// through `compare`; without a bytea arm they raise 42804. PostgreSQL's
    /// `byteacmp` is bytewise over the common prefix, then length.
    #[test]
    fn bytea_compares_bytewise_then_by_length() {
        let b = |bytes: &[u8]| Datum::Bytea(bytes.to_vec());
        for (left, right, want) in [
            (b(&[1, 2]), b(&[1, 2]), Some(Ordering::Equal)),
            (b(&[1, 2]), b(&[1, 3]), Some(Ordering::Less)),
            (b(&[1, 3]), b(&[1, 2]), Some(Ordering::Greater)),
            // A prefix sorts before the longer value.
            (b(&[1, 2]), b(&[1, 2, 0]), Some(Ordering::Less)),
            (b(&[]), b(&[0]), Some(Ordering::Less)),
            // High bytes are unsigned, so 0x80 sorts above 0x7f.
            (b(&[0x7f]), b(&[0x80]), Some(Ordering::Less)),
            (Datum::Null, b(&[1]), None),
        ] {
            let got = compare(&left, &right).expect("bytea comparison is defined");
            assert!(got == want, "{left:?} vs {right:?}");
        }
    }

    use std::cmp::Ordering;

    use super::*;
    use crate::{Datum, TypeError};

    #[test]
    fn datetime_arithmetic_matrix() {
        use crate::datetime::Interval;
        let d = |s: &str| Datum::Date(crate::datetime::parse_date(s).expect("date"));
        let iv = |m, days, us| {
            Datum::Interval(Interval {
                months: m,
                days,
                micros: us,
            })
        };
        assert_eq!(
            add(&d("2024-01-01"), &Datum::Int4(31)).expect("d+i"),
            d("2024-02-01")
        );
        assert_eq!(
            sub(&d("2024-02-01"), &d("2024-01-01")).expect("d-d"),
            Datum::Int4(31)
        );
        assert_eq!(
            add(&d("2024-01-01"), &iv(0, 1, 0)).expect("d+iv"),
            Datum::Timestamp(crate::datetime::parse_timestamp("2024-01-02 00:00:00").expect("ts"))
        );
        assert_eq!(add(&iv(1, 0, 0), &iv(0, 5, 0)).expect("iv+iv"), iv(1, 5, 0));
        assert_eq!(
            mul(&iv(1, 2, 0), &Datum::Int4(3)).expect("iv*3"),
            iv(3, 6, 0)
        );
        let ts = |s: &str| Datum::Timestamp(crate::datetime::parse_timestamp(s).expect("ts"));
        assert_eq!(
            sub(&ts("2024-01-02 00:00:00"), &ts("2024-01-01 00:00:00")).expect("ts-ts"),
            iv(0, 0, 86_400_000_000)
        );
    }

    /// SP37 §8 GAP A: `time ± interval → time` — uses ONLY the interval micros,
    /// ignores days/months, and wraps mod 24 h.
    #[test]
    fn time_plus_interval_wraps_and_ignores_days() {
        let t = |s: &str| Datum::Time(crate::datetime::parse_time(s).expect("t"));
        let iv = |m, d, us| {
            Datum::Interval(crate::datetime::Interval {
                months: m,
                days: d,
                micros: us,
            })
        };
        // +2 hours wraps past midnight.
        assert_eq!(
            add(&t("23:00:00"), &iv(0, 0, 2 * 3600 * 1_000_000)).expect("a"),
            t("01:00:00")
        );
        // +1 day is ignored (a time has no date): result unchanged.
        assert_eq!(add(&t("12:00:00"), &iv(0, 1, 0)).expect("a"), t("12:00:00"));
        // +1 month is also ignored.
        assert_eq!(add(&t("12:00:00"), &iv(1, 0, 0)).expect("a"), t("12:00:00"));
        // interval + time is symmetric.
        assert_eq!(
            add(&iv(0, 0, 90 * 60 * 1_000_000), &t("10:00:00")).expect("a"),
            t("11:30:00")
        );
        // time - interval wraps backward past midnight.
        assert_eq!(
            sub(&t("00:30:00"), &iv(0, 0, 3600 * 1_000_000)).expect("s"),
            t("23:30:00")
        );
        // infer_type agrees: `time ± interval` plans as Time.
        assert_eq!(
            add(&t("23:00:00"), &iv(0, 0, 0)).expect("a").column_type(),
            Some(crate::ColumnType::Time)
        );
    }

    /// SP37 §8 GAP B: `date + time` / `time + date → timestamp` — combine the
    /// calendar date and the wall-clock time.
    #[test]
    fn date_plus_time_makes_timestamp() {
        let d = Datum::Date(crate::datetime::parse_date("2024-01-15").expect("d"));
        let t = Datum::Time(crate::datetime::parse_time("13:45:06").expect("t"));
        let want =
            Datum::Timestamp(crate::datetime::parse_timestamp("2024-01-15 13:45:06").expect("ts"));
        assert_eq!(add(&d, &t).expect("a"), want);
        // time + date is symmetric.
        assert_eq!(add(&t, &d).expect("a"), want);
        // The produced value's type is Timestamp.
        assert_eq!(
            add(&d, &t).expect("a").column_type(),
            Some(crate::ColumnType::Timestamp)
        );
    }

    #[test]
    fn datetime_comparison_orders_and_promotes() {
        use std::cmp::Ordering;
        let d = |s: &str| Datum::Date(crate::datetime::parse_date(s).expect("date"));
        assert_eq!(
            compare(&d("2024-01-01"), &d("2024-02-01")).expect("cmp"),
            Some(Ordering::Less)
        );
        let ts =
            Datum::Timestamp(crate::datetime::parse_timestamp("2024-01-01 00:00:01").expect("ts"));
        assert_eq!(
            compare(&d("2024-01-01"), &ts).expect("cmp"),
            Some(Ordering::Less)
        );
    }

    /// SP37: `Timestamptz` comparison orders by absolute instant (UTC µs), so
    /// two values with the same wall-clock time but different offsets are NOT equal
    /// — the one with the larger (more-negative) offset is a LATER instant.
    /// This test covers the `(Datum::Timestamptz, Datum::Timestamptz)` arm in
    /// `compare`, ensuring that arm exists and is mutation-baseline covered.
    #[test]
    fn timestamptz_compare_orders_by_absolute_instant() {
        use std::cmp::Ordering;
        let tz_utc = jiff::tz::TimeZone::UTC;
        let tz_ny = jiff::tz::TimeZone::get("America/New_York").expect("tzdb has NY");

        // Parse the same wall-clock "2024-01-15 12:00:00" in two different zones.
        // In UTC  it is the instant 2024-01-15 12:00:00 UTC.
        // In NY (EST = -05) it is the instant 2024-01-15 17:00:00 UTC — 5 h later.
        let ts_utc =
            crate::datetime::parse_timestamptz("2024-01-15 12:00:00", &tz_utc).expect("UTC tstz");
        let ts_ny =
            crate::datetime::parse_timestamptz("2024-01-15 12:00:00", &tz_ny).expect("NY tstz");

        let a = Datum::Timestamptz(ts_utc);
        let b = Datum::Timestamptz(ts_ny);

        // UTC noon is BEFORE NY noon (NY noon = UTC 17:00), so a < b.
        assert_eq!(compare(&a, &b).expect("cmp"), Some(Ordering::Less));
        assert_eq!(compare(&b, &a).expect("cmp"), Some(Ordering::Greater));
        // An identical instant compares Equal.
        assert_eq!(compare(&a, &a).expect("cmp"), Some(Ordering::Equal));

        // An explicit UTC+00 literal vs the same with UTC+00 — same instant → Equal.
        let ts_explicit = crate::datetime::parse_timestamptz("2024-01-15 12:00:00+00", &tz_ny)
            .expect("explicit +00");
        assert_eq!(
            compare(
                &Datum::Timestamptz(ts_utc),
                &Datum::Timestamptz(ts_explicit)
            )
            .expect("cmp"),
            Some(Ordering::Equal),
            "explicit +00 and UTC parse to the same instant"
        );
    }

    #[test]
    fn integer_literal_picks_narrowest_type() {
        assert_eq!(int_literal("5").expect("5"), Datum::Int4(5));
        assert_eq!(
            int_literal("2147483648").expect("big"),
            Datum::Int8(2_147_483_648)
        );
        assert!(matches!(
            int_literal("99999999999999999999"),
            Err(TypeError::Overflow)
        ));
    }

    #[test]
    fn arithmetic_type_promotion_and_overflow() {
        assert_eq!(
            add(&Datum::Int4(1), &Datum::Int4(2)).expect("ok"),
            Datum::Int4(3)
        );
        assert_eq!(
            add(&Datum::Int4(1), &Datum::Int8(2)).expect("ok"),
            Datum::Int8(3)
        );
        assert!(matches!(
            add(&Datum::Int4(i32::MAX), &Datum::Int4(1)),
            Err(TypeError::Overflow)
        ));
        assert!(matches!(
            div(&Datum::Int4(1), &Datum::Int4(0)),
            Err(TypeError::DivisionByZero)
        ));
    }

    #[test]
    fn modulo_sign_promotion_zero_and_min() {
        // Remainder takes the dividend's sign (truncated division, like PG).
        assert_eq!(
            rem(&Datum::Int4(11), &Datum::Int4(3)).expect("ok"),
            Datum::Int4(2)
        );
        assert_eq!(
            rem(&Datum::Int4(-11), &Datum::Int4(3)).expect("ok"),
            Datum::Int4(-2)
        );
        // Mixed width promotes to int8.
        assert_eq!(
            rem(&Datum::Int4(11), &Datum::Int8(3)).expect("ok"),
            Datum::Int8(2)
        );
        // NULL propagates; a zero divisor is 22012 (and NULL short-circuits it).
        assert_eq!(rem(&Datum::Null, &Datum::Int4(0)).expect("ok"), Datum::Null);
        assert!(matches!(
            rem(&Datum::Int4(1), &Datum::Int4(0)),
            Err(TypeError::DivisionByZero)
        ));
        // i32::MIN % -1 is mathematically 0, never an overflow trap.
        assert_eq!(
            rem(&Datum::Int4(i32::MIN), &Datum::Int4(-1)).expect("ok"),
            Datum::Int4(0)
        );
        // A non-integer operand is a type mismatch (42804).
        assert!(matches!(
            rem(&Datum::Text("x".into()), &Datum::Int4(1)),
            Err(TypeError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn concat_renders_each_operand_and_propagates_null() {
        let tz = jiff::tz::TimeZone::UTC;
        assert_eq!(
            concat(
                &Datum::Text("ab".into()),
                &Datum::Text("cd".into()),
                crate::encoding::OutputStyle::with_zone(&tz)
            )
            .expect("ok"),
            Datum::Text("abcd".into())
        );
        // Non-text operands render via their canonical text encoding.
        assert_eq!(
            concat(
                &Datum::Text("id=".into()),
                &Datum::Int4(5),
                crate::encoding::OutputStyle::with_zone(&tz)
            )
            .expect("ok"),
            Datum::Text("id=5".into())
        );
        assert_eq!(
            concat(
                &Datum::Int8(9_000_000_000),
                &Datum::Text("!".into()),
                crate::encoding::OutputStyle::with_zone(&tz)
            )
            .expect("ok"),
            Datum::Text("9000000000!".into())
        );
        // `||` coerces a bool through its *cast* to text (`true`/`false`), not
        // through its output function (`t`/`f`) — `SELECT true || 'x'` is
        // `truex` on the oracle.
        assert_eq!(
            concat(
                &Datum::Bool(true),
                &Datum::Text("x".into()),
                crate::encoding::OutputStyle::with_zone(&tz)
            )
            .expect("ok"),
            Datum::Text("truex".into())
        );
        // Either NULL operand yields NULL.
        assert_eq!(
            concat(
                &Datum::Null,
                &Datum::Text("x".into()),
                crate::encoding::OutputStyle::with_zone(&tz)
            )
            .expect("ok"),
            Datum::Null
        );
        assert_eq!(
            concat(
                &Datum::Text("x".into()),
                &Datum::Null,
                crate::encoding::OutputStyle::with_zone(&tz)
            )
            .expect("ok"),
            Datum::Null
        );
    }

    #[test]
    fn float_literal_and_overflow() {
        assert_eq!(float_literal("1.5").expect("1.5"), Datum::Float8(1.5));
        assert_eq!(float_literal(".5").expect(".5"), Datum::Float8(0.5));
        assert_eq!(float_literal("2e3").expect("2e3"), Datum::Float8(2000.0));
        // A literal overflowing to infinity is out of range.
        assert!(matches!(float_literal("1e400"), Err(TypeError::Overflow)));
    }

    #[test]
    fn float_arithmetic_promotion_and_division() {
        // int ⊕ float promotes to float.
        assert_eq!(
            add(&Datum::Int4(3), &Datum::Float8(0.5)).expect("ok"),
            Datum::Float8(3.5)
        );
        assert_eq!(
            mul(&Datum::Float8(2.0), &Datum::Int8(3)).expect("ok"),
            Datum::Float8(6.0)
        );
        // float division is real division (not integer truncation).
        assert_eq!(
            div(&Datum::Float8(5.0), &Datum::Float8(2.0)).expect("ok"),
            Datum::Float8(2.5)
        );
        // a zero float divisor is 22012 (NULL still short-circuits first).
        assert!(matches!(
            div(&Datum::Float8(1.0), &Datum::Float8(0.0)),
            Err(TypeError::DivisionByZero)
        ));
        assert_eq!(
            div(&Datum::Null, &Datum::Float8(0.0)).expect("ok"),
            Datum::Null
        );
        // finite × finite overflowing to infinity is 22003; an infinite operand
        // propagates Infinity without error.
        assert!(matches!(
            mul(&Datum::Float8(1e308), &Datum::Float8(1e308)),
            Err(TypeError::Overflow)
        ));
        assert_eq!(
            mul(&Datum::Float8(f64::INFINITY), &Datum::Float8(2.0)).expect("ok"),
            Datum::Float8(f64::INFINITY)
        );
    }

    /// PostgreSQL's result-type ladder for the four arithmetic operators, with
    /// the values it computes. Each row was checked with `pg_typeof` on
    /// PostgreSQL 18.4: `int2 ⊕ int2 → int2`, `int2 ⊕ int4 → int4`,
    /// `float4 ⊕ float4 → float4`, and `float4 ⊕ anything-else → float8`
    /// (there is no `float4 ⊕ int` operator, so both sides widen to the
    /// preferred type).
    #[test]
    fn int2_and_float4_arithmetic_promotion_matches_postgres() {
        use assert2::assert;
        type Case = (Datum, Datum, Datum, Datum, Datum, Datum);
        // (a, b, a+b, a-b, a*b, a/b)
        let cases: &[Case] = &[
            (
                Datum::Int2(7),
                Datum::Int2(2),
                Datum::Int2(9),
                Datum::Int2(5),
                Datum::Int2(14),
                Datum::Int2(3),
            ),
            (
                Datum::Int2(7),
                Datum::Int4(2),
                Datum::Int4(9),
                Datum::Int4(5),
                Datum::Int4(14),
                Datum::Int4(3),
            ),
            (
                Datum::Int4(7),
                Datum::Int2(2),
                Datum::Int4(9),
                Datum::Int4(5),
                Datum::Int4(14),
                Datum::Int4(3),
            ),
            (
                Datum::Int2(7),
                Datum::Int8(2),
                Datum::Int8(9),
                Datum::Int8(5),
                Datum::Int8(14),
                Datum::Int8(3),
            ),
            (
                Datum::Float4(3.0),
                Datum::Float4(2.0),
                Datum::Float4(5.0),
                Datum::Float4(1.0),
                Datum::Float4(6.0),
                Datum::Float4(1.5),
            ),
            (
                Datum::Float4(3.0),
                Datum::Int4(2),
                Datum::Float8(5.0),
                Datum::Float8(1.0),
                Datum::Float8(6.0),
                Datum::Float8(1.5),
            ),
            (
                Datum::Float4(3.0),
                Datum::Int2(2),
                Datum::Float8(5.0),
                Datum::Float8(1.0),
                Datum::Float8(6.0),
                Datum::Float8(1.5),
            ),
            (
                Datum::Float4(3.0),
                Datum::Float8(2.0),
                Datum::Float8(5.0),
                Datum::Float8(1.0),
                Datum::Float8(6.0),
                Datum::Float8(1.5),
            ),
            (
                Datum::Int2(7),
                num("2"),
                num("9"),
                num("5"),
                num("14"),
                num("3.5000000000000000000"),
            ),
        ];
        for (a, b, sum, difference, product, quotient) in cases {
            assert!(add(a, b).expect("add") == *sum, "{a:?} + {b:?}");
            assert!(sub(a, b).expect("sub") == *difference, "{a:?} - {b:?}");
            assert!(mul(a, b).expect("mul") == *product, "{a:?} * {b:?}");
            assert!(div(a, b).expect("div") == *quotient, "{a:?} / {b:?}");
        }
        // `float4 ⊕ numeric → float8`; the numeric side promotes past its own rung.
        assert!(add(&Datum::Float4(1.5), &num("2")).expect("f4+num") == Datum::Float8(3.5));
        // `float4` arithmetic is computed in f32, not f64-and-rounded: 1/3 keeps
        // exactly the single-precision digits PostgreSQL prints.
        assert!(
            crate::encoding::encode_text(
                &div(&Datum::Float4(1.0), &Datum::Float4(3.0)).expect("f4/f4"),
                &jiff::tz::TimeZone::UTC,
            ) == b"0.33333334"
        );
        // `mod` follows the same width ladder.
        assert!(rem(&Datum::Int2(7), &Datum::Int2(2)).expect("i2%i2") == Datum::Int2(1));
        assert!(rem(&Datum::Int2(7), &Datum::Int4(2)).expect("i2%i4") == Datum::Int4(1));
        assert!(rem(&Datum::Int2(-7), &Datum::Int2(2)).expect("sign") == Datum::Int2(-1));
    }

    /// The 22003 / 22012 boundaries, each with PostgreSQL's exact message.
    #[test]
    fn int2_and_float4_arithmetic_errors_match_postgres() {
        use assert2::assert;
        type BinOp = fn(&Datum, &Datum) -> Result<Datum, TypeError>;
        let smallint_overflow: &[(Datum, Datum, BinOp)] = &[
            (Datum::Int2(i16::MAX), Datum::Int2(1), add),
            (Datum::Int2(i16::MIN), Datum::Int2(1), sub),
            (Datum::Int2(i16::MIN), Datum::Int2(-1), mul),
            // `-32768 / -1` is +32768, one past the top of the range.
            (Datum::Int2(i16::MIN), Datum::Int2(-1), div),
        ];
        for (a, b, op) in smallint_overflow {
            let err = op(a, b).expect_err("smallint overflow");
            assert!(err.sqlstate() == "22003");
            assert!(err.to_string() == "smallint out of range", "{a:?} {b:?}");
        }
        // A widened operand lifts the ceiling: -32768::int2 * -1::int4 is int4.
        assert!(
            mul(&Datum::Int2(i16::MIN), &Datum::Int4(-1)).expect("i2*i4") == Datum::Int4(32_768)
        );
        // finite × finite overflowing f32 is 22003; an infinite operand propagates.
        let err = mul(&Datum::Float4(3.4e38), &Datum::Float4(2.0)).expect_err("f4 overflow");
        assert!(err.sqlstate() == "22003");
        assert!(err.to_string() == "value out of range: overflow");
        assert!(
            mul(&Datum::Float4(f32::INFINITY), &Datum::Float4(2.0)).expect("inf")
                == Datum::Float4(f32::INFINITY)
        );
        // Zero divisors are 22012 at both float widths and at int2.
        for (a, b) in [
            (Datum::Int2(1), Datum::Int2(0)),
            (Datum::Float4(1.0), Datum::Float4(0.0)),
            (Datum::Float4(1.0), Datum::Float4(-0.0)),
        ] {
            assert!(
                matches!(div(&a, &b), Err(TypeError::DivisionByZero)),
                "{a:?}"
            );
        }
        // NULL still short-circuits ahead of both.
        assert!(div(&Datum::Null, &Datum::Int2(0)).expect("null") == Datum::Null);
        assert!(add(&Datum::Null, &Datum::Float4(1.0)).expect("null") == Datum::Null);
    }

    /// Comparison across the widths: exact integer order, NaN largest, and
    /// `float4 → float8` widening (which is why `1.1::float4 <> 1.1::float8`).
    #[test]
    fn int2_and_float4_comparison_matches_postgres() {
        use assert2::assert;
        let cases: &[(Datum, Datum, Ordering)] = &[
            (Datum::Int2(1), Datum::Int2(2), Ordering::Less),
            (Datum::Int2(1), Datum::Int4(1), Ordering::Equal),
            (Datum::Int2(1), Datum::Int8(1), Ordering::Equal),
            (Datum::Int2(-32_768), Datum::Int8(0), Ordering::Less),
            (Datum::Int2(2), num("2.5"), Ordering::Less),
            (Datum::Int2(2), Datum::Float4(2.5), Ordering::Less),
            (Datum::Float4(1.5), Datum::Float4(1.5), Ordering::Equal),
            (Datum::Float4(-0.0), Datum::Float4(0.0), Ordering::Equal),
            (
                Datum::Float4(f32::NAN),
                Datum::Float4(f32::NAN),
                Ordering::Equal,
            ),
            (
                Datum::Float4(f32::NAN),
                Datum::Float4(f32::INFINITY),
                Ordering::Greater,
            ),
            (
                Datum::Float4(f32::NAN),
                Datum::Float8(1e308),
                Ordering::Greater,
            ),
            (
                Datum::Float4(f32::NAN),
                Datum::Int8(i64::MAX),
                Ordering::Greater,
            ),
            // f32 1.1 is strictly greater than f64 1.1 once widened exactly.
            (Datum::Float4(1.1), Datum::Float8(1.1), Ordering::Greater),
            (Datum::Float4(0.5), Datum::Float8(0.5), Ordering::Equal),
        ];
        for (a, b, expected) in cases {
            assert!(
                compare(a, b).expect("cmp") == Some(*expected),
                "{a:?} vs {b:?}"
            );
            assert!(
                compare(b, a).expect("cmp") == Some(expected.reverse()),
                "{b:?} vs {a:?}"
            );
        }
        assert!(compare(&Datum::Int2(1), &Datum::Null).expect("null") == None);
    }

    /// `date ± int2` resolves through PostgreSQL's implicit `int2 → int4` cast.
    #[test]
    fn date_arithmetic_accepts_an_int2_operand() {
        use assert2::assert;
        assert!(add(&date("2024-01-01"), &Datum::Int2(31)).expect("d+i2") == date("2024-02-01"));
        assert!(add(&Datum::Int2(31), &date("2024-01-01")).expect("i2+d") == date("2024-02-01"));
        assert!(sub(&date("2024-02-01"), &Datum::Int2(31)).expect("d-i2") == date("2024-01-01"));
        assert!(mul(&ivl(1, 2, 0), &Datum::Int2(3)).expect("iv*i2") == ivl(3, 6, 0));
    }

    #[test]
    fn float_comparison_orders_nan_last_and_equal_zeros() {
        // NaN equals itself and is greater than every non-NaN (PG float ordering).
        assert_eq!(
            compare(&Datum::Float8(f64::NAN), &Datum::Float8(f64::NAN)).expect("ok"),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare(&Datum::Float8(f64::NAN), &Datum::Float8(1.0)).expect("ok"),
            Some(Ordering::Greater)
        );
        // -0.0 and +0.0 compare equal.
        assert_eq!(
            compare(&Datum::Float8(-0.0), &Datum::Float8(0.0)).expect("ok"),
            Some(Ordering::Equal)
        );
        // mixed int/float comparison promotes to float.
        assert_eq!(
            compare(&Datum::Int4(2), &Datum::Float8(2.5)).expect("ok"),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn null_propagates_through_arithmetic() {
        assert_eq!(add(&Datum::Null, &Datum::Int4(1)).expect("ok"), Datum::Null);
        // NULL propagates BEFORE division-by-zero is evaluated: NULL / 0 is NULL,
        // not a 22012 error (the null check must short-circuit on EITHER operand).
        assert_eq!(div(&Datum::Null, &Datum::Int4(0)).expect("ok"), Datum::Null);
    }

    #[test]
    fn comparison_returns_none_for_null() {
        assert_eq!(
            compare(&Datum::Int4(1), &Datum::Int4(2)).expect("ok"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&Datum::Int4(1), &Datum::Int8(1)).expect("ok"),
            Some(Ordering::Equal)
        );
        assert_eq!(compare(&Datum::Null, &Datum::Int4(1)).expect("ok"), None);
        assert_eq!(
            compare(&Datum::Text("a".into()), &Datum::Text("b".into())).expect("ok"),
            Some(Ordering::Less)
        );
        // bool compares false < true (its own arm, not the integer fallback).
        assert_eq!(
            compare(&Datum::Bool(false), &Datum::Bool(true)).expect("ok"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&Datum::Bool(true), &Datum::Bool(true)).expect("ok"),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn three_valued_boolean_logic() {
        // Fully-defined operands: true AND true = true, false OR false = false.
        assert_eq!(
            and(&Datum::Bool(true), &Datum::Bool(true)).expect("ok"),
            Datum::Bool(true)
        );
        assert_eq!(
            or(&Datum::Bool(false), &Datum::Bool(false)).expect("ok"),
            Datum::Bool(false)
        );
        assert_eq!(
            and(&Datum::Null, &Datum::Bool(false)).expect("ok"),
            Datum::Bool(false)
        );
        assert_eq!(
            and(&Datum::Null, &Datum::Bool(true)).expect("ok"),
            Datum::Null
        );
        assert_eq!(
            or(&Datum::Null, &Datum::Bool(true)).expect("ok"),
            Datum::Bool(true)
        );
        assert_eq!(
            or(&Datum::Null, &Datum::Bool(false)).expect("ok"),
            Datum::Null
        );
        assert_eq!(not(&Datum::Null).expect("ok"), Datum::Null);
        assert_eq!(not(&Datum::Bool(true)).expect("ok"), Datum::Bool(false));
    }

    // -----------------------------------------------------------------------
    // SP37 mutation-killing tests: every temporal match arm in
    // temporal_add/sub/mul/div/compare (INCLUDING the commutative forms) and the
    // `is_null() || is_null()` NULL short-circuit in add/sub/mul/div, each pinned
    // to its exact PG-faithful value so a deleted arm or `|| → &&` flip fails.
    // -----------------------------------------------------------------------
    use crate::datetime::Interval;

    fn date(s: &str) -> Datum {
        Datum::Date(crate::datetime::parse_date(s).expect("date"))
    }
    fn tstamp(s: &str) -> Datum {
        Datum::Timestamp(crate::datetime::parse_timestamp(s).expect("ts"))
    }
    fn ivl(months: i32, days: i32, micros: i64) -> Datum {
        Datum::Interval(Interval {
            months,
            days,
            micros,
        })
    }

    #[test]
    fn temporal_add_every_arm() {
        // date + int8 → date (arm 166).
        assert_eq!(
            add(&date("2024-01-01"), &Datum::Int8(31)).expect("d+i8"),
            date("2024-02-01")
        );
        // int4 + date → date (arm 168, commutative).
        assert_eq!(
            add(&Datum::Int4(31), &date("2024-01-01")).expect("i4+d"),
            date("2024-02-01")
        );
        // int8 + date → date (arm 169, commutative).
        assert_eq!(
            add(&Datum::Int8(31), &date("2024-01-01")).expect("i8+d"),
            date("2024-02-01")
        );
        // interval + date → timestamp (arm 172, commutative).
        assert_eq!(
            add(&ivl(0, 1, 0), &date("2024-01-01")).expect("iv+d"),
            tstamp("2024-01-02 00:00:00")
        );
        // timestamp + interval → timestamp (arm 184).
        assert_eq!(
            add(&tstamp("2024-01-01 00:00:00"), &ivl(0, 0, 3_600_000_000)).expect("ts+iv"),
            tstamp("2024-01-01 01:00:00")
        );
        // interval + timestamp → timestamp (arm 187, commutative).
        assert_eq!(
            add(&ivl(0, 0, 3_600_000_000), &tstamp("2024-01-01 00:00:00")).expect("iv+ts"),
            tstamp("2024-01-01 01:00:00")
        );
    }

    #[test]
    fn temporal_sub_every_arm() {
        // date - int4 → date (arm 209).
        assert_eq!(
            sub(&date("2024-02-01"), &Datum::Int4(31)).expect("d-i4"),
            date("2024-01-01")
        );
        // date - int8 → date (arm 213).
        assert_eq!(
            sub(&date("2024-02-01"), &Datum::Int8(31)).expect("d-i8"),
            date("2024-01-01")
        );
        // date - interval → timestamp (arm 219).
        assert_eq!(
            sub(&date("2024-01-02"), &ivl(0, 1, 0)).expect("d-iv"),
            tstamp("2024-01-01 00:00:00")
        );
        // timestamp - interval → timestamp (arm 230).
        assert_eq!(
            sub(&tstamp("2024-01-01 01:00:00"), &ivl(0, 0, 3_600_000_000)).expect("ts-iv"),
            tstamp("2024-01-01 00:00:00")
        );
        // interval - interval → interval (arm 237).
        assert_eq!(
            sub(&ivl(2, 5, 7_000_000), &ivl(1, 2, 3_000_000)).expect("iv-iv"),
            ivl(1, 3, 4_000_000)
        );
    }

    #[test]
    fn temporal_mul_and_div_arms() {
        // num * interval → interval (arm 257, commutative form): 3 * interval.
        assert_eq!(
            mul(&Datum::Int4(3), &ivl(1, 2, 0)).expect("3*iv"),
            ivl(3, 6, 0)
        );
        // interval * num is the other arm (251) — already covered in the matrix,
        // pinned here too with a fractional factor that spills.
        assert_eq!(
            mul(&ivl(3, 4, 6_000_000), &Datum::Float8(1.5)).expect("iv*1.5"),
            ivl(4, 21, 9_000_000)
        );
        // interval / num → interval (arm 273): /4.
        assert_eq!(
            div(&ivl(2, 4, 6_000_000), &Datum::Int4(4)).expect("iv/4"),
            ivl(0, 16, 1_500_000)
        );
        // A non-numeric multiplier/divisor is a type mismatch (the arm's `?`).
        assert!(matches!(
            mul(&ivl(1, 0, 0), &Datum::Text("x".into())),
            Err(TypeError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn temporal_compare_every_arm() {
        // (Time, Time) — arm 450.
        let t = |s: &str| Datum::Time(crate::datetime::parse_time(s).expect("t"));
        assert_eq!(
            compare(&t("01:00:00"), &t("02:00:00")).expect("cmp"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&t("02:00:00"), &t("02:00:00")).expect("cmp"),
            Some(Ordering::Equal)
        );
        // (Timestamp, Timestamp) — arm 451.
        assert_eq!(
            compare(
                &tstamp("2024-01-01 00:00:00"),
                &tstamp("2024-01-02 00:00:00")
            )
            .expect("cmp"),
            Some(Ordering::Less)
        );
        // (Interval, Interval) — arm 454 (canonical estimate: 30 days < 1 month? no,
        // equal; 1 day < 1 month).
        assert_eq!(
            compare(&ivl(0, 1, 0), &ivl(1, 0, 0)).expect("cmp"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&ivl(0, 30, 0), &ivl(1, 0, 0)).expect("cmp"),
            Some(Ordering::Equal)
        );
        // (Timestamp, Date) — arm 457 (promote the date to midnight): a timestamp
        // one second past midnight is AFTER the date's midnight.
        assert_eq!(
            compare(&tstamp("2024-01-01 00:00:01"), &date("2024-01-01")).expect("cmp"),
            Some(Ordering::Greater)
        );
        assert_eq!(
            compare(&tstamp("2024-01-01 00:00:00"), &date("2024-01-01")).expect("cmp"),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn null_propagates_through_temporal_arithmetic_both_positions() {
        // The `a.is_null() || b.is_null()` short-circuit in add/sub/mul/div must
        // fire for EITHER operand when the OTHER is temporal — proving the `||` is
        // OR, not AND (under `&&` only the both-null case returns Null, so a
        // single-null temporal operand would dispatch into temporal_* and error or
        // return non-Null).
        let d = date("2024-01-01");
        let i = ivl(0, 1, 0);
        // add
        assert_eq!(add(&Datum::Null, &d).expect("ok"), Datum::Null);
        assert_eq!(add(&d, &Datum::Null).expect("ok"), Datum::Null);
        // sub
        assert_eq!(sub(&Datum::Null, &d).expect("ok"), Datum::Null);
        assert_eq!(sub(&d, &Datum::Null).expect("ok"), Datum::Null);
        // mul (interval operand)
        assert_eq!(mul(&Datum::Null, &i).expect("ok"), Datum::Null);
        assert_eq!(mul(&i, &Datum::Null).expect("ok"), Datum::Null);
        // div (interval operand)
        assert_eq!(div(&Datum::Null, &i).expect("ok"), Datum::Null);
        assert_eq!(div(&i, &Datum::Null).expect("ok"), Datum::Null);
    }

    // -----------------------------------------------------------------------
    // Pre-existing SP30 (float8) / SP32 (numeric) helper-path mutants surfaced by
    // a full-file mutation sweep. These are NOT date/time, but pgtypes is a
    // zero-survivor baseline crate, so they are killed here too.
    // -----------------------------------------------------------------------

    fn num(s: &str) -> Datum {
        Datum::Numeric(crate::numeric::parse(s).expect("numeric literal"))
    }

    #[test]
    fn numeric_and_float_promotion_paths_in_arith() {
        // numeric ⊕ float8 → float8: exercises `as_f64`'s Numeric arm (line 53) and
        // the `is_float` promotion. 2.5(numeric) + 1.0(float8) = 3.5(float8).
        assert_eq!(
            add(&num("2.5"), &Datum::Float8(1.0)).expect("num+f8"),
            Datum::Float8(3.5)
        );
        // int ⊕ numeric → numeric: exercises `as_numeric`'s Int4 arm (line 62) and
        // the `is_numeric(a) || is_numeric(b)` branch (line 130). 1 + 2 = 3 numeric.
        assert_eq!(add(&Datum::Int4(1), &num("2")).expect("i+num"), num("3"));
        // int8 ⊕ numeric → numeric: exercises `as_numeric`'s Int8 arm (line 63).
        assert_eq!(
            add(&Datum::Int8(9_000_000_000), &num("1")).expect("i8+num"),
            num("9000000001")
        );
        // numeric ⊕ numeric → numeric: exercises `as_numeric`'s Numeric arm (line
        // 64), the `is_numeric` predicate (line 74), and the `(Some, Some)` arm
        // (line 132). 1.5 + 1.5 = 3.0.
        assert_eq!(add(&num("1.5"), &num("1.5")).expect("num+num"), num("3.0"));
    }

    #[test]
    fn float_subtraction_is_real_subtraction() {
        // Line 315 `|x, y| x - y`: 5.0 - 2.0 = 3.0 (NOT 7.0 for `+`, NOT 2.5 for `/`).
        assert_eq!(
            sub(&Datum::Float8(5.0), &Datum::Float8(2.0)).expect("f8-f8"),
            Datum::Float8(3.0)
        );
    }

    #[test]
    fn division_dispatches_float_numeric_and_integer_paths() {
        // Float divisor path (line 345 `is_float(a) || is_float(b)`): int / float8.
        assert_eq!(
            div(&Datum::Int4(5), &Datum::Float8(2.0)).expect("i/f8"),
            Datum::Float8(2.5)
        );
        // The `||` (not `&&`) in line 345 matters for the SQLSTATE: `int / float 0`
        // must take div's float fast-path → 22012 DivisionByZero. Under `&&`, a
        // single-float operand would skip that path and fall to `arith`'s float
        // branch, where 1.0/0.0 → ∞ with finite operands → 22003 Overflow — a
        // DIFFERENT error. Pinning DivisionByZero here kills the `||→&&` mutant.
        assert!(
            matches!(
                div(&Datum::Int4(1), &Datum::Float8(0.0)),
                Err(TypeError::DivisionByZero)
            ),
            "int / float-zero is 22012, not 22003"
        );
        // Numeric divisor path (line 358): int / numeric → numeric exact division.
        assert_eq!(
            div(&Datum::Int4(7), &num("2")).expect("i/num"),
            num("3.5000000000000000000")
        );
        // Integer division closure (line 376 `|x, y| x / y` for op_i8): int8 / int8
        // truncates. 7 / 2 = 3 (NOT 1 for `%`, NOT 14 for `*`).
        assert_eq!(
            div(&Datum::Int8(7), &Datum::Int8(2)).expect("i8/i8"),
            Datum::Int8(3)
        );
    }

    #[test]
    fn numeric_modulo_path() {
        // Line 391 `is_numeric(a) || is_numeric(b)`: int % numeric → numeric.
        assert_eq!(rem(&Datum::Int4(7), &num("3")).expect("i%num"), num("1"));
        assert_eq!(rem(&num("7.5"), &num("2")).expect("num%num"), num("1.5"));
    }

    #[test]
    fn compare_routes_float_numeric_and_integer_pairs_distinctly() {
        // Integer pair must NOT go through float_cmp (line 460 guard): two large
        // i64 that are DISTINCT but collapse to the same f64 must still compare as
        // distinct integers. i64::MAX vs i64::MAX-1 round to the same f64.
        assert_eq!(
            compare(&Datum::Int8(i64::MAX), &Datum::Int8(i64::MAX - 1)).expect("cmp"),
            Some(Ordering::Greater),
            "integers must compare exactly, not via lossy f64"
        );
        // Numeric pair (line 465 `is_numeric` guard + line 466 `(Some, Some)` arm):
        // compares by value, ignoring scale, and mixes with int.
        assert_eq!(
            compare(&num("2.50"), &num("2.5")).expect("cmp"),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare(&Datum::Int4(2), &num("2.5")).expect("cmp"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&num("3"), &Datum::Int4(2)).expect("cmp"),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn jsonb_compare_uses_the_jsonb_btree_order() {
        use assert2::assert;
        let j = |text: &str| Datum::Jsonb(crate::jsonb::parse(text).expect("jsonb"));
        // Object > Array > Bool > Number > String > Null, then within-type order.
        let cases: &[(&str, &str, Ordering)] = &[
            ("null", r#""s""#, Ordering::Less),
            (r#""s""#, "1", Ordering::Less),
            ("1", "true", Ordering::Less),
            ("true", "[]", Ordering::Less),
            ("[]", "{}", Ordering::Less),
            ("1", "2", Ordering::Less),
            ("1.0", "1.00", Ordering::Equal),
            (r#"{"a":1,"b":2}"#, r#"{"b":2,"a":1}"#, Ordering::Equal),
            (r#"[1]"#, r#"[0,0]"#, Ordering::Less),
        ];
        for (left, right, expected) in cases {
            assert!(
                compare(&j(left), &j(right)).expect("jsonb cmp") == Some(*expected),
                "{left} vs {right}"
            );
        }
        // NULL still short-circuits before the jsonb arm.
        assert!(compare(&j("1"), &Datum::Null).expect("null") == None);
    }

    #[test]
    fn array_compare_is_element_wise_with_nulls_greater() {
        use assert2::assert;

        use crate::{ArrayValue, ElemType};
        let a = |values: &[Option<i32>]| {
            Datum::Array(ArrayValue::new(
                ElemType::Int4,
                values
                    .iter()
                    .map(|v| v.map_or(Datum::Null, Datum::Int4))
                    .collect(),
            ))
        };
        type Case = (Vec<Option<i32>>, Vec<Option<i32>>, Ordering);
        let cases: &[Case] = &[
            (vec![], vec![], Ordering::Equal),
            (vec![], vec![Some(1)], Ordering::Less),
            (vec![Some(1)], vec![Some(2)], Ordering::Less),
            // Element-wise wins over length: {2} > {1,9}.
            (vec![Some(2)], vec![Some(1), Some(9)], Ordering::Greater),
            // Equal prefix: shorter first.
            (vec![Some(1)], vec![Some(1), Some(0)], Ordering::Less),
            // A NULL element sorts greater than any value, and two NULLs are equal.
            (vec![None], vec![Some(i32::MAX)], Ordering::Greater),
            (vec![None], vec![None], Ordering::Equal),
            (
                vec![Some(1), None],
                vec![Some(1), Some(2)],
                Ordering::Greater,
            ),
        ];
        for (left, right, expected) in cases {
            assert!(
                compare(&a(left), &a(right)).expect("array cmp") == Some(*expected),
                "{left:?} vs {right:?}"
            );
            assert!(
                compare(&a(right), &a(left)).expect("array cmp") == Some(expected.reverse()),
                "{right:?} vs {left:?}"
            );
        }
    }
}
