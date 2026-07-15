//! SP31: explicit type casts — `CAST(expr AS type)` and `expr::type`.
//!
//! This is the *explicit* cast context (the broadest PostgreSQL cast context),
//! among the slice's five runtime types (`bool`, `int4`, `int8`, `text`,
//! `float8`). It is a pure value transform — no I/O, no catalog, no concurrency —
//! so it lives here in the type layer and is proven exhaustively by unit tests.
//!
//! Two entry points, sharing one cast matrix:
//!   * [`cast_allowed`] — a *static* (plan-time) predicate on `(from, to)` column
//!     types, so [`crate::ops`]-free callers can reject an undefined cast with
//!     SQLSTATE 42846 before any row is produced (and so `RowDescription` knows
//!     the result type).
//!   * [`cast`] — the *runtime* value conversion of one (possibly NULL) `Datum`.
//!
//! The defined casts (NULL → NULL for every one of them):
//!   * identity `T → T`;
//!   * numeric ↔ numeric (`int4`/`int8`/`float8`, any direction) — widening,
//!     range-checked narrowing (22003), and `float8 → int` rounding half-to-even;
//!   * `bool → int4` (`false`→0, `true`→1) and `int4 → bool` (0→false, else true)
//!     — PostgreSQL has these only for `int4`, not `int8`;
//!   * any type `→ text` (the type's output text), and `text →` any type (parsed,
//!     22P02 on bad syntax, 22003 on overflow).
//!
//! Everything else (e.g. `float8`/`int8` ↔ `bool`) is undefined → 42846.

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible cast semantics kept structurally close to donor"
)]

use crate::{ColumnType, Datum, TypeError};

/// Is an explicit cast from `from` to `to` defined among the slice's types? Used
/// at plan time so an undefined cast surfaces as 42846 before execution, and so
/// the result column type is known for `RowDescription`.
pub fn cast_allowed(from: ColumnType, to: ColumnType) -> bool {
    use ColumnType::{Bool, Date, Int4, Text, Time, Timestamp, Timestamptz};
    // SP32: the numeric family — int4/int8/float8/numeric — all interconvert.
    let num_family = |t: ColumnType| {
        matches!(t, ColumnType::Int4 | ColumnType::Int8 | ColumnType::Float8) || t.is_numeric()
    };
    match (from, to) {
        // Identity (e.g. numeric → numeric, even across differing typmods).
        (a, b) if a == b => true,
        _ if from.is_numeric() && to.is_numeric() => true,
        // Numeric family ↔ numeric family, any direction.
        _ if num_family(from) && num_family(to) => true,
        // PostgreSQL defines bool↔int only for int4 (not int8 / float8 / numeric).
        (Bool, Int4) | (Int4, Bool) => true,
        // `regclass` interconverts with the integer oid family; text↔regclass is
        // covered by the string rules below.
        (Int4 | ColumnType::Int8, ColumnType::Regclass)
        | (ColumnType::Regclass, Int4 | ColumnType::Int8) => true,
        _ if from.is_string() || to.is_string() => true,
        // Anything → text (the output function), and text → anything (the input
        // function). Together these also cover text→text (already by identity),
        // temporal→text, and text→temporal — all valid explicit casts in PostgreSQL.
        (_, Text) | (Text, _) => true,
        // SP37: cross-temporal casts. Interval only interconverts with text (above).
        // date → {timestamp, timestamptz}
        (Date, Timestamp) | (Date, Timestamptz) => true,
        // timestamp → {date, time, timestamptz}
        (Timestamp, Date) | (Timestamp, Time) | (Timestamp, Timestamptz) => true,
        // timestamptz → {date, time, timestamp}
        (Timestamptz, Date) | (Timestamptz, Time) | (Timestamptz, Timestamp) => true,
        // Everything else — including numeric/bool ↔ temporal, interval ↔ temporal,
        // and time → timestamp/timestamptz: undefined → 42846.
        _ => false,
    }
}

/// Perform an explicit cast of a (possibly NULL) `Datum` to `to`. NULL casts to
/// NULL of the target type. A text-parse failure is 22P02; a numeric overflow is
/// 22003; an undefined `(from, to)` pair is 42846 — though callers that gate on
/// [`cast_allowed`] at plan time never reach that arm for a non-NULL value.
///
/// `tz` is forwarded to `encode_text` for the `* → text` cast arms involving
/// `Timestamptz`; all other cast paths ignore it. Task 7 will add `text →
/// timestamptz` and will use `tz` for parsing.
pub fn cast(value: &Datum, to: ColumnType, tz: &jiff::tz::TimeZone) -> Result<Datum, TypeError> {
    use ColumnType::{Bool, Float8, Int4, Int8, Numeric, Text};
    if value.is_null() {
        return Ok(Datum::Null);
    }
    match (value, to) {
        // Identity (each variant to its own type).
        (Datum::Bool(b), Bool) => Ok(Datum::Bool(*b)),
        (Datum::Int4(n), Int4) => Ok(Datum::Int4(*n)),
        (Datum::Int8(n), Int8) => Ok(Datum::Int8(*n)),
        (Datum::Float8(f), Float8) => Ok(Datum::Float8(*f)),
        (Datum::Text(s), Text) => Ok(Datum::Text(s.clone())),
        (Datum::Text(s), ColumnType::Varchar(n)) => {
            crate::string::apply_varchar_typmod(s, n).map(Datum::Text)
        }
        (Datum::Text(s), ColumnType::Char(n)) => {
            crate::string::apply_char_typmod(s, n).map(Datum::Text)
        }
        // SP37: temporal identity casts — `cast_allowed(T, T)` is true via the
        // `(a,b) if a==b` guard, so these arms must exist or `cast()` would fall
        // through to `cannot_cast` and return 42846 on e.g. `x::date` where x is
        // already a date.
        (Datum::Date(d), ColumnType::Date) => Ok(Datum::Date(*d)),
        (Datum::Time(t), ColumnType::Time) => Ok(Datum::Time(*t)),
        (Datum::Timestamp(dt), ColumnType::Timestamp) => Ok(Datum::Timestamp(*dt)),
        (Datum::Timestamptz(ts), ColumnType::Timestamptz) => Ok(Datum::Timestamptz(*ts)),
        (Datum::Interval(i), ColumnType::Interval) => Ok(Datum::Interval(*i)),
        // Numeric (int/float) ↔ numeric (int/float).
        (Datum::Int4(n), Int8) => Ok(Datum::Int8(i64::from(*n))),
        (Datum::Int4(n), Float8) => Ok(Datum::Float8(f64::from(*n))),
        (Datum::Int8(n), Int4) => i4_from_i64(*n),
        (Datum::Int8(n), Float8) => Ok(Datum::Float8(*n as f64)),
        (Datum::Float8(f), Int4) => i4_from_f64(*f),
        (Datum::Float8(f), Int8) => i8_from_f64(*f),
        // SP32: → numeric (applying any `numeric(p,s)` modifier on the target).
        (Datum::Int4(n), Numeric(tm)) => to_numeric(crate::numeric::from_i64(i64::from(*n)), tm),
        (Datum::Int8(n), Numeric(tm)) => to_numeric(crate::numeric::from_i64(*n), tm),
        (Datum::Float8(f), Numeric(tm)) => to_numeric(crate::numeric::from_f64(*f)?, tm),
        (Datum::Numeric(d), Numeric(tm)) => to_numeric(d.clone(), tm),
        // SP32: numeric → int/float/text.
        (Datum::Numeric(d), Int4) => crate::numeric::to_i32(d).map(Datum::Int4),
        (Datum::Numeric(d), Int8) => crate::numeric::to_i64(d).map(Datum::Int8),
        (Datum::Numeric(d), Float8) => Ok(Datum::Float8(crate::numeric::to_f64(d))),
        // bool ↔ int4.
        (Datum::Bool(b), Int4) => Ok(Datum::Int4(i32::from(*b))),
        (Datum::Int4(n), Bool) => Ok(Datum::Bool(*n != 0)),
        // → text. `bool` renders as PostgreSQL's `booltext` cast (`true`/`false`),
        // NOT the `t`/`f` of `boolout`; the others reuse the wire text encoding.
        (Datum::Bool(b), Text) => Ok(Datum::Text((if *b { "true" } else { "false" }).into())),
        (d, Text) => Ok(Datum::Text(text_of(d, tz))),
        (d, ColumnType::Varchar(n)) => {
            crate::string::apply_varchar_typmod(&string_cast_input(d, tz), n).map(Datum::Text)
        }
        (d, ColumnType::Char(n)) => {
            crate::string::apply_char_typmod(&string_cast_input(d, tz), n).map(Datum::Text)
        }
        // text → other.
        (Datum::Text(s), Bool) => text_to_bool(s),
        (Datum::Text(s), Int4) => text_to_i32(s),
        (Datum::Text(s), Int8) => text_to_i64(s),
        (Datum::Text(s), Float8) => text_to_f64(s),
        // `regclass` values are relation oids in the Int4 datum. The pure cast
        // accepts numeric input only; a relation NAME needs catalog resolution,
        // which the executor layers perform before reaching here (a name that
        // falls through is 22P02, mirroring an unresolvable input).
        (Datum::Int4(n), ColumnType::Regclass) => Ok(Datum::Int4(*n)),
        (Datum::Int8(n), ColumnType::Regclass) => i4_from_i64(*n),
        (Datum::Text(s), ColumnType::Regclass) => {
            s.trim()
                .parse::<i32>()
                .map(Datum::Int4)
                .map_err(|_| TypeError::InvalidText {
                    type_name: "regclass",
                    value: s.clone(),
                })
        }
        (Datum::Text(s), Numeric(tm)) => {
            let d = crate::numeric::parse(s).ok_or_else(|| TypeError::InvalidText {
                type_name: "numeric",
                value: s.to_string(),
            })?;
            to_numeric(d, tm)
        }
        // SP37: text → temporal (parse errors propagate as 22007/22008/InvalidText).
        (Datum::Text(s), ColumnType::Date) => crate::datetime::parse_date(s).map(Datum::Date),
        (Datum::Text(s), ColumnType::Time) => crate::datetime::parse_time(s).map(Datum::Time),
        (Datum::Text(s), ColumnType::Timestamp) => {
            crate::datetime::parse_timestamp(s).map(Datum::Timestamp)
        }
        (Datum::Text(s), ColumnType::Timestamptz) => {
            crate::datetime::parse_timestamptz(s, tz).map(Datum::Timestamptz)
        }
        (Datum::Text(s), ColumnType::Interval) => {
            crate::datetime::parse_interval(s).map(Datum::Interval)
        }
        (Datum::Text(s), ColumnType::Uuid) => {
            crate::uuid::UuidBytes::parse(s).map(|uuid| Datum::Text(uuid.to_canonical_text()))
        }
        // SP37: temporal → text already handled by the `(d, Text)` arm above via
        // tz-aware `text_of`. The remaining cross-temporal casts:
        //
        // date → timestamp: midnight (no timezone involved).
        (Datum::Date(d), ColumnType::Timestamp) => {
            Ok(Datum::Timestamp(crate::datetime::date_to_midnight(*d)))
        }
        // date → timestamptz: midnight in the session tz → absolute instant.
        (Datum::Date(d), ColumnType::Timestamptz) => {
            let dt = crate::datetime::date_to_midnight(*d);
            dt.to_zoned(tz.clone())
                .map(|z| Datum::Timestamptz(z.timestamp()))
                .map_err(|_| TypeError::DatetimeFieldOverflow {
                    value: format!("{d}"),
                })
        }
        // timestamp → date: truncate to date part.
        (Datum::Timestamp(dt), ColumnType::Date) => Ok(Datum::Date(dt.date())),
        // timestamp → time: extract time-of-day.
        (Datum::Timestamp(dt), ColumnType::Time) => Ok(Datum::Time(dt.time())),
        // timestamp → timestamptz: interpret wall-clock as session tz → instant.
        (Datum::Timestamp(dt), ColumnType::Timestamptz) => dt
            .to_zoned(tz.clone())
            .map(|z| Datum::Timestamptz(z.timestamp()))
            .map_err(|_| TypeError::DatetimeFieldOverflow {
                value: format!("{dt}"),
            }),
        // timestamptz → timestamp: render instant in session tz → wall-clock datetime.
        (Datum::Timestamptz(ts), ColumnType::Timestamp) => {
            Ok(Datum::Timestamp(tz.to_datetime(*ts)))
        }
        // timestamptz → date: render in session tz, take date part.
        (Datum::Timestamptz(ts), ColumnType::Date) => Ok(Datum::Date(tz.to_datetime(*ts).date())),
        // timestamptz → time: render in session tz, take time-of-day.
        (Datum::Timestamptz(ts), ColumnType::Time) => Ok(Datum::Time(tz.to_datetime(*ts).time())),
        // No defined cast.
        (v, to) => Err(cannot_cast(v, to)),
    }
}

/// Wrap a `BigDecimal` as a numeric `Datum`, applying a `numeric(p,s)` modifier
/// (round to scale + precision overflow → 22003) when the target carries one.
fn to_numeric(
    d: bigdecimal::BigDecimal,
    tm: Option<crate::numeric::Typmod>,
) -> Result<Datum, TypeError> {
    match tm {
        Some(tm) => Ok(Datum::Numeric(crate::numeric::apply_typmod(&d, tm)?)),
        None => Ok(Datum::Numeric(crate::numeric::canonical(d))),
    }
}

/// The canonical wire text rendering of a non-NULL Datum (the same encoder the
/// DataRow path uses), for the numeric/`*`→`text` casts.
fn text_of(d: &Datum, tz: &jiff::tz::TimeZone) -> String {
    String::from_utf8(crate::encoding::encode_text(d, tz))
        .expect("a Datum's text encoding is always valid UTF-8")
}

fn string_cast_input(d: &Datum, tz: &jiff::tz::TimeZone) -> String {
    match d {
        Datum::Bool(true) => "true".to_string(),
        Datum::Bool(false) => "false".to_string(),
        _ => text_of(d, tz),
    }
}

/// `int8 → int4`: out-of-range is 22003 (PostgreSQL `int84`).
fn i4_from_i64(n: i64) -> Result<Datum, TypeError> {
    i32::try_from(n)
        .map(Datum::Int4)
        .map_err(|_| TypeError::Overflow)
}

/// `float8 → int4`: round half-to-even (PostgreSQL `dtoi4`/`rint`), then
/// range-check; a non-finite or out-of-range value is 22003.
fn i4_from_f64(f: f64) -> Result<Datum, TypeError> {
    let r = f.round_ties_even();
    if r.is_finite() && (f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&r) {
        Ok(Datum::Int4(r as i32))
    } else {
        Err(TypeError::Overflow)
    }
}

/// `float8 → int8`: round half-to-even then range-check; non-finite / out of
/// range is 22003.
fn i8_from_f64(f: f64) -> Result<Datum, TypeError> {
    let r = f.round_ties_even();
    if r.is_finite() && (i64::MIN as f64..=i64::MAX as f64).contains(&r) {
        Ok(Datum::Int8(r as i64))
    } else {
        Err(TypeError::Overflow)
    }
}

/// `text → bool`, mirroring PostgreSQL `boolin`/`parse_bool_with_len`: case-
/// insensitive, leading/trailing whitespace trimmed, then a non-empty prefix of
/// `true`/`false`/`yes`/`no`/`on`/`off`, or the single chars `1`/`0`. The `o`
/// prefix is ambiguous between `on`/`off` and PostgreSQL resolves it to `on`
/// (true) by testing `on` first; everything else is 22P02.
fn text_to_bool(s: &str) -> Result<Datum, TypeError> {
    let t = s.trim().to_ascii_lowercase();
    let v = match t.as_bytes().first() {
        Some(b't') if "true".starts_with(&t) => true,
        Some(b'f') if "false".starts_with(&t) => false,
        Some(b'y') if "yes".starts_with(&t) => true,
        Some(b'n') if "no".starts_with(&t) => false,
        Some(b'o') if "on".starts_with(&t) => true, // `on` checked before `off`
        Some(b'o') if "off".starts_with(&t) => false,
        Some(b'1') if t.len() == 1 => true,
        Some(b'0') if t.len() == 1 => false,
        _ => {
            return Err(TypeError::InvalidText {
                type_name: "boolean",
                value: s.to_string(),
            });
        }
    };
    Ok(Datum::Bool(v))
}

/// `text → int4` / `int8`, matching PostgreSQL integer input: leading/trailing
/// whitespace trimmed, an optional leading sign, then digits only (no decimal
/// point, no exponent). Bad syntax is 22P02; a syntactically-valid value that
/// does not fit the target width is 22003.
fn text_to_i32(s: &str) -> Result<Datum, TypeError> {
    require_int_syntax(s, "integer")?;
    s.trim()
        .parse::<i32>()
        .map(Datum::Int4)
        .map_err(|_| TypeError::Overflow)
}

fn text_to_i64(s: &str) -> Result<Datum, TypeError> {
    require_int_syntax(s, "bigint")?;
    s.trim()
        .parse::<i64>()
        .map(Datum::Int8)
        .map_err(|_| TypeError::Overflow)
}

/// 22P02 unless the trimmed text is `[+-]?[0-9]+`. Separating the syntax check
/// from the width parse lets an out-of-range-but-well-formed value (e.g.
/// `'99999999999'`) report 22003 rather than being lumped into 22P02.
fn require_int_syntax(s: &str, type_name: &'static str) -> Result<(), TypeError> {
    let t = s.trim();
    let digits = t.strip_prefix(['+', '-']).unwrap_or(t);
    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
        Ok(())
    } else {
        Err(TypeError::InvalidText {
            type_name,
            value: s.to_string(),
        })
    }
}

/// `text → float8`, matching PostgreSQL `float8in`: trimmed, accepts decimal /
/// exponent forms and the specials `Infinity`/`-Infinity`/`NaN`/`inf` (case-
/// insensitive). Bad syntax is 22P02; a *finite* literal that overflows to
/// infinity (e.g. `'1e400'`) is 22003 — but an explicit infinity spelling is the
/// value `Infinity`, not an error (this is why it cannot just reuse
/// [`crate::ops::float_literal`], whose grammar has no infinity spelling).
fn text_to_f64(s: &str) -> Result<Datum, TypeError> {
    let t = s.trim();
    match t.parse::<f64>() {
        Ok(v) if v.is_infinite() && !is_infinity_spelling(t) => Err(TypeError::Overflow),
        Ok(v) => Ok(Datum::Float8(v)),
        Err(_) => Err(TypeError::InvalidText {
            type_name: "double precision",
            value: s.to_string(),
        }),
    }
}

/// Does `t` (already trimmed) literally spell infinity (so a parsed ∞ is the
/// intended value, not a finite-literal overflow)?
fn is_infinity_spelling(t: &str) -> bool {
    let body = t.strip_prefix(['+', '-']).unwrap_or(t);
    body.eq_ignore_ascii_case("inf") || body.eq_ignore_ascii_case("infinity")
}

fn cannot_cast(v: &Datum, to: ColumnType) -> TypeError {
    TypeError::CannotCast {
        from: v.column_type().map(ColumnType::name).unwrap_or("unknown"),
        to: to.name(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnType, Datum, TypeError};

    fn utc() -> jiff::tz::TimeZone {
        jiff::tz::TimeZone::UTC
    }

    // ---- the static matrix ----

    #[test]
    fn cast_allowed_matches_the_postgres_matrix() {
        use ColumnType::{Bool, Float8, Int4, Int8, Text};

        use crate::numeric::Typmod;
        // Identity for every type.
        for t in [Bool, Int4, Int8, Text, Float8] {
            assert!(cast_allowed(t, t), "{t:?} -> {t:?}");
        }
        // Numeric ↔ numeric, every direction.
        for a in [Int4, Int8, Float8] {
            for b in [Int4, Int8, Float8] {
                assert!(cast_allowed(a, b), "{a:?} -> {b:?}");
            }
        }
        // bool ↔ int4 only.
        assert!(cast_allowed(Bool, Int4));
        assert!(cast_allowed(Int4, Bool));
        // text ↔ everything.
        for t in [Bool, Int4, Int8, Float8] {
            assert!(cast_allowed(t, Text), "{t:?} -> text");
            assert!(cast_allowed(Text, t), "text -> {t:?}");
        }
        // The undefined casts: bool ↔ {int8, float8}.
        assert!(!cast_allowed(Bool, Int8));
        assert!(!cast_allowed(Int8, Bool));
        assert!(!cast_allowed(Bool, Float8));
        assert!(!cast_allowed(Float8, Bool));
        // SP32: numeric joins the numeric family (↔ int4/int8/float8/numeric), but
        // there is no numeric ↔ bool cast.
        let num = ColumnType::Numeric(None);
        for t in [Int4, Int8, Float8, num] {
            assert!(cast_allowed(num, t), "numeric -> {t:?}");
            assert!(cast_allowed(t, num), "{t:?} -> numeric");
        }
        assert!(cast_allowed(
            num,
            ColumnType::Numeric(Some(Typmod {
                precision: 5,
                scale: 2
            }))
        ));
        assert!(cast_allowed(num, Text) && cast_allowed(Text, num));
        assert!(!cast_allowed(num, Bool));
        assert!(!cast_allowed(Bool, num));
        // SP37: temporal types — allowed pairs.
        use ColumnType::{Date, Interval, Time, Timestamp, Timestamptz};
        // identity
        for t in [Date, Time, Timestamp, Timestamptz, Interval] {
            assert!(cast_allowed(t, t), "{t:?} -> {t:?}");
        }
        // text ↔ every temporal type
        for t in [Date, Time, Timestamp, Timestamptz, Interval] {
            assert!(cast_allowed(t, Text), "{t:?} -> text");
            assert!(cast_allowed(Text, t), "text -> {t:?}");
        }
        // date → {timestamp, timestamptz}
        assert!(cast_allowed(Date, Timestamp));
        assert!(cast_allowed(Date, Timestamptz));
        // timestamp → {date, time, timestamptz}
        assert!(cast_allowed(Timestamp, Date));
        assert!(cast_allowed(Timestamp, Time));
        assert!(cast_allowed(Timestamp, Timestamptz));
        // timestamptz → {date, time, timestamp}
        assert!(cast_allowed(Timestamptz, Date));
        assert!(cast_allowed(Timestamptz, Time));
        assert!(cast_allowed(Timestamptz, Timestamp));
        // NOT allowed: interval ↔ date/time/timestamp/timestamptz
        assert!(!cast_allowed(Interval, Date));
        assert!(!cast_allowed(Interval, Time));
        assert!(!cast_allowed(Interval, Timestamp));
        assert!(!cast_allowed(Interval, Timestamptz));
        assert!(!cast_allowed(Date, Interval));
        assert!(!cast_allowed(Time, Interval));
        assert!(!cast_allowed(Timestamp, Interval));
        assert!(!cast_allowed(Timestamptz, Interval));
        // NOT allowed: numeric/bool ↔ temporal
        assert!(!cast_allowed(Int4, Date));
        assert!(!cast_allowed(Date, Int4));
        assert!(!cast_allowed(Bool, Timestamp));
        assert!(!cast_allowed(Timestamp, Bool));
        assert!(!cast_allowed(Float8, Timestamptz));
        // NOT allowed: time → timestamp (time has no date component in PG's standard matrix)
        assert!(!cast_allowed(Time, Timestamp));
        assert!(!cast_allowed(Time, Timestamptz));
        assert!(!cast_allowed(Date, Time));
    }

    #[test]
    fn numeric_casts_convert_and_apply_typmod() {
        use ColumnType::{Float8, Int4};

        use crate::numeric::{Typmod, to_text};
        let num = ColumnType::Numeric(None);
        let tz = utc();
        // int/float/text → numeric.
        assert!(matches!(
            cast(&Datum::Int4(5), num, &tz).expect("i4->num"),
            Datum::Numeric(ref d) if to_text(d) == "5"
        ));
        assert!(matches!(
            cast(&Datum::Text("12.34".into()), num, &tz).expect("text->num"),
            Datum::Numeric(ref d) if to_text(d) == "12.34"
        ));
        assert!(matches!(
            cast(&Datum::Float8(0.1), num, &tz).expect("f8->num"),
            Datum::Numeric(ref d) if to_text(d) == "0.1" // shortest text, not binary expansion
        ));
        // numeric → int rounds half away from zero; → float8; → text.
        assert_eq!(
            cast(
                &Datum::Numeric(crate::numeric::parse("2.5").expect("p")),
                Int4,
                &tz,
            )
            .expect("num->i4"),
            Datum::Int4(3)
        );
        assert_eq!(
            cast(
                &Datum::Numeric(crate::numeric::parse("1.5").expect("p")),
                Float8,
                &tz,
            )
            .expect("f8"),
            Datum::Float8(1.5)
        );
        // cast to numeric(p,s) rounds + overflows (22003).
        let tm = ColumnType::Numeric(Some(Typmod {
            precision: 4,
            scale: 1,
        }));
        assert!(matches!(
            cast(&Datum::Text("123.45".into()), tm, &tz).expect("ok"),
            Datum::Numeric(ref d) if to_text(d) == "123.5"
        ));
        assert!(matches!(
            cast(&Datum::Text("1234.5".into()), tm, &tz),
            Err(TypeError::Overflow)
        ));
        // bad text → numeric is 22P02.
        assert!(matches!(
            cast(&Datum::Text("abc".into()), num, &tz),
            Err(TypeError::InvalidText { .. })
        ));
    }

    #[test]
    fn string_casts_apply_varchar_and_char_typmods() {
        let tz = utc();
        assert!(cast_allowed(ColumnType::Text, ColumnType::Varchar(Some(3))));
        assert!(cast_allowed(
            ColumnType::Varchar(Some(3)),
            ColumnType::Char(Some(3))
        ));
        assert!(cast_allowed(ColumnType::Int4, ColumnType::Varchar(Some(3))));
        assert!(cast_allowed(ColumnType::Varchar(Some(3)), ColumnType::Int4));
        assert_eq!(
            cast(
                &Datum::Text("abc".into()),
                ColumnType::Varchar(Some(3)),
                &tz
            )
            .expect("varchar"),
            Datum::Text("abc".into())
        );
        assert_eq!(
            cast(&Datum::Text("a".into()), ColumnType::Char(Some(3)), &tz).expect("char"),
            Datum::Text("a  ".into())
        );
        assert_eq!(
            cast(&Datum::Int4(12), ColumnType::Varchar(Some(3)), &tz).expect("int varchar"),
            Datum::Text("12".into())
        );
        assert!(matches!(
            cast(
                &Datum::Text("abcd".into()),
                ColumnType::Varchar(Some(3)),
                &tz
            ),
            Err(TypeError::StringDataRightTruncation)
        ));
    }

    #[test]
    fn uuid_casts_parse_and_canonicalize_text() {
        let tz = utc();
        let canonical = "550e8400-e29b-41d4-a716-446655440000";
        assert!(cast_allowed(ColumnType::Text, ColumnType::Uuid));
        assert!(cast_allowed(ColumnType::Uuid, ColumnType::Text));
        assert_eq!(
            cast(
                &Datum::Text("{550E8400-E29B-41D4-A716-446655440000}".into()),
                ColumnType::Uuid,
                &tz,
            )
            .expect("uuid"),
            Datum::Text(canonical.into())
        );
        assert!(matches!(
            cast(&Datum::Text("not-a-uuid".into()), ColumnType::Uuid, &tz),
            Err(TypeError::InvalidText {
                type_name: "uuid",
                ..
            })
        ));
    }

    // ---- NULL ----

    #[test]
    fn null_casts_to_null_for_every_target() {
        let tz = utc();
        for t in [
            ColumnType::Bool,
            ColumnType::Int4,
            ColumnType::Int8,
            ColumnType::Text,
            ColumnType::Float8,
        ] {
            assert_eq!(cast(&Datum::Null, t, &tz).expect("null"), Datum::Null);
        }
    }

    // ---- numeric ↔ numeric ----

    #[test]
    fn numeric_widening_and_narrowing() {
        let tz = utc();
        assert_eq!(
            cast(&Datum::Int4(5), ColumnType::Int8, &tz).expect("i4->i8"),
            Datum::Int8(5)
        );
        assert_eq!(
            cast(&Datum::Int4(5), ColumnType::Float8, &tz).expect("i4->f8"),
            Datum::Float8(5.0)
        );
        assert_eq!(
            cast(&Datum::Int8(5), ColumnType::Int4, &tz).expect("i8->i4"),
            Datum::Int4(5)
        );
        // int8 that does not fit int4 is 22003.
        assert!(matches!(
            cast(&Datum::Int8(3_000_000_000), ColumnType::Int4, &tz),
            Err(TypeError::Overflow)
        ));
        assert_eq!(
            cast(&Datum::Int8(9_000_000_000), ColumnType::Float8, &tz).expect("i8->f8"),
            Datum::Float8(9_000_000_000.0)
        );
    }

    #[test]
    fn float_to_int_rounds_half_to_even_and_range_checks() {
        let tz = utc();
        // Round half-to-even (banker's rounding), like PG float8→int (rint).
        for (f, n) in [
            (2.5, 2),
            (3.5, 4),
            (0.5, 0),
            (1.5, 2),
            (-2.5, -2),
            (2.4, 2),
            (2.6, 3),
        ] {
            assert_eq!(
                cast(&Datum::Float8(f), ColumnType::Int4, &tz).expect("f8->i4"),
                Datum::Int4(n),
                "round {f}"
            );
        }
        assert_eq!(
            cast(&Datum::Float8(-3.5), ColumnType::Int8, &tz).expect("f8->i8"),
            Datum::Int8(-4)
        );
        // Out of int4 range, and non-finite, are 22003.
        assert!(matches!(
            cast(&Datum::Float8(3e9), ColumnType::Int4, &tz),
            Err(TypeError::Overflow)
        ));
        assert!(matches!(
            cast(&Datum::Float8(f64::NAN), ColumnType::Int4, &tz),
            Err(TypeError::Overflow)
        ));
        assert!(matches!(
            cast(&Datum::Float8(f64::INFINITY), ColumnType::Int8, &tz),
            Err(TypeError::Overflow)
        ));
    }

    // ---- bool ↔ int4 ----

    #[test]
    fn bool_int4_round_trip() {
        let tz = utc();
        assert_eq!(
            cast(&Datum::Bool(true), ColumnType::Int4, &tz).expect("true->i4"),
            Datum::Int4(1)
        );
        assert_eq!(
            cast(&Datum::Bool(false), ColumnType::Int4, &tz).expect("false->i4"),
            Datum::Int4(0)
        );
        assert_eq!(
            cast(&Datum::Int4(0), ColumnType::Bool, &tz).expect("0->bool"),
            Datum::Bool(false)
        );
        assert_eq!(
            cast(&Datum::Int4(5), ColumnType::Bool, &tz).expect("5->bool"),
            Datum::Bool(true)
        );
        assert_eq!(
            cast(&Datum::Int4(-1), ColumnType::Bool, &tz).expect("-1->bool"),
            Datum::Bool(true)
        );
    }

    // ---- to text ----

    #[test]
    fn to_text_uses_output_form_and_bool_is_true_false() {
        let tz = utc();
        assert_eq!(
            cast(&Datum::Int4(42), ColumnType::Text, &tz).expect("i4->text"),
            Datum::Text("42".into())
        );
        assert_eq!(
            cast(&Datum::Int8(9_000_000_000), ColumnType::Text, &tz).expect("i8->text"),
            Datum::Text("9000000000".into())
        );
        assert_eq!(
            cast(&Datum::Float8(1.5), ColumnType::Text, &tz).expect("f8->text"),
            Datum::Text("1.5".into())
        );
        // bool → text is `true`/`false` (PG `booltext`), NOT `t`/`f`.
        assert_eq!(
            cast(&Datum::Bool(true), ColumnType::Text, &tz).expect("true->text"),
            Datum::Text("true".into())
        );
        assert_eq!(
            cast(&Datum::Bool(false), ColumnType::Text, &tz).expect("false->text"),
            Datum::Text("false".into())
        );
    }

    // ---- text → bool ----

    #[test]
    fn text_to_bool_accepts_postgres_spellings() {
        let tz = utc();
        for s in ["t", "true", "TRUE", "  tr ", "yes", "y", "on", "1"] {
            assert_eq!(
                cast(&Datum::Text(s.into()), ColumnType::Bool, &tz).expect(s),
                Datum::Bool(true),
                "{s:?}"
            );
        }
        for s in ["f", "false", "FALSE", " no ", "n", "off", "0"] {
            assert_eq!(
                cast(&Datum::Text(s.into()), ColumnType::Bool, &tz).expect(s),
                Datum::Bool(false),
                "{s:?}"
            );
        }
        // `o` is the prefix PG resolves to `on` → true (checked before `off`);
        // `of` is a prefix only of `off` → false.
        assert_eq!(
            cast(&Datum::Text("o".into()), ColumnType::Bool, &tz).expect("o"),
            Datum::Bool(true)
        );
        assert_eq!(
            cast(&Datum::Text("of".into()), ColumnType::Bool, &tz).expect("of"),
            Datum::Bool(false)
        );
        for s in ["maybe", "", "  ", "2", "tru e"] {
            assert!(
                matches!(
                    cast(&Datum::Text(s.into()), ColumnType::Bool, &tz),
                    Err(TypeError::InvalidText { .. })
                ),
                "{s:?} should be 22P02"
            );
        }
    }

    // ---- text → int ----

    #[test]
    fn text_to_int_parses_signs_and_distinguishes_syntax_from_overflow() {
        let tz = utc();
        assert_eq!(
            cast(&Datum::Text("42".into()), ColumnType::Int4, &tz).expect("42"),
            Datum::Int4(42)
        );
        assert_eq!(
            cast(&Datum::Text("  -7 ".into()), ColumnType::Int4, &tz).expect("-7"),
            Datum::Int4(-7)
        );
        assert_eq!(
            cast(&Datum::Text("+7".into()), ColumnType::Int4, &tz).expect("+7"),
            Datum::Int4(7)
        );
        assert_eq!(
            cast(&Datum::Text("9000000000".into()), ColumnType::Int8, &tz).expect("i8"),
            Datum::Int8(9_000_000_000)
        );
        // Bad syntax (decimal point, letters, empty, lone sign) → 22P02.
        for s in ["1.5", "abc", "", "  ", "-", "1e3", "0x10"] {
            assert!(
                matches!(
                    cast(&Datum::Text(s.into()), ColumnType::Int4, &tz),
                    Err(TypeError::InvalidText { .. })
                ),
                "{s:?} should be 22P02"
            );
        }
        // Well-formed but out of range → 22003 (NOT 22P02).
        assert!(matches!(
            cast(&Datum::Text("99999999999".into()), ColumnType::Int4, &tz),
            Err(TypeError::Overflow)
        ));
        assert!(matches!(
            cast(
                &Datum::Text("99999999999999999999".into()),
                ColumnType::Int8,
                &tz,
            ),
            Err(TypeError::Overflow)
        ));
    }

    // ---- text → float8 ----

    #[test]
    fn text_to_float_parses_finite_specials_and_overflow() {
        let tz = utc();
        assert_eq!(
            cast(&Datum::Text("1.5".into()), ColumnType::Float8, &tz).expect("1.5"),
            Datum::Float8(1.5)
        );
        assert_eq!(
            cast(&Datum::Text(" 2 ".into()), ColumnType::Float8, &tz).expect("2"),
            Datum::Float8(2.0)
        );
        assert_eq!(
            cast(&Datum::Text("1e3".into()), ColumnType::Float8, &tz).expect("1e3"),
            Datum::Float8(1000.0)
        );
        // Explicit infinity / NaN spellings are values, not errors.
        assert_eq!(
            cast(&Datum::Text("Infinity".into()), ColumnType::Float8, &tz).expect("inf"),
            Datum::Float8(f64::INFINITY)
        );
        assert_eq!(
            cast(&Datum::Text("-inf".into()), ColumnType::Float8, &tz).expect("-inf"),
            Datum::Float8(f64::NEG_INFINITY)
        );
        assert!(matches!(
            cast(&Datum::Text("nan".into()), ColumnType::Float8, &tz),
            Ok(Datum::Float8(f)) if f.is_nan()
        ));
        // A finite literal that overflows to ∞ is 22003, NOT the value Infinity.
        assert!(matches!(
            cast(&Datum::Text("1e400".into()), ColumnType::Float8, &tz),
            Err(TypeError::Overflow)
        ));
        // Garbage is 22P02.
        assert!(matches!(
            cast(&Datum::Text("1.2.3".into()), ColumnType::Float8, &tz),
            Err(TypeError::InvalidText { .. })
        ));
    }

    // ---- datetime cast matrix ----

    #[test]
    fn datetime_cast_matrix() {
        use ColumnType::{Date, Text, Time, Timestamp, Timestamptz};
        let utc = &jiff::tz::TimeZone::UTC;
        let d = Datum::Date(crate::datetime::parse_date("2024-01-15").expect("d"));
        assert_eq!(
            cast(&Datum::Text("2024-01-15".into()), Date, utc).expect("t->d"),
            d
        );
        assert_eq!(
            cast(&d, Text, utc).expect("d->t"),
            Datum::Text("2024-01-15".into())
        );
        assert_eq!(
            cast(&d, Timestamp, utc).expect("d->ts"),
            Datum::Timestamp(crate::datetime::parse_timestamp("2024-01-15 00:00:00").expect("ts"))
        );
        let ts =
            Datum::Timestamp(crate::datetime::parse_timestamp("2024-01-15 13:45:06").expect("ts"));
        assert_eq!(cast(&ts, Date, utc).expect("ts->d"), d);
        assert_eq!(
            cast(&ts, Time, utc).expect("ts->t"),
            Datum::Time(crate::datetime::parse_time("13:45:06").expect("tm"))
        );
        assert!(matches!(
            cast(&Datum::Int4(1), Date, utc),
            Err(crate::TypeError::CannotCast { .. })
        ));
        assert!(cast_allowed(Date, Timestamptz));
        assert!(!cast_allowed(ColumnType::Interval, Date));
        assert!(!cast_allowed(ColumnType::Int4, Date));
    }

    #[test]
    fn datetime_cast_text_round_trips() {
        use ColumnType::{Date, Interval, Time, Timestamp, Timestamptz};
        let utc = &jiff::tz::TimeZone::UTC;

        // text → date → text
        let d = cast(&Datum::Text("2024-03-01".into()), Date, utc).expect("text->date");
        assert_eq!(
            cast(&d, ColumnType::Text, utc).expect("date->text"),
            Datum::Text("2024-03-01".into())
        );

        // text → time → text
        let t = cast(&Datum::Text("13:45:06".into()), Time, utc).expect("text->time");
        assert_eq!(
            cast(&t, ColumnType::Text, utc).expect("time->text"),
            Datum::Text("13:45:06".into())
        );

        // text → timestamp → text
        let ts = cast(&Datum::Text("2024-03-01 13:45:06".into()), Timestamp, utc)
            .expect("text->timestamp");
        assert_eq!(
            cast(&ts, ColumnType::Text, utc).expect("ts->text"),
            Datum::Text("2024-03-01 13:45:06".into())
        );

        // text → timestamptz → text (UTC stays at +00)
        let tstz = cast(
            &Datum::Text("2024-03-01 13:45:06+00".into()),
            Timestamptz,
            utc,
        )
        .expect("text->tstz");
        assert_eq!(
            cast(&tstz, ColumnType::Text, utc).expect("tstz->text"),
            Datum::Text("2024-03-01 13:45:06+00".into())
        );

        // text → interval → text
        let iv = cast(&Datum::Text("1 day".into()), Interval, utc).expect("text->interval");
        assert_eq!(
            cast(&iv, ColumnType::Text, utc).expect("iv->text"),
            Datum::Text("1 day".into())
        );
    }

    #[test]
    fn datetime_cross_type_casts() {
        use ColumnType::{Date, Time, Timestamp, Timestamptz};
        let utc = &jiff::tz::TimeZone::UTC;

        // date → timestamptz (midnight UTC)
        let d = crate::datetime::parse_date("2024-06-01").expect("date");
        let tstz = cast(&Datum::Date(d), Timestamptz, utc).expect("date->tstz");
        // Should render as midnight UTC
        assert_eq!(
            cast(&tstz, ColumnType::Text, utc).expect("tstz->text"),
            Datum::Text("2024-06-01 00:00:00+00".into())
        );

        // timestamp → timestamptz (interpreted as UTC)
        let ts_val = crate::datetime::parse_timestamp("2024-06-01 12:00:00").expect("ts");
        let tstz2 = cast(&Datum::Timestamp(ts_val), Timestamptz, utc).expect("ts->tstz");
        assert_eq!(
            cast(&tstz2, ColumnType::Text, utc).expect("tstz->text"),
            Datum::Text("2024-06-01 12:00:00+00".into())
        );

        // timestamptz → timestamp (render in UTC)
        let tstz3 =
            crate::datetime::parse_timestamptz("2024-06-01 15:30:00+00", utc).expect("tstz");
        let ts_back = cast(&Datum::Timestamptz(tstz3), Timestamp, utc).expect("tstz->ts");
        assert_eq!(
            cast(&ts_back, ColumnType::Text, utc).expect("ts->text"),
            Datum::Text("2024-06-01 15:30:00".into())
        );

        // timestamptz → date (render in UTC)
        let d_back = cast(&Datum::Timestamptz(tstz3), Date, utc).expect("tstz->date");
        assert_eq!(
            cast(&d_back, ColumnType::Text, utc).expect("date->text"),
            Datum::Text("2024-06-01".into())
        );

        // timestamptz → time (render in UTC)
        let t_back = cast(&Datum::Timestamptz(tstz3), Time, utc).expect("tstz->time");
        assert_eq!(
            cast(&t_back, ColumnType::Text, utc).expect("time->text"),
            Datum::Text("15:30:00".into())
        );
    }

    #[test]
    fn datetime_undefined_casts_are_42846() {
        let utc = &jiff::tz::TimeZone::UTC;
        use ColumnType::{Bool, Date, Int4, Interval, Time, Timestamp, Timestamptz};

        // numeric/bool ↔ temporal
        let d = Datum::Date(crate::datetime::parse_date("2024-01-01").expect("d"));
        assert!(matches!(
            cast(&d, Int4, utc),
            Err(TypeError::CannotCast { .. })
        ));
        assert!(matches!(
            cast(&d, Bool, utc),
            Err(TypeError::CannotCast { .. })
        ));
        assert!(matches!(
            cast(&Datum::Int4(1), Date, utc),
            Err(TypeError::CannotCast { .. })
        ));
        assert!(matches!(
            cast(&Datum::Bool(true), Timestamp, utc),
            Err(TypeError::CannotCast { .. })
        ));

        // interval ↔ date/time/timestamp/timestamptz
        let iv = Datum::Interval(crate::datetime::Interval {
            months: 1,
            days: 0,
            micros: 0,
        });
        assert!(matches!(
            cast(&iv, Date, utc),
            Err(TypeError::CannotCast { .. })
        ));
        assert!(matches!(
            cast(&iv, Time, utc),
            Err(TypeError::CannotCast { .. })
        ));
        assert!(matches!(
            cast(&iv, Timestamp, utc),
            Err(TypeError::CannotCast { .. })
        ));
        assert!(matches!(
            cast(&iv, Timestamptz, utc),
            Err(TypeError::CannotCast { .. })
        ));

        // cast_allowed is false for these
        assert!(!cast_allowed(Interval, Date));
        assert!(!cast_allowed(Interval, Time));
        assert!(!cast_allowed(Interval, Timestamp));
        assert!(!cast_allowed(Interval, Timestamptz));
        assert!(!cast_allowed(Date, Interval));
        assert!(!cast_allowed(Int4, Date));
        assert!(!cast_allowed(Date, Int4));
        assert!(!cast_allowed(Bool, Timestamp));
        assert!(!cast_allowed(Time, Timestamp)); // time→timestamp is NOT in PG's standard matrix
    }

    #[test]
    fn datetime_identity_and_tz_direction_casts() {
        use ColumnType::{Date, Time, Timestamp, Timestamptz};
        // Identity casts (the Critical fix): each temporal -> itself returns the same value.
        let utc = &jiff::tz::TimeZone::UTC;
        let d = Datum::Date(crate::datetime::parse_date("2024-01-15").expect("d"));
        assert_eq!(cast(&d, Date, utc).expect("d->d"), d);
        let tm = Datum::Time(crate::datetime::parse_time("13:45:06").expect("t"));
        assert_eq!(cast(&tm, Time, utc).expect("t->t"), tm);
        let ts =
            Datum::Timestamp(crate::datetime::parse_timestamp("2024-06-01 00:00:00").expect("ts"));
        assert_eq!(cast(&ts, Timestamp, utc).expect("ts->ts"), ts);
        let iv = Datum::Interval(crate::datetime::Interval {
            months: 1,
            days: 2,
            micros: 3,
        });
        assert_eq!(cast(&iv, ColumnType::Interval, utc).expect("iv->iv"), iv);

        // tz direction: timestamp '2024-06-01 00:00:00' interpreted in NY (EDT=-04) = 04:00 UTC.
        let ny = &jiff::tz::TimeZone::get("America/New_York").expect("ny");
        let tstz = cast(&ts, Timestamptz, ny).expect("ts->tstz");
        // render the resulting instant in UTC: must be 2024-06-01 04:00:00+00.
        assert_eq!(
            String::from_utf8(crate::encoding::encode_text(&tstz, utc)).expect("utf8"),
            "2024-06-01 04:00:00+00"
        );
        // reverse: timestamptz -> timestamp in NY must give back the 00:00 wall clock.
        assert_eq!(cast(&tstz, Timestamp, ny).expect("tstz->ts"), ts);
        // timestamptz -> date in NY = 2024-06-01.
        assert_eq!(
            cast(&tstz, Date, ny).expect("tstz->date"),
            Datum::Date(crate::datetime::parse_date("2024-06-01").expect("d2"))
        );
    }

    // ---- undefined casts ----

    #[test]
    fn undefined_casts_are_42846_with_type_names() {
        let tz = utc();
        let err = cast(&Datum::Float8(1.5), ColumnType::Bool, &tz).expect_err("f8->bool");
        assert_eq!(err.sqlstate(), "42846");
        assert_eq!(
            err,
            TypeError::CannotCast {
                from: "double precision",
                to: "boolean",
            }
        );
        assert!(matches!(
            cast(&Datum::Int8(1), ColumnType::Bool, &tz),
            Err(TypeError::CannotCast { .. })
        ));
        assert!(matches!(
            cast(&Datum::Bool(true), ColumnType::Int8, &tz),
            Err(TypeError::CannotCast { .. })
        ));
        assert!(matches!(
            cast(&Datum::Bool(true), ColumnType::Float8, &tz),
            Err(TypeError::CannotCast { .. })
        ));
    }
}
