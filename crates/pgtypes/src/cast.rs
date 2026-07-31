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

use crate::{ColumnType, Datum, TypeError, string::Coercion};

/// Is an explicit cast from `from` to `to` defined among the slice's types? Used
/// at plan time so an undefined cast surfaces as 42846 before execution, and so
/// the result column type is known for `RowDescription`.
pub fn cast_allowed(from: ColumnType, to: ColumnType) -> bool {
    use ColumnType::{Bool, Date, Int4, Text, Time, Timestamp, Timestamptz};
    // SP32: the numeric family — int2/int4/int8/float4/float8/numeric — all
    // interconvert. `bool` is deliberately NOT in it: PostgreSQL has bool↔int4
    // only, so `true::int2` and `1::int2::bool` are 42846 on both sides.
    let num_family = |t: ColumnType| {
        matches!(
            t,
            ColumnType::Int2
                | ColumnType::Int4
                | ColumnType::Int8
                | ColumnType::Float4
                | ColumnType::Float8
        ) || t.is_numeric()
    };
    match (from, to) {
        // Identity (e.g. numeric → numeric, even across differing typmods).
        (a, b) if a == b => true,
        // A domain casts exactly as its base type does, in both directions:
        // `PostgreSQL` coerces through the base and then applies the domain's
        // constraints. This arm must come first so a domain never falls into
        // the string rules below on the strength of its *name*.
        (ColumnType::Domain(d), _) => cast_allowed(*d.base, to),
        (_, ColumnType::Domain(d)) => cast_allowed(from, *d.base),
        // Composite → composite is allowed whenever the field lists line up;
        // the field-wise check happens at conversion, so the plan-time answer
        // is "yes, a record coercion exists".
        (ColumnType::Record(_), ColumnType::Record(_)) => true,
        // A record only otherwise converts through its text form.
        (ColumnType::Record(_), _) | (_, ColumnType::Record(_)) => {
            from.is_string() || to.is_string()
        }
        // An enum converts with the string family (`enum_in`/`enum_out`) and
        // with itself; nothing else.
        (ColumnType::Enum(_), _) | (_, ColumnType::Enum(_)) => from.is_string() || to.is_string(),
        // Array → array whenever the element cast is defined (PostgreSQL builds
        // an array coercion from the element coercion). Must precede the string
        // rules so `text[] → int4[]` is judged element-wise, not as a string cast.
        (ColumnType::Array(a), ColumnType::Array(b)) => {
            cast_allowed(a.column_type(), b.column_type())
        }
        // `jsonb` and arrays otherwise interconvert ONLY with the string family
        // (the rule below): PostgreSQL has no jsonb/array ↔ number/bool/temporal
        // cast, and this arm keeps the permissive numeric rules from claiming one.
        (ColumnType::Jsonb | ColumnType::Array(_), _)
        | (_, ColumnType::Jsonb | ColumnType::Array(_))
            if !from.is_string() && !to.is_string() =>
        {
            false
        }
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
        // timestamptz → {date, time, timestamp, timetz}
        (Timestamptz, Date) | (Timestamptz, Time) | (Timestamptz, Timestamp) => true,
        // time ↔ timetz. PostgreSQL has no `timestamp → timetz`, because a civil
        // timestamp carries no offset to attach.
        (Time, ColumnType::Timetz) | (ColumnType::Timetz, Time) => true,
        (Timestamptz, ColumnType::Timetz) => true,
        // Everything else — including numeric/bool ↔ temporal, interval ↔ temporal,
        // and time → timestamp/timestamptz: undefined → 42846.
        _ => false,
    }
}

/// Is an *implicit-or-assignment* cast from `from` to `to` defined — the pairs
/// PostgreSQL 18's `pg_cast` marks `castcontext` `'i'` or `'a'`, restricted to
/// crabka's types? A strict SUBSET of [`cast_allowed`]: assignment (INSERT /
/// UPDATE SET into a column) converts through these pairs automatically, while
/// everything else keeps requiring an explicit `CAST`.
///
/// The allowed pairs and their `pg_cast` contexts:
///   * identity `T → T` (no cast needed);
///   * numeric family (`int4`/`int8`/`float8`/`numeric`) interconversion —
///     widenings are `'i'`, narrowings are `'a'`;
///   * string family (`text`/`varchar`/`char`) interconversion — `'i'`/`'a'`
///     (length re-coercion applies at assignment);
///   * `date → timestamp` and `date → timestamptz` — `'i'`;
///   * `timestamp → timestamptz` — `'i'`; `timestamptz → timestamp` — `'a'`
///     (both rotate through the session time zone).
///
/// Deliberately NOT allowed (explicit-only in this matrix):
///   * non-string ↔ string (PostgreSQL's I/O-conversion casts are
///     explicit-only since 8.3 — `INSERT` of an `int4` into a `text` column
///     errors, and vice versa);
///   * `bool ↔ int4` (`castcontext` `'e'`);
///   * `timestamp`/`timestamptz` → `date`/`time` (kept explicit-only here as a
///     conservative subset, though PostgreSQL marks these `'a'`);
///   * everything involving `interval`, `bytea`, `uuid`, `regclass` across
///     type families.
pub fn assignment_cast_allowed(from: ColumnType, to: ColumnType) -> bool {
    use ColumnType::{Date, Timestamp, Timestamptz};
    let num_family = |t: ColumnType| {
        matches!(
            t,
            ColumnType::Int2
                | ColumnType::Int4
                | ColumnType::Int8
                | ColumnType::Float4
                | ColumnType::Float8
        ) || t.is_numeric()
    };
    match (from, to) {
        (a, b) if a == b => true,
        _ if num_family(from) && num_family(to) => true,
        _ if from.is_string() && to.is_string() => true,
        (Date, Timestamp | Timestamptz) | (Timestamp, Timestamptz) | (Timestamptz, Timestamp) => {
            true
        }
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
    cast_in(value, to, crate::encoding::OutputStyle::with_zone(tz))
}

/// [`cast`] under assignment rules rather than explicit-cast rules.
///
/// The two contexts differ only in how a `varchar(n)`/`char(n)` target treats an
/// over-long value: an explicit cast truncates it, while an assignment rejects it
/// with `string_data_right_truncation` unless the discarded characters are all
/// spaces. Use this wherever a value is being *stored* — a column, a routine
/// parameter — and [`cast`] for a cast the query wrote out.
///
/// # Errors
///
/// As [`cast`], plus 22001 for an over-long bounded-string assignment.
pub fn cast_assign(
    value: &Datum,
    to: ColumnType,
    tz: &jiff::tz::TimeZone,
) -> Result<Datum, TypeError> {
    // Cast to the *unbounded* type first so `cast_in` applies no modifier of its
    // own, then apply the modifier under assignment rules. Splitting it this way
    // keeps one implementation of every cast arm.
    match to {
        ColumnType::Varchar(Some(_)) | ColumnType::Char(Some(_)) => {
            let unbounded = cast(value, unbounded_string(to), tz)?;
            bounded_string(&unbounded, to)
        }
        // An array of a bounded string enforces the bound on every element: the
        // modifier rides on the element type, so `varchar(3)[]` rejects a row
        // whose element is over-long just as `varchar(3)` does.
        ColumnType::Array(elem)
            if matches!(
                elem,
                crate::ElemType::Varchar(Some(_)) | crate::ElemType::Char(Some(_))
            ) =>
        {
            let unbounded = crate::ElemType::from_column_type(unbounded_string(elem.column_type()))
                .expect("an unbounded string is an element type");
            let Datum::Array(mut array) = cast(value, ColumnType::Array(unbounded), tz)? else {
                return Ok(Datum::Null);
            };
            for element in &mut array.elems {
                *element = bounded_string(element, elem.column_type())?;
            }
            array.elem = elem;
            Ok(Datum::Array(array))
        }
        _ => cast(value, to, tz),
    }
}

/// The same string type with its length modifier removed.
fn unbounded_string(to: ColumnType) -> ColumnType {
    match to {
        ColumnType::Char(_) => ColumnType::Char(None),
        _ => ColumnType::Varchar(None),
    }
}

/// Apply a bounded string type's modifier to an already-rendered value under
/// assignment rules. A NULL passes through untouched.
fn bounded_string(value: &Datum, to: ColumnType) -> Result<Datum, TypeError> {
    let Datum::Text(text) = value else {
        return Ok(value.clone());
    };
    match to {
        ColumnType::Char(n) => {
            crate::string::apply_char_typmod(text, n, Coercion::Assignment).map(Datum::Text)
        }
        ColumnType::Varchar(n) => {
            crate::string::apply_varchar_typmod(text, n, Coercion::Assignment).map(Datum::Text)
        }
        _ => Ok(value.clone()),
    }
}

/// [`cast`] with the session's `DateStyle` field order, which decides how an
/// otherwise-ambiguous all-numeric date literal (`01/02/03`) is read on the
/// `text → date`/`timestamp`/`timestamptz` arms. Every other arm ignores it.
pub fn cast_in(
    value: &Datum,
    to: ColumnType,
    style: crate::encoding::OutputStyle<'_>,
) -> Result<Datum, TypeError> {
    use ColumnType::{Bool, Float4, Float8, Int2, Int4, Int8, Numeric, Text};
    let tz = style.time_zone;
    let order = style.date_order;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    // The user-defined types are matched ahead of the built-in table because a
    // composite, an enum or a domain would otherwise be caught by the
    // string-family fall-throughs on the strength of its `name()`.
    if let Some(converted) = cast_user_type(value, to, style)? {
        return Ok(converted);
    }
    match (value, to) {
        // Identity (each variant to its own type).
        (Datum::Bool(b), Bool) => Ok(Datum::Bool(*b)),
        (Datum::Int2(n), Int2) => Ok(Datum::Int2(*n)),
        (Datum::Int4(n), Int4) => Ok(Datum::Int4(*n)),
        (Datum::Int8(n), Int8) => Ok(Datum::Int8(*n)),
        (Datum::Float4(f), Float4) => Ok(Datum::Float4(*f)),
        (Datum::Float8(f), Float8) => Ok(Datum::Float8(*f)),
        (Datum::Text(s), Text) => Ok(Datum::Text(s.clone())),
        (Datum::Text(s), ColumnType::Varchar(n)) => {
            crate::string::apply_varchar_typmod(s, n, Coercion::Explicit).map(Datum::Text)
        }
        (Datum::Text(s), ColumnType::Char(n)) => {
            crate::string::apply_char_typmod(s, n, Coercion::Explicit).map(Datum::Text)
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
        // jsonb / array identity and text conversions. These must be explicit:
        // `cast_allowed` says yes for `T → T` and for anything ↔ the string
        // family, so without them a runtime cast would wrongly report 42846.
        (Datum::Jsonb(j), ColumnType::Jsonb) => Ok(Datum::Jsonb(j.clone())),
        // text → jsonb is `jsonb_in` (22P02 on bad JSON).
        (Datum::Text(s), ColumnType::Jsonb) => crate::jsonb::parse(s).map(Datum::Jsonb),
        // text → array is `array_in`: split the literal, then run the element
        // type's input function over each element.
        (Datum::Text(s), ColumnType::Array(elem)) => {
            let raw = crate::array::parse_literal(s)?;
            let elems = raw
                .elements
                .into_iter()
                .map(|e| match e {
                    None => Ok(Datum::Null),
                    Some(text) => cast(&Datum::Text(text), elem.column_type(), tz),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Datum::Array(crate::datum::ArrayValue::with_dims(
                elem, elems, raw.dims,
            )))
        }
        // array → array: identity when the element types match, element-wise
        // conversion otherwise (PostgreSQL's array coercion).
        (Datum::Array(a), ColumnType::Array(elem)) => {
            if a.elem == elem {
                return Ok(Datum::Array(a.clone()));
            }
            let elems = a
                .elems
                .iter()
                .map(|e| cast(e, elem.column_type(), tz))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Datum::Array(crate::datum::ArrayValue::with_dims(
                elem,
                elems,
                a.dims.clone(),
            )))
        }
        // Numeric (int/float) ↔ numeric (int/float).
        (Datum::Int2(n), Int4) => Ok(Datum::Int4(i32::from(*n))),
        (Datum::Int2(n), Int8) => Ok(Datum::Int8(i64::from(*n))),
        (Datum::Int2(n), Float4) => Ok(Datum::Float4(f32::from(*n))),
        (Datum::Int2(n), Float8) => Ok(Datum::Float8(f64::from(*n))),
        (Datum::Int4(n), Int2) => i2_from_i64(i64::from(*n)),
        (Datum::Int4(n), Int8) => Ok(Datum::Int8(i64::from(*n))),
        (Datum::Int4(n), Float4) => Ok(Datum::Float4(*n as f32)),
        (Datum::Int4(n), Float8) => Ok(Datum::Float8(f64::from(*n))),
        (Datum::Int8(n), Int2) => i2_from_i64(*n),
        (Datum::Int8(n), Int4) => i4_from_i64(*n),
        (Datum::Int8(n), Float4) => Ok(Datum::Float4(*n as f32)),
        (Datum::Int8(n), Float8) => Ok(Datum::Float8(*n as f64)),
        // `float4 → float8` is exact widening; `float8 → float4` narrows with
        // PostgreSQL's `dtof` overflow/underflow checks.
        (Datum::Float4(f), Float8) => Ok(Datum::Float8(f64::from(*f))),
        (Datum::Float4(f), Int2) => i2_from_f64(f64::from(*f)),
        (Datum::Float4(f), Int4) => i4_from_f64(f64::from(*f)),
        (Datum::Float4(f), Int8) => i8_from_f64(f64::from(*f)),
        (Datum::Float8(f), Float4) => f4_from_f64(*f),
        (Datum::Float8(f), Int2) => i2_from_f64(*f),
        (Datum::Float8(f), Int4) => i4_from_f64(*f),
        (Datum::Float8(f), Int8) => i8_from_f64(*f),
        // SP32: → numeric (applying any `numeric(p,s)` modifier on the target).
        (Datum::Int2(n), Numeric(tm)) => to_numeric(crate::numeric::from_i64(i64::from(*n)), tm),
        (Datum::Int4(n), Numeric(tm)) => to_numeric(crate::numeric::from_i64(i64::from(*n)), tm),
        (Datum::Int8(n), Numeric(tm)) => to_numeric(crate::numeric::from_i64(*n), tm),
        (Datum::Float4(f), Numeric(tm)) => to_numeric(crate::numeric::from_f32(*f), tm),
        (Datum::Float8(f), Numeric(tm)) => to_numeric(crate::numeric::from_f64(*f), tm),
        (Datum::Numeric(d), Numeric(tm)) => to_numeric(d.clone(), tm),
        // SP32: numeric → int/float/text. A special value has no integer form —
        // PostgreSQL raises 0A000 `cannot convert NaN to smallint` and friends,
        // which `numeric::to_i16`/`to_i32`/`to_i64` spell per target type.
        (Datum::Numeric(d), Int2) => crate::numeric::to_i16(d).map(Datum::Int2),
        (Datum::Numeric(d), Int4) => crate::numeric::to_i32(d).map(Datum::Int4),
        (Datum::Numeric(d), Int8) => crate::numeric::to_i64(d).map(Datum::Int8),
        (Datum::Numeric(d), Float4) => f4_from_f64(crate::numeric::to_f64(d)),
        (Datum::Numeric(d), Float8) => Ok(Datum::Float8(crate::numeric::to_f64(d))),
        // bool ↔ int4.
        (Datum::Bool(b), Int4) => Ok(Datum::Int4(i32::from(*b))),
        (Datum::Int4(n), Bool) => Ok(Datum::Bool(*n != 0)),
        // → text. `bool` renders as PostgreSQL's `booltext` cast (`true`/`false`),
        // NOT the `t`/`f` of `boolout`; the others reuse the wire text encoding.
        (Datum::Bool(b), Text) => Ok(Datum::Text((if *b { "true" } else { "false" }).into())),
        (d, Text) => Ok(Datum::Text(text_of(d, style))),
        (d, ColumnType::Varchar(n)) => {
            crate::string::apply_varchar_typmod(&string_cast_input(d, style), n, Coercion::Explicit)
                .map(Datum::Text)
        }
        (d, ColumnType::Char(n)) => {
            crate::string::apply_char_typmod(&string_cast_input(d, style), n, Coercion::Explicit)
                .map(Datum::Text)
        }
        // text → other.
        (Datum::Text(s), Bool) => text_to_bool(s),
        (Datum::Text(s), Int2) => text_to_i16(s),
        (Datum::Text(s), Int4) => text_to_i32(s),
        (Datum::Text(s), Int8) => text_to_i64(s),
        (Datum::Text(s), Float4) => text_to_f32(s),
        (Datum::Text(s), Float8) => text_to_f64(s),
        // `regclass` → the oid family drops the name and keeps the oid, which is
        // what `regclass::oid`/`::int` yields in PostgreSQL.
        (Datum::Regclass(r), ColumnType::Regclass) => Ok(Datum::Regclass(r.clone())),
        (Datum::Regclass(r), Int4) => Ok(Datum::Int4(r.oid)),
        (Datum::Regclass(r), Int8) => Ok(Datum::Int8(i64::from(r.oid))),
        // → `regclass`. The pure cast has no catalog, so it can only produce the
        // unresolved rendering (`regclassout`'s bare-oid fallback); the executor
        // resolves the name before reaching here when a catalog is in scope. A
        // relation NAME likewise needs the catalog — a non-numeric string that
        // falls through is 22P02, mirroring an unresolvable input.
        (Datum::Int4(n), ColumnType::Regclass) => {
            Ok(Datum::Regclass(crate::RegclassValue::unresolved(*n)))
        }
        (Datum::Int8(n), ColumnType::Regclass) => i4_from_i64(*n).map(|d| match d {
            Datum::Int4(n) => Datum::Regclass(crate::RegclassValue::unresolved(n)),
            other => other,
        }),
        (Datum::Text(s), ColumnType::Regclass) => s
            .trim()
            .parse::<i32>()
            .map(|n| Datum::Regclass(crate::RegclassValue::unresolved(n)))
            .map_err(|_| TypeError::InvalidText {
                type_name: "regclass",
                value: s.clone(),
            }),
        (Datum::Text(s), Numeric(tm)) => {
            let d = crate::numeric::parse(s).ok_or_else(|| TypeError::InvalidText {
                type_name: "numeric",
                value: s.to_string(),
            })?;
            to_numeric(d, tm)
        }
        // SP37: text → temporal (parse errors propagate as 22007/22008/InvalidText).
        (Datum::Text(s), ColumnType::Date) => {
            crate::datetime::parse_date_in(s, order, tz).map(Datum::Date)
        }
        (Datum::Text(s), ColumnType::Time) => {
            crate::datetime::parse_time_in(s, order, tz).map(Datum::Time)
        }
        (Datum::Text(s), ColumnType::Timestamp) => {
            crate::datetime::parse_timestamp_in(s, order, tz).map(Datum::Timestamp)
        }
        (Datum::Text(s), ColumnType::Timestamptz) => {
            crate::datetime::parse_timestamptz_in(s, order, tz).map(Datum::Timestamptz)
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
        // date → timestamp: midnight (no timezone involved). A non-finite date
        // casts to the non-finite timestamp of the same sign — every temporal
        // cast below carries infinity through rather than computing with it.
        (Datum::Date(d), ColumnType::Timestamp) => match crate::datetime::date_infinite_sign(*d) {
            0 => Ok(Datum::Timestamp(crate::datetime::date_to_midnight(*d))),
            sign => Ok(Datum::Timestamp(
                crate::datetime::timestamp_infinity_of_sign(sign),
            )),
        },
        // date → timestamptz: midnight in the session tz → absolute instant.
        (Datum::Date(d), ColumnType::Timestamptz) => {
            match crate::datetime::date_infinite_sign(*d) {
                0 => crate::datetime::date_to_midnight(*d)
                    .to_zoned(tz.clone())
                    .map(|z| Datum::Timestamptz(z.timestamp()))
                    .map_err(|_| TypeError::DatetimeFieldOverflow {
                        value: format!("{d}"),
                    }),
                sign => Ok(Datum::Timestamptz(
                    crate::datetime::timestamptz_infinity_of_sign(sign),
                )),
            }
        }
        // timestamp → date: truncate to date part.
        (Datum::Timestamp(dt), ColumnType::Date) => {
            match crate::datetime::timestamp_infinite_sign(*dt) {
                0 => Ok(Datum::Date(dt.date())),
                sign => Ok(Datum::Date(crate::datetime::date_infinity_of_sign(sign))),
            }
        }
        // timestamp → time: extract time-of-day. A non-finite timestamp has no
        // time-of-day at all, and PostgreSQL yields NULL rather than erroring.
        (Datum::Timestamp(dt), ColumnType::Time) => {
            if crate::datetime::timestamp_is_infinite(*dt) {
                return Ok(Datum::Null);
            }
            Ok(Datum::Time(dt.time()))
        }
        // timestamp → timestamptz: interpret wall-clock as session tz → instant.
        (Datum::Timestamp(dt), ColumnType::Timestamptz) => {
            match crate::datetime::timestamp_infinite_sign(*dt) {
                0 => dt
                    .to_zoned(tz.clone())
                    .map(|z| Datum::Timestamptz(z.timestamp()))
                    .map_err(|_| TypeError::DatetimeFieldOverflow {
                        value: format!("{dt}"),
                    }),
                sign => Ok(Datum::Timestamptz(
                    crate::datetime::timestamptz_infinity_of_sign(sign),
                )),
            }
        }
        // timestamptz → timestamp: render instant in session tz → wall-clock datetime.
        (Datum::Timestamptz(ts), ColumnType::Timestamp) => {
            match crate::datetime::timestamptz_infinite_sign(*ts) {
                0 => Ok(Datum::Timestamp(tz.to_datetime(*ts))),
                sign => Ok(Datum::Timestamp(
                    crate::datetime::timestamp_infinity_of_sign(sign),
                )),
            }
        }
        // timestamptz → date: render in session tz, take date part.
        (Datum::Timestamptz(ts), ColumnType::Date) => {
            match crate::datetime::timestamptz_infinite_sign(*ts) {
                0 => Ok(Datum::Date(tz.to_datetime(*ts).date())),
                sign => Ok(Datum::Date(crate::datetime::date_infinity_of_sign(sign))),
            }
        }
        // timestamptz → time: render in session tz, take time-of-day.
        (Datum::Timestamptz(ts), ColumnType::Time) => {
            if crate::datetime::timestamptz_is_infinite(*ts) {
                return Ok(Datum::Null);
            }
            Ok(Datum::Time(tz.to_datetime(*ts).time()))
        }
        // timetz identity, and the two casts PostgreSQL defines for it. A `time`
        // gains the session zone's offset; a `timestamptz` keeps its own.
        (Datum::Timetz(t), ColumnType::Timetz) => Ok(Datum::Timetz(*t)),
        (Datum::Timetz(t), ColumnType::Time) => Ok(Datum::Time(t.time)),
        (Datum::Time(t), ColumnType::Timetz) => Ok(Datum::Timetz(crate::datetime::TimeTz {
            time: *t,
            offset: tz.to_offset(jiff::Timestamp::now()),
        })),
        (Datum::Timestamptz(ts), ColumnType::Timetz) => {
            if crate::datetime::timestamptz_is_infinite(*ts) {
                return Ok(Datum::Null);
            }
            Ok(Datum::Timetz(crate::datetime::TimeTz {
                time: tz.to_datetime(*ts).time(),
                offset: tz.to_offset(*ts),
            }))
        }
        (Datum::Text(s), ColumnType::Timetz) => {
            crate::datetime::parse_timetz_in(s, order, tz).map(Datum::Timetz)
        }
        // No defined cast.
        (v, to) => Err(cannot_cast(v, to)),
    }
}

/// Wrap a value as a numeric `Datum`, applying a `numeric(p,s)` modifier
/// (round to scale + precision overflow → 22003) when the target carries one.
/// Casts to and from the user-defined types, `Ok(None)` when neither side is
/// one and the built-in table should decide.
///
/// A domain is unwrapped to its base — the value of a domain *is* a base value,
/// and the domain's own constraints are checked by the executor, which is the
/// only layer that can evaluate their `CHECK` expressions. A composite converts
/// from its text form (`record_in`), from another composite field by field, and
/// to text (`record_out`). An enum converts from and to text only.
fn cast_user_type(
    value: &Datum,
    to: ColumnType,
    style: crate::encoding::OutputStyle<'_>,
) -> Result<Option<Datum>, TypeError> {
    // Casting *to* a domain is casting to its base: the constraint check is the
    // executor's, and it needs the base value to test.
    if let ColumnType::Domain(domain) = to {
        return cast_in(value, *domain.base, style).map(Some);
    }
    match (value, to) {
        (Datum::Record(r), ColumnType::Record(target)) => Ok(Some(cast_record(r, target, style)?)),
        (Datum::Text(text), ColumnType::Record(Some(target))) => {
            Ok(Some(record_from_text(text, target, style)?))
        }
        // `record_in` for the anonymous `record` has no attribute list to type
        // the fields with, which is exactly why PostgreSQL refuses `'…'::record`
        // ("input of anonymous composite types is not implemented", 0A000).
        (Datum::Text(_), ColumnType::Record(None)) => Err(TypeError::Coded {
            sqlstate: "0A000",
            message: "input of anonymous composite types is not implemented".into(),
        }),
        (Datum::Record(_), _) if to.is_string() => {
            Ok(Some(string_result(text_of(value, style), to)?))
        }
        (Datum::Text(text), ColumnType::Enum(target)) => Ok(Some(enum_from_text(text, target)?)),
        (Datum::Enum(_), ColumnType::Enum(target)) if value_enum_type(value) == Some(target) => {
            Ok(Some(value.clone()))
        }
        (Datum::Enum(e), _) if to.is_string() => Ok(Some(string_result(e.label.clone(), to)?)),
        // Either operand is a user type and no rule above applies.
        (Datum::Record(_) | Datum::Enum(_), _)
        | (_, ColumnType::Record(_) | ColumnType::Enum(_)) => Err(cannot_cast(value, to)),
        _ => Ok(None),
    }
}

fn value_enum_type(value: &Datum) -> Option<crate::usertype::UserTypeRef> {
    match value {
        Datum::Enum(e) => Some(e.ty),
        _ => None,
    }
}

/// Apply the target string type's length modifier to an already-rendered value.
fn string_result(text: String, to: ColumnType) -> Result<Datum, TypeError> {
    match to {
        ColumnType::Varchar(n) => {
            crate::string::apply_varchar_typmod(&text, n, Coercion::Explicit).map(Datum::Text)
        }
        ColumnType::Char(n) => {
            crate::string::apply_char_typmod(&text, n, Coercion::Explicit).map(Datum::Text)
        }
        _ => Ok(Datum::Text(text)),
    }
}

/// A composite → composite coercion: field counts must agree and each field is
/// cast to the target attribute's type.
fn cast_record(
    r: &crate::datum::RecordValue,
    target: Option<crate::usertype::UserTypeRef>,
    style: crate::encoding::OutputStyle<'_>,
) -> Result<Datum, TypeError> {
    let Some(named) = target else {
        // Every composite value is already a `record`; the pseudo-type imposes
        // no attribute list of its own.
        return Ok(Datum::Record(r.clone()));
    };
    let fields = composite_fields(named)?;
    if fields.len() != r.values.len() {
        return Err(TypeError::Coded {
            sqlstate: "42P16",
            message: format!(
                "cannot cast type record to {}: input has too {} columns",
                named.name,
                if r.values.len() < fields.len() {
                    "few"
                } else {
                    "many"
                }
            ),
        });
    }
    let mut values = Vec::with_capacity(fields.len());
    for (value, field) in r.values.iter().zip(&fields) {
        values.push(cast_in(value, field.ty, style)?);
    }
    Ok(Datum::Record(crate::datum::RecordValue::named(
        Some(named),
        field_names(&fields),
        values,
    )))
}

/// `record_in` for a named composite.
fn record_from_text(
    text: &str,
    named: crate::usertype::UserTypeRef,
    style: crate::encoding::OutputStyle<'_>,
) -> Result<Datum, TypeError> {
    let fields = composite_fields(named)?;
    let mut parts = crate::composite::record_fields(text)?;
    // `()` splits as one NULL field, which against a zero-attribute composite
    // is the empty record — the literal alone cannot express the difference.
    if fields.is_empty() && parts.len() == 1 && parts[0].is_none() {
        parts.clear();
    }
    if parts.len() != fields.len() {
        return Err(crate::composite::malformed(
            text,
            if parts.len() < fields.len() {
                "Too few columns."
            } else {
                "Too many columns."
            },
        ));
    }
    let mut values = Vec::with_capacity(fields.len());
    for (part, field) in parts.into_iter().zip(&fields) {
        values.push(match part {
            None => Datum::Null,
            Some(field_text) => cast_in(&Datum::Text(field_text), field.ty, style)?,
        });
    }
    Ok(Datum::Record(crate::datum::RecordValue::named(
        Some(named),
        field_names(&fields),
        values,
    )))
}

/// `enum_in`: the label, whitespace-trimmed as `PostgreSQL` trims it, must be
/// one of the type's.
fn enum_from_text(text: &str, named: crate::usertype::UserTypeRef) -> Result<Datum, TypeError> {
    let Some(ty) = crate::usertype::lookup_oid(named.oid) else {
        return Err(undefined_type(named.name));
    };
    let trimmed = text.trim();
    if ty
        .labels()
        .is_some_and(|labels| labels.iter().any(|label| label == trimmed))
    {
        return Ok(Datum::Enum(crate::datum::EnumValue {
            ty: named,
            label: trimmed.to_string(),
        }));
    }
    Err(TypeError::Coded {
        sqlstate: "22P02",
        message: format!("invalid input value for enum {}: \"{text}\"", named.name),
    })
}

fn composite_fields(
    named: crate::usertype::UserTypeRef,
) -> Result<Vec<crate::usertype::CompositeField>, TypeError> {
    let ty = crate::usertype::lookup_oid(named.oid).ok_or_else(|| undefined_type(named.name))?;
    Ok(ty.fields().unwrap_or(&[]).to_vec())
}

fn field_names(fields: &[crate::usertype::CompositeField]) -> std::sync::Arc<[String]> {
    fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>()
        .into()
}

fn undefined_type(name: &str) -> TypeError {
    TypeError::Coded {
        sqlstate: "42704",
        message: format!("type \"{name}\" does not exist"),
    }
}

fn to_numeric(
    d: crate::numeric::NumericValue,
    tm: Option<crate::numeric::Typmod>,
) -> Result<Datum, TypeError> {
    match tm {
        Some(tm) => Ok(Datum::Numeric(crate::numeric::apply_typmod(&d, tm)?)),
        None => Ok(Datum::Numeric(d)),
    }
}

/// The canonical wire text rendering of a non-NULL Datum (the same encoder the
/// DataRow path uses), for the numeric/`*`→`text` casts.
fn text_of(d: &Datum, style: crate::encoding::OutputStyle<'_>) -> String {
    String::from_utf8(crate::encoding::encode_text_in(d, style))
        .expect("a Datum's text encoding is always valid UTF-8")
}

fn string_cast_input(d: &Datum, style: crate::encoding::OutputStyle<'_>) -> String {
    match d {
        Datum::Bool(true) => "true".to_string(),
        Datum::Bool(false) => "false".to_string(),
        _ => text_of(d, style),
    }
}

/// `int4`/`int8`/`numeric` → `int2`: out of range is 22003, spelled the way
/// PostgreSQL's `int42`/`int82`/`numeric_int2` spell it.
fn i2_from_i64(n: i64) -> Result<Datum, TypeError> {
    i16::try_from(n)
        .map(Datum::Int2)
        .map_err(|_| TypeError::out_of_range_for("smallint"))
}

/// `float4`/`float8` → `int2`: round half-to-even (PostgreSQL `dtoi2`/`ftoi2`
/// use `rint`), then range-check; non-finite or out of range is 22003.
fn i2_from_f64(f: f64) -> Result<Datum, TypeError> {
    let r = f.round_ties_even();
    if r.is_finite() && (f64::from(i16::MIN)..=f64::from(i16::MAX)).contains(&r) {
        Ok(Datum::Int2(r as i16))
    } else {
        Err(TypeError::out_of_range_for("smallint"))
    }
}

/// `float8`/`numeric` → `float4` (PostgreSQL `dtof`): a finite input that
/// becomes infinite is `value out of range: overflow`, and a non-zero input
/// that flushes to zero is `value out of range: underflow`. An input that is
/// *already* infinite passes straight through, as it does in PostgreSQL.
fn f4_from_f64(f: f64) -> Result<Datum, TypeError> {
    let narrowed = f as f32;
    if narrowed.is_infinite() && f.is_finite() {
        return Err(TypeError::float_overflow());
    }
    if narrowed == 0.0 && f != 0.0 {
        return Err(TypeError::float_underflow());
    }
    Ok(Datum::Float4(narrowed))
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
/// `text → int2` (PostgreSQL `int2in`). Out of range reports the *original*
/// string, spaces and all, exactly as `pg_strtoint16_safe` does.
fn text_to_i16(s: &str) -> Result<Datum, TypeError> {
    require_int_syntax(s, "smallint")?;
    s.trim()
        .parse::<i16>()
        .map(Datum::Int2)
        .map_err(|_| TypeError::value_out_of_range(s, "smallint"))
}

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

/// `text → float4`, matching PostgreSQL `float4in`: trimmed, decimal/exponent
/// forms plus the case-insensitive `Infinity`/`inf`/`NaN` spellings. Bad syntax
/// is 22P02; a finite literal that overflows to infinity OR flushes a non-zero
/// magnitude to zero is 22003 — `strtof` reports both through `ERANGE`, so
/// `'1e39'` and `'1e-46'` are equally out of range while the subnormal
/// `'1e-45'` is a value. The out-of-range message quotes the *trimmed* text,
/// as PostgreSQL does.
fn text_to_f32(s: &str) -> Result<Datum, TypeError> {
    let t = s.trim();
    let Ok(parsed) = t.parse::<f32>() else {
        return Err(TypeError::InvalidText {
            type_name: "real",
            value: s.to_string(),
        });
    };
    let underflowed = parsed == 0.0 && has_nonzero_digit(t);
    if underflowed || (parsed.is_infinite() && !is_infinity_spelling(t)) {
        return Err(TypeError::float_text_out_of_range(t, "real"));
    }
    Ok(Datum::Float4(parsed))
}

/// Does `t` contain a non-zero decimal digit? A parse result of exactly zero
/// from such a string is an underflow, not the value zero (`'1e-46'` vs
/// `'0.000'`).
fn has_nonzero_digit(t: &str) -> bool {
    let mantissa = t.split(['e', 'E']).next().unwrap_or(t);
    mantissa.bytes().any(|b| b.is_ascii_digit() && b != b'0')
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
    fn assignment_cast_matrix_is_a_strict_subset_of_the_explicit_matrix() {
        use ColumnType::{
            Bool, Bytea, Date, Float8, Int4, Int8, Interval, Jsonb, Regclass, Text, Time,
            Timestamp, Timestamptz, Uuid,
        };
        use assert2::assert;

        use crate::ElemType;
        let types = [
            Jsonb,
            ColumnType::Array(ElemType::Int4),
            ColumnType::Array(ElemType::Text),
            Bool,
            Int4,
            Int8,
            Float8,
            ColumnType::Numeric(None),
            Text,
            ColumnType::Varchar(Some(8)),
            ColumnType::Char(Some(8)),
            Date,
            Time,
            Timestamp,
            Timestamptz,
            Interval,
            Bytea,
            Uuid,
            Regclass,
        ];
        for from in types {
            for to in types {
                assert!(
                    !assignment_cast_allowed(from, to) || cast_allowed(from, to),
                    "assignment allows {from:?} -> {to:?} but explicit does not"
                );
            }
        }
    }

    #[test]
    fn assignment_cast_allowed_matches_pg_cast_implicit_and_assignment_pairs() {
        use ColumnType::{Bool, Date, Float8, Int4, Int8, Text, Time, Timestamp, Timestamptz};
        use assert2::assert;
        let numeric = ColumnType::Numeric(None);
        // Allowed: pg_cast castcontext 'i'/'a' pairs among crabka's types.
        for (from, to) in [
            // Numeric family widenings ('i') and narrowings ('a').
            (Int4, Int8),
            (Int8, Int4),
            (Int4, Float8),
            (Float8, Int4),
            (Int8, numeric),
            (numeric, Float8),
            // date → timestamp/timestamptz ('i').
            (Date, Timestamp),
            (Date, Timestamptz),
            // timestamp → timestamptz ('i'); timestamptz → timestamp ('a').
            (Timestamp, Timestamptz),
            (Timestamptz, Timestamp),
            // String family ('i'/'a').
            (Text, ColumnType::Varchar(Some(4))),
            (ColumnType::Varchar(None), Text),
            (Text, ColumnType::Char(Some(4))),
        ] {
            assert!(assignment_cast_allowed(from, to), "{from:?} -> {to:?}");
        }
        // NOT allowed: explicit-only pairs must keep requiring a CAST.
        for (from, to) in [
            // I/O-conversion casts (explicit-only since PostgreSQL 8.3).
            (Int4, Text),
            (Text, Int4),
            (Float8, Text),
            (Text, Timestamp),
            (Timestamptz, Text),
            (Bool, Text),
            (Text, Bool),
            // bool ↔ int4 is castcontext 'e'.
            (Bool, Int4),
            (Int4, Bool),
            // Conservative subset: temporal truncations stay explicit here.
            (Timestamp, Date),
            (Timestamptz, Date),
            (Timestamp, Time),
            (Timestamptz, Time),
            // Cross-family nonsense.
            (Int4, Timestamp),
            (Timestamp, Int4),
            (Date, Int8),
            (ColumnType::Interval, Timestamp),
            (Time, Timestamp),
        ] {
            assert!(!assignment_cast_allowed(from, to), "{from:?} -> {to:?}");
        }
    }

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
        // `cast` is the EXPLICIT context, which truncates an over-long value —
        // `'abcd'::varchar(3)` is `abc` on PostgreSQL, not an error.
        assert_eq!(
            cast(
                &Datum::Text("abcd".into()),
                ColumnType::Varchar(Some(3)),
                &tz
            )
            .expect("an explicit cast truncates"),
            Datum::Text("abc".into())
        );
        assert_eq!(
            cast(&Datum::Text("abcd".into()), ColumnType::Char(Some(2)), &tz)
                .expect("an explicit cast truncates"),
            Datum::Text("ab".into())
        );
        // `cast_assign` is the ASSIGNMENT context, which rejects it — unless the
        // characters it would discard are all spaces.
        assert!(matches!(
            cast_assign(
                &Datum::Text("abcd".into()),
                ColumnType::Varchar(Some(3)),
                &tz
            ),
            Err(TypeError::StringDataRightTruncation)
        ));
        assert_eq!(
            cast_assign(
                &Datum::Text("abc  ".into()),
                ColumnType::Varchar(Some(3)),
                &tz
            )
            .expect("trailing spaces are discardable"),
            Datum::Text("abc".into())
        );
        // The same split applies per element through an array of a bounded
        // string, and a NULL passes either way untouched.
        let elem = ColumnType::Array(crate::ElemType::Varchar(Some(3)));
        assert!(matches!(
            cast_assign(&Datum::Text("{abcd}".into()), elem, &tz),
            Err(TypeError::StringDataRightTruncation)
        ));
        assert_eq!(
            cast_assign(&Datum::Null, ColumnType::Varchar(Some(3)), &tz).expect("null"),
            Datum::Null
        );
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

    // ---- jsonb / arrays ----

    #[test]
    fn jsonb_casts_parse_and_render_canonically() {
        use assert2::assert;

        use crate::JsonbValue;
        let tz = utc();
        let jsonb = ColumnType::Jsonb;
        // text → jsonb decomposes (key order + duplicate keys normalized).
        let value =
            cast(&Datum::Text(r#"{"b":1,"a":2,"a":3}"#.into()), jsonb, &tz).expect("text -> jsonb");
        assert!(value == cast(&Datum::Text(r#"{"a":3,"b":1}"#.into()), jsonb, &tz).expect("same"));
        // jsonb → text is the canonical rendering, and identity round-trips.
        assert!(
            cast(&value, ColumnType::Text, &tz).expect("jsonb -> text")
                == Datum::Text(r#"{"a": 3, "b": 1}"#.into())
        );
        assert!(cast(&value, jsonb, &tz).expect("identity") == value);
        // Bad JSON is 22P02, not a panic.
        let err = cast(&Datum::Text("{oops".into()), jsonb, &tz).expect_err("bad json");
        assert!(err.sqlstate() == "22P02");
        // NULL casts to NULL like every other target.
        assert!(cast(&Datum::Null, jsonb, &tz).expect("null") == Datum::Null);
        // `json` is an input alias for the same type.
        assert!(ColumnType::from_sql_name("json") == Some(ColumnType::Jsonb));
        assert!(
            cast(&Datum::Text("[]".into()), jsonb, &tz).expect("empty")
                == Datum::Jsonb(JsonbValue::Array(vec![]))
        );
    }

    #[test]
    fn array_casts_parse_render_and_convert_element_wise() {
        use assert2::assert;

        use crate::{ArrayValue, ElemType};
        let tz = utc();
        let int_array = ColumnType::Array(ElemType::Int4);
        let text_array = ColumnType::Array(ElemType::Text);
        let parsed =
            cast(&Datum::Text("{1,NULL,3}".into()), int_array, &tz).expect("text -> int[]");
        assert!(
            parsed
                == Datum::Array(ArrayValue::new(
                    ElemType::Int4,
                    vec![Datum::Int4(1), Datum::Null, Datum::Int4(3)],
                ))
        );
        // → text renders the literal back.
        assert!(
            cast(&parsed, ColumnType::Text, &tz).expect("int[] -> text")
                == Datum::Text("{1,NULL,3}".into())
        );
        // Identity keeps the element type; a differing element type converts.
        assert!(cast(&parsed, int_array, &tz).expect("identity") == parsed);
        assert!(
            cast(&parsed, text_array, &tz).expect("int[] -> text[]")
                == Datum::Array(ArrayValue::new(
                    ElemType::Text,
                    vec![
                        Datum::Text("1".into()),
                        Datum::Null,
                        Datum::Text("3".into())
                    ],
                ))
        );
        // An empty array is typed by its target.
        assert!(
            cast(&Datum::Text("{}".into()), text_array, &tz).expect("empty")
                == Datum::Array(ArrayValue::new(ElemType::Text, vec![]))
        );
        // A bad element is the element type's own error (22P02 here).
        let err = cast(&Datum::Text("{x}".into()), int_array, &tz).expect_err("bad element");
        assert!(err.sqlstate() == "22P02");
        // A multidimensional literal keeps its dimension header through the cast.
        let square = cast(&Datum::Text("{{1,2},{3,4}}".into()), int_array, &tz).expect("multidim");
        assert!(
            square
                == Datum::Array(ArrayValue::with_dims(
                    ElemType::Int4,
                    vec![
                        Datum::Int4(1),
                        Datum::Int4(2),
                        Datum::Int4(3),
                        Datum::Int4(4)
                    ],
                    vec![crate::ArrayDim::new(1, 2), crate::ArrayDim::new(1, 2)],
                ))
        );
        assert!(
            cast(&square, ColumnType::Text, &tz).expect("multidim -> text")
                == Datum::Text("{{1,2},{3,4}}".into())
        );
        // The element-wise conversion keeps the header too.
        let as_text = cast(&square, text_array, &tz).expect("int[] -> text[]");
        assert!(
            cast(&as_text, ColumnType::Text, &tz).expect("text[] -> text")
                == Datum::Text("{{1,2},{3,4}}".into())
        );
        // A non-default lower bound survives both directions.
        let shifted = cast(&Datum::Text("[2:4]={1,2,3}".into()), int_array, &tz).expect("bounds");
        assert!(
            cast(&shifted, ColumnType::Text, &tz).expect("bounds -> text")
                == Datum::Text("[2:4]={1,2,3}".into())
        );
    }

    #[test]
    fn jsonb_and_array_cast_matrix_allows_only_string_conversions() {
        use assert2::assert;

        use crate::ElemType;
        let jsonb = ColumnType::Jsonb;
        let int_array = ColumnType::Array(ElemType::Int4);
        let text_array = ColumnType::Array(ElemType::Text);
        // Allowed: identity, ↔ the string family, and array ↔ array when the
        // element cast is defined.
        for (from, to) in [
            (jsonb, jsonb),
            (jsonb, ColumnType::Text),
            (ColumnType::Text, jsonb),
            (ColumnType::Varchar(None), jsonb),
            (int_array, int_array),
            (int_array, text_array),
            (text_array, int_array),
            (int_array, ColumnType::Text),
            (ColumnType::Text, int_array),
        ] {
            assert!(cast_allowed(from, to), "{from:?} -> {to:?}");
        }
        // Not allowed: jsonb/array ↔ anything outside the string family.
        for (from, to) in [
            (jsonb, ColumnType::Int4),
            (ColumnType::Int4, jsonb),
            (jsonb, ColumnType::Bool),
            (jsonb, int_array),
            (int_array, jsonb),
            (int_array, ColumnType::Int4),
            (ColumnType::Int4, int_array),
            (int_array, ColumnType::Array(ElemType::Date)),
            (ColumnType::Timestamp, jsonb),
        ] {
            assert!(!cast_allowed(from, to), "{from:?} -> {to:?}");
        }
        // Assignment context stays identity-only for these types.
        assert!(assignment_cast_allowed(jsonb, jsonb));
        assert!(assignment_cast_allowed(int_array, int_array));
        assert!(!assignment_cast_allowed(ColumnType::Text, jsonb));
        assert!(!assignment_cast_allowed(int_array, text_array));
    }

    // ---- int2 / float4 ----

    /// Every expectation is `SELECT <value>::<target>` on PostgreSQL 18.4.
    #[test]
    fn int2_and_float4_value_casts_match_postgres() {
        use ColumnType::{Float4, Float8, Int2, Int4, Int8, Numeric, Text};
        use assert2::assert;
        let tz = utc();
        let num = |s: &str| Datum::Numeric(crate::numeric::parse(s).expect("numeric"));
        let cases: &[(Datum, ColumnType, Datum)] = &[
            // int2 widens exactly into every wider numeric type.
            (Datum::Int2(5), Int4, Datum::Int4(5)),
            (Datum::Int2(5), Int8, Datum::Int8(5)),
            (Datum::Int2(5), Float4, Datum::Float4(5.0)),
            (Datum::Int2(5), Float8, Datum::Float8(5.0)),
            (Datum::Int2(-32_768), Text, Datum::Text("-32768".into())),
            (Datum::Int2(5), Numeric(None), num("5")),
            // … and narrows back, rounding half-to-even from the floats and
            // half-away-from-zero from numeric (PostgreSQL's `rint` vs `HalfUp`).
            (Datum::Int4(-32_768), Int2, Datum::Int2(-32_768)),
            (Datum::Int8(32_767), Int2, Datum::Int2(32_767)),
            (Datum::Float8(2.5), Int2, Datum::Int2(2)),
            (Datum::Float8(3.5), Int2, Datum::Int2(4)),
            (Datum::Float4(-2.5), Int2, Datum::Int2(-2)),
            (num("2.5"), Int2, Datum::Int2(3)),
            (num("-2.5"), Int2, Datum::Int2(-3)),
            (Datum::Text("  -32768  ".into()), Int2, Datum::Int2(-32_768)),
            (Datum::Text("+5".into()), Int2, Datum::Int2(5)),
            // float4 ↔ the rest. `float4 → float8` is exact widening, so the
            // f32 rounding of 1.1 becomes visible.
            (Datum::Float4(1.1), Float8, Datum::Float8(f64::from(1.1f32))),
            (Datum::Float8(1.1), Float4, Datum::Float4(1.1)),
            (Datum::Float4(2.5), Int4, Datum::Int4(2)),
            (Datum::Float4(3.5), Int4, Datum::Int4(4)),
            (Datum::Float4(1.5), Int8, Datum::Int8(2)),
            (Datum::Float4(1.5), Text, Datum::Text("1.5".into())),
            (Datum::Text(" 1.5 ".into()), Float4, Datum::Float4(1.5)),
            (
                Datum::Text("INFINITY".into()),
                Float4,
                Datum::Float4(f32::INFINITY),
            ),
            (
                Datum::Text("-inf".into()),
                Float4,
                Datum::Float4(f32::NEG_INFINITY),
            ),
            // The subnormal `1e-45` is a value, not an underflow.
            (Datum::Text("1e-45".into()), Float4, Datum::Float4(1e-45)),
            (Datum::Int4(5), Float4, Datum::Float4(5.0)),
            (Datum::Int8(5), Float4, Datum::Float4(5.0)),
            (num("0.1"), Float4, Datum::Float4(0.1)),
            // `float4 → numeric` goes through `%.6g`, NOT the shortest
            // round-tripping text, so precision is deliberately lost.
            (Datum::Float4(1.1), Numeric(None), num("1.1")),
            (Datum::Float4(0.1), Numeric(None), num("0.1")),
            (Datum::Float4(2.0), Numeric(None), num("2")),
            (Datum::Float4(16_777_216.0), Numeric(None), num("16777200")),
            (
                Datum::Float4(3.402_823_5e38),
                Numeric(None),
                num("340282000000000000000000000000000000000"),
            ),
            (Datum::Null, Int2, Datum::Null),
            (Datum::Null, Float4, Datum::Null),
        ];
        for (value, target, expected) in cases {
            assert!(
                cast(value, *target, &tz).expect("defined cast") == *expected,
                "{value:?} -> {target:?}"
            );
        }
        // NaN survives the float4 round trip (it is never `==` itself, so it
        // cannot ride in the table above).
        assert!(matches!(
            cast(&Datum::Text("NaN".into()), Float4, &tz),
            Ok(Datum::Float4(f)) if f.is_nan()
        ));
    }

    /// PostgreSQL's exact SQLSTATE *and* message for every out-of-range or
    /// malformed int2/float4 conversion.
    #[test]
    fn int2_and_float4_cast_errors_match_postgres() {
        use ColumnType::{Float4, Int2};
        use assert2::assert;
        let tz = utc();
        let num = |s: &str| Datum::Numeric(crate::numeric::parse(s).expect("numeric"));
        let cases: &[(Datum, ColumnType, &str, &str)] = &[
            (Datum::Int4(100_000), Int2, "22003", "smallint out of range"),
            (Datum::Int8(-40_000), Int2, "22003", "smallint out of range"),
            (
                Datum::Float8(40_000.0),
                Int2,
                "22003",
                "smallint out of range",
            ),
            (
                Datum::Float8(f64::NAN),
                Int2,
                "22003",
                "smallint out of range",
            ),
            (num("32768"), Int2, "22003", "smallint out of range"),
            (
                Datum::Text("  99999  ".into()),
                Int2,
                "22003",
                // PostgreSQL quotes the ORIGINAL string, spaces included.
                "value \"  99999  \" is out of range for type smallint",
            ),
            (
                Datum::Text("abc".into()),
                Int2,
                "22P02",
                "invalid input syntax for type smallint: \"abc\"",
            ),
            (
                Datum::Text("1.5".into()),
                Int2,
                "22P02",
                "invalid input syntax for type smallint: \"1.5\"",
            ),
            (
                Datum::Float8(1e39),
                Float4,
                "22003",
                "value out of range: overflow",
            ),
            (
                Datum::Float8(1e-46),
                Float4,
                "22003",
                "value out of range: underflow",
            ),
            (
                Datum::Text(" 1e39 ".into()),
                Float4,
                "22003",
                "\"1e39\" is out of range for type real",
            ),
            (
                Datum::Text("1e-400".into()),
                Float4,
                "22003",
                "\"1e-400\" is out of range for type real",
            ),
            (
                Datum::Text("1.2.3".into()),
                Float4,
                "22P02",
                "invalid input syntax for type real: \"1.2.3\"",
            ),
        ];
        for (value, target, sqlstate, message) in cases {
            let err = cast(value, *target, &tz).expect_err("out of range / malformed");
            assert!(err.sqlstate() == *sqlstate, "{value:?} -> {target:?}");
            assert!(err.to_string() == *message, "{value:?} -> {target:?}");
        }
        // A zero-valued literal is NOT an underflow, however it is spelled.
        for zero in ["0", "0.000", "0e100", "-0"] {
            assert!(
                matches!(
                    cast(&Datum::Text(zero.into()), Float4, &tz),
                    Ok(Datum::Float4(f)) if f == 0.0
                ),
                "{zero:?} is the value zero"
            );
        }
    }

    /// int2/float4 join the numeric family for casting, but — like PostgreSQL —
    /// neither gains a `bool` cast: only `int4` has one.
    #[test]
    fn int2_and_float4_cast_matrix_excludes_bool() {
        use ColumnType::{Bool, Float4, Float8, Int2, Int4, Int8, Text};
        use assert2::assert;
        let num = ColumnType::Numeric(None);
        for a in [Int2, Int4, Int8, Float4, Float8, num] {
            for b in [Int2, Int4, Int8, Float4, Float8, num] {
                assert!(cast_allowed(a, b), "{a:?} -> {b:?}");
                assert!(assignment_cast_allowed(a, b), "assign {a:?} -> {b:?}");
            }
            assert!(cast_allowed(a, Text) && cast_allowed(Text, a), "{a:?}/text");
        }
        for (from, to) in [(Bool, Int2), (Int2, Bool), (Bool, Float4), (Float4, Bool)] {
            assert!(!cast_allowed(from, to), "{from:?} -> {to:?}");
        }
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
